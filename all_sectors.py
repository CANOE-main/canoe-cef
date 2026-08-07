"""
Orchestrates the CEF -> CANOE aggregation pipeline: validate against the
shared database -> acquire CEF data -> transform -> construct canoe_schema
rows -> write. The mapping/filtering/aggregation logic below is unchanged
from the pre-refactor version; only how rows reach the database has
changed (Pydantic-model-driven upserts instead of hand-written SQL strings,
and no more direct writes to global tables).
"""

from __future__ import annotations

import sqlite3

import pandas as pd
from canoe_schema.v4_0.enums import CommodityTypeCode, OperatorCode, TechnologyTypeCode
from canoe_schema.v4_0.models import (
    Commodity,
    CommodityLabel,
    DataSet,
    DataSource,
    DataSourceLabel,
    Demand,
    DemandSpecificDistribution,
    Efficiency,
    LimitTechInputSplitAnnual,
    Technology,
    TechnologyLabel,
)

import data_scraper
import db
import validation
from config import Bibliography, CANOECEFConfig
from sql_helpers import upsert_many
from time_utils import period_index


def build(cfg: CANOECEFConfig | None = None) -> None:
    cfg = cfg or CANOECEFConfig.load()
    refs = Bibliography(cfg.data_variant)
    dsd_path = f"{cfg.input_files_dir}/dsd_electricity.csv"

    conn = db.open_database(cfg)
    try:
        validation.validate_db_against_config(cfg, conn, dsd_path=dsd_path if cfg.use_dsd else None)

        if cfg.force_wipe_database:
            db.wipe_module_data(cfg, conn)
            print("Cleared canoe-cef's previous rows prior to aggregation. See params.\n")

        build_sectors(cfg, refs, conn)
        if cfg.use_dsd:
            build_dsd(cfg, conn, dsd_path)
        build_metadata(cfg, refs, conn)
    finally:
        conn.close()

    print("\nFinished!")


def build_sectors(cfg: CANOECEFConfig, refs: Bibliography, conn: sqlite3.Connection) -> None:
    print("Adding sector processes...")
    curs = conn.cursor()

    df_cef = data_scraper.load_cef_end_use_demand(f"{cfg.input_files_dir}/end-use-demand-2023.csv")

    region_map = cfg.region_map
    commodity_map = cfg.commodity_map
    sector_map = cfg.sector_map
    technology_map = cfg.technology_map

    # We use period-end data in CANOE, so convert CEF year to period-end
    # df_cef["Year"] -= 5 # TODO: Check if this is correct

    # Filter relevant data
    df_cef = df_cef[
        (df_cef["Scenario"] == cfg.scenario)
        & df_cef["Sector"].isin(sector_map.keys())
        & df_cef["Region"].isin(region_map.keys())
        & df_cef["Variable"].isin(commodity_map.keys())
        & df_cef["Year"].isin(cfg.model_periods)
    ]

    # Convert to CANOE indexing
    df_cef["region"] = df_cef["Region"].map(region_map, na_action="ignore")
    df_cef["tag"] = df_cef["Sector"].map(sector_map, na_action="ignore")
    df_cef["tech"] = df_cef["Sector"].map(technology_map, na_action="ignore")
    df_cef["comm"] = df_cef["Variable"].map(commodity_map, na_action="ignore")
    df_cef["tech"] = df_cef["tag"] + "_" + df_cef["tech"]
    df_cef["comm"] = df_cef["tag"] + "_" + df_cef["comm"]
    df_cef["period"] = df_cef["Year"]

    # hack: change rpp to appropriate
    df_cef['comm'] = df_cef['comm'].replace({'C_rpp': 'C_oil', 'R_rpp': 'R_oil', "I_rpp": "I_hfo"})

    # Convert energy units and filter out tiny values
    df_cef["value"] = df_cef["Value"] * cfg.conversion_factor
    df_cef["value"] = df_cef["value"].round(cfg.decimal_places)

    # Get the total energy for each process in each period
    df_sum = df_cef.groupby(["region", "tech", "period"])["value"].sum()

    # Filter out unused or tiny energy streams
    df = df_cef.groupby(["region", "tech", "comm"])[["period", "value"]]
    to_drop = set()
    for (region, tech, comm), _df in df:
        # Drop if this commodity isn't used by this sector in this region
        if _df["value"].sum() == 0:
            to_drop.add((region, tech, comm))
            continue
        _df = _df.set_index("period")
        _df["prop"] = [value.iloc[0] / df_sum.loc[(region, tech, period)] for period, value in _df.iterrows()]
        _df["keep"] = _df["prop"] >= cfg.prop_thresh

        # Drop if this commodity doesn't meet the proportion threshold
        if _df["keep"].sum() == 0:
            to_drop.add((region, tech, comm))
            continue

    df_cef = df_cef.set_index(["region", "tech", "comm"])
    df_cef = df_cef[~df_cef.index.isin(to_drop)]
    df_cef = df_cef.reset_index()

    # Group indices nicely
    df_cef = df_cef.set_index(["region", "tech", "period", "comm"])["value"]
    df_cef = df_cef.sort_index()

    # Add whole-sector processes: Technology + demand Commodity
    technologies = []
    technology_labels = []
    demand_commodities = []
    commodity_labels = []
    for tech in df_cef.index.get_level_values("tech").unique():
        sector = cfg.sector_by_tag[tech.split("_")[0]]
        data_id = cfg.data_id(sector.code)

        technologies.append(
            Technology(
                tech=tech,
                flag=TechnologyTypeCode.P,
                sector=sector.sector,
                unlim_cap=1,
                annual=1,
                description=sector.tech_desc,
                data_id=data_id,
            )
        )
        technology_labels.append(TechnologyLabel(tech=tech))

        dem_comm = f"{tech.split('_')[0]}_D_{tech.split('_')[1].lower()}"
        demand_commodities.append(
            Commodity(
                name=dem_comm,
                flag=CommodityTypeCode.D,
                description=f"({cfg.energy_units}) {sector.sector} energy demand",
                data_id=data_id,
            )
        )
        commodity_labels.append(CommodityLabel(commodity=dem_comm))

    upsert_many(curs, technologies)
    upsert_many(curs, technology_labels)
    upsert_many(curs, demand_commodities)
    upsert_many(curs, commodity_labels)

    # Add fuel commodities
    fuel_commodities = []
    fuel_commodity_labels = []
    for comm in df_cef.index.get_level_values("comm").unique():
        sector = cfg.sector_by_tag[comm.split("_")[0]]
        data_id = cfg.data_id(sector.code)

        # Because two CEF biofuel types go to bio
        commodity = cfg.commodity_by_code[comm.split("_")[1]]
        desc = f"{commodity.description} ({sector.sector})"

        fuel_commodities.append(
            Commodity(
                name=comm,
                flag=commodity.flag,
                description=f"({cfg.energy_units}) {desc}",
                data_id=data_id,
            )
        )
        fuel_commodity_labels.append(CommodityLabel(commodity=comm))

    upsert_many(curs, fuel_commodities)
    upsert_many(curs, fuel_commodity_labels)

    # First vintage processes for all techs
    efficiencies = []
    for region, tech, comm in df_cef.xs(cfg.model_periods[0], level="period").index:
        sector_code = cfg.sector_by_tag[comm.split("_")[0]].code
        data_id = cfg.data_id(sector_code, region)
        dem_comm = f"{tech.split('_')[0]}_D_{tech.split('_')[1].lower()}"

        efficiencies.append(
            Efficiency(
                region=region,
                input_comm=comm,
                tech=tech,
                vintage=period_index(cfg.model_periods[0]),
                output_comm=dem_comm,
                efficiency=1.0,
                data_id=data_id,
            )
        )
    upsert_many(curs, efficiencies)

    ref = refs.add("cer", cfg.cef_reference)

    # Demands for each sector
    demands = []
    splits = []
    for (region, tech, period), demand in df_cef.reset_index().groupby(["region", "tech", "period"]):
        p = period_index(period)

        sector_code = cfg.sector_by_tag[tech.split("_")[0]].code
        data_id = cfg.data_id(sector_code, region)

        demand = demand.round(cfg.decimal_places)
        dem_tot = round(demand["value"].sum(), cfg.decimal_places)
        dem_comm = f"{tech.split('_')[0]}_D_{tech.split('_')[1].lower()}"

        demands.append(
            Demand(
                region=region,
                period=p,
                commodity=dem_comm,
                demand=dem_tot,
                units=cfg.energy_units,
                data_source=ref.id,
                data_id=data_id,
            )
        )

        # Zero out tiny proportions and renormalise
        demand["prop"] = demand["value"] / dem_tot
        demand["prop"] = demand["prop"].where(demand["prop"] > cfg.prop_thresh, 0)
        demand["prop"] /= demand["prop"].sum()

        for _, row in demand.iterrows():
            splits.append(
                LimitTechInputSplitAnnual(
                    region=region,
                    period=p,
                    input_comm=row["comm"],
                    tech=tech,
                    operator=OperatorCode.LE,  # <= should, in theory, be least likely to cause infeasibility / numerical issues?
                    proportion=row["prop"],
                    data_source=ref.id,
                    data_id=data_id,
                )
            )

    upsert_many(curs, demands)
    upsert_many(curs, splits)

    conn.commit()


def build_dsd(cfg: CANOECEFConfig, conn: sqlite3.Connection, dsd_path: str) -> None:
    print("Adding DSDs...", end="")
    df_dsd = data_scraper.load_dsd_electricity(dsd_path)
    curs = conn.cursor()

    rows = []
    progr = 0
    n_steps = len(cfg.model_regions) * len(cfg.sectors)
    for region in cfg.model_regions:
        data_id = cfg.data_id(region)
        for sector in cfg.sectors:
            dem_comm = f"{sector.tag}_D_{sector.code.lower()}"
            for period in cfg.model_periods:
                p = period_index(period)
                for _, row in df_dsd.iterrows():
                    rows.append(
                        DemandSpecificDistribution(
                            region=region,
                            period=p,
                            season=row["season"],
                            tod=row["tod"],
                            demand_name=dem_comm,
                            dsd=row[f"{region}.{sector.tag}"],
                            data_id=data_id,
                        )
                    )
            progr += 1
            print(f"\rAdding DSDs... {progr / n_steps * 100:.0f}% complete.", end="")

    print("")
    upsert_many(curs, rows)
    conn.commit()


def build_metadata(cfg: CANOECEFConfig, refs: Bibliography, conn: sqlite3.Connection) -> None:
    curs = conn.cursor()

    # Add all references in the bibliography to the references tables
    sources = []
    source_labels = []
    for reference in refs:
        for sector in cfg.sectors:
            sources.append(
                DataSource(source_id=reference.id, source=reference.citation, data_id=cfg.data_id(sector.code))
            )
        source_labels.append(DataSourceLabel(source_id=reference.id))
    upsert_many(curs, sources)
    upsert_many(curs, source_labels)

    # Add datasets to DataSet table, now that all other metadata is known
    data_sets = [DataSet(data_id=data_id) for data_id in sorted(cfg.data_ids)]
    upsert_many(curs, data_sets)

    conn.commit()

    # Check for missing data IDs
    print("Checking that all data has a dataset ID...", end="")
    tables = [t[0] for t in curs.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]

    all_good = True
    for table in tables:
        cols = [c[1] for c in curs.execute(f"PRAGMA table_info({table})").fetchall()]
        if "data_id" in cols:
            bad_rows = pd.read_sql_query(f"SELECT * FROM {table} WHERE data_id is NULL", conn)
            if len(bad_rows) > 0:
                print(f"\nFound some rows missing data IDs in {table}")
                print(bad_rows)
                all_good = False

    if all_good:
        print(" All good!")


if __name__ == "__main__":
    build()
