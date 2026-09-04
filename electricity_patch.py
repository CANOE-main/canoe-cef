# -*- coding: utf-8 -*-
"""Electricity transfer bridges for the low-resolution CEF sectors."""

from __future__ import annotations

import sqlite3

import pandas as pd

from canoe_schema.v4_0.enums import TechnologyTypeCode
from canoe_schema.v4_0.models import (
    Efficiency,
    LifetimeTech,
    Technology,
    TechnologyLabel,
)

from config import CANOECEFConfig
from sql_helpers import upsert_many
from time_utils import period_index


def add_electricity_bridges(
    cfg: CANOECEFConfig,
    cursor: sqlite3.Cursor,
    df_cef: pd.Series,
) -> None:
    """Add electricity-sector transfer technologies used by active CEF sectors.

    For each sector that still has an electricity input after CEF filtering,
    create a pathway such as:

        E_elc_dem -> E_I_ELC -> I_elc -> I_IND -> I_D_ind

    Equivalent bridges are generated dynamically for Commercial, Residential,
    and Transportation when those sector/electricity combinations are present.
    """

    configured_commodities = {
        commodity.comm.lower()
        for commodity in cfg.commodities
        if commodity.include
    }

    if "elc" not in configured_commodities:
        print("Electricity bridge skipped: 'elc' is not an included CEF commodity.")
        return

    # CEF expects to append to a shared CANOE database. The electricity sector
    # should therefore already own/create the upstream electricity commodity.
    source_exists = cursor.execute(
        "SELECT 1 FROM Commodity WHERE name = ? LIMIT 1",
        ("E_elc_dem",),
    ).fetchone()

    if source_exists is None:
        raise RuntimeError(
            "Cannot create CEF electricity bridges because 'E_elc_dem' is "
            "missing from Commodity. Build the electricity sector before CEF."
        )

    active = df_cef.reset_index()
    electricity_rows = active[
        active["comm"].astype(str).str.endswith("_elc")
    ].copy()

    if electricity_rows.empty:
        print("Electricity bridge skipped: no CEF electricity streams survived filtering.")
        return

    # Identify only sector/region combinations that actually use electricity.
    active_pairs = (
        electricity_rows.assign(
            tag=electricity_rows["comm"].str.split("_", n=1).str[0]
        )[["region", "tag"]]
        .drop_duplicates()
        .sort_values(["tag", "region"])
    )

    active_tags = sorted(active_pairs["tag"].unique())

    technologies: list[Technology] = []
    technology_labels: list[TechnologyLabel] = []
    efficiencies: list[Efficiency] = []
    lifetimes: list[LifetimeTech] = []

    for tag in active_tags:
        sector = cfg.sector_by_tag[tag]
        transfer_tech = f"E_{tag}_ELC"

        # Follow CEF's existing ownership convention for Technology:
        # sector-level data_id, without a regional suffix.
        technologies.append(
            Technology(
                tech=transfer_tech,
                flag=TechnologyTypeCode.P,
                sector=sector.sector,
                unlim_cap=1,
                annual=0,
                description=(
                    f"Electricity transfer from the electricity sector "
                    f"to the {sector.sector} sector"
                ),
                data_id=cfg.data_id(sector.code),
            )
        )
        technology_labels.append(TechnologyLabel(tech=transfer_tech))

    for row in active_pairs.itertuples(index=False):
        region = row.region
        tag = row.tag
        sector = cfg.sector_by_tag[tag]

        transfer_tech = f"E_{tag}_ELC"
        output_comm = f"{tag}_elc"
        data_id = cfg.data_id(sector.code, region)

        # CEF model_periods are end-year labels. Convert each one to the
        # planning-period/vintage value stored in the CANOE database.
        for model_period in cfg.model_periods:
            efficiencies.append(
                Efficiency(
                    region=region,
                    input_comm="E_elc_dem",
                    tech=transfer_tech,
                    vintage=period_index(model_period),
                    output_comm=output_comm,
                    efficiency=1.0,
                    notes="Arbitrary unit efficiency for electricity transfer technology",
                    data_id=data_id,
                )
            )

        # Match the existing transfer-technology convention: renew every
        # five years so the bridge remains available through the horizon.
        lifetimes.append(
            LifetimeTech(
                region=region,
                tech=transfer_tech,
                lifetime=5,
                notes=(
                    "Arbitrary five-year lifetime so the electricity transfer "
                    "technology is renewed each model period"
                ),
                data_id=data_id,
            )
        )

    upsert_many(cursor, technologies)
    upsert_many(cursor, technology_labels)
    upsert_many(cursor, efficiencies)
    upsert_many(cursor, lifetimes)

    print(
        "Added CEF electricity bridges: "
        f"{len(technologies)} technologies, "
        f"{len(efficiencies)} efficiencies, "
        f"{len(lifetimes)} lifetimes."
    )