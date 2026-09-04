"""
Persistence layer for canoe-cef.

The shared database is built once, from canoe_schema's versioned DDL, by
canoe-base - this module only ever opens it and appends its own rows.
`build_test_database` is the one deliberate exception: a small, fully
self-contained standalone database for local Temoa test runs. It is never
called from the normal `all_sectors.build()` pipeline (see __main__.py's
--build-test-db flag).
"""

from __future__ import annotations

import os
import sqlite3

import pandas as pd
from canoe_schema.sql import get_sql_schema
from canoe_schema.v4_0.enums import TimePeriodTypeCode
from canoe_schema.v4_0.models import Region, TimeOfDay, TimePeriod, TimeSeason

from config import CANOECEFConfig
from sql_helpers import upsert_many
from time_utils import period_index

# Content tables this module writes to, scoped by data_id. Registry/label
# tables (technology_label, commodity_label, data_source_label) are excluded
# deliberately: they're append-only "this code exists" markers, not per-run
# content, so a rerun should never need to clear them.
_MODULE_TABLES = (
    "technology",
    "commodity",
    "efficiency",
    "demand",
    "limit_tech_input_split_annual",
    "demand_specific_distribution",
    "data_source",
    "data_set",
)


def open_database(cfg: CANOECEFConfig) -> sqlite3.Connection:
    """Open the shared database that canoe-base already built.

    canoe-cef never creates or drops this database - only canoe-base does,
    from canoe_schema's versioned schema.sql.
    """
    if not os.path.exists(cfg.database_file):
        raise FileNotFoundError(
            f"Database not found at {cfg.database_file!r}. canoe-cef expects "
            f"canoe-base to have already built it from canoe_schema's "
            f"v{cfg.schema_version} schema."
        )
    return sqlite3.connect(cfg.database_file)


def wipe_module_data(cfg: CANOECEFConfig, conn: sqlite3.Connection) -> None:
    """Delete this module's own previously-written rows ahead of a clean
    re-run, scoped to the data_ids this config's naming convention can
    produce. Never touches other modules' rows or the database itself."""
    data_ids = _possible_data_ids(cfg)
    if not data_ids:
        return

    placeholders = ", ".join("?" for _ in data_ids)
    curs = conn.cursor()
    for table in _MODULE_TABLES:
        curs.execute(f"DELETE FROM {table} WHERE data_id IN ({placeholders})", tuple(data_ids))
    conn.commit()


def _possible_data_ids(cfg: CANOECEFConfig) -> set[str]:
    """Every data_id this config's naming convention (sector/region/variant/
    version) could produce, without mutating cfg's accumulated data_ids.

    Enumerates over *all* known sectors/regions (regardless of their current
    `include` flag), not just the ones active in this run - otherwise a
    region/sector toggled off since the last run would leave its old rows
    behind uncleared."""
    ids: set[str] = set()
    all_regions = [r.region for r in cfg.regions]
    for sector in cfg.sectors:
        ids.add(f"{sector.code}{cfg.data_variant}{cfg.data_version}")
        for region in all_regions:
            ids.add(f"{sector.code}{cfg.data_variant}{region}{cfg.data_version}")
    for region in all_regions:
        # matches build_dsd()'s config.data_id(region) call convention
        ids.add(f"{region}{cfg.data_variant}{cfg.data_version}")
    return ids


def build_test_database(cfg: CANOECEFConfig, dsd_path: str) -> None:
    """Build a small, self-contained standalone SQLite database for local
    dev/Temoa test runs.

    NOT the shared production database - this is a separate, explicitly
    invoked testing workflow. It builds its own database from canoe_schema's
    DDL directly (bypassing canoe-base) and mutates commodities so the model
    is solvable standalone, without any supply-side technologies.
    """
    df_dsd = pd.read_csv(dsd_path)

    conn = sqlite3.connect(cfg.database_file)
    conn.executescript(get_sql_schema(cfg.schema_version))
    curs = conn.cursor()

    # Make every annual commodity a source commodity so the standalone
    # model is self-sufficient without any supply-side technologies.
    # (A predicate-based bulk UPDATE like this has no CanoeBaseModel
    # equivalent - to_update_sql() operates on one already-known row/PK.)
    curs.execute("UPDATE commodity SET flag = 's' WHERE flag = 'a'")

    upsert_many(curs, [Region(region=region) for region in cfg.model_regions])

    periods = [period_index(p) for p in cfg.model_periods]
    time_periods = [
        TimePeriod(sequence=i, period=period, flag=TimePeriodTypeCode.F)
        for i, period in enumerate(periods)
    ]
    # Trailing terminal period, matching the original build_tester() behaviour.
    time_periods.append(
        TimePeriod(sequence=len(periods), period=periods[-1] + 5, flag=TimePeriodTypeCode.F)
    )
    upsert_many(curs, time_periods)

    # v4.0 folds the old SeasonLabel/TimeSegmentFraction tables into
    # TimeSeason.segment_fraction and TimeOfDay.hours, both period-
    # independent. df_dsd's season/tod values are calendar days (D001-D365)
    # and hours (H01-H24), so a uniform split reproduces the original
    # uniform 1/len(df_dsd) segment fraction.
    seasons = list(df_dsd["season"].unique())
    tods = list(df_dsd["tod"].unique())

    upsert_many(
        curs,
        [
            TimeSeason(sequence=i, season=season, segment_fraction=1 / len(seasons))
            for i, season in enumerate(seasons)
        ],
    )
    upsert_many(
        curs,
        [TimeOfDay(sequence=i, tod=tod, hours=24 / len(tods)) for i, tod in enumerate(tods)],
    )

    conn.commit()
    conn.close()
