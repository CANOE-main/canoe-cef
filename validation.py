"""
Validation for canoe-cef: checks this module's configuration against the
global, canoe-base-owned tables already in the database (time_period,
region, time_season, time_of_day), before any writes happen. Read-only.
"""

from __future__ import annotations

import sqlite3

import pandas as pd

from config import CANOECEFConfig
from time_utils import period_index


def validate_db_against_config(
    cfg: CANOECEFConfig,
    conn: sqlite3.Connection,
    dsd_path: str | None = None,
) -> None:
    missing_periods = check_missing_periods(conn, cfg.model_periods)
    if missing_periods:
        _handle(cfg, f"time_period is missing period(s) required by model_periods: {missing_periods}")

    missing_regions = check_missing_regions(conn, cfg.model_regions)
    if missing_regions:
        _handle(cfg, f"region is missing region(s) required by model_regions: {missing_regions}")

    if cfg.use_dsd and dsd_path:
        missing_seasons, missing_tods = check_missing_time_slices(conn, dsd_path)
        if missing_seasons:
            _handle(cfg, f"time_season is missing season(s) required by the DSD input: {missing_seasons}")
        if missing_tods:
            _handle(cfg, f"time_of_day is missing tod(s) required by the DSD input: {missing_tods}")


def check_missing_periods(conn: sqlite3.Connection, model_periods: list[int]) -> list[int]:
    """model_periods holds CEF year labels; time_period stores period_index(p)."""
    existing = {row[0] for row in conn.execute("SELECT period FROM time_period").fetchall()}
    return [p for p in model_periods if period_index(p) not in existing]


def check_missing_regions(conn: sqlite3.Connection, model_regions: list[str]) -> list[str]:
    existing = {row[0] for row in conn.execute("SELECT region FROM region").fetchall()}
    return [r for r in model_regions if r not in existing]


def check_missing_time_slices(conn: sqlite3.Connection, dsd_path: str) -> tuple[list[str], list[str]]:
    df_dsd = pd.read_csv(dsd_path)
    existing_seasons = {row[0] for row in conn.execute("SELECT season FROM time_season").fetchall()}
    existing_tods = {row[0] for row in conn.execute("SELECT tod FROM time_of_day").fetchall()}
    missing_seasons = sorted(set(df_dsd["season"].unique()) - existing_seasons)
    missing_tods = sorted(set(df_dsd["tod"].unique()) - existing_tods)
    return missing_seasons, missing_tods


def _handle(cfg: CANOECEFConfig, message: str) -> None:
    if cfg.validation_behavior == "error":
        raise ValueError(message)
    print(f"WARNING: {message}")
