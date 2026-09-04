"""
Data acquisition for canoe-cef.

Every function here returns a plain pandas.DataFrame and imports nothing from
canoe_schema and writes no SQL. Transformation and persistence live elsewhere
(all_sectors.py).
"""

import pandas as pd


def load_cef_end_use_demand(path: str) -> pd.DataFrame:
    """Load Canada's Energy Future (CEF) end-use demand projections.

    Source: Canada Energy Regulator, "Canada's Energy Future" report
    (https://www.cer-rec.gc.ca/en/data-analysis/canada-energy-future/).
    This is a bundled, manually-downloaded CSV (see SOURCES.md) with no
    network fetch or caching involved.

    Returns a DataFrame with columns: Scenario, Region, Variable, Year,
    Value, Sector.
    """
    return pd.read_csv(path)


def load_dsd_electricity(path: str) -> pd.DataFrame:
    """Load electricity Demand Specific Distribution (DSD) shares.

    Source: bundled input file (see SOURCES.md), not fetched over the
    network.

    Returns a DataFrame with columns: season, tod, and one column per
    "<region>.<sector_tag>" combination giving that slice's share of
    annual electricity demand.
    """
    return pd.read_csv(path)
