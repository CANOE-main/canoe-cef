"""
Configuration for canoe-cef.

Loads params.yaml plus the region/commodity/sector mapping CSVs into a single
validated CANOECEFConfig. No SQL or database access happens here — see db.py
for persistence and all_sectors.py for the pipeline that consumes this
config.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import pandas as pd
import yaml
from pydantic import BaseModel, ConfigDict, PrivateAttr, field_validator

from canoe_schema.v4_0.enums import CommodityTypeCode

THIS_DIR = os.path.realpath(os.path.dirname(__file__))
INPUT_FILES_DIR = os.path.join(THIS_DIR, "input_files")


class CEFRegionMapping(BaseModel):
    """One row of regions.csv: maps a CANOE region code to its CEF region name."""

    model_config = ConfigDict(extra="forbid")

    region: str
    cef_region: str
    description: str | None = None
    include: bool = False


class CEFCommodityMapping(BaseModel):
    """One row of commodities.csv: maps a CANOE commodity code to a CEF fuel/variable name."""

    model_config = ConfigDict(extra="forbid")

    comm: str
    cef_fuel: str
    description: str | None = None
    include: bool = False
    flag: CommodityTypeCode


class CEFSectorMapping(BaseModel):
    """One row of sectors.csv: maps a CANOE sector tag/code to a CEF sector name."""

    model_config = ConfigDict(extra="forbid")

    tag: str
    code: str
    cef_sector: str
    sector: str
    tech_desc: str
    include: bool = False


class CANOECEFConfig(BaseModel):
    """Structured configuration for one canoe-cef aggregation run."""

    model_config = ConfigDict(extra="forbid")

    input_files_dir: str = INPUT_FILES_DIR
    database_file: Path
    schema_version: str = "4.0"

    model_periods: list[int]
    scenario: str
    data_variant: str
    data_version: str
    cef_reference: str

    energy_units: str = "PJ"
    conversion_factor: float = 1.0
    decimal_places: int = 2
    prop_thresh: float = 0.02

    validation_behavior: Literal["error", "warning"] = "error"
    force_wipe_database: bool = False
    build_test_model: bool = False
    use_dsd: bool = False

    # CEF commodity codes that should be folded into a different CANOE
    # commodity than their own mapping row would suggest (CEF reports a
    # separate diesel demand for these sectors; CANOE treats it as "oil").
    commodity_remap: dict[str, str] = {"C_dsl": "C_oil", "R_dsl": "R_oil"}

    regions: list[CEFRegionMapping]
    commodities: list[CEFCommodityMapping]
    sectors: list[CEFSectorMapping]

    _data_ids: set[str] = PrivateAttr(default_factory=set)

    @property
    def model_regions(self) -> list[str]:
        return sorted(r.region for r in self.regions if r.include)

    @property
    def region_map(self) -> dict[str, str]:
        """cef_region name -> CANOE region code, included regions only."""
        return {r.cef_region: r.region for r in self.regions if r.include}

    @property
    def commodity_map(self) -> dict[str, str]:
        """cef_fuel name -> CANOE commodity code, included commodities only."""
        return {c.cef_fuel: c.comm for c in self.commodities if c.include}

    @property
    def commodity_by_code(self) -> dict[str, CEFCommodityMapping]:
        """CANOE commodity code -> representative mapping row (first match wins,
        matching the historical behaviour where a code like "bio" appears
        under more than one cef_fuel name)."""
        out: dict[str, CEFCommodityMapping] = {}
        for c in self.commodities:
            out.setdefault(c.comm, c)
        return out

    @property
    def sector_map(self) -> dict[str, str]:
        """cef_sector name -> sector tag, included sectors only."""
        return {s.cef_sector: s.tag for s in self.sectors if s.include}

    @property
    def technology_map(self) -> dict[str, str]:
        """cef_sector name -> sector code, included sectors only."""
        return {s.cef_sector: s.code for s in self.sectors if s.include}

    @property
    def sector_by_tag(self) -> dict[str, CEFSectorMapping]:
        return {s.tag: s for s in self.sectors}

    def data_id(self, sector: str = "", region: str = "") -> str:
        """Format and register a dataset ID for later DataSet/DataSource rows."""
        data_id = f"{sector}{self.data_variant}{region}{self.data_version}"
        self._data_ids.add(data_id)
        return data_id

    @property
    def data_ids(self) -> set[str]:
        return set(self._data_ids)

    @classmethod
    def load(cls, input_files_dir: str = INPUT_FILES_DIR) -> "CANOECEFConfig":
        with open(os.path.join(input_files_dir, "params.yaml")) as stream:
            params = dict(yaml.safe_load(stream))

        model_periods = sorted(params["model_periods"])

        regions_df = pd.read_csv(os.path.join(input_files_dir, "regions.csv"), index_col=0)
        regions = [
            CEFRegionMapping(region=region, **row.to_dict())
            for region, row in regions_df.iterrows()
        ]

        commodities_df = pd.read_csv(os.path.join(input_files_dir, "commodities.csv"), index_col=0)
        commodities = [
            CEFCommodityMapping(comm=comm, **row.to_dict())
            for comm, row in commodities_df.iterrows()
        ]

        sectors_df = pd.read_csv(os.path.join(input_files_dir, "sectors.csv"), index_col=0)
        sectors = [
            CEFSectorMapping(tag=tag, **row.to_dict())
            for tag, row in sectors_df.iterrows()
        ]

        return cls(
            input_files_dir=input_files_dir,
            database_file=params["sqlite_database"],
            schema_version=params.get("schema_version", "4.0"),
            model_periods=model_periods,
            scenario=params["scenario"],
            data_variant=params["data_variant"],
            data_version=params["data_version"],
            cef_reference=params["cef_reference"],
            energy_units=params["energy_units"],
            conversion_factor=params["conversion_factor"],
            decimal_places=params["decimal_places"],
            prop_thresh=params["prop_thresh"],
            force_wipe_database=params["force_wipe_database"],
            build_test_model=params["build_test_model"],
            use_dsd=params["use_dsd"],
            regions=regions,
            commodities=commodities,
            sectors=sectors,
        )

    @field_validator("database_file")
    @classmethod
    def expand_path(cls, v: Path) -> Path:
        return v.expanduser()


class Reference(BaseModel):
    """A single citation, and its assigned source_id in the DataSource table."""

    model_config = ConfigDict(extra="forbid")

    id: str
    citation: str


class Bibliography:
    """Tracks citations and assigns them unique, namespaced source IDs."""

    def __init__(self, data_variant: str):
        self._data_variant = data_variant
        self._references: dict[str, Reference] = {}

    def __iter__(self):
        return iter(self._references.values())

    def add(self, name: str, citation: str) -> Reference:
        """Add a reference to the log (if not already present) and return it."""
        if name in self._references:
            return self._references[name]
        num = len(self._references) + 1
        source_id = f"{self._data_variant}{num:02d}"  # source 01 to 99
        ref = Reference(id=source_id, citation=citation)
        self._references[name] = ref
        return ref

    def get(self, name: str) -> Reference:
        return self._references[name]
