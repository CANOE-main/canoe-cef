# CANOE-CEF

**Author:** Ian David Elder  
**Project:** CANOE Model  

This tool takes annual energy demand projections from the [Canada's Energy Future (CEF)](https://www.cer-rec.gc.ca/en/data-analysis/canada-energy-future/) model and converts them into Temoa-compatible annual demands for the CANOE model.

For documentation: [Visit here](https://canoe-main.github.io/canoe-cef/)

## Features

- **Data Ingestion:** Reads CEF demand data from CSV files (`data_scraper.py`).
- **Data Transformation:** Maps CEF regions, sectors, and commodities to CANOE model definitions.
- **Validation:** Checks periods/regions/time-slices against the shared database before writing anything (`validation.py`).
- **Database Output:** Writes `Technology`, `Commodity`, `Demand`, `Efficiency`, `LimitTechInputSplitAnnual`, and (optionally) `DemandSpecificDistribution` rows into the database canoe-base already built, via `canoe_schema.v4_0.models` + upserts. canoe-cef never creates or drops that database itself.
- **Electricity Distributions:** Option to apply Demand Specific Distributions (DSD) for electricity.

## Prerequisites

- Python 3.x
- `pandas`
- `PyYAML`
- `pydantic`
- `canoe_schema` (pinned to the `yep/v4` branch - see `requirements.txt`)

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd canoe-cef
   ```

2. Install the required Python packages:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Configuration is managed via files in the `input_files/` directory, loaded into a
validated `CANOECEFConfig` (see `config.py`).

- `params.yaml`: Main configuration settings (scenario selection, database path, schema version, etc.).
- `regions.csv`: Mapping of CEF regions to model regions.
- `commodities.csv`: Mapping of CEF variables/fuels to model commodities.
- `sectors.csv`: Mapping of CEF sectors to model sectors.
- `end-use-demand-2023.csv`: The source data from Canada's Energy Future.

See `SOURCES.md` for the external-source reference table, and `DECISIONS.md`
for judgment calls made during the v4.0 refactor.

## Usage

canoe-cef expects the target SQLite database to already exist, built by
canoe-base from `canoe_schema`'s v4.0 `schema.sql` (global tables like
`region` and `time_period` must already be populated - canoe-cef validates
this and fails loudly if they're missing).

To run the conversion process and populate the database:

```bash
python .
```

Or run the module directly:

```bash
python __main__.py
```

This will:
1. Validate `params.yaml`'s periods/regions (and time slices, if `use_dsd` is
   set) against the database's existing global tables.
2. Clear canoe-cef's own previously-written rows if `force_wipe_database` is
   set (never the whole database).
3. Read the CEF input data.
4. Filter and aggregate data based on the configuration.
5. Upsert the `Technology`, `Commodity`, `Demand`, and `Efficiency` rows (and
   `DemandSpecificDistribution`, if `use_dsd` is set).

### Standalone test/dev database

To build a small, self-contained SQLite database for a local Temoa test run
(not the shared production database - see `DECISIONS.md`):

```bash
python . --build-test-db
```

## Data Sources

- **Canada's Energy Future (CEF):** [https://www.cer-rec.gc.ca/en/data-analysis/canada-energy-future/](https://www.cer-rec.gc.ca/en/data-analysis/canada-energy-future/)

## Annual Updates

When updating for a new year:
1. Download the new end-use demand data from the CER website.
2. Replace the `end-use-demand-XXXX.csv` in `input_files/`.
3. Update `params.yaml` and mapping CSVs if scenario names or dimensions have changed.
