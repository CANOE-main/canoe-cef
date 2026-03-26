# CANOE-CEF

**Author:** Ian David Elder  
**Project:** CANOE Model  

This tool takes annual energy demand projections from the [Canada's Energy Future (CEF)](https://www.cer-rec.gc.ca/en/data-analysis/canada-energy-future/) model and converts them into Temoa-compatible annual demands for the CANOE model.

For documentation: [Visit here](https://canoe-main.github.io/canoe-cef/)

## Features

- **Data Ingestion:** Reads CEF demand data from CSV files.
- **Data Transformation:** Maps CEF regions, sectors, and commodities to CANOE model definitions.
- **Database Output:** Populates a Temoa-compatible SQLite database with the processed demand data.
- **Electricity Distributions:** Option to apply Demand Specific Distributions (DSD) for electricity.

## Prerequisites

- Python 3.x
- `pandas`
- `PyYAML`

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

Configuration is managed via files in the `input_files/` directory, primarily `params.yaml`.

- `params.yaml`: Main configuration settings (scenario selection, database paths, etc.).
- `regions.csv`: Mapping of CEF regions to model regions.
- `commodities.csv`: Mapping of CEF variables/fuels to model commodities.
- `sectors.csv`: Mapping of CEF sectors to model sectors.
- `end-use-demand-2023.csv`: The source data from Canada's Energy Future.

## Usage

To run the conversion process and populate the database:

```bash
python .
```

Or run the module directly:

```bash
python __main__.py
```

This will:
1. Initialize the SQLite database (or clear it if configured to do so).
2. Read the CEF input data.
3. Filter and aggregate data based on the configuration.
4. Write the `Technology`, `Commodity`, `Demand`, and `Efficiency` tables to the SQLite database.

## Data Sources

- **Canada's Energy Future (CEF):** [https://www.cer-rec.gc.ca/en/data-analysis/canada-energy-future/](https://www.cer-rec.gc.ca/en/data-analysis/canada-energy-future/)

## Annual Updates

When updating for a new year:
1. Download the new end-use demand data from the CER website.
2. Replace the `end-use-demand-XXXX.csv` in `input_files/`.
3. Update `params.yaml` and mapping CSVs if scenario names or dimensions have changed.
