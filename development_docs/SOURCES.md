# External sources

For human reference only — not a machine-readable manifest.

| Source | What it provides | Accessed by | Cache file |
|---|---|---|---|
| Canada's Energy Future (CEF), Canada Energy Regulator | Annual end-use energy demand by region/sector/fuel/scenario | `data_scraper.load_cef_end_use_demand` | `input_files/end-use-demand-2023.csv` (manually downloaded, no live fetch) |
| Electricity Demand Specific Distribution shares (internal derivation, not CEF) | Seasonal/time-of-day shares of annual electricity demand, by region and sector | `data_scraper.load_dsd_electricity` | `input_files/dsd_electricity.csv` (bundled, no live fetch) |

Both sources are manually downloaded/prepared files checked into `input_files/`,
not live API calls — there is no caching layer because there is no network
fetch to cache. See README.md's "Annual Updates" section for how to refresh
the CEF CSV.
