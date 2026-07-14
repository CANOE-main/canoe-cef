# Decisions

Per-module judgment calls made during the v4.0 refactor (see
`CANOE_module_refactor_design.md` §6.3, §13).

### `sector_label`

- Question: should canoe-cef keep writing `sector_label` rows (previous
  behaviour), or should ownership move elsewhere?
- Decision: canoe-cef no longer writes `sector_label`. It's a small,
  finite, cross-sector registry (one row per `canoe-*` module) and doesn't
  belong to any single sector module.
- Owner/rationale: confirmed by the repo owner (2026-07-10).
- Follow-ups: **canoe-base needs to seed `sector_label`** with at least
  `commercial`, `industrial`, `residential`, `transportation` (the sectors
  canoe-cef's config currently defines in `input_files/sectors.csv`) before
  canoe-cef's `technology.sector` foreign key references resolve cleanly
  against a fully-populated database.

### Upsert vs. insert-or-ignore semantics

- Question: the design doc's default write mode is idempotent
  `INSERT OR IGNORE`, but canoe-cef previously used `REPLACE INTO`, which
  lets a rerun (e.g. a new scenario/variant) update previously-written
  values rather than silently skip them.
- Decision: use `to_upsert_sql`/`ON CONFLICT ... DO UPDATE` (via the new
  `sql_helpers.upsert_many` batch helper) everywhere, preserving update-on-
  rerun behaviour without hand-written SQL strings.
- Owner/rationale: confirmed by the repo owner (2026-07-10).

### Data quality fields (`dq_cred`/`dq_geog`/`dq_struc`/`dq_tech`/`dq_time`)

- Question: should these newly-available v4.0 fields be populated with a
  `DataQualityProfile`-style default, following the pattern the design doc
  suggests for `canoe-agriculture`?
- Decision: left unset (`None`) for now, matching the pre-refactor state
  where these fields didn't exist/weren't populated at all.
- Owner/rationale: confirmed by the repo owner (2026-07-10) - no scores
  have been assigned yet, so leaving them empty is more honest than
  guessing.
- Follow-ups: revisit once someone with domain knowledge of the CEF data's
  credibility/geography/structure/technology/time resolution can assign
  scores.

### Standalone test database (`build_tester()` / `db.build_test_database`)

- Question: this module's `build_tester()` previously wrote directly to
  global (B) tables (`region`, `time_period`, and the now-removed
  `time_season`/`time_of_day`/`SeasonLabel`/`TimeSegmentFraction` tables),
  which the refactor's Workstream 3 flags as an anti-pattern for the shared
  production database.
- Decision: kept as an explicit, separate testing workflow
  (`db.build_test_database`, invoked via `python . --build-test-db`), never
  called from the normal `all_sectors.build()` pipeline. It builds its own
  standalone SQLite file from `canoe_schema.sql.get_sql_schema()` directly,
  bypassing canoe-base entirely - this is fine precisely because it's not
  the shared database.
- Owner/rationale: confirmed by the repo owner (2026-07-10).
