# Session Handoff

## Current state

Implementation and independent Codex review of the bounded DuckDB/Python prototype are
complete. All eight embedded inputs were deterministically extracted, the default pipeline
runs idempotently, and 15 focused tests pass. The evaluator-facing documents now lead with
plain-language explanations, and `analytics/` contains five verified business queries.

## Decisions made & implemented

- **Fixture Extraction:** Deterministically parsed and extracted all 8 files from `task_instructions.md` into `data/` using `code/extract_fixtures.py`.
- **Environment:** Managed via `uv` with minimal locked dependencies (`duckdb` and `pytest`).
- **Database Schema:** Modular SQL DDL scripts placed in `sql/` (`01_control_tables.sql`, `02_raw_tables.sql`, `03_staging_tables.sql`, `04_curated_tables.sql`).
- **Control Tables:** `ingestion_file_manifest` guarantees file-level idempotency by SHA-256 hash; `cdc_processing_ledger` tracks per-LSN processing state; `quarantine` captures non-conforming rows with reason codes and severity.
- **Vendor Reconciliation:** Handled approved alias `method -> payment_method` in 2024-03-02 delivery; deduplicated identical repeated deposits (`VDEP002`, `VDEP005`); quarantined domain violation `VDEP001` (`NEGATIVE_AMOUNT` [CRITICAL]) and referential orphan `VDEP020` (`ORPHAN_CLIENT` [ERROR]); preserved composite key `(source_system, deposit_id)`.
- **CDC Historization:** Enforced strict ascending LSN order (ignoring arrival order). Applied SCD Type 2 on `risk_category` and `account_status` in `dim_client`, appended balance observations to `fact_client_balance_history`, and soft-deleted `CL012` with `is_deleted = TRUE`, `deleted_at`, and `valid_to`.
- **Historical Replay:** Implemented `code/replay.py` (`--replay`) enabling deterministic reprocessing of CDC date ranges with zero SCD interval overlaps.
- **Independent fixes:** Added atomic publication/rollback, raw file-row uniqueness, complete
  input-set and schema validation, UTC session handling, CDC arrival-sequence provenance,
  conflicting-LSN detection, actual-change balance grain, and replay that includes a full
  date-only end day while rebuilding later affected history.
- **Verification:** Ran the default CLI twice, ran one-day replay, queried warehouse
  invariants directly, and verified 15/15 passing tests in `tests/test_pipeline.py`.
- **Analytics:** Added client value, country, instrument, deposit-operations, and point-in-time
  queries. Verified all five against a fresh warehouse and documented representative results.

## Next action

Review the analytics results and simplified documents as a submission candidate, then
practice the technical walkthrough.

## Resume references

- Assessment brief: `task_instructions.md`
- AI provenance: `PROMPTS.md`
- Pipeline entry point: `code/run_pipeline.py`
- Test suite: `tests/test_pipeline.py`
- Analytics guide: `analytics/README.md`
- Detailed context: `.omx/context/deriv-assessment-definition-of-done-20260829T114300Z.md`
