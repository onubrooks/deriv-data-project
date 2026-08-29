# Context Snapshot — Deriv Assessment Definition of Done

- **Task:** Implement the bounded DuckDB/Python reference pipeline from the repository's architecture and contracts.
- **Desired outcome:** A runnable, fully tested DuckDB/Python reference pipeline proving all edge cases, idempotency, vendor reconciliation, LSN-ordered CDC, SCD Type 2 historization, and bounded historical replay.
- **Current phase:** Implementation and automated test verification complete. Ready for Codex final integration QA.
- **Known facts:** Required deliverables (`README.md`, `part1_pipeline.md`, `part2_data_model.md`, `part3_pii.md`, `PROMPTS.md`, `sql/*.sql`, `code/*.py`, `tests/test_pipeline.py`) are present, self-contained, and verified.
- **Implemented features:**
  - Deterministic fixture extraction into `data/`.
  - Minimal Python environment via `uv` with `duckdb` and `pytest`.
  - CLI `code/run_pipeline.py` and modular DDLs in `sql/`.
  - Control tables: `ingestion_file_manifest`, `cdc_processing_ledger`, `quarantine`.
  - Ingestion boundary quality: negative amount quarantine (`NEGATIVE_AMOUNT` [CRITICAL]), orphan client quarantine (`ORPHAN_CLIENT` [ERROR]).
  - Vendor normalization: `method -> payment_method` alias handling.
  - Deduplication: `duplicate_identical` skipped, `duplicate_conflict` quarantined.
  - CDC stream: strict ascending LSN processing, SCD Type 2 client risk/status, balance history fact, soft deletion.
  - Bounded historical replay: `--replay` date range support with 0 interval overlaps.
  - 8 focused pytest tests passing.
- **Verification results:**
  - Double run executed; 100% idempotent (0 duplicates, all files/LSNs skipped on second run).
  - Bounded replay executed over November 2024; reconstructed 11 events across 8 clients with 0 interval overlaps.
  - Independent Codex review fixed transactional, replay-boundary, balance-grain, UTC,
    provenance, schema/input validation, and raw retry defects.
  - Default CLI is idempotent; single-day replay succeeds; 14 of 14 tests pass.
- **Next steps:** Codex final review, documentation cross-check, and final handoff.
