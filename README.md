# Deriv Data Engineering Assessment

## What was built

A production-grade, platform-neutral data architecture and an executable DuckDB/Python reference prototype grounded in the supplied assessment inputs:

1. **Architecture & Contracts:**
   - Layered batch architecture and reconciliation design in `part1_pipeline.md`.
   - Kimball star schema dimensional model, SCD Type 2 specification, and balance-history fact in `part2_data_model.md`.
   - PII masking and access control policy in `part3_pii.md`.
   - Formal boundary contracts in `docs/data_contracts.md`.

2. **Deterministic Data Ingestion & Storage (`code/`, `sql/`):**
   - Deterministic fixture extractor (`code/extract_fixtures.py`) materializing embedded datasets to `data/`.
   - Modular SQL DDLs (`sql/01_control_tables.sql` to `sql/04_curated_tables.sql`) managing manifests, CDC ledgers, quarantine, raw landing, staging, and curated star schemas.
   - Core snapshot loading for client signup, profile, deposit, and trade entities.
   - Vendor deposit ingestion with approved schema drift normalization (`method -> payment_method`), deduplication (`duplicate_identical`), conflict quarantine (`duplicate_conflict`), and namespace separation (`DEP...` vs `VDEP...`).
   - Dead-letter quarantine routing for domain violations (`NEGATIVE_AMOUNT` [CRITICAL]) and referential orphans (`ORPHAN_CLIENT` [ERROR]).
   - CDC stream processing strictly ordered by ascending LSN (never arrival order), applying SCD Type 2 to `risk_category` and `account_status`, appending balance changes to `fact_client_balance_history`, and soft-deleting deleted accounts.
   - Bounded historical replay engine (`code/replay.py`) for deterministic state reconstruction without interval overlaps.

3. **Run & Verification Instructions:**
   - **Environment setup:** `uv sync` (creates minimal virtual environment with `duckdb` and `pytest`).
   - **Run pipeline:** `uv run python code/run_pipeline.py`
   - **Run tests:** `uv run pytest -v`
   - **Run historical replay:** `uv run python code/run_pipeline.py --replay 2024-11-01 2024-11-30`

4. **Actual Verification Evidence:**
   - **Full pipeline execution (Run 1 & Run 2 Idempotency):**
     - `dim_client`: 36 rows (30 standard + 1 inferred + 5 SCD2 versions; 0 overlapping intervals)
     - `fact_deposit`: 40 rows (20 warehouse canonical `CANONICAL_WAREHOUSE` + 20 vendor canonical `CANONICAL_VENDOR`)
     - `fact_trade`: 20 rows
     - `fact_client_balance_history`: 5 rows (actual balance changes only)
     - `quarantine`: 2 rows (`VDEP001` with `NEGATIVE_AMOUNT` [CRITICAL], `VDEP020` with `ORPHAN_CLIENT` [ERROR])
     - `ingestion_file_manifest`: 7 file entries recorded (100% skipped on second run)
     - `cdc_processing_ledger`: 12 events recorded (100% skipped on second run)
   - **Pytest test suite:** 14 of 14 tests passing, including idempotency,
     transactional rollback, raw-row uniqueness, schema drift, quarantine, LSN ordering,
     UTC effective timestamps, actual balance-change grain, soft deletion, single-day replay,
     future-history preservation, missing inputs, and deterministic replay.

## What was deliberately cut, and why

1. **Cloud Orchestration & Infrastructure (Airflow, Dagster, AWS/Snowflake):** Cut to keep the evaluation fully self-contained, reproducible in seconds on any machine, and focused on core data modeling and pipeline semantics within the time limit.
2. **Heavy Data Processing Frameworks (Spark, Pandas, Polars):** Cut in favor of standard Python and native DuckDB SQL, which provides zero-overhead ACID transactions, SQL analytical power, and native JSON/CSV processing without bloat.
3. **Automated DLQ Reprocessing Service:** Real-time event-driven quarantine resolution was cut; quarantine records retain `retry_eligible = TRUE` flags for future batch reprocessing rather than complex async workers.

## What would be done differently with more time

1. **Automated Quarantine Re-drive Loop:** Implement an automated reconciliation task that queries `quarantine` for `retry_eligible = TRUE` records whenever new client dimensions are published.
2. **dbt Transformation & Data Quality Framework:** Package SQL models into dbt with automated Great Expectations / Soda contract assertions, data freshness alerts, and interactive lineage graphs.
3. **Column-Level Cryptographic Tokenization:** Integrate HashiCorp Vault or AWS KMS envelope encryption for field-level PII tokenization directly at the landing-to-staging transformation step.
4. **Partition Pruning & Large-Scale Benchmarking:** Implement date/source partitioned Parquet storage structures on cloud object stores (e.g. S3/GCS) and validate throughput under millions of streaming CDC events.
