# Deriv Data Engineering Assessment

## What was built

This repository turns the supplied signup, profile, deposit, trade, vendor, and CDC files
into analysis-ready DuckDB tables. It keeps the original records for audit, rejects or
quarantines bad data, safely handles reruns, preserves client history, and never hard-deletes
warehouse history.

Start here:

- `part1_pipeline.md` explains how data moves through the pipeline.
- `part2_data_model.md` explains the warehouse tables and history model.
- `part3_pii.md` explains how personal data is protected.
- `analytics/` contains business-facing example queries.

Run it locally:

```bash
uv sync
uv run python code/run_pipeline.py
uv run pytest -q
duckdb warehouse.duckdb < analytics/01_client_value.sql
```

Verified output: 40 accepted deposits, 20 trades, five real CDC balance changes, two
quarantined vendor rows, no overlapping client-history intervals, and 15 passing tests.

## What was deliberately cut, and why

Cloud infrastructure and orchestration were not built. DuckDB keeps the assessment
self-contained and easy to run, while the design explains how the same controls map to a
production warehouse. Spark, pandas, and similar frameworks were unnecessary for this data
size. Automated quarantine reprocessing was also deferred; retryable rows remain clearly
marked for a later batch.

## What would be done differently with more time

Add automatic quarantine retries, production alerting and lineage, and managed tokenization
for PII. At production scale, move the SQL models to the chosen cloud warehouse, partition
large facts by date, and benchmark concurrent ingestion and replay. The balance model would
also seed an opening balance observation so point-in-time analytics covers clients with no
CDC balance change.
