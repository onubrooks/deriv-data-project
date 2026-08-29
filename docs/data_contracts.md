# Prototype Data Contracts

This file converts the architecture decisions into implementation boundaries. The assessment
documents remain self-contained; this is the executable team's concise contract.

## Canonical deposit

- Key: `(source_system, deposit_id)` where source is `warehouse` or `vendor`.
- Required: `deposit_id`, `client_id`, `deposit_date`, `amount_usd`, `status`.
- `payment_method` accepts the source alias `method` only.
- Financial values use `DECIMAL`, never binary floating point for equality decisions.
- Negative amount: quarantine with `NEGATIVE_AMOUNT`.
- Missing client: quarantine with `ORPHAN_CLIENT`; eligible for retry.
- Repeated key plus same row hash: `duplicate_identical`; repeated key plus different hash:
  `duplicate_conflict` and no curated overwrite.

## CDC

- Ordering/idempotency key: integer `lsn`.
- Required by operation: insert → `after`; update → `before` and `after`; delete → `before`.
- Store payload unchanged before processing.
- Risk/status produce SCD2 versions; balance produces one event per changed LSN.
- Delete end-dates and flags the current client version; it never removes history.

## Required controls

- `ingestion_file_manifest(file_hash UNIQUE, file_name, source, status, row counts, times)`.
- `cdc_processing_ledger(lsn UNIQUE, payload_hash, batch_id, outcome, applied_at)`.
- Quarantine records include source identity, batch, row payload, code, severity, and time.
- Curated publication and successful control-state update share one transaction.
