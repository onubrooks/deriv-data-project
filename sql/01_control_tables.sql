-- 01_control_tables.sql
-- Control tables for pipeline orchestration, idempotency, and auditability.
-- Human-readable justification: Tracks file ingestion status and CDC LSN state to guarantee idempotency and dead-letter quarantine.

-- Ingestion file manifest ensures each file is processed exactly once per unique content hash
CREATE TABLE IF NOT EXISTS ingestion_file_manifest (
    file_hash VARCHAR PRIMARY KEY,
    file_name VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    status VARCHAR NOT NULL, -- 'SUCCESS', 'FAILED', 'SKIPPED'
    row_count_landed BIGINT NOT NULL DEFAULT 0,
    row_count_staged BIGINT NOT NULL DEFAULT 0,
    row_count_quarantined BIGINT NOT NULL DEFAULT 0,
    arrived_at TIMESTAMP NOT NULL,
    processed_at TIMESTAMP NOT NULL,
    batch_id VARCHAR NOT NULL
);

-- CDC processing ledger ensures CDC events are applied in strict ascending LSN order and never re-applied
CREATE TABLE IF NOT EXISTS cdc_processing_ledger (
    lsn BIGINT PRIMARY KEY,
    payload_hash VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    client_id VARCHAR NOT NULL,
    op VARCHAR NOT NULL,
    outcome VARCHAR NOT NULL, -- 'APPLIED', 'RECONCILED', 'QUARANTINED', 'SKIPPED'
    applied_at TIMESTAMP NOT NULL
);

-- Centralized quarantine table for non-conforming rows, schema violations, and referential integrity orphans
CREATE TABLE IF NOT EXISTS quarantine (
    quarantine_id VARCHAR PRIMARY KEY,
    source_system VARCHAR NOT NULL,
    source_identifier VARCHAR NOT NULL,
    record_id VARCHAR,
    batch_id VARCHAR NOT NULL,
    raw_payload VARCHAR NOT NULL,
    reason_code VARCHAR NOT NULL, -- 'NEGATIVE_AMOUNT', 'ORPHAN_CLIENT', 'DUPLICATE_CONFLICT', 'SCHEMA_DRIFT_UNAPPROVED'
    severity VARCHAR NOT NULL,    -- 'CRITICAL', 'ERROR', 'WARNING'
    quarantined_at TIMESTAMP NOT NULL,
    is_resolved BOOLEAN NOT NULL DEFAULT FALSE,
    retry_eligible BOOLEAN NOT NULL DEFAULT TRUE
);
