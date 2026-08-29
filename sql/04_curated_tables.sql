-- 04_curated_tables.sql
-- Curated dimensional model (Kimball star schema) supporting SCD2 historization, balance ledger, and analytics.
-- Human-readable justification: Implements Kimball dimensional model with SCD2 for risk/status, append-only balance history, and non-collapsing deposit keys.

CREATE TABLE IF NOT EXISTS dim_client (
    client_sk BIGINT PRIMARY KEY,
    client_id VARCHAR NOT NULL,
    full_name VARCHAR,
    date_of_birth DATE,
    email VARCHAR,
    country VARCHAR,
    nationality VARCHAR,
    account_type VARCHAR,
    kyc_status VARCHAR,
    referral_source VARCHAR,
    signup_platform VARCHAR,
    promo_code VARCHAR,
    assigned_manager VARCHAR,
    preferred_language VARCHAR,
    risk_category VARCHAR,
    account_status VARCHAR,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN NOT NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    deleted_at TIMESTAMP,
    is_inferred BOOLEAN NOT NULL DEFAULT FALSE,
    batch_id VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key DATE PRIMARY KEY,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_instrument (
    instrument_sk BIGINT PRIMARY KEY,
    instrument_name VARCHAR NOT NULL UNIQUE,
    asset_class VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_deposit (
    source_system VARCHAR NOT NULL, -- 'warehouse' or 'vendor'
    deposit_id VARCHAR NOT NULL,
    client_sk BIGINT,
    client_id VARCHAR NOT NULL,
    deposit_date DATE NOT NULL,
    amount_usd DECIMAL(18,2) NOT NULL,
    payment_method VARCHAR,
    currency_original VARCHAR NOT NULL,
    exchange_rate DECIMAL(18,4) NOT NULL,
    status VARCHAR NOT NULL,
    processing_days INTEGER NOT NULL,
    fee_usd DECIMAL(18,2) NOT NULL,
    reconciliation_status VARCHAR NOT NULL, -- 'CANONICAL_WAREHOUSE', 'CANONICAL_VENDOR'
    batch_id VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (source_system, deposit_id)
);

CREATE TABLE IF NOT EXISTS fact_trade (
    trade_id VARCHAR PRIMARY KEY,
    client_sk BIGINT,
    client_id VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    instrument_sk BIGINT,
    instrument VARCHAR NOT NULL,
    direction VARCHAR NOT NULL,
    volume_lots DECIMAL(18,4) NOT NULL,
    open_price DECIMAL(18,4) NOT NULL,
    close_price DECIMAL(18,4) NOT NULL,
    pnl_usd DECIMAL(18,2) NOT NULL,
    trade_status VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_client_balance_history (
    lsn BIGINT PRIMARY KEY,
    client_id VARCHAR NOT NULL,
    observed_at TIMESTAMP NOT NULL,
    balance_usd DECIMAL(18,2) NOT NULL,
    batch_id VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL
);

-- Masked analytics view for PII compliance (General Analytics / BI Role)
CREATE OR REPLACE VIEW v_client_analytics AS
SELECT
    client_sk,
    client_id,
    regexp_replace(full_name, '(^.).*(.)$', '\1***\2') AS full_name_masked,
    date_trunc('year', date_of_birth) AS birth_year,
    regexp_replace(email, '(^.).*(@.*)$', '\1***\2') AS email_masked,
    country,
    nationality,
    account_type,
    kyc_status,
    referral_source,
    signup_platform,
    preferred_language,
    risk_category,
    account_status,
    valid_from,
    valid_to,
    is_current,
    is_deleted
FROM dim_client;
