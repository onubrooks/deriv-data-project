-- 03_staging_tables.sql
-- Typed staging tables performing normalization, validation, and row-hash computation.
-- Human-readable justification: Casts string raw fields to validated types and normalizes schema variations before warehouse merging.

CREATE TABLE IF NOT EXISTS stg_client_signup (
    client_id VARCHAR PRIMARY KEY,
    signup_date DATE NOT NULL,
    country VARCHAR NOT NULL,
    email VARCHAR NOT NULL,
    kyc_status VARCHAR NOT NULL,
    account_type VARCHAR NOT NULL,
    referral_source VARCHAR NOT NULL,
    signup_platform VARCHAR NOT NULL,
    promo_code VARCHAR,
    assigned_manager VARCHAR,
    row_hash VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    staged_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS stg_client_profile (
    client_id VARCHAR PRIMARY KEY,
    full_name VARCHAR NOT NULL,
    date_of_birth DATE NOT NULL,
    nationality VARCHAR NOT NULL,
    risk_category VARCHAR NOT NULL,
    account_balance_usd DECIMAL(18,2) NOT NULL,
    account_status VARCHAR NOT NULL,
    currency VARCHAR NOT NULL,
    last_login_date DATE,
    preferred_language VARCHAR NOT NULL,
    row_hash VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    staged_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS stg_client_deposit (
    deposit_id VARCHAR PRIMARY KEY,
    client_id VARCHAR NOT NULL,
    deposit_date DATE NOT NULL,
    amount_usd DECIMAL(18,2) NOT NULL,
    payment_method VARCHAR,
    currency_original VARCHAR NOT NULL,
    exchange_rate DECIMAL(18,4) NOT NULL,
    status VARCHAR NOT NULL,
    processing_days INTEGER NOT NULL,
    fee_usd DECIMAL(18,2) NOT NULL,
    row_hash VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    staged_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS stg_client_trades (
    trade_id VARCHAR PRIMARY KEY,
    client_id VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    instrument VARCHAR NOT NULL,
    direction VARCHAR NOT NULL,
    volume_lots DECIMAL(18,4) NOT NULL,
    open_price DECIMAL(18,4) NOT NULL,
    close_price DECIMAL(18,4) NOT NULL,
    pnl_usd DECIMAL(18,2) NOT NULL,
    trade_status VARCHAR NOT NULL,
    row_hash VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    staged_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS stg_vendor_deposit (
    deposit_id VARCHAR NOT NULL,
    client_id VARCHAR NOT NULL,
    deposit_date DATE NOT NULL,
    amount_usd DECIMAL(18,2) NOT NULL,
    payment_method VARCHAR NOT NULL,
    currency_original VARCHAR NOT NULL,
    exchange_rate DECIMAL(18,4) NOT NULL,
    status VARCHAR NOT NULL,
    processing_days INTEGER NOT NULL,
    fee_usd DECIMAL(18,2) NOT NULL,
    canonical_row_hash VARCHAR NOT NULL,
    file_name VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    staged_at TIMESTAMP NOT NULL,
    PRIMARY KEY (deposit_id, canonical_row_hash)
);

CREATE TABLE IF NOT EXISTS stg_cdc_events (
    lsn BIGINT PRIMARY KEY,
    commit_ts TIMESTAMP NOT NULL,
    op VARCHAR NOT NULL,
    client_id VARCHAR NOT NULL,
    before_json VARCHAR,
    after_json VARCHAR,
    payload_hash VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    staged_at TIMESTAMP NOT NULL
);
