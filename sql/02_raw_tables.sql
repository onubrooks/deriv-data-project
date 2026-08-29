-- 02_raw_tables.sql
-- Immutable landing zone tables preserving exact source payload and lineage metadata.
-- Human-readable justification: Stores verbatim source data to enable full auditability, replayability, and lineage tracking.

CREATE TABLE IF NOT EXISTS raw_client_signup (
    client_id VARCHAR,
    signup_date VARCHAR,
    country VARCHAR,
    email VARCHAR,
    kyc_status VARCHAR,
    account_type VARCHAR,
    referral_source VARCHAR,
    signup_platform VARCHAR,
    promo_code VARCHAR,
    assigned_manager VARCHAR,
    file_name VARCHAR NOT NULL,
    file_hash VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    landed_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_client_profile (
    client_id VARCHAR,
    full_name VARCHAR,
    date_of_birth VARCHAR,
    nationality VARCHAR,
    risk_category VARCHAR,
    account_balance_usd VARCHAR,
    account_status VARCHAR,
    currency VARCHAR,
    last_login_date VARCHAR,
    preferred_language VARCHAR,
    file_name VARCHAR NOT NULL,
    file_hash VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    landed_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_client_deposit (
    deposit_id VARCHAR,
    client_id VARCHAR,
    deposit_date VARCHAR,
    amount_usd VARCHAR,
    payment_method VARCHAR,
    currency_original VARCHAR,
    exchange_rate VARCHAR,
    status VARCHAR,
    processing_days VARCHAR,
    fee_usd VARCHAR,
    raw_json VARCHAR, -- captures non-standard fields like credit_card
    file_name VARCHAR NOT NULL,
    file_hash VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    landed_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_client_trades (
    trade_id VARCHAR,
    client_id VARCHAR,
    trade_date VARCHAR,
    instrument VARCHAR,
    direction VARCHAR,
    volume_lots VARCHAR,
    open_price VARCHAR,
    close_price VARCHAR,
    pnl_usd VARCHAR,
    trade_status VARCHAR,
    file_name VARCHAR NOT NULL,
    file_hash VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    landed_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_vendor_deposit (
    deposit_id VARCHAR,
    client_id VARCHAR,
    deposit_date VARCHAR,
    amount_usd VARCHAR,
    payment_method VARCHAR,
    currency_original VARCHAR,
    exchange_rate VARCHAR,
    status VARCHAR,
    processing_days VARCHAR,
    fee_usd VARCHAR,
    raw_payload VARCHAR,
    file_name VARCHAR NOT NULL,
    file_hash VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    row_num BIGINT NOT NULL,
    landed_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_cdc_events (
    lsn BIGINT NOT NULL,
    commit_ts VARCHAR NOT NULL,
    op VARCHAR NOT NULL,
    client_id VARCHAR NOT NULL,
    before_json VARCHAR,
    after_json VARCHAR,
    payload_hash VARCHAR NOT NULL,
    file_name VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    arrival_sequence BIGINT,
    landed_at TIMESTAMP NOT NULL,
    PRIMARY KEY (lsn, payload_hash)
);

-- Lightweight forward migration keeps locally generated databases runnable after schema additions.
ALTER TABLE raw_vendor_deposit ADD COLUMN IF NOT EXISTS raw_payload VARCHAR;
ALTER TABLE raw_cdc_events ADD COLUMN IF NOT EXISTS arrival_sequence BIGINT;

-- File-row uniqueness makes retries safe even if control state must be reconstructed.
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_signup_file_row
    ON raw_client_signup(file_hash, client_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_profile_file_row
    ON raw_client_profile(file_hash, client_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_deposit_file_row
    ON raw_client_deposit(file_hash, deposit_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_trade_file_row
    ON raw_client_trades(file_hash, trade_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_vendor_file_row
    ON raw_vendor_deposit(file_hash, row_num);
