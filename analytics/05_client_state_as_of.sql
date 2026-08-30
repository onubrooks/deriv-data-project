-- Business question: What client state did the warehouse know at 2024-11-20 12:00 UTC?
WITH parameters AS (
    SELECT TIMESTAMP '2024-11-20 12:00:00' AS as_of_ts
),
client_state AS (
    SELECT
        d.client_id,
        d.risk_category,
        d.account_status,
        d.is_deleted,
        p.as_of_ts
    FROM dim_client d
    CROSS JOIN parameters p
    WHERE d.valid_from <= p.as_of_ts
      AND (d.valid_to > p.as_of_ts OR d.valid_to IS NULL)
),
balance_state AS (
    SELECT
        b.client_id,
        b.balance_usd,
        b.observed_at
    FROM fact_client_balance_history b
    CROSS JOIN parameters p
    WHERE b.observed_at <= p.as_of_ts
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY b.client_id ORDER BY b.observed_at DESC, b.lsn DESC
    ) = 1
)
SELECT
    c.client_id,
    c.risk_category,
    c.account_status,
    c.is_deleted,
    b.balance_usd AS latest_cdc_balance_usd,
    b.observed_at AS balance_observed_at,
    c.as_of_ts
FROM client_state c
LEFT JOIN balance_state b USING (client_id)
WHERE b.client_id IS NOT NULL
ORDER BY c.client_id;
