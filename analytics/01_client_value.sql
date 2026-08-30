-- Business question: Which clients contribute the most financial value?
WITH latest_client AS (
    SELECT * EXCLUDE (version_rank)
    FROM (
        SELECT
            client_id,
            country,
            account_type,
            account_status,
            is_deleted,
            ROW_NUMBER() OVER (
                PARTITION BY client_id ORDER BY valid_from DESC, client_sk DESC
            ) AS version_rank
        FROM dim_client
    )
    WHERE version_rank = 1
),
deposit_value AS (
    SELECT
        client_id,
        SUM(amount_usd) FILTER (WHERE status = 'completed') AS completed_deposits_usd,
        SUM(fee_usd) AS deposit_fees_usd
    FROM fact_deposit
    GROUP BY client_id
),
trade_value AS (
    SELECT client_id, SUM(pnl_usd) AS trading_pnl_usd
    FROM fact_trade
    GROUP BY client_id
),
latest_balance AS (
    SELECT client_id, balance_usd
    FROM fact_client_balance_history
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY client_id ORDER BY observed_at DESC, lsn DESC
    ) = 1
)
SELECT
    c.client_id,
    c.country,
    c.account_type,
    c.account_status,
    c.is_deleted,
    b.balance_usd AS latest_cdc_balance_usd,
    COALESCE(d.completed_deposits_usd, 0) AS completed_deposits_usd,
    COALESCE(d.deposit_fees_usd, 0) AS deposit_fees_usd,
    COALESCE(t.trading_pnl_usd, 0) AS trading_pnl_usd
FROM latest_client c
LEFT JOIN deposit_value d USING (client_id)
LEFT JOIN trade_value t USING (client_id)
LEFT JOIN latest_balance b USING (client_id)
WHERE COALESCE(d.completed_deposits_usd, 0) > 0
   OR COALESCE(t.trading_pnl_usd, 0) <> 0
ORDER BY completed_deposits_usd DESC, trading_pnl_usd DESC
LIMIT 15;
