-- Business question: Which countries have the strongest client and financial activity?
WITH latest_client AS (
    SELECT client_id, country
    FROM dim_client
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY client_id ORDER BY valid_from DESC, client_sk DESC
    ) = 1
),
deposits AS (
    SELECT client_id, SUM(amount_usd) AS deposit_usd
    FROM fact_deposit
    WHERE status = 'completed'
    GROUP BY client_id
),
trades AS (
    SELECT client_id, COUNT(*) AS trade_count, SUM(pnl_usd) AS pnl_usd
    FROM fact_trade
    GROUP BY client_id
)
SELECT
    c.country,
    COUNT(DISTINCT c.client_id) AS clients,
    SUM(COALESCE(d.deposit_usd, 0)) AS completed_deposits_usd,
    SUM(COALESCE(t.trade_count, 0)) AS trades,
    SUM(COALESCE(t.pnl_usd, 0)) AS trading_pnl_usd
FROM latest_client c
LEFT JOIN deposits d USING (client_id)
LEFT JOIN trades t USING (client_id)
GROUP BY c.country
ORDER BY completed_deposits_usd DESC, trading_pnl_usd DESC;
