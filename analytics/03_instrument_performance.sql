-- Business question: Which instruments drive trading activity and profit?
SELECT
    instrument,
    COUNT(*) AS trades,
    SUM(volume_lots) AS total_volume_lots,
    SUM(pnl_usd) AS total_pnl_usd,
    ROUND(AVG(pnl_usd), 2) AS average_pnl_usd,
    ROUND(100.0 * AVG(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END), 1) AS profitable_trade_pct
FROM fact_trade
GROUP BY instrument
ORDER BY total_pnl_usd DESC;
