-- Business question: What deposit value arrived from each source, and what needs attention?
SELECT
    'curated_deposits' AS record_group,
    source_system AS category,
    COUNT(*) AS records,
    SUM(amount_usd) AS amount_usd,
    SUM(fee_usd) AS fee_usd
FROM fact_deposit
GROUP BY source_system

UNION ALL

SELECT
    'quarantine' AS record_group,
    reason_code AS category,
    COUNT(*) AS records,
    NULL AS amount_usd,
    NULL AS fee_usd
FROM quarantine
GROUP BY reason_code
ORDER BY record_group, category;
