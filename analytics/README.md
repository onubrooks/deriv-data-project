# Analytics examples

These queries show why the curated model is useful. Each file starts with a business question
and queries only curated warehouse tables or control tables.

Build the local warehouse first:

```bash
uv run python code/run_pipeline.py
```

Run one query:

```bash
duckdb warehouse.duckdb < analytics/01_client_value.sql
```

The examples cover:

1. Client value: latest balance change captured by CDC, completed deposits, fees, and trading P&L.
2. Country performance: client reach and financial activity by country.
3. Instrument performance: volume, profitability, and win rate.
4. Deposit operations: source totals plus quarantined data-quality issues.
5. Point-in-time state: client risk/status and the latest CDC balance change known at a
   chosen timestamp.

The fixture data is small, so these results illustrate business use rather than statistical
significance. Production dashboards should also apply approved access controls because the
underlying facts contain confidential financial information.

## Verified example insights

- Singapore has the highest completed deposit value in these fixtures: `$80,620`.
- UAE clients have the highest combined trading P&L: `$2,890`.
- BTC/USD produces the highest instrument P&L: `$2,600` across three trades.
- The warehouse contains `$123,000` of warehouse-feed deposits and `$28,525` of accepted
  vendor-feed deposits.
- Two vendor rows require attention: one negative amount and one unknown client.
