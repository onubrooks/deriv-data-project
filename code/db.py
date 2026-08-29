"""
Database connection and schema initialization utilities for DuckDB.
"""

from pathlib import Path
import duckdb

# Human-readable justification:
# Initializes the DuckDB database connection and applies all modular DDL scripts in dependency order.

def get_connection(db_path: str | Path = "warehouse.duckdb") -> duckdb.DuckDBPyConnection:
    """Create or connect to a DuckDB database instance."""
    if str(db_path) == ":memory:":
        conn = duckdb.connect(":memory:")
    else:
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(path))
    # CDC commit timestamps are UTC (`Z`); a fixed session timezone prevents local-time shifts.
    conn.execute("SET TimeZone = 'UTC'")
    return conn

def init_schema(conn: duckdb.DuckDBPyConnection, sql_dir: Path | None = None) -> None:
    """Execute all SQL initialization scripts in numerical order."""
    if sql_dir is None:
        sql_dir = Path(__file__).resolve().parent.parent / "sql"

    sql_files = sorted(sql_dir.glob("*.sql"))
    for sql_file in sql_files:
        ddl = sql_file.read_text(encoding="utf-8")
        conn.execute(ddl)

    # Populate dim_date calendar dimension if empty
    res = conn.execute("SELECT COUNT(*) FROM dim_date").fetchone()
    if res and res[0] == 0:
        conn.execute("""
            INSERT INTO dim_date (date_key, year, quarter, month, day, day_of_week, is_weekend)
            SELECT
                d::DATE AS date_key,
                EXTRACT(year FROM d)::INTEGER AS year,
                EXTRACT(quarter FROM d)::INTEGER AS quarter,
                EXTRACT(month FROM d)::INTEGER AS month,
                EXTRACT(day FROM d)::INTEGER AS day,
                EXTRACT(isodow FROM d)::INTEGER AS day_of_week,
                CASE WHEN EXTRACT(isodow FROM d) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
            FROM generate_series(DATE '2024-01-01', DATE '2024-12-31', INTERVAL 1 DAY) tbl(d)
        """)

    # Populate dim_instrument trading dimension if empty
    res = conn.execute("SELECT COUNT(*) FROM dim_instrument").fetchone()
    if res and res[0] == 0:
        conn.execute("""
            INSERT INTO dim_instrument (instrument_sk, instrument_name, asset_class)
            VALUES
                (1, 'EUR/USD', 'forex'),
                (2, 'USD/JPY', 'forex'),
                (3, 'Gold', 'commodities'),
                (4, 'BTC/USD', 'crypto'),
                (5, 'S&P500', 'indices')
        """)
