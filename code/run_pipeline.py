"""
CLI entry point for the Deriv trading warehouse data pipeline.
Usage:
    uv run python code/run_pipeline.py
    uv run python code/run_pipeline.py --replay 2024-11-01 2024-11-30
"""

import argparse
import datetime
from pathlib import Path
import sys
import uuid

from db import get_connection, init_schema
from extract_fixtures import extract_fixtures
from ingestion import apply_cdc_stream, load_core_tables, process_vendor_deposits
from replay import replay_cdc_range

# Human-readable justification:
# Orchestrates end-to-end extraction, schema initialization, core loading, vendor reconciliation,
# and CDC application, providing detailed execution metrics and invariant checks.

def run_pipeline(db_path: str = "warehouse.duckdb", data_dir: Path | None = None) -> int:
    repo_root = Path(__file__).resolve().parent.parent
    if data_dir is None:
        data_dir = repo_root / "data"

    instructions_file = repo_root / "task_instructions.md"
    sql_dir = repo_root / "sql"

    required_inputs = {
        "client_signup.json", "client_profile.json", "client_deposit.json",
        "client_trades.json", "deposits_vendor_20240301.csv",
        "deposits_vendor_20240302.csv", "deposits_vendor_20240303.csv",
        "client_profile_changes.jsonl",
    }

    # 1. Extract only for a genuinely new data directory; partial input sets fail loudly.
    if not data_dir.exists():
        print("[INFO] Extracting fixtures from task_instructions.md...")
        extract_fixtures(instructions_file, data_dir)
    missing_inputs = sorted(name for name in required_inputs if not (data_dir / name).is_file())
    if missing_inputs:
        raise FileNotFoundError(f"Missing required input files: {missing_inputs}")

    # 2. Connect to database and initialize schema
    print(f"[INFO] Connecting to DuckDB at {db_path}...")
    conn = get_connection(db_path)
    init_schema(conn, sql_dir)

    batch_id = f"batch_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:6]}"
    print(f"[INFO] Starting pipeline run with Batch ID: {batch_id}")

    # Publish data and successful control state atomically so failed runs remain retryable.
    conn.execute("BEGIN TRANSACTION")
    try:
        print("[INFO] Loading core warehouse snapshots (signup, profile, deposit, trade)...")
        core_metrics = load_core_tables(conn, data_dir, batch_id)
        for tbl, count in core_metrics.items():
            print(f"  - Landed {tbl}: {count} records")

        print("[INFO] Ingesting and reconciling vendor deposit feeds...")
        vendor_metrics = process_vendor_deposits(conn, data_dir, batch_id)
        print(f"  - Files processed: {vendor_metrics['files_processed']}, skipped: {vendor_metrics['files_skipped']}")
        print(f"  - Rows landed: {vendor_metrics['rows_landed']}, staged: {vendor_metrics['rows_staged']}, quarantined: {vendor_metrics['rows_quarantined']}")
        print(f"  - Exact duplicates skipped: {vendor_metrics['duplicates_skipped']}")

        print("[INFO] Processing CDC stream by ascending LSN order...")
        cdc_metrics = apply_cdc_stream(conn, data_dir, batch_id)
        print(f"  - Events seen: {cdc_metrics['events_seen']}, applied: {cdc_metrics['applied']}, reconciled: {cdc_metrics['reconciled']}, skipped: {cdc_metrics['skipped']}")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    # 6. Verification and Invariant Summary
    print("\n" + "=" * 60)
    print("PIPELINE EXECUTION SUMMARY & INVARIANT CHECKS")
    print("=" * 60)

    tables_to_check = [
        "dim_client",
        "fact_deposit",
        "fact_trade",
        "fact_client_balance_history",
        "quarantine",
        "ingestion_file_manifest",
        "cdc_processing_ledger"
    ]

    for tbl in tables_to_check:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"  * {tbl.ljust(30)}: {cnt} rows")

    # Check SCD2 overlapping intervals
    overlap_res = conn.execute("""
        SELECT a.client_id, a.client_sk, b.client_sk
        FROM dim_client a
        JOIN dim_client b ON a.client_id = b.client_id AND a.client_sk < b.client_sk
        WHERE (a.valid_to IS NULL OR a.valid_to > b.valid_from)
          AND (b.valid_to IS NULL OR b.valid_to > a.valid_from)
    """).fetchall()
    print(f"  * SCD2 overlapping intervals check : {len(overlap_res)} violations")

    # Check quarantine details
    quarantine_rows = conn.execute("""
        SELECT reason_code, severity, COUNT(*)
        FROM quarantine
        GROUP BY reason_code, severity
    """).fetchall()
    print("\nQuarantine Summary:")
    for code, sev, count in quarantine_rows:
        print(f"  * [{sev}] {code}: {count} row(s)")

    # Check canonical deposit reconciliation breakdown
    deposit_breakdown = conn.execute("""
        SELECT source_system, reconciliation_status, COUNT(*)
        FROM fact_deposit
        GROUP BY source_system, reconciliation_status
    """).fetchall()
    print("\nFact Deposit Reconciliation Breakdown:")
    for src, status, count in deposit_breakdown:
        print(f"  * Source [{src}] -> Status [{status}]: {count} row(s)")

    print("=" * 60 + "\n")
    return 0

def main():
    parser = argparse.ArgumentParser(description="Deriv Data Engineering Pipeline Prototype")
    parser.add_argument("--db-path", default="warehouse.duckdb", help="Path to DuckDB database file")
    parser.add_argument("--replay", nargs=2, metavar=("START_DATE", "END_DATE"), help="Replay CDC for date range (e.g. 2024-11-01 2024-11-30)")

    args = parser.parse_args()

    if args.replay:
        start_date, end_date = args.replay
        print(f"[INFO] Running bounded historical replay for window [{start_date} to {end_date}]...")
        conn = get_connection(args.db_path)
        res = replay_cdc_range(conn, start_date, end_date)
        print(f"[INFO] Replay completed: {res}")
        return

    exit_code = run_pipeline(db_path=args.db_path)
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
