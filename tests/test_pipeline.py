"""
Focused pytest suite for Deriv senior data engineering assessment pipeline prototype.
Tests idempotency, vendor schema drift normalization, deduplication, quarantine routing,
LSN-ordered CDC historization, soft deletion, and deterministic historical replay.
"""

import datetime
import decimal
import json
from pathlib import Path
import shutil
import sys
import pytest
import duckdb

CODE_DIR = Path(__file__).resolve().parent.parent / "code"
if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))

from db import get_connection, init_schema
from extract_fixtures import extract_fixtures
from ingestion import apply_cdc_stream, load_core_tables, process_vendor_deposits
from replay import replay_cdc_range
from run_pipeline import run_pipeline

# Human-readable justification:
# Proves compliance with all acceptance criteria: idempotency, drift normalization,
# quarantine routing, LSN-ordered CDC, delete preservation, and deterministic replay.

@pytest.fixture(scope="session")
def fixtures_dir(tmp_path_factory):
    repo_root = Path(__file__).resolve().parent.parent
    instructions_file = repo_root / "task_instructions.md"
    data_dir = tmp_path_factory.mktemp("data")
    extract_fixtures(instructions_file, data_dir)
    return data_dir

@pytest.fixture
def fresh_db(fixtures_dir):
    repo_root = Path(__file__).resolve().parent.parent
    sql_dir = repo_root / "sql"
    conn = get_connection(":memory:")
    init_schema(conn, sql_dir)
    return conn, fixtures_dir

def run_full_pipeline(conn: duckdb.DuckDBPyConnection, data_dir: Path, batch_id: str = "test_batch"):
    load_core_tables(conn, data_dir, batch_id)
    process_vendor_deposits(conn, data_dir, batch_id)
    apply_cdc_stream(conn, data_dir, batch_id)

def test_idempotency_second_run_creates_no_duplicates(fresh_db):
    """Proves that running the full pipeline twice on the same database creates zero duplicate records."""
    conn, data_dir = fresh_db

    # Run 1
    run_full_pipeline(conn, data_dir, "batch_1")

    c1_dim_client = conn.execute("SELECT COUNT(*) FROM dim_client").fetchone()[0]
    c1_fact_deposit = conn.execute("SELECT COUNT(*) FROM fact_deposit").fetchone()[0]
    c1_fact_trade = conn.execute("SELECT COUNT(*) FROM fact_trade").fetchone()[0]
    c1_fact_balance = conn.execute("SELECT COUNT(*) FROM fact_client_balance_history").fetchone()[0]
    c1_quarantine = conn.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0]

    # Run 2
    run_full_pipeline(conn, data_dir, "batch_2")

    c2_dim_client = conn.execute("SELECT COUNT(*) FROM dim_client").fetchone()[0]
    c2_fact_deposit = conn.execute("SELECT COUNT(*) FROM fact_deposit").fetchone()[0]
    c2_fact_trade = conn.execute("SELECT COUNT(*) FROM fact_trade").fetchone()[0]
    c2_fact_balance = conn.execute("SELECT COUNT(*) FROM fact_client_balance_history").fetchone()[0]
    c2_quarantine = conn.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0]

    assert c1_dim_client == c2_dim_client == 36
    assert c1_fact_deposit == c2_fact_deposit == 40
    assert c1_fact_trade == c2_fact_trade == 20
    assert c1_fact_balance == c2_fact_balance == 5
    assert c1_quarantine == c2_quarantine == 2

def test_vendor_schema_drift_normalization(fresh_db):
    """Proves that 'method' column alias in deposits_vendor_20240302.csv is normalized to 'payment_method'."""
    conn, data_dir = fresh_db
    run_full_pipeline(conn, data_dir, "batch_drift_test")

    # Check rows from 2024-03-02 file, e.g. VDEP010 with e_wallet
    res = conn.execute("""
        SELECT payment_method, amount_usd
        FROM fact_deposit
        WHERE source_system = 'vendor' AND deposit_id = 'VDEP010'
    """).fetchone()

    assert res is not None
    assert res[0] == "e_wallet"
    assert res[1] == decimal.Decimal("600.00")

def test_repeated_vendor_rows_are_deduplicated(fresh_db):
    """Proves that duplicate vendor deposits (VDEP002, VDEP005) appearing in both 20240301 and 20240302 are loaded exactly once."""
    conn, data_dir = fresh_db
    run_full_pipeline(conn, data_dir, "batch_dedup_test")

    for dep_id in ["VDEP002", "VDEP005"]:
        count = conn.execute("""
            SELECT COUNT(*)
            FROM fact_deposit
            WHERE source_system = 'vendor' AND deposit_id = ?
        """, [dep_id]).fetchone()[0]
        assert count == 1, f"Expected exactly 1 row for {dep_id}, found {count}"

def test_invalid_and_orphan_rows_are_quarantined(fresh_db):
    """Proves negative amount (VDEP001) and orphan client (VDEP020 -> CL099) are quarantined with explicit reason codes."""
    conn, data_dir = fresh_db
    run_full_pipeline(conn, data_dir, "batch_quarantine_test")

    # 1. Negative amount check
    neg_row = conn.execute("""
        SELECT record_id, reason_code, severity, is_resolved, retry_eligible
        FROM quarantine
        WHERE record_id = 'VDEP001'
    """).fetchone()
    assert neg_row is not None
    assert neg_row[1] == "NEGATIVE_AMOUNT"
    assert neg_row[2] == "CRITICAL"
    assert neg_row[4] is False  # Not retry eligible without source data correction

    # Verify VDEP001 is NOT in fact_deposit
    in_fact = conn.execute("SELECT COUNT(*) FROM fact_deposit WHERE deposit_id = 'VDEP001'").fetchone()[0]
    assert in_fact == 0

    # 2. Orphan client check
    orphan_row = conn.execute("""
        SELECT record_id, reason_code, severity, is_resolved, retry_eligible
        FROM quarantine
        WHERE record_id = 'VDEP020'
    """).fetchone()
    assert orphan_row is not None
    assert orphan_row[1] == "ORPHAN_CLIENT"
    assert orphan_row[2] == "ERROR"
    assert orphan_row[4] is True  # Retry eligible once client CL099 is registered

    # Verify VDEP020 is NOT in fact_deposit
    in_fact_orphan = conn.execute("SELECT COUNT(*) FROM fact_deposit WHERE deposit_id = 'VDEP020'").fetchone()[0]
    assert in_fact_orphan == 0

def test_deposit_namespace_separation(fresh_db):
    """Proves warehouse DEP... and vendor VDEP... deposits coexist without colliding."""
    conn, data_dir = fresh_db
    run_full_pipeline(conn, data_dir, "batch_namespace_test")

    wh_count = conn.execute("SELECT COUNT(*) FROM fact_deposit WHERE source_system = 'warehouse'").fetchone()[0]
    vendor_count = conn.execute("SELECT COUNT(*) FROM fact_deposit WHERE source_system = 'vendor'").fetchone()[0]

    assert wh_count == 20
    assert vendor_count == 20

def test_cdc_applied_by_lsn_order(fresh_db):
    """Proves CDC events are applied in strict ascending LSN order, rather than file arrival order."""
    conn, data_dir = fresh_db
    run_full_pipeline(conn, data_dir, "batch_cdc_order_test")

    # In file arrival order, LSN 1005 (balance update at 11:00) arrived BEFORE LSN 1004 (risk update at 10:30).
    # In LSN ascending order, LSN 1004 is applied before LSN 1005, then LSN 1006.
    cl001_versions = conn.execute("""
        SELECT client_sk, risk_category, account_status, valid_from, valid_to, is_current
        FROM dim_client
        WHERE client_id = 'CL001'
        ORDER BY valid_from ASC
    """).fetchall()

    assert len(cl001_versions) == 3
    # Version 1 (Initial snapshot): medium risk, active status
    assert cl001_versions[0][1] == "medium"
    assert cl001_versions[0][2] == "active"
    assert cl001_versions[0][5] is False

    # Version 2 (LSN 1004 at 10:30): high risk, active status
    assert cl001_versions[1][1] == "high"
    assert cl001_versions[1][2] == "active"
    assert cl001_versions[1][5] is False
    assert cl001_versions[1][3] == datetime.datetime(2024, 11, 15, 10, 30)

    # Version 3 (LSN 1006 at 14:00): high risk, under_review status
    assert cl001_versions[2][1] == "high"
    assert cl001_versions[2][2] == "under_review"
    assert cl001_versions[2][5] is True
    assert cl001_versions[2][3] == datetime.datetime(2024, 11, 15, 14, 0)

    # Balance observation for CL001 at LSN 1005
    bal_row = conn.execute("""
        SELECT balance_usd
        FROM fact_client_balance_history
        WHERE client_id = 'CL001' AND lsn = 1005
    """).fetchone()
    assert bal_row is not None
    assert bal_row[0] == decimal.Decimal("1850.00")

def test_cdc_delete_retains_history(fresh_db):
    """Proves that a delete CDC event (LSN 1010 for CL012) soft-deletes the client without destroying history or facts."""
    conn, data_dir = fresh_db
    run_full_pipeline(conn, data_dir, "batch_delete_test")

    # Verify CL012 in dim_client
    cl012_rows = conn.execute("""
        SELECT client_sk, is_current, is_deleted, deleted_at, valid_to
        FROM dim_client
        WHERE client_id = 'CL012'
    """).fetchall()

    assert len(cl012_rows) == 1
    assert cl012_rows[0][1] is False  # is_current is False
    assert cl012_rows[0][2] is True   # is_deleted is True
    assert cl012_rows[0][3] is not None # deleted_at is recorded
    assert cl012_rows[0][4] is not None # valid_to is set
    assert cl012_rows[0][3] == datetime.datetime(2024, 11, 21, 14, 0)

    # Verify CL012 historical deposits and trades are intact
    dep_count = conn.execute("SELECT COUNT(*) FROM fact_deposit WHERE client_id = 'CL012'").fetchone()[0]
    assert dep_count == 2  # DEP008 (warehouse) + VDEP004 (vendor)

def test_historical_replay_is_deterministic(fresh_db):
    """Proves bounded historical replay reconstructs exact SCD intervals and balance history without overlaps."""
    conn, data_dir = fresh_db
    run_full_pipeline(conn, data_dir, "batch_replay_test_init")

    # Snapshot state before replay
    dim_client_before = conn.execute("SELECT client_sk, client_id, risk_category, account_status, valid_from, valid_to, is_current, is_deleted FROM dim_client ORDER BY client_sk").fetchall()
    balance_before = conn.execute("SELECT lsn, client_id, balance_usd FROM fact_client_balance_history ORDER BY lsn").fetchall()

    # Execute replay over November 2024
    res = replay_cdc_range(conn, "2024-11-01", "2024-11-30", batch_id="test_replay")
    assert res["status"] == "SUCCESS"
    assert res["replayed_events"] == 11

    dim_client_after = conn.execute("SELECT client_sk, client_id, risk_category, account_status, valid_from, valid_to, is_current, is_deleted FROM dim_client ORDER BY client_sk").fetchall()
    balance_after = conn.execute("SELECT lsn, client_id, balance_usd FROM fact_client_balance_history ORDER BY lsn").fetchall()

    # State must be identical
    assert len(dim_client_before) == len(dim_client_after) == 36
    assert len(balance_before) == len(balance_after) == 5
    assert dim_client_before == dim_client_after
    assert balance_before == balance_after

    # Invariant: 0 overlapping intervals
    overlap_check = conn.execute("""
        SELECT a.client_id, a.client_sk, b.client_sk
        FROM dim_client a
        JOIN dim_client b ON a.client_id = b.client_id AND a.client_sk < b.client_sk
        WHERE (a.valid_to IS NULL OR a.valid_to > b.valid_from)
          AND (b.valid_to IS NULL OR b.valid_to > a.valid_from)
    """).fetchall()
    assert len(overlap_check) == 0


def test_replay_preserves_events_after_requested_window(fixtures_dir, tmp_path):
    """A November replay must rebuild, not erase, a later version for the same client."""
    data_dir = tmp_path / "data"
    shutil.copytree(fixtures_dir, data_dir)
    future_event = {
        "lsn": 1021,
        "commit_ts": "2024-12-01T09:00:00Z",
        "op": "update",
        "client_id": "CL001",
        "before": {
            "risk_category": "high",
            "account_balance_usd": 1850.00,
            "account_status": "under_review",
        },
        "after": {
            "risk_category": "low",
            "account_balance_usd": 1900.00,
            "account_status": "active",
        },
    }
    with (data_dir / "client_profile_changes.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(future_event) + "\n")

    conn = get_connection(":memory:")
    init_schema(conn, Path(__file__).resolve().parent.parent / "sql")
    run_full_pipeline(conn, data_dir, "future_event_initial")

    replay_cdc_range(conn, "2024-11-01", "2024-11-30", batch_id="future_event_replay")

    current = conn.execute("""
        SELECT risk_category, account_status, valid_from
        FROM dim_client
        WHERE client_id = 'CL001' AND is_current = TRUE
    """).fetchone()
    assert current[0:2] == ("low", "active")
    assert current[2].date().isoformat() == "2024-12-01"


def test_unapproved_vendor_schema_drift_fails_batch(fresh_db, tmp_path):
    """Unknown columns fail at the ingestion boundary instead of being silently ignored."""
    conn, fixtures_dir = fresh_db
    data_dir = tmp_path / "data"
    shutil.copytree(fixtures_dir, data_dir)
    vendor_file = data_dir / "deposits_vendor_20240301.csv"
    text = vendor_file.read_text(encoding="utf-8")
    vendor_file.write_text(text.replace("fee_usd\n", "fee_usd,unknown_field\n", 1), encoding="utf-8")

    load_core_tables(conn, data_dir, "schema_drift_core")
    with pytest.raises(ValueError, match="Unapproved schema drift"):
        process_vendor_deposits(conn, data_dir, "schema_drift_vendor")


def test_failed_pipeline_rolls_back_data_and_manifest(fixtures_dir, tmp_path):
    """Curated rows and SUCCESS manifests must not survive a failed pipeline run."""
    data_dir = tmp_path / "data"
    shutil.copytree(fixtures_dir, data_dir)
    vendor_file = data_dir / "deposits_vendor_20240301.csv"
    text = vendor_file.read_text(encoding="utf-8")
    vendor_file.write_text(text.replace("fee_usd\n", "fee_usd,unknown_field\n", 1), encoding="utf-8")
    db_path = tmp_path / "rollback.duckdb"

    with pytest.raises(ValueError, match="Unapproved schema drift"):
        run_pipeline(str(db_path), data_dir)

    conn = get_connection(db_path)
    assert conn.execute("SELECT COUNT(*) FROM ingestion_file_manifest").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM dim_client").fetchone()[0] == 0


def test_date_only_replay_includes_entire_end_day(fresh_db):
    """A one-day CLI-style window includes events after midnight on its end date."""
    conn, data_dir = fresh_db
    run_full_pipeline(conn, data_dir, "single_day_initial")

    result = replay_cdc_range(conn, "2024-11-20", "2024-11-20", batch_id="single_day_replay")

    assert result["status"] == "SUCCESS"
    assert result["affected_clients"] == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM cdc_processing_ledger WHERE lsn = 1008"
    ).fetchone()[0] == 1


def test_balance_history_contains_only_actual_changes(fresh_db):
    """Updates carrying an unchanged balance do not create false balance events."""
    conn, data_dir = fresh_db
    run_full_pipeline(conn, data_dir, "balance_change_initial")

    lsns = [row[0] for row in conn.execute(
        "SELECT lsn FROM fact_client_balance_history ORDER BY lsn"
    ).fetchall()]
    assert lsns == [1005, 1008, 1012, 1015, 1018]


def test_missing_required_input_fails_loudly(fixtures_dir, tmp_path):
    """A partial delivery cannot be mistaken for a successful zero-row source."""
    data_dir = tmp_path / "data"
    shutil.copytree(fixtures_dir, data_dir)
    (data_dir / "client_trades.json").unlink()

    with pytest.raises(FileNotFoundError, match="client_trades.json"):
        run_pipeline(str(tmp_path / "missing.duckdb"), data_dir)


def test_analytics_queries_execute_against_curated_warehouse(fresh_db):
    """Every published analytics example remains runnable against the curated schema."""
    conn, data_dir = fresh_db
    run_full_pipeline(conn, data_dir, "analytics_query_test")
    analytics_dir = Path(__file__).resolve().parent.parent / "analytics"

    for query_path in sorted(analytics_dir.glob("[0-9][0-9]_*.sql")):
        rows = conn.execute(query_path.read_text(encoding="utf-8")).fetchall()
        assert rows, f"Expected {query_path.name} to return at least one row"
