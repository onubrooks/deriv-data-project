"""
Bounded historical replay module for Deriv trading warehouse.
Allows deterministic reprocessing of CDC change events for a specific date range
without creating overlapping SCD Type 2 intervals or duplicate balance observations.
"""

import datetime
import decimal
import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb

# Human-readable justification:
# Deterministically rebuilds client SCD2 history and balance facts within an arbitrary date range,
# ensuring zero interval overlap and full reproducibility.

def _parse_utc_ts(ts: datetime.datetime | str) -> datetime.datetime:
    """Parse string or datetime to timezone-aware UTC datetime."""
    if isinstance(ts, str):
        cleaned = ts.strip()
        if "T" not in cleaned and " " not in cleaned:
            cleaned = f"{cleaned}T00:00:00+00:00"
        elif cleaned.endswith("Z"):
            cleaned = cleaned[:-1] + "+00:00"
        elif "+" not in cleaned and "-" not in cleaned[10:]:
            cleaned = f"{cleaned}+00:00"
        dt = datetime.datetime.fromisoformat(cleaned)
    else:
        dt = ts
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt

def replay_cdc_range(
    conn: duckdb.DuckDBPyConnection,
    start_ts: datetime.datetime | str,
    end_ts: datetime.datetime | str,
    batch_id: str = "replay_batch"
) -> dict[str, Any]:
    """
    Replay CDC events for an arbitrary timestamp window [start_ts, end_ts].
    Reconstructs SCD2 intervals and balance history deterministically.
    """
    start_dt = _parse_utc_ts(start_ts)
    end_dt = _parse_utc_ts(end_ts)
    if isinstance(end_ts, str) and len(end_ts.strip()) == 10:
        # If date-only end date was provided (e.g. 2024-11-30), include the whole day
        end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
    if start_dt > end_dt:
        raise ValueError("Replay start must not be after replay end")

    now = datetime.datetime.now(datetime.timezone.utc)

    # Identify raw CDC events in the requested window
    raw_events = conn.execute("""
        SELECT lsn, commit_ts, op, client_id, before_json, after_json, payload_hash
        FROM raw_cdc_events
        ORDER BY lsn ASC
    """).fetchall()

    # Filter in memory by timestamp
    all_events = []
    window_events = []
    affected_client_ids = set()
    for row in raw_events:
        lsn, commit_ts_str, op, client_id, before_json, after_json, payload_hash = row
        event_ts = datetime.datetime.fromisoformat(commit_ts_str.replace("Z", "+00:00"))
        event = {
            "lsn": lsn,
            "commit_ts": event_ts,
            "op": op,
            "client_id": client_id,
            "before": json.loads(before_json) if before_json else None,
            "after": json.loads(after_json) if after_json else None,
            "payload_hash": payload_hash,
        }
        all_events.append(event)
        if start_dt <= event_ts <= end_dt:
            window_events.append(event)
            affected_client_ids.add(client_id)

    if not window_events:
        return {"replayed_events": 0, "affected_clients": 0, "status": "NOOP"}

    # Rebuild through the latest event for each affected client. Replaying only the
    # requested window would delete valid later versions and corrupt current state.
    replay_events = [
        event for event in all_events
        if event["client_id"] in affected_client_ids and event["commit_ts"] >= start_dt
    ]

    conn.execute("BEGIN TRANSACTION")
    try:
        result = _rebuild_affected_history(
            conn, start_dt, affected_client_ids, replay_events, batch_id, now
        )
        conn.execute("COMMIT")
        return result
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _rebuild_affected_history(
    conn: duckdb.DuckDBPyConnection,
    start_dt: datetime.datetime,
    affected_client_ids: set[str],
    replay_events: list[dict[str, Any]],
    batch_id: str,
    now: datetime.datetime,
) -> dict[str, Any]:
    """Rebuild affected clients from the replay boundary through their latest event."""
    # 1. For each affected client, purge generated history from the replay boundary.
    for client_id in affected_client_ids:
        conn.execute("""
            DELETE FROM fact_client_balance_history
            WHERE client_id = ? AND observed_at >= ?
        """, [client_id, start_dt])

        # Delete dim_client versions created in or after start_dt
        conn.execute("""
            DELETE FROM dim_client
            WHERE client_id = ? AND valid_from >= ?
        """, [client_id, start_dt])

        # Re-open the version that was active prior to start_dt (if any)
        conn.execute("""
            UPDATE dim_client
            SET valid_to = NULL, is_current = TRUE, is_deleted = FALSE, deleted_at = NULL
            WHERE client_id = ? AND valid_from < ? AND (valid_to >= ? OR valid_to IS NULL)
        """, [client_id, start_dt, start_dt])

    # 2. Reset processing ledger for every event that will be rebuilt.
    for ev in replay_events:
        conn.execute("DELETE FROM cdc_processing_ledger WHERE lsn = ?", [ev["lsn"]])

    # 3. Re-apply affected history in strict LSN order.
    replayed_count = 0
    for ev in replay_events:
        lsn = ev["lsn"]
        commit_ts = ev["commit_ts"]
        op = ev["op"]
        client_id = ev["client_id"]
        before = ev.get("before") or {}
        after = ev.get("after") or {}
        payload_hash = ev["payload_hash"]

        if op == "insert":
            existing = conn.execute("SELECT client_sk FROM dim_client WHERE client_id = ?", [client_id]).fetchone()
            if existing:
                outcome = "RECONCILED"
            else:
                res = conn.execute("SELECT COALESCE(MAX(client_sk), 0) FROM dim_client").fetchone()
                next_sk = (res[0] if res else 0) + 1
                conn.execute("""
                    INSERT INTO dim_client (
                        client_sk, client_id, full_name, date_of_birth, email, country,
                        nationality, account_type, kyc_status, referral_source, signup_platform,
                        promo_code, assigned_manager, preferred_language, risk_category,
                        account_status, valid_from, valid_to, is_current, is_deleted,
                        deleted_at, is_inferred, batch_id, created_at
                    ) VALUES (
                        ?, ?, ?, ?::DATE, ?, 'Unknown',
                        ?, 'standard', 'approved', 'cdc_stream', 'system',
                        NULL, 'UNASSIGNED', ?, ?,
                        ?, ?, NULL, TRUE, FALSE,
                        NULL, FALSE, ?, ?
                    )
                """, [
                    next_sk, client_id, after.get("full_name"), after.get("date_of_birth"),
                    f"{client_id.lower()}@cdc.deriv.com", after.get("nationality"),
                    after.get("preferred_language", "English"), after.get("risk_category", "medium"),
                    after.get("account_status", "active"), commit_ts, batch_id, now
                ])
                outcome = "APPLIED"
        elif op == "update":
            curr_row = conn.execute("""
                SELECT client_sk, full_name, date_of_birth, email, country,
                       nationality, account_type, kyc_status, referral_source,
                       signup_platform, promo_code, assigned_manager, preferred_language,
                       risk_category, account_status
                FROM dim_client
                WHERE client_id = ? AND is_current = TRUE
            """, [client_id]).fetchone()

            if curr_row:
                (
                    curr_sk, full_name, dob, email, country,
                    nat, acct_type, kyc, ref_src,
                    plat, promo, mgr, lang,
                    curr_risk, curr_status
                ) = curr_row

                new_risk = after.get("risk_category", curr_risk)
                new_status = after.get("account_status", curr_status)

                if new_risk != curr_risk or new_status != curr_status:
                    res = conn.execute("SELECT COALESCE(MAX(client_sk), 0) FROM dim_client").fetchone()
                    next_sk = (res[0] if res else 0) + 1

                    conn.execute("""
                        UPDATE dim_client
                        SET valid_to = ?, is_current = FALSE
                        WHERE client_sk = ?
                    """, [commit_ts, curr_sk])

                    conn.execute("""
                        INSERT INTO dim_client (
                            client_sk, client_id, full_name, date_of_birth, email, country,
                            nationality, account_type, kyc_status, referral_source, signup_platform,
                            promo_code, assigned_manager, preferred_language, risk_category,
                            account_status, valid_from, valid_to, is_current, is_deleted,
                            deleted_at, is_inferred, batch_id, created_at
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?,
                            ?, ?, ?, ?,
                            ?, ?, NULL, TRUE, FALSE,
                            NULL, FALSE, ?, ?
                        )
                    """, [
                        next_sk, client_id, full_name, dob, email, country,
                        nat, acct_type, kyc, ref_src, plat,
                        promo, mgr, lang, new_risk,
                        new_status, commit_ts, batch_id, now
                    ])

                if (
                    "account_balance_usd" in after
                    and before.get("account_balance_usd") != after.get("account_balance_usd")
                ):
                    bal = decimal.Decimal(str(after["account_balance_usd"]))
                    conn.execute("""
                        INSERT INTO fact_client_balance_history (
                            lsn, client_id, observed_at, balance_usd, batch_id, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT (lsn) DO NOTHING
                    """, [lsn, client_id, commit_ts, bal, batch_id, now])
            outcome = "APPLIED"
        elif op == "delete":
            conn.execute("""
                UPDATE dim_client
                SET valid_to = ?, is_current = FALSE, is_deleted = TRUE, deleted_at = ?
                WHERE client_id = ? AND is_current = TRUE
            """, [commit_ts, commit_ts, client_id])
            outcome = "APPLIED"

        conn.execute("""
            INSERT INTO cdc_processing_ledger (
                lsn, payload_hash, batch_id, client_id, op, outcome, applied_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (lsn) DO UPDATE SET
                outcome = excluded.outcome,
                applied_at = excluded.applied_at
        """, [lsn, payload_hash, batch_id, client_id, op, outcome, now])
        replayed_count += 1

    # Invariant verification: zero overlapping SCD intervals
    overlap_check = conn.execute("""
        SELECT a.client_id, a.client_sk, b.client_sk
        FROM dim_client a
        JOIN dim_client b ON a.client_id = b.client_id AND a.client_sk < b.client_sk
        WHERE (a.valid_to IS NULL OR a.valid_to > b.valid_from)
          AND (b.valid_to IS NULL OR b.valid_to > a.valid_from)
    """).fetchall()

    assert len(overlap_check) == 0, f"Invariant violation: found overlapping SCD intervals: {overlap_check}"

    return {
        "replayed_events": replayed_count,
        "affected_clients": len(affected_client_ids),
        "status": "SUCCESS",
        "overlap_count": 0
    }
