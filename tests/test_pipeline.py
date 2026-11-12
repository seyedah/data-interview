"""Part 9 — Pipeline tests.

Covers the four required test areas plus extra checks:
  1. No duplicates in the analytics mart by composite key
  2. No invalid/out-of-range timestamps in stg or mart
  3. No negative bytes in clean or mart
  4. Re-run idempotency: running twice creates no duplicates in any layer
  +  stop condition, quarantine reasons, median imputation, real-file smoke.
"""
import os
from datetime import datetime

import pytest

from pipeline.db import parse_timestamp_strict
from tests.conftest import (
    PROJECT_ROOT,
    fixture_median_bytes,
    open_db,
    run_full_pipeline,
    run_pipeline_cli,
)

REAL_CSV = PROJECT_ROOT / "network_logs_dirty.csv"


def test_mart_has_no_duplicate_composite_keys(fixture_csv, db_path):
    result = run_full_pipeline(fixture_csv, db_path)
    assert result.returncode == 0, result.stdout + result.stderr

    with open_db(db_path) as conn:
        dupes = conn.execute(
            """
            SELECT event_date, src_ip, COUNT(*) AS n
            FROM mart_daily_network_stats
            GROUP BY event_date, src_ip
            HAVING COUNT(*) > 1
            """
        ).fetchall()
    assert dupes == [], f"duplicate (event_date, src_ip) keys found: {dupes}"


def test_no_invalid_or_out_of_range_timestamps(fixture_csv, db_path):
    result = run_full_pipeline(fixture_csv, db_path)
    assert result.returncode == 0, result.stdout + result.stderr

    with open_db(db_path) as conn:
        timestamps = [r["event_timestamp"] for r in conn.execute(
            "SELECT event_timestamp FROM stg_network_logs"
        )]
        dates = [r["event_date"] for r in conn.execute(
            "SELECT event_date FROM mart_daily_network_stats"
        )]

    for value in timestamps:
        parsed = parse_timestamp_strict(value)
        assert parsed is not None, f"invalid timestamp in stg: {value!r}"
        assert datetime(2024, 1, 1) <= parsed <= datetime(2024, 12, 31, 23, 59, 59), \
            f"out-of-range timestamp in stg: {value!r}"
    for value in dates:
        assert "2024-01-01" <= value <= "2024-12-31", f"bad mart date: {value!r}"


def test_no_negative_bytes_in_clean_and_mart(fixture_csv, db_path):
    result = run_full_pipeline(fixture_csv, db_path)
    assert result.returncode == 0, result.stdout + result.stderr

    with open_db(db_path) as conn:
        stg_neg = conn.execute(
            "SELECT COUNT(*) FROM stg_network_logs WHERE bytes < 0"
        ).fetchone()[0]
        stg_null = conn.execute(
            "SELECT COUNT(*) FROM stg_network_logs WHERE bytes IS NULL"
        ).fetchone()[0]
        mart_neg = conn.execute(
            "SELECT COUNT(*) FROM mart_daily_network_stats"
            " WHERE total_bytes < 0 OR error_events < 0"
        ).fetchone()[0]
    assert stg_neg == 0
    assert stg_null == 0  # NULL bytes were imputed with the batch median
    assert mart_neg == 0


def test_rerun_idempotency_no_duplicates_anywhere(fixture_csv, db_path):
    first = run_full_pipeline(fixture_csv, db_path)
    assert first.returncode == 0, first.stdout + first.stderr

    def snapshot():
        with open_db(db_path) as conn:
            return {
                "counts": {
                    table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    for table in ("pipeline_batches", "raw_network_logs",
                                  "stg_network_logs", "rejected_network_logs",
                                  "mart_daily_network_stats")
                },
                "stg": conn.execute(
                    "SELECT batch_id, source_record_id, event_timestamp, src_ip,"
                    " dst_ip, port, bytes, bytes_imputed, status"
                    " FROM stg_network_logs ORDER BY batch_id, source_record_id"
                ).fetchall(),
                "rejected": conn.execute(
                    "SELECT batch_id, source_record_id, rejection_reason"
                    " FROM rejected_network_logs ORDER BY batch_id, source_record_id"
                ).fetchall(),
                "mart": conn.execute(
                    "SELECT event_date, src_ip, total_events, total_bytes,"
                    " avg_bytes, error_events FROM mart_daily_network_stats"
                    " ORDER BY event_date, src_ip"
                ).fetchall(),
            }

    before = snapshot()
    second = run_full_pipeline(fixture_csv, db_path)
    assert second.returncode == 0, second.stdout + second.stderr
    after = snapshot()

    assert before["counts"] == after["counts"], "row counts changed after re-run"
    assert before["stg"] == after["stg"], "staging rows changed after re-run"
    assert before["rejected"] == after["rejected"], "quarantine rows changed after re-run"
    assert before["mart"] == after["mart"], "mart rows changed after re-run"
    assert after["counts"]["pipeline_batches"] == 1, "same file must create only one batch"


def test_pipeline_stop_condition_on_excessive_bad_timestamps(stop_condition_csv, db_path):
    result = run_full_pipeline(stop_condition_csv, db_path)
    assert result.returncode == 2, f"expected exit code 2, got {result.returncode}"
    assert "STOP" in result.stderr, "stop condition message missing from output"

    with open_db(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM raw_network_logs").fetchone()[0] == 10
        # Nothing downstream may be produced when the pipeline stops.
        assert conn.execute("SELECT COUNT(*) FROM stg_network_logs").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM rejected_network_logs").fetchone()[0] == 0


def test_quarantine_reasons_and_cleaning_policy(fixture_csv, db_path):
    result = run_full_pipeline(fixture_csv, db_path)
    assert result.returncode == 0, result.stdout + result.stderr

    with open_db(db_path) as conn:
        reasons = {
            r["rejection_reason"] for r in conn.execute(
                "SELECT DISTINCT rejection_reason FROM rejected_network_logs"
            )
        }
        counts = conn.execute(
            "SELECT (SELECT COUNT(*) FROM raw_network_logs) AS raw,"
            " (SELECT COUNT(*) FROM stg_network_logs) AS clean,"
            " (SELECT COUNT(*) FROM rejected_network_logs) AS rejected"
        ).fetchone()
        imputed = conn.execute(
            "SELECT bytes, COUNT(*) AS n FROM stg_network_logs"
            " WHERE bytes_imputed = 1 GROUP BY bytes"
        ).fetchall()
        unknown = conn.execute(
            "SELECT COUNT(*) FROM stg_network_logs WHERE status = 'UNKNOWN'"
        ).fetchone()[0]
        stg_columns = {r["name"] for r in conn.execute(
            "PRAGMA table_info(stg_network_logs)"
        )}

    # Every cleaning-policy rejection reason is exercised by the fixture.
    assert reasons == {
        "INVALID_TIMESTAMP", "TIMESTAMP_OUT_OF_RANGE", "MISSING_REQUIRED_FIELD",
        "INVALID_IP", "INVALID_PORT", "NEGATIVE_BYTES", "DUPLICATE_RECORD",
    }
    # raw = clean + quarantined (each raw row goes to exactly one place)
    assert counts["raw"] == counts["clean"] + counts["rejected"]
    # Empty bytes must equal the fixture's batch median (computed with the
    # same rule as pipeline/clean.py).
    assert imputed and imputed[0]["bytes"] == fixture_median_bytes()
    assert unknown == 1, "empty status must become UNKNOWN"
    # label/anomaly_type must not leak into the clean layer (data contract rule 7)
    assert "label" not in stg_columns and "anomaly_type" not in stg_columns


@pytest.mark.skipif(not REAL_CSV.exists(), reason="real network_logs_dirty.csv not present")
def test_real_file_end_to_end_smoke(tmp_path, monkeypatch):
    db_file = tmp_path / "real_warehouse.sqlite"
    first = run_full_pipeline(REAL_CSV, db_file)
    assert first.returncode == 0, first.stdout + first.stderr

    with open_db(db_file) as conn:
        raw = conn.execute("SELECT COUNT(*) FROM raw_network_logs").fetchone()[0]
        clean = conn.execute("SELECT COUNT(*) FROM stg_network_logs").fetchone()[0]
        rejected = conn.execute("SELECT COUNT(*) FROM rejected_network_logs").fetchone()[0]
        mart = conn.execute("SELECT COUNT(*) FROM mart_daily_network_stats").fetchone()[0]

    assert raw == 10300
    assert clean + rejected == raw
    assert mart > 0

    second = run_full_pipeline(REAL_CSV, db_file)
    assert second.returncode == 0, second.stdout + second.stderr
    with open_db(db_file) as conn:
        assert conn.execute("SELECT COUNT(*) FROM pipeline_batches").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM stg_network_logs").fetchone()[0] == clean
        assert conn.execute("SELECT COUNT(*) FROM rejected_network_logs").fetchone()[0] == rejected
        assert conn.execute("SELECT COUNT(*) FROM mart_daily_network_stats").fetchone()[0] == mart


def test_status_command_reports_batches(fixture_csv, db_path):
    run_full_pipeline(fixture_csv, db_path)
    result = run_pipeline_cli(["status", "--db", str(db_path)])
    assert result.returncode == 0
    assert "COMPLETED" in result.stderr
