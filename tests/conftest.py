"""Shared pytest fixtures: a small deterministic CSV covering every data
quality issue, plus helpers to run the pipeline CLI in a subprocess with an
isolated temporary SQLite database.
"""
import os
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]

FIXTURE_SPECIAL_ROWS = """2024-03-01 10:00:00,10.0.0.1,8.8.8.8,443,1000,TCP,SUCCESS,benign,
2024-03-01 10:00:00,10.0.0.1,8.8.8.8,443,1000,TCP,SUCCESS,benign,
2024-03-01 11:00:00,10.0.0.2,8.8.8.8,80,,TCP,TIMEOUT,benign,
2024-03-02 09:00:00,10.0.0.3,1.1.1.1,22,500,SSH,,benign,
not-a-timestamp,10.0.0.4,1.1.1.1,22,300,TCP,RESET,benign,
2024-02-30 12:00:00,10.0.0.5,1.1.1.1,22,300,TCP,ERROR,benign,
2019-05-16 08:00:00,10.0.0.6,1.1.1.1,22,300,TCP,REFUSED,benign,
2024-03-02 09:30:00,,1.1.1.1,22,300,TCP,SUCCESS,benign,
2024-03-02 09:40:00,999.999.999.999,1.1.1.1,22,300,TCP,SUCCESS,benign,
2024-03-02 09:50:00,10.0.0.9,1.1.1.1,70000,300,TCP,SUCCESS,benign,
2024-03-02 10:00:00,10.0.0.10,1.1.1.1,22,-500,TCP,SUCCESS,benign,
2024-03-02 10:10:00,10.0.0.11,1.1.1.1,0,300,TCP,SUCCESS,benign,
2024-03-03 10:00:00,10.0.0.12,8.8.4.4,53,2000,UDP,UNREACHABLE,benign,
2024-03-03 10:00:00,10.0.0.12,8.8.4.4,53,2000,UDP,UNREACHABLE,benign,
2024-03-04 12:00:00,10.0.0.13,8.8.8.8,443,4000,TCP,RESET,malicious,ddos
"""

_HEADER = "timestamp,src_ip,dst_ip,port,bytes,protocol,status,label,anomaly_type\n"


def _build_fixture_csv() -> str:
    """80 valid filler rows keep the invalid-timestamp share of the special
    rows below the 5% pipeline-stop threshold (3/95 = 3.2%)."""
    lines = [_HEADER]
    for i in range(80):
        total = i * 61
        hour = 8 + total // 3600
        minute = (total % 3600) // 60
        second = total % 60
        lines.append(
            f"2024-06-01 {hour:02d}:{minute:02d}:{second:02d},10.9.0.{i + 1},8.8.4.4,"
            f"{8000 + i},{100 + i},TCP,SUCCESS,benign,\n"
        )
    lines.append(FIXTURE_SPECIAL_ROWS)
    return "".join(lines)


FIXTURE_CSV_ROWS = _build_fixture_csv()


def fixture_median_bytes() -> int:
    """Replicate the pipeline's batch-median rule over the fixture's raw bytes."""
    values = sorted(
        int(row.split(",")[4])
        for row in FIXTURE_CSV_ROWS.splitlines()[1:]
        if row.split(",")[4].lstrip("+-").isdigit()
        and int(row.split(",")[4]) >= 0
    )
    n = len(values)
    mid = n // 2
    median = values[mid] if n % 2 == 1 else (values[mid - 1] + values[mid]) / 2
    return int(median + 0.5) if isinstance(median, float) else median

# 10 rows, 6 with bad timestamps (60% > 5% stop threshold).
STOP_CONDITION_CSV_ROWS = """timestamp,src_ip,dst_ip,port,bytes,protocol,status,label,anomaly_type
2024-03-01 10:00:00,10.0.0.1,8.8.8.8,443,1000,TCP,SUCCESS,benign,
2024-03-01 10:05:00,10.0.0.1,8.8.8.8,443,1000,TCP,SUCCESS,benign,
2024-03-01 10:10:00,10.0.0.1,8.8.8.8,443,1000,TCP,SUCCESS,benign,
2024-03-01 10:15:00,10.0.0.1,8.8.8.8,443,1000,TCP,SUCCESS,benign,
,10.0.0.2,8.8.8.8,80,100,TCP,SUCCESS,benign,
broken-ts,10.0.0.3,8.8.8.8,80,100,TCP,SUCCESS,benign,
2024-02-30 10:00:00,10.0.0.4,8.8.8.8,80,100,TCP,SUCCESS,benign,
2019-05-16 08:00:00,10.0.0.5,8.8.8.8,80,100,TCP,SUCCESS,benign,
2030-05-01 08:00:00,10.0.0.6,8.8.8.8,80,100,TCP,SUCCESS,benign,
2024/01/01 10:00:00,10.0.0.7,8.8.8.8,80,100,TCP,SUCCESS,benign,
"""


def _write(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    return path


@pytest.fixture
def fixture_csv(tmp_path) -> Path:
    return _write(tmp_path / "fixture.csv", FIXTURE_CSV_ROWS)


@pytest.fixture
def stop_condition_csv(tmp_path) -> Path:
    return _write(tmp_path / "stop_condition.csv", STOP_CONDITION_CSV_ROWS)


@pytest.fixture
def db_path(tmp_path, monkeypatch) -> Path:
    """Isolated SQLite DB for the test (config reads PIPELINE_DB at import)."""
    database = tmp_path / "test_warehouse.sqlite"
    monkeypatch.setenv("PIPELINE_DB", str(database))
    return database


def run_pipeline_cli(args: list[str]) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    return subprocess.run(
        [sys.executable, "-m", "pipeline", *args],
        cwd=str(PROJECT_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )


def run_full_pipeline(csv_path: Path, db_file: Path) -> subprocess.CompletedProcess:
    return run_pipeline_cli(["run", "--csv", str(csv_path), "--db", str(db_file)])


def open_db(db_file: Path):
    import sqlite3

    conn = sqlite3.connect(str(db_file))
    conn.row_factory = sqlite3.Row
    return conn
