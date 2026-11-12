# Network Logs Data Pipeline

A reliable, idempotent ETL pipeline that ingests a dirty network-logs CSV,
validates and cleans it, quarantines bad records, and builds a daily
analytics mart.

```
CSV  ->  raw_network_logs  ->  stg_network_logs  ->  mart_daily_network_stats
              |                                              ^
              +--> rejected_network_logs (quarantine)        |
                                                   mart_calc (intermediate)
```

Everything about the input data, its known quality issues and the cleaning
policy is described in [`DATA_DICTIONARY.md`](DATA_DICTIONARY.md) and
[`CANDIDATE_TASK_SPEC.md`](CANDIDATE_TASK_SPEC.md). Design rationale for
scale/streaming/late-data is in [`DESIGN.md`](DESIGN.md).

---

## 1. Results on the provided dataset

One full run (`make run`) on `network_logs_dirty.csv` (10,300 rows):

| Layer | Count | Notes |
|---|---|---|
| `raw_network_logs` | 10,300 | faithful copy, nothing dropped |
| `stg_network_logs` (clean) | 9,382 | 91.1% clean/raw ratio |
| `rejected_network_logs` | 918 | see breakdown below |
| `mart_daily_network_stats` | 9,382 | (event_date, src_ip) groups |

Quarantine breakdown: `DUPLICATE_RECORD` 287, `INVALID_TIMESTAMP` 190,
`INVALID_PORT` 125, `TIMESTAMP_OUT_OF_RANGE` 100, `INVALID_IP` 85,
`MISSING_REQUIRED_FIELD` 81, `NEGATIVE_BYTES` 50.

Checks logged by `validate` (stored in `dq_check_results`): 288 duplicate
rows, 62 missing + 128 unparseable + 100 out-of-range timestamps = 290
(2.82%) — below the 5% stop threshold, so the pipeline continues. 35 empty
`bytes` values were imputed with the batch median 5,175,860 (logged), 232
empty `status` values became `UNKNOWN`. Raw = clean + quarantined exactly
(10,300 = 9,382 + 918).

---

## 2. Prerequisites

- Python **3.10+** (developed and tested on 3.14)
- That's it. The pipeline uses **only the Python standard library**
  (`csv`, `sqlite3`, `hashlib`, `argparse`, `logging`, `re`, `ipaddress`,
  `json`) — no runtime dependencies. `pytest` is needed only for tests.
- The bundled Python `sqlite3` module is used (requires SQLite ≥ 3.35 for
  window functions / `ON CONFLICT`, which every Python ≥ 3.10 build ships).

### Why SQLite?

PostgreSQL was preferred by the spec but is not available in this
environment, and the assignment explicitly allows SQLite if the trade-offs
are documented. **Trade-offs / limitations:**

| Limitation | Mitigation here |
|---|---|
| Single writer, no concurrency | fine for file-batch ETL; WAL mode enabled |
| No native `DATE`/`TIMESTAMP` types | ISO-8601 strings (`'YYYY-MM-DD HH:MM:SS'`); lexicographic comparisons are correct for this format |
| No `PERCENTILE_CONT` / median | median computed in Python (`pipeline/clean.py`), passed into the SQL as a parameter |
| No strict datetime/IP parsing in SQL | small deterministic UDFs (`parse_ts_ok`, `ts_in_2024`, `valid_ipv4`, `strict_int`) registered in `pipeline/db.py`; with PostgreSQL these become `::timestamptz` casts and the `inet`/`::int` casts |
| `DOUBLE PRECISION` → `REAL` | same semantics for this data |
| UDF/SQL dialect is SQLite-specific | all business rules live in `sql/*.sql`; only the 4 parsing helpers are Python — the porting surface is deliberately tiny |

---

## 3. Setup

```bash
git clone https://github.com/seyedah/data-interview
cd data-interview

# optional but recommended
make setup                 # creates .venv and installs requirements.txt
source .venv/bin/activate  # or rely on the Makefile's automatic .venv detection
```

Place the input file `network_logs_dirty.csv` (provided by the hiring team)
in the project root, or point `--csv`/`CSV=` at it. It is intentionally not
committed (see `.gitignore`).

The database is created automatically on first run
(`data/warehouse.sqlite`, configurable). No manual DB setup is needed;
`sql/schema.sql` is applied idempotently (`CREATE TABLE IF NOT EXISTS`)
on every command.

---

## 4. Running the pipeline

### One command (recommended)

```bash
make run                       # = python -m pipeline run --csv network_logs_dirty.csv --db data/warehouse.sqlite
```

### Step by step (each step is independently executable)

```bash
make ingest      # CSV        -> raw_network_logs          (batch_id = sha256(file)[:16])
make validate    # DQ checks  -> dq_check_results          (stop condition enforced)
make clean       # raw        -> stg_network_logs + rejected_network_logs
make aggregate   # stg        -> mart_calc (daily aggregation)
make load        # mart_calc  -> mart_daily_network_stats  (UPSERT)
make analytics   # analytical queries -> reports/*.csv
make status      # batches + per-layer row counts
make test        # pytest suite
```

CLI equivalents and options:

```bash
python -m pipeline <command> [--csv PATH] [--db PATH] [-v]
python -m pipeline clean --db data/warehouse.sqlite --batch-id c9d80a0be9cb4006
```

- `--batch-id` targets a previously ingested batch; default is the latest batch.
- Exit codes: `0` success, `1` error, `2` pipeline stop condition triggered.
- Logs go to the console (INFO) and to `logs/pipeline.log` (DEBUG) with, per
  step: start/end time, step name, batch_id, input/output/rejected counts and
  success/failure/stop status. Step results are also stored in the
  `pipeline_run_log` table.

### Re-running a batch

Simply run the same command again:

```bash
make run        # second run with the same file
```

- `ingest` sees the same `batch_id` (SHA-256 of the file) and **skips**
  re-insertion — the raw layer never gets a second copy of the batch.
- `validate` recomputes and overwrites the DQ metrics (`INSERT ... ON CONFLICT`).
- `clean` rebuilds this batch's staging + quarantine rows with **DELETE+INSERT
  inside one transaction** — deterministic (first-occurrence dedup by row
  number), so re-runs produce identical rows.
- `load` **UPSERTs** the mart (`ON CONFLICT (event_date, src_ip) DO UPDATE`),
  recomputing from the full clean layer, so totals stay correct even if
  several batches exist.

Verified by `test_rerun_idempotency_no_duplicates_anywhere`: two full runs
produce identical row sets in all four layers and only one batch record.

### Idempotency keys (as required by the spec)

| Layer | Table | Key / strategy |
|---|---|---|
| Raw | `raw_network_logs` | `batch_id` (file hash) + skip-if-exists on the batch |
| Staging | `stg_network_logs` | PK `(batch_id, event_timestamp, src_ip, dst_ip, port)` + DELETE+INSERT re-run |
| Quarantine | `rejected_network_logs` | PK `(batch_id, source_record_id)` |
| Mart | `mart_daily_network_stats` | PK `(event_date, src_ip)` + `ON CONFLICT DO UPDATE` |

---

## 5. Project layout

```
├── pipeline/                  Python package (stdlib only)
│   ├── cli.py                 orchestration CLI (Part 6)
│   ├── config.py              constants, thresholds, env overrides
│   ├── db.py                  SQLite connection, UDFs, SQL loading, run-log
│   ├── steps.py               uniform step runner (start/end/count logging)
│   ├── ingest.py              Part 1: CSV -> raw
│   ├── validate.py            Part 2: DQ checks + stop condition
│   ├── clean.py               Part 3: median imputation + SQL execution
│   ├── aggregate.py           Part 4: stg -> mart_calc
│   ├── load.py                Part 4/5: mart_calc -> mart (UPSERT)
│   └── analytics.py           Part 8: analytical queries -> reports/
├── sql/
│   ├── schema.sql             DDL for all layers
│   ├── cleaning.sql           Part 3 business rules (quarantine + dedup + imputation)
│   ├── aggregation.sql        Part 4 aggregation + mart UPSERT
│   └── analytics.sql          Part 8 queries (top errors, ranking, 7d trend)
├── tests/                     pytest suite (8 tests)
├── reports/                   analytics CSV output (gitignored)
├── logs/                      pipeline.log (gitignored)
├── data/                      SQLite database (gitignored)
├── DATA_DICTIONARY.md         provided by the hiring team
├── CANDIDATE_TASK_SPEC.md     provided by the hiring team
├── DESIGN.md                  scaling / streaming / late-data answers
├── Makefile / requirements.txt / README.md
```

All business logic lives in the `sql/` files (single source of truth for the
SQL); Python orchestrates, parses strictly, and logs. `sql/cleaning.sql`,
`sql/aggregation.sql` and `sql/analytics.sql` are split on `-- >>>` markers
and executed with bound parameters by the corresponding modules.

---

## 6. Data quality & cleaning policy (summary)

Rejection reasons are checked in strict priority order, so each record gets
exactly **one** reason and lands in exactly one place (clean or quarantine):

| # | Condition | Action | Reason |
|---|---|---|---|
| 1 | timestamp empty/unparseable (`not-a-timestamp`, `2024-02-30`, `2024/01/01 …`) | quarantine | `INVALID_TIMESTAMP` |
| 2 | timestamp parses but outside 2024-01-01..2024-12-31 UTC | quarantine | `TIMESTAMP_OUT_OF_RANGE` |
| 3 | `src_ip` or `dst_ip` empty | quarantine | `MISSING_REQUIRED_FIELD` |
| 4 | IP present but not valid IPv4 (`999.999.999.999`, `192.168.1`) | quarantine | `INVALID_IP` |
| 5 | port not an integer in 1–65535 (`0`, `-1`, `70000`, `abc`, empty) | quarantine | `INVALID_PORT` |
| 6 | `bytes` < 0 | quarantine | `NEGATIVE_BYTES` |
| 7 | duplicate composite key (`timestamp+src_ip+dst_ip+port`) | keep **first occurrence** (lowest row number), quarantine the rest | `DUPLICATE_RECORD` |
| — | `bytes` empty | impute with **batch median** of non-negative values, flag `bytes_imputed=1` | — |
| — | `status` empty | set to `UNKNOWN` | — |

Notes:

- Duplicates are removed **only among valid rows**; the validation metric
  (`duplicate_records`) counts extra occurrences of a composite key among rows
  where all four key fields are present, so it may differ slightly from the
  final `DUPLICATE_RECORD` quarantine count (a duplicate row that also fails
  another check is quarantined with the higher-priority reason).
- The stop condition uses the share of records whose timestamp is missing,
  unparseable **or out-of-range** (`>5%` -> exit code 2, nothing downstream is
  produced). On the provided file it is 2.82%.
- `clean/raw < 50%` logs a **warning** (does not stop the pipeline); the ratio
  here is 91.1%.
- `label` / `anomaly_type` are preserved in `raw_network_logs` and in the
  quarantine JSON payload only — they never reach the clean layer or mart
  (data contract rule 7).

---

## 7. Analytics

`make analytics` (or `python -m pipeline analytics`) runs
[`sql/analytics.sql`](sql/analytics.sql) and writes CSV reports to `reports/`:

1. `query1_top_error_sources.csv` — top 5 `src_ip` by `SUM(error_events)`
2. `query2_daily_traffic_ranking.csv` — `RANK()` per `event_date` by `total_bytes`
3. `query3_weekly_trend_bonus.csv` — bonus: 7-day moving average of daily events

Error events are `status IN ('TIMEOUT','RESET')`; `UNKNOWN` is not an error.
NULL bytes never break `SUM`/`AVG` (they are ignored, and after imputation
they are non-NULL anyway).

---

## 8. Tests

```bash
make test          # or: python -m pytest -v
```

8 tests, all hermetic (temporary DB per test, deterministic fixture CSV that
exercises every rejection reason at a 3.2% bad-timestamp rate — below the stop
threshold):

1. `test_mart_has_no_duplicate_composite_keys` — no duplicate `(event_date, src_ip)` in the mart
2. `test_no_invalid_or_out_of_range_timestamps` — strict re-parse of every staging timestamp, mart dates inside 2024
3. `test_no_negative_bytes_in_clean_and_mart` — no negative/NULL bytes anywhere
4. `test_rerun_idempotency_no_duplicates_anywhere` — two full runs -> identical rows in all layers, one batch
5. `test_pipeline_stop_condition_on_excessive_bad_timestamps` — 60% bad timestamps -> exit code 2, no downstream rows
6. `test_quarantine_reasons_and_cleaning_policy` — all 7 reasons appear, raw = clean + rejected, median imputation, `UNKNOWN` status, no `label` leak
7. `test_real_file_end_to_end_smoke` — runs the real 10,300-row file twice (skipped if the CSV is absent)
8. `test_status_command_reports_batches` — observability command works

---

## 9. Assumptions & decisions

- **Database**: SQLite (stdlib, zero setup) instead of PostgreSQL — not
  available here; trade-offs documented in §2. The porting surface is four
  parsing UDFs; the DDL uses standard types except `TEXT` dates.
- **batch_id**: first 16 hex chars of the file's SHA-256 — stable, cheap,
  and guarantees "same file = same batch".
- **source_record_id**: 1-based data-row number in the CSV (header excluded);
  makes every raw row addressable and the quarantine traceable.
- **One rejection reason per row** (priority list in §6) — deterministic and
  keeps the quarantine PK `(batch_id, source_record_id)` natural. A record
  failing several checks appears in the per-issue DQ metrics of each failed
  check, but is quarantined once with the highest-priority reason.
- **Duplicate handling**: keep the first occurrence in file order (lowest
  `source_record_id`) — deterministic, repeatable, and simple to explain.
- **Median imputation**: median over all *raw* non-negative integer byte
  values of the batch (pre-dedup); half-values round half-up to keep `bytes`
  an integer; the value is logged and stored implicitly via `bytes_imputed`.
- **Mart is global**: aggregation reads the whole clean layer (all batches)
  and upserts, so the mart stays correct with multiple batches and is
  idempotent under any re-run order.
- **Empty `protocol`** becomes NULL (optional field); empty `status` becomes
  `UNKNOWN` per contract; values are otherwise stored as-is.
- **Port stored as INTEGER in clean layer**, raw text in raw layer; whitespace
  is trimmed before validation (defensive; the generator emits none).
- **Timestamps are UTC** by contract; no timezone conversion is performed —
  `DATE(timestamp)` takes the date part of the UTC string.

## 10. Roadmap if this went to production

See [`DESIGN.md`](DESIGN.md) — partitioned columnar warehouse for billions of
rows, streaming ingestion with watermarks, and late-arriving data handling.
