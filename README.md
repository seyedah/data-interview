## Setup

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

## Running the pipeline

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
