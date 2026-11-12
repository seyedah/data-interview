# Candidate Task Specification: Network Logs Data Pipeline

## Overview

This document specifies the complete requirements for the Task-home assignment. You are expected to implement a data pipeline that processes network logs from a CSV file, validates and cleans the data, aggregates it, and stores it in a database with proper idempotency guarantees.

---

## 1. Project Goal

Build a **reliable, maintainable Data Pipeline** for processing and analyzing network logs. The pipeline must follow the architecture:

```
CSV → Raw → Staging/Clean → Analytics Mart
```

**Key Focus Areas:**
- ETL Pipeline design & implementation
- Python for data processing
- SQL for data transformation
- Data Quality & Validation
- Database Design & Data Modeling
- Idempotency & re-run handling
- Logging & Error Handling
- Testing & Documentation

---

## 2. Input Dataset

**File:** `network_logs_dirty.csv` (provided by hiring team)
**Format:** CSV, UTF-8, Comma-delimited, UTC timezone
**Records:** 10,300 rows (includes ~300 duplicates)

**Actual Columns in File:**
| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | STRING | Event timestamp (UTC) |
| `src_ip` | STRING | Source IPv4 address |
| `dst_ip` | STRING | Destination IPv4 address |
| `port` | INTEGER | Destination port |
| `bytes` | INTEGER | Data volume transferred (bytes) |
| `protocol` | STRING | Communication protocol |
| `status` | STRING | Connection status |
| `label` | STRING | Traffic classification (`benign`, `malicious`) |
| `anomaly_type` | STRING | Anomaly type if malicious (`port_scan`, `ddos`, `brute_force`, `data_exfil`, empty) |

**Known Data Quality Issues (Injected):**

| Issue | Approx. Count | Rate | Examples |
|-------|---------------|------|----------|
| Duplicate records (by composite key) | ~288 | ~2.8% | Exact copies of valid rows |
| Invalid timestamps | ~190 | ~1.8% | `not-a-timestamp`, `2024-02-30 12:00:00`, `2024/01/01 12:00:00`, `01-01-2024 12:00:00`, empty |
| Out-of-range timestamps | ~100 | ~1.0% | `2019-05-16`, `2021-01-09`, `2022-02-02`, `2029-01-01`, `2030-05-01` |
| Missing `src_ip` / `dst_ip` | ~166 | ~1.6% | Empty, `not.an.ip`, `192.168.1` (incomplete) |
| Invalid IP format | ~166 | ~1.6% | `999.999.999.999`, `300.200.100.50`, `192.168.1.256` |
| Invalid port | ~125 | ~1.2% | `-1`, `0`, `65536`, `99999`, `abc`, empty |
| Negative `bytes` | ~50 | ~0.5% | `-7461`, `-359`, `-5290`, `-9776`, `-4166` |
| NULL/empty `status` | ~234 | ~2.3% | Empty string |
| Missing `bytes` | ~454 (total across cols) | ~4.4% | Empty string |

> **Important**: Total invalid + out-of-range timestamps = **~290 (2.8%)** — **below the 5% pipeline stop threshold**.

**Data Contract Rules:**
1. Timestamps interpreted as UTC
2. **Allowed timestamp range: 2024-01-01 00:00:00 → 2024-12-31 23:59:59 (UTC)** — see `DATA_DICTIONARY.md`
3. `timestamp + src_ip + dst_ip + port` = composite key for deduplication
4. `bytes` can be NULL but not negative
5. Empty `status` handled per cleaning policy (set to `UNKNOWN`)
6. Records with missing/invalid required fields (`timestamp`, `src_ip`, `dst_ip`, `port`) → Quarantine
7. `label` and `anomaly_type` preserved in Raw layer only

---

## 3. Implementation Requirements

### Part 1: Data Ingestion
- Read CSV file and load into database
- Store raw data in `raw_network_logs` table (unchanged)
- Track record count per batch
- Handle file read errors gracefully with logging
- **Batch Management:** Each run gets a `batch_id` (use file hash)
- Re-running same file must NOT create duplicate batch in Raw layer

### Part 2: Data Quality Validation
Perform and log these checks:

| Check | Description | Expected Count (approx.) |
|-------|-------------|--------------------------|
| Total Records | Count of all input records | 10,300 |
| Duplicate Records | Count of duplicates (by composite key) | ~288 |
| Missing Timestamp | Count of NULL/empty timestamps | ~190 |
| Missing Bytes | Count of NULL/empty bytes | ~454 (total missing across cols) |
| Missing Status | Count of NULL/empty status | ~234 |
| Invalid Timestamp | Count of unparseable timestamps | ~190 |
| Future/Out-of-Range Timestamp | Count outside 2024-01-01 to 2024-12-31 | ~100 |
| Negative Bytes | Count of bytes < 0 | ~50 |
| Invalid IP | Count of malformed src_ip or dst_ip | ~166 |
| Invalid Port | Count of ports outside 1-65535 | ~125 |

**Duplicate Definition:** Same `timestamp + src_ip + dst_ip + port` → keep ONE (deterministic, repeatable criteria — e.g., first encountered or max bytes)

**Pipeline Stop Condition:** If >5% of records have invalid/missing timestamps → STOP pipeline  
*(Actual rate in provided file: ~2.8% — pipeline should NOT stop)*

### Part 3: Data Cleaning & Quarantine (via SQL)

**Cleaning Policy:**

| Data Issue | Action | Rejection Reason Code |
|------------|--------|----------------------|
| Invalid timestamp (unparseable) | Quarantine | `INVALID_TIMESTAMP` |
| Timestamp out of range (not in 2024) | Quarantine | `TIMESTAMP_OUT_OF_RANGE` |
| Missing `src_ip` or `dst_ip` (empty) | Quarantine | `MISSING_REQUIRED_FIELD` |
| Invalid IP format | Quarantine | `INVALID_IP` |
| Invalid port (not 1-65535) | Quarantine | `INVALID_PORT` |
| `bytes` = NULL/empty | Replace with **batch median** (non-negative) | — (not a rejection) |
| `bytes` < 0 | Quarantine | `NEGATIVE_BYTES` |
| `status` = NULL/empty | Set to `UNKNOWN` | — (not a rejection) |
| Duplicate (by composite key) | Keep 1 (deterministic), quarantine rest | `DUPLICATE_RECORD` |

**Quarantine Table:** `rejected_network_logs`
Required columns:
- `batch_id`
- `source_record_id` (input row number, 1-based)
- `rejection_reason` (one of: `INVALID_TIMESTAMP`, `TIMESTAMP_OUT_OF_RANGE`, `MISSING_REQUIRED_FIELD`, `INVALID_IP`, `INVALID_PORT`, `NEGATIVE_BYTES`, `DUPLICATE_RECORD`)
- `rejected_at` (timestamp of rejection)
- `original_record` (full record as JSON or CSV string)

**Clean Table:** `stg_network_logs`
- Re-running same batch must produce identical results
- Log computed median value for bytes imputation
- Must contain NO invalid timestamps, NO negative bytes, NO invalid IPs/ports
- `status` column: no NULLs (all `UNKNOWN` or valid values)

### Part 4: Daily Aggregation

**Table:** `mart_daily_network_stats`

| Column | Type | Description |
|--------|------|-------------|
| `event_date` | DATE | Event date (UTC, derived from `timestamp`) |
| `src_ip` | VARCHAR(45) | Source IP |
| `total_events` | BIGINT | Count of events for that IP on that day |
| `total_bytes` | BIGINT | Sum of `bytes` (NULLs ignored) |
| `avg_bytes` | DOUBLE PRECISION | Average of `bytes` (NULLs ignored) |
| `error_events` | BIGINT | Count where `status` IN (`TIMEOUT`, `RESET`) |

**Error Events:** `status` IN (`TIMEOUT`, `RESET`) — `UNKNOWN` is **NOT** an error event

**Requirements:**
- Date based on UTC (`DATE(timestamp AT TIME ZONE 'UTC')` or equivalent)
- NULL `bytes` must not break SUM/AVG (use `COALESCE` or natural NULL handling)
- SQL-based computation (single query or CTE)
- Unique constraint on (`event_date`, `src_ip`) for idempotency

### Part 5: Database & Idempotency

**Database:** PostgreSQL (preferred), ClickHouse, or SQLite
- If SQLite: Document limitations/trade-offs in README

**Idempotency Requirements:**

| Layer | Table | Unique Key / Upsert Strategy |
|-------|-------|------------------------------|
| Raw | `raw_network_logs` | `batch_id` (file hash) — prevent re-insert of same batch |
| Staging | `stg_network_logs` | `batch_id` + composite key (`timestamp`, `src_ip`, `dst_ip`, `port`) — re-run produces identical rows |
| Quarantine | `rejected_network_logs` | `batch_id` + `source_record_id` — no duplicate quarantine rows |
| Analytics Mart | `mart_daily_network_stats` | `event_date` + `src_ip` — UPSERT on re-run |

**Allowed Techniques:** UPSERT (`ON CONFLICT`), DELETE+INSERT in transaction, Unique Constraints
- Document chosen approach in README

### Part 6: Pipeline Orchestration

Implement **one** of:
- Python CLI (recommended)
- Makefile
- Apache Airflow (bonus)

**Runnable Steps:**
1. `ingest`
2. `validate`
3. `clean`
4. `aggregate`
5. `load`
6. `test`

Steps must be independently executable.

### Part 7: Logging & Monitoring

**Per Step, Log:**
- Start/end timestamps
- Step name
- `batch_id`
- Input record count
- Output record count
- Rejected/quarantined count
- Success/failure status
- Error details (if failed)

**Warning Threshold (Post-Cleaning):**
```
if clean_records / raw_records < 0.5:
    LOG WARNING
```
- Warning only, does NOT stop pipeline
- Independent of timestamp stop condition

### Part 8: Analytical SQL Queries

Write SQL for:

**Query 1: Top Error Sources**
- Top 5 `src_ip` by `total_error_events`
- Output: `src_ip`, `total_error_events`

**Query 2: Daily Traffic Ranking**
- Window function (`RANK()` or `DENSE_RANK()`)
- Rank each IP by `total_bytes` per day
- Output: `event_date`, `src_ip`, `total_bytes`, `daily_rank`

**Query 3: Weekly Trend (Optional - Bonus)**
- 7-day moving average of event count
- Extra credit

### Part 9: Testing

Minimum 4 tests (pytest recommended):

1. **No duplicates** in Analytics Mart by composite key
2. **No invalid/out-of-range timestamps** in Analytics Mart
3. **No negative bytes** in Clean or Analytics Mart
4. **Re-run idempotency**: Running pipeline twice for same batch creates no duplicates in any layer

---

## 4. Required Deliverables

Your Git repository must contain:

### 1. Source Code
- Python and SQL files implementing the pipeline

### 2. README.md
Must include:
- Project description
- Prerequisites
- Dependency installation
- Database setup instructions
- Pipeline execution command
- How to re-run a batch
- Technical assumptions & decisions
- Test execution instructions

### 3. SQL Files
- Schema creation (`schema.sql`)
- Cleaning logic (`cleaning.sql`)
- Aggregation logic (`aggregation.sql`)
- Analytical queries (`analytics.sql`)

### 4. DESIGN.md (Max 1 page)
Answer these three questions:
1. **Scaling to billions of records:** What architectural changes?
2. **Streaming data:** What changes needed?
3. **Late-arriving data:** How to handle?

### 5. Tests
- Test files verifying pipeline correctness

### 6. Additional Files
- `DATA_DICTIONARY.md` **(provided — see repository)**
- `requirements.txt`
- `Makefile` (if used)

---

## 5. Acceptance Criteria

Your implementation passes if:

- [ ] Pipeline executes successfully end-to-end
- [ ] Raw data is recoverable and traceable
- [ ] Invalid records captured in Quarantine
- [ ] Clean table has no invalid timestamps or negative bytes
- [ ] Daily aggregation matches specified logic
- [ ] Re-running batch produces no duplicate data in any layer
- [ ] Data Quality logs and reports are visible
- [ ] Analytical queries return correct results
- [ ] Documentation enables setup and execution

---

## 6. Evaluation Rubric (100 Points)

| Criterion | Points |
|-----------|--------|
| Data Pipeline Correctness & Structure | 20 |
| SQL Quality & Data Transformation | 20 |
| Data Quality & Validation | 15 |
| Idempotency & Re-run Handling | 15 |
| Database Design | 10 |
| Logging & Error Handling | 10 |
| Testing & Documentation | 5 |
| Technical Explanation & Code Ownership | 5 |

**Minimum Thresholds (must score ≥50% on each):**
- Data Pipeline Correctness
- SQL & Data Transformation
- Idempotency

**Overall Result:**
- 80-100: Excellent
- 70-79: Acceptable
- 60-69: Borderline
- <60: Not Accepted

---

## 7. Technical Session (Post-Submission)

30-minute technical review:
- 5 min: Architecture & execution walkthrough
- 8 min: Code walkthrough (ingest → load)
- 7 min: SQL, Data Quality, Idempotency deep-dive
- 5 min: Change scenario discussion
- 5 min: Q&A

**Potential Scenarios:**
- Late-arriving historical data
- Mid-pipeline failure recovery
- 100x data volume increase
- Input schema changes

---

## 8. Important Notes

- **Simple, understandable tools preferred** — No requirement for Airflow, Docker, etc.
- **Code quality, correctness, and reasoning > tool count**
- **Not expected to be production-ready**
- **AI assistance allowed** but you must understand and explain all code
- **Document rationale** for any external libraries/tools used

---

## 9. Submission

- Submit Git repository URL
- Submit at least **1 business day before** technical session
- Ensure `network_logs_dirty.csv` is NOT in repo (hiring team provides it)

---

**Good Luck!** 🚀