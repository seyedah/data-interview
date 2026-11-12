-- ============================================================
-- schema.sql — Database schema (SQLite)
-- Every layer enforces its idempotency key with a PRIMARY KEY /
-- UNIQUE constraint (see README, "Idempotency" section).
-- ============================================================

-- One row per ingested file. batch_id = first 16 hex chars of the file's SHA-256.
CREATE TABLE IF NOT EXISTS pipeline_batches (
    batch_id      TEXT PRIMARY KEY,
    source_file   TEXT NOT NULL,
    file_sha256   TEXT NOT NULL,
    record_count  INTEGER,
    ingested_at   TEXT NOT NULL,
    validated_at  TEXT,
    cleaned_at    TEXT,
    status        TEXT NOT NULL DEFAULT 'INGESTED'
                  CHECK (status IN ('INGESTED','VALIDATED','CLEANED','AGGREGATED','COMPLETED'))
);

-- Layer 1: RAW — faithful copy of the CSV (all columns TEXT, values untouched).
CREATE TABLE IF NOT EXISTS raw_network_logs (
    batch_id         TEXT NOT NULL REFERENCES pipeline_batches(batch_id),
    source_record_id INTEGER NOT NULL,               -- 1-based CSV data-row number
    timestamp        TEXT,
    src_ip           TEXT,
    dst_ip           TEXT,
    port             TEXT,                           -- raw text: may be 'abc', '-1', ''
    bytes            TEXT,                           -- raw text: may be '', '-5'
    protocol         TEXT,
    status           TEXT,
    label            TEXT,                           -- kept here only (data contract rule 7)
    anomaly_type     TEXT,                           -- kept here only (data contract rule 7)
    PRIMARY KEY (batch_id, source_record_id)
);

-- Data-quality check results per batch, written by the `validate` step.
CREATE TABLE IF NOT EXISTS dq_check_results (
    batch_id    TEXT NOT NULL,
    check_name  TEXT NOT NULL,
    check_value INTEGER NOT NULL,
    checked_at  TEXT NOT NULL,
    PRIMARY KEY (batch_id, check_name)
);

-- Layer 2: QUARANTINE — one row per rejected record (highest-priority reason).
CREATE TABLE IF NOT EXISTS rejected_network_logs (
    batch_id         TEXT NOT NULL REFERENCES pipeline_batches(batch_id),
    source_record_id INTEGER NOT NULL,
    rejection_reason TEXT NOT NULL CHECK (rejection_reason IN (
        'INVALID_TIMESTAMP','TIMESTAMP_OUT_OF_RANGE','MISSING_REQUIRED_FIELD',
        'INVALID_IP','INVALID_PORT','NEGATIVE_BYTES','DUPLICATE_RECORD')),
    rejected_at      TEXT NOT NULL,
    original_record  TEXT NOT NULL,                  -- full raw record as JSON
    PRIMARY KEY (batch_id, source_record_id)
);

-- Layer 3: CLEAN / STAGING.
CREATE TABLE IF NOT EXISTS stg_network_logs (
    batch_id         TEXT NOT NULL REFERENCES pipeline_batches(batch_id),
    source_record_id INTEGER NOT NULL,               -- row number of the KEPT record
    event_timestamp  TEXT NOT NULL,                  -- 'YYYY-MM-DD HH:MM:SS' (UTC)
    src_ip           TEXT NOT NULL,
    dst_ip           TEXT NOT NULL,
    port             INTEGER NOT NULL CHECK (port BETWEEN 1 AND 65535),
    bytes            INTEGER CHECK (bytes IS NULL OR bytes >= 0),
    bytes_imputed    INTEGER NOT NULL DEFAULT 0,     -- 1 = bytes came from batch-median imputation
    protocol         TEXT,
    status           TEXT NOT NULL,                  -- '' replaced with 'UNKNOWN'
    PRIMARY KEY (batch_id, event_timestamp, src_ip, dst_ip, port)
);

-- Internal aggregation result; rebuilt by `aggregate`, consumed by `load`.
CREATE TABLE IF NOT EXISTS mart_calc (
    event_date   TEXT NOT NULL,
    src_ip       TEXT NOT NULL,
    total_events INTEGER NOT NULL,
    total_bytes  INTEGER NOT NULL,
    avg_bytes    REAL,
    error_events INTEGER NOT NULL
);

-- Layer 4: ANALYTICS MART — UPSERT key: (event_date, src_ip).
CREATE TABLE IF NOT EXISTS mart_daily_network_stats (
    event_date   TEXT NOT NULL,                      -- UTC date
    src_ip       TEXT NOT NULL,
    total_events INTEGER NOT NULL CHECK (total_events > 0),
    total_bytes  INTEGER NOT NULL CHECK (total_bytes >= 0),
    avg_bytes    REAL,
    error_events INTEGER NOT NULL CHECK (error_events >= 0),
    updated_at   TEXT NOT NULL,
    PRIMARY KEY (event_date, src_ip)
);

-- Step-level run log (also written to logs/pipeline.log).
CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id       TEXT,
    step           TEXT NOT NULL,
    started_at     TEXT NOT NULL,
    finished_at    TEXT,
    status         TEXT NOT NULL,                    -- RUNNING / SUCCESS / FAILED / STOPPED
    input_count    INTEGER,
    output_count   INTEGER,
    rejected_count INTEGER,
    error_detail   TEXT
);

CREATE INDEX IF NOT EXISTS idx_raw_batch       ON raw_network_logs (batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_timestamp   ON stg_network_logs (event_timestamp);
CREATE INDEX IF NOT EXISTS idx_rejected_batch  ON rejected_network_logs (batch_id);
CREATE INDEX IF NOT EXISTS idx_mart_event_date ON mart_daily_network_stats (event_date);
