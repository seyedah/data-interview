-- ============================================================
-- cleaning.sql — Part 3: Data Cleaning & Quarantine (raw → stg)
--
-- Executed by pipeline/clean.py, which splits this file on the
-- "-- >>>" marker lines and runs each statement with bound
-- parameters: :batch_id, :median_bytes, :rejected_at
--
-- SQLite helper functions registered in pipeline/db.py:
--   parse_ts_ok(s) -> 1 if s is exactly 'YYYY-MM-DD HH:MM:SS' and a
--                     real calendar date, else 0
--   ts_in_2024(s)  -> 1 if s parses and is within
--                     2024-01-01 00:00:00 .. 2024-12-31 23:59:59, else 0
--   valid_ipv4(s)  -> 1 if s is a valid dotted-quad IPv4, else 0
--   strict_int(s)  -> INTEGER or NULL (strict integer parse)
--
-- Rejection-reason priority (a row gets exactly ONE reason):
--   1. INVALID_TIMESTAMP       5. INVALID_PORT
--   2. TIMESTAMP_OUT_OF_RANGE  6. NEGATIVE_BYTES
--   3. MISSING_REQUIRED_FIELD  7. DUPLICATE_RECORD (valid rows only)
--   4. INVALID_IP
-- ============================================================

-- >>> QUARANTINE_INVALID_ROWS
INSERT INTO rejected_network_logs
    (batch_id, source_record_id, rejection_reason, rejected_at, original_record)
WITH enriched AS (
    SELECT
        r.source_record_id,
        r.timestamp, r.src_ip, r.dst_ip, r.port, r.bytes,   -- untouched originals (JSON payload)
        TRIM(COALESCE(r.timestamp,    '')) AS ts_raw,
        TRIM(COALESCE(r.src_ip,       '')) AS src_raw,
        TRIM(COALESCE(r.dst_ip,       '')) AS dst_raw,
        TRIM(COALESCE(r.port,         '')) AS port_raw,
        TRIM(COALESCE(r.bytes,        '')) AS bytes_raw,
        TRIM(COALESCE(r.protocol,     '')) AS protocol_raw,
        TRIM(COALESCE(r.status,       '')) AS status_raw,
        COALESCE(r.label,        '') AS label_raw,
        COALESCE(r.anomaly_type, '') AS anomaly_raw
    FROM raw_network_logs r
    WHERE r.batch_id = :batch_id
),
classified AS (
    SELECT
        e.*,
        CASE
            WHEN e.ts_raw = '' OR parse_ts_ok(e.ts_raw) = 0
                THEN 'INVALID_TIMESTAMP'
            WHEN ts_in_2024(e.ts_raw) = 0
                THEN 'TIMESTAMP_OUT_OF_RANGE'
            WHEN e.src_raw = '' OR e.dst_raw = ''
                THEN 'MISSING_REQUIRED_FIELD'
            WHEN valid_ipv4(e.src_raw) = 0 OR valid_ipv4(e.dst_raw) = 0
                THEN 'INVALID_IP'
            WHEN strict_int(e.port_raw) IS NULL
              OR strict_int(e.port_raw) < 1
              OR strict_int(e.port_raw) > 65535
                THEN 'INVALID_PORT'
            WHEN strict_int(e.bytes_raw) IS NOT NULL
             AND strict_int(e.bytes_raw) < 0
                THEN 'NEGATIVE_BYTES'
            ELSE NULL          -- row passes all validity checks
        END AS rejection_reason
    FROM enriched e
)
SELECT
    :batch_id,
    c.source_record_id,
    c.rejection_reason,
    :rejected_at,
    json_object(
        'source_record_id', c.source_record_id,
        'timestamp', c.timestamp, 'src_ip', c.src_ip, 'dst_ip', c.dst_ip,
        'port', c.port, 'bytes', c.bytes, 'protocol', c.protocol_raw,
        'status', c.status_raw, 'label', c.label_raw, 'anomaly_type', c.anomaly_raw
    )
FROM classified c
WHERE c.rejection_reason IS NOT NULL;

-- >>> QUARANTINE_DUPLICATE_ROWS
-- Valid rows whose composite key (timestamp+src_ip+dst_ip+port) already
-- appeared earlier in the file (dedup_rank > 1). The kept row is the first
-- occurrence (lowest source_record_id) — deterministic and repeatable.
INSERT INTO rejected_network_logs
    (batch_id, source_record_id, rejection_reason, rejected_at, original_record)
WITH enriched AS (
    SELECT
        r.source_record_id,
        r.timestamp, r.src_ip, r.dst_ip, r.port, r.bytes,   -- untouched originals (JSON payload)
        TRIM(COALESCE(r.timestamp,    '')) AS ts_raw,
        TRIM(COALESCE(r.src_ip,       '')) AS src_raw,
        TRIM(COALESCE(r.dst_ip,       '')) AS dst_raw,
        TRIM(COALESCE(r.port,         '')) AS port_raw,
        TRIM(COALESCE(r.bytes,        '')) AS bytes_raw,
        TRIM(COALESCE(r.protocol,     '')) AS protocol_raw,
        TRIM(COALESCE(r.status,       '')) AS status_raw,
        COALESCE(r.label,        '') AS label_raw,
        COALESCE(r.anomaly_type, '') AS anomaly_raw
    FROM raw_network_logs r
    WHERE r.batch_id = :batch_id
),
classified AS (
    SELECT
        e.*,
        CASE
            WHEN e.ts_raw = '' OR parse_ts_ok(e.ts_raw) = 0
                THEN 'INVALID_TIMESTAMP'
            WHEN ts_in_2024(e.ts_raw) = 0
                THEN 'TIMESTAMP_OUT_OF_RANGE'
            WHEN e.src_raw = '' OR e.dst_raw = ''
                THEN 'MISSING_REQUIRED_FIELD'
            WHEN valid_ipv4(e.src_raw) = 0 OR valid_ipv4(e.dst_raw) = 0
                THEN 'INVALID_IP'
            WHEN strict_int(e.port_raw) IS NULL
              OR strict_int(e.port_raw) < 1
              OR strict_int(e.port_raw) > 65535
                THEN 'INVALID_PORT'
            WHEN strict_int(e.bytes_raw) IS NOT NULL
             AND strict_int(e.bytes_raw) < 0
                THEN 'NEGATIVE_BYTES'
            ELSE NULL          -- row passes all validity checks
        END AS rejection_reason
    FROM enriched e
),
valid_dedup AS (
    SELECT
        c.*,
        ROW_NUMBER() OVER (
            PARTITION BY c.ts_raw, c.src_raw, c.dst_raw, strict_int(c.port_raw)
            ORDER BY c.source_record_id
        ) AS dedup_rank
    FROM classified c
    WHERE c.rejection_reason IS NULL
)
SELECT
    :batch_id,
    v.source_record_id,
    'DUPLICATE_RECORD',
    :rejected_at,
    json_object(
        'source_record_id', v.source_record_id,
        'timestamp', v.timestamp, 'src_ip', v.src_ip, 'dst_ip', v.dst_ip,
        'port', v.port, 'bytes', v.bytes, 'protocol', v.protocol_raw,
        'status', v.status_raw, 'label', v.label_raw, 'anomaly_type', v.anomaly_raw
    )
FROM valid_dedup v
WHERE v.dedup_rank > 1;

-- >>> BUILD_STG
INSERT INTO stg_network_logs
    (batch_id, source_record_id, event_timestamp, src_ip, dst_ip,
     port, bytes, bytes_imputed, protocol, status)
WITH enriched AS (
    SELECT
        r.source_record_id,
        TRIM(COALESCE(r.timestamp, '')) AS ts_raw,
        TRIM(COALESCE(r.src_ip,    '')) AS src_raw,
        TRIM(COALESCE(r.dst_ip,    '')) AS dst_raw,
        TRIM(COALESCE(r.port,      '')) AS port_raw,
        TRIM(COALESCE(r.bytes,     '')) AS bytes_raw,
        TRIM(COALESCE(r.protocol,  '')) AS protocol_raw,
        TRIM(COALESCE(r.status,    '')) AS status_raw
    FROM raw_network_logs r
    WHERE r.batch_id = :batch_id
),
classified AS (
    SELECT
        e.*,
        CASE
            WHEN e.ts_raw = '' OR parse_ts_ok(e.ts_raw) = 0
                THEN 'INVALID_TIMESTAMP'
            WHEN ts_in_2024(e.ts_raw) = 0
                THEN 'TIMESTAMP_OUT_OF_RANGE'
            WHEN e.src_raw = '' OR e.dst_raw = ''
                THEN 'MISSING_REQUIRED_FIELD'
            WHEN valid_ipv4(e.src_raw) = 0 OR valid_ipv4(e.dst_raw) = 0
                THEN 'INVALID_IP'
            WHEN strict_int(e.port_raw) IS NULL
              OR strict_int(e.port_raw) < 1
              OR strict_int(e.port_raw) > 65535
                THEN 'INVALID_PORT'
            WHEN strict_int(e.bytes_raw) IS NOT NULL
             AND strict_int(e.bytes_raw) < 0
                THEN 'NEGATIVE_BYTES'
            ELSE NULL
        END AS rejection_reason
    FROM enriched e
),
valid_dedup AS (
    -- Deduplicate VALID rows only: keep the first occurrence in file order
    -- (lowest source_record_id) per composite key — deterministic & repeatable.
    SELECT
        c.*,
        ROW_NUMBER() OVER (
            PARTITION BY c.ts_raw, c.src_raw, c.dst_raw, strict_int(c.port_raw)
            ORDER BY c.source_record_id
        ) AS dedup_rank
    FROM classified c
    WHERE c.rejection_reason IS NULL
)
SELECT
    :batch_id,
    v.source_record_id,
    v.ts_raw,
    v.src_raw,
    v.dst_raw,
    strict_int(v.port_raw),
    COALESCE(strict_int(v.bytes_raw), :median_bytes),   -- impute missing bytes with batch median
    CASE WHEN strict_int(v.bytes_raw) IS NULL THEN 1 ELSE 0 END,
    CASE WHEN v.protocol_raw = '' THEN NULL ELSE v.protocol_raw END,
    CASE WHEN v.status_raw  = '' THEN 'UNKNOWN' ELSE v.status_raw END
FROM valid_dedup v
WHERE v.dedup_rank = 1;
