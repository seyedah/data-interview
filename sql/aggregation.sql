-- ============================================================
-- aggregation.sql — Part 4: Daily Aggregation
--
-- Executed by pipeline/aggregate.py and pipeline/load.py.
-- Split on "-- >>>" markers.
--
-- The mart is a GLOBAL table keyed by (event_date, src_ip): the
-- aggregation always recomputes from the FULL clean layer (all
-- batches), so totals stay correct if several batches are ingested,
-- and re-running any batch is idempotent (UPSERT overwrites the
-- same values).
-- ============================================================

-- >>> REBUILD_MART_CALC
DROP TABLE IF EXISTS mart_calc;
CREATE TABLE mart_calc AS
SELECT
    DATE(event_timestamp)                                    AS event_date,   -- UTC date part
    src_ip,
    COUNT(*)                                                 AS total_events,
    COALESCE(SUM(bytes), 0)                                  AS total_bytes,  -- SUM ignores NULLs
    AVG(CAST(bytes AS REAL))                                 AS avg_bytes,    -- AVG ignores NULLs
    COALESCE(SUM(CASE WHEN status IN ('TIMEOUT', 'RESET')
                      THEN 1 ELSE 0 END), 0)                 AS error_events  -- UNKNOWN is NOT an error
FROM stg_network_logs
GROUP BY DATE(event_timestamp), src_ip;

-- >>> UPSERT_MART
INSERT INTO mart_daily_network_stats
    (event_date, src_ip, total_events, total_bytes, avg_bytes, error_events, updated_at)
SELECT event_date, src_ip, total_events, total_bytes, avg_bytes, error_events, :updated_at
FROM mart_calc
WHERE true                                                  -- guards ON CONFLICT parsing
ON CONFLICT (event_date, src_ip) DO UPDATE SET
    total_events = excluded.total_events,
    total_bytes  = excluded.total_bytes,
    avg_bytes    = excluded.avg_bytes,
    error_events = excluded.error_events,
    updated_at   = excluded.updated_at;
