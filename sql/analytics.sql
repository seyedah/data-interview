-- ============================================================
-- analytics.sql — Part 8: Analytical Queries
-- Executed by pipeline/analytics.py (results saved to reports/).
-- ============================================================

-- >>> QUERY1_TOP_ERROR_SOURCES
-- Top 5 source IPs by total error events (TIMEOUT + RESET).
SELECT src_ip, SUM(error_events) AS total_error_events
FROM mart_daily_network_stats
GROUP BY src_ip
ORDER BY total_error_events DESC, src_ip ASC
LIMIT 5;

-- >>> QUERY2_DAILY_TRAFFIC_RANKING
-- Rank each source IP by total bytes per day (window function).
SELECT
    event_date,
    src_ip,
    total_bytes,
    RANK() OVER (PARTITION BY event_date ORDER BY total_bytes DESC) AS daily_rank
FROM mart_daily_network_stats
ORDER BY event_date ASC, daily_rank ASC, src_ip ASC;

-- >>> QUERY3_WEEKLY_TREND_BONUS
-- 7-day moving average of total daily event count (bonus query).
SELECT
    event_date,
    SUM(total_events) AS daily_total_events,
    ROUND(AVG(SUM(total_events)) OVER (
        ORDER BY event_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS moving_avg_events_7d
FROM mart_daily_network_stats
GROUP BY event_date
ORDER BY event_date ASC;
