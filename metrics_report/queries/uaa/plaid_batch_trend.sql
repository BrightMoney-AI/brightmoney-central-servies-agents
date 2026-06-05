WITH error_data AS (
    SELECT
        CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) AS ts,
        'error' AS status
    FROM uaa_db.plaid_batch_refresh_error_data
    WHERE CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) >= CURRENT_DATE - INTERVAL '1' DAY
),
success_data AS (
    SELECT
        CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) AS ts,
        'success' AS status
    FROM uaa_db.plaid_batch_refresh_data
    WHERE CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) >= CURRENT_DATE - INTERVAL '1' DAY
),
combined AS (
    SELECT * FROM success_data
    UNION ALL
    SELECT * FROM error_data
)
SELECT
    date_trunc('hour', ts)                                                            AS metric_hour,
    ROUND(100.0 * SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) / COUNT(*), 2) AS success_pct,
    ROUND(100.0 * SUM(CASE WHEN status = 'error'   THEN 1 ELSE 0 END) / COUNT(*), 2) AS error_pct
FROM combined
GROUP BY date_trunc('hour', ts)
ORDER BY metric_hour DESC
