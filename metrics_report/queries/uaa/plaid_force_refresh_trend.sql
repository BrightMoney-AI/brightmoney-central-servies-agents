WITH error_raw AS (
    SELECT
        CAST(from_iso8601_timestamp(bright_updated_at) AS DATE) AS bright_date,
        item_pid
    FROM uaa_db.plaid_force_refresh_error_data
    WHERE CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) >= CURRENT_DATE - INTERVAL '7' DAY
      AND CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) >= TIMESTAMP '2025-01-09'
),
success_raw AS (
    SELECT
        CAST(from_iso8601_timestamp(bright_updated_at) AS DATE) AS bright_date,
        item_pid
    FROM uaa_db.plaid_force_refresh_data
    WHERE CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) >= CURRENT_DATE - INTERVAL '7' DAY
      AND CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) >= TIMESTAMP '2025-01-09'
),
unique_errors AS (
    SELECT bright_date, COUNT(DISTINCT item_pid) AS error_count
    FROM error_raw
    WHERE item_pid NOT IN (SELECT item_pid FROM success_raw)
    GROUP BY bright_date
),
unique_success AS (
    SELECT bright_date, COUNT(DISTINCT item_pid) AS success_count
    FROM success_raw
    GROUP BY bright_date
),
eligible AS (
    SELECT run_date AS bright_date, COUNT(DISTINCT item_pid) AS total_count
    FROM uaa_db.plaid_force_refresh_metadata
    WHERE run_date >= CURRENT_DATE - INTERVAL '7' DAY
      AND run_date >= DATE '2025-01-09'
      AND state = 'ELIGIBLE_FOR_FORCE_REFRESH'
    GROUP BY run_date
)
SELECT
    e.bright_date,
    ROUND((COALESCE(us.success_count, 0) * 100.0) / NULLIF(e.total_count, 0), 2) AS success_pct,
    ROUND((COALESCE(ue.error_count,   0) * 100.0) / NULLIF(e.total_count, 0), 2) AS error_pct
FROM eligible e
LEFT JOIN unique_errors  ue ON e.bright_date = ue.bright_date
LEFT JOIN unique_success us ON e.bright_date = us.bright_date
ORDER BY e.bright_date DESC
