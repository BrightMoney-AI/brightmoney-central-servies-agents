WITH summary AS (
    SELECT state, COUNT(DISTINCT item_pid) AS item_counts
    FROM uaa_db.plaid_force_refresh_metadata
    WHERE run_date = CURRENT_DATE
    GROUP BY state
    UNION ALL
    SELECT 'Total' AS state, COUNT(DISTINCT item_pid) AS item_counts
    FROM uaa_db.plaid_force_refresh_metadata
    WHERE run_date = CURRENT_DATE
),
totals AS (
    SELECT
        MAX(CASE WHEN state = 'Total'                            THEN item_counts END) AS total_items,
        MAX(CASE WHEN state = 'NOT_FOUND_IN_PLAID_METADATA'      THEN item_counts END) AS not_found,
        MAX(CASE WHEN state = 'ELIGIBLE_FOR_FORCE_REFRESH'       THEN item_counts END) AS eligible,
        MAX(CASE WHEN state = 'REJECTED_DUE_TO_RECENCY'          THEN item_counts END) AS rejected_recency,
        MAX(CASE WHEN state = 'REJECTED_DUE_TO_NULL_LAST_UPDATE' THEN item_counts END) AS rejected_null
    FROM summary
),
success_items AS (
    SELECT DISTINCT item_pid
    FROM uaa_db.plaid_force_refresh_data
    WHERE CAST(from_iso8601_timestamp(bright_updated_at) AS DATE) = CURRENT_DATE
),
error_items AS (
    SELECT DISTINCT item_pid
    FROM uaa_db.plaid_force_refresh_error_data
    WHERE CAST(from_iso8601_timestamp(bright_updated_at) AS DATE) = CURRENT_DATE
),
unique_errors AS (
    SELECT item_pid FROM error_items
    WHERE item_pid NOT IN (SELECT item_pid FROM success_items)
)
SELECT 'Total Items' AS metric,
       total_items   AS count,
       CAST(NULL AS DOUBLE) AS percentage
FROM totals
UNION ALL
SELECT 'Rejected (Recency + Null Last Update)',
       (COALESCE(rejected_recency, 0) + COALESCE(rejected_null, 0)),
       ROUND(((COALESCE(rejected_recency, 0) + COALESCE(rejected_null, 0)) * 100.0)
             / NULLIF(total_items - COALESCE(not_found, 0), 0), 2)
FROM totals
UNION ALL
SELECT 'Eligible for Force Refresh',
       eligible,
       ROUND((COALESCE(eligible, 0) * 100.0) / NULLIF(total_items - COALESCE(not_found, 0), 0), 2)
FROM totals
UNION ALL
SELECT 'Success',
       COUNT(DISTINCT item_pid),
       ROUND((COUNT(DISTINCT item_pid) * 100.0) / NULLIF((SELECT eligible FROM totals), 0), 2)
FROM success_items
UNION ALL
SELECT 'Error (unique failures)',
       COUNT(DISTINCT item_pid),
       ROUND((COUNT(DISTINCT item_pid) * 100.0) / NULLIF((SELECT eligible FROM totals), 0), 2)
FROM unique_errors
ORDER BY CASE metric
    WHEN 'Total Items'                            THEN 1
    WHEN 'Rejected (Recency + Null Last Update)'  THEN 2
    WHEN 'Eligible for Force Refresh'             THEN 3
    WHEN 'Success'                                THEN 4
    ELSE 5
END
