WITH error_data AS (
    SELECT
        item_pid,
        CAST(from_iso8601_timestamp(bright_updated_at) AS DATE) AS bright_date,
        CASE
            WHEN stitch_failed_reason LIKE 'no valid subtype%'
                THEN 'no valid subtype'
            WHEN stitch_failed_reason LIKE 'failed:"api error:INSTITUTION_NOT_RESPONDING"%'
                THEN 'INSTITUTION_NOT_RESPONDING'
            WHEN stitch_failed_reason LIKE 'failed:"api error:ITEM_LOGIN_REQUIRED"%'
                THEN 'ITEM_LOGIN_REQUIRED'
            WHEN stitch_failed_reason LIKE 'failed:"api error:NO_ACCOUNTS"%'
                THEN 'NO_ACCOUNTS'
            WHEN stitch_failed_reason LIKE 'failed:"api error:ITEM_NOT_FOUND"%'
                THEN 'ITEM_NOT_FOUND'
            WHEN stitch_failed_reason LIKE 'failed:"api error:INTERNAL_SERVER_ERROR"'
                THEN 'INTERNAL_SERVER_ERROR'
            WHEN stitch_failed_reason LIKE 'failed:"api error:ITEM_NOT_SUPPORTED"'
                THEN 'ITEM_NOT_SUPPORTED'
            WHEN stitch_failed_reason LIKE 'failed:"api error:MFA_NOT_SUPPORTED"'
                THEN 'MFA_NOT_SUPPORTED'
            WHEN stitch_failed_reason LIKE 'failed:"api error:INVALID_FIELD"'
                THEN 'INVALID_FIELD'
            WHEN stitch_failed_reason LIKE 'failed:"api error:ACCESS_NOT_GRANTED"'
                THEN 'ACCESS_NOT_GRANTED'
            WHEN stitch_failed_reason LIKE 'failed:"errors: error in fetching Item ITEM_NOT_FOUND"'
                THEN 'ITEM_NOT_FOUND'
            WHEN stitch_failed_reason LIKE 'failed:"errors: error in fetching Item ITEM_GET_LIMIT"'
                THEN 'ITEM_GET_LIMIT'
            WHEN stitch_failed_reason LIKE 'failed:"errors: error in fetching Item INTERNAL_SERVER_ERROR"'
                THEN 'INTERNAL_SERVER_ERROR'
            WHEN stitch_failed_reason LIKE 'failed:"no accounts%'
                THEN 'no accounts from API'
            WHEN stitch_failed_reason LIKE 'escalated%'
                THEN 'escalated_accounts'
            ELSE stitch_failed_reason
        END AS reason
    FROM uaa_db.plaid_force_refresh_error_data
    WHERE CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) >= CURRENT_DATE - INTERVAL '7' DAY
      AND CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) >= TIMESTAMP '2025-01-06'
)
SELECT
    bright_date,
    reason,
    COUNT(DISTINCT item_pid) AS counts
FROM error_data
GROUP BY bright_date, reason
ORDER BY bright_date DESC, counts DESC
