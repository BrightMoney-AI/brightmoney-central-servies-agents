WITH error_data AS (
    SELECT
        item_pid,
        date_trunc('hour', CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP)) AS hour,
        CASE
            WHEN stitch_failed_reason LIKE 'no valid subtype%'
                THEN 'no valid subtype'
            WHEN stitch_failed_reason LIKE 'BETA item account mapped response%'
                THEN 'BETA item no mapped account'
            WHEN stitch_failed_reason LIKE 'ALPHA item account mapped response%'
                THEN 'ALPHA item no mapped account'
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
            WHEN stitch_failed_reason LIKE 'failed:"errors: error in fetching Item ITEM_NOT_FOUND"'
                THEN 'ITEM_NOT_FOUND'
            WHEN stitch_failed_reason LIKE 'failed:"errors: error in fetching Item ITEM_GET_LIMIT"'
                THEN 'ITEM_GET_LIMIT'
            WHEN stitch_failed_reason LIKE 'failed:"errors: error in fetching Item INTERNAL_SERVER_ERROR"'
                THEN 'INTERNAL_SERVER_ERROR'
            WHEN stitch_failed_reason LIKE 'failed:"last_successful_updated_on < last_balance_force_fetched%'
                THEN 'already have latest balance'
            WHEN stitch_failed_reason LIKE 'failed:"no accounts%'
                THEN 'no accounts from API'
            WHEN stitch_failed_reason LIKE 'escalated%'
                THEN 'escalated_accounts'
            ELSE stitch_failed_reason
        END AS reason
    FROM uaa_db.plaid_batch_refresh_error_data
    WHERE CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) >= CURRENT_DATE - INTERVAL '1' DAY
)
SELECT
    hour,
    reason,
    COUNT(DISTINCT item_pid) AS counts
FROM error_data
GROUP BY hour, reason
ORDER BY hour DESC, counts DESC
