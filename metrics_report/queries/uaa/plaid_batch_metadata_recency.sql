SELECT
    MIN(DATE_DIFF('minute', run_timestamp, current_timestamp) / 60.0) AS recency_hrs
FROM uaa_db.plaid_batch_refresh_metadata
