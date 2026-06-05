WITH recency_delta AS (
    SELECT
        DATE_DIFF('hour',
            GREATEST(COALESCE(last_balance_force_fetched, last_data_updated_at), last_data_updated_at),
            current_timestamp
        ) AS delta_hrs
    FROM uaa_db.plaid_batch_refresh_metadata
    WHERE COALESCE(last_balance_force_fetched, last_data_updated_at) IS NOT NULL
)
SELECT
    COUNT(*)                            AS number_of_accounts,
    approx_percentile(delta_hrs, 0.50) AS p50,
    approx_percentile(delta_hrs, 0.75) AS p75,
    approx_percentile(delta_hrs, 0.90) AS p90,
    approx_percentile(delta_hrs, 0.95) AS p95,
    approx_percentile(delta_hrs, 0.99) AS p99
FROM recency_delta
