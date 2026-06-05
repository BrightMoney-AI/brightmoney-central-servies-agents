WITH base AS (
    SELECT
        date(ca.created_at)                                                      AS cohort_date,
        ca.provider,
        date_diff('day', tm.earliest_posted_date, tm.latest_posted_date)         AS txn_duration_days,
        tm.count_posted_transactions                                              AS txn_count
    FROM iceberg_db.uaa__checking_account__entity ca
    LEFT JOIN iceberg_db.uaa_transaction_dump_daily_metrics tm
        ON  ca.account_id = tm.account_id
        AND tm.run_date   = (SELECT MAX(run_date) FROM iceberg_db.uaa_transaction_dump_daily_metrics)
    WHERE ca.created_at > DATE_ADD('day', -2, CURRENT_DATE)
),
per_provider AS (
    -- rows for specific providers
    SELECT cohort_date, provider, txn_duration_days, txn_count
    FROM base
    WHERE provider IN ('PLAID', 'DL_CAPITALONE')
    UNION ALL
    -- aggregate row across all providers
    SELECT cohort_date, 'All' AS provider, txn_duration_days, txn_count
    FROM base
)
SELECT
    cohort_date,
    provider,
    ROUND(AVG(txn_duration_days), 1)           AS avg_txn_duration_days,
    approx_percentile(txn_duration_days, 0.95) AS p95_txn_duration_days,
    ROUND(AVG(txn_count), 1)                   AS avg_txn_count,
    approx_percentile(txn_count, 0.95)         AS p95_txn_count
FROM per_provider
GROUP BY cohort_date, provider
ORDER BY cohort_date DESC, provider
