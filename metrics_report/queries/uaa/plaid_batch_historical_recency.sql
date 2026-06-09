SELECT *
FROM uaa_db.plaid_batch_refresh_recency_metrics
WHERE institution_name = 'Overall'
  AND DATE(metric_calculated_time) >= CURRENT_DATE - INTERVAL '2' DAY
  AND DATE(metric_calculated_time) < CURRENT_DATE
ORDER BY metric_calculated_time DESC
