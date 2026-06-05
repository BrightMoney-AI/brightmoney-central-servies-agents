SELECT *
FROM uaa_db.plaid_batch_refresh_recency_metrics
WHERE institution_name = 'Overall'
  AND metric_calculated_time >= date_add('day', -2, current_timestamp)
ORDER BY metric_calculated_time DESC
