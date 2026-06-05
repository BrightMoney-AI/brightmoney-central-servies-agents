WITH latest_run AS (
    SELECT run_version
    FROM iceberg_db.cosmos_db__public__cdc_table_validation_stats__current_view_presto
    ORDER BY id DESC
    LIMIT 1
)
SELECT DISTINCT d.name
FROM iceberg_db.cosmos_db__public__dataset_dataset__current_view_presto d
LEFT JOIN iceberg_db.cosmos_db__public__cdc_table_validation_stats_last_passed__current_view_presto s
    ON s.dataset_id = d.id
LEFT JOIN iceberg_db.cosmos_db__public__cdc_table_validation_stats__current_view_presto st
    ON st.dataset_id = d.id
    AND st.run_version = (SELECT run_version FROM latest_run)
WHERE d.type IN ('cdc', 'kafka')
  AND d.is_active = TRUE
  AND (s.created_at IS NULL OR s.created_at < (CURRENT_TIMESTAMP - INTERVAL '24' HOUR))
  AND st.status <> 'PASSED'
ORDER BY 1
