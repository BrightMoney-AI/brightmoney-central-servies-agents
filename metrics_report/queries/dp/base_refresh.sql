SELECT d.name
FROM iceberg_db.cosmos_db__public__dataset_dataset__current_view_presto d
JOIN iceberg_db.cosmos_db__public__features_mirrortable__current_view_presto m
    ON m.cdc_dataset_id = d.id
WHERE m.is_active = TRUE
  AND d.is_active = TRUE
  AND (
      m.last_base_refresh_success < (CURRENT_TIMESTAMP - INTERVAL '60' DAY)
      OR m.last_run_trx_ts IS NULL
  )
