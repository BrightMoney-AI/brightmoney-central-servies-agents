SELECT d.name
FROM iceberg_db.cosmos_db__public__features_validation__current_view_presto f
JOIN iceberg_db.cosmos_db__public__features_mirrortable__current_view_presto m
    ON m.id = f.mirror_table_id
JOIN iceberg_db.cosmos_db__public__dataset_dataset__current_view_presto d
    ON d.id = m.cdc_dataset_id
WHERE m.is_active = TRUE
  AND d.is_active = TRUE
  AND f.is_active = TRUE
  AND (
      (
          f.last_full_validated_at < (CURRENT_TIMESTAMP - INTERVAL '90' DAY)
          AND f.last_validated_transaction_ts > (CURRENT_TIMESTAMP - INTERVAL '30' DAY)
      )
      OR f.sanity_passed = FALSE
      OR f.last_full_validated_at IS NULL
      OR f.last_validated_transaction_ts IS NULL
  )
ORDER BY 1
