SELECT d.name
FROM iceberg_db.cosmos_db__public__dataset_dataset__current_view_presto d
JOIN iceberg_db.cosmos_db__public__features_hivesummary__current_view_presto h
    ON h.dataset_id = d.id
JOIN iceberg_db.cosmos_db__public__features_mirrortable__current_view_presto m
    ON m.cdc_dataset_id = d.id
WHERE d.is_active = TRUE
  AND m.is_active = TRUE
  AND h.presto_view_updated_till < (CURRENT_TIMESTAMP - INTERVAL '60' DAY)
  AND h.hive_view_updated_till   < (CURRENT_TIMESTAMP - INTERVAL '60' DAY)
  AND m.last_run_trx_ts          > (CURRENT_TIMESTAMP - INTERVAL '60' DAY)
