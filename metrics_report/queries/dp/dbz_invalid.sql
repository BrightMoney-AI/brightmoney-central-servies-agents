SELECT DISTINCT d.name
FROM iceberg_db.cosmos_db__public__dataset_dataset__current_view_presto d
JOIN iceberg_db.cosmos_db__public__dataset_datasetbaserefresh__current_view_presto dr
    ON dr.dataset_id = d.id
WHERE d.is_active = TRUE
  AND dr.last_base_updated           < (CURRENT_TIMESTAMP - INTERVAL '36' HOUR)
  AND dr.last_debezium_invalid_found > dr.last_base_updated
