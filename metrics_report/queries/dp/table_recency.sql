WITH tables AS (
    SELECT tbl_name, CAST(tbl_id AS BIGINT) AS tbl_id, CAST(db_id AS VARCHAR) AS db_id, NULL AS last_modified
    FROM iceberg_db.hive__tbls
    WHERE tbl_name LIKE '%__cdc' AND tbl_type = 'EXTERNAL_TABLE'
      AND NOT (tbl_name LIKE '%test%')        AND NOT (tbl_name LIKE '%depri%')
      AND NOT (tbl_name LIKE 'hive__%')       AND NOT (tbl_name LIKE '%repartitioned%')
    UNION ALL
    SELECT tbl_name, tbl_id, CAST(db_id AS VARCHAR) AS db_id, cdc_transaction_ts AS last_modified
    FROM iceberg_db.hive__TBLS__cdc
    WHERE tbl_name LIKE '%__cdc' AND tbl_type = 'EXTERNAL_TABLE'
      AND NOT (tbl_name LIKE '%test%')        AND NOT (tbl_name LIKE '%depri%')
      AND NOT (tbl_name LIKE 'hive__%')       AND NOT (tbl_name LIKE '%repartitioned%')
      AND cdc_transaction_ts > current_timestamp - INTERVAL '4' DAY
),
latest_tables AS (
    SELECT db_id, tbl_name, MAX(last_modified) AS last_modified, CAST(MAX(tbl_id) AS VARCHAR) AS tbl_id
    FROM tables
    GROUP BY db_id, tbl_name
),
params AS (
    SELECT CAST(tbl_id AS VARCHAR) AS tbl_id, param_value, cdc_transaction_ts
    FROM iceberg_db.hive__TABLE_PARAMS__cdc
    WHERE param_key = 'current-snapshot-summary'
      AND cdc_transaction_ts > current_timestamp - INTERVAL '4' DAY
),
ranked_params AS (
    SELECT tbl_id, param_value, cdc_transaction_ts,
        ROW_NUMBER() OVER (
            PARTITION BY tbl_id
            ORDER BY
                CASE WHEN json_extract_scalar(json_parse(param_value), '$.snapshot_metadata') IS NOT NULL THEN 1 ELSE 2 END,
                cdc_transaction_ts DESC NULLS LAST
        ) AS rnk
    FROM params
),
final_data AS (
    SELECT t.tbl_name, t.tbl_id,
        COALESCE(
            json_extract_scalar(json_parse(json_extract_scalar(json_parse(param_value), '$.snapshot_metadata')), '$.table_metadata_details.transaction_ts'),
            json_extract_scalar(json_parse(json_extract_scalar(json_parse(param_value), '$.snapshot_metadata')), '$.heartbeat_metadata_details.transaction_ts')
        ) AS trx_ts
    FROM latest_tables t
    LEFT JOIN ranked_params p ON p.tbl_id = t.tbl_id
    WHERE p.rnk = 1
)
SELECT f.tbl_name
FROM final_data AS f
INNER JOIN iceberg_db.cosmos_db__public__dataset_dataset__current_view_presto AS d
    ON d.name = f.tbl_name AND d.is_active = TRUE
WHERE f.trx_ts IS NULL
   OR CAST(f.trx_ts AS TIMESTAMP) < (CURRENT_TIMESTAMP - INTERVAL '3' HOUR)
ORDER BY f.tbl_name
