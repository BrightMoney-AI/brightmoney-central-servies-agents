WITH _tables AS (
    SELECT tbl_name, CAST(tbl_id AS VARCHAR) AS tbl_id, NULL AS last_modified
    FROM iceberg_db.hive__tbls
    WHERE (tbl_name LIKE '%__cdc' OR tbl_name LIKE 'api_rr%')
      AND tbl_type = 'EXTERNAL_TABLE'
      AND NOT (tbl_name LIKE '%test%') AND NOT (tbl_name LIKE '%repartitioned%')
    UNION ALL
    SELECT tbl_name, CAST(tbl_id AS VARCHAR) AS tbl_id, cdc_transaction_ts AS last_modified
    FROM iceberg_db.hive__TBLS__cdc
    WHERE (tbl_name LIKE '%__cdc' OR tbl_name LIKE 'api_rr%')
      AND tbl_type = 'EXTERNAL_TABLE'
      AND NOT (tbl_name LIKE '%test%') AND NOT (tbl_name LIKE '%repartitioned%')
      AND cdc_transaction_ts > current_timestamp - INTERVAL '3' DAY
),
latest_tables AS (
    SELECT tbl_id, tbl_name, MAX(last_modified) AS last_modified
    FROM _tables
    GROUP BY tbl_id, tbl_name
),
params AS (
    SELECT CAST(tbl_id AS VARCHAR) AS tbl_id, param_value, cdc_transaction_ts
    FROM iceberg_db.hive__TABLE_PARAMS__cdc
    WHERE param_key = 'numFiles'
      AND cdc_transaction_ts > current_timestamp - INTERVAL '3' DAY
),
ranked_params AS (
    SELECT tbl_id, param_value, cdc_transaction_ts,
        ROW_NUMBER() OVER (PARTITION BY tbl_id ORDER BY cdc_transaction_ts DESC NULLS LAST) AS latest_rnk,
        ROW_NUMBER() OVER (PARTITION BY tbl_id ORDER BY ABS(DATE_DIFF('day', cdc_transaction_ts, CURRENT_DATE) - 3) ASC) AS week_old_rnk
    FROM params
)
SELECT
    t.tbl_name,
    CAST(p_latest.param_value AS INTEGER) - CAST(p_week_old.param_value AS INTEGER) AS num_files_growth
FROM latest_tables t
LEFT JOIN ranked_params p_latest   ON p_latest.tbl_id   = t.tbl_id AND p_latest.latest_rnk   = 1
LEFT JOIN ranked_params p_week_old ON p_week_old.tbl_id = t.tbl_id AND p_week_old.week_old_rnk = 1
WHERE CAST(p_latest.param_value AS INTEGER) - CAST(p_week_old.param_value AS INTEGER) > 300
ORDER BY 2 DESC
