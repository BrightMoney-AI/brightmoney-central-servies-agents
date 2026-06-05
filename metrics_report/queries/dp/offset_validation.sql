WITH dataset AS (
    SELECT d.name AS tbl_name, d.id AS dataset_id
    FROM iceberg_db.cosmos_db__public__dataset_dataset__current_view_presto d
    WHERE d.is_active = TRUE AND d.type IN ('cdc', 'kafka')
),
validated_data AS (
    SELECT tbl_name, dataset_id, partition_id, validated_till_offset, validated_till_transaction_ts
    FROM (
        SELECT
            d.tbl_name, s.dataset_id, s.status, s.partition_id,
            s.validated_till_offset,
            from_iso8601_timestamp(s.last_committed_at) AS validated_till_transaction_ts,
            ROW_NUMBER() OVER (
                PARTITION BY s.dataset_id, s.partition_id
                ORDER BY s.created_at DESC
            ) AS rnk
        FROM iceberg_db.cosmos_db__public__cdc_table_validation_stats__cdc s
        JOIN dataset d ON d.dataset_id = s.dataset_id
        WHERE s.validated_till_transaction_ts IS NOT NULL
          AND s.validated_till_offset IS NOT NULL
    ) tmp1
    WHERE rnk = 1 AND status = 'PASSED'
),
base_hive_tables AS (
    SELECT tbl_name, tbl_id FROM (
        SELECT d.tbl_name, CAST(t.tbl_id AS BIGINT) AS tbl_id
        FROM iceberg_db.hive__tbls t
        JOIN dataset d ON d.tbl_name = t.tbl_name
        UNION ALL
        SELECT d.tbl_name, CAST(c.tbl_id AS BIGINT) AS tbl_id
        FROM iceberg_db.hive__tbls__cdc c
        JOIN dataset d ON d.tbl_name = c.tbl_name
    ) t
    GROUP BY tbl_name, tbl_id
),
hive_table_params AS (
    SELECT tbl_id, cdc_transaction_ts, param_key, param_value
    FROM iceberg_db.hive__TABLE_PARAMS__cdc cdc
    WHERE cdc.param_key = 'current-snapshot-summary'
      AND cdc.cdc_transaction_ts > CURRENT_TIMESTAMP - INTERVAL '2' DAY
      AND cdc.cdc_transaction_ts <= CURRENT_TIMESTAMP - INTERVAL '15' MINUTE
),
params AS (
    SELECT
        b.tbl_name,
        CAST(p.partition AS BIGINT) AS partition,
        MAX(CAST(p.offset AS BIGINT)) AS max_offset,
        MAX(cdc.cdc_transaction_ts)   AS validated_till
    FROM base_hive_tables b
    JOIN hive_table_params cdc ON cdc.tbl_id = b.tbl_id
    CROSS JOIN UNNEST(
        CAST(json_extract(
            json_parse(json_extract_scalar(json_parse(cdc.param_value), '$.snapshot_metadata')),
            '$.partition_metadata_details.max_offset'
        ) AS MAP(VARCHAR, VARCHAR))
    ) AS p(partition, offset)
    JOIN validated_data vd
        ON  vd.tbl_name = b.tbl_name
        AND CAST(p.partition AS BIGINT) = vd.partition_id
    WHERE cdc.cdc_transaction_ts >= date_trunc('second', vd.validated_till_transaction_ts)
    GROUP BY p.partition, b.tbl_name
),
offset_diff AS (
    SELECT
        vd.tbl_name,
        SUM(COALESCE(p.max_offset, validated_till_offset, 0) - COALESCE(validated_till_offset, 0)) AS offset_diff,
        MIN(vd.validated_till_transaction_ts) AS min_validated_till_transaction_ts
    FROM validated_data vd
    LEFT JOIN params p ON p.tbl_name = vd.tbl_name AND p.partition = vd.partition_id
    GROUP BY vd.tbl_name
),
cdc_ranked AS (
    SELECT
        h.tbl_id, b.tbl_name, h.param_value, h.cdc_transaction_ts,
        o.offset_diff, o.min_validated_till_transaction_ts,
        ROW_NUMBER() OVER (PARTITION BY b.tbl_name ORDER BY h.cdc_transaction_ts ASC) AS rn
    FROM hive_table_params h
    JOIN base_hive_tables b ON h.tbl_id = b.tbl_id
    JOIN offset_diff o ON o.tbl_name = b.tbl_name
    WHERE element_at(CAST(json_parse(param_value) AS MAP(VARCHAR, VARCHAR)), 'removed-files-size') IS NULL
      AND h.cdc_transaction_ts >= date_trunc('second', o.min_validated_till_transaction_ts)
),
final_data AS (
    SELECT
        tbl_name,
        MAX(offset_diff) AS offset_diff,
        SUM(CAST(element_at(CAST(json_parse(param_value) AS MAP(VARCHAR, VARCHAR)), 'added-records') AS BIGINT)) AS added_records
    FROM cdc_ranked
    WHERE rn > 1
    GROUP BY tbl_name
)
SELECT tbl_name, offset_diff, added_records
FROM final_data
WHERE offset_diff <> added_records
ORDER BY 1
