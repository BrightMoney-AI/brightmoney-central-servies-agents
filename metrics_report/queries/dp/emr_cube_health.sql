WITH ranked AS (
    SELECT
        ch.cube_name,
        cc.ingestion_type,
        cc.cron_expression,
        jrd.job_start_time AS last_run_time,
        ch.recency AS data_recency,
        ch.recency_breach,
        ROUND(
            (to_unixtime(now()) - to_unixtime(ch.recency)) / 3600.0,
            2
        ) AS data_age_hrs,
        ch.total_rows,
        ROW_NUMBER() OVER (
            PARTITION BY ch.cube_name
            ORDER BY jrd.job_start_time DESC
        ) AS rn
    FROM iceberg_db.cosmos_db__public__sqlframework_cubehealth__current_view_presto ch
    JOIN iceberg_db.cosmos_db__public__sqlframework_jobrundetails__current_view_presto jrd
        ON ch.job_run_pid = jrd.job_run_pid
    JOIN iceberg_db.cosmos_db__public__sqlframework_cubeconfig__current_view_presto cc
        ON ch.cube_name = cc.cube_name
       AND ch.iceberg_db_name = cc.iceberg_db_name
)
SELECT
    cube_name,
    ingestion_type,
    cron_expression,
    last_run_time,
    data_recency,
    recency_breach,
    data_age_hrs,
    total_rows
FROM ranked
WHERE rn = 1
ORDER BY cube_name
