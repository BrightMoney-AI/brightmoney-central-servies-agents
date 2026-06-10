WITH latest_runs AS (
    SELECT
        ch.cube_name,
        ch.job_run_pid,
        jrd.job_start_time AS last_run_time,
        ch.recency AS data_recency,
        ch.recency_breach,
        ROUND(
            (to_unixtime(now()) - to_unixtime(ch.recency)) / 3600.0,
            2
        ) AS staleness_hrs,
        ROW_NUMBER() OVER (
            PARTITION BY ch.cube_name
            ORDER BY jrd.job_start_time DESC
        ) AS rn
    FROM iceberg_db.cosmos_db__public__sqlframework_cubehealth__current_view_presto ch
    JOIN iceberg_db.cosmos_db__public__sqlframework_jobrundetails__current_view_presto jrd
        ON ch.job_run_pid = jrd.job_run_pid
)
SELECT
    lr.cube_name,
    cc.ingestion_type,
    cc.cron_expression,
    lr.last_run_time,
    lr.data_recency,
    lr.recency_breach,
    lr.staleness_hrs
FROM latest_runs lr
JOIN iceberg_db.cosmos_db__public__sqlframework_cubeconfig__current_view_presto cc
    ON lr.cube_name = cc.cube_name
WHERE lr.rn = 1
ORDER BY lr.staleness_hrs DESC
