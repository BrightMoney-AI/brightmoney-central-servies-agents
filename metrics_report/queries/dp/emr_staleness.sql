SELECT
    ch.cube_name,
    jrd.job_end_time AS last_run_time,
    ch.recency AS data_recency,
    ROUND(
        (
            to_unixtime(jrd.job_end_time)
            - to_unixtime(CAST(ch.recency AS timestamp))
        ) / 3600.0,
        2
    ) AS staleness_hrs
FROM iceberg_db.cosmos_db__public__sqlframework_cubehealth__current_view_presto ch
JOIN iceberg_db.cosmos_db__public__sqlframework_jobrundetails__current_view_presto jrd
    ON ch.job_run_pid = jrd.job_run_pid
WHERE ch.job_run_pid IN (
    SELECT MAX(job_run_pid)
    FROM iceberg_db.cosmos_db__public__sqlframework_cubehealth__current_view_presto
    GROUP BY cube_name
)
ORDER BY staleness_hrs DESC
