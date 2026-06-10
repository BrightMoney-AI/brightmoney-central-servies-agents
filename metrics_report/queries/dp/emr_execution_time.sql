SELECT
    sm.job_name AS cube_name,
    ROUND(
        approx_percentile(sm.execution_time / 3600000.0, 0.5),
        6
    ) AS p50_execution_time_hrs,
    ROUND(
        approx_percentile(sm.execution_time / 3600000.0, 0.95),
        6
    ) AS p95_execution_time_hrs
FROM iceberg_db.yarn_api_new_emr__spark_job_metrics sm
WHERE sm.job_name IN (
    SELECT cube_name
    FROM iceberg_db.cosmos_db__public__sqlframework_cubeconfig__current_view_presto
    WHERE is_active = true
)
GROUP BY sm.job_name
ORDER BY p95_execution_time_hrs DESC
