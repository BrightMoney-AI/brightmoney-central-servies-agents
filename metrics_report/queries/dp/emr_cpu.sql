SELECT
    jrd.cube_name,
    ROUND(
        approx_percentile(sm.cpu_utilization_pct, 0.5),
        2
    ) AS p50_cpu_utilization_pct,
    ROUND(
        approx_percentile(sm.cpu_utilization_pct, 0.95),
        2
    ) AS p95_cpu_utilization_pct
FROM iceberg_db.cosmos_db__public__sqlframework_jobrundetails__current_view_presto jrd
JOIN iceberg_db.spark_job_metrics sm
    ON jrd.application_id = sm.app_id
GROUP BY jrd.cube_name
ORDER BY p50_cpu_utilization_pct ASC
