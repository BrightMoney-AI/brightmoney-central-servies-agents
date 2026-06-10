SELECT
    jrd.cube_name,
    ROUND(
        approx_percentile(
            sm.executor_memory_used_bytes / 1073741824.0,
            0.5
        ),
        4
    ) AS p50_memory_used_gb,
    ROUND(
        approx_percentile(
            sm.executor_memory_used_bytes / 1073741824.0,
            0.95
        ),
        4
    ) AS p95_memory_used_gb,
    ROUND(
        approx_percentile(
            sm.peak_jvm_heap_memory_bytes / 1073741824.0,
            0.95
        ),
        4
    ) AS p95_peak_heap_gb
FROM iceberg_db.cosmos_db__public__sqlframework_jobrundetails__current_view_presto jrd
JOIN iceberg_db.spark_job_metrics sm
    ON jrd.application_id = sm.app_id
GROUP BY jrd.cube_name
ORDER BY p95_memory_used_gb DESC
LIMIT 10
