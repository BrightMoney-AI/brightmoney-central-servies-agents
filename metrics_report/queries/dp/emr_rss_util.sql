SELECT
    jrd.cube_name,
    COUNT(DISTINCT sm.app_id) AS run_count,
    MAX(sm.executor_memory_requested) AS executor_memory_requested,
    AVG(sm.peak_process_tree_jvm_rss_memory_bytes)
        / 1073741824 AS avg_peak_rss_mem_used,
    ROUND(
        approx_percentile(
            sm.peak_process_tree_jvm_rss_memory_bytes / 1073741824.0,
            0.95
        ),
        4
    ) AS p95_memory_used_gb,
    ROUND(
        AVG(
            sm.peak_process_tree_jvm_rss_memory_bytes * 1.0
            / NULLIF(
                CAST(
                    REGEXP_REPLACE(
                        sm.executor_memory_requested,
                        '[^0-9]',
                        ''
                    ) AS BIGINT
                ) * 1073741824,
                0
            )
        ) * 100,
        2
    ) AS avg_rss_util_pct
FROM iceberg_db.spark_job_metrics sm
JOIN iceberg_db.cosmos_db__public__sqlframework_jobrundetails__current_view_presto jrd
    ON jrd.application_id = sm.app_id
WHERE sm.app_start_time >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY jrd.cube_name
ORDER BY avg_rss_util_pct ASC
