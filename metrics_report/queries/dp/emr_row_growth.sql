WITH ranked AS (
    SELECT
        ch.cube_name,
        ch.job_run_pid,
        jrd.job_start_time AS run_time,
        ch.total_rows,
        LAG(ch.total_rows) OVER (
            PARTITION BY ch.cube_name
            ORDER BY jrd.job_start_time
        ) AS prev_total_rows
    FROM iceberg_db.cosmos_db__public__sqlframework_cubehealth__current_view_presto ch
    JOIN iceberg_db.cosmos_db__public__sqlframework_jobrundetails__current_view_presto jrd
        ON ch.job_run_pid = jrd.job_run_pid
),
deltas AS (
    SELECT
        cube_name,
        run_time,
        total_rows,
        prev_total_rows,
        (total_rows - prev_total_rows) AS new_rows_added,
        ROUND(
            100.0 * (total_rows - prev_total_rows)
            / NULLIF(prev_total_rows, 0),
            2
        ) AS pct_new_rows_added
    FROM ranked
    WHERE prev_total_rows IS NOT NULL
)
SELECT *
FROM deltas
ORDER BY new_rows_added DESC
