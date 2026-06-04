"""
dp_business_collector.py — Data Platform business metrics.

All queries run through Trino (database 6). Tables from the Cosmos Django app are
accessed via their Iceberg mirror views: iceberg_db.cosmos_db__public__<table>.

All 8 queries run concurrently. Canvas is skipped when collect_dp_business_metrics()
returns an empty list.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

from .trino_client import execute_query

log = logging.getLogger(__name__)


@dataclass
class BusinessMetric:
    display_name: str
    query_name:   str
    section:      str
    metric_type:  str              # "failure_count" | "total_count"
    value:        float
    details:      list[str] = field(default_factory=list)   # affected table/dataset names


# ── queries ────────────────────────────────────────────────────────────────────

_TRINO_RECENCY = """
WITH tables AS (
    SELECT tbl_name, CAST(tbl_id AS BIGINT) AS tbl_id, CAST(db_id AS VARCHAR) AS db_id, NULL AS last_modified
    FROM iceberg_db.hive__tbls
    WHERE tbl_name LIKE '%__cdc' AND tbl_type = 'EXTERNAL_TABLE'
      AND NOT (tbl_name LIKE '%test%') AND NOT (tbl_name LIKE '%depri%')
      AND NOT (tbl_name LIKE 'hive__%') AND NOT (tbl_name LIKE '%repartitioned%')
    UNION ALL
    SELECT tbl_name, tbl_id, CAST(db_id AS VARCHAR) AS db_id, cdc_transaction_ts AS last_modified
    FROM iceberg_db.hive__TBLS__cdc
    WHERE tbl_name LIKE '%__cdc' AND tbl_type = 'EXTERNAL_TABLE'
      AND NOT (tbl_name LIKE '%test%') AND NOT (tbl_name LIKE '%depri%')
      AND NOT (tbl_name LIKE 'hive__%') AND NOT (tbl_name LIKE '%repartitioned%')
      AND cdc_transaction_ts > current_timestamp - INTERVAL '4' DAY
),
latest_tables AS (
    SELECT db_id, tbl_name, MAX(last_modified) AS last_modified, CAST(MAX(tbl_id) AS VARCHAR) AS tbl_id
    FROM tables GROUP BY db_id, tbl_name
),
params AS (
    SELECT CAST(tbl_id AS VARCHAR) AS tbl_id, param_value, cdc_transaction_ts
    FROM iceberg_db.hive__TABLE_PARAMS__cdc
    WHERE param_key = 'current-snapshot-summary'
      AND cdc_transaction_ts > current_timestamp - INTERVAL '4' DAY
),
ranked_params AS (
    SELECT tbl_id, param_value, cdc_transaction_ts,
        ROW_NUMBER() OVER (
            PARTITION BY tbl_id
            ORDER BY CASE WHEN json_extract_scalar(json_parse(param_value), '$.snapshot_metadata') IS NOT NULL THEN 1 ELSE 2 END,
                     cdc_transaction_ts DESC NULLS LAST
        ) AS rnk
    FROM params
),
final_data AS (
    SELECT t.tbl_name, t.tbl_id,
        COALESCE(
            json_extract_scalar(json_parse(json_extract_scalar(json_parse(param_value), '$.snapshot_metadata')), '$.table_metadata_details.transaction_ts'),
            json_extract_scalar(json_parse(json_extract_scalar(json_parse(param_value), '$.snapshot_metadata')), '$.heartbeat_metadata_details.transaction_ts')
        ) AS trx_ts
    FROM latest_tables t
    LEFT JOIN ranked_params p ON p.tbl_id = t.tbl_id
    WHERE p.rnk = 1
)
SELECT f.tbl_name
FROM final_data AS f
INNER JOIN iceberg_db.cosmos_db__public__dataset_dataset AS d ON d.name = f.tbl_name AND d.is_active = TRUE
WHERE f.trx_ts IS NULL OR CAST(f.trx_ts AS TIMESTAMP) < (CURRENT_TIMESTAMP - INTERVAL '3' HOUR)
ORDER BY f.tbl_name
"""

_TRINO_COMPACTION = """
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
    SELECT tbl_id, tbl_name, MAX(last_modified) AS last_modified FROM _tables GROUP BY tbl_id, tbl_name
),
params AS (
    SELECT CAST(tbl_id AS VARCHAR) AS tbl_id, param_value, cdc_transaction_ts
    FROM iceberg_db.hive__TABLE_PARAMS__cdc
    WHERE param_key = 'numFiles' AND cdc_transaction_ts > current_timestamp - INTERVAL '3' DAY
),
ranked_params AS (
    SELECT tbl_id, param_value, cdc_transaction_ts,
        ROW_NUMBER() OVER (PARTITION BY tbl_id ORDER BY cdc_transaction_ts DESC NULLS LAST) AS latest_rnk,
        ROW_NUMBER() OVER (PARTITION BY tbl_id ORDER BY ABS(DATE_DIFF('day', cdc_transaction_ts, CURRENT_DATE) - 3) ASC) AS week_old_rnk
    FROM params
)
SELECT t.tbl_name,
    CAST(p_latest.param_value AS INTEGER) - CAST(p_week_old.param_value AS INTEGER) AS num_files_growth
FROM latest_tables t
LEFT JOIN ranked_params p_latest   ON p_latest.tbl_id   = t.tbl_id AND p_latest.latest_rnk   = 1
LEFT JOIN ranked_params p_week_old ON p_week_old.tbl_id = t.tbl_id AND p_week_old.week_old_rnk = 1
WHERE CAST(p_latest.param_value AS INTEGER) - CAST(p_week_old.param_value AS INTEGER) > 300
ORDER BY 2 DESC
"""

_TRINO_OFFSET_VALIDATION = """
WITH dataset AS (
    SELECT d.name AS tbl_name, d.id AS dataset_id
    FROM iceberg_db.cosmos_db__public__dataset_dataset d
    WHERE d.is_active = TRUE AND d.type IN ('cdc', 'kafka')
),
validated_data AS (
    SELECT tbl_name, dataset_id, partition_id, validated_till_offset, validated_till_transaction_ts
    FROM (
        SELECT d.tbl_name, s.dataset_id, s.status, s.partition_id, s.validated_till_offset,
            from_iso8601_timestamp(s.last_committed_at) AS validated_till_transaction_ts,
            ROW_NUMBER() OVER (PARTITION BY s.dataset_id, s.partition_id ORDER BY s.created_at DESC) AS rnk
        FROM iceberg_db.cosmos_db__public__cdc_table_validation_stats__cdc s
        JOIN dataset d ON d.dataset_id = s.dataset_id
        WHERE s.validated_till_transaction_ts IS NOT NULL AND s.validated_till_offset IS NOT NULL
    ) tmp1
    WHERE rnk = 1 AND status = 'PASSED'
),
base_hive_tables AS (
    SELECT tbl_name, tbl_id FROM (
        SELECT d.tbl_name, CAST(t.tbl_id AS BIGINT) AS tbl_id FROM iceberg_db.hive__tbls t JOIN dataset d ON d.tbl_name = t.tbl_name
        UNION ALL
        SELECT d.tbl_name, CAST(c.tbl_id AS BIGINT) AS tbl_id FROM iceberg_db.hive__tbls__cdc c JOIN dataset d ON d.tbl_name = c.tbl_name
    ) t GROUP BY tbl_name, tbl_id
),
hive_table_params AS (
    SELECT tbl_id, cdc_transaction_ts, param_key, param_value
    FROM iceberg_db.hive__TABLE_PARAMS__cdc cdc
    WHERE cdc.param_key = 'current-snapshot-summary'
      AND cdc.cdc_transaction_ts > CURRENT_TIMESTAMP - INTERVAL '2' DAY
      AND cdc.cdc_transaction_ts <= CURRENT_TIMESTAMP - INTERVAL '15' MINUTE
),
params AS (
    SELECT b.tbl_name, CAST(p.partition AS BIGINT) AS partition,
        MAX(CAST(p.offset AS BIGINT)) AS max_offset, MAX(cdc.cdc_transaction_ts) AS validated_till
    FROM base_hive_tables b
    JOIN hive_table_params cdc ON cdc.tbl_id = b.tbl_id
    CROSS JOIN UNNEST(CAST(json_extract(json_parse(json_extract_scalar(json_parse(cdc.param_value), '$.snapshot_metadata')), '$.partition_metadata_details.max_offset') AS MAP(VARCHAR, VARCHAR))) AS p(partition, offset)
    JOIN validated_data vd ON vd.tbl_name = b.tbl_name AND CAST(p.partition AS BIGINT) = vd.partition_id
    WHERE cdc.cdc_transaction_ts >= date_trunc('second', vd.validated_till_transaction_ts)
    GROUP BY p.partition, b.tbl_name
),
offset_diff AS (
    SELECT vd.tbl_name,
        SUM(COALESCE(p.max_offset, validated_till_offset, 0) - COALESCE(validated_till_offset, 0)) AS offset_diff,
        MIN(vd.validated_till_transaction_ts) AS min_validated_till_transaction_ts
    FROM validated_data vd
    LEFT JOIN params p ON p.tbl_name = vd.tbl_name AND p.partition = vd.partition_id
    GROUP BY vd.tbl_name
),
cdc_ranked AS (
    SELECT h.tbl_id, b.tbl_name, h.param_value, h.cdc_transaction_ts,
        o.offset_diff, o.min_validated_till_transaction_ts,
        ROW_NUMBER() OVER (PARTITION BY b.tbl_name ORDER BY h.cdc_transaction_ts ASC) AS rn
    FROM hive_table_params h
    JOIN base_hive_tables b ON h.tbl_id = b.tbl_id
    JOIN offset_diff o ON o.tbl_name = b.tbl_name
    WHERE element_at(CAST(json_parse(param_value) AS MAP(VARCHAR, VARCHAR)), 'removed-files-size') IS NULL
      AND h.cdc_transaction_ts >= date_trunc('second', o.min_validated_till_transaction_ts)
),
final_data AS (
    SELECT tbl_name, MAX(offset_diff) AS offset_diff,
        SUM(CAST(element_at(CAST(json_parse(param_value) AS MAP(VARCHAR, VARCHAR)), 'added-records') AS BIGINT)) AS added_records
    FROM cdc_ranked WHERE rn > 1
    GROUP BY tbl_name
)
SELECT tbl_name, offset_diff, added_records
FROM final_data
WHERE offset_diff <> added_records
ORDER BY 1
"""

_TRINO_VIEW_STALE = """
SELECT d.name
FROM iceberg_db.cosmos_db__public__dataset_dataset d
JOIN iceberg_db.cosmos_db__public__features_hivesummary h ON h.dataset_id = d.id
JOIN iceberg_db.cosmos_db__public__features_mirrortable m ON m.cdc_dataset_id = d.id
WHERE d.is_active = TRUE AND m.is_active = TRUE
  AND h.presto_view_updated_till < (CURRENT_TIMESTAMP - INTERVAL '60' DAY)
  AND h.hive_view_updated_till   < (CURRENT_TIMESTAMP - INTERVAL '60' DAY)
  AND m.last_run_trx_ts          > (CURRENT_TIMESTAMP - INTERVAL '60' DAY)
"""

_TRINO_DBZ_INVALID = """
SELECT d.name
FROM iceberg_db.cosmos_db__public__dataset_dataset d
JOIN iceberg_db.cosmos_db__public__dataset_datasetbaserefresh dr ON dr.dataset_id = d.id
WHERE d.is_active = TRUE
  AND dr.last_base_updated        < (CURRENT_TIMESTAMP - INTERVAL '36' HOUR)
  AND dr.last_debezium_invalid_found > dr.last_base_updated
"""

_TRINO_FULL_VALIDATION = """
SELECT d.name
FROM iceberg_db.cosmos_db__public__features_validation f
JOIN iceberg_db.cosmos_db__public__features_mirrortable m ON m.id = f.mirror_table_id
JOIN iceberg_db.cosmos_db__public__dataset_dataset d       ON d.id = m.cdc_dataset_id
WHERE m.is_active = TRUE AND d.is_active = TRUE AND f.is_active = TRUE
  AND (
    (f.last_full_validated_at < (CURRENT_TIMESTAMP - INTERVAL '90' DAY) AND f.last_validated_transaction_ts > (CURRENT_TIMESTAMP - INTERVAL '30' DAY))
    OR f.sanity_passed = FALSE
    OR f.last_full_validated_at IS NULL
    OR f.last_validated_transaction_ts IS NULL
  )
ORDER BY 1
"""

_TRINO_BASE_REFRESH = """
SELECT d.name
FROM iceberg_db.cosmos_db__public__dataset_dataset d
JOIN iceberg_db.cosmos_db__public__features_mirrortable m ON m.cdc_dataset_id = d.id
WHERE m.is_active = TRUE AND d.is_active = TRUE
  AND (m.last_base_refresh_success < (CURRENT_TIMESTAMP - INTERVAL '60' DAY) OR m.last_run_trx_ts IS NULL)
"""

_TRINO_VALIDATION_STALE = """
WITH latest_run AS (
    SELECT run_version
    FROM iceberg_db.cosmos_db__public__cdc_table_validation_stats
    ORDER BY id DESC LIMIT 1
)
SELECT DISTINCT d.name
FROM iceberg_db.cosmos_db__public__dataset_dataset d
LEFT JOIN iceberg_db.cosmos_db__public__cdc_table_validation_stats_last_passed s
    ON s.dataset_id = d.id
LEFT JOIN iceberg_db.cosmos_db__public__cdc_table_validation_stats st
    ON st.dataset_id = d.id AND st.run_version = (SELECT run_version FROM latest_run)
WHERE d.type IN ('cdc', 'kafka') AND d.is_active = TRUE
  AND (s.created_at IS NULL OR s.created_at < (CURRENT_TIMESTAMP - INTERVAL '24' HOUR))
  AND st.status <> 'PASSED'
ORDER BY 1
"""


# ── per-query fetch functions ──────────────────────────────────────────────────

async def _fetch_table_recency() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_RECENCY)
    except Exception as exc:
        log.warning("table_recency query failed: %s", exc)
        return []
    names = [r["tbl_name"] for r in rows]
    return [BusinessMetric(display_name="Stale / null CDC tables", query_name="null-or-old-table-recency",
                           section="Table Recency", metric_type="failure_count", value=float(len(names)), details=names)]


async def _fetch_compaction() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_COMPACTION)
    except Exception as exc:
        log.warning("compaction query failed: %s", exc)
        return []
    details = [f"{r['tbl_name']}  (+{r['num_files_growth']} files)" for r in rows]
    return [BusinessMetric(display_name="File growth >300 in 3d", query_name="compaction-needed-tables",
                           section="Compaction", metric_type="failure_count", value=float(len(rows)), details=details)]


async def _fetch_offset_validation() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_OFFSET_VALIDATION)
    except Exception as exc:
        log.warning("offset_validation query failed: %s", exc)
        return []
    details = [f"{r['tbl_name']}  (Δoffset={r['offset_diff']}, records={r['added_records']})" for r in rows]
    return [BusinessMetric(display_name="Offset mismatch", query_name="real-time-offset-based-validation",
                           section="Validation", metric_type="failure_count", value=float(len(rows)), details=details)]


async def _fetch_view_stale() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_VIEW_STALE)
    except Exception as exc:
        log.warning("view_stale query failed: %s", exc)
        return []
    names = [r["name"] for r in rows]
    return [BusinessMetric(display_name="Views not updated >60d", query_name="view-update-older-than-60-day",
                           section="View Health", metric_type="failure_count", value=float(len(names)), details=names)]


async def _fetch_dbz_invalid() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_DBZ_INVALID)
    except Exception as exc:
        log.warning("dbz_invalid query failed: %s", exc)
        return []
    names = [r["name"] for r in rows]
    return [BusinessMetric(display_name="DBZ invalid, base not refreshed", query_name="dbz-invalid-base-not-refreshed",
                           section="CDC Health", metric_type="failure_count", value=float(len(names)), details=names)]


async def _fetch_full_validation() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_FULL_VALIDATION)
    except Exception as exc:
        log.warning("full_validation query failed: %s", exc)
        return []
    names = [r["name"] for r in rows]
    return [BusinessMetric(display_name="Full validation needed", query_name="full-validation-needed",
                           section="Validation", metric_type="failure_count", value=float(len(names)), details=names)]


async def _fetch_base_refresh() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_BASE_REFRESH)
    except Exception as exc:
        log.warning("base_refresh query failed: %s", exc)
        return []
    names = [r["name"] for r in rows]
    return [BusinessMetric(display_name="Base refresh overdue >60d", query_name="base-refresh-needed",
                           section="CDC Health", metric_type="failure_count", value=float(len(names)), details=names)]


async def _fetch_validation_stale() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_VALIDATION_STALE)
    except Exception as exc:
        log.warning("validation_stale query failed: %s", exc)
        return []
    names = [r["name"] for r in rows]
    return [BusinessMetric(display_name="Validation stale >24h", query_name="older-than-1-day-validated",
                           section="Validation", metric_type="failure_count", value=float(len(names)), details=names)]


# ── main collector ─────────────────────────────────────────────────────────────

async def collect_dp_business_metrics() -> list[BusinessMetric]:
    results = await asyncio.gather(
        _fetch_table_recency(),
        _fetch_compaction(),
        _fetch_offset_validation(),
        _fetch_view_stale(),
        _fetch_dbz_invalid(),
        _fetch_full_validation(),
        _fetch_base_refresh(),
        _fetch_validation_stale(),
    )
    metrics = [m for batch in results for m in batch]
    log.info("Data Platform business metrics collected: %d metric(s).", len(metrics))
    return metrics
