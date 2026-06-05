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

from .queries import load_dp
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


# ── query loading ──────────────────────────────────────────────────────────────

_TRINO_RECENCY            = load_dp("table_recency")
_TRINO_COMPACTION         = load_dp("compaction")
_TRINO_OFFSET_VALIDATION  = load_dp("offset_validation")
_TRINO_VIEW_STALE         = load_dp("view_stale")
_TRINO_DBZ_INVALID        = load_dp("dbz_invalid")
_TRINO_FULL_VALIDATION    = load_dp("full_validation")
_TRINO_BASE_REFRESH       = load_dp("base_refresh")
_TRINO_VALIDATION_STALE   = load_dp("validation_stale")


# ── per-query fetch functions ──────────────────────────────────────────────────

async def _fetch_table_recency() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_RECENCY)
    except Exception as exc:
        log.warning("table_recency query failed: %s", exc)
        return []
    names = [r["tbl_name"] for r in rows]
    return [BusinessMetric(
        display_name="Stale / null CDC tables",
        query_name="null-or-old-table-recency",
        section="Table Recency",
        metric_type="failure_count",
        value=float(len(names)),
        details=names,
    )]


async def _fetch_compaction() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_COMPACTION)
    except Exception as exc:
        log.warning("compaction query failed: %s", exc)
        return []
    details = [f"{r['tbl_name']}  (+{r['num_files_growth']} files)" for r in rows]
    return [BusinessMetric(
        display_name="File growth >300 in 3d",
        query_name="compaction-needed-tables",
        section="Compaction",
        metric_type="failure_count",
        value=float(len(rows)),
        details=details,
    )]


async def _fetch_offset_validation() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_OFFSET_VALIDATION)
    except Exception as exc:
        log.warning("offset_validation query failed: %s", exc)
        return []
    details = [f"{r['tbl_name']}  (Δoffset={r['offset_diff']}, records={r['added_records']})" for r in rows]
    return [BusinessMetric(
        display_name="Offset mismatch",
        query_name="real-time-offset-based-validation",
        section="Validation",
        metric_type="failure_count",
        value=float(len(rows)),
        details=details,
    )]


async def _fetch_view_stale() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_VIEW_STALE)
    except Exception as exc:
        log.warning("view_stale query failed: %s", exc)
        return []
    names = [r["name"] for r in rows]
    return [BusinessMetric(
        display_name="Views not updated >60d",
        query_name="view-update-older-than-60-day",
        section="View Health",
        metric_type="failure_count",
        value=float(len(names)),
        details=names,
    )]


async def _fetch_dbz_invalid() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_DBZ_INVALID)
    except Exception as exc:
        log.warning("dbz_invalid query failed: %s", exc)
        return []
    names = [r["name"] for r in rows]
    return [BusinessMetric(
        display_name="DBZ invalid, base not refreshed",
        query_name="dbz-invalid-base-not-refreshed",
        section="CDC Health",
        metric_type="failure_count",
        value=float(len(names)),
        details=names,
    )]


async def _fetch_full_validation() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_FULL_VALIDATION)
    except Exception as exc:
        log.warning("full_validation query failed: %s", exc)
        return []
    names = [r["name"] for r in rows]
    return [BusinessMetric(
        display_name="Full validation needed",
        query_name="full-validation-needed",
        section="Validation",
        metric_type="failure_count",
        value=float(len(names)),
        details=names,
    )]


async def _fetch_base_refresh() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_BASE_REFRESH)
    except Exception as exc:
        log.warning("base_refresh query failed: %s", exc)
        return []
    names = [r["name"] for r in rows]
    return [BusinessMetric(
        display_name="Base refresh overdue >60d",
        query_name="base-refresh-needed",
        section="CDC Health",
        metric_type="failure_count",
        value=float(len(names)),
        details=names,
    )]


async def _fetch_validation_stale() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_VALIDATION_STALE)
    except Exception as exc:
        log.warning("validation_stale query failed: %s", exc)
        return []
    names = [r["name"] for r in rows]
    return [BusinessMetric(
        display_name="Validation stale >24h",
        query_name="older-than-1-day-validated",
        section="Validation",
        metric_type="failure_count",
        value=float(len(names)),
        details=names,
    )]


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
