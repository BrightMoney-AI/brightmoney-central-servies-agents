"""
uaa_business_collector.py — UAA Services business metrics from Trino/Iceberg.

Add one async function per query block, return list[BusinessMetric] per function,
then call them all inside collect_uaa_business_metrics().
Canvas is skipped automatically when this returns an empty list.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from .trino_client import execute_query

log = logging.getLogger(__name__)


@dataclass
class BusinessMetric:
    display_name: str
    query_name:   str
    section:      str
    metric_type:  str    # "success_rate" | "failure_count" | "total_count" | "rate"
    value:        float


async def collect_uaa_business_metrics() -> list[BusinessMetric]:
    """
    Collect all UAA business metrics from Trino.
    Returns empty list until query functions are added — canvas is skipped when empty.
    """
    metrics: list[BusinessMetric] = []

    # ── Add query sections here as they are defined ──────────────────────────
    # Example:
    #   metrics += await _fetch_alsm_sessions()

    log.info("UAA business metrics collected: %d metric(s).", len(metrics))
    return metrics
