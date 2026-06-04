"""
Collects business/application-level metrics for Central Services.
Queries are defined in central_business.json at the project root.
Entries with no data from VictoriaMetrics are silently skipped.
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .vm_client import VMClient

log = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass
class BusinessMetric:
    display_name: str
    query_name: str
    section: str
    metric_type: str   # "success_rate" | "failure_count" | "total_count" | "rate"
    value: float


async def collect_business_metrics(vm: VMClient) -> list[BusinessMetric]:
    """Run all queries from central_business.json; skip entries that return no data."""
    path = _PROJECT_ROOT / "central_business.json"
    if not path.exists():
        log.warning("central_business.json not found at %s", path)
        return []

    entries: list[dict] = json.loads(path.read_text())

    async def run_one(entry: dict) -> Optional[BusinessMetric]:
        try:
            val = await vm.query(entry["query"])
        except Exception as exc:
            log.warning("Business metric query failed [%s]: %s", entry["query_name"], exc)
            return None
        if val is None:
            return None
        return BusinessMetric(
            display_name=entry["display_name"],
            query_name=entry["query_name"],
            section=entry.get("section", "Other"),
            metric_type=entry.get("metric_type", "total_count"),
            value=val,
        )

    results: list[Optional[BusinessMetric]] = await asyncio.gather(
        *[run_one(e) for e in entries]
    )
    metrics = [r for r in results if r is not None]
    log.info(
        "Business metrics: %d/%d queries returned data.",
        len(metrics), len(entries),
    )
    return metrics
