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
    # Optional per-metric flag thresholds (see central_business.json)
    warn_below: Optional[float] = None   # success_rate: flag if value < this
    crit_below: Optional[float] = None   # success_rate: critical if value < this
    warn_above: Optional[float] = None   # failure_count: flag if value > this
    crit_above: Optional[float] = None   # failure_count: critical if value > this


async def collect_business_metrics(vm: VMClient) -> list[BusinessMetric]:
    """Run all queries from central_business.json; skip entries that return no data.

    Queries are issued with a concurrency cap (_SEM_LIMIT) to avoid bursting
    VictoriaMetrics with O(100) simultaneous requests and triggering 429s.
    """
    path = _PROJECT_ROOT / "central_business.json"
    if not path.exists():
        log.warning("central_business.json not found at %s", path)
        return []

    entries: list[dict] = json.loads(path.read_text())
    _sem = asyncio.Semaphore(10)  # max 10 in-flight at once; avoids 429 burst

    async def run_one(entry: dict) -> list[BusinessMetric]:
        taglist = entry.get("taglist")
        section = entry.get("section", "Other")
        metric_type = entry.get("metric_type", "total_count")
        thresholds = {
            "warn_below": entry.get("warn_below"),
            "crit_below": entry.get("crit_below"),
            "warn_above": entry.get("warn_above"),
            "crit_above": entry.get("crit_above"),
        }

        if taglist:
            try:
                async with _sem:
                    pairs = await vm.query_vector(entry["query"], id_label=taglist)
            except Exception as exc:
                log.warning("Business metric query failed [%s]: %s", entry["query_name"], exc)
                return []
            return [
                BusinessMetric(
                    display_name=label,
                    query_name=f"{entry['query_name']}_{label}",
                    section=section,
                    metric_type=metric_type,
                    value=val,
                    **thresholds,
                )
                for label, val in pairs
            ]

        try:
            async with _sem:
                val = await vm.query(entry["query"])
        except Exception as exc:
            log.warning("Business metric query failed [%s]: %s", entry["query_name"], exc)
            return []
        if val is None:
            return []
        return [BusinessMetric(
            display_name=entry["display_name"],
            query_name=entry["query_name"],
            section=section,
            metric_type=metric_type,
            value=val,
            **thresholds,
        )]

    results: list[list[BusinessMetric]] = await asyncio.gather(
        *[run_one(e) for e in entries]
    )
    metrics = [m for sublist in results for m in sublist]
    log.info(
        "Business metrics: %d/%d queries returned data.",
        len(metrics), len(entries),
    )
    return metrics
