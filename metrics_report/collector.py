"""
MetricsCollector — runs all L0 queries through the gateway sequentially
and returns a structured MetricsReport.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from .gateway import FailedQuery, MetricsGateway
from .queries import build_api_queries, build_system_queries
from .services import ServiceDef
from .vm_client import VMClient

log = logging.getLogger(__name__)


@dataclass
class MetricsReport:
    values: dict[str, Optional[float]] = field(default_factory=dict)
    failures: list[FailedQuery] = field(default_factory=list)


async def collect(
    vm_client: VMClient,
    gateway: MetricsGateway,
    service: Optional[ServiceDef] = None,
) -> MetricsReport:
    gateway.reset_failures()
    values: dict[str, Optional[float]] = {}

    sys_sel = service.system_selector if service else ""
    api_sel = service.api_selector if service else ""

    all_queries = build_system_queries(sys_sel) + build_api_queries(api_sel)

    for query in all_queries:
        log.info("Collecting [%s]: %s", service.display_name if service else "all", query.name)
        result = await gateway.fetch(
            name=query.name,
            coro_fn=lambda q=query: vm_client.query(q.promql),
        )
        values[query.name] = result

    return MetricsReport(values=values, failures=list(gateway.failures))
