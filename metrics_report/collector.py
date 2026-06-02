"""
MetricsCollector — runs all L0 queries through the gateway sequentially.

Per-server queries (per_server=True) use query_vector() → stored in server_values.
Aggregate queries (per_server=False) use query()       → stored in values.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from .config import settings
from .gateway import FailedQuery, MetricsGateway
from .queries import build_api_queries, build_system_queries
from .services import ServiceDef
from .vm_client import VMClient

log = logging.getLogger(__name__)


@dataclass
class MetricsReport:
    # Aggregate (API) metrics — one float per metric name
    values: dict[str, Optional[float]] = field(default_factory=dict)
    # Per-server system metrics — metric name → [(server_name, value), ...]
    server_values: dict[str, list[tuple[str, float]]] = field(default_factory=dict)
    failures: list[FailedQuery] = field(default_factory=list)


async def collect(
    vm_client: VMClient,
    gateway: MetricsGateway,
    service: Optional[ServiceDef] = None,
) -> MetricsReport:
    gateway.reset_failures()
    values: dict[str, Optional[float]] = {}
    server_values: dict[str, list[tuple[str, float]]] = {}

    sys_sel = service.system_selector if service else ""
    api_sel = service.api_selector if service else ""
    window = settings.query_window

    for query in build_system_queries(sys_sel, window):
        log.info("Collecting [%s]: %s", service.display_name if service else "all", query.name)
        if query.per_server:
            result = await gateway.fetch(
                name=query.name,
                coro_fn=lambda q=query: vm_client.query_vector(q.promql),
            )
            server_values[query.name] = result or []
        else:
            result = await gateway.fetch(
                name=query.name,
                coro_fn=lambda q=query: vm_client.query(q.promql),
            )
            values[query.name] = result

    for query in build_api_queries(api_sel, window):
        log.info("Collecting [%s]: %s", service.display_name if service else "all", query.name)
        result = await gateway.fetch(
            name=query.name,
            coro_fn=lambda q=query: vm_client.query(q.promql),
        )
        values[query.name] = result

    return MetricsReport(values=values, server_values=server_values, failures=list(gateway.failures))
