"""
MetricsCollector — runs all L0 queries through the gateway sequentially.

Per-server queries (per_server=True) use query_vector() → stored in server_values.
Aggregate queries (per_server=False) use query()       → stored in values.
Per-endpoint queries                  use query_vector(id_label="endpoint") → endpoint_values.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from .config import settings
from .gateway import FailedQuery, MetricsGateway
from .queries import build_api_queries, build_per_endpoint_queries, build_system_queries
from .services import ServiceDef
from .vm_client import VMClient

log = logging.getLogger(__name__)


@dataclass
class MetricsReport:
    # Aggregate (API) metrics — one float per metric name
    values: dict[str, Optional[float]] = field(default_factory=dict)
    # Per-server system metrics — metric name → [(server_name, value), ...]
    server_values: dict[str, list[tuple[str, float]]] = field(default_factory=dict)
    # Per-endpoint API metrics — metric name → [(endpoint_path, value), ...]
    endpoint_values: dict[str, list[tuple[str, float]]] = field(default_factory=dict)
    failures: list[FailedQuery] = field(default_factory=list)


async def collect(
    vm_client: VMClient,
    gateway: MetricsGateway,
    service: Optional[ServiceDef] = None,
) -> MetricsReport:
    gateway.reset_failures()
    values: dict[str, Optional[float]] = {}
    server_values: dict[str, list[tuple[str, float]]] = {}
    endpoint_values: dict[str, list[tuple[str, float]]] = {}

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

    api_method   = service.api_method if service else None
    api_excludes = service.api_exclude_endpoints if service else []

    for query in build_api_queries(api_sel, exclude_endpoints=api_excludes or None, method=api_method, window=window):
        log.info("Collecting [%s]: %s", service.display_name if service else "all", query.name)
        result = await gateway.fetch(
            name=query.name,
            coro_fn=lambda q=query: vm_client.query(q.promql),
        )
        values[query.name] = result

    # Per-endpoint queries — only when the service has Django API metrics configured
    if service and service.api_job:
        ep_queries = build_per_endpoint_queries(
            selector=api_sel,
            exclude_endpoints=service.api_exclude_endpoints if service.api_exclude_endpoints else None,
            method=service.api_method,
            window=window,
        )
        for query in ep_queries:
            log.info("Collecting [%s] per-endpoint: %s", service.display_name, query.name)
            result = await gateway.fetch(
                name=query.name,
                coro_fn=lambda q=query: vm_client.query_vector(q.promql, id_label="endpoint"),
            )
            endpoint_values[query.name] = result or []

    return MetricsReport(
        values=values,
        server_values=server_values,
        endpoint_values=endpoint_values,
        failures=list(gateway.failures),
    )
