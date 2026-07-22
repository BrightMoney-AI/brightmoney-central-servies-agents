"""
MetricsCollector — fires all L0 queries for a service concurrently.

All four query types (system, API, per-endpoint, queue) are gathered in a
single asyncio.gather() call.  Spike range queries are gathered separately
(best-effort — failures are warned and skipped).

Concurrency is managed by the two-layer rate-limiter + semaphore in vm_client,
not by the gateway.  The gateway now only handles per-query timeouts and failure
recording (no serialising lock).

Per-server queries (per_server=True) use query_vector() → stored in server_values.
Aggregate queries (per_server=False) use query()       → stored in values.
Per-endpoint queries                  use query_vector(id_label="endpoint") → endpoint_values.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional

from .config import settings
from .gateway import FailedQuery, MetricsGateway
from .queries import (
    build_api_queries,
    build_per_endpoint_queries,
    build_queue_queries,
    build_spike_queries,
    build_system_queries,
)
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
    # Per-queue RabbitMQ depth — metric name → [(queue_name, value), ...]
    queue_values: dict[str, list[tuple[str, float]]] = field(default_factory=dict)
    # Spike analysis — metric_name → list of 30-min bucket values (oldest first)
    spike_series: dict[str, list[float]] = field(default_factory=dict)
    failures: list[FailedQuery] = field(default_factory=list)


async def collect(
    vm_client: VMClient,
    gateway: MetricsGateway,
    service: Optional[ServiceDef] = None,
) -> MetricsReport:
    gateway.reset_failures()

    svc_name = service.display_name if service else "all"
    sys_sel  = service.system_selector if service else ""
    api_sel  = service.api_selector    if service else ""
    window   = settings.query_window

    api_method          = service.api_method           if service else None
    api_excludes        = service.api_exclude_endpoints if service else []
    api_request_metric  = service.api_request_metric    if service else "django_request_count"
    api_response_metric = service.api_response_metric   if service else "django_http_responses_total_by_status"

    # ── Build all instant queries upfront ─────────────────────────────────────

    sys_queries   = list(build_system_queries(sys_sel, window))
    api_queries   = list(build_api_queries(
        api_sel,
        exclude_endpoints=api_excludes or None,
        method=api_method,
        window=window,
        api_request_metric=api_request_metric,
        api_response_metric=api_response_metric,
    ))
    ep_queries    = list(build_per_endpoint_queries(
        selector=api_sel,
        exclude_endpoints=service.api_exclude_endpoints if service and service.api_exclude_endpoints else None,
        method=service.api_method if service else None,
        window=window,
        api_request_metric=api_request_metric,
        api_response_metric=api_response_metric,
    )) if (service and service.api_job) else []
    queue_queries = list(build_queue_queries(service.rabbitmq_queues)) \
        if (service and service.rabbitmq_queues) else []
    spike_qs      = list(build_spike_queries(
        sys_sel, api_sel,
        api_request_metric=api_request_metric,
        api_response_metric=api_response_metric,
    ))

    # ── Coroutine factories for each query type ────────────────────────────────

    async def _sys(q) -> tuple[str, str, object]:
        log.info("Collecting [%s]: %s", svc_name, q.name)
        if q.per_server:
            val = await gateway.fetch(q.name, lambda q=q: vm_client.query_vector(q.promql))
            return "server", q.name, val or []
        val = await gateway.fetch(q.name, lambda q=q: vm_client.query(q.promql))
        return "scalar", q.name, val

    async def _api(q) -> tuple[str, str, object]:
        log.info("Collecting [%s]: %s", svc_name, q.name)
        val = await gateway.fetch(q.name, lambda q=q: vm_client.query(q.promql))
        return "scalar", q.name, val

    async def _ep(q) -> tuple[str, str, object]:
        log.info("Collecting [%s] per-endpoint: %s", svc_name, q.name)
        val = await gateway.fetch(
            q.name, lambda q=q: vm_client.query_vector(q.promql, id_label="endpoint")
        )
        return "endpoint", q.name, val or []

    async def _queue(q) -> tuple[str, str, object]:
        log.info("Collecting [%s] queue: %s", svc_name, q.name)
        val = await gateway.fetch(
            q.name, lambda q=q: vm_client.query_vector(q.promql, id_label="queue")
        )
        return "queue", q.name, val or []

    # ── Fire all instant queries concurrently ─────────────────────────────────

    instant_coros = (
        [_sys(q)   for q in sys_queries  ] +
        [_api(q)   for q in api_queries  ] +
        [_ep(q)    for q in ep_queries   ] +
        [_queue(q) for q in queue_queries]
    )
    instant_results = await asyncio.gather(*instant_coros) if instant_coros else []

    values:         dict[str, Optional[float]]          = {}
    server_values:  dict[str, list[tuple[str, float]]]  = {}
    endpoint_values: dict[str, list[tuple[str, float]]] = {}
    queue_values:   dict[str, list[tuple[str, float]]]  = {}

    for kind, name, val in instant_results:
        if kind == "server":
            server_values[name] = val        # type: ignore[assignment]
        elif kind == "endpoint":
            endpoint_values[name] = val      # type: ignore[assignment]
        elif kind == "queue":
            queue_values[name] = val         # type: ignore[assignment]
        else:
            values[name] = val               # type: ignore[assignment]

    # ── Spike range queries concurrently (best-effort) ────────────────────────

    async def _spike(metric_name: str, promql: str) -> tuple[str, list[float]]:
        try:
            buckets = await vm_client.query_range(promql)
            return metric_name, buckets if buckets else []
        except Exception as exc:
            log.warning("Spike query failed [%s]: %s", metric_name, exc)
            return metric_name, []

    spike_results = await asyncio.gather(*[
        _spike(name, promql)
        for name, _, _, promql in spike_qs
    ]) if spike_qs else []
    spike_series = {name: v for name, v in spike_results if v}

    return MetricsReport(
        values=values,
        server_values=server_values,
        endpoint_values=endpoint_values,
        queue_values=queue_values,
        spike_series=spike_series,
        failures=list(gateway.failures),
    )
