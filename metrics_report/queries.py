"""
L0 PromQL query builders.

System health queries (per_server=True) return one value per matched instance
via query_vector(). API queries (per_server=False) return a single aggregated
value via query(). Per-endpoint queries (per_server=True, id_label="endpoint")
use Django statsd metrics from job="platform_statsd_metrics".

  selector = 'name=~"p-uaa-em-.*|p-uaa-entity-manager.*", job="system_metrics"'
  sys_queries = build_system_queries(selector, window="24h")
  api_queries = build_api_queries(selector, window="24h")
  ep_queries  = build_per_endpoint_queries(api_selector, endpoints=[...], method="POST", window="24h")
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Query:
    name: str
    promql: str
    unit: str           # "%", "rps", "ms", "count"
    per_server: bool = False


# ── Label-injection helpers ────────────────────────────────────────────────────

def _wrap(sel: str) -> str:
    """Add selector to a bare metric: metric → metric{sel}"""
    return f"{{{sel}}}" if sel else ""


def _app(sel: str) -> str:
    """Append selector inside existing labels: {existing} → {existing, sel}"""
    return f", {sel}" if sel else ""


# ── System Health — per_server=True, one result row per instance ───────────────

def build_system_queries(selector: str = "", window: str = "24h") -> list[Query]:
    w = _wrap(selector)
    a = _app(selector)
    return [
        # Counter: avg across CPUs on the same instance, keep one row per (instance, name)
        Query(
            name="cpu_usage_pct",
            promql=f'100 - avg by (instance, name) (rate(node_cpu_seconds_total{{mode="idle"{a}}}[{window}])) * 100',
            unit="%",
            per_server=True,
        ),
        # Gauge: (Total - Free - Cached - Buffers) / Total — matches Grafana dashboard formula
        Query(
            name="memory_usage_pct",
            promql=(
                f"(avg_over_time(node_memory_MemTotal_bytes{w}[{window}])"
                f" - avg_over_time(node_memory_MemFree_bytes{w}[{window}])"
                f" - (avg_over_time(node_memory_Cached_bytes{w}[{window}])"
                f" + avg_over_time(node_memory_Buffers_bytes{w}[{window}])))"
                f" / avg_over_time(node_memory_MemTotal_bytes{w}[{window}]) * 100"
            ),
            unit="%",
            per_server=True,
        ),
        # Gauge: (size - free) / size — device!~rootfs excludes overlay/tmpfs duplicates
        Query(
            name="disk_usage_pct",
            promql=(
                f'(avg_over_time(node_filesystem_size_bytes{{mountpoint="/", device!~"rootfs"{a}}}[{window}])'
                f' - avg_over_time(node_filesystem_free_bytes{{mountpoint="/", device!~"rootfs"{a}}}[{window}]))'
                f' / avg_over_time(node_filesystem_size_bytes{{mountpoint="/", device!~"rootfs"{a}}}[{window}]) * 100'
            ),
            unit="%",
            per_server=True,
        ),
        # Instant aggregate — total count of up/down servers
        Query(
            name="servers_up",
            promql=f"count(up{w} == 1)",
            unit="count",
            per_server=False,
        ),
        Query(
            name="servers_down",
            promql=f"count(up{w} == 0) or vector(0)",
            unit="count",
            per_server=False,
        ),
    ]


# ── API Metrics — aggregated across all instances ──────────────────────────────
# Uses Django statsd metrics (same source as per-endpoint queries).
# django_request_latency_seconds is a Prometheus Summary — quantile labels are
# used directly instead of histogram_quantile().

def build_api_queries(
    selector: str = "",
    exclude_endpoints: Optional[list[str]] = None,
    method: Optional[str] = None,
    window: str = "24h",
    api_request_metric: str = "django_request_count",
    api_response_metric: str = "django_http_responses_total_by_status",
) -> list[Query]:
    base: list[str] = []
    if selector:
        base.append(selector)
    if exclude_endpoints:
        for ep in exclude_endpoints:
            base.append(f'endpoint!="{ep}"')

    base_with_method = base + ([f'method="{method}"'] if method else [])

    def mk(*extra: str) -> str:
        parts = base_with_method + list(extra)
        return "{" + ", ".join(parts) + "}"

    def mk_lat(*extra: str) -> str:
        # latency metric has no method label
        parts = base + list(extra)
        return "{" + ", ".join(parts) + "}"

    s_base    = mk()
    s_success = mk('status=~"2.."')
    s_error   = mk('status=~"[^2].."')
    s_p50     = mk_lat('quantile="0.5"')

    return [
        Query(
            name="api_throughput_rps",
            promql=f"sum(rate({api_request_metric}{s_base}[{window}]))",
            unit="rps",
        ),
        Query(
            name="api_success_rate_pct",
            promql=(
                f"sum(rate({api_response_metric}{s_success}[{window}]))"
                f" / sum(rate({api_response_metric}{s_base}[{window}])) * 100"
            ),
            unit="%",
        ),
        Query(
            name="api_error_rate_pct",
            promql=(
                f"sum(rate({api_response_metric}{s_error}[{window}]))"
                f" / sum(rate({api_response_metric}{s_base}[{window}])) * 100"
            ),
            unit="%",
        ),
        Query(
            name="api_avg_latency_ms",
            promql=f"avg(avg_over_time(django_request_latency_seconds{s_p50}[{window}])) * 1000",
            unit="ms",
        ),
        # 7-day baseline ending 24h ago — used to detect latency spikes vs normal operating range
        Query(
            name="api_avg_latency_baseline_ms",
            promql=f"avg(avg_over_time(django_request_latency_seconds{s_p50}[7d] offset 24h)) * 1000",
            unit="ms",
        ),
    ]


# ── Per-endpoint API Metrics — Django statsd metrics, one row per endpoint ────
# Uses:
#   django_request_count                    — request counter
#   django_http_responses_total_by_status   — response counter with status label
#   django_request_latency_seconds          — Prometheus Summary (quantile label, not histogram)
#
# All endpoints are discovered dynamically via `by (endpoint)`.
# Noisy/internal endpoints are excluded via api_exclude_endpoints in services.json.


def build_per_endpoint_queries(
    selector: str = "",
    exclude_endpoints: Optional[list[str]] = None,
    method: Optional[str] = None,
    window: str = "24h",
    api_request_metric: str = "django_request_count",
    api_response_metric: str = "django_http_responses_total_by_status",
) -> list[Query]:
    base: list[str] = []
    if selector:
        base.append(selector)
    if exclude_endpoints:
        for ep in exclude_endpoints:
            base.append(f'endpoint!="{ep}"')

    # method filter applies to request_count / responses_by_status but NOT to
    # django_request_latency_seconds (that metric has no method label)
    base_with_method = base + ([f'method="{method}"'] if method else [])

    def mk(*extra: str) -> str:
        parts = base_with_method + list(extra)
        return "{" + ", ".join(parts) + "}"

    def mk_lat(*extra: str) -> str:
        parts = base + list(extra)
        return "{" + ", ".join(parts) + "}"

    s_base    = mk()
    s_success = mk('status=~"2.."')
    s_error   = mk('status!="200"', 'status!="201"')
    s_latency = mk_lat('quantile="0.99"')

    return [
        Query(
            name="endpoint_hits",
            promql=f"sum(increase({api_request_metric}{s_base}[{window}])) by (endpoint)",
            unit="count",
            per_server=True,
        ),
        Query(
            name="endpoint_success_pct",
            promql=(
                f"sum(rate({api_response_metric}{s_success}[{window}])) by (endpoint)"
                f" / sum(rate({api_response_metric}{s_base}[{window}])) by (endpoint) * 100"
            ),
            unit="%",
            per_server=True,
        ),
        Query(
            name="endpoint_error_count",
            promql=f"sum(increase({api_response_metric}{s_error}[{window}])) by (endpoint)",
            unit="count",
            per_server=True,
        ),
        Query(
            name="endpoint_p99_latency_ms",
            promql=f"avg by (endpoint) (avg_over_time(django_request_latency_seconds{s_latency}[{window}])) * 1000",
            unit="ms",
            per_server=True,
        ),
        # 7-day baseline per endpoint — used to detect latency spikes vs normal operating range
        Query(
            name="endpoint_p99_latency_baseline_ms",
            promql=f"avg by (endpoint) (avg_over_time(django_request_latency_seconds{s_latency}[7d] offset 24h)) * 1000",
            unit="ms",
            per_server=True,
        ),
    ]


# ── RabbitMQ Queue Depth — instant gauge per queue ────────────────────────────

def build_queue_queries(queues: list[str]) -> list[Query]:
    """Return ready/unacked/total instant gauge queries for the given queue names."""
    sel = "|".join(queues)
    return [
        Query(
            name="queue_ready",
            promql=f'rabbitmq_queue_messages_ready{{queue=~"{sel}"}}',
            unit="count",
            per_server=True,
        ),
        Query(
            name="queue_unacked",
            promql=f'rabbitmq_queue_messages_unacked{{queue=~"{sel}"}}',
            unit="count",
            per_server=True,
        ),
        Query(
            name="queue_total",
            promql=f'rabbitmq_queue_messages{{queue=~"{sel}"}}',
            unit="count",
            per_server=True,
        ),
    ]
