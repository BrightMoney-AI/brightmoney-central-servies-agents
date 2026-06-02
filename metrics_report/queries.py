"""
L0 PromQL query builders.

Queries are built at call-time by injecting a label selector fragment so the
same query set can be scoped to any service without duplication.

  selector = 'name=~"p-uaa-entity-manager.*", job="system_metrics"'
  queries  = build_system_queries(selector)

  # no filter — query all series
  queries  = build_system_queries()
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class Query:
    name: str
    promql: str
    unit: str   # "%", "rps", "ms", "count"


# ── Label-injection helpers ────────────────────────────────────────────────────

def _wrap(sel: str) -> str:
    """Add selector to a bare metric: metric → metric{sel}"""
    return f"{{{sel}}}" if sel else ""


def _app(sel: str) -> str:
    """Append selector inside existing labels: {existing} → {existing, sel}"""
    return f", {sel}" if sel else ""


# ── System Health ──────────────────────────────────────────────────────────────

def build_system_queries(selector: str = "") -> list[Query]:
    w = _wrap(selector)
    a = _app(selector)
    return [
        Query(
            name="cpu_usage_pct",
            promql=f'100 - avg(rate(node_cpu_seconds_total{{mode="idle"{a}}}[10m])) * 100',
            unit="%",
        ),
        Query(
            name="memory_usage_pct",
            promql=f"(1 - avg(node_memory_MemAvailable_bytes{w} / node_memory_MemTotal_bytes{w})) * 100",
            unit="%",
        ),
        Query(
            name="disk_usage_pct",
            promql=f'(1 - avg(node_filesystem_avail_bytes{{mountpoint="/"{a}}} / node_filesystem_size_bytes{{mountpoint="/"{a}}})) * 100',
            unit="%",
        ),
        Query(
            name="servers_up",
            promql=f"count(up{w} == 1)",
            unit="count",
        ),
        Query(
            name="servers_down",
            promql=f"count(up{w} == 0) or vector(0)",
            unit="count",
        ),
    ]


# ── API Metrics ────────────────────────────────────────────────────────────────

def build_api_queries(selector: str = "") -> list[Query]:
    w = _wrap(selector)
    a = _app(selector)
    return [
        Query(
            name="api_throughput_rps",
            promql=f"sum(rate(http_requests_total{w}[10m]))",
            unit="rps",
        ),
        Query(
            name="api_success_rate_pct",
            promql=f'sum(rate(http_requests_total{{status=~"2.."{a}}}[10m])) / sum(rate(http_requests_total{w}[10m])) * 100',
            unit="%",
        ),
        Query(
            name="api_error_rate_pct",
            promql=f'sum(rate(http_requests_total{{status=~"5.."{a}}}[10m])) / sum(rate(http_requests_total{w}[10m])) * 100',
            unit="%",
        ),
        Query(
            name="api_avg_latency_ms",
            promql=f"sum(rate(http_request_duration_seconds_sum{w}[10m])) / sum(rate(http_request_duration_seconds_count{w}[10m])) * 1000",
            unit="ms",
        ),
        Query(
            name="api_p95_latency_ms",
            promql=f"histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{w}[10m])) by (le)) * 1000",
            unit="ms",
        ),
        Query(
            name="api_p99_latency_ms",
            promql=f"histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{w}[10m])) by (le)) * 1000",
            unit="ms",
        ),
    ]
