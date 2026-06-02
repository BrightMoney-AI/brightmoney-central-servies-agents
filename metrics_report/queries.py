"""
L0 PromQL query builders.

System health queries (per_server=True) return one value per matched instance
via query_vector(). API queries (per_server=False) return a single aggregated
value via query().

  selector = 'name=~"p-uaa-em-.*|p-uaa-entity-manager.*", job="system_metrics"'
  sys_queries = build_system_queries(selector, window="24h")
  api_queries = build_api_queries(selector, window="24h")
"""
from dataclasses import dataclass


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
        # Gauge: one series per instance already — no outer avg needed
        Query(
            name="memory_usage_pct",
            promql=f"(1 - avg_over_time(node_memory_MemAvailable_bytes{w}[{window}]) / avg_over_time(node_memory_MemTotal_bytes{w}[{window}])) * 100",
            unit="%",
            per_server=True,
        ),
        # Gauge: one series per (instance, mountpoint)
        Query(
            name="disk_usage_pct",
            promql=f'(1 - avg_over_time(node_filesystem_avail_bytes{{mountpoint="/"{a}}}[{window}]) / avg_over_time(node_filesystem_size_bytes{{mountpoint="/"{a}}}[{window}])) * 100',
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

def build_api_queries(selector: str = "", window: str = "24h") -> list[Query]:
    w = _wrap(selector)
    a = _app(selector)
    return [
        Query(
            name="api_throughput_rps",
            promql=f"sum(rate(http_requests_total{w}[{window}]))",
            unit="rps",
        ),
        Query(
            name="api_success_rate_pct",
            promql=f'sum(rate(http_requests_total{{status=~"2.."{a}}}[{window}])) / sum(rate(http_requests_total{w}[{window}])) * 100',
            unit="%",
        ),
        Query(
            name="api_error_rate_pct",
            promql=f'sum(rate(http_requests_total{{status=~"5.."{a}}}[{window}])) / sum(rate(http_requests_total{w}[{window}])) * 100',
            unit="%",
        ),
        Query(
            name="api_avg_latency_ms",
            promql=f"sum(rate(http_request_duration_seconds_sum{w}[{window}])) / sum(rate(http_request_duration_seconds_count{w}[{window}])) * 1000",
            unit="ms",
        ),
        Query(
            name="api_p95_latency_ms",
            promql=f"histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{w}[{window}])) by (le)) * 1000",
            unit="ms",
        ),
        Query(
            name="api_p99_latency_ms",
            promql=f"histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{w}[{window}])) by (le)) * 1000",
            unit="ms",
        ),
    ]
