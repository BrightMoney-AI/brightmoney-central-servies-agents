"""
L0 PromQL query builders.

Queries are built at call-time with a configurable time window and an optional
label selector so the same query set can be scoped to any service.

  selector = 'name=~"p-uaa-entity-manager.*", job="system_metrics"'
  queries  = build_system_queries(selector, window="24h")

Metric types and their window handling:
  - Counters  (cpu, http_requests, latency buckets) → rate(...[window])
  - Gauges    (memory, disk)                        → avg_over_time(...[window])
  - Instant   (servers up/down)                     → no window — current state
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

def build_system_queries(selector: str = "", window: str = "24h") -> list[Query]:
    w = _wrap(selector)
    a = _app(selector)
    return [
        # Counter — rate() over window gives average CPU usage across the period
        Query(
            name="cpu_usage_pct",
            promql=f'100 - avg(rate(node_cpu_seconds_total{{mode="idle"{a}}}[{window}])) * 100',
            unit="%",
        ),
        # Gauge — avg_over_time() averages the gauge value across the period
        Query(
            name="memory_usage_pct",
            promql=f"(1 - avg(avg_over_time(node_memory_MemAvailable_bytes{w}[{window}]) / avg_over_time(node_memory_MemTotal_bytes{w}[{window}]))) * 100",
            unit="%",
        ),
        # Gauge — avg_over_time() for disk
        Query(
            name="disk_usage_pct",
            promql=f'(1 - avg(avg_over_time(node_filesystem_avail_bytes{{mountpoint="/"{a}}}[{window}]) / avg_over_time(node_filesystem_size_bytes{{mountpoint="/"{a}}}[{window}]))) * 100',
            unit="%",
        ),
        # Instant — current server state, not an average
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

def build_api_queries(selector: str = "", window: str = "24h") -> list[Query]:
    w = _wrap(selector)
    a = _app(selector)
    return [
        # All API metrics are counters — rate() over window
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
