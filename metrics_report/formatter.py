"""
ReportFormatter — converts MetricsReport into a Slack Block Kit payload.

Builds an L0Report from the raw MetricsReport, then delegates to renderer.render().
The public API (build_slack_payload) is unchanged — scheduler.py is unaffected.
"""
from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional

from .collector import MetricsReport
from .config import settings
from .models import (
    ApiMetrics, Endpoint, FlaggingThresholds, L0Report,
    QueueDepth, QueueHealth, Server, ServerMetrics, Status, SystemHealth,
)
from .renderer import render


# ── Status helpers (mirror the original icon logic exactly) ────────────────────

def _icon(value: Optional[float], warn: float, crit: float, invert: bool = False) -> str:
    if value is None:
        return "⚪"
    if invert:
        return "🟢" if value >= warn else ("🟡" if value >= crit else "🔴")
    return "🟢" if value < warn else ("🟡" if value < crit else "🔴")


def _icons_to_status(icons: list[str]) -> Status:
    if "🔴" in icons:
        return Status.CRITICAL
    if "🟡" in icons:
        return Status.WARNING
    if all(i == "⚪" for i in icons):
        return Status.UNKNOWN
    return Status.HEALTHY


# ── Group inference ────────────────────────────────────────────────────────────

def _infer_group(server_name: str) -> str:
    name = server_name.lower()
    for keyword in ("celery", "worker", "app", "web", "api"):
        if keyword in name.split("-"):
            return keyword
    if "celery" in name:
        return "celery"
    if "worker" in name:
        return "worker"
    parts = name.split("-")
    if parts and parts[0] == "p":
        parts = parts[1:]
    while parts and parts[-1].isdigit():
        parts = parts[:-1]
    return "-".join(parts[-2:]) if len(parts) >= 2 else (parts[-1] if parts else "server")


# ── Main builder ───────────────────────────────────────────────────────────────

def to_l0_report(report: MetricsReport, service_name: str = "All Services", show_api_metrics: bool = True) -> L0Report:
    """Convert a raw MetricsReport into a typed L0Report. Reusable by canvas renderer."""
    v  = report.values
    sv = report.server_values
    ev = report.endpoint_values

    cpu_map  = dict(sv.get("cpu_usage_pct",    []))
    mem_map  = dict(sv.get("memory_usage_pct",  []))
    disk_map = dict(sv.get("disk_usage_pct",    []))
    all_servers = sorted(set(cpu_map) | set(mem_map) | set(disk_map))

    tput    = v.get("api_throughput_rps")
    success = v.get("api_success_rate_pct")
    error   = v.get("api_error_rate_pct")
    avg_lat = v.get("api_avg_latency_ms")

    ep_hits_map    = dict(ev.get("endpoint_hits",           []))
    ep_success_map = dict(ev.get("endpoint_success_pct",    []))
    ep_error_map   = dict(ev.get("endpoint_error_count",    []))
    ep_p99_map     = dict(ev.get("endpoint_p99_latency_ms", []))
    all_ep_paths   = sorted(set(ep_hits_map) | set(ep_success_map) | set(ep_error_map) | set(ep_p99_map))

    active_ep = sorted(
        [ep for ep in all_ep_paths if (ep_hits_map.get(ep) or 0) >= 1],
        key=lambda ep: ep_hits_map.get(ep) or 0,
        reverse=True,
    )

    server_icons = (
        [_icon(cpu_map.get(s),  settings.cpu_warn_pct,  settings.cpu_crit_pct)  for s in all_servers]
        + [_icon(mem_map.get(s),  settings.mem_warn_pct,  settings.mem_crit_pct)  for s in all_servers]
        + [_icon(disk_map.get(s), settings.disk_warn_pct, settings.disk_crit_pct) for s in all_servers]
    )
    api_icons = [
        _icon(error,   settings.error_rate_warn_pct, settings.error_rate_crit_pct),
        _icon(success, warn=95.0, crit=90.0, invert=True),
        _icon(avg_lat, settings.avg_latency_warn_ms, settings.avg_latency_crit_ms),
    ]
    ep_icons = [
        _icon(ep_success_map.get(ep), warn=95.0, crit=90.0, invert=True)
        for ep in active_ep
    ] + [
        _icon(ep_p99_map.get(ep),
              settings.avg_latency_warn_ms * 3,
              settings.avg_latency_crit_ms * 3)
        for ep in active_ep
    ]
    for ep in active_ep:
        if (ep_error_map.get(ep) or 0) > 0:
            ep_icons.append("🔴")

    overall_status = _icons_to_status(server_icons + api_icons + ep_icons)

    thresholds = FlaggingThresholds(
        metric_warn_pct  = settings.disk_warn_pct,
        metric_crit_pct  = settings.disk_crit_pct,
        p99_warn_ms      = settings.avg_latency_warn_ms * 3,
        p99_crit_ms      = settings.avg_latency_crit_ms * 3,
        success_warn_pct = 95.0,
        top_n_unflagged  = 5,
    )

    servers = [
        Server(
            name    = name,
            group   = _infer_group(name),
            metrics = ServerMetrics(
                cpu_pct  = cpu_map.get(name,  0.0),
                mem_pct  = mem_map.get(name,  0.0),
                disk_pct = disk_map.get(name, 0.0),
            ),
            status  = Status.HEALTHY,
        )
        for name in all_servers
    ]

    endpoints = [
        Endpoint(
            path        = ep,
            hits        = int(ep_hits_map.get(ep) or 0),
            success_pct = ep_success_map.get(ep) or 0.0,
            errors      = int(ep_error_map[ep]) if ep in ep_error_map else None,
            p99_ms      = ep_p99_map.get(ep) or 0.0,
        )
        for ep in active_ep
    ]

    # Queue health (only present when the service has RabbitMQ queues configured)
    queue_health: Optional[QueueHealth] = None
    if report.queue_values:
        ready_map   = dict(report.queue_values.get("queue_ready",   []))
        unacked_map = dict(report.queue_values.get("queue_unacked", []))
        total_map   = dict(report.queue_values.get("queue_total",   []))
        all_queues  = sorted(set(ready_map) | set(unacked_map) | set(total_map))
        if all_queues:
            queue_health = QueueHealth(queues=[
                QueueDepth(
                    name    = q,
                    ready   = int(ready_map.get(q)   or 0),
                    unacked = int(unacked_map.get(q) or 0),
                    total   = int(total_map.get(q)   or 0),
                )
                for q in all_queues
            ])

    return L0Report(
        service              = service_name,
        reported_at          = datetime.now(timezone.utc),
        status               = overall_status,
        system               = SystemHealth(servers=servers),
        api                  = ApiMetrics(
            throughput_rps     = tput    or 0.0,
            success_rate_pct   = success or 0.0,
            error_rate_pct     = error   or 0.0,
            avg_latency_p50_ms = int(avg_lat or 0),
        ),
        endpoints            = endpoints,
        thresholds           = thresholds,
        total_endpoint_count = len(active_ep),
        queues               = queue_health,
        show_api_metrics     = show_api_metrics,
    )


def build_slack_payload(report: MetricsReport, service_name: str = "All Services") -> dict:
    l0 = to_l0_report(report, service_name)

    payload = render(l0)

    # Append failed-query block after render (informational, doesn't affect status)
    if report.failures:
        payload["blocks"] += [
            {"type": "divider"},
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*⚠️   QUERIES FAILED — data shown as N/A*"},
            },
            {
                "type": "context",
                "elements": [{
                    "type": "mrkdwn",
                    "text": "\n".join(f"›  `{f.name}`   {f.reason}" for f in report.failures),
                }],
            },
        ]
        # Trim to Slack's hard limit if the failures block pushed us over
        payload["blocks"] = payload["blocks"][:50]

    return payload
