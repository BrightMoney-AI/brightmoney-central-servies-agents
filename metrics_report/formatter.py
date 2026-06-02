"""
formatter.py — bridge between MetricsReport (raw VictoriaMetrics data)
and the structured L0Report model used by renderer.py.

Public API (unchanged for scheduler.py):
    build_slack_payload(report: MetricsReport, service_name: str) -> dict
"""
from __future__ import annotations

from datetime import datetime, timezone

from .collector import MetricsReport
from .config import settings
from .models import (
    ApiMetrics,
    Endpoint,
    FlaggingThresholds,
    L0Report,
    Server,
    ServerMetrics,
    Status,
    SystemHealth,
)
from .renderer import _detect_group, render


# ── Derive overall status from all metric values ───────────────────────────────

def _derive_status(
    thresholds: FlaggingThresholds,
    cpu_map: dict,
    mem_map: dict,
    disk_map: dict,
    success: float | None,
    error: float | None,
    avg_lat: float | None,
    ep_success_map: dict,
    ep_p99_map: dict,
    ep_error_map: dict,
) -> Status:
    worst = Status.HEALTHY

    def _bump(s: Status) -> None:
        nonlocal worst
        if s == Status.CRITICAL or (s == Status.WARNING and worst == Status.HEALTHY):
            worst = s

    for v in list(cpu_map.values()) + list(mem_map.values()) + list(disk_map.values()):
        if v >= thresholds.metric_crit_pct:
            _bump(Status.CRITICAL)
        elif v >= thresholds.metric_warn_pct:
            _bump(Status.WARNING)

    if success is not None:
        if success < 80.0:
            _bump(Status.CRITICAL)
        elif success < thresholds.success_warn_pct:
            _bump(Status.WARNING)

    if error is not None:
        if error >= 5.0:
            _bump(Status.CRITICAL)
        elif error >= 1.0:
            _bump(Status.WARNING)

    if avg_lat is not None:
        if avg_lat >= thresholds.p99_crit_ms / 3:
            _bump(Status.CRITICAL)
        elif avg_lat >= thresholds.p99_warn_ms / 3:
            _bump(Status.WARNING)

    all_eps = sorted(set(ep_success_map) | set(ep_p99_map) | set(ep_error_map))
    for ep in all_eps:
        suc = ep_success_map.get(ep)
        p99 = ep_p99_map.get(ep)
        err = ep_error_map.get(ep)
        if suc is not None and suc < 80.0:
            _bump(Status.CRITICAL)
        elif suc is not None and suc < thresholds.success_warn_pct:
            _bump(Status.WARNING)
        if p99 is not None and p99 >= thresholds.p99_crit_ms:
            _bump(Status.CRITICAL)
        elif p99 is not None and p99 >= thresholds.p99_warn_ms:
            _bump(Status.WARNING)
        if err is not None and err > 0:
            _bump(Status.WARNING)

    return worst


# ── Convert MetricsReport → L0Report ──────────────────────────────────────────

def _to_l0report(raw: MetricsReport, service_name: str) -> L0Report:
    v  = raw.values
    sv = raw.server_values
    ev = raw.endpoint_values

    # System metrics maps
    cpu_map  = dict(sv.get("cpu_usage_pct",   []))
    mem_map  = dict(sv.get("memory_usage_pct", []))
    disk_map = dict(sv.get("disk_usage_pct",   []))
    all_server_names = sorted(set(cpu_map) | set(mem_map) | set(disk_map))

    # Per-endpoint maps
    ep_hits_map    = dict(ev.get("endpoint_hits",          []))
    ep_success_map = dict(ev.get("endpoint_success_pct",   []))
    ep_error_map   = dict(ev.get("endpoint_error_count",   []))
    ep_p99_map     = dict(ev.get("endpoint_p99_latency_ms", []))

    # Aggregate API values
    tput    = v.get("api_throughput_rps") or 0.0
    success = v.get("api_success_rate_pct")
    error   = v.get("api_error_rate_pct")
    avg_lat = v.get("api_avg_latency_ms")

    # Build FlaggingThresholds from config settings
    thresholds = FlaggingThresholds(
        metric_warn_pct  = min(settings.cpu_warn_pct, settings.mem_warn_pct, settings.disk_warn_pct),
        metric_crit_pct  = min(settings.cpu_crit_pct, settings.mem_crit_pct, settings.disk_crit_pct),
        p99_warn_ms      = settings.avg_latency_warn_ms * 3,
        p99_crit_ms      = settings.avg_latency_crit_ms * 3,
        success_warn_pct = 99.0,
        top_n_unflagged  = 5,
    )

    # Build Server objects
    servers: list[Server] = []
    for name in all_server_names:
        cpu  = cpu_map.get(name)
        mem  = mem_map.get(name)
        disk = disk_map.get(name)
        if cpu is None and mem is None and disk is None:
            continue
        servers.append(Server(
            name=name,
            group=_detect_group(name),
            metrics=ServerMetrics(
                cpu_pct  = cpu  or 0.0,
                mem_pct  = mem  or 0.0,
                disk_pct = disk or 0.0,
            ),
            status=Status.HEALTHY,
        ))

    # Add placeholder entries for any servers reported as "down" (no metric data)
    servers_down_count = int(v.get("servers_down") or 0)
    for i in range(servers_down_count):
        servers.append(Server(
            name=f"[down-{i + 1}]",
            group="down",
            metrics=ServerMetrics(cpu_pct=0.0, mem_pct=0.0, disk_pct=0.0),
            status=Status.UNKNOWN,
        ))

    # Build Endpoint objects (active endpoints only: hits >= 1)
    all_ep_names = sorted(
        set(ep_hits_map) | set(ep_success_map) | set(ep_error_map) | set(ep_p99_map)
    )
    endpoints: list[Endpoint] = []
    for path in all_ep_names:
        hits = ep_hits_map.get(path)
        if (hits or 0) < 1:
            continue
        errors_raw = ep_error_map.get(path)
        endpoints.append(Endpoint(
            path        = path,
            hits        = int(hits),
            success_pct = ep_success_map.get(path) or 0.0,
            errors      = int(errors_raw) if errors_raw is not None else None,
            p99_ms      = ep_p99_map.get(path) or 0.0,
        ))

    # Sort by hits descending
    endpoints.sort(key=lambda e: e.hits, reverse=True)

    # Overall status
    status = _derive_status(
        thresholds, cpu_map, mem_map, disk_map,
        success, error, avg_lat,
        ep_success_map, ep_p99_map, ep_error_map,
    )

    return L0Report(
        service              = service_name,
        reported_at          = datetime.now(timezone.utc),
        status               = status,
        system               = SystemHealth(servers=servers),
        api                  = ApiMetrics(
            throughput_rps    = tput,
            success_rate_pct  = success or 0.0,
            error_rate_pct    = error   or 0.0,
            avg_latency_p50_ms= avg_lat or 0.0,
        ),
        endpoints            = endpoints,
        thresholds           = thresholds,
        total_endpoint_count = len(endpoints),
    )


# ── Public API (called by scheduler.py — signature unchanged) ──────────────────

def build_slack_payload(report: MetricsReport, service_name: str = "All Services") -> dict:
    l0 = _to_l0report(report, service_name)
    return render(l0)
