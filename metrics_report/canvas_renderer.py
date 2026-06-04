"""
canvas_renderer.py — Renders a list of L0Reports as Slack Canvas markdown.

All service reports are stacked in one document.
Each service is a collapsible # heading; System Health / API Metrics / Endpoints
are ## sub-sections that get collapse arrows in the Slack Canvas UI.
"""
from __future__ import annotations
from datetime import timezone
from typing import Optional

from .models import AirflowHealth, KafkaConnectHealth, L0Report, Status
from .renderer import (
    IST,
    _endpoint_is_flagged,
    _flag_reasons,
    _fmt_hits,
    _fmt_p99,
    _fmt_pct,
    _group_summary,
)

_EMOJI = {
    Status.HEALTHY:  "🟢",
    Status.WARNING:  "🟡",
    Status.CRITICAL: "🔴",
    Status.UNKNOWN:  "⚪",
}


def _svc_emoji(status: Status) -> str:
    return _EMOJI.get(status, "⚪")


def _render_service(report: L0Report) -> str:
    lines: list[str] = []
    t = report.thresholds

    # ── Service heading ────────────────────────────────────────────────────────
    # Use ## (not #) so the canvas title (set via API) is the top-level heading.
    # Emoji after the dash keeps the heading scannable without breaking title parsing.
    dt = report.reported_at
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    ts       = dt.astimezone(IST)
    date_str = ts.strftime("%a %d %b %Y")
    time_str = ts.strftime("%I:%M %p IST")

    emoji = _svc_emoji(report.status)
    label = report.status.value.upper()

    lines.append(f"## {report.service} — {emoji} {label}")
    lines.append(f"_{date_str} · {time_str}_")
    lines.append("")

    # ── System Health ──────────────────────────────────────────────────────────
    sys = report.system
    lines.append(f"### System Health · {sys.online} online · {sys.down} down")
    lines.append("")

    groups = sorted(set(s.group for s in sys.servers))
    for group in groups:
        s = _group_summary(sys.servers, group)
        lines.append(
            f"**{group} group** — "
            f"CPU {s['avg_cpu']:.0f}% · "
            f"MEM {s['avg_mem']:.0f}% · "
            f"Disk {s['avg_disk']:.0f}%"
        )

    if sys.servers:
        lines.append("")
        for server in sys.servers:
            m = server.metrics
            lines.append(
                f"- `{server.name}` "
                f"CPU {m.cpu_pct:.0f}% · "
                f"MEM {m.mem_pct:.0f}% · "
                f"Disk {m.disk_pct:.0f}%"
            )

    lines.append("")

    if not report.show_api_metrics:
        return "\n".join(lines)

    # ── API Metrics ────────────────────────────────────────────────────────────
    a = report.api
    lines.append("### API Metrics")
    lines.append("")
    lines.append("| Throughput | Success rate | Error rate | Avg latency (p50) |")
    lines.append("|---|---|---|---|")
    lines.append(
        f"| {a.throughput_rps:.1f} rps "
        f"| {_fmt_pct(a.success_rate_pct)} "
        f"| {_fmt_pct(a.error_rate_pct)} "
        f"| {a.avg_latency_p50_ms} ms |"
    )
    lines.append("")

    # ── API Endpoints ──────────────────────────────────────────────────────────
    endpoints = report.endpoints
    if not endpoints:
        return "\n".join(lines)

    total_count = report.total_endpoint_count or len(endpoints)
    lines.append(f"### API Endpoints · {total_count} with traffic")
    lines.append("")

    flagged = sorted(
        [ep for ep in endpoints if _endpoint_is_flagged(ep, t)],
        key=lambda ep: ep.hits, reverse=True,
    )
    unflagged = sorted(
        [ep for ep in endpoints if not _endpoint_is_flagged(ep, t)],
        key=lambda ep: ep.hits, reverse=True,
    )

    if flagged:
        lines.append(f"**⚠️ Flagged ({len(flagged)}) — errors · low success · slow p99**")
        lines.append("")
        for ep in flagged:
            reasons   = _flag_reasons(ep, t)
            suc_emoji = "🔴" if ep.success_pct < 80 else ("🟡" if ep.success_pct < t.success_warn_pct else "🟢")
            p99_emoji = "🔴" if ep.p99_ms >= t.p99_crit_ms else ("🟡" if ep.p99_ms >= t.p99_warn_ms else "🟢")
            err_str   = "N/A" if ep.errors is None else str(ep.errors)

            lines.append(f"**`{ep.path}`**")
            lines.append(
                f"{_fmt_hits(ep.hits)} hits · "
                f"{suc_emoji} {_fmt_pct(ep.success_pct)} · "
                f"{err_str} errors · "
                f"{p99_emoji} {_fmt_p99(ep.p99_ms)} p99"
            )
            lines.append(f"_{', '.join(reasons)}_")
            lines.append("")

    top_n = unflagged[:t.top_n_unflagged]
    rest  = unflagged[t.top_n_unflagged:]

    if top_n:
        lines.append(f"**Top {len(top_n)} endpoints by hits**")
        lines.append("")
        for ep in top_n:
            err_str = "N/A" if ep.errors is None else str(ep.errors)
            lines.append(
                f"- `{ep.path}` · "
                f"{_fmt_hits(ep.hits)} · "
                f"{_fmt_pct(ep.success_pct)} · "
                f"{err_str} errors · "
                f"{_fmt_p99(ep.p99_ms)} p99"
            )
        lines.append("")

    extra        = max(0, report.total_endpoint_count - len(endpoints))
    total_hidden = len(rest) + extra
    if total_hidden > 0:
        lines.append(f"_+{total_hidden} more endpoints, all healthy_")
        lines.append("")

    return "\n".join(lines)


def _render_queue_section(reports: list[tuple[str, L0Report]]) -> str:
    """Render a single ## Queue Metrics section covering all services with queues."""
    services_with_queues = [
        (name, report) for name, report in reports
        if report.queues and report.queues.queues
    ]
    if not services_with_queues:
        return ""

    lines: list[str] = ["## Queue Metrics", ""]
    for name, report in services_with_queues:
        lines.append(f"### {name}")
        lines.append("")
        lines.append("| Queue | Ready | Unacked | Total |")
        lines.append("|---|---|---|---|")
        for q in sorted(report.queues.queues, key=lambda x: x.ready, reverse=True):
            ready_flag = " ⚠️" if q.ready >= 500 else (" 🟡" if q.ready >= 100 else "")
            lines.append(f"| `{q.name}` | {q.ready}{ready_flag} | {q.unacked} | {q.total} |")
        lines.append("")

    return "\n".join(lines)


def _render_connector_section(connector_health: KafkaConnectHealth) -> str:
    """Render a ## Connector Health section with one ### per Kafka Connect instance."""
    if not connector_health.instances:
        return ""

    lines: list[str] = ["## Connector Health", ""]
    for instance in connector_health.instances:
        n_unhealthy = len(instance.unhealthy)
        if n_unhealthy == 0:
            summary = f"✅ All {instance.total} healthy"
        else:
            summary = f"⚠️ {n_unhealthy} unhealthy"

        lines.append(f"### {instance.name} · {instance.total} connectors · {summary}")
        lines.append("")

        if instance.unhealthy:
            lines.append("| Connector | State | Tasks |")
            lines.append("|---|---|---|")
            for c in sorted(instance.unhealthy, key=lambda x: x.name):
                running = sum(1 for t in c.tasks if t.state == "RUNNING")
                total_t = len(c.tasks)
                task_str = f"{running}/{total_t} running" if c.tasks else "—"
                state_emoji = "🔴" if c.state in ("FAILED", "UNKNOWN") else "🟡"
                lines.append(f"| `{c.name}` | {state_emoji} {c.state} | {task_str} |")
            lines.append("")

    return "\n".join(lines)


def _render_airflow_section(airflow_health: AirflowHealth) -> str:
    """Render a ## Airflow DAGs section showing failed/non-success DAGs."""
    if not airflow_health.dag_runs:
        return ""

    total      = len(airflow_health.dag_runs)
    successful = sum(1 for d in airflow_health.dag_runs if d.state == "success")
    unhealthy  = [d for d in airflow_health.dag_runs if d.state != "success"]

    lines: list[str] = ["## Airflow DAGs", ""]

    if not unhealthy:
        lines.append(f"✅ All {total} DAGs succeeded")
        lines.append("")
        return "\n".join(lines)

    n_failed  = sum(1 for d in unhealthy if d.state == "failed")
    n_running = sum(1 for d in unhealthy if d.state == "running")
    n_other   = len(unhealthy) - n_failed - n_running

    parts = [f"{total} DAGs", f"🟢 {successful} success"]
    if n_failed:
        parts.append(f"🔴 {n_failed} failed")
    if n_running:
        parts.append(f"🔵 {n_running} running")
    if n_other:
        parts.append(f"⚪ {n_other} other")
    lines.append("  ·  ".join(parts))
    lines.append("")

    lines.append("| DAG | State | Last run |")
    lines.append("|---|---|---|")
    for d in sorted(unhealthy, key=lambda x: x.dag_id):
        state_emoji = "🔴" if d.state == "failed" else ("🔵" if d.state == "running" else "⚪")
        started = d.start_date.strftime("%d %b %H:%M IST") if d.start_date else "—"
        lines.append(f"| `{d.dag_id}` | {state_emoji} {d.state} | {started} |")
    lines.append("")

    return "\n".join(lines)


def render_canvas(
    reports: list[tuple[str, L0Report]],
    title: str = "",
    connector_health: Optional[KafkaConnectHealth] = None,
    airflow_health: Optional[AirflowHealth] = None,
) -> str:
    """
    Returns full canvas markdown.

    Structure:
      # {title}
      ## Service A ...
      ---
      ## Service B ...
      ---
      ## Queue Metrics       ← consolidated queue table, one ### per service
      ---
      ## Connector Health    ← Data Platform only, one ### per KC instance
      ---
      legend
    """
    sections = [_render_service(report) for _, report in reports]
    queue_section = _render_queue_section(reports)
    if queue_section:
        sections.append(queue_section)
    if connector_health:
        connector_section = _render_connector_section(connector_health)
        if connector_section:
            sections.append(connector_section)
    if airflow_health:
        airflow_section = _render_airflow_section(airflow_health)
        if airflow_section:
            sections.append(airflow_section)

    header = f"# {title}\n\n" if title else ""
    footer = "\n\n---\n\n🟢 Healthy   🟡 Warning 40-59%   🔴 Critical ≥60%   ·   brightmoney observability"
    return header + "\n\n---\n\n".join(sections) + footer
