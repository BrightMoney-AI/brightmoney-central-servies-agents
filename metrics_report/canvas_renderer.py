"""
canvas_renderer.py — Renders a list of L0Reports as Slack Canvas markdown.

All service reports are stacked in one document.
Each service is a collapsible # heading; System Health / API Metrics / Endpoints
are ## sub-sections that get collapse arrows in the Slack Canvas UI.
"""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import Optional

from .models import AirflowDagRun, AirflowHealth, KafkaConnectHealth, L0Report, Status
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
    lat_str = f"{a.avg_latency_p50_ms} ms"
    if a.avg_latency_baseline_ms and a.avg_latency_baseline_ms > 0:
        _r = a.avg_latency_p50_ms / a.avg_latency_baseline_ms
        _pct = (_r - 1) * 100
        _te = "🔴" if _r >= 2.0 else ("🟡" if _r >= 1.5 else "🟢")
        lat_str += f" ({_pct:+.0f}% {_te} vs 7d)"
    lines.append(
        f"| {a.throughput_rps:.1f} rps "
        f"| {_fmt_pct(a.success_rate_pct)} "
        f"| {_fmt_pct(a.error_rate_pct)} "
        f"| {lat_str} |"
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
        lines.append(f"**⚠️ Flagged ({len(flagged)}) — success drop · latency spike vs 7d baseline**")
        lines.append("")
        for ep in flagged:
            reasons   = _flag_reasons(ep, t)
            if ep.success_baseline_pct is not None:
                _drop = ep.success_baseline_pct - ep.success_pct
                suc_emoji = "🔴" if _drop >= 10.0 else ("🟡" if _drop >= 5.0 else "🟢")
            else:
                suc_emoji = "🔴" if ep.success_pct < 80 else ("🟡" if ep.success_pct < t.success_warn_pct else "🟢")
            if ep.p99_baseline_ms and ep.p99_baseline_ms > 0:
                _r = ep.p99_ms / ep.p99_baseline_ms
                p99_emoji = "🔴" if _r >= 2.0 else ("🟡" if _r >= 1.5 else "🟢")
            else:
                p99_emoji = "🔴" if ep.p99_ms >= t.p99_crit_ms else ("🟡" if ep.p99_ms >= t.p99_warn_ms else "🟢")
            err_str   = "N/A" if ep.errors is None else str(ep.errors)

            p99_str = _fmt_p99(ep.p99_ms)
            if ep.p99_baseline_ms and ep.p99_baseline_ms > 0:
                _r = ep.p99_ms / ep.p99_baseline_ms
                _pct = (_r - 1) * 100
                p99_str += f" ({_pct:+.0f}% vs 7d)"
            lines.append(f"**`{ep.path}`**")
            lines.append(
                f"{_fmt_hits(ep.hits)} hits · "
                f"{suc_emoji} {_fmt_pct(ep.success_pct)} · "
                f"{err_str} errors · "
                f"{p99_emoji} {p99_str} p99"
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
            p99_str = _fmt_p99(ep.p99_ms)
            if ep.p99_baseline_ms and ep.p99_baseline_ms > 0:
                _r = ep.p99_ms / ep.p99_baseline_ms
                _pct = (_r - 1) * 100
                _te = "🔴" if _r >= 2.0 else ("🟡" if _r >= 1.5 else "🟢")
                p99_str += f" ({_pct:+.0f}% {_te})"
            lines.append(
                f"- `{ep.path}` · "
                f"{_fmt_hits(ep.hits)} · "
                f"{_fmt_pct(ep.success_pct)} · "
                f"{err_str} errors · "
                f"{p99_str} p99"
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


def _state_emoji(state: str) -> str:
    if state == "success":
        return "🟢"
    elif state == "failed":
        return "🔴"
    elif state == "running":
        return "🔵"
    return "⚪"


def _render_airflow_section(airflow_health: AirflowHealth) -> str:
    """Render a ## Airflow DAGs section with DB-based DAG status and 24h view flow summary."""
    if not airflow_health.dag_runs and not airflow_health.view_flow and not airflow_health.pipeline_runs:
        return ""

    lines: list[str] = ["## Airflow DAGs", ""]

    # ── Pipeline DAGs: today vs yesterday ─────────────────────────────────────
    if airflow_health.pipeline_runs:
        IST_TZ = timezone(timedelta(hours=5, minutes=30))
        today_ist     = datetime.now(IST_TZ).date()
        yesterday_ist = today_ist - timedelta(days=1)

        # Index by (dag_id, run_date)
        run_index: dict[tuple, AirflowDagRun] = {}
        for r in airflow_health.pipeline_runs:
            if r.run_date:
                run_index[(r.dag_id, r.run_date)] = r

        dag_ids = sorted({r.dag_id for r in airflow_health.pipeline_runs})

        lines.append("| DAG | Today | Yesterday |")
        lines.append("|---|---|---|")
        def _cell(dag_id: str, d):
            r = run_index.get((dag_id, d))
            if not r:
                return "—"
            started = r.start_date.strftime("%H:%M IST") if r.start_date else ""
            return f"{_state_emoji(r.state)} {r.state}" + (f" ({started})" if started else "")

        for dag_id in dag_ids:
            lines.append(f"| `{dag_id}` | {_cell(dag_id, today_ist)} | {_cell(dag_id, yesterday_ist)} |")
        lines.append("")

    # ── DAG status from DB (dp_cosmos_flag_debezium_invalid_tables) ───────────
    if airflow_health.dag_runs:
        lines.append("| DAG | State | Last run |")
        lines.append("|---|---|---|")
        for d in sorted(airflow_health.dag_runs, key=lambda x: x.dag_id):
            started = d.start_date.strftime("%d %b %H:%M IST") if d.start_date else "—"
            lines.append(f"| `{d.dag_id}` | {_state_emoji(d.state)} {d.state} | {started} |")
        lines.append("")

    # ── View flow 24h summary (dp_cosmos_execute_view_flow) ───────────────────
    vf = airflow_health.view_flow
    if vf:
        lines.append("### dp_cosmos_execute_view_flow · last 24h")
        lines.append("")
        n_other = vf.total - vf.successful - len(vf.failed) - len(vf.running)
        parts = [f"{vf.total} runs", f"🟢 {vf.successful} success"]
        if vf.failed:
            parts.append(f"🔴 {len(vf.failed)} failed")
        if vf.running:
            parts.append(f"🔵 {len(vf.running)} running")
        if n_other > 0:
            parts.append(f"⚪ {n_other} other")
        lines.append("  ·  ".join(parts))
        lines.append("")

        if vf.failed:
            lines.append("**Failed table refreshes:**")
            lines.append("")
            lines.append("| Table | Started |")
            lines.append("|---|---|")
            for r in sorted(vf.failed, key=lambda x: x.table_name):
                started = r.start_date.strftime("%d %b %H:%M IST") if r.start_date else "—"
                lines.append(f"| `{r.table_name}` | {started} |")
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
