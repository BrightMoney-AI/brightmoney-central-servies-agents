"""
canvas_renderer.py — Renders a list of L0Reports as Slack Canvas markdown.

All service reports are stacked in one document.
Each service is a collapsible # heading; System Health / API Metrics / Endpoints
are ## sub-sections that get collapse arrows in the Slack Canvas UI.
"""
from __future__ import annotations
from datetime import timezone, timedelta

from .models import Endpoint, FlaggingThresholds, L0Report, Status
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


def render_canvas(reports: list[tuple[str, L0Report]], title: str = "") -> str:
    """
    Returns full canvas markdown.

    Structure:
      # {title}          ← top-level heading = canvas display title
      ## Service A ...   ← each service is a collapsible ## section
      ---
      ## Service B ...
      ...
    """
    sections = [_render_service(report) for _, report in reports]
    body     = "\n\n---\n\n".join(sections)
    header   = f"# {title}\n\n" if title else ""
    footer   = "\n\n---\n\n🟢 Healthy   🟡 Warning 40-59%   🔴 Critical ≥60%   ·   brightmoney observability"
    return header + body + footer
