"""
l0_manager_renderer.py — Renders a single all-groups L0 snapshot canvas for
the manager / senior-engineering review channel.

Design goals:
  · One canvas covers all groups — no separate docs per group.
  · Flagged services surfaced at the very top so managers see them first.
  · Per-group tables show exactly what L0 exposes: status, servers, success
    rate, error rate, and latency P50 (with 7d baseline trend where available).
  · No L1/L2 deep-dive — a footnote points readers to the detailed channel.

Consumed by hl_scheduler.run_hl_report() after the group-level canvases are
posted, using the already-collected `groups` dict (zero extra VM queries).
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from .models import L0Report, Status

log = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))

_EMOJI = {
    Status.HEALTHY:  "🟢",
    Status.WARNING:  "🟡",
    Status.CRITICAL: "🔴",
    Status.UNKNOWN:  "⚪",
}

_GROUP_ORDER = ["UAA Services", "UKS Services", "Central Services", "Data Platform"]


# ── Formatting helpers ─────────────────────────────────────────────────────────

def _fmt_servers(report: L0Report) -> str:
    total  = len(report.system.servers)
    online = report.system.online
    if report.system.down:
        return f"🔴 {online}/{total}"
    return f"🟢 {online}/{total}"


def _fmt_success(report: L0Report) -> str:
    if not report.show_api_metrics or report.api is None:
        return "—"
    val = report.api.success_rate_pct
    if val is None:
        return "—"
    base = report.api.success_rate_baseline_pct
    icon = "🔴" if val < 95 else ("🟡" if val < 99 else "🟢")
    if base is not None and base > 0:
        drop = base - val
        if abs(drop) >= 0.5:
            arrow = "▼" if drop > 0 else "▲"
            return f"{icon} {val:.1f}% {arrow}{abs(drop):.1f}pp"
    return f"{icon} {val:.1f}%"


def _fmt_error(report: L0Report) -> str:
    if not report.show_api_metrics or report.api is None:
        return "—"
    val = report.api.error_rate_pct
    if val is None:
        return "—"
    icon = "🔴" if val >= 5 else ("🟡" if val >= 1 else "🟢")
    base = report.api.error_rate_baseline_pct
    if base is not None and base >= 0.5:
        ratio = val / base if base > 0 else 0
        if ratio >= 1.5:
            return f"{icon} {val:.2f}% (▲{ratio:.1f}×)"
    return f"{icon} {val:.2f}%"


def _fmt_latency(report: L0Report) -> str:
    if not report.show_api_metrics or report.api is None:
        return "—"
    val = report.api.avg_latency_p50_ms
    if val is None:
        return "—"
    icon = "🔴" if val >= 1000 else ("🟡" if val >= 500 else "🟢")
    base = report.api.avg_latency_baseline_ms
    if base is not None and base > 0:
        ratio = val / base
        if ratio >= 1.3:
            return f"{icon} {val:.0f}ms (▲{ratio:.1f}×)"
    return f"{icon} {val:.0f}ms"


# ── Overall scorecard ──────────────────────────────────────────────────────────

def _scorecard(groups: dict[str, list[tuple[str, L0Report]]]) -> tuple[str, str, int, int, int]:
    """Return (emoji, label, n_crit, n_warn, n_ok) across all services."""
    all_reports = [r for services in groups.values() for _, r in services]
    n_crit = sum(1 for r in all_reports if r.status == Status.CRITICAL)
    n_warn = sum(1 for r in all_reports if r.status == Status.WARNING)
    n_ok   = sum(1 for r in all_reports if r.status == Status.HEALTHY)
    if n_crit:
        return "🔴", "Action Required", n_crit, n_warn, n_ok
    if n_warn:
        return "🟡", "Degraded", n_crit, n_warn, n_ok
    return "🟢", "All Systems Healthy", n_crit, n_warn, n_ok


# ── Flagged section ────────────────────────────────────────────────────────────

def _render_flagged(groups: dict[str, list[tuple[str, L0Report]]]) -> list[str]:
    flagged: list[tuple[str, str, L0Report]] = []
    ordered = [g for g in _GROUP_ORDER if g in groups] + [g for g in groups if g not in _GROUP_ORDER]
    for grp in ordered:
        for svc, r in groups[grp]:
            if r.status in (Status.CRITICAL, Status.WARNING):
                flagged.append((grp, svc, r))

    if not flagged:
        return ["### ✅ All Services Healthy", "", "_No flags across any group._", ""]

    crit = [(g, s, r) for g, s, r in flagged if r.status == Status.CRITICAL]
    warn = [(g, s, r) for g, s, r in flagged if r.status == Status.WARNING]

    counts = []
    if crit:
        counts.append(f"🔴 {len(crit)} critical")
    if warn:
        counts.append(f"🟡 {len(warn)} warning")

    lines: list[str] = [f"### ⚠️ Flagged   ·   " + " · ".join(counts), ""]

    def _reason(r: L0Report) -> str:
        reasons = []
        if r.system.down:
            reasons.append(f"servers {r.system.online}/{len(r.system.servers)} online")
        if r.show_api_metrics and r.api:
            if r.api.success_rate_pct is not None and r.api.success_rate_pct < 99:
                reasons.append(f"success {r.api.success_rate_pct:.1f}%")
            if r.api.error_rate_pct is not None and r.api.error_rate_pct >= 1:
                reasons.append(f"error rate {r.api.error_rate_pct:.2f}%")
            if r.api.avg_latency_p50_ms is not None and r.api.avg_latency_p50_ms >= 500:
                reasons.append(f"p50 {r.api.avg_latency_p50_ms:.0f}ms")
        return " · ".join(reasons) if reasons else "see full report"

    for grp, svc, r in crit:
        lines.append(f"- 🔴 **{svc}** · _{grp}_ · {_reason(r)}")
    for grp, svc, r in warn:
        lines.append(f"- 🟡 **{svc}** · _{grp}_ · {_reason(r)}")

    lines.append("")
    return lines


# ── UAA business compact block ─────────────────────────────────────────────────

def _render_uaa_biz_compact(biz: list[Any]) -> list[str]:
    """Render onboarding-by-provider and Plaid batch avg for the manager canvas.

    Designed to appear directly below the UAA Services service-health table so
    managers see business signal alongside infrastructure signal in one scroll.
    """
    if not biz:
        return []

    by_sec: dict[str, list[Any]] = defaultdict(list)
    for m in biz:
        by_sec[m.section].append(m)

    lines: list[str] = []

    # ── Onboarding by provider ─────────────────────────────────────────────────
    for m in by_sec.get("Onboarding", []):
        if m.metric_type == "provider_comparison" and m.details:
            provider_rows: list[str] = []
            for detail_row in m.details:
                parts = [p.strip() for p in detail_row.split("|")]
                if len(parts) < 2:
                    continue
                provider = parts[0]
                if not provider or provider in ("", "—"):
                    continue
                try:
                    d_sessions = int(parts[1]) if parts[1] not in ("", "—") else 0
                except ValueError:
                    d_sessions = 0

                # Success % from "420 (93.3%)" format
                d_pct: Optional[float] = None
                if len(parts) > 2 and "(" in parts[2]:
                    try:
                        d_pct = float(parts[2].split("(")[1].rstrip("%)").strip())
                    except (ValueError, IndexError):
                        pass

                try:
                    d1_sessions = int(parts[3]) if len(parts) > 3 and parts[3] not in ("", "—") else 0
                except ValueError:
                    d1_sessions = 0

                if d1_sessions > 0:
                    chg   = (d_sessions - d1_sessions) / d1_sessions * 100
                    trend = f"{'▲' if chg >= 0 else '▼'} {abs(chg):.1f}%"
                    icon  = "🔴" if chg <= -50 else ("🟡" if chg <= -20 else "🟢")
                else:
                    trend = "—"
                    icon  = "⚪"

                suc_str = f"{d_pct:.1f}%" if d_pct is not None else "—"
                provider_rows.append(f"| {provider} | {d_sessions:,} | {suc_str} | {trend} | {icon} |")

            if provider_rows:
                lines += [
                    "",
                    "**Onboarding by Provider** _(D-1 sessions vs D-2)_",
                    "",
                    "| Provider | Sessions | Success Rate | vs D-1 | Status |",
                    "|---|---|---|---|---|",
                ]
                lines += provider_rows
            break

    # ── Plaid Batch Refresh — 24h avg + latest 1h ─────────────────────────────
    for m in by_sec.get("Plaid Batch Refresh", []):
        if "hourly" in m.display_name.lower() and len(m.details) >= 2:
            hdrs = [h.strip().lower() for h in m.details[0].split("|")]
            try:
                rate_idx = next(i for i, h in enumerate(hdrs) if "success" in h)
            except StopIteration:
                break

            success_vals: list[float] = []
            for detail_row in m.details[1:]:
                parts = [p.strip().rstrip("%") for p in detail_row.split("|")]
                if len(parts) > rate_idx:
                    try:
                        success_vals.append(float(parts[rate_idx]))
                    except ValueError:
                        pass

            if success_vals:
                avg_s    = sum(success_vals) / len(success_vals)
                latest_s = success_vals[0]   # DESC order → index 0 = newest
                icon_avg = "🔴" if avg_s    < 90 else ("🟡" if avg_s    < 95 else "🟢")
                icon_lat = "🔴" if latest_s < 90 else ("🟡" if latest_s < 95 else "🟢")
                lines += [
                    "",
                    "**Plaid Batch Refresh** _(last 24 hours)_",
                    "",
                    "| Avg Success (24h) | Latest 1h | Status |",
                    "|---|---|---|",
                    f"| {icon_avg} {avg_s:.1f}% | {icon_lat} {latest_s:.1f}% | {icon_avg} |",
                ]
            break

    return lines


# ── Kafka / TI Pipeline compact block (manager canvas) ────────────────────────

def _render_kafka_compact(kafka: Any) -> list[str]:
    """Compact Kafka flags block for the manager canvas.

    Shows only flagged items — if nothing is flagged, returns [] so the
    section is omitted entirely (keeps the manager canvas noise-free).
    """
    if kafka is None:
        return []

    flag_items = kafka._flag_items()
    if not flag_items:
        return []

    lines: list[str] = [
        "",
        "**Kafka / TI Pipeline** _(flagged only)_",
        "",
        "| Issue | Severity |",
        "|---|---|",
    ]
    for severity, desc in flag_items:
        icon = "🔴" if severity == "crit" else "🟡"
        lines.append(f"| {desc} | {icon} {'Critical' if severity == 'crit' else 'Warning'} |")

    return lines


# ── Per-group section ──────────────────────────────────────────────────────────

def _render_group(group_name: str, services: list[tuple[str, L0Report]]) -> list[str]:
    n_crit = sum(1 for _, r in services if r.status == Status.CRITICAL)
    n_warn = sum(1 for _, r in services if r.status == Status.WARNING)
    n_ok   = sum(1 for _, r in services if r.status == Status.HEALTHY)
    total  = len(services)

    if n_crit:
        grp_icon  = "🔴"
        grp_label = f"{n_crit} critical"
    elif n_warn:
        grp_icon  = "🟡"
        grp_label = f"{n_warn} warning"
    else:
        grp_icon  = "🟢"
        grp_label = f"all {total} healthy"

    lines: list[str] = [
        f"## {group_name}   {grp_icon} {grp_label}",
        "",
        "| Service | Status | Servers | Success Rate | Error Rate | Latency P50 |",
        "|---|---|---|---|---|---|",
    ]

    # Critical first, then warning, then healthy — alphabetical within each tier
    sorted_svcs = sorted(services, key=lambda x: (
        0 if x[1].status == Status.CRITICAL else
        1 if x[1].status == Status.WARNING  else 2,
        x[0],
    ))

    for svc, r in sorted_svcs:
        icon   = _EMOJI[r.status]
        label  = r.status.value.capitalize()
        lines.append(
            f"| **{svc}** | {icon} {label} | {_fmt_servers(r)} "
            f"| {_fmt_success(r)} | {_fmt_error(r)} | {_fmt_latency(r)} |"
        )

    lines.append("")
    return lines


# ── Public entry point ─────────────────────────────────────────────────────────

def render_l0_manager_canvas(
    groups: dict[str, list[tuple[str, L0Report]]],
    date_str: str,
    uaa_biz_metrics: Optional[list[Any]] = None,
    ti_kafka_metrics: Optional[Any] = None,
) -> str:
    """Render a single all-groups L0 health canvas for the manager channel.

    Args:
        groups:            Mapping group_name → [(service_name, L0Report)].
        date_str:          Formatted date string, e.g. "22 Jul 2026".
        uaa_biz_metrics:   UAA business metrics list from collect_uaa_business_metrics().
                           When supplied, onboarding-by-provider and Plaid batch avg
                           are rendered directly below the UAA Services service table.
        ti_kafka_metrics:  TIKafkaMetrics from uaa_kafka_collector.  When supplied,
                           a compact Kafka flags summary is appended below UAA biz block
                           (only if any metric is flagged).

    Returns:
        Markdown string ready to be posted as a Slack canvas.
    """
    if not groups:
        return ""

    emoji, label, n_crit, n_warn, n_ok = _scorecard(groups)
    n_groups   = len(groups)
    n_services = sum(len(v) for v in groups.values())
    ts_str     = datetime.now(IST).strftime("%I:%M %p IST")

    lines: list[str] = []

    # ── Intro line (canvas title is set separately via canvases.create) ─────────
    lines += [
        f"_{date_str}  ·  {ts_str}  ·  {n_groups} groups  ·  {n_services} services_",
        "",
    ]

    # ── Overall scorecard ────────────────────────────────────────────────────────
    lines += [
        "## Overall Status",
        "",
        f"| | |",
        "|---|---|",
        f"| **Status** | {emoji} **{label}** |",
        f"| **Groups** | {n_groups} |",
        f"| **Services** | {n_services} |",
        f"| 🔴 Critical | {n_crit} |",
        f"| 🟡 Warning | {n_warn} |",
        f"| 🟢 Healthy | {n_ok} |",
        "",
        "---",
        "",
    ]

    # ── Flagged services ─────────────────────────────────────────────────────────
    lines += _render_flagged(groups)
    lines += ["---", ""]

    # ── Per-group breakdowns ─────────────────────────────────────────────────────
    ordered = [g for g in _GROUP_ORDER if g in groups] + [g for g in groups if g not in _GROUP_ORDER]
    for grp in ordered:
        lines += _render_group(grp, groups[grp])

        # After UAA Services, append onboarding-by-provider + batch avg + Kafka flags
        if grp == "UAA Services":
            if uaa_biz_metrics:
                biz_block = _render_uaa_biz_compact(uaa_biz_metrics)
                if biz_block:
                    lines += biz_block
                    lines.append("")
            if ti_kafka_metrics is not None:
                kafka_block = _render_kafka_compact(ti_kafka_metrics)
                if kafka_block:
                    lines += kafka_block
                    lines.append("")

    # ── Footer ───────────────────────────────────────────────────────────────────
    lines += [
        "---",
        "",
        "_L0 overview  ·  Success Rate ▲/▼ = change vs 7d baseline  ·  "
        "Full L1/L2 analysis → detailed metrics channel  ·  "
        "brightmoney observability_",
    ]

    return "\n".join(lines)


def render_l0_manager_summary_blocks(
    groups: dict[str, list[tuple[str, L0Report]]],
    date_str: str,
) -> list[dict]:
    """Block Kit blocks for the chat message that precedes the canvas card."""
    emoji, label, n_crit, n_warn, n_ok = _scorecard(groups)
    n_services = sum(len(v) for v in groups.values())
    ts_str     = datetime.now(IST).strftime("%a %d %b %Y · %I:%M %p IST")

    # Collect flagged services for the message body
    flagged = [
        (grp, svc, r)
        for grp in ([g for g in _GROUP_ORDER if g in groups] + [g for g in groups if g not in _GROUP_ORDER])
        for svc, r in groups[grp]
        if r.status in (Status.CRITICAL, Status.WARNING)
    ]

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"📊  Engineering Health Snapshot — {date_str}",
                "emoji": True,
            },
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": ts_str}],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Overall:* {emoji} *{label}*   ·   "
                    f"*{n_services}* services   ·   "
                    f"🔴 {n_crit} critical   🟡 {n_warn} warning   🟢 {n_ok} healthy"
                ),
            },
        },
    ]

    if flagged:
        lines = []
        for grp, svc, r in flagged[:8]:
            icon = "🔴" if r.status == Status.CRITICAL else "🟡"
            lines.append(f"{icon} *{svc}* — {grp}")
        if len(flagged) > 8:
            lines.append(f"_+{len(flagged) - 8} more — see canvas_")
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(lines)},
        })

    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": "Full per-service breakdown with metrics → canvas below ↓"}],
    })

    return blocks
