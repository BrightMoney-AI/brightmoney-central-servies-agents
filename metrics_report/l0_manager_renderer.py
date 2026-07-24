"""
l0_manager_renderer.py — Manager-level L0 canvas rendering.

Two rendering modes:

1. Per-group canvases  (primary, used by hl_scheduler)
   render_l0_group_canvas() + render_l0_group_summary_blocks()
   One focused canvas per group (UAA / DP / Central / UKS), each containing:
     · A plain-English health verdict ("All 8 services running and healthy — …")
     · Flagged items for that group (critical first, then warnings)
     · Compact service-health table  (status · servers · success · error · P50)
     · Group-specific L0 metrics:
         UAA      → Business Trends (onboarding, Plaid batch, ALSM/SAISM) + Kafka TI
         DP       → Data Quality Trends (CDC lag, stale tables, Airflow, EMR)
         Central  → Business Metrics Scorecard
         UKS      → KYC pass rate + Celery task health
   All posted to SLACK_L0_CHANNEL_ID — separate canvas messages per group.

2. All-groups combined canvas  (legacy / backup)
   render_l0_manager_canvas() — kept for backward compatibility.

Consumed by hl_scheduler.run_hl_report() and run_l0_manager_only() after the
per-group HL canvases are posted; reuses already-collected data (zero extra VM
queries).
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
    # No traffic: success=0 and error=0 means the service received no requests —
    # show "—" instead of a misleading 🔴 0.0%.
    err = report.api.error_rate_pct
    if val == 0.0 and (err is None or err == 0.0):
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
    # No traffic: both error and success are 0 — show "—" instead of 🟢 0.00%.
    success = report.api.success_rate_pct
    if val == 0.0 and (success is None or success == 0.0):
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
    # 0 ms is never a real latency value — it means no requests were served.
    if val == 0.0:
        return "—"
    icon = "🔴" if val >= 1000 else ("🟡" if val >= 500 else "🟢")
    base = report.api.avg_latency_baseline_ms

    # Build the primary "24h avg" portion
    if base is not None and base > 0:
        ratio = val / base
        if ratio >= 1.3:
            primary = f"{icon} {val:.0f}ms (▲{ratio:.1f}×)"
        else:
            primary = f"{icon} {val:.0f}ms"
    else:
        primary = f"{icon} {val:.0f}ms"

    # Append live "now" (1h) so engineers can tell if the spike is resolved or ongoing.
    cur = report.api.avg_latency_current_ms
    if cur is not None and cur > 0:
        cur_icon = "🔴" if cur >= 1000 else ("🟡" if cur >= 500 else "✅")
        primary += f" · now {cur:.0f}ms {cur_icon}"

    # Append spike time window when anomaly was detected, e.g.:
    # "🔴 1779ms (▲2.2×) · now 95ms ✅ · spike 5:30 AM–9:00 AM IST (peak 3200ms)"
    sw = report.api.latency_spike_window
    if sw is not None:
        start_s, end_s, peak_ms = sw
        primary += f" · spike {start_s}–{end_s} (peak {peak_ms:.0f}ms)"

    return primary


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


# ── Per-group canvas: health verdict ──────────────────────────────────────────

_GROUP_INSIGHTS: dict[str, dict[str, str]] = {
    # {group: {status: insight_suffix}}  — appended after the status headline.
    "UAA Services": {
        "healthy": "Onboarding, Plaid batch and enrichment flows are nominal.",
        "warning": "Review flagged items — onboarding or Plaid batch may need attention.",
        "critical": "Service disruption detected — check flagged items immediately.",
    },
    "Data Platform": {
        "healthy": "CDC sinks within normal range. Airflow DAGs running clean. No EMR breaches.",
        "warning": "Some data-quality signals degraded — review CDC lag, Airflow or EMR below.",
        "critical": "Data pipeline disruption detected — check flagged items immediately.",
    },
    "Central Services": {
        "healthy": "Business metric checks passing across all sections.",
        "warning": "One or more business metric sections flagged — review below.",
        "critical": "Critical business metric breach detected — check flagged items immediately.",
    },
    "UKS Services": {
        "healthy": "KYC pipeline healthy. Celery tasks and API views running nominally.",
        "warning": "KYC pass rate or task success degraded — review below.",
        "critical": "KYC pipeline disruption detected — check flagged items immediately.",
    },
}

_DEFAULT_INSIGHTS = {
    "healthy": "All checks passing.",
    "warning": "One or more signals degraded — review flagged items below.",
    "critical": "Critical issue detected — review flagged items immediately.",
}


def _group_health_verdict(
    group_name: str,
    services: list[tuple[str, L0Report]],
    has_extra_flags: bool = False,
) -> str:
    """Return a plain-English, manager-readable health verdict for a group.

    Combines service-level status with any extra flags (e.g. Kafka, CDC lag)
    to produce one concise sentence that gives immediate situational awareness.
    """
    n_total = len(services)
    n_crit  = sum(1 for _, r in services if r.status == Status.CRITICAL)
    n_warn  = sum(1 for _, r in services if r.status == Status.WARNING)
    n_ok    = sum(1 for _, r in services if r.status == Status.HEALTHY)

    insights = _GROUP_INSIGHTS.get(group_name, _DEFAULT_INSIGHTS)

    if n_crit:
        icon   = "🔴"
        head   = f"**{n_crit} of {n_total} service(s) critical — immediate attention required.**"
        suffix = insights["critical"]
    elif n_warn or has_extra_flags:
        icon   = "🟡"
        if n_warn:
            head = f"**{n_warn} of {n_total} service(s) degraded.**"
        else:
            head = f"**All {n_total} service(s) healthy — pipeline flags detected.**"
        suffix = insights["warning"]
    else:
        icon   = "🟢"
        head   = f"**All {n_total} service(s) running and healthy.**"
        suffix = insights["healthy"]

    return f"> {icon} {head} {suffix}"


def _render_group_flagged_section(
    services: list[tuple[str, L0Report]],
    extra_flags: list[tuple[str, str]],   # [(severity, description), ...]
) -> list[str]:
    """Render the Flags section for a single group.

    `extra_flags` carries pipeline-level flags (Kafka, CDC lag etc.) in the
    same (severity-str, desc) format produced by _flag_items().
    Returns [] when everything is healthy — keeps the canvas noise-free.
    """
    # Service-level flags
    crit_lines: list[str] = []
    warn_lines: list[str] = []

    for svc, r in services:
        if r.status == Status.CRITICAL:
            reasons = []
            if r.system.down:
                reasons.append(f"servers {r.system.online}/{len(r.system.servers)} online")
            if r.show_api_metrics and r.api:
                if r.api.success_rate_pct is not None and r.api.success_rate_pct < 95:
                    reasons.append(f"success {r.api.success_rate_pct:.1f}%")
                if r.api.error_rate_pct is not None and r.api.error_rate_pct >= 5:
                    reasons.append(f"errors {r.api.error_rate_pct:.2f}%")
                if r.api.avg_latency_p50_ms is not None and r.api.avg_latency_p50_ms >= 1000:
                    reasons.append(f"p50 {r.api.avg_latency_p50_ms:.0f}ms")
            detail = " · ".join(reasons) if reasons else "see full report"
            crit_lines.append(f"- 🔴 **{svc}** · {detail}")
        elif r.status == Status.WARNING:
            reasons = []
            if r.system.down:
                reasons.append(f"servers {r.system.online}/{len(r.system.servers)} online")
            if r.show_api_metrics and r.api:
                if r.api.success_rate_pct is not None and r.api.success_rate_pct < 99:
                    reasons.append(f"success {r.api.success_rate_pct:.1f}%")
                if r.api.error_rate_pct is not None and r.api.error_rate_pct >= 1:
                    reasons.append(f"errors {r.api.error_rate_pct:.2f}%")
                if r.api.avg_latency_p50_ms is not None and r.api.avg_latency_p50_ms >= 500:
                    reasons.append(f"p50 {r.api.avg_latency_p50_ms:.0f}ms")
            detail = " · ".join(reasons) if reasons else "see full report"
            warn_lines.append(f"- 🟡 **{svc}** · {detail}")

    # Pipeline-level flags (Kafka, CDC, etc.)
    for severity, desc in extra_flags:
        line = f"- {'🔴' if severity == 'crit' else '🟡'} {desc}"
        if severity == "crit":
            crit_lines.append(line)
        else:
            warn_lines.append(line)

    if not crit_lines and not warn_lines:
        return []

    counts = []
    if crit_lines:
        counts.append(f"🔴 {len(crit_lines)} critical")
    if warn_lines:
        counts.append(f"🟡 {len(warn_lines)} warning")

    lines: list[str] = [
        "### ⚠️ Flags   ·   " + " · ".join(counts),
        "",
    ]
    lines += crit_lines
    lines += warn_lines
    lines.append("")
    return lines


# ── Per-group L0 metrics blocks ────────────────────────────────────────────────
# These import from hl_canvas_renderer to avoid duplicating rendering logic.

def _render_uaa_l0_block(
    uaa_biz: Optional[list[Any]],
    ti_kafka: Optional[Any],
) -> list[str]:
    """UAA L0: Business Trends table + full Kafka L1 section.

    Business Trends table — always rendered when data is present:
      · Onboarding by provider, Account Linkings, ALSM/SAISM P99, Plaid Batch
      · Kafka flagged rows spliced in when there are active flags

    Kafka / TI Pipeline section — always rendered when kafka data exists:
      · Producer Health (success rate, failures, enrichment errors)
      · Consumer Lag per group
      · Consumer / Producer throughput per topic
      · CDC RR Logs
    """
    from .hl_canvas_renderer import (
        _render_l0_uaa_biz,
        _render_l0_ti_kafka,
        _render_l1_ti_kafka,
    )

    _flags: list[tuple[int, str]] = []   # collect flags but discard — already shown above

    biz_rows = _render_l0_uaa_biz(uaa_biz or [], _flags)

    if biz_rows and ti_kafka:
        kafka_flag_rows = _render_l0_ti_kafka(ti_kafka, _flags)
        if kafka_flag_rows:
            # Splice kafka flagged rows before the trailing "" that closes the table
            biz_rows = biz_rows[:-1] + kafka_flag_rows + [""]
    elif not biz_rows and ti_kafka:
        kafka_flag_rows = _render_l0_ti_kafka(ti_kafka, _flags)
        if kafka_flag_rows:
            biz_rows = (
                ["### Business Trends — UAA", "",
                 "| Metric | Current | Trend | Status |", "|---|---|---|---|"]
                + kafka_flag_rows
                + [""]
            )

    # If everything is healthy and there's no biz data, show a healthy Kafka line
    if not biz_rows and ti_kafka is not None and not ti_kafka._flag_items():
        biz_rows = ["_Kafka TI Pipeline: 🟢 all metrics nominal — no flags_", ""]

    # ── Full Kafka L1 section (always shown) ──────────────────────────────────
    kafka_l1 = _render_l1_ti_kafka(ti_kafka) if ti_kafka is not None else []

    return biz_rows + (["---", ""] + kafka_l1 if kafka_l1 else [])


def _render_dp_l0_block(
    dp_biz: Optional[list[Any]],
    dp_l0: Any,
    emr: Any,
    airflow: Any,
    connector_health: Any = None,
) -> list[str]:
    """DP L0: Data Quality Trends summary + CDC sink detail + Airflow/connector detail.

    Layer 1 — summary counts (Stale CDC, DBZ Invalid, Compaction, EMR breaches, CDC lag, Airflow)
    Layer 2 — CDC flagged-sinks detail table (coord lag, offset lag, heartbeat, status)
    Layer 3 — Airflow DAG per-run table + view-flow failures
    Layer 4 — Kafka Connect connector health (unhealthy connectors)
    """
    from .hl_canvas_renderer import _render_l0_dp, _render_l1_dp
    _flags: list[tuple[int, str]] = []
    summary = _render_l0_dp(dp_biz, dp_l0, emr, airflow, _flags)
    detail  = _render_l1_dp(dp_l0, connector_health, airflow, dp_biz, _flags)
    if summary and detail:
        return summary + ["---", ""] + detail
    return summary + detail


def _render_central_l0_block(central_biz: Optional[list[Any]]) -> list[str]:
    """Central Services L0: Scorecard summary + per-section metric values.

    Layer 1 — Scorecard: one bullet per section showing N/M checks healthy with icon.
    Layer 2 — Full metric values: per-section table of (metric name, value) so managers
              see the actual numbers, not just pass/fail counts.
    """
    if not central_biz:
        return []
    from .hl_canvas_renderer import _render_l0_central_scorecard, _render_l2_central
    _flags: list[tuple[int, str]] = []
    scorecard = _render_l0_central_scorecard(central_biz, _flags)
    detail    = _render_l2_central(central_biz)
    if scorecard and detail:
        return scorecard + ["---", ""] + detail
    return scorecard + detail


def _render_uks_l0_block(uks: Any) -> list[str]:
    """UKS L0: KYC overview + per-task P99 table + per-view API breakdown.

    Layer 1 — KYC Overview: pass rate, fail rate, task summary count.
    Layer 2 — Celery Tasks detail: per-task success rate and P99 latency for all tasks.
    Layer 3 — API Views: per-view success rate and req/min (high-volume views first).
    """
    if uks is None:
        return []
    from .hl_canvas_renderer import _render_l0_uks, _render_l1_uks
    _flags: list[tuple[int, str]] = []
    overview = _render_l0_uks(uks, _flags)
    detail   = _render_l1_uks(uks, _flags)
    if overview and detail:
        return overview + ["---", ""] + detail
    return overview + detail


# ── Per-group canvas public entry points ───────────────────────────────────────

def render_l0_group_canvas(
    group_name: str,
    services: list[tuple[str, L0Report]],
    date_str: str,
    *,
    # UAA-specific
    uaa_biz_metrics: Optional[list[Any]] = None,
    ti_kafka_metrics: Optional[Any] = None,
    # DP-specific
    dp_biz_metrics: Optional[list[Any]] = None,
    dp_l0_report: Optional[Any] = None,
    emr_report: Optional[Any] = None,
    airflow_health: Optional[Any] = None,
    connector_health: Optional[Any] = None,
    # Central-specific
    central_biz_metrics: Optional[list[Any]] = None,
    # UKS-specific
    uks_metrics: Optional[Any] = None,
) -> str:
    """Render a focused L0 manager canvas for a single group.

    Contains exactly what the HL canvas L0 section shows — service health,
    group-specific business/ops trends — distilled to what a manager needs:

      · A plain-English health verdict ("All 8 services running and healthy …")
      · Flagged items at the top (critical first, then warnings)
      · Compact service-health table  (status · servers · success · error · P50)
      · Group-specific L0 metrics (Business Trends / Data Quality / Scorecard / KYC)

    Zero extra VM queries — reuses data already collected by the HL report job.
    """
    if not services:
        return ""

    ts_str = datetime.now(IST).strftime("%I:%M %p IST")

    # ── Gather pipeline-level flags for the verdict & flags section ────────────
    extra_flags: list[tuple[str, str]] = []
    if group_name == "UAA Services" and ti_kafka_metrics is not None:
        extra_flags = ti_kafka_metrics._flag_items()
    elif group_name == "Data Platform":
        # Surface CDC / Airflow / EMR critical items from the DP L0 block
        from .hl_canvas_renderer import _render_l0_dp
        _tmp: list[tuple[int, str]] = []
        _render_l0_dp(dp_biz_metrics, dp_l0_report, emr_report, airflow_health, _tmp)
        extra_flags = [
            ("crit" if sev == 0 else "warn", desc.split(" · ", 1)[-1])
            for sev, desc in _tmp
        ]
    elif group_name == "UKS Services" and uks_metrics is not None:
        from .hl_canvas_renderer import _render_l0_uks
        _tmp2: list[tuple[int, str]] = []
        _render_l0_uks(uks_metrics, _tmp2)
        extra_flags = [
            ("crit" if sev == 0 else "warn", desc.split(" · ", 1)[-1])
            for sev, desc in _tmp2
        ]
    elif group_name == "Central Services" and central_biz_metrics:
        from .hl_canvas_renderer import _render_l0_central_scorecard
        _tmp3: list[tuple[int, str]] = []
        _render_l0_central_scorecard(central_biz_metrics, _tmp3)
        extra_flags = [
            ("crit" if sev == 0 else "warn", desc.split(" · ", 1)[-1])
            for sev, desc in _tmp3
        ]

    has_extra_flags = bool(extra_flags)

    n_services = len(services)
    lines: list[str] = []

    # ── Intro line ──────────────────────────────────────────────────────────────
    lines += [
        f"_{date_str}  ·  {ts_str}  ·  {n_services} service(s)_",
        "",
    ]

    # ── Health verdict — the plain-English "meaningful insight" ────────────────
    lines += [
        _group_health_verdict(group_name, services, has_extra_flags),
        "",
        "---",
        "",
    ]

    # ── Flagged items (service + pipeline level) ───────────────────────────────
    flag_lines = _render_group_flagged_section(services, extra_flags)
    if flag_lines:
        lines += flag_lines
        lines += ["---", ""]
    else:
        lines += ["### ✅ No Flags — All Checks Healthy", "", "---", ""]

    # ── Compact service-health table ───────────────────────────────────────────
    lines += _render_group(group_name, services)
    lines += ["---", ""]

    # ── Group-specific L0 metrics ──────────────────────────────────────────────
    if group_name == "UAA Services":
        l0_block = _render_uaa_l0_block(uaa_biz_metrics, ti_kafka_metrics)
    elif group_name == "Data Platform":
        l0_block = _render_dp_l0_block(dp_biz_metrics, dp_l0_report, emr_report, airflow_health, connector_health)
    elif group_name == "Central Services":
        l0_block = _render_central_l0_block(central_biz_metrics)
    elif group_name == "UKS Services":
        l0_block = _render_uks_l0_block(uks_metrics)
    else:
        l0_block = []

    if l0_block:
        lines += l0_block
        lines += ["---", ""]

    # ── Footer ──────────────────────────────────────────────────────────────────
    lines += [
        "_L0 manager snapshot  ·  Success Rate ▲/▼ = change vs 7d baseline  ·  "
        f"Full L1/L2 analysis → detailed metrics channel_",
    ]

    return "\n".join(lines)


def render_l0_group_summary_blocks(
    group_name: str,
    services: list[tuple[str, L0Report]],
    date_str: str,
) -> list[dict]:
    """Block Kit blocks for the chat message that precedes the per-group canvas card."""
    n_total = len(services)
    n_crit  = sum(1 for _, r in services if r.status == Status.CRITICAL)
    n_warn  = sum(1 for _, r in services if r.status == Status.WARNING)
    n_ok    = sum(1 for _, r in services if r.status == Status.HEALTHY)
    ts_str  = datetime.now(IST).strftime("%a %d %b %Y · %I:%M %p IST")

    if n_crit:
        emoji, label = "🔴", "CRITICAL"
    elif n_warn:
        emoji, label = "🟡", "DEGRADED"
    else:
        emoji, label = "🟢", "ALL HEALTHY"

    flagged = [
        (svc, r) for svc, r in services
        if r.status in (Status.CRITICAL, Status.WARNING)
    ]

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"📊  {group_name} — L0 Snapshot — {date_str}",
                "emoji": True,
            },
        },
        {"type": "context", "elements": [{"type": "mrkdwn", "text": ts_str}]},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Overall:* {emoji} *{label}*   ·   "
                    f"*{n_total}* services   ·   "
                    f"🔴 {n_crit} critical   🟡 {n_warn} warning   🟢 {n_ok} healthy"
                ),
            },
        },
    ]

    if flagged:
        flag_text = "\n".join(
            f"{'🔴' if r.status == Status.CRITICAL else '🟡'} *{svc}*"
            for svc, r in flagged[:6]
        )
        if len(flagged) > 6:
            flag_text += f"\n_+{len(flagged) - 6} more — see canvas_"
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": flag_text},
        })

    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": "L0 metrics + health breakdown → canvas below ↓"}],
    })
    return blocks


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
