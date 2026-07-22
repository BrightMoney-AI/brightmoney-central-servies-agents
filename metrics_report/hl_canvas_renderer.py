"""
hl_canvas_renderer.py — Renders one high-level canvas per report group.

Three-tier structure per canvas:
  L0  Trends & Instance Health  — scan daily (<1 min)
  L1  Current State Detail       — drill when L0 flags
  L2  Deep Analysis              — root cause / historical context

Consumed by hl_scheduler.py.  Existing canvas_renderer.py is unchanged.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Optional

from .models import AirflowHealth, KafkaConnectHealth, L0Report, Status
from .renderer import (
    IST,
    _endpoint_is_flagged,
    _flag_reasons,
    _fmt_hits,
    _fmt_p99,
    _fmt_pct,
)

log = logging.getLogger(__name__)

_EMOJI = {
    Status.HEALTHY:  "🟢",
    Status.WARNING:  "🟡",
    Status.CRITICAL: "🔴",
    Status.UNKNOWN:  "⚪",
}

# ── Trend helpers ──────────────────────────────────────────────────────────────

def _latency_trend(current: Optional[float], baseline: Optional[float]) -> tuple[str, str]:
    """Return (display_str, status_icon) for P50 latency."""
    if current is None:
        return "—", "⚪"
    cur = float(current)
    if baseline is None or baseline <= 0:
        icon = "🔴" if cur >= 1000 else ("🟡" if cur >= 500 else "🟢")
        return _fmt_p99(cur), icon
    ratio = cur / float(baseline)
    pct   = (ratio - 1) * 100
    icon  = "🔴" if ratio >= 2.0 else ("🟡" if ratio >= 1.5 else "🟢")
    sign  = "▲" if pct > 0 else "▼"
    return f"{_fmt_p99(cur)} {sign} {abs(pct):.0f}% vs 7d", icon


def _success_trend(current: Optional[float], baseline: Optional[float]) -> tuple[str, str]:
    """Return (display_str, status_icon) for success rate."""
    if current is None:
        return "—", "⚪"
    cur = float(current)
    if baseline is None:
        icon = "🔴" if cur < 95 else ("🟡" if cur < 99 else "🟢")
        return f"{cur:.1f}%", icon
    drop = float(baseline) - cur
    icon = "🔴" if drop >= 10.0 else ("🟡" if drop >= 5.0 else "🟢")
    sign = "▼" if drop > 0 else "▲"
    return f"{cur:.1f}% {sign} {abs(drop):.1f} pp vs 7d", icon


def _error_trend(current: Optional[float], baseline: Optional[float]) -> tuple[str, str]:
    """Return (display_str, status_icon) for error rate."""
    if current is None:
        return "—", "⚪"
    cur = float(current)
    if baseline is None or baseline < 0.5:
        icon = "🔴" if cur >= 5.0 else ("🟡" if cur >= 1.0 else "🟢")
        return f"{cur:.2f}%", icon
    ratio = cur / float(baseline)
    icon  = "🔴" if ratio >= 3.0 else ("🟡" if ratio >= 2.0 else "🟢")
    sign  = "▲" if cur > float(baseline) else "▼"
    return f"{cur:.2f}% ({sign} {ratio:.1f}× vs 7d)", icon


# ── CDC sink helpers (avoids private import from dp_l0_renderer) ───────────────

_SINK_SUFFIXES = ["-iceberg-cdc-sink-v3", "-iceberg-cdc-sink-v2", "-cdc-sink-v2"]


def _short_sink(name: str) -> str:
    for sfx in _SINK_SUFFIXES:
        if name.endswith(sfx):
            return name[: -len(sfx)]
    return name


def _sink_icon(s: Any) -> str:
    coord  = getattr(s, "coord_status", "ok")
    lag    = getattr(s, "lag_delta_status", "ok")
    hb     = getattr(s, "heartbeat_status", "ok")
    if "critical" in (coord, lag, hb):
        return "🔴"
    if getattr(s, "is_flagged", False):
        return "🟡"
    return "🟢"


# ── multi_col_table renderer helper ───────────────────────────────────────────

def _table_from_metric(m: Any) -> list[str]:
    """Render a multi_col_table BusinessMetric as markdown table lines."""
    if not m.details or len(m.details) < 2:
        return [f"_No data for {m.display_name}_", ""]
    hdrs  = [h.strip() for h in m.details[0].split("|")]
    lines = [
        "| " + " | ".join(hdrs) + " |",
        "|" + "|".join("---" for _ in hdrs) + "|",
    ]
    for row in m.details[1:]:
        parts = [p.strip() for p in row.split("|")]
        while len(parts) < len(hdrs):
            parts.append("—")
        lines.append("| " + " | ".join(parts[: len(hdrs)]) + " |")
    lines.append("")
    return lines


# ── Attention Required ─────────────────────────────────────────────────────────

def _render_attention(flags: list[tuple[int, str]]) -> str:
    lines = ["## ⚠️ Attention Required", ""]
    if not flags:
        lines += ["✅ All checks healthy — nothing to flag", ""]
        return "\n".join(lines)
    for _, text in sorted(flags, key=lambda x: x[0]):
        lines.append(text)
    lines.append("")
    return "\n".join(lines)


# ── L0 — Trends & Instance Health ─────────────────────────────────────────────

def _render_l0_service(
    svc_name: str,
    report: L0Report,
    flags: list[tuple[int, str]],
) -> list[str]:
    emoji = _EMOJI[report.status]
    label = report.status.value.upper()
    lines: list[str] = [f"### {svc_name} — {emoji} {label}", ""]

    rows: list[tuple[str, str, str]] = []

    sys      = report.system
    total    = len(sys.servers)
    srv_icon = "🔴" if sys.down > 0 else "🟢"
    if sys.down > 0:
        flags.append((0, f"🔴 {svc_name} · L0 · Servers · {sys.online}/{total} online ({sys.down} down)"))
    rows.append(("Servers", f"{sys.online} / {total} online", srv_icon))

    if report.show_api_metrics:
        a = report.api

        suc_str, suc_icon = _success_trend(a.success_rate_pct, a.success_rate_baseline_pct)
        rows.append(("Success Rate", suc_str, suc_icon))
        if suc_icon in ("🟡", "🔴"):
            flags.append((0 if suc_icon == "🔴" else 1, f"{suc_icon} {svc_name} · L0 · Success Rate · {suc_str}"))

        err_str, err_icon = _error_trend(a.error_rate_pct, a.error_rate_baseline_pct)
        rows.append(("Error Rate", err_str, err_icon))
        if err_icon in ("🟡", "🔴"):
            flags.append((0 if err_icon == "🔴" else 1, f"{err_icon} {svc_name} · L0 · Error Rate · {err_str}"))

        lat_str, lat_icon = _latency_trend(a.avg_latency_p50_ms, a.avg_latency_baseline_ms)
        rows.append(("Latency P50", lat_str, lat_icon))
        if lat_icon in ("🟡", "🔴"):
            flags.append((0 if lat_icon == "🔴" else 1, f"{lat_icon} {svc_name} · L0 · Latency P50 · {lat_str}"))

    lines.append("| Metric | Value | Status |")
    lines.append("|---|---|---|")
    for metric, value, icon in rows:
        lines.append(f"| {metric} | {value} | {icon} |")
    lines.append("")
    return lines


def _render_l0_uaa_biz(biz: list[Any], flags: list[tuple[int, str]]) -> list[str]:
    """UAA business trends table for L0."""
    if not biz:
        return []

    by_sec: dict[str, list[Any]] = defaultdict(list)
    for m in biz:
        by_sec[m.section].append(m)

    rows: list[str] = []

    # Onboarding sessions — sum D vs D-1 across providers
    for m in by_sec.get("Onboarding", []):
        if m.metric_type == "provider_comparison" and m.details:
            total_d = total_d1 = 0
            for row in m.details:
                parts = [p.strip() for p in row.split("|")]
                if len(parts) >= 3:
                    try:
                        total_d  += int(parts[1]) if parts[1] not in ("", "—") else 0
                        total_d1 += int(parts[2]) if parts[2] not in ("", "—") else 0
                    except ValueError:
                        pass
            if total_d1 > 0:
                pct   = (total_d - total_d1) / total_d1 * 100
                trend = f"{'▲' if pct >= 0 else '▼'} {abs(pct):.1f}% vs D-1"
            else:
                trend = "—"
            rows.append(f"| Onboarding Sessions | {total_d:,} | {trend} | — |")
            break

    # Account linking — sum yesterday vs day-before
    for m in by_sec.get("Account Linking", []):
        if m.metric_type == "source_comparison" and m.details:
            total_y = total_d = 0
            for row in m.details:
                parts = [p.strip() for p in row.split("|")]
                if len(parts) >= 4:
                    try:
                        total_y += int(parts[2]) if parts[2] not in ("", "—") else 0
                        total_d += int(parts[3]) if parts[3] not in ("", "—") else 0
                    except ValueError:
                        pass
            if total_d > 0:
                pct   = (total_y - total_d) / total_d * 100
                trend = f"{'▲' if pct >= 0 else '▼'} {abs(pct):.1f}% vs D-2"
            else:
                trend = "—"
            rows.append(f"| Account Linkings (D-1) | {total_y:,} | {trend} | — |")
            break

    # ALSM / SAISM P99 per aggregator vs yesterday
    for section_lbl in ("ALSM", "SAISM"):
        for m in by_sec.get(section_lbl, []):
            if m.metric_type == "multi_col_table" and len(m.details) >= 2:
                hdrs = [h.strip().lower() for h in m.details[0].split("|")]
                try:
                    p99_today_idx = next(i for i, h in enumerate(hdrs) if "p99" in h and "today" in h)
                    p99_yest_idx  = next(i for i, h in enumerate(hdrs) if "p99" in h and "yesterday" in h)
                except StopIteration:
                    continue
                for row in m.details[1:]:
                    parts = [p.strip() for p in row.split("|")]
                    if len(parts) <= max(p99_today_idx, p99_yest_idx):
                        continue
                    aggregator = parts[0] if parts else "?"
                    try:
                        today_v = float(parts[p99_today_idx])
                        yest_v  = float(parts[p99_yest_idx])
                        diff    = today_v - yest_v
                        sign    = "▲" if diff > 0 else "▼"
                        trend   = f"{sign} {abs(diff):.1f}s vs yesterday"
                        icon    = "🟡" if diff > 0 else "🟢"
                        if icon == "🟡":
                            flags.append((1, f"🟡 UAA · L0 · {section_lbl} P99 ({aggregator}) · {today_v:.1f}s {trend}"))
                        rows.append(f"| {section_lbl} P99 ({aggregator}) | {today_v:.1f}s | {trend} | {icon} |")
                    except (ValueError, IndexError):
                        pass

    # Plaid batch success — latest hour
    for m in by_sec.get("Plaid Batch Refresh", []):
        if "hourly" in m.display_name.lower() and len(m.details) >= 2:
            hdrs = [h.strip().lower() for h in m.details[0].split("|")]
            try:
                rate_idx = next(i for i, h in enumerate(hdrs) if "rate" in h)
            except StopIteration:
                continue
            parts = [p.strip() for p in m.details[-1].split("|")]
            if len(parts) > rate_idx:
                try:
                    rate = float(parts[rate_idx])
                    icon = "🔴" if rate < 90 else ("🟡" if rate < 95 else "🟢")
                    if icon in ("🟡", "🔴"):
                        sev = 0 if icon == "🔴" else 1
                        flags.append((sev, f"{icon} UAA · L0 · Plaid Batch Success · {rate:.1f}% (latest hour)"))
                    rows.append(f"| Plaid Batch Success | {rate:.1f}% | latest hour | {icon} |")
                except ValueError:
                    pass
            break

    if not rows:
        return []

    lines = ["### Business Trends — UAA", "", "| Metric | Current | Trend | Status |", "|---|---|---|---|"]
    lines += rows
    lines.append("")
    return lines


def _render_l0_dp(
    dp_biz: Optional[list[Any]],
    dp_l0: Any,
    emr: Any,
    airflow: Optional[AirflowHealth],
    flags: list[tuple[int, str]],
) -> list[str]:
    """DP data-quality trend counts for L0."""
    rows: list[str] = []

    if dp_biz:
        by_sec: dict[str, list[Any]] = defaultdict(list)
        for m in dp_biz:
            by_sec[m.section].append(m)

        for m in by_sec.get("Table Recency", []):
            if m.metric_type == "failure_count":
                n = int(m.value)
                icon = "🔴" if n > 0 else "🟢"
                if n > 0:
                    flags.append((0, f"🔴 DP · L0 · Stale CDC Tables · {n} stale"))
                rows.append(f"| Stale CDC Tables | {n} | {icon} |")
                break

        for m in by_sec.get("CDC Health", []):
            if m.metric_type == "failure_count" and (
                "dbz" in m.display_name.lower() or "invalid" in m.display_name.lower()
            ):
                n = int(m.value)
                icon = "🔴" if n > 0 else "🟢"
                if n > 0:
                    flags.append((0, f"🔴 DP · L0 · DBZ Invalid Tables · {n} invalid"))
                rows.append(f"| DBZ Invalid Tables | {n} | {icon} |")
                break

        for m in by_sec.get("Compaction", []):
            if m.metric_type == "failure_count":
                n = int(m.value)
                icon = "🔴" if n > 0 else "🟢"
                if n > 0:
                    flags.append((0, f"🔴 DP · L0 · Compaction Needed · {n} tables"))
                rows.append(f"| Compaction Needed | {n} | {icon} |")
                break

    # EMR breach count
    if emr and emr.sections:
        cube_sec = next((s for s in emr.sections if "health" in s.title.lower()), None)
        if cube_sec:
            n = cube_sec.flag_count
            icon = "🔴" if n > 0 else "🟢"
            if n > 0:
                flags.append((0, f"🔴 DP · L0 · EMR Cube Breaches · {n} breached"))
            rows.append(f"| EMR Cube Breaches | {n} | {icon} |")

    # CDC aggregate lag trend
    if dp_l0 and dp_l0.sinks:
        deltas = [s.offset_lag_delta for s in dp_l0.sinks if s.offset_lag_delta is not None]
        if deltas:
            total_delta = sum(deltas)
            if total_delta > 5_000:
                icon      = "🔴"
                trend_str = f"▲ +{total_delta:,.0f} msgs/24h (growing fast)"
                flags.append((0, f"🔴 DP · L0 · CDC Lag Trend · {trend_str}"))
            elif total_delta > 100:
                icon      = "🟡"
                trend_str = f"▲ +{total_delta:,.0f} msgs/24h (growing)"
                flags.append((1, f"🟡 DP · L0 · CDC Lag Trend · {trend_str}"))
            else:
                icon      = "🟢"
                trend_str = "stable" if total_delta >= 0 else f"▼ {abs(total_delta):,.0f} msgs/24h (draining)"
            rows.append(f"| CDC Lag Trend | {trend_str} | {icon} |")

    # Airflow DAG summary
    if airflow and (airflow.dag_runs or airflow.pipeline_runs):
        all_runs = list(airflow.dag_runs) + list(airflow.pipeline_runs)
        n_failed = sum(1 for r in all_runs if r.state == "failed")
        n_ok     = sum(1 for r in all_runs if r.state == "success")
        icon     = "🔴" if n_failed > 0 else "🟢"
        if n_failed > 0:
            flags.append((0, f"🔴 DP · L0 · Airflow DAGs · {n_failed} failed"))
        rows.append(f"| Airflow DAGs | {n_ok} ok / {n_failed} failed | {icon} |")

    if not rows:
        return []

    lines = ["### Data Quality Trends — DP", "", "| Metric | Count | Status |", "|---|---|---|"]
    lines += rows
    lines.append("")
    return lines


def _render_l0(
    group_name: str,
    reports: list[tuple[str, L0Report]],
    uaa_biz: Optional[list[Any]],
    central_biz: Optional[list[Any]],
    dp_biz: Optional[list[Any]],
    dp_l0: Any,
    emr: Any,
    airflow: Optional[AirflowHealth],
    flags: list[tuple[int, str]],
) -> str:
    lines: list[str] = ["## L0 — Trends & Instance Health", ""]
    for svc_name, report in reports:
        lines += _render_l0_service(svc_name, report, flags)

    if group_name == "UAA Services" and uaa_biz:
        lines += _render_l0_uaa_biz(uaa_biz, flags)
    elif group_name == "Data Platform":
        lines += _render_l0_dp(dp_biz, dp_l0, emr, airflow, flags)
    elif group_name == "Central Services" and central_biz:
        lines += _render_l0_central_scorecard(central_biz, flags)

    return "\n".join(lines)


def _render_l0_central_scorecard(central_biz: list[Any], flags: list[tuple[int, str]]) -> list[str]:
    if not central_biz:
        return []

    by_sec: dict[str, list[Any]] = defaultdict(list)
    for m in central_biz:
        by_sec[m.section].append(m)

    lines = ["### Business Metrics Scorecard — Central", ""]
    for section, items in sorted(by_sec.items()):
        flagged = [
            m for m in items
            if (
                (getattr(m, "crit_above", None) is not None and m.value >= m.crit_above)
                or (getattr(m, "warn_above", None) is not None and m.value >= m.warn_above)
                or (getattr(m, "crit_below", None) is not None and m.value <= m.crit_below)
                or (getattr(m, "warn_below", None) is not None and m.value <= m.warn_below)
            )
        ]
        n_flag  = len(flagged)
        n_total = len(items)
        icon    = "🔴" if n_flag > 0 else "🟢"
        if n_flag > 0:
            flags.append((0, f"🔴 Central · L0 · {section} · {n_flag}/{n_total} checks flagged"))
        lines.append(f"- {icon} **{section}** — {n_total - n_flag}/{n_total} checks healthy")

    lines.append("")
    return lines


# ── L1 — Current State Detail ──────────────────────────────────────────────────

def _render_l1_servers(
    reports: list[tuple[str, L0Report]],
    flags: list[tuple[int, str]],
) -> list[str]:
    CPU_WARN, CPU_CRIT = 70.0, 90.0
    MEM_WARN, MEM_CRIT = 75.0, 90.0
    DSK_WARN, DSK_CRIT = 80.0, 90.0

    lines: list[str] = []
    has_any = False
    for svc_name, report in reports:
        if not report.system.servers:
            continue
        if not has_any:
            lines += ["### Server Health", ""]
            has_any = True
        lines.append(f"**{svc_name}**")
        lines.append("")
        lines.append("| Server | CPU | Memory | Disk |")
        lines.append("|---|---|---|---|")
        for s in sorted(report.system.servers, key=lambda x: x.name):
            m        = s.metrics
            cpu_icon = "🔴" if m.cpu_pct  >= CPU_CRIT else ("🟡" if m.cpu_pct  >= CPU_WARN else "🟢")
            mem_icon = "🔴" if m.mem_pct  >= MEM_CRIT else ("🟡" if m.mem_pct  >= MEM_WARN else "🟢")
            dsk_icon = "🔴" if m.disk_pct >= DSK_CRIT else ("🟡" if m.disk_pct >= DSK_WARN else "🟢")
            for icon, metric, val in [
                (cpu_icon, "CPU",    m.cpu_pct),
                (mem_icon, "Memory", m.mem_pct),
                (dsk_icon, "Disk",   m.disk_pct),
            ]:
                if icon in ("🟡", "🔴"):
                    flags.append((0 if icon == "🔴" else 1, f"{icon} {svc_name} · L1 · {s.name} {metric} · {val:.0f}%"))
            lines.append(
                f"| `{s.name}` "
                f"| {cpu_icon} {m.cpu_pct:.0f}% "
                f"| {mem_icon} {m.mem_pct:.0f}% "
                f"| {dsk_icon} {m.disk_pct:.0f}% |"
            )
        lines.append("")
    return lines


def _render_l1_endpoints(
    reports: list[tuple[str, L0Report]],
    flags: list[tuple[int, str]],
) -> list[str]:
    lines: list[str] = []
    flagged_header_written = False
    top_header_written     = False

    for svc_name, report in reports:
        if not report.endpoints or not report.show_api_metrics:
            continue
        t = report.thresholds

        flagged_eps   = sorted(
            [ep for ep in report.endpoints if _endpoint_is_flagged(ep, t) and ep.hits >= 100],
            key=lambda e: e.hits, reverse=True,
        )
        unflagged_eps = sorted(
            [ep for ep in report.endpoints if not _endpoint_is_flagged(ep, t)],
            key=lambda e: e.hits, reverse=True,
        )

        if flagged_eps:
            if not flagged_header_written:
                lines += ["### Flagged Endpoints", ""]
                flagged_header_written = True
            lines.append(f"**{svc_name}**")
            lines.append("")
            for ep in flagged_eps:
                reasons = _flag_reasons(ep, t)
                if ep.success_baseline_pct is not None:
                    drop = ep.success_baseline_pct - ep.success_pct
                    suc_str  = f"{ep.success_pct:.1f}% (▼ {abs(drop):.0f} pp vs 7d)"
                    suc_icon = "🔴" if drop >= 10 else "🟡"
                else:
                    suc_str  = f"{ep.success_pct:.1f}%"
                    suc_icon = "🔴" if ep.success_pct < 80 else "🟡"
                lat_str  = _fmt_p99(ep.p99_ms)
                if ep.p99_baseline_ms and ep.p99_baseline_ms > 0:
                    ratio    = ep.p99_ms / ep.p99_baseline_ms
                    lat_str += f" ({ratio:.1f}× vs 7d)"
                    lat_icon = "🔴" if ratio >= 2.0 else "🟡"
                else:
                    lat_icon = "🟢"
                flags.append((
                    0 if suc_icon == "🔴" or lat_icon == "🔴" else 1,
                    f"{suc_icon} {svc_name} · L1 · {ep.path} · {suc_str}",
                ))
                lines.append(
                    f"- `{ep.path}` · {_fmt_hits(ep.hits)} hits · "
                    f"{suc_icon} {suc_str} · {lat_icon} {lat_str} · "
                    f"_{', '.join(reasons)}_"
                )
            lines.append("")

        if unflagged_eps[:5]:
            if not top_header_written:
                lines += ["### Top Endpoints (healthy)", ""]
                top_header_written = True
            lines.append(f"**{svc_name} — top {min(5, len(unflagged_eps))} by volume**")
            lines.append("")
            for ep in unflagged_eps[:5]:
                lines.append(
                    f"- `{ep.path}` · {_fmt_hits(ep.hits)} hits · "
                    f"{_fmt_pct(ep.success_pct)} · {_fmt_p99(ep.p99_ms)} p99"
                )
            if len(unflagged_eps) > 5:
                lines.append(f"_+{len(unflagged_eps) - 5} more, all healthy_")
            lines.append("")

    return lines


def _render_l1_queues(
    reports: list[tuple[str, L0Report]],
    flags: list[tuple[int, str]],
) -> list[str]:
    services = [(n, r) for n, r in reports if r.queues and r.queues.queues]
    if not services:
        return []
    lines: list[str] = ["### Queue Depths", ""]
    for svc_name, report in services:
        lines.append(f"**{svc_name}**")
        lines.append("")
        lines.append("| Queue | Ready | Unacked | Total |")
        lines.append("|---|---|---|---|")
        for q in sorted(report.queues.queues, key=lambda x: x.ready, reverse=True):
            icon = "🔴" if q.ready >= 500 else ("🟡" if q.ready >= 100 else "🟢")
            if icon in ("🟡", "🔴"):
                flags.append((0 if icon == "🔴" else 1, f"{icon} {svc_name} · L1 · Queue {q.name} · {q.ready} ready"))
            lines.append(f"| `{q.name}` | {icon} {q.ready} | {q.unacked} | {q.total} |")
        lines.append("")
    return lines


def _render_l1_uaa_biz(biz: list[Any]) -> list[str]:
    """L1 comparison and trend tables for UAA."""
    if not biz:
        return []
    by_sec: dict[str, list[Any]] = defaultdict(list)
    for m in biz:
        by_sec[m.section].append(m)

    lines: list[str] = []

    # Onboarding by provider
    for m in by_sec.get("Onboarding", []):
        if m.metric_type == "provider_comparison" and m.details:
            lines += ["### Onboarding by Provider", ""]
            lines += [
                "| Provider | D Sessions | D-1 Sessions | D Success | D-1 Success |",
                "|---|---|---|---|---|",
            ]
            for row in m.details:
                parts = [p.strip() for p in row.split("|")]
                if len(parts) >= 5:
                    lines.append("| " + " | ".join(parts[:5]) + " |")
            lines.append("")
            break

    # Account linking by source
    for m in by_sec.get("Account Linking", []):
        if m.metric_type == "source_comparison" and m.details:
            lines += ["### Account Linking by Source", ""]
            lines += [
                "| Source | Flow | Yesterday | Day Before | Change |",
                "|---|---|---|---|---|",
            ]
            for row in m.details:
                parts = [p.strip() for p in row.split("|")]
                if len(parts) >= 5:
                    delta = parts[4]
                    delta_fmt = f"🟢 {delta}" if not delta.startswith("-") else f"🔴 {delta}"
                    lines.append(f"| {parts[0]} | {parts[1]} | {parts[2]} | {parts[3]} | {delta_fmt} |")
            lines.append("")
            break

    # ALSM / SAISM latency tables
    for section_lbl in ("ALSM", "SAISM"):
        for m in by_sec.get(section_lbl, []):
            if m.metric_type == "multi_col_table" and len(m.details) >= 2:
                lines.append(f"### {section_lbl} Latency (P50 / P99 today vs yesterday)")
                lines.append("")
                lines += _table_from_metric(m)
                break

    # Plaid batch hourly trend
    for m in by_sec.get("Plaid Batch Refresh", []):
        if "hourly" in m.display_name.lower() and len(m.details) >= 2:
            lines += ["### Plaid Batch — Hourly Trend (last 24h)", ""]
            lines += _table_from_metric(m)
            break

    return lines


def _render_l1_airflow(airflow: AirflowHealth) -> list[str]:
    """Simplified Airflow summary for HL L1."""
    from datetime import datetime, timedelta, timezone
    lines: list[str] = ["### Airflow DAGs", ""]

    IST_TZ    = timezone(timedelta(hours=5, minutes=30))
    today_ist = datetime.now(IST_TZ).date()
    yesterday = today_ist - timedelta(days=1)

    if airflow.pipeline_runs:
        run_index = {(r.dag_id, r.run_date): r for r in airflow.pipeline_runs if r.run_date}
        dag_ids   = sorted({r.dag_id for r in airflow.pipeline_runs})

        def _state_icon(state: str) -> str:
            return {"success": "🟢", "failed": "🔴", "running": "🔵"}.get(state, "⚪")

        def _cell(dag_id: str, d) -> str:
            r = run_index.get((dag_id, d))
            if not r:
                return "—"
            started = r.start_date.strftime("%H:%M IST") if r.start_date else ""
            suffix  = f" ({started})" if started else ""
            return f"{_state_icon(r.state)} {r.state}{suffix}"

        lines += ["| DAG | Today | Yesterday |", "|---|---|---|"]
        for dag_id in dag_ids:
            lines.append(f"| `{dag_id}` | {_cell(dag_id, today_ist)} | {_cell(dag_id, yesterday)} |")
        lines.append("")

    if airflow.dag_runs:
        def _state_icon2(state: str) -> str:
            return {"success": "🟢", "failed": "🔴", "running": "🔵"}.get(state, "⚪")

        lines += ["| DAG | State | Last run |", "|---|---|---|"]
        for d in sorted(airflow.dag_runs, key=lambda x: x.dag_id):
            started = d.start_date.strftime("%d %b %H:%M IST") if d.start_date else "—"
            lines.append(f"| `{d.dag_id}` | {_state_icon2(d.state)} {d.state} | {started} |")
        lines.append("")

    if airflow.view_flow:
        vf = airflow.view_flow
        n_other = vf.total - vf.successful - len(vf.failed) - len(vf.running)
        parts = [f"{vf.total} runs", f"🟢 {vf.successful} success"]
        if vf.failed:
            parts.append(f"🔴 {len(vf.failed)} failed")
        if vf.running:
            parts.append(f"🔵 {len(vf.running)} running")
        if n_other > 0:
            parts.append(f"⚪ {n_other} other")
        lines.append("**dp_cosmos_execute_view_flow · last 24h:** " + "  ·  ".join(parts))
        lines.append("")
        if vf.failed:
            lines += ["| Failed table | Started |", "|---|---|"]
            for r in sorted(vf.failed, key=lambda x: x.table_name):
                started = r.start_date.strftime("%d %b %H:%M IST") if r.start_date else "—"
                lines.append(f"| `{r.table_name}` | {started} |")
            lines.append("")

    return lines


def _render_l1_dp(
    dp_l0: Any,
    connector_health: Optional[KafkaConnectHealth],
    airflow: Optional[AirflowHealth],
    dp_biz: Optional[list[Any]],
    flags: list[tuple[int, str]],
) -> list[str]:
    lines: list[str] = []

    # Iceberg / Debezium VM disk
    if dp_l0 and dp_l0.vm_disks:
        flagged_vms = dp_l0.flagged_vms
        if flagged_vms:
            lines += ["### Iceberg / Debezium VM Disk", ""]
            lines += ["| VM | Disk % | Status |", "|---|---|---|"]
            for v in sorted(flagged_vms, key=lambda x: x.disk_pct, reverse=True):
                icon = "🔴" if v.disk_pct >= 90 else "🟡"
                flags.append((0 if icon == "🔴" else 1, f"{icon} DP · L1 · {v.vm_name} disk · {v.disk_pct:.1f}%"))
                lines.append(f"| `{v.vm_name}` | {v.disk_pct:.1f}% | {icon} |")
            lines.append("")

    # CDC per-sink detail
    if dp_l0 and dp_l0.sinks:
        lines += ["### CDC Sinks — Per Sink Detail", ""]
        lines += ["| Sink | Coord Lag | Offset Lag | Lag Δ 24h | Heartbeat | Status |", "|---|---|---|---|---|---|"]
        for s in sorted(dp_l0.sinks, key=lambda x: x.sink):
            icon   = _sink_icon(s)
            coord  = f"{s.coord_lag:,.0f}" if s.coord_lag is not None else "—"
            offset = f"{s.offset_lag:,.0f}" if s.offset_lag is not None else "—"
            if s.offset_lag_delta is not None:
                delta = f"+{s.offset_lag_delta:,.0f}" if s.offset_lag_delta > 0 else f"{s.offset_lag_delta:,.0f}"
            else:
                delta = "—"
            hb = f"{s.heartbeat_rate:.1f}" if s.heartbeat_rate is not None else "—"
            lines.append(f"| `{_short_sink(s.sink)}` | {coord} | {offset} | {delta} | {hb} msg/5m | {icon} |")
        lines.append("")

    # Kafka Connect connector health
    if connector_health and connector_health.instances:
        any_unhealthy = any(inst.unhealthy for inst in connector_health.instances)
        if any_unhealthy:
            lines += ["### Kafka Connectors — Unhealthy", ""]
            for inst in connector_health.instances:
                if not inst.unhealthy:
                    continue
                lines.append(f"**{inst.name}**")
                lines.append("")
                lines += ["| Connector | State | Tasks |", "|---|---|---|"]
                for c in sorted(inst.unhealthy, key=lambda x: x.name):
                    running  = sum(1 for t in c.tasks if t.state == "RUNNING")
                    task_str = f"{running}/{len(c.tasks)} running" if c.tasks else "—"
                    icon     = "🔴" if c.state in ("FAILED", "UNKNOWN") else "🟡"
                    flags.append((0 if icon == "🔴" else 1, f"{icon} DP · L1 · Connector {c.name} · {c.state}"))
                    lines.append(f"| `{c.name}` | {icon} {c.state} | {task_str} |")
                lines.append("")
        else:
            total = sum(inst.total for inst in connector_health.instances)
            lines += [f"### Kafka Connectors — ✅ All {total} healthy", ""]

    # Airflow
    if airflow and (airflow.dag_runs or airflow.pipeline_runs or airflow.view_flow):
        lines += _render_l1_airflow(airflow)
        if airflow.view_flow and airflow.view_flow.failed:
            flags.append((0, f"🔴 DP · L1 · View Flow · {len(airflow.view_flow.failed)} failed table refreshes"))

    # Stale and invalid table names
    if dp_biz:
        by_sec: dict[str, list[Any]] = defaultdict(list)
        for m in dp_biz:
            by_sec[m.section].append(m)

        for m in by_sec.get("Table Recency", []):
            if m.metric_type == "failure_count" and m.value > 0 and m.details:
                lines += ["### Stale CDC Tables", ""]
                for tbl in m.details[:20]:
                    lines.append(f"- `{tbl}`")
                if len(m.details) > 20:
                    lines.append(f"_+{len(m.details) - 20} more_")
                lines.append("")
                break

        for m in by_sec.get("CDC Health", []):
            if (
                m.metric_type == "failure_count" and m.value > 0 and m.details
                and ("dbz" in m.display_name.lower() or "invalid" in m.display_name.lower())
            ):
                lines += ["### DBZ Invalid Tables", ""]
                for tbl in m.details[:20]:
                    lines.append(f"- `{tbl}`")
                if len(m.details) > 20:
                    lines.append(f"_+{len(m.details) - 20} more_")
                lines.append("")
                break

    return lines


def _render_l1(
    group_name: str,
    reports: list[tuple[str, L0Report]],
    uaa_biz: Optional[list[Any]],
    dp_l0: Any,
    connector_health: Optional[KafkaConnectHealth],
    airflow: Optional[AirflowHealth],
    dp_biz: Optional[list[Any]],
    flags: list[tuple[int, str]],
) -> str:
    lines: list[str] = ["## L1 — Current State Detail", ""]
    lines.append("_Drill down when L0 shows 🟡 or 🔴_")
    lines.append("")

    server_lines   = _render_l1_servers(reports, flags)
    endpoint_lines = _render_l1_endpoints(reports, flags)
    queue_lines    = _render_l1_queues(reports, flags)

    lines += server_lines
    if server_lines and endpoint_lines:
        lines += ["---", ""]
    lines += endpoint_lines
    if (server_lines or endpoint_lines) and queue_lines:
        lines += ["---", ""]
    lines += queue_lines

    if group_name == "UAA Services" and uaa_biz:
        biz_l1 = _render_l1_uaa_biz(uaa_biz)
        if biz_l1:
            if server_lines or endpoint_lines or queue_lines:
                lines += ["---", ""]
            lines += biz_l1

    if group_name == "Data Platform":
        dp_l1 = _render_l1_dp(dp_l0, connector_health, airflow, dp_biz, flags)
        if dp_l1:
            if server_lines or endpoint_lines or queue_lines:
                lines += ["---", ""]
            lines += dp_l1

    content = "\n".join(lines)
    # Return empty if nothing was written beyond the header
    if content.strip() == "## L1 — Current State Detail\n\n_Drill down when L0 shows 🟡 or 🔴_":
        return ""
    return content


# ── L2 — Deep Analysis ────────────────────────────────────────────────────────

def _render_l2_uaa(biz: list[Any]) -> list[str]:
    if not biz:
        return []
    by_sec: dict[str, list[Any]] = defaultdict(list)
    for m in biz:
        by_sec[m.section].append(m)

    lines: list[str] = []

    for display_filter, section_name, heading in [
        ("error",       "Plaid Batch Refresh",  "Plaid Batch — Error Reasons (last 24h)"),
        ("historical",  "Plaid Batch Refresh",  "Plaid Batch — Historical Recency"),
        ("error",       "Plaid Force Refresh",  "Plaid Force Refresh — Error Reasons (last 7 days)"),
    ]:
        for m in by_sec.get(section_name, []):
            if display_filter in m.display_name.lower() and m.metric_type == "multi_col_table":
                lines += [f"### {heading}", ""] + _table_from_metric(m)
                break

    for section_name, heading in [
        ("Partner Costs", "Partner Costs (yesterday)"),
        ("Txn Quality",   "Txn Quality by Cohort"),
    ]:
        for m in by_sec.get(section_name, []):
            if m.metric_type == "multi_col_table":
                lines += [f"### {heading}", ""] + _table_from_metric(m)
                break

    return lines


def _render_l2_central(central_biz: list[Any]) -> list[str]:
    if not central_biz:
        return []
    by_sec: dict[str, list[Any]] = defaultdict(list)
    for m in central_biz:
        by_sec[m.section].append(m)

    lines: list[str] = []
    for section, items in sorted(by_sec.items()):
        lines += [f"### {section}", "", "| Metric | Value |", "|---|---|"]
        for m in items:
            lines.append(f"| {m.display_name} | {m.value} |")
        lines.append("")
    return lines


def _render_l2_dp(dp_biz: Optional[list[Any]], emr: Any) -> list[str]:
    lines: list[str] = []

    if dp_biz:
        by_sec: dict[str, list[Any]] = defaultdict(list)
        for m in dp_biz:
            by_sec[m.section].append(m)

        for section_name in ("Validation", "View Health", "Compaction"):
            for m in by_sec.get(section_name, []):
                if m.metric_type == "failure_count" and m.value > 0 and m.details:
                    lines += [f"### {m.display_name}", ""]
                    for item in m.details[:20]:
                        lines.append(f"- `{item}`")
                    if len(m.details) > 20:
                        lines.append(f"_+{len(m.details) - 20} more_")
                    lines.append("")

    if emr and emr.sections:
        for section in emr.sections:
            if not section.rows or section.failed or not section.headers:
                continue
            lines += [f"### EMR — {section.title}", ""]
            lines += [
                "| " + " | ".join(section.headers) + " |",
                "|" + "|".join("---" for _ in section.headers) + "|",
            ]
            for row in section.rows:
                flag_marker = " 🔴" if row.flagged else ""
                lines.append("| " + " | ".join(row.cells) + f" |{flag_marker}")
            lines.append("")

    return lines


def _render_l2(
    group_name: str,
    uaa_biz: Optional[list[Any]],
    central_biz: Optional[list[Any]],
    dp_biz: Optional[list[Any]],
    emr: Any,
) -> str:
    if group_name == "UAA Services":
        content = _render_l2_uaa(uaa_biz)
    elif group_name == "Central Services":
        content = _render_l2_central(central_biz)
    elif group_name == "Data Platform":
        content = _render_l2_dp(dp_biz, emr)
    else:
        return ""

    if not content:
        return ""

    lines = [
        "## L2 — Deep Analysis",
        "",
        "_Root cause / historical context — drill when L1 doesn't fully explain_",
        "",
    ]
    lines += content
    return "\n".join(lines)


# ── UKS Services rendering ────────────────────────────────────────────────────

def _render_l0_uks(uks: Any, flags: list[tuple[int, str]]) -> list[str]:
    if uks is None:
        return []
    lines: list[str] = ["### UKS KYC — Overview", ""]
    lines += ["| Metric | Value | Status |", "|---|---|---|"]

    # KYC pass rate
    if uks.kyc_pass_rate is not None:
        icon = "🔴" if uks.kyc_pass_rate < 90 else ("🟡" if uks.kyc_pass_rate < 95 else "🟢")
        rpm  = f"  ·  {uks.kyc_per_min:.1f}/min" if uks.kyc_per_min else ""
        lines.append(f"| KYC Pass Rate | {uks.kyc_pass_rate:.1f}%{rpm} | {icon} |")
        if icon in ("🟡", "🔴"):
            flags.append((0 if icon == "🔴" else 1, f"{icon} UKS · L0 · KYC Pass Rate · {uks.kyc_pass_rate:.1f}%"))
        if uks.kyc_fail_rate is not None:
            lines.append(f"| KYC Fail Rate | {uks.kyc_fail_rate:.1f}% | {'🔴' if uks.kyc_fail_rate > 10 else '🟡' if uks.kyc_fail_rate > 5 else '🟢'} |")
    else:
        lines.append("| KYC Pass Rate | — | ⚪ |")

    # Task summary — count flagged
    flagged_tasks = [t for t in uks.tasks if t.success_rate is not None and t.success_rate < 95.0]
    total_tasks   = len(uks.tasks)
    if total_tasks:
        task_icon = "🔴" if flagged_tasks else "🟢"
        task_val  = f"{total_tasks - len(flagged_tasks)}/{total_tasks} healthy"
        lines.append(f"| Celery Tasks | {task_val} | {task_icon} |")
        for t in flagged_tasks:
            icon = "🔴" if t.success_rate is not None and t.success_rate < 90 else "🟡"
            flags.append((0 if icon == "🔴" else 1, f"{icon} UKS · L0 · Task {t.name} · {t.success_rate:.1f}% success"))

    lines.append("")
    return lines


def _render_l1_uks(uks: Any, flags: list[tuple[int, str]]) -> list[str]:
    if uks is None:
        return []
    lines: list[str] = []

    # Per-task table
    if uks.tasks:
        lines += ["### UKS Celery Tasks", ""]
        lines += ["| Task | Success Rate | P99 Latency |", "|---|---|---|"]
        for t in sorted(uks.tasks, key=lambda x: x.name):
            suc  = f"{t.success_rate:.1f}%" if t.success_rate is not None else "—"
            p99  = f"{t.p99_ms:.0f} ms"    if t.p99_ms is not None else "—"
            icon = ("🔴" if t.success_rate is not None and t.success_rate < 90
                    else "🟡" if t.success_rate is not None and t.success_rate < 95
                    else "🟢")
            lines.append(f"| `{t.name}` | {icon} {suc} | {p99} |")
        lines.append("")

    # Per-view API table
    if uks.api_views:
        lines += ["### UKS Incoming API — By View", ""]
        lines += ["| View | Success Rate | Req/min |", "|---|---|---|"]
        for v in sorted(uks.api_views, key=lambda x: -(x.req_per_min or 0)):
            suc = f"{v.success_rate:.1f}%" if v.success_rate is not None else "—"
            rpm = f"{v.req_per_min:.1f}"   if v.req_per_min is not None else "—"
            icon = ("🔴" if v.success_rate is not None and v.success_rate < 90
                    else "🟡" if v.success_rate is not None and v.success_rate < 95
                    else "🟢")
            lines.append(f"| `{v.view}` | {icon} {suc} | {rpm} |")
        lines.append("")

    return lines


# ── Public entry point ─────────────────────────────────────────────────────────

def render_hl_canvas(
    group_name: str,
    reports: list[tuple[str, L0Report]],
    title: str = "",
    uaa_biz_metrics: Optional[list[Any]] = None,
    central_biz_metrics: Optional[list[Any]] = None,
    dp_biz_metrics: Optional[list[Any]] = None,
    dp_l0_report: Optional[Any] = None,
    emr_report: Optional[Any] = None,
    connector_health: Optional[KafkaConnectHealth] = None,
    airflow_health: Optional[AirflowHealth] = None,
    uks_metrics: Optional[Any] = None,
) -> str:
    flags: list[tuple[int, str]] = []

    l0 = _render_l0(
        group_name, reports,
        uaa_biz_metrics, central_biz_metrics, dp_biz_metrics,
        dp_l0_report, emr_report, airflow_health, flags,
    )
    if group_name == "UKS Services":
        l0 += "\n" + "\n".join(_render_l0_uks(uks_metrics, flags))

    l1 = _render_l1(
        group_name, reports,
        uaa_biz_metrics, dp_l0_report, connector_health, airflow_health, dp_biz_metrics, flags,
    )
    if group_name == "UKS Services":
        uks_l1 = _render_l1_uks(uks_metrics, flags)
        if uks_l1:
            l1 = (l1 + "\n" + "\n".join(uks_l1)) if l1 else "\n".join(uks_l1)

    l2 = _render_l2(group_name, uaa_biz_metrics, central_biz_metrics, dp_biz_metrics, emr_report)

    attn   = _render_attention(flags)
    header = f"# {title}\n\n" if title else ""

    parts = [attn, "", "---", "", l0]
    if l1:
        parts += ["", "---", "", l1]
    if l2:
        parts += ["", "---", "", l2]
    parts += [
        "", "---", "",
        "🟢 Healthy   🟡 Warning   🔴 Critical   ·   "
        "L0: scan daily · L1: drill on flags · L2: root cause   ·   "
        "brightmoney observability",
    ]

    return header + "\n".join(parts)
