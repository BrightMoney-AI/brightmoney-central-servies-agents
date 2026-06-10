"""
dp_l0_renderer.py — Renders DPL0Report as Slack Canvas markdown.

Only flagged sinks / VMs appear; healthy ones are collapsed to a single summary line.
Title posted as "Data Platform — Sink Health — <date>".
"""
from __future__ import annotations

from typing import Optional

from .dp_l0_collector import (
    DPL0Report, SinkHealth, VMDiskHealth,
    _COORD_LAG_CRIT, _COORD_LAG_WARN,
    _HEARTBEAT_MIN, _DISK_WARN_PCT, _DISK_CRIT_PCT,
    _LAG_DELTA_CRIT,
)


# ── icon helpers ───────────────────────────────────────────────────────────────

def _sink_overall_icon(s: SinkHealth) -> str:
    if (
        s.coord_status == "critical"
        or s.lag_delta_status == "critical"
        or s.heartbeat_status == "critical"
    ):
        return "🔴"
    if s.is_flagged:
        return "🟡"
    return "🟢"


def _disk_icon(v: VMDiskHealth) -> str:
    return "🔴" if v.status == "critical" else "🟡"


# ── sink short name ────────────────────────────────────────────────────────────

_SINK_SUFFIXES = ["-iceberg-cdc-sink-v3", "-iceberg-cdc-sink-v2", "-cdc-sink-v2"]


def _short(sink: str) -> str:
    for sfx in _SINK_SUFFIXES:
        if sink.endswith(sfx):
            return sink[: -len(sfx)]
    return sink


# ── per-sink block ─────────────────────────────────────────────────────────────

def _render_sink(s: SinkHealth) -> str:
    icon = _sink_overall_icon(s)
    lines: list[str] = [f"**{icon} `{_short(s.sink)}`**"]

    # Coord lag
    if s.coord_lag is not None:
        c_icon = "🔴" if s.coord_status == "critical" else ("🟡" if s.coord_status == "warning" else "🟢")
        lines.append(f"  - Coord Lag: {c_icon} {s.coord_lag:,.0f}  _(warn >{_COORD_LAG_WARN:,} · crit >{_COORD_LAG_CRIT:,})_")
    else:
        lines.append("  - Coord Lag: ⚪ no data")

    # Offset lag + 24 h trend
    if s.offset_lag is not None:
        trend_str = ""
        if s.lag_increasing:
            delta_icon = "🔴" if s.lag_delta_status == "critical" else "🟡"
            delta_val  = f"{s.offset_lag_delta:+,.0f}" if s.offset_lag_delta is not None else "?"
            trend_str  = f"  {delta_icon} **+{_fmt_abs(s.offset_lag_delta)} over 24 h** (still growing)"
        elif s.offset_lag_delta is not None and s.offset_lag_delta < 0:
            trend_str = f"  🟢 {_fmt_abs(s.offset_lag_delta)} recovered over 24 h"
        lines.append(f"  - Offset Lag: {s.offset_lag:,.0f}{trend_str}")
    else:
        lines.append("  - Offset Lag: ⚪ no data")

    # Throughput (heartbeat rate)
    if s.heartbeat_topic is None:
        lines.append("  - Throughput: — no heartbeat configured")
    elif s.heartbeat_rate is None:
        lines.append(f"  - Throughput: 🔴 no data  _(connector may be down)_")
    elif s.heartbeat_rate < _HEARTBEAT_MIN:
        lines.append(f"  - Throughput: 🔴 {s.heartbeat_rate:.1f} msg/5m  _(< {_HEARTBEAT_MIN} → stalled)_")
    else:
        lines.append(f"  - Throughput: 🟢 {s.heartbeat_rate:.1f} msg/5m")

    return "\n".join(lines)


def _fmt_abs(v: Optional[float]) -> str:
    if v is None:
        return "?"
    return f"{abs(v):,.0f}"


# ── public entry point ─────────────────────────────────────────────────────────

def render_dp_l0_canvas(report: DPL0Report, title: str = "") -> str:
    lines: list[str] = []

    if title:
        lines += [f"# {title}", ""]

    total   = len(report.sinks)
    flagged = report.flagged_sinks
    n_flag  = len(flagged)
    n_ok    = total - n_flag

    # ── CDC Sinks section ──────────────────────────────────────────────────────
    if n_flag == 0:
        lines += [f"## ✅ CDC Sinks — all {total} healthy", ""]
    else:
        lines += [f"## CDC Sinks — {n_flag} flagged  ·  {n_ok} healthy", ""]

        # sort: critical first, then by sink name
        sorted_flagged = sorted(
            flagged,
            key=lambda s: (0 if _sink_overall_icon(s) == "🔴" else 1, s.sink),
        )
        for s in sorted_flagged:
            lines += [_render_sink(s), ""]

    # ── Kafka Sinks section ────────────────────────────────────────────────────
    if report.kafka_sinks:
        lines += ["---", ""]
        n_kflag = len(report.flagged_kafka_sinks)
        n_kok   = len(report.kafka_sinks) - n_kflag
        if n_kflag == 0:
            lines += [f"## ✅ Kafka Sinks — all {len(report.kafka_sinks)} healthy", ""]
        else:
            lines += [f"## Kafka Sinks — {n_kflag} flagged  ·  {n_kok} healthy", ""]
            sorted_kflag = sorted(
                report.flagged_kafka_sinks,
                key=lambda s: (0 if _sink_overall_icon(s) == "🔴" else 1, s.sink),
            )
            for s in sorted_kflag:
                lines += [_render_sink(s), ""]

    # ── Throughput summary — CDC sinks only (Kafka sinks have no heartbeat) ────
    lines += ["---", "", "## Heartbeat Throughput — CDC sinks (msg/5m)", ""]
    for s in sorted(report.sinks, key=lambda x: x.sink):
        name = _short(s.sink)
        if s.heartbeat_topic is None:
            icon, val = "⚪", "—"
        elif s.heartbeat_rate is None:
            icon, val = "🔴", "no data"
        elif s.heartbeat_rate < _HEARTBEAT_MIN:
            icon, val = "🔴", f"{s.heartbeat_rate:.1f}"
        else:
            icon, val = "🟢", f"{s.heartbeat_rate:.1f}"
        lines.append(f"- {icon} `{name}`: {val}")
    lines.append("")

    # ── VM Disk section ────────────────────────────────────────────────────────
    lines.append("---")
    lines.append("")

    all_vms    = report.vm_disks
    flagged_vm = report.flagged_vms

    if not all_vms:
        lines += ["## ⚪ Iceberg Sink VMs — no disk data", ""]
    elif not flagged_vm:
        lines += [f"## ✅ Iceberg Sink VMs — all {len(all_vms)} disk healthy", ""]
    else:
        lines += [f"## Disk — {len(flagged_vm)} VM(s) above threshold", ""]
        for v in sorted(flagged_vm, key=lambda x: x.disk_pct, reverse=True):
            icon = _disk_icon(v)
            lines.append(f"- {icon} `{v.vm_name}` — {v.disk_pct:.1f}%  _(warn >{_DISK_WARN_PCT:.0f}% · crit >{_DISK_CRIT_PCT:.0f}%)_")
        lines.append("")

        ok_vms = [v for v in all_vms if not v.is_flagged]
        if ok_vms:
            lines.append(f"_🟢 {len(ok_vms)} VM(s) disk healthy_")
            lines.append("")

    lines.append("---")
    lines.append("")
    lines.append(
        f"_Thresholds: coord lag crit >{_COORD_LAG_CRIT:,} · "
        f"lag delta crit >{_LAG_DELTA_CRIT:,} over 24 h · "
        f"throughput crit <{_HEARTBEAT_MIN} msg/5m · "
        f"disk crit >{_DISK_CRIT_PCT:.0f}% (VM-level)_"
    )

    return "\n".join(lines)
