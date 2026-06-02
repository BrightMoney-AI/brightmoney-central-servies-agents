"""
renderer.py — converts an L0Report into a Slack Block Kit payload dict.

Usage:
    from metrics_report.renderer import render
    payload = render(report)   # pass to slack_sdk / requests / webhook
"""
from __future__ import annotations

import re
from datetime import timezone, timedelta
from typing import Optional

from .models import Endpoint, FlaggingThresholds, L0Report, Server, Status

IST = timezone(timedelta(hours=5, minutes=30))

# ── Primitive block helpers ────────────────────────────────────────────────────

def _txt(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _fields(*items: str) -> dict:
    return {"type": "section", "fields": [{"type": "mrkdwn", "text": t} for t in items]}


def _divider() -> dict:
    return {"type": "divider"}


def _context(text: str) -> dict:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}


# ── Status helpers ─────────────────────────────────────────────────────────────

def _metric_status(value: float, t: FlaggingThresholds) -> Status:
    if value >= t.metric_crit_pct:
        return Status.CRITICAL
    if value >= t.metric_warn_pct:
        return Status.WARNING
    return Status.HEALTHY


def _worst_status(*statuses: Status) -> Status:
    for s in (Status.CRITICAL, Status.WARNING, Status.HEALTHY):
        if s in statuses:
            return s
    return Status.UNKNOWN


def _status_emoji(status: Status) -> str:
    return {
        Status.HEALTHY:  ":green_circle:",
        Status.WARNING:  ":yellow_circle:",
        Status.CRITICAL: ":red_circle:",
        Status.UNKNOWN:  ":white_circle:",
    }.get(status, ":white_circle:")


# ── Endpoint helpers ───────────────────────────────────────────────────────────

def _endpoint_is_flagged(ep: Endpoint, t: FlaggingThresholds) -> bool:
    if ep.errors is not None and ep.errors > 0:
        return True
    if ep.success_pct < t.success_warn_pct:
        return True
    if ep.p99_ms >= t.p99_warn_ms:
        return True
    return False


def _flag_reasons(ep: Endpoint, t: FlaggingThresholds) -> list[str]:
    reasons: list[str] = []
    if ep.p99_ms >= t.p99_crit_ms:
        reasons.append("critical p99")
    elif ep.p99_ms >= t.p99_warn_ms:
        reasons.append("slow p99")
    if ep.success_pct < 80.0:
        reasons.append("critical success rate")
    elif ep.success_pct < t.success_warn_pct:
        reasons.append("low success rate")
    if ep.errors is not None and ep.errors > 0:
        reasons.append("errors")
    return reasons


def _ep_p99_status(ms: float, t: FlaggingThresholds) -> Status:
    if ms >= t.p99_crit_ms:
        return Status.CRITICAL
    if ms >= t.p99_warn_ms:
        return Status.WARNING
    return Status.HEALTHY


def _ep_success_status(pct: float, t: FlaggingThresholds) -> Status:
    if pct < 80.0:
        return Status.CRITICAL
    if pct < t.success_warn_pct:
        return Status.WARNING
    return Status.HEALTHY


# ── Number formatters ──────────────────────────────────────────────────────────

def _fmt_hits(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1e6:.1f}M"
    if n >= 1_000:
        return f"{round(n / 1000)}K"
    return str(n)


def _fmt_p99(ms: float) -> str:
    if ms >= 1000:
        return f"{ms / 1000:.1f}s"
    if ms < 1:
        return f"{ms:.1f}ms"
    return f"{round(ms)}ms"


def _fmt_pct(v: float) -> str:
    return f"{v:.1f}%"


# ── Server name helpers ────────────────────────────────────────────────────────

_SHORT_AFFIXES = {"em", "uaa", "bwg", "wms", "p"}


def _short_name(name: str) -> str:
    """
    Strip known prefixes for compact display.
    'p-uaa-em-celery-05'       -> 'cel-05'
    'p-uaa-entity-manager-06'  -> 'em-06'
    Falls back to last 10 chars if no prefix matches.
    """
    m = re.match(r"^(.*)-(\d+)$", name)
    if not m:
        return name[-10:]
    prefix, num = m.group(1), m.group(2)
    parts = [p for p in prefix.split("-") if p and p != "p"]
    if not parts:
        return f"{prefix[-10:]}-{num}"
    last = parts[-1]
    second_last = parts[-2] if len(parts) >= 2 else ""
    if second_last and second_last not in _SHORT_AFFIXES and len(second_last) >= 3:
        abbrev = second_last[0] + last[0]
    else:
        abbrev = last if len(last) <= 4 else last[:3]
    return f"{abbrev}-{num}"


def _detect_group(name: str) -> str:
    """Derive a group key from a server name by stripping the trailing -NN index."""
    m = re.match(r"^(.*)-\d+$", name)
    if not m:
        return "default"
    prefix = m.group(1)
    parts = [p for p in prefix.split("-") if p and p != "p"]
    if not parts:
        return "default"
    last = parts[-1]
    second_last = parts[-2] if len(parts) >= 2 else ""
    if second_last and second_last not in _SHORT_AFFIXES and len(second_last) >= 3:
        return f"{second_last}-{last}"
    return last


def _group_summary(servers: list[Server], group: str) -> dict:
    members = [s for s in servers if s.group == group]
    if not members:
        return {"avg_cpu": 0.0, "avg_mem": 0.0, "avg_disk": 0.0, "max_disk": 0.0}
    avg_cpu  = sum(s.metrics.cpu_pct  for s in members) / len(members)
    avg_mem  = sum(s.metrics.mem_pct  for s in members) / len(members)
    avg_disk = sum(s.metrics.disk_pct for s in members) / len(members)
    max_disk = max(s.metrics.disk_pct for s in members)
    return {"avg_cpu": avg_cpu, "avg_mem": avg_mem, "avg_disk": avg_disk, "max_disk": max_disk}


# ── Block builders ─────────────────────────────────────────────────────────────

def _block_header(report: L0Report) -> list[dict]:
    ts = report.reported_at.astimezone(IST)
    date_str = ts.strftime("%a %d %b %Y")
    time_str = ts.strftime("%I:%M %p IST")
    emoji = _status_emoji(report.status)
    label = report.status.value.upper()

    return [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "L0 Daily Metrics Report", "emoji": True},
        },
        _context(f"{date_str}  ·  {time_str}  ·  {report.service}"),
        _txt(f"*Overall status:*  {emoji}  {label}"),
        _divider(),
    ]


def _block_system_health(report: L0Report) -> list[dict]:
    t = report.thresholds
    sys = report.system
    live = [s for s in sys.servers if s.status != Status.UNKNOWN]

    blocks: list[dict] = []
    blocks.append(_txt(
        f"🖥  *System Health*  ·  {sys.online} online  ·  {sys.down} down"
    ))

    # Group summary cards (2-column fields)
    groups = sorted(set(s.group for s in live))
    if groups:
        group_fields: list[str] = []
        for grp in groups:
            sm = _group_summary(live, grp)
            ci = _metric_status(sm["avg_cpu"],  t)
            mi = _metric_status(sm["avg_mem"],  t)
            di = _metric_status(sm["avg_disk"], t)
            worst = _worst_status(ci, mi, di)
            group_fields.append(
                f"*{grp} group*  {_status_emoji(worst)}\n"
                f"CPU {sm['avg_cpu']:.1f}%  ·  MEM {sm['avg_mem']:.1f}%  ·  Disk {sm['avg_disk']:.1f}%"
            )
        for i in range(0, len(group_fields), 2):
            blocks.append(_fields(*group_fields[i:i + 2]))

    # Individual server rows (2-column fields, emoji only when non-healthy)
    if live:
        server_fields: list[str] = []
        for s in live:
            ci = _metric_status(s.metrics.cpu_pct, t)
            mi = _metric_status(s.metrics.mem_pct, t)
            di = _metric_status(s.metrics.disk_pct, t)

            cpu_part  = (f"{_status_emoji(ci)} " if ci  != Status.HEALTHY else "") + f"{s.metrics.cpu_pct:.1f}%"
            mem_part  = (f"{_status_emoji(mi)} " if mi  != Status.HEALTHY else "") + f"{s.metrics.mem_pct:.1f}%"
            # Disk always shows status emoji
            disk_part = f"{_status_emoji(di)} {s.metrics.disk_pct:.1f}%"

            server_fields.append(
                f"`{_short_name(s.name)}`  CPU {cpu_part}  ·  MEM {mem_part}  ·  Disk {disk_part}"
            )

        # Fields blocks (max 10 items each = 5 rows of 2)
        for i in range(0, len(server_fields), 10):
            blocks.append(_fields(*server_fields[i:i + 10]))

    blocks.append(_divider())
    return blocks


def _block_api_metrics(report: L0Report) -> list[dict]:
    t = report.thresholds
    api = report.api

    tput_ic = ":green_circle:" if api.throughput_rps > 0 else ":white_circle:"

    suc_s = _ep_success_status(api.success_rate_pct, t)
    err_s = (
        Status.CRITICAL if api.error_rate_pct >= 5.0
        else Status.WARNING if api.error_rate_pct >= 1.0
        else Status.HEALTHY
    )
    lat_s = (
        Status.CRITICAL if api.avg_latency_p50_ms >= t.p99_crit_ms / 3
        else Status.WARNING if api.avg_latency_p50_ms >= t.p99_warn_ms / 3
        else Status.HEALTHY
    )

    return [
        _txt("📈  *API Metrics*"),
        _fields(
            f"*Throughput*\n{tput_ic}  *{api.throughput_rps:.1f} rps*",
            f"*Success rate*\n{_status_emoji(suc_s)}  *{_fmt_pct(api.success_rate_pct)}*",
            f"*Error rate*\n{_status_emoji(err_s)}  *{_fmt_pct(api.error_rate_pct)}*",
            f"*Avg latency (p50)*\n{_status_emoji(lat_s)}  *{round(api.avg_latency_p50_ms)} ms*",
        ),
        _divider(),
    ]


def _block_endpoints(report: L0Report, block_budget: int = 47) -> list[dict]:
    t = report.thresholds
    eps = report.endpoints

    flagged   = sorted([ep for ep in eps if     _endpoint_is_flagged(ep, t)], key=lambda e: e.hits, reverse=True)
    unflagged = sorted([ep for ep in eps if not _endpoint_is_flagged(ep, t)], key=lambda e: e.hits, reverse=True)

    blocks: list[dict] = []

    # ── Flagged section ───────────────────────────────────────────────────────
    if flagged:
        all_reasons: list[str] = []
        seen: set[str] = set()
        for ep in flagged:
            for r in _flag_reasons(ep, t):
                if r not in seen:
                    all_reasons.append(r)
                    seen.add(r)
        reason_str = "  ·  ".join(all_reasons)
        header = f":warning:  *Flagged endpoints ({len(flagged)})*"
        if reason_str:
            header += f"  —  {reason_str}"
        blocks.append(_txt(header))

        for ep in flagged:
            # Guard block count
            if len(blocks) >= block_budget - 4:
                remaining = len(flagged) - flagged.index(ep)
                blocks.append(_context(f"+{remaining} more flagged endpoints"))
                break

            p99_s  = _ep_p99_status(ep.p99_ms, t)
            suc_s  = _ep_success_status(ep.success_pct, t)
            err_ic = ":red_circle:" if (ep.errors or 0) > 0 else (":green_circle:" if ep.errors is not None else ":white_circle:")
            err_str = "N/A" if ep.errors is None else str(ep.errors)
            reasons = _flag_reasons(ep, t)

            path = ep.path if len(ep.path) <= 60 else ep.path[:57] + "…"
            blocks.append(_txt(
                f"`{path}`\n"
                f"{_fmt_hits(ep.hits)} hits  ·  "
                f"{_status_emoji(suc_s)} {_fmt_pct(ep.success_pct)}  ·  "
                f"{err_ic} {err_str} errors  ·  "
                f"{_status_emoji(p99_s)} {_fmt_p99(ep.p99_ms)} p99\n"
                f"_{', '.join(reasons)}_"
            ))

    # ── Top N unflagged ────────────────────────────────────────────────────────
    top_n = t.top_n_unflagged
    shown  = unflagged[:top_n]
    hidden = unflagged[top_n:]

    if shown:
        blocks.append(_txt(f"*Top {len(shown)} endpoints by hits*"))
        lines: list[str] = []
        for ep in shown:
            p99_s = _ep_p99_status(ep.p99_ms, t)
            err_str = "N/A" if ep.errors is None else str(ep.errors)
            path = ep.path if len(ep.path) <= 50 else ep.path[:47] + "…"
            lines.append(
                f"`{path}`  ·  {_fmt_hits(ep.hits)} hits  ·  "
                f"{_fmt_pct(ep.success_pct)}  ·  {err_str} errors  ·  "
                f"{_status_emoji(p99_s)} {_fmt_p99(ep.p99_ms)} p99"
            )
        # All in one context block (compact, under 3000 chars)
        text = "\n".join(lines)
        if len(text) > 2900:
            text = text[:2897] + "…"
        blocks.append(_context(text))

    # ── Collapsed remainder ────────────────────────────────────────────────────
    hidden_in_list = len(hidden)
    hidden_not_fetched = max(0, report.total_endpoint_count - len(eps))
    total_hidden = hidden_in_list + hidden_not_fetched
    if total_hidden > 0:
        blocks.append(_context(f"+{total_hidden} more endpoints, all healthy"))

    return blocks


def _footer_context() -> dict:
    return _context(
        "🟢 Healthy   🟡 Warning   🔴 Critical   ⚪ No data   ·   brightmoney observability"
    )


# ── Main entry point ───────────────────────────────────────────────────────────

def render(report: L0Report) -> dict:
    """
    Assemble all block sections into a Slack Block Kit payload dict.
    Raises ValueError if total blocks exceed Slack's hard limit of 50.
    """
    blocks = [
        *_block_header(report),
        *_block_system_health(report),
        *_block_api_metrics(report),
        *_block_endpoints(report),
        _footer_context(),
    ]

    if len(blocks) > 50:
        raise ValueError(
            f"Slack block limit exceeded: {len(blocks)} blocks (max 50). "
            "Reduce endpoint count or service count."
        )

    return {
        "text": f"L0 Daily Metrics  —  {report.service}  —  {report.status.value.upper()}",
        "blocks": blocks,
    }
