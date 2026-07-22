"""
renderer.py — converts L0Report into a Slack Block Kit payload dict.

render(report) -> {"text": ..., "blocks": [...]}
"""
from __future__ import annotations
from datetime import timezone, timedelta

from .models import Endpoint, FlaggingThresholds, L0Report, Server, Status

IST = timezone(timedelta(hours=5, minutes=30))
_STATUS_ORDER = {Status.HEALTHY: 0, Status.WARNING: 1, Status.CRITICAL: 2, Status.UNKNOWN: -1}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _metric_status(value: float, thresholds: FlaggingThresholds) -> Status:
    if value >= thresholds.metric_crit_pct:
        return Status.CRITICAL
    if value >= thresholds.metric_warn_pct:
        return Status.WARNING
    return Status.HEALTHY


def _endpoint_is_flagged(ep: Endpoint, t: FlaggingThresholds) -> bool:
    # Success rate: flag on a spike down vs baseline, OR whenever the absolute
    # success rate is below the warn floor (an always-low endpoint is still bad).
    if ep.success_baseline_pct is not None:
        if ep.success_baseline_pct - ep.success_pct >= 5.0:
            return True
    if ep.success_pct < t.success_warn_pct:
        return True
    # p99 latency: flag only if it's a spike vs baseline
    if ep.p99_baseline_ms and ep.p99_baseline_ms > 0:
        if ep.p99_ms / ep.p99_baseline_ms >= 1.5:
            return True
    elif ep.p99_ms >= t.p99_warn_ms:
        return True
    # Any hard errors on the endpoint are always worth surfacing.
    if ep.errors is not None and ep.errors > 0:
        return True
    return False


def _status_emoji(status: Status) -> str:
    return {
        Status.HEALTHY:  "🟢",
        Status.WARNING:  "🟡",
        Status.CRITICAL: "🔴",
        Status.UNKNOWN:  "⚪",
    }.get(status, "⚪")


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


def _short_name(name: str) -> str:
    PREFIXES = [
        ("p-uaa-em-celery-",     "cel-"),
        ("p-uaa-entity-manager-", "em-"),
        ("p-uaa-em-",            "em-"),
    ]
    for long_prefix, short_prefix in PREFIXES:
        if name.startswith(long_prefix):
            return short_prefix + name[len(long_prefix):]
    return name[-10:]


def _group_summary(servers: list[Server], group: str) -> dict:
    gs = [s for s in servers if s.group == group]
    if not gs:
        return {"avg_cpu": 0.0, "avg_mem": 0.0, "avg_disk": 0.0, "max_disk": 0.0}
    return {
        "avg_cpu":  sum(s.metrics.cpu_pct  for s in gs) / len(gs),
        "avg_mem":  sum(s.metrics.mem_pct  for s in gs) / len(gs),
        "avg_disk": sum(s.metrics.disk_pct for s in gs) / len(gs),
        "max_disk": max(s.metrics.disk_pct for s in gs),
    }


def _worst_status(*statuses: Status) -> Status:
    return max(statuses, key=lambda s: _STATUS_ORDER.get(s, -1))


def _flag_reasons(ep: Endpoint, t: FlaggingThresholds) -> list[str]:
    reasons = []
    if ep.p99_baseline_ms and ep.p99_baseline_ms > 0:
        ratio = ep.p99_ms / ep.p99_baseline_ms
        if ratio >= 2.0:
            reasons.append(f"p99 spike {ratio:.1f}× baseline")
        elif ratio >= 1.5:
            reasons.append(f"p99 elevated {ratio:.1f}× baseline")
    else:
        if ep.p99_ms >= t.p99_crit_ms:
            reasons.append("critical p99")
        elif ep.p99_ms >= t.p99_warn_ms:
            reasons.append("slow p99")
    if ep.success_baseline_pct is not None:
        drop = ep.success_baseline_pct - ep.success_pct
        if drop >= 10.0:
            reasons.append(f"success dropped {drop:.0f}pp vs baseline")
        elif drop >= 5.0:
            reasons.append(f"success down {drop:.0f}pp vs baseline")
    else:
        if ep.success_pct < 80:
            reasons.append("critical success rate")
        elif ep.success_pct < t.success_warn_pct:
            reasons.append("low success rate")
    if ep.errors is not None and ep.errors > 0:
        reasons.append("errors")
    return reasons


# ── Block builders ─────────────────────────────────────────────────────────────

def _block_header(report: L0Report) -> list[dict]:
    dt = report.reported_at
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    ts_ist   = dt.astimezone(IST)
    date_str = ts_ist.strftime("%a %d %b %Y")
    time_str = ts_ist.strftime("%I:%M %p IST")
    emoji    = _status_emoji(report.status)
    label    = report.status.value.upper()
    return [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "L0 Daily Metrics Report", "emoji": True},
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"{date_str} · {time_str} · {report.service}"}],
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Overall status:* {emoji} *{label}*"},
        },
        {"type": "divider"},
    ]


def _block_system_health(report: L0Report) -> list[dict]:
    t   = report.thresholds
    sys = report.system
    blocks: list[dict] = [{
        "type": "section",
        "text": {"type": "mrkdwn", "text": f"*System Health* · {sys.online} online · {sys.down} down"},
    }]

    if not sys.servers:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": ":white_circle: No server data"}],
        })
        blocks.append({"type": "divider"})
        return blocks

    # Group summary cards (2-column layout via fields)
    groups = sorted(set(s.group for s in sys.servers))
    group_fields = []
    for group in groups:
        summary = _group_summary(sys.servers, group)
        worst   = _worst_status(
            _metric_status(summary["avg_cpu"],  t),
            _metric_status(summary["avg_mem"],  t),
            _metric_status(summary["avg_disk"], t),
        )
        group_fields.append({
            "type": "mrkdwn",
            "text": (
                f"*{group} group* {_status_emoji(worst)}\n"
                f"CPU {summary['avg_cpu']:.0f}% · MEM {summary['avg_mem']:.0f}% · Disk {summary['avg_disk']:.0f}%"
            ),
        })
    for i in range(0, len(group_fields), 10):
        blocks.append({"type": "section", "fields": group_fields[i:i+10]})

    # Individual servers (2-column layout via fields, emoji only if non-healthy)
    server_fields = []
    for server in sys.servers:
        m       = server.metrics
        cpu_st  = _metric_status(m.cpu_pct,  t)
        mem_st  = _metric_status(m.mem_pct,  t)
        disk_st = _metric_status(m.disk_pct, t)

        def _part(val: float, st: Status) -> str:
            prefix = f"{_status_emoji(st)} " if st != Status.HEALTHY else ""
            return f"{prefix}{val:.0f}%"

        server_fields.append({
            "type": "mrkdwn",
            "text": (
                f"`{_short_name(server.name)}` "
                f"{_part(m.cpu_pct, cpu_st)} · "
                f"{_part(m.mem_pct, mem_st)} · "
                f"{_part(m.disk_pct, disk_st)}"
            ),
        })
    for i in range(0, len(server_fields), 10):
        blocks.append({"type": "section", "fields": server_fields[i:i+10]})

    blocks.append({"type": "divider"})
    return blocks


def _block_api_metrics(report: L0Report) -> list[dict]:
    a = report.api
    return [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*API Metrics*"},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Throughput*\n{a.throughput_rps:.1f} rps"},
                {"type": "mrkdwn", "text": f"*Success rate*\n{_fmt_pct(a.success_rate_pct)}"},
                {"type": "mrkdwn", "text": f"*Error rate*\n{_fmt_pct(a.error_rate_pct)}"},
                {"type": "mrkdwn", "text": f"*Avg latency (p50)*\n{a.avg_latency_p50_ms} ms"},
            ],
        },
        {"type": "divider"},
    ]


def _block_endpoints(report: L0Report) -> list[dict]:
    t         = report.thresholds
    endpoints = report.endpoints

    if not endpoints:
        return []

    flagged   = sorted([ep for ep in endpoints if     _endpoint_is_flagged(ep, t)],
                       key=lambda ep: ep.hits, reverse=True)
    unflagged = sorted([ep for ep in endpoints if not _endpoint_is_flagged(ep, t)],
                       key=lambda ep: ep.hits, reverse=True)

    total_count = report.total_endpoint_count or len(endpoints)
    blocks: list[dict] = [{
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"*API Endpoints* · {total_count} with traffic   _hits · success · errors · p99_",
        },
    }]

    # Flagged endpoints
    if flagged:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":warning: *Flagged endpoints ({len(flagged)})* — errors · low success · slow p99",
            },
        })
        for i, ep in enumerate(flagged):
            # Guard: if we're deep into the block budget, collapse remaining into context
            if len(blocks) >= 40:
                remaining = flagged[i:]
                compact = "\n".join(
                    f"`{e.path}` · {_fmt_hits(e.hits)} hits · {', '.join(_flag_reasons(e, t))}"
                    for e in remaining
                )
                blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": compact}]})
                break

            reasons = _flag_reasons(ep, t)
            if ep.success_baseline_pct is not None:
                _drop = ep.success_baseline_pct - ep.success_pct
                suc_emoji = ":red_circle:" if _drop >= 10.0 else (":yellow_circle:" if _drop >= 5.0 else ":green_circle:")
            else:
                suc_emoji = (
                    ":red_circle:"    if ep.success_pct < 80
                    else ":yellow_circle:" if ep.success_pct < t.success_warn_pct
                    else ":green_circle:"
                )
            if ep.p99_baseline_ms and ep.p99_baseline_ms > 0:
                _r = ep.p99_ms / ep.p99_baseline_ms
                p99_emoji = ":red_circle:" if _r >= 2.0 else (":yellow_circle:" if _r >= 1.5 else ":green_circle:")
            else:
                p99_emoji = (
                    ":red_circle:"    if ep.p99_ms >= t.p99_crit_ms
                    else ":yellow_circle:" if ep.p99_ms >= t.p99_warn_ms
                    else ":green_circle:"
                )
            errors_str = "N/A" if ep.errors is None else str(ep.errors)
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"`{ep.path}`\n"
                        f"{_fmt_hits(ep.hits)} hits · "
                        f"{suc_emoji} {_fmt_pct(ep.success_pct)} · "
                        f"{errors_str} errors · "
                        f"{p99_emoji} {_fmt_p99(ep.p99_ms)} p99\n"
                        f"_{', '.join(reasons)}_"
                    ),
                },
            })

    # Top N unflagged
    top_n = unflagged[:t.top_n_unflagged]
    rest  = unflagged[t.top_n_unflagged:]

    if top_n:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Top {len(top_n)} endpoints by hits*"},
        })
        lines = []
        for ep in top_n:
            errors_str = "N/A" if ep.errors is None else str(ep.errors)
            lines.append(
                f"`{ep.path}` · {_fmt_hits(ep.hits)} · "
                f"{_fmt_pct(ep.success_pct)} · "
                f"{errors_str} errors · "
                f"{_fmt_p99(ep.p99_ms)} p99"
            )
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "\n".join(lines)}],
        })

    # Collapsed remainder
    extra        = max(0, report.total_endpoint_count - len(endpoints))
    total_hidden = len(rest) + extra
    if total_hidden > 0:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"+{total_hidden} more endpoints, all healthy"}],
        })

    return blocks


def _footer_context() -> dict:
    return {
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": "🟢 Healthy   🟡 Warning 40-59%   🔴 Critical ≥60%   ·   brightmoney observability",
        }],
    }


# ── Main entry point ───────────────────────────────────────────────────────────

def render(report: L0Report) -> dict:
    blocks = [
        *_block_header(report),
        *_block_system_health(report),
        *_block_api_metrics(report),
        *_block_endpoints(report),
        _footer_context(),
    ]
    if len(blocks) > 50:
        raise ValueError(f"Slack payload exceeds 50-block limit: {len(blocks)} blocks")
    return {
        "text": f"L0 Daily Metrics — {report.service} — {report.status.value.upper()}",
        "blocks": blocks,
    }
