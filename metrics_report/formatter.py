"""
ReportFormatter — builds a Slack Block Kit payload from a MetricsReport.

System health: one row per server (CPU / MEM / Disk).
API metrics:   aggregated across all instances.

Status icons:
  🟢  healthy    🟡  warning    🔴  critical    ⚪  no data
"""
from datetime import datetime, timezone, timedelta
from typing import Optional

from .collector import MetricsReport
from .config import settings

IST = timezone(timedelta(hours=5, minutes=30))


# ── Helpers ────────────────────────────────────────────────────────────────────

def _icon(value: Optional[float], warn: float, crit: float, invert: bool = False) -> str:
    if value is None:
        return "⚪"
    if invert:
        return "🟢" if value >= warn else ("🟡" if value >= crit else "🔴")
    return "🟢" if value < warn else ("🟡" if value < crit else "🔴")


def _pct(value: Optional[float]) -> str:
    return "N/A" if value is None else f"{value:.1f}%"


def _ms(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    return f"{value:,.0f} ms" if value >= 10 else f"{value:.1f} ms"


def _rps(value: Optional[float]) -> str:
    return "N/A" if value is None else f"{value:.1f} rps"


def _overall_status(icons: list[str]) -> tuple[str, str]:
    if "🔴" in icons:
        return "🔴", "CRITICAL"
    if "🟡" in icons:
        return "🟡", "DEGRADED"
    if all(i == "⚪" for i in icons):
        return "⚪", "NO DATA"
    return "🟢", "ALL SYSTEMS HEALTHY"


def _txt(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _fields(*items: str) -> dict:
    return {"type": "section", "fields": [{"type": "mrkdwn", "text": t} for t in items]}


def _divider() -> dict:
    return {"type": "divider"}


def _context(text: str) -> dict:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}


# ── Main builder ───────────────────────────────────────────────────────────────

def build_slack_payload(report: MetricsReport, service_name: str = "All Services") -> dict:
    v = report.values
    sv = report.server_values
    now = datetime.now(IST)
    date_str = now.strftime("%A, %d %b %Y")
    time_str = now.strftime("%I:%M %p IST")

    # ── Per-server lookups ────────────────────────────────────────────────────
    cpu_map  = dict(sv.get("cpu_usage_pct",   []))
    mem_map  = dict(sv.get("memory_usage_pct", []))
    disk_map = dict(sv.get("disk_usage_pct",   []))

    all_servers = sorted(set(cpu_map) | set(mem_map) | set(disk_map))

    # ── Aggregate values ──────────────────────────────────────────────────────
    s_up  = v.get("servers_up")
    s_dn  = v.get("servers_down")
    tput    = v.get("api_throughput_rps")
    success = v.get("api_success_rate_pct")
    error   = v.get("api_error_rate_pct")
    avg_lat = v.get("api_avg_latency_ms")
    p95_lat = v.get("api_p95_latency_ms")
    p99_lat = v.get("api_p99_latency_ms")

    # ── Collect all status icons for overall health ───────────────────────────
    server_icons = [
        _icon(cpu_map.get(s),  settings.cpu_warn_pct,  settings.cpu_crit_pct)
        for s in all_servers
    ] + [
        _icon(mem_map.get(s),  settings.mem_warn_pct,  settings.mem_crit_pct)
        for s in all_servers
    ] + [
        _icon(disk_map.get(s), settings.disk_warn_pct, settings.disk_crit_pct)
        for s in all_servers
    ]
    api_icons = [
        _icon(error,   settings.error_rate_warn_pct,     settings.error_rate_crit_pct),
        _icon(success, warn=95.0, crit=90.0, invert=True),
        _icon(avg_lat, settings.avg_latency_warn_ms,     settings.avg_latency_crit_ms),
        _icon(p95_lat, settings.avg_latency_warn_ms * 2, settings.avg_latency_crit_ms * 2),
        _icon(p99_lat, settings.avg_latency_warn_ms * 3, settings.avg_latency_crit_ms * 3),
    ]
    status_icon, status_label = _overall_status(server_icons + api_icons)

    blocks: list[dict] = []

    # ── Header ────────────────────────────────────────────────────────────────
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": "📊  L0 Daily Metrics Report", "emoji": True},
    })
    blocks.append(_context(f"*{date_str}*   ·   {time_str}   ·   {service_name}"))
    blocks.append(_divider())

    # ── Overall status banner ─────────────────────────────────────────────────
    blocks.append(_txt(f"{status_icon}  *OVERALL STATUS:  {status_label}*"))
    blocks.append(_divider())

    # ── System Health — one row per server ────────────────────────────────────
    blocks.append(_txt("*🖥   SYSTEM HEALTH*"))

    if all_servers:
        server_lines = []
        for server in all_servers:
            cpu  = cpu_map.get(server)
            mem  = mem_map.get(server)
            disk = disk_map.get(server)
            ci = _icon(cpu,  settings.cpu_warn_pct,  settings.cpu_crit_pct)
            mi = _icon(mem,  settings.mem_warn_pct,  settings.mem_crit_pct)
            di = _icon(disk, settings.disk_warn_pct, settings.disk_crit_pct)
            server_lines.append(
                f"*{server}*\n"
                f"{ci} CPU `{_pct(cpu)}`   {mi} MEM `{_pct(mem)}`   {di} Disk `{_pct(disk)}`"
            )
        blocks.append(_txt("\n\n".join(server_lines)))
    else:
        blocks.append(_txt("⚪  No server data returned"))

    srv_ic = "🟢" if (s_dn or 0) == 0 else ("🟡" if (s_dn or 0) <= 1 else "🔴")
    srv_line = (
        f"{srv_ic}  *{int(s_up or 0)} servers online*   ·   *{int(s_dn or 0)} down*"
        if s_up is not None or s_dn is not None
        else "⚪  *Servers:* N/A"
    )
    blocks.append(_txt(srv_line))
    blocks.append(_divider())

    # ── API Metrics ───────────────────────────────────────────────────────────
    err_ic  = _icon(error,   settings.error_rate_warn_pct,     settings.error_rate_crit_pct)
    suc_ic  = _icon(success, warn=95.0, crit=90.0, invert=True)
    tput_ic = "🟢" if tput is not None else "⚪"
    alat_ic = _icon(avg_lat, settings.avg_latency_warn_ms,     settings.avg_latency_crit_ms)
    p95_ic  = _icon(p95_lat, settings.avg_latency_warn_ms * 2, settings.avg_latency_crit_ms * 2)
    p99_ic  = _icon(p99_lat, settings.avg_latency_warn_ms * 3, settings.avg_latency_crit_ms * 3)

    blocks.append(_txt("*🌐   API METRICS*"))
    blocks.append(_fields(
        f"{tput_ic}  *Throughput*\n`{_rps(tput)}`",
        f"{suc_ic}  *Success Rate*\n`{_pct(success)}`",
    ))
    blocks.append(_fields(
        f"{err_ic}  *Error Rate*\n`{_pct(error)}`",
        f"{alat_ic}  *Avg Latency*\n`{_ms(avg_lat)}`",
    ))
    blocks.append(_fields(
        f"{p95_ic}  *p95 Latency*\n`{_ms(p95_lat)}`",
        f"{p99_ic}  *p99 Latency*\n`{_ms(p99_lat)}`",
    ))

    # ── Failed queries ────────────────────────────────────────────────────────
    if report.failures:
        blocks.append(_divider())
        blocks.append(_txt("*⚠️   QUERIES FAILED — data shown as N/A*"))
        lines = "\n".join(f"›  `{f.name}`   {f.reason}" for f in report.failures)
        blocks.append(_context(lines))

    # ── Footer ────────────────────────────────────────────────────────────────
    blocks.append(_divider())
    blocks.append(_context("🟢 Healthy   🟡 Warning   🔴 Critical   ⚪ No data   ·   brightmoney observability"))

    return {"blocks": blocks}
