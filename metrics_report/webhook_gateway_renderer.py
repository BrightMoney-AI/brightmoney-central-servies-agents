from __future__ import annotations

"""webhook_gateway_renderer.py — Slack canvas + Block Kit renderer for the Webhook Gateway report.

Canvas structure:
  L0  Pipeline Health   — EB%, MSK%, API-dest%, S3 overflow, invalid count
  L0  Infra Health      — API GW 5XX/4XX%, throughput, E2E p99, APIGW p99
  L1  Per-Slug          — EB% and MSK% per webhook provider slug
  L1  Lambda            — errors, throttles, invocations, duration p99 per function
  L2  DLQ               — SQS depth + age of oldest message
  L2  WAF               — block %, total blocked, per-rule breakdown

Thresholds (hard-coded to match collector._compute_status):
  EB/MSK/API-dest success:  <95% → 🔴   <99% → 🟡   ≥99% → 🟢
  5XX error rate:           ≥5%  → 🔴   ≥1%  → 🟡   <1%  → 🟢
  E2E latency p99:          ≥10s → 🔴   ≥2s  → 🟡   <2s  → 🟢
  DLQ depth:                ≥10  → 🔴   ≥1   → 🟡   0    → 🟢
  WAF block %:              ≥40% → 🔴   ≥10% → 🟡   <10% → 🟢
"""

from datetime import datetime, timedelta, timezone

from .models import Status, WebhookGatewayReport

IST = timezone(timedelta(hours=5, minutes=30))


# ── Status icons ──────────────────────────────────────────────────────────────

def _success_icon(pct: float | None) -> str:
    if pct is None:
        return "⚪"
    if pct < 95:
        return "🔴"
    if pct < 99:
        return "🟡"
    return "🟢"


def _error_rate_icon(pct: float | None) -> str:
    if pct is None:
        return "⚪"
    if pct >= 5:
        return "🔴"
    if pct >= 1:
        return "🟡"
    return "🟢"


def _latency_icon(ms: float | None) -> str:
    if ms is None:
        return "⚪"
    if ms >= 10_000:
        return "🔴"
    if ms >= 2_000:
        return "🟡"
    return "🟢"


def _dlq_icon(depth: int) -> str:
    if depth >= 10:
        return "🔴"
    if depth >= 1:
        return "🟡"
    return "🟢"


def _waf_icon(pct: float | None) -> str:
    if pct is None:
        return "⚪"
    if pct >= 40:
        return "🔴"
    if pct >= 10:
        return "🟡"
    return "🟢"


def _overflow_icon(count: int) -> str:
    return "🟡" if count >= 1 else "🟢"


def _status_emoji(status: Status) -> str:
    return {"healthy": "🟢", "warning": "🟡", "critical": "🔴", "unknown": "⚪"}.get(status.value, "⚪")


def _fmt_pct(v: float | None) -> str:
    return f"{v:.2f}%" if v is not None else "N/A"


def _fmt_ms(v: float | None) -> str:
    return f"{v:,.0f} ms" if v is not None else "N/A"


def _fmt_int(v: int) -> str:
    return f"{v:,}"


# ── Flag builder ──────────────────────────────────────────────────────────────

def _build_flags(report: WebhookGatewayReport) -> list[str]:
    """Return a list of flag strings (one per degraded metric)."""
    flags: list[str] = []
    p = report.pipeline
    i = report.infra
    d = report.dlq
    w = report.waf

    if p.eb_success_pct is not None and p.eb_success_pct < 99:
        icon = "🔴" if p.eb_success_pct < 95 else "🟡"
        flags.append(f"{icon} EB Publish Success: {_fmt_pct(p.eb_success_pct)}")

    if p.msk_success_pct is not None and p.msk_success_pct < 99:
        icon = "🔴" if p.msk_success_pct < 95 else "🟡"
        flags.append(f"{icon} MSK Publish Success: {_fmt_pct(p.msk_success_pct)}")

    if p.api_dest_pct is not None and p.api_dest_pct < 99:
        icon = "🔴" if p.api_dest_pct < 95 else "🟡"
        flags.append(f"{icon} API-Dest Delivery: {_fmt_pct(p.api_dest_pct)}")

    if p.overflow_count >= 1:
        flags.append(f"🟡 S3 Overflow: {_fmt_int(p.overflow_count)} events")

    if i.apigw_5xx_pct is not None and i.apigw_5xx_pct >= 1:
        icon = "🔴" if i.apigw_5xx_pct >= 5 else "🟡"
        flags.append(f"{icon} API GW 5XX: {_fmt_pct(i.apigw_5xx_pct)}")

    if i.e2e_latency_p99_ms is not None and i.e2e_latency_p99_ms >= 2_000:
        icon = "🔴" if i.e2e_latency_p99_ms >= 10_000 else "🟡"
        flags.append(f"{icon} E2E Latency p99: {_fmt_ms(i.e2e_latency_p99_ms)}")

    if d.depth_now >= 1:
        icon = "🔴" if d.depth_now >= 10 else "🟡"
        flags.append(f"{icon} DLQ Depth: {_fmt_int(d.depth_now)} messages")

    if w.block_pct is not None and w.block_pct >= 10:
        icon = "🔴" if w.block_pct >= 40 else "🟡"
        flags.append(f"{icon} WAF Block Rate: {_fmt_pct(w.block_pct)}")

    for slug in report.slugs:
        if slug.eb_success_pct is not None and slug.eb_success_pct < 95:
            flags.append(f"🔴 Slug EB [{slug.slug}]: {_fmt_pct(slug.eb_success_pct)}")
        if slug.msk_success_pct is not None and slug.msk_success_pct < 95:
            flags.append(f"🔴 Slug MSK [{slug.slug}]: {_fmt_pct(slug.msk_success_pct)}")

    if report.failures:
        for f in report.failures:
            flags.append(f"⚠️ Collector error: {f}")

    return flags


# ── Canvas section renderers ──────────────────────────────────────────────────

def _render_attention(flags: list[str]) -> str:
    if not flags:
        return ""
    crit = [f for f in flags if f.startswith("🔴")]
    warn = [f for f in flags if f.startswith("🟡")]
    err  = [f for f in flags if f.startswith("⚠️")]

    lines: list[str] = ["## ⚠️ Attention Required\n"]
    if crit:
        lines.append(f"### 🔴 Critical ({len(crit)} flag{'s' if len(crit) != 1 else ''})")
        for f in crit:
            lines.append(f"- {f}")
        lines.append("")
    if warn:
        lines.append(f"### 🟡 Warning ({len(warn)} flag{'s' if len(warn) != 1 else ''})")
        for f in warn:
            lines.append(f"- {f}")
        lines.append("")
    if err:
        lines.append("### ⚠️ Collector Errors")
        for f in err:
            lines.append(f"- {f}")
        lines.append("")
    return "\n".join(lines)


def _render_l0_pipeline(report: WebhookGatewayReport) -> str:
    p = report.pipeline
    i = report.infra

    rows = [
        ("EB Publish Success",   _success_icon(p.eb_success_pct),    _fmt_pct(p.eb_success_pct)),
        ("MSK Publish Success",  _success_icon(p.msk_success_pct),   _fmt_pct(p.msk_success_pct)),
        ("API-Dest Delivery",    _success_icon(p.api_dest_pct),      _fmt_pct(p.api_dest_pct)),
        ("S3 Overflow Events",   _overflow_icon(p.overflow_count),   _fmt_int(p.overflow_count)),
        ("API GW Throughput",    "📊",                                _fmt_int(i.apigw_throughput) + " req"),
        ("API GW 5XX Rate",      _error_rate_icon(i.apigw_5xx_pct),  _fmt_pct(i.apigw_5xx_pct)),
        ("API GW 4XX Rate",      "ℹ️",                               _fmt_pct(i.apigw_4xx_pct)),
        ("E2E Latency p99",      _latency_icon(i.e2e_latency_p99_ms),_fmt_ms(i.e2e_latency_p99_ms)),
        ("API GW Latency p99",   _latency_icon(i.apigw_latency_p99_ms), _fmt_ms(i.apigw_latency_p99_ms)),
    ]

    lines = [
        "## L0 — Pipeline & Infra Health\n",
        "| Metric | Status | Value (24h) |",
        "| --- | --- | --- |",
    ]
    for name, icon, val in rows:
        lines.append(f"| {name} | {icon} | {val} |")
    lines.append("")
    return "\n".join(lines)


def _render_l1_slugs(report: WebhookGatewayReport) -> str:
    if not report.slugs:
        return "## L1 — Per-Slug Health\n\n_No slug data available._\n\n"

    lines = [
        "## L1 — Per-Slug Health\n",
        "| Slug | EB Success | MSK Success |",
        "| --- | --- | --- |",
    ]
    for s in sorted(report.slugs, key=lambda x: x.slug):
        eb_icon  = _success_icon(s.eb_success_pct)
        msk_icon = _success_icon(s.msk_success_pct)
        eb_val   = _fmt_pct(s.eb_success_pct)
        msk_val  = _fmt_pct(s.msk_success_pct)
        lines.append(f"| `{s.slug}` | {eb_icon} {eb_val} | {msk_icon} {msk_val} |")
    lines.append("")
    return "\n".join(lines)


def _render_l1_lambda(report: WebhookGatewayReport) -> str:
    if not report.lambdas:
        return "## L1 — Lambda Health\n\n_No Lambda data available._\n\n"

    lines = [
        "## L1 — Lambda Health\n",
        "| Function | Invocations | Errors | Throttles | Duration p99 |",
        "| --- | --- | --- | --- | --- |",
    ]
    for lam in report.lambdas:
        err_icon = "🔴" if lam.errors > 0 else "🟢"
        thr_icon = "🟡" if lam.throttles > 0 else "🟢"
        short    = lam.name.replace("webhook-", "").replace("-prod", "")
        dur      = _fmt_ms(lam.duration_p99) if lam.duration_p99 else "N/A"
        lines.append(
            f"| `{short}` | {_fmt_int(lam.invocations)} | {err_icon} {_fmt_int(lam.errors)} "
            f"| {thr_icon} {_fmt_int(lam.throttles)} | {dur} |"
        )
    lines.append("")
    return "\n".join(lines)


def _render_l2_dlq(report: WebhookGatewayReport) -> str:
    d = report.dlq
    icon = _dlq_icon(d.depth_now)

    age_str = "N/A"
    if d.age_oldest_s is not None:
        minutes = d.age_oldest_s // 60
        if minutes >= 60:
            age_str = f"{minutes // 60}h {minutes % 60}m"
        else:
            age_str = f"{minutes}m"

    lines = [
        "## L2 — Dead-Letter Queue\n",
        "| Metric | Status | Value |",
        "| --- | --- | --- |",
        f"| DLQ Depth (visible) | {icon} | {_fmt_int(d.depth_now)} messages |",
        f"| Age of Oldest Message | {'🟡' if d.age_oldest_s and d.age_oldest_s > 300 else '🟢'} | {age_str} |",
        "",
    ]
    return "\n".join(lines)


def _render_l2_waf(report: WebhookGatewayReport) -> str:
    w = report.waf
    icon = _waf_icon(w.block_pct)

    lines = [
        "## L2 — WAF Security\n",
        "| Metric | Status | Value (24h) |",
        "| --- | --- | --- |",
        f"| Block Rate | {icon} | {_fmt_pct(w.block_pct)} |",
        f"| Total Blocked Requests | {'📊'} | {_fmt_int(w.blocked_total)} |",
        "",
    ]

    if w.rules:
        lines.append("**Per-Rule Blocked Counts (24h)**\n")
        lines.append("| WAF Rule | Blocked |")
        lines.append("| --- | --- |")
        for rule, count in sorted(w.rules.items(), key=lambda x: -x[1]):
            lines.append(f"| `{rule}` | {_fmt_int(count)} |")
        lines.append("")

    return "\n".join(lines)


# ── Public API ────────────────────────────────────────────────────────────────

def render_webhook_gateway_canvas(report: WebhookGatewayReport, date_str: str) -> str:
    """Render the full Webhook Gateway markdown canvas."""
    status_emoji = _status_emoji(report.status)
    ts_str = report.reported_at.strftime("%a %d %b %Y · %I:%M %p IST")

    flags = _build_flags(report)

    sections: list[str] = [
        f"# {status_emoji} Webhook Gateway — Health Overview — {date_str}\n",
        f"_Reported: {ts_str}_\n",
        "---\n",
    ]

    if flags:
        sections.append(_render_attention(flags))
        sections.append("---\n")
    else:
        sections.append("✅ _All metrics within healthy thresholds._\n\n---\n")

    sections.append(_render_l0_pipeline(report))
    sections.append("---\n")
    sections.append(_render_l1_slugs(report))
    sections.append("---\n")
    sections.append(_render_l1_lambda(report))
    sections.append("---\n")
    sections.append(_render_l2_dlq(report))
    sections.append("---\n")
    sections.append(_render_l2_waf(report))

    return "\n".join(sections)


def render_webhook_gateway_summary_blocks(report: WebhookGatewayReport, date_str: str) -> list[dict]:
    """Render Slack Block Kit summary blocks for the Webhook Gateway report."""
    status_emoji = _status_emoji(report.status)
    status_label = {
        Status.HEALTHY:  "ALL SYSTEMS HEALTHY",
        Status.WARNING:  "DEGRADED",
        Status.CRITICAL: "CRITICAL",
        Status.UNKNOWN:  "UNKNOWN",
    }.get(report.status, "UNKNOWN")

    ts_str = report.reported_at.strftime("%a %d %b %Y · %I:%M %p IST")

    p = report.pipeline
    i = report.infra
    d = report.dlq

    flags = _build_flags(report)
    n_crit = sum(1 for f in flags if f.startswith("🔴"))
    n_warn = sum(1 for f in flags if f.startswith("🟡"))

    scorecard = (
        f"EB {_success_icon(p.eb_success_pct)} {_fmt_pct(p.eb_success_pct)}   "
        f"MSK {_success_icon(p.msk_success_pct)} {_fmt_pct(p.msk_success_pct)}   "
        f"API-Dest {_success_icon(p.api_dest_pct)} {_fmt_pct(p.api_dest_pct)}   "
        f"5XX {_error_rate_icon(i.apigw_5xx_pct)} {_fmt_pct(i.apigw_5xx_pct)}   "
        f"DLQ {_dlq_icon(d.depth_now)} {_fmt_int(d.depth_now)}"
    )

    flag_line = ""
    if n_crit or n_warn:
        flag_line = f"🔴 {n_crit} critical   🟡 {n_warn} warning"
    else:
        flag_line = "✅ No issues flagged"

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🔗  Webhook Gateway — Health Overview — {date_str}",
                "emoji": True,
            },
        },
        {"type": "context", "elements": [{"type": "mrkdwn", "text": ts_str}]},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Overall:* {status_emoji} *{status_label}*\n{scorecard}",
            },
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": flag_line}],
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "Full L0→L1→L2 detail in canvas below ↓"}],
        },
    ]

    return blocks
