"""
Scheduler — fires the report job daily at 10:00 IST (04:30 UTC).
Collects all service metrics, then posts ONE Slack Canvas per report_group
(e.g. "UAA Services", "Central Services", "Data Platform").
"""
from __future__ import annotations
import logging
from collections import defaultdict

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from datetime import datetime, timezone, timedelta

from .canvas_renderer import render_canvas
from .config import settings
from .collector import collect
from .formatter import to_l0_report
from .gateway import MetricsGateway
from .models import Status
from .services import load_services
from .slack_publisher import publish_canvas
from .vm_client import VMClient

IST = timezone(timedelta(hours=5, minutes=30))

log = logging.getLogger(__name__)

# Canonical display order for canvas groups
_GROUP_ORDER = ["UAA Services", "Central Services", "Data Platform"]


async def run_report() -> None:
    services = load_services()
    log.info("Starting L0 metrics report for %d service(s)...", len(services))

    gateway = MetricsGateway(timeout_secs=settings.gateway_timeout_secs)
    groups: dict[str, list[tuple[str, object]]] = defaultdict(list)

    async with VMClient(settings.vm_base_url) as vm:
        for service in services:
            log.info("Collecting: %s [group=%s]", service.display_name, service.report_group)
            raw = await collect(vm, gateway, service)
            l0  = to_l0_report(raw, service_name=service.display_name)
            groups[service.report_group].append((service.display_name, l0))

            if raw.failures:
                log.warning(
                    "[%s] %d failed queries: %s",
                    service.display_name,
                    len(raw.failures),
                    [f.name for f in raw.failures],
                )
            else:
                log.info("[%s] All queries succeeded.", service.display_name)

    if not groups:
        log.warning("No services collected — skipping Canvas post.")
        return

    ts_ist   = datetime.now(IST)
    date_str = ts_ist.strftime("%d %b %Y")

    # Post canvases in canonical order, then any unrecognised groups last
    ordered_keys = [g for g in _GROUP_ORDER if g in groups]
    ordered_keys += [g for g in groups if g not in _GROUP_ORDER]

    for group_name in ordered_keys:
        collected      = groups[group_name]
        canvas_title   = f"{group_name} — L0 Daily Metrics — {date_str}"
        markdown       = render_canvas(collected, title=canvas_title)
        summary_blocks = _summary_blocks(collected, group_name)
        await publish_canvas(markdown, summary_blocks, title=canvas_title)
        log.info("Canvas posted: %r (%d service(s)).", canvas_title, len(collected))


def _summary_blocks(collected: list[tuple[str, object]], group_name: str = "L0 Daily Metrics") -> list[dict]:
    """Build compact Block Kit blocks for the chat notification message."""
    from .models import L0Report

    reports: list[L0Report] = [r for _, r in collected]
    n_crit  = sum(1 for r in reports if r.status == Status.CRITICAL)
    n_warn  = sum(1 for r in reports if r.status == Status.WARNING)
    n_ok    = sum(1 for r in reports if r.status == Status.HEALTHY)

    ts_ist   = datetime.now(IST)
    date_str = ts_ist.strftime("%a %d %b %Y · %I:%M %p IST")

    # Status line: pick the worst across all services
    if n_crit:
        overall_emoji, overall_label = "🔴", "CRITICAL"
    elif n_warn:
        overall_emoji, overall_label = "🟡", "DEGRADED"
    else:
        overall_emoji, overall_label = "🟢", "ALL SYSTEMS HEALTHY"

    # One line per critical/warning service (up to 10 to stay compact)
    flagged = [
        (name, r) for name, r in collected
        if r.status in (Status.CRITICAL, Status.WARNING)
    ]
    service_lines = []
    for name, r in flagged[:10]:
        icon = "🔴" if r.status == Status.CRITICAL else "🟡"
        service_lines.append(f"{icon} {name}")
    if len(flagged) > 10:
        service_lines.append(f"_+{len(flagged) - 10} more_")

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"📊  {group_name} — L0 Daily Metrics", "emoji": True},
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": date_str}],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Overall:* {overall_emoji} *{overall_label}*   ·   "
                    f"*{len(reports)}* services   ·   "
                    f"🔴 {n_crit} critical   🟡 {n_warn} warning   🟢 {n_ok} healthy"
                ),
            },
        },
    ]

    if service_lines:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(service_lines)},
        })

    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": "Full per-endpoint breakdown → canvas below ↓"}],
    })

    return blocks


def create_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler()
    # 10:00 IST = 04:30 UTC
    scheduler.add_job(
        run_report,
        trigger=CronTrigger(hour=4, minute=30, timezone="UTC"),
        id="l0_daily_report",
        name="L0 Daily Metrics Report",
        misfire_grace_time=300,
    )
    return scheduler
