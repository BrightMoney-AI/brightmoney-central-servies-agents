"""
Scheduler — fires the report job daily at 10:00 IST (04:30 UTC).
Collects all service metrics, then posts ONE Slack Canvas per report_group
(e.g. "UAA Services", "Central Services", "Data Platform").
"""
from __future__ import annotations
import asyncio
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
from .airflow_client import fetch_airflow_health, fetch_view_flow_health
from .models import AirflowHealth
from .kafka_connect import fetch_all_connector_health
from .models import Status
from .services import load_services
from .slack_publisher import publish_canvas
from .vm_client import VMClient

IST = timezone(timedelta(hours=5, minutes=30))

log = logging.getLogger(__name__)

# Canonical display order for canvas groups
_GROUP_ORDER = ["UAA Services", "Central Services", "Data Platform"]


async def run_report(group: str | None = None) -> None:
    services = load_services()
    if group:
        services = [s for s in services if s.report_group == group]
        log.info("Filtered to group=%r: %d service(s).", group, len(services))
    log.info("Starting L0 metrics report for %d service(s)...", len(services))

    gateway = MetricsGateway(timeout_secs=settings.gateway_timeout_secs)
    groups: dict[str, list[tuple[str, object]]] = defaultdict(list)

    collect_central_biz = group is None or group == "Central Services"
    collect_uaa_biz     = group is None or group == "UAA Services"
    collect_dp_biz      = group is None or group == "Data Platform"

    biz_metrics:     list = []
    uaa_biz_metrics: list = []
    dp_biz_metrics:  list = []
    emr_report             = None
    dp_l0_report           = None

    async with VMClient(settings.vm_base_url) as vm:
        for service in services:
            # Skip reference-only entries (no VM/API selectors) — they have no metrics to collect
            if not service.system_selector and not service.api_selector:
                log.info("Skipping %s — reference-only entry (no VM/API selector).", service.display_name)
                continue
            log.info("Collecting: %s [group=%s]", service.display_name, service.report_group)
            raw = await collect(vm, gateway, service)
            l0  = to_l0_report(raw, service_name=service.display_name, show_api_metrics=bool(service.api_job))
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

        if collect_dp_biz:
            from .dp_l0_collector import collect_dp_l0
            dp_l0_svc = next((s for s in services if s.display_name == "Data Platform L0"), None)
            if dp_l0_svc and (dp_l0_svc.kafka_cdc_sinks or dp_l0_svc.kafka_sinks):
                dp_l0_report = await collect_dp_l0(
                    vm,
                    dp_l0_svc.kafka_cdc_sinks,
                    dp_l0_svc.kafka_sinks or None,
                )

        if collect_central_biz:
            from .central_business_collector import collect_business_metrics
            biz_metrics = await collect_business_metrics(vm)

    # Trino-based business metrics (run outside VM context — separate connection)
    if collect_uaa_biz:
        from .uaa_business_collector import collect_uaa_business_metrics
        uaa_biz_metrics = await collect_uaa_business_metrics()

    if collect_dp_biz:
        from .dp_business_collector import collect_dp_business_metrics
        dp_biz_metrics = await collect_dp_business_metrics()

    if collect_dp_biz:
        from .emr_collector import collect_emr_metrics
        emr_report = await collect_emr_metrics()

    if not groups and not biz_metrics and not uaa_biz_metrics and not dp_biz_metrics:
        log.warning("No services collected — skipping Canvas post.")
        return

    ts_ist   = datetime.now(IST)
    date_str = ts_ist.strftime("%d %b %Y")

    # Fetch Data Platform extras concurrently
    connector_result, db_result, view_flow_result = await asyncio.gather(
        fetch_all_connector_health(settings.kafka_connect_instances) if settings.kafka_connect_instances else asyncio.sleep(0),
        fetch_airflow_health(settings.airflow_db_url),
        fetch_view_flow_health(settings.airflow_api_url, settings.airflow_api_username, settings.airflow_api_password),
    )
    connector_health = connector_result if connector_result else None
    airflow_health = AirflowHealth(
        dag_runs=db_result.dag_runs if db_result else [],
        view_flow=view_flow_result,
    )
    if connector_health:
        log.info("Connector health fetched: %d instance(s).", len(connector_health.instances))
    log.info("Airflow health fetched: %d DAG run(s), view_flow=%s.",
             len(airflow_health.dag_runs), "yes" if view_flow_result else "no")

    # Post canvases in canonical order, then any unrecognised groups last
    ordered_keys = [g for g in _GROUP_ORDER if g in groups]
    ordered_keys += [g for g in groups if g not in _GROUP_ORDER]

    for group_name in ordered_keys:
        collected      = groups[group_name]
        canvas_title   = f"{group_name} — L0 Daily Metrics — {date_str}"
        ch             = connector_health if group_name == "Data Platform" else None
        ah             = airflow_health   if group_name == "Data Platform" else None
        markdown       = render_canvas(collected, title=canvas_title, connector_health=ch, airflow_health=ah)
        summary_blocks = _summary_blocks(collected, group_name)
        await publish_canvas(markdown, summary_blocks, title=canvas_title)
        log.info("Canvas posted: %r (%d service(s)).", canvas_title, len(collected))

    if biz_metrics:
        from .central_business_renderer import render_business_canvas
        biz_title  = f"Central Services — Business Metrics — {date_str}"
        biz_md     = render_business_canvas(biz_metrics, title=biz_title)
        biz_blocks = _business_summary_blocks(biz_metrics, date_str)
        await publish_canvas(biz_md, biz_blocks, title=biz_title)
        log.info("Central business metrics canvas posted (%d metrics).", len(biz_metrics))

    if uaa_biz_metrics:
        from .uaa_business_renderer import render_uaa_business_canvas
        uaa_biz_title  = f"UAA Services — Business Metrics — {date_str}"
        uaa_biz_md     = render_uaa_business_canvas(uaa_biz_metrics, title=uaa_biz_title)
        uaa_biz_blocks = _uaa_biz_summary_blocks(uaa_biz_metrics, date_str)
        await publish_canvas(uaa_biz_md, uaa_biz_blocks, title=uaa_biz_title)
        log.info("UAA business metrics canvas posted (%d metrics).", len(uaa_biz_metrics))

    if dp_biz_metrics:
        from .dp_business_renderer import render_dp_business_canvas
        dp_biz_title  = f"Data Platform — Business Metrics — {date_str}"
        dp_biz_md     = render_dp_business_canvas(dp_biz_metrics, title=dp_biz_title)
        dp_biz_blocks = _dp_biz_summary_blocks(dp_biz_metrics, date_str)
        await publish_canvas(dp_biz_md, dp_biz_blocks, title=dp_biz_title)
        log.info("Data Platform business metrics canvas posted (%d metrics).", len(dp_biz_metrics))

    if emr_report is not None:
        from .emr_renderer import render_emr_canvas
        emr_title  = f"Data Platform — EMR Metrics — {date_str}"
        emr_md     = render_emr_canvas(emr_report, title=emr_title)
        emr_blocks = _emr_summary_blocks(emr_report, date_str)
        await publish_canvas(emr_md, emr_blocks, title=emr_title)
        log.info("EMR metrics canvas posted (%d flags).", emr_report.total_flags)

    if dp_l0_report is not None:
        from .dp_l0_renderer import render_dp_l0_canvas
        dp_l0_title  = f"Data Platform — connector health — {date_str}"
        dp_l0_md     = render_dp_l0_canvas(dp_l0_report, title=dp_l0_title)
        dp_l0_blocks = _dp_l0_summary_blocks(dp_l0_report, date_str)
        await publish_canvas(dp_l0_md, dp_l0_blocks, title=dp_l0_title)
        log.info(
            "Data Platform connector health canvas posted (%d flagged sinks, %d flagged VMs).",
            len(dp_l0_report.flagged_sinks), len(dp_l0_report.flagged_vms),
        )


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


def _business_summary_blocks(metrics: list, date_str: str) -> list[dict]:
    """Slack Block Kit notification for the business metrics canvas."""
    from .central_business_renderer import _is_critical, _is_flagged

    flagged = [m for m in metrics if _is_flagged(m)]
    n_sections = len({m.section for m in metrics})

    if not flagged:
        overall_emoji, overall_label = "🟢", "ALL HEALTHY"
    elif any(_is_critical(m) for m in flagged):
        overall_emoji, overall_label = "🔴", "CRITICAL"
    else:
        overall_emoji, overall_label = "🟡", "DEGRADED"

    ts_ist   = datetime.now(IST)
    date_str = ts_ist.strftime("%a %d %b %Y · %I:%M %p IST")

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "📊  Central Services — Business Metrics", "emoji": True},
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
                    f"*{n_sections}* services   ·   "
                    f"*{len(metrics)}* checks   ·   "
                    f"🚨 {len(flagged)} flagged"
                ),
            },
        },
    ]

    if flagged:
        lines = []
        for m in flagged[:10]:
            e = "🔴" if _is_critical(m) else "🟡"
            if m.metric_type == "success_rate":
                lines.append(f"{e} *{m.section}* · {m.display_name}: {m.value:.1f}%")
            else:
                lines.append(f"{e} *{m.section}* · {m.display_name}: {m.value:.0f}")
        if len(flagged) > 10:
            lines.append(f"_+{len(flagged) - 10} more_")
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(lines)},
        })

    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": "Full breakdown → canvas below ↓"}],
    })
    return blocks


def _uaa_biz_summary_blocks(metrics: list, date_str: str) -> list[dict]:
    from .uaa_business_renderer import _is_flagged, _RATE_CRIT

    flagged    = [m for m in metrics if _is_flagged(m)]
    n_sections = len({m.section for m in metrics})

    if not flagged:
        overall_emoji, overall_label = "🟢", "ALL HEALTHY"
    elif any(m.metric_type == "success_rate" and m.value < _RATE_CRIT for m in flagged):
        overall_emoji, overall_label = "🔴", "CRITICAL"
    else:
        overall_emoji, overall_label = "🟡", "DEGRADED"

    ts_ist   = datetime.now(IST)
    date_str = ts_ist.strftime("%a %d %b %Y · %I:%M %p IST")

    blocks: list[dict] = [
        {"type": "header", "text": {"type": "plain_text", "text": "📊  UAA Services — Business Metrics", "emoji": True}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": date_str}]},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Overall:* {overall_emoji} *{overall_label}*   ·   "
                    f"*{n_sections}* sections   ·   "
                    f"*{len(metrics)}* checks   ·   "
                    f"🚨 {len(flagged)} flagged"
                ),
            },
        },
    ]

    if flagged:
        lines = []
        for m in flagged[:10]:
            e = "🔴" if (m.metric_type == "success_rate" and m.value < _RATE_CRIT) else "🟡"
            val = f"{m.value:.1f}%" if m.metric_type == "success_rate" else f"{m.value:.0f}"
            lines.append(f"{e} *{m.section}* · {m.display_name}: {val}")
        if len(flagged) > 10:
            lines.append(f"_+{len(flagged) - 10} more_")
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}})

    blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": "Full breakdown → canvas below ↓"}]})
    return blocks


def _dp_biz_summary_blocks(metrics: list, date_str: str) -> list[dict]:
    from .dp_business_renderer import _is_flagged, _RATE_CRIT

    flagged    = [m for m in metrics if _is_flagged(m)]
    n_sections = len({m.section for m in metrics})

    if not flagged:
        overall_emoji, overall_label = "🟢", "ALL HEALTHY"
    elif any(m.metric_type == "success_rate" and m.value < _RATE_CRIT for m in flagged):
        overall_emoji, overall_label = "🔴", "CRITICAL"
    else:
        overall_emoji, overall_label = "🟡", "DEGRADED"

    ts_ist   = datetime.now(IST)
    date_str = ts_ist.strftime("%a %d %b %Y · %I:%M %p IST")

    blocks: list[dict] = [
        {"type": "header", "text": {"type": "plain_text", "text": "📊  Data Platform — Business Metrics", "emoji": True}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": date_str}]},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Overall:* {overall_emoji} *{overall_label}*   ·   "
                    f"*{n_sections}* sections   ·   "
                    f"*{len(metrics)}* checks   ·   "
                    f"🚨 {len(flagged)} flagged"
                ),
            },
        },
    ]

    if flagged:
        lines = []
        for m in flagged[:10]:
            e = "🔴" if (m.metric_type == "success_rate" and m.value < _RATE_CRIT) else "🟡"
            val = f"{m.value:.1f}%" if m.metric_type == "success_rate" else f"{m.value:.0f}"
            lines.append(f"{e} *{m.section}* · {m.display_name}: {val}")
        if len(flagged) > 10:
            lines.append(f"_+{len(flagged) - 10} more_")
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}})

    blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": "Full breakdown → canvas below ↓"}]})
    return blocks


def _emr_summary_blocks(report: object, date_str: str) -> list[dict]:
    from .emr_collector import EmrReport
    r: EmrReport = report  # type: ignore[assignment]

    total_flags = r.total_flags
    failed      = sum(1 for s in r.sections if s.failed)

    if total_flags == 0 and failed == 0:
        overall_emoji, overall_label = "🟢", "ALL HEALTHY"
    elif total_flags > 0:
        overall_emoji, overall_label = "🔴", "ISSUES FOUND"
    else:
        overall_emoji, overall_label = "🟡", "QUERY FAILURES"

    ts_ist   = datetime.now(IST)
    date_str = ts_ist.strftime("%a %d %b %Y · %I:%M %p IST")

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "📊  Data Platform — EMR Metrics", "emoji": True},
        },
        {"type": "context", "elements": [{"type": "mrkdwn", "text": date_str}]},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Overall:* {overall_emoji} *{overall_label}*   ·   "
                    f"*{len(r.sections)}* sections   ·   "
                    f"🔴 {total_flags} flag(s)"
                    + (f"   ·   ⚪ {failed} query failure(s)" if failed else "")
                ),
            },
        },
    ]

    flagged_sections = [s for s in r.sections if s.flag_count > 0]
    if flagged_sections:
        lines = [f"🔴 *{s.title}* — {s.flag_count} flagged" for s in flagged_sections[:8]]
        if len(flagged_sections) > 8:
            lines.append(f"_+{len(flagged_sections) - 8} more_")
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}})

    blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": "Full breakdown → canvas below ↓"}]})
    return blocks


def _dp_l0_summary_blocks(report: object, date_str: str) -> list[dict]:
    from .dp_l0_collector import DPL0Report
    r: DPL0Report = report  # type: ignore[assignment]

    n_sinks    = len(r.sinks)
    n_flag_s   = len(r.flagged_sinks)
    n_flag_k   = len(r.flagged_kafka_sinks)
    n_flag_v   = len(r.flagged_vms)
    n_ok_s     = n_sinks - n_flag_s

    if n_flag_s == 0 and n_flag_v == 0:
        overall_emoji, overall_label = "🟢", "ALL HEALTHY"
    elif any(
        s.coord_status == "critical" or s.lag_delta_status == "critical" or s.heartbeat_status == "critical"
        for s in r.flagged_sinks
    ) or any(v.status == "critical" for v in r.flagged_vms):
        overall_emoji, overall_label = "🔴", "CRITICAL"
    else:
        overall_emoji, overall_label = "🟡", "DEGRADED"

    ts_ist   = datetime.now(IST)
    date_str = ts_ist.strftime("%a %d %b %Y · %I:%M %p IST")

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "📊  Data Platform — connector health", "emoji": True},
        },
        {"type": "context", "elements": [{"type": "mrkdwn", "text": date_str}]},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Overall:* {overall_emoji} *{overall_label}*   ·   "
                    f"*{n_sinks}* CDC  ·  *{len(r.kafka_sinks)}* Kafka   ·   "
                    f"🔴🟡 {n_flag_s + n_flag_k} flagged   🟢 {n_ok_s + len(r.kafka_sinks) - n_flag_k} healthy"
                    + (f"   ·   💾 {n_flag_v} disk issue(s)" if n_flag_v else "")
                ),
            },
        },
    ]

    flagged = r.flagged_sinks[:10]
    if flagged:
        from .dp_l0_renderer import _sink_overall_icon, _short
        lines = [f"{_sink_overall_icon(s)} `{_short(s.sink)}`" for s in flagged]
        if len(r.flagged_sinks) > 10:
            lines.append(f"_+{len(r.flagged_sinks) - 10} more_")
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}})

    blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": "Full breakdown → canvas below ↓"}]})
    return blocks


def create_scheduler(group: str | None = None) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler()
    # 10:00 IST = 04:30 UTC
    scheduler.add_job(
        run_report,
        kwargs={"group": group},
        trigger=CronTrigger(hour=4, minute=30, timezone="UTC"),
        id="l0_daily_report",
        name="L0 Daily Metrics Report",
        misfire_grace_time=300,
    )
    return scheduler
