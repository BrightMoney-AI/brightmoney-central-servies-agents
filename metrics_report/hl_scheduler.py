"""
hl_scheduler.py — High-level channel report job.

Fires at 04:30 UTC (10:00 IST) alongside the detailed report.
Reuses all existing collectors unchanged; posts 3 canvases to SLACK_HL_CHANNEL_ID.

Existing scheduler.py is completely unchanged.
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient

from .airflow_client import fetch_airflow_health, fetch_view_flow_health
from .collector import collect
from .config import settings
from .formatter import to_l0_report
from .gateway import MetricsGateway
from .hl_canvas_renderer import render_dp_l2_canvas, render_hl_canvas
from .l0_manager_renderer import (
    render_l0_group_canvas,
    render_l0_group_summary_blocks,
    render_l0_manager_canvas,
    render_l0_manager_summary_blocks,
)
from .kafka_connect import fetch_all_connector_health
from .models import AirflowHealth, Status
from .services import load_services
from .vm_client import VMClient

IST = timezone(timedelta(hours=5, minutes=30))
log = logging.getLogger(__name__)

_GROUP_ORDER = ["UAA Services", "UKS Services", "Central Services", "Data Platform"]


# ── Slack publish (HL channel) ─────────────────────────────────────────────────

_MAX_CANVAS_CHARS = 80_000  # Slack gateway times out above ~100 KB; stay safe


def _trim_canvas(markdown: str) -> str:
    """Fallback trimmer for non-DP canvases: drop L2 if content exceeds the limit.

    For Data Platform, overflow is handled proactively by posting L2 as a
    separate canvas (see ``run_hl_report``), so this function is only a
    safety-net for UAA / Central / UKS which have smaller L2 sections.
    """
    if len(markdown) <= _MAX_CANVAS_CHARS:
        return markdown
    # Find the L2 heading and cut there
    for marker in ("\n---\n## L2", "\n## L2"):
        idx = markdown.find(marker)
        if idx != -1:
            trimmed = markdown[:idx] + "\n\n> *L2 deep-analysis section omitted — canvas size limit reached.*\n"
            log.warning("Canvas trimmed at L2 boundary: %d → %d chars", len(markdown), len(trimmed))
            return trimmed
    # No L2 marker — hard truncate at word boundary
    truncated = markdown[:_MAX_CANVAS_CHARS].rsplit("\n", 1)[0]
    truncated += "\n\n> *Canvas truncated — content exceeded size limit.*\n"
    log.warning("Canvas hard-truncated: %d → %d chars", len(markdown), len(truncated))
    return truncated


async def _publish_dp_l2_canvas(
    markdown: str,
    title: str,
    channel: str,
    client: "AsyncWebClient",
) -> None:
    """Create the standalone DP L2 canvas and post a link message to the channel."""
    if not markdown:
        log.info("DP L2 canvas skipped — no content.")
        return

    log.info("DP L2 canvas size: %d chars  title=%r", len(markdown), title)

    # Hard-cap the L2 canvas itself too (safety net)
    if len(markdown) > _MAX_CANVAS_CHARS:
        markdown = markdown[:_MAX_CANVAS_CHARS].rsplit("\n", 1)[0]
        markdown += "\n\n> *Canvas truncated — content exceeded size limit.*\n"
        log.warning("DP L2 canvas hard-truncated to %d chars", len(markdown))

    try:
        resp = await client.api_call(
            "canvases.create",
            json={
                "title": title,
                "document_content": {"type": "markdown", "markdown": markdown},
            },
        )
        canvas_id = resp.get("canvas_id", "")
        log.info("DP L2 canvas created: canvas_id=%s  title=%r", canvas_id, title)
    except SlackApiError as exc:
        log.error("DP L2 canvas create error: %s", exc.response["error"])
        return

    # Build and post the link card
    canvas_url = ""
    try:
        auth      = await client.auth_test()
        team_id   = auth.get("team_id", "")
        workspace = auth.get("url", "").rstrip("/")
        canvas_url = f"{workspace}/docs/{team_id}/{canvas_id}"
    except SlackApiError:
        pass

    try:
        await client.chat_postMessage(
            channel=channel,
            text=canvas_url or f"📋 {title}",
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"*📋 {title}*\n"
                            "_L2 deep analysis — validation failures, stale views, EMR cube detail_"
                        ),
                    },
                },
            ],
            unfurl_links=bool(canvas_url),
        )
        if canvas_url:
            await client.chat_postMessage(
                channel=channel,
                text=canvas_url,
                unfurl_links=True,
            )
        log.info("DP L2 canvas card posted: %s", canvas_url)
    except SlackApiError as exc:
        log.error("DP L2 canvas card post error: %s", exc.response["error"])


async def _publish_l0_manager_canvas(
    markdown: str,
    summary_blocks: list[dict],
    title: str,
) -> None:
    """Post the all-groups L0 snapshot canvas to SLACK_L0_CHANNEL_ID."""
    client  = AsyncWebClient(token=settings.slack_bot_token)
    channel = settings.slack_l0_channel_id

    if len(markdown) > _MAX_CANVAS_CHARS:
        markdown = markdown[:_MAX_CANVAS_CHARS].rsplit("\n", 1)[0]
        markdown += "\n\n> *Canvas truncated — content exceeded size limit.*\n"
        log.warning("L0 manager canvas truncated to %d chars", len(markdown))

    log.info("L0 manager canvas: %d chars  title=%r", len(markdown), title)

    try:
        resp = await client.api_call(
            "canvases.create",
            json={
                "title": title,
                "document_content": {"type": "markdown", "markdown": markdown},
            },
        )
        canvas_id = resp.get("canvas_id", "")
        log.info("L0 manager canvas created: canvas_id=%s", canvas_id)
    except SlackApiError as exc:
        log.error("L0 manager canvas create error: %s", exc.response["error"])
        raise

    canvas_url = ""
    try:
        auth      = await client.auth_test()
        team_id   = auth.get("team_id", "")
        workspace = auth.get("url", "").rstrip("/")
        canvas_url = f"{workspace}/docs/{team_id}/{canvas_id}"
    except SlackApiError:
        pass

    try:
        await client.chat_postMessage(
            channel=channel,
            text=f"📊 {title}",
            blocks=summary_blocks,
        )
        log.info("L0 manager summary posted: %r", title)
    except SlackApiError as exc:
        log.error("L0 manager summary post error: %s", exc.response["error"])
        raise

    if canvas_url:
        try:
            await client.chat_postMessage(
                channel=channel,
                text=canvas_url,
                unfurl_links=True,
            )
            log.info("L0 manager canvas card posted: %s", canvas_url)
        except SlackApiError as exc:
            log.error("L0 manager canvas card post error: %s", exc.response["error"])


async def _publish_l0_group_canvas(
    markdown: str,
    summary_blocks: list[dict],
    title: str,
) -> None:
    """Post a single per-group L0 manager canvas to SLACK_L0_CHANNEL_ID.

    Each group (UAA / DP / Central / UKS) gets its own canvas posted as a
    separate message to the manager channel — managers can read them in order
    or jump to the group they care about.
    """
    channel = settings.slack_l0_channel_id
    client  = AsyncWebClient(token=settings.slack_bot_token)

    if len(markdown) > _MAX_CANVAS_CHARS:
        markdown = markdown[:_MAX_CANVAS_CHARS].rsplit("\n", 1)[0]
        markdown += "\n\n> *Canvas truncated — content exceeded size limit.*\n"
        log.warning("L0 group canvas truncated to %d chars for %r", len(markdown), title)

    log.info("L0 group canvas: %d chars  title=%r", len(markdown), title)

    try:
        resp = await client.api_call(
            "canvases.create",
            json={
                "title": title,
                "document_content": {"type": "markdown", "markdown": markdown},
            },
        )
        canvas_id = resp.get("canvas_id", "")
        log.info("L0 group canvas created: canvas_id=%s  title=%r", canvas_id, title)
    except SlackApiError as exc:
        log.error("L0 group canvas create error [%r]: %s", title, exc.response["error"])
        return

    canvas_url = ""
    try:
        auth      = await client.auth_test()
        team_id   = auth.get("team_id", "")
        workspace = auth.get("url", "").rstrip("/")
        canvas_url = f"{workspace}/docs/{team_id}/{canvas_id}"
    except SlackApiError:
        pass

    try:
        await client.chat_postMessage(
            channel=channel,
            text=f"📊 {title}",
            blocks=summary_blocks,
        )
    except SlackApiError as exc:
        log.error("L0 group canvas summary post error [%r]: %s", title, exc.response["error"])

    if canvas_url:
        try:
            await client.chat_postMessage(
                channel=channel,
                text=canvas_url,
                unfurl_links=True,
            )
            log.info("L0 group canvas card posted: %s", canvas_url)
        except SlackApiError as exc:
            log.error("L0 group canvas card post error [%r]: %s", title, exc.response["error"])


async def _publish_hl_canvas(markdown: str, summary_blocks: list[dict], title: str) -> None:
    """Create a canvas and post summary + canvas-card to SLACK_HL_CHANNEL_ID."""
    client  = AsyncWebClient(token=settings.slack_bot_token)
    channel = settings.slack_hl_channel_id

    markdown = _trim_canvas(markdown)
    log.info("Canvas size: %d chars for %r", len(markdown), title)

    try:
        resp = await client.api_call(
            "canvases.create",
            json={
                "title": title,
                "document_content": {"type": "markdown", "markdown": markdown},
            },
        )
        canvas_id = resp.get("canvas_id", "")
        log.info("HL canvas created: canvas_id=%s  title=%r", canvas_id, title)
    except SlackApiError as exc:
        log.error("HL canvas create error: %s", exc.response["error"])
        raise

    canvas_url = ""
    try:
        auth      = await client.auth_test()
        team_id   = auth.get("team_id", "")
        workspace = auth.get("url", "").rstrip("/")
        canvas_url = f"{workspace}/docs/{team_id}/{canvas_id}"
    except SlackApiError:
        pass

    try:
        await client.chat_postMessage(
            channel=channel,
            text=f"📊 {title}",
            blocks=summary_blocks,
        )
        log.info("HL summary posted for %r", title)
    except SlackApiError as exc:
        log.error("HL summary post error: %s", exc.response["error"])
        raise

    if canvas_url:
        try:
            await client.chat_postMessage(
                channel=channel,
                text=canvas_url,
                unfurl_links=True,
            )
            log.info("HL canvas card posted: %s", canvas_url)
        except SlackApiError as exc:
            log.error("HL canvas card post error: %s", exc.response["error"])


def _hl_summary_blocks(
    collected: list[tuple[str, object]],
    group_name: str,
) -> list[dict]:
    from .models import L0Report
    reports: list[L0Report] = [r for _, r in collected]
    n_crit = sum(1 for r in reports if r.status == Status.CRITICAL)
    n_warn = sum(1 for r in reports if r.status == Status.WARNING)
    n_ok   = sum(1 for r in reports if r.status == Status.HEALTHY)

    if n_crit:
        overall_emoji, overall_label = "🔴", "CRITICAL"
    elif n_warn:
        overall_emoji, overall_label = "🟡", "DEGRADED"
    else:
        overall_emoji, overall_label = "🟢", "ALL SYSTEMS HEALTHY"

    ts_str = datetime.now(IST).strftime("%a %d %b %Y · %I:%M %p IST")

    return [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"📊  {group_name} — HL Health Overview", "emoji": True},
        },
        {"type": "context", "elements": [{"type": "mrkdwn", "text": ts_str}]},
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
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "L0 → L1 → L2 tiered detail in canvas below ↓"}],
        },
    ]


# ── Main report job ────────────────────────────────────────────────────────────

async def run_hl_report() -> None:
    if not settings.slack_hl_channel_id:
        log.info("SLACK_HL_CHANNEL_ID not set — HL report skipped.")
        return

    services = load_services()
    log.info("Starting HL report for %d service(s)...", len(services))

    gateway = MetricsGateway(timeout_secs=settings.gateway_timeout_secs)
    groups: dict[str, list[tuple[str, object]]] = defaultdict(list)

    central_biz_metrics: list = []
    uaa_biz_metrics:     list = []
    dp_biz_metrics:      list = []
    emr_report                = None
    dp_l0_report              = None
    uks_metrics               = None
    ti_kafka_metrics          = None

    async with VMClient(settings.vm_base_url, headers=settings.vm_headers) as vm:
        for service in services:
            if not service.system_selector and not service.api_selector:
                continue
            log.info("HL collecting: %s [group=%s]", service.display_name, service.report_group)
            raw = await collect(vm, gateway, service)
            l0  = to_l0_report(raw, service_name=service.display_name, show_api_metrics=bool(service.api_job))
            groups[service.report_group].append((service.display_name, l0))

        dp_l0_svc = next((s for s in services if s.display_name == "Data Platform L0"), None)
        if dp_l0_svc and (dp_l0_svc.kafka_cdc_sinks or dp_l0_svc.kafka_sinks):
            from .dp_l0_collector import collect_dp_l0
            dp_l0_report = await collect_dp_l0(
                vm,
                dp_l0_svc.kafka_cdc_sinks,
                dp_l0_svc.kafka_sinks or None,
            )

        from .uks_collector import collect_uks_metrics
        uks_metrics = await collect_uks_metrics(vm)

        from .central_business_collector import collect_business_metrics
        central_biz_metrics = await collect_business_metrics(vm)

    from .uaa_business_collector import collect_uaa_business_metrics
    from .uaa_kafka_collector import collect_ti_kafka_metrics
    uaa_biz_metrics, ti_kafka_metrics = await asyncio.gather(
        collect_uaa_business_metrics(),
        collect_ti_kafka_metrics(),
    )

    from .dp_business_collector import collect_dp_business_metrics
    dp_biz_metrics = await collect_dp_business_metrics()

    from .emr_collector import collect_emr_metrics
    emr_report = await collect_emr_metrics()

    date_str = datetime.now(IST).strftime("%d %b %Y")

    connector_result, db_result, view_flow_result = await asyncio.gather(
        fetch_all_connector_health(settings.kafka_connect_instances) if settings.kafka_connect_instances else asyncio.sleep(0),
        fetch_airflow_health(settings.airflow_db_url),
        fetch_view_flow_health(
            settings.airflow_api_url,
            settings.airflow_api_username,
            settings.airflow_api_password,
        ),
    )
    connector_health = connector_result if connector_result else None
    airflow_health   = AirflowHealth(
        dag_runs=db_result.dag_runs if db_result else [],
        view_flow=view_flow_result,
    )

    ordered_keys  = [g for g in _GROUP_ORDER if g in groups]
    ordered_keys += [g for g in groups if g not in _GROUP_ORDER]

    client = AsyncWebClient(token=settings.slack_bot_token)

    for group_name in ordered_keys:
        collected    = groups[group_name]
        canvas_title = f"{group_name} — Health Overview — {date_str}"

        is_dp  = group_name == "Data Platform"
        is_uaa = group_name == "UAA Services"
        is_cen = group_name == "Central Services"
        is_uks = group_name == "UKS Services"

        # ── Data Platform: L2 is ALWAYS posted as a separate canvas ──────────
        # This keeps the main canvas focused on live health (L0/L1) and the
        # L2 canvas as a dedicated deep-analysis document — regardless of size.
        if is_dp:
            l2_title    = f"Data Platform — L2 Deep Analysis — {date_str}"
            l2_markdown = render_dp_l2_canvas(
                dp_biz_metrics, emr_report, title=l2_title, date_str=date_str
            )

            markdown = render_hl_canvas(
                group_name=group_name,
                reports=collected,
                title=canvas_title,
                dp_biz_metrics=dp_biz_metrics,
                dp_l0_report=dp_l0_report,
                emr_report=emr_report,
                connector_health=connector_health,
                airflow_health=airflow_health,
                include_l2=False,
                l2_canvas_note=(
                    f"→ *L2 Deep Analysis* (base refresh failures, stale views, full EMR tables) "
                    f"continues in the next canvas: *{l2_title}*"
                ) if l2_markdown else "",
            )

            log.info("DP main canvas: %d chars (L2 excluded).", len(markdown))
            summary_blocks = _hl_summary_blocks(collected, group_name)
            await _publish_hl_canvas(markdown, summary_blocks, title=canvas_title)
            log.info("HL canvas posted: %r (%d service(s)).", canvas_title, len(collected))

            if l2_markdown:
                await _publish_dp_l2_canvas(
                    l2_markdown, l2_title,
                    channel=settings.slack_hl_channel_id,
                    client=client,
                )
            continue

        # ── Non-DP groups ──────────────────────────────────────────────────────
        markdown = render_hl_canvas(
            group_name=group_name,
            reports=collected,
            title=canvas_title,
            uaa_biz_metrics=uaa_biz_metrics         if is_uaa else None,
            central_biz_metrics=central_biz_metrics if is_cen else None,
            connector_health=connector_health       if is_dp  else None,
            airflow_health=airflow_health           if is_dp  else None,
            uks_metrics=uks_metrics                 if is_uks else None,
            ti_kafka_metrics=ti_kafka_metrics        if is_uaa else None,
        )

        summary_blocks = _hl_summary_blocks(collected, group_name)
        await _publish_hl_canvas(markdown, summary_blocks, title=canvas_title)
        log.info("HL canvas posted: %r (%d service(s)).", canvas_title, len(collected))

    # ── L0 Manager Snapshots — one focused canvas per group ───────────────────
    # Posts to SLACK_L0_CHANNEL_ID.  Each group (UAA / DP / Central / UKS) gets
    # its own canvas with L0 metrics + a plain-English health verdict.
    # Reuses already-collected data — zero extra VM queries.
    if settings.slack_l0_channel_id and groups:
        _l0_group_kwargs: dict[str, dict] = {
            "UAA Services": {
                "uaa_biz_metrics":   uaa_biz_metrics,
                "ti_kafka_metrics":  ti_kafka_metrics,
            },
            "Data Platform": {
                "dp_biz_metrics": dp_biz_metrics,
                "dp_l0_report":   dp_l0_report,
                "emr_report":     emr_report,
                "airflow_health": airflow_health,
            },
            "Central Services": {
                "central_biz_metrics": central_biz_metrics,
            },
            "UKS Services": {
                "uks_metrics": uks_metrics,
            },
        }

        l0_ordered = [g for g in _GROUP_ORDER if g in groups] + [
            g for g in groups if g not in _GROUP_ORDER
        ]
        for grp in l0_ordered:
            grp_services = groups[grp]
            kwargs       = _l0_group_kwargs.get(grp, {})
            grp_title    = f"{grp} — Manager Snapshot — {date_str}"
            grp_md       = render_l0_group_canvas(
                grp, grp_services, date_str, **kwargs
            )
            if grp_md:
                grp_blocks = render_l0_group_summary_blocks(grp, grp_services, date_str)
                try:
                    await _publish_l0_group_canvas(grp_md, grp_blocks, title=grp_title)
                    log.info("L0 group canvas posted: %r", grp_title)
                except Exception as exc:
                    log.error("L0 group canvas failed [%r]: %s", grp_title, exc)


async def run_l0_manager_only() -> None:
    """Collect all metrics and post per-group L0 manager canvases.

    Posts one focused canvas per group (UAA / DP / Central / UKS) to
    SLACK_L0_CHANNEL_ID.  Skips the per-group HL canvases (SLACK_HL_CHANNEL_ID).
    Use ``python -m metrics_report.main --l0-now`` to invoke this.
    """
    if not settings.slack_l0_channel_id:
        log.info("SLACK_L0_CHANNEL_ID not set — L0 manager snapshots skipped.")
        return

    services = load_services()
    log.info("L0 manager: collecting metrics for %d service(s)...", len(services))

    gateway = MetricsGateway(timeout_secs=settings.gateway_timeout_secs)
    groups: dict[str, list[tuple[str, object]]] = defaultdict(list)

    uaa_biz_metrics:     list = []
    central_biz_metrics: list = []
    dp_biz_metrics:      list = []
    ti_kafka_metrics           = None
    uks_metrics                = None
    dp_l0_report               = None
    emr_report                 = None
    airflow_health             = None

    async with VMClient(settings.vm_base_url, headers=settings.vm_headers) as vm:
        for service in services:
            if not service.system_selector and not service.api_selector:
                continue
            raw = await collect(vm, gateway, service)
            l0  = to_l0_report(raw, service_name=service.display_name, show_api_metrics=bool(service.api_job))
            groups[service.report_group].append((service.display_name, l0))

        from .uks_collector import collect_uks_metrics
        uks_metrics = await collect_uks_metrics(vm)

        from .central_business_collector import collect_business_metrics
        central_biz_metrics = await collect_business_metrics(vm)

        dp_l0_svc = next((s for s in services if s.display_name == "Data Platform L0"), None)
        if dp_l0_svc and (dp_l0_svc.kafka_cdc_sinks or dp_l0_svc.kafka_sinks):
            from .dp_l0_collector import collect_dp_l0
            dp_l0_report = await collect_dp_l0(
                vm,
                dp_l0_svc.kafka_cdc_sinks,
                dp_l0_svc.kafka_sinks or None,
            )

    # Out-of-VM collectors (Trino-based)
    from .uaa_business_collector import collect_uaa_business_metrics
    from .uaa_kafka_collector import collect_ti_kafka_metrics
    from .dp_business_collector import collect_dp_business_metrics
    from .emr_collector import collect_emr_metrics

    (
        uaa_biz_metrics,
        ti_kafka_metrics,
        dp_biz_metrics,
        emr_report,
    ) = await asyncio.gather(
        collect_uaa_business_metrics(),
        collect_ti_kafka_metrics(),
        collect_dp_business_metrics(),
        collect_emr_metrics(),
    )

    # Airflow health (non-blocking — skip silently if not configured)
    _, airflow_db_result, view_flow_result = await asyncio.gather(
        asyncio.sleep(0),
        fetch_airflow_health(settings.airflow_db_url),
        fetch_view_flow_health(
            settings.airflow_api_url,
            settings.airflow_api_username,
            settings.airflow_api_password,
        ),
    )
    airflow_health = AirflowHealth(
        dag_runs=airflow_db_result.dag_runs if airflow_db_result else [],
        view_flow=view_flow_result,
    )

    date_str = datetime.now(IST).strftime("%d %b %Y")

    _l0_group_kwargs: dict[str, dict] = {
        "UAA Services": {
            "uaa_biz_metrics":  uaa_biz_metrics,
            "ti_kafka_metrics": ti_kafka_metrics,
        },
        "Data Platform": {
            "dp_biz_metrics": dp_biz_metrics,
            "dp_l0_report":   dp_l0_report,
            "emr_report":     emr_report,
            "airflow_health": airflow_health,
        },
        "Central Services": {
            "central_biz_metrics": central_biz_metrics,
        },
        "UKS Services": {
            "uks_metrics": uks_metrics,
        },
    }

    l0_ordered = [g for g in _GROUP_ORDER if g in groups] + [
        g for g in groups if g not in _GROUP_ORDER
    ]
    posted = 0
    for grp in l0_ordered:
        grp_services = groups[grp]
        kwargs       = _l0_group_kwargs.get(grp, {})
        grp_title    = f"{grp} — Manager Snapshot — {date_str}"
        grp_md       = render_l0_group_canvas(grp, grp_services, date_str, **kwargs)
        if grp_md:
            grp_blocks = render_l0_group_summary_blocks(grp, grp_services, date_str)
            try:
                await _publish_l0_group_canvas(grp_md, grp_blocks, title=grp_title)
                log.info("L0 group canvas posted: %r", grp_title)
                posted += 1
            except Exception as exc:
                log.error("L0 group canvas failed [%r]: %s", grp_title, exc)

    if posted:
        log.info("L0 manager snapshots complete: %d group canvas(es) posted to %s.", posted, settings.slack_l0_channel_id)
    else:
        log.warning("L0 manager: no canvases posted — groups may be empty.")


def create_hl_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        run_hl_report,
        trigger=CronTrigger(hour=4, minute=30, timezone="UTC"),
        id="hl_daily_report",
        name="HL Daily Metrics Report",
        misfire_grace_time=300,
    )
    return scheduler
