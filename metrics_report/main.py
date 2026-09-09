from __future__ import annotations

"""
Entry point.

Setup (from project root):
  python3 -m venv .venv && source .venv/bin/activate
  pip install -r requirements.txt
  cp .env.example .env   # set SLACK_BOT_TOKEN, SLACK_CHANNEL_ID, etc.

Services: ems.json (EMS dashboard) + services.json (general) are merged automatically.

  # Run on the daily schedule (blocks until killed; fires 10:00 IST / 04:30 UTC)
  python -m metrics_report.main

  # Fire ALL reports immediately and exit (detailed + HL canvases + L0 manager snapshot)
  python -m metrics_report.main --now

  # Fire HL + L0 manager reports only (skip detailed per-service canvases)
  python -m metrics_report.main --hl-now

  # Fire L0 manager snapshot only
  python -m metrics_report.main --l0-now

  # Limit to one group (detailed + HL only — L0 manager snapshot requires all groups)
  python -m metrics_report.main --now --group "Central Services"
  python -m metrics_report.main --now "Central Services"   # positional shorthand
"""
import argparse
import asyncio
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)

log = logging.getLogger(__name__)


async def _scheduled(group: str | None) -> None:
    # Legacy detailed-report scheduler is disabled — only HL + L0 run on schedule.
    # from .scheduler import create_scheduler
    from .hl_scheduler import create_hl_scheduler
    from .config import settings

    # scheduler = create_scheduler(group=group)
    # scheduler.start()
    # label = f"group={group!r}" if group else "all groups"
    # log.info("Scheduler started — detailed report fires daily at 10:00 IST (04:30 UTC) [%s]. Ctrl-C to stop.", label)

    hl_scheduler = None
    if settings.slack_hl_channel_id or settings.slack_l0_channel_id:
        hl_scheduler = create_hl_scheduler()
        hl_scheduler.start()
        log.info(
            "HL + L0 scheduler started — fires daily at 04:30 UTC "
            "(HL channel: %s  L0 channel: %s).",
            settings.slack_hl_channel_id or "disabled",
            settings.slack_l0_channel_id or "disabled",
        )
    else:
        log.warning("No HL or L0 channel configured — nothing scheduled.")

    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        if hl_scheduler:
            hl_scheduler.shutdown()
        log.info("Scheduler stopped.")


async def _now(group: str | None) -> None:
    """Fire L0 + HL reports immediately (legacy detailed report is disabled).

    L0 manager snapshots post first so managers get the quick health verdict
    immediately.  HL canvases (full L0/L1/L2 detail) follow.
    """
    # from .scheduler import run_report   # legacy — disabled
    from .hl_scheduler import run_hl_report
    from .config import settings

    # run_hl_report posts L0 snapshots first, then HL canvases.
    await run_hl_report()

    # Legacy detailed per-service report — disabled.
    # await run_report(group=group)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Brightmoney metrics reports")
    parser.add_argument(
        "--now",
        action="store_true",
        help="Fire detailed report + HL canvases + L0 manager snapshot immediately and exit",
    )
    parser.add_argument(
        "--hl-now",
        action="store_true",
        help="Fire HL canvases + L0 manager snapshot only (skips detailed per-service report)",
    )
    parser.add_argument(
        "--l0-now",
        action="store_true",
        help="Fire L0 manager snapshot only (all-groups overview to manager channel)",
    )
    parser.add_argument(
        "--webhook-now",
        action="store_true",
        help="Fire Webhook Gateway canvas only (posts to SLACK_L0_CHANNEL_ID + SLACK_HL_CHANNEL_ID)",
    )
    parser.add_argument(
        "--group",
        metavar="GROUP",
        default=None,
        help='Limit to one report_group, e.g. "Central Services" (applies to --now only)',
    )
    parser.add_argument(
        "group_name",
        nargs="?",
        default=None,
        help=argparse.SUPPRESS,  # convenience: --now "Central Services"
    )
    args = parser.parse_args()

    group = args.group or args.group_name

    if args.webhook_now:
        async def _run_webhook() -> None:
            from datetime import datetime, timedelta, timezone

            from slack_sdk.web.async_client import AsyncWebClient
            from slack_sdk.errors import SlackApiError

            from metrics_report.config import settings
            from metrics_report.webhook_gateway_collector import collect_webhook_gateway
            from metrics_report.webhook_gateway_renderer import (
                render_webhook_gateway_canvas,
                render_webhook_gateway_summary_blocks,
            )

            IST = timezone(timedelta(hours=5, minutes=30))
            date_str = datetime.now(IST).strftime("%d %b %Y")
            channels = [ch for ch in [settings.slack_l0_channel_id, settings.slack_hl_channel_id] if ch]
            if not channels:
                channels = [settings.slack_channel_id]

            log.info("Collecting Webhook Gateway metrics from CloudWatch...")
            report = await collect_webhook_gateway()
            title  = f"Webhook Gateway — Health Overview — {date_str}"
            md     = render_webhook_gateway_canvas(report, date_str)
            blocks = render_webhook_gateway_summary_blocks(report, date_str)
            log.info("Canvas: %d chars  status=%s  collector_failures=%d", len(md), report.status.value, len(report.failures))

            client = AsyncWebClient(token=settings.slack_bot_token)
            try:
                resp = await client.api_call(
                    "canvases.create",
                    json={"title": title, "document_content": {"type": "markdown", "markdown": md}},
                )
                canvas_id = resp.get("canvas_id", "")
                log.info("Canvas created: canvas_id=%s", canvas_id)
            except SlackApiError as exc:
                log.error("Canvas create failed: %s", exc.response["error"])
                return

            canvas_url = ""
            try:
                auth = await client.auth_test()
                canvas_url = f"{auth.get('url', '').rstrip('/')}/docs/{auth.get('team_id', '')}/{canvas_id}"
            except SlackApiError:
                pass

            for ch in channels:
                try:
                    await client.chat_postMessage(channel=ch, text=f"📊 {title}", blocks=blocks)
                    log.info("Summary posted to %s", ch)
                except SlackApiError as exc:
                    log.error("Summary post failed [%s]: %s", ch, exc.response["error"])
                if canvas_url:
                    try:
                        await client.chat_postMessage(channel=ch, text=canvas_url, unfurl_links=True)
                        log.info("Canvas card posted to %s: %s", ch, canvas_url)
                    except SlackApiError as exc:
                        log.error("Canvas card post failed [%s]: %s", ch, exc.response["error"])

        asyncio.run(_run_webhook())
    elif args.l0_now:
        from metrics_report.hl_scheduler import run_l0_manager_only
        asyncio.run(run_l0_manager_only())
        log.info("L0 manager snapshot complete.")
    elif args.hl_now:
        from metrics_report.hl_scheduler import run_hl_report
        asyncio.run(run_hl_report())
    elif args.now:
        asyncio.run(_now(group))
    else:
        asyncio.run(_scheduled(group))
