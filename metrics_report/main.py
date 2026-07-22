from __future__ import annotations

"""
Entry point.

Setup (from project root):
  python3 -m venv .venv && source .venv/bin/activate
  pip install -r requirements.txt
  cp .env.example .env   # set SLACK_BOT_TOKEN and SLACK_CHANNEL_ID

Services: ems.json (EMS dashboard) + services.json (general) are merged automatically.

  # Run on the daily schedule (blocks until killed; fires 10:00 IST)
  python -m metrics_report.main

  # Fire a report immediately and exit
  python -m metrics_report.main --now

  # Fire a report for one group only
  python -m metrics_report.main --now --group "Central Services"
  python -m metrics_report.main --now "Central Services"   # same, positional shorthand
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
    from .scheduler import create_scheduler
    from .hl_scheduler import create_hl_scheduler
    from .config import settings

    scheduler = create_scheduler(group=group)
    scheduler.start()
    label = f"group={group!r}" if group else "all groups"
    log.info("Scheduler started — L0 report fires daily at 10:00 IST (04:30 UTC) [%s]. Ctrl-C to stop.", label)

    if settings.slack_hl_channel_id and group is None:
        hl_scheduler = create_hl_scheduler()
        hl_scheduler.start()
        log.info("HL scheduler started — HL report fires at 10:00 IST to channel %s.", settings.slack_hl_channel_id)
    else:
        hl_scheduler = None

    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        if hl_scheduler:
            hl_scheduler.shutdown()
        log.info("Scheduler stopped.")


async def _now(group: str | None) -> None:
    from .scheduler import run_report
    from .hl_scheduler import run_hl_report
    from .config import settings
    await run_report(group=group)
    if settings.slack_hl_channel_id and group is None:
        await run_hl_report()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="L0 metrics report")
    parser.add_argument("--now",    action="store_true", help="Fire detailed + HL report immediately and exit")
    parser.add_argument("--hl-now", action="store_true", help="Fire HL report only immediately and exit")
    parser.add_argument("--group", metavar="GROUP", default=None,
                        help='Limit to one report_group, e.g. "Central Services"')
    parser.add_argument("group_name", nargs="?", default=None,
                        help=argparse.SUPPRESS)  # convenience: --now "Central Services"
    args = parser.parse_args()

    group = args.group or args.group_name

    if args.hl_now:
        from metrics_report.hl_scheduler import run_hl_report
        asyncio.run(run_hl_report())
    elif args.now:
        asyncio.run(_now(group))
    else:
        asyncio.run(_scheduled(group))
