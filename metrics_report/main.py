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

    scheduler = create_scheduler(group=group)
    scheduler.start()
    label = f"group={group!r}" if group else "all groups"
    log.info("Scheduler started — L0 report fires daily at 10:00 IST (04:30 UTC) [%s]. Ctrl-C to stop.", label)
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        log.info("Scheduler stopped.")


async def _now(group: str | None) -> None:
    from .scheduler import run_report
    await run_report(group=group)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="L0 metrics report")
    parser.add_argument("--now", action="store_true", help="Fire report immediately and exit")
    parser.add_argument("--group", metavar="GROUP", default=None,
                        help='Limit to one report_group, e.g. "Central Services"')
    args = parser.parse_args()

    if args.now:
        asyncio.run(_now(args.group))
    else:
        asyncio.run(_scheduled(args.group))
