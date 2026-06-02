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
"""
import asyncio
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)

log = logging.getLogger(__name__)


async def _scheduled() -> None:
    from .scheduler import create_scheduler

    scheduler = create_scheduler()
    scheduler.start()
    log.info("Scheduler started — L0 report fires daily at 10:00 IST (04:30 UTC). Ctrl-C to stop.")
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        log.info("Scheduler stopped.")


async def _now() -> None:
    from .scheduler import run_report
    await run_report()


if __name__ == "__main__":
    if "--now" in sys.argv:
        asyncio.run(_now())
    else:
        asyncio.run(_scheduled())
