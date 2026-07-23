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

    if args.l0_now:
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
