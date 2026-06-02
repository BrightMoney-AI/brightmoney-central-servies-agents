"""
Scheduler — fires the report job daily at 10:00 IST (04:30 UTC).
Iterates over every service defined in services.json and posts one
Slack message per service.
"""
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from .config import settings
from .collector import collect
from .formatter import build_slack_payload
from .gateway import MetricsGateway
from .services import load_services
from .slack_publisher import publish
from .vm_client import VMClient

log = logging.getLogger(__name__)


async def run_report() -> None:
    services = load_services()
    log.info("Starting L0 metrics report for %d service(s)...", len(services))

    gateway = MetricsGateway(timeout_secs=settings.gateway_timeout_secs)

    async with VMClient(settings.vm_base_url) as vm:
        for service in services:
            log.info("Collecting: %s", service.display_name)
            report = await collect(vm, gateway, service)
            payload = build_slack_payload(report, service_name=service.display_name)
            await publish(payload)

            if report.failures:
                log.warning(
                    "[%s] Published with %d failed queries: %s",
                    service.display_name,
                    len(report.failures),
                    [f.name for f in report.failures],
                )
            else:
                log.info("[%s] All queries succeeded.", service.display_name)


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
