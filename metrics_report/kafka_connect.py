"""
kafka_connect.py — Fetches connector health from Kafka Connect REST API instances.

For each configured instance:
  GET /connectors           → list of connector names
  GET /connectors/{n}/status → state + task states

Only connectors that are not fully RUNNING are surfaced in the report.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import httpx

from .models import ConnectorStatus, ConnectorTask, KafkaConnectHealth, KafkaConnectInstance
from .pagerduty import fire_alert

log = logging.getLogger(__name__)

_TIMEOUT = 5.0


async def _fetch_instance(
    client: httpx.AsyncClient,
    base_url: str,
    name: str,
) -> Optional[KafkaConnectInstance]:
    url = base_url.rstrip("/")
    try:
        resp = await client.get(f"{url}/connectors", timeout=_TIMEOUT)
        if not resp.is_success:
            asyncio.create_task(fire_alert(
                summary=f"Kafka Connect '{name}' returned HTTP {resp.status_code} — instance may be degraded",
                severity="critical" if resp.status_code >= 500 else "warning",
                source=f"{url}/connectors",
                component="kafka_connect",
                details={
                    "instance": name,
                    "url": url,
                    "status_code": resp.status_code,
                    "body_preview": resp.text[:300],
                },
                dedup_key=f"kafka-connect-{name.lower().replace(' ', '-')}-http-{resp.status_code}",
            ))
        resp.raise_for_status()
        connector_names: list[str] = resp.json()
    except Exception as exc:
        log.error("Cannot reach Kafka Connect %s (%s): %s", name, url, exc)
        # Network-level failure (connection refused, timeout) — fire PD for unreachable instance
        if not isinstance(exc, httpx.HTTPStatusError):
            asyncio.create_task(fire_alert(
                summary=f"Kafka Connect '{name}' unreachable: {type(exc).__name__}: {exc}",
                severity="critical",
                source=url,
                component="kafka_connect",
                details={"instance": name, "url": url, "error": str(exc)},
                dedup_key=f"kafka-connect-{name.lower().replace(' ', '-')}-unreachable",
            ))
        return None

    unhealthy: list[ConnectorStatus] = []
    for cname in connector_names:
        try:
            resp = await client.get(f"{url}/connectors/{cname}/status", timeout=_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            connector_state = data.get("connector", {}).get("state", "UNKNOWN")
            tasks = [
                ConnectorTask(id=t["id"], state=t.get("state", "UNKNOWN"))
                for t in data.get("tasks", [])
            ]
            status = ConnectorStatus(name=cname, state=connector_state, tasks=tasks)
            if not status.is_healthy:
                unhealthy.append(status)
        except Exception as exc:
            log.warning("Failed to get status for %s / %s: %s", name, cname, exc)
            unhealthy.append(ConnectorStatus(name=cname, state="UNKNOWN", tasks=[]))

    return KafkaConnectInstance(name=name, total=len(connector_names), unhealthy=unhealthy)


async def fetch_all_connector_health(instances: dict) -> KafkaConnectHealth:
    """instances: {display_name: base_url}"""
    if not instances:
        return KafkaConnectHealth(instances=[])

    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[
            _fetch_instance(client, url, name)
            for name, url in instances.items()
        ])

    return KafkaConnectHealth(instances=[r for r in results if r is not None])
