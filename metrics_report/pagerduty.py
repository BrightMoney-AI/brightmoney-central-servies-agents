"""
pagerduty.py — Thin PagerDuty Events API v2 client.

Fires "trigger" events to the configured routing key.  Both async and sync
variants are provided:

    await fire_alert(...)         — use from async code (vm_client, airflow_client, kafka_connect)
    fire_alert_sync(...)          — use from thread-executor code (trino_client)

Dedup keys collapse repeated alerts for the same root cause into one PD incident.
All network errors are caught and logged — a PD failure must never crash a report run.

Disabled entirely when PAGERDUTY_ROUTING_KEY is empty.
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Optional

import httpx

log = logging.getLogger(__name__)

_PD_EVENTS_URL = "https://events.pagerduty.com/v2/enqueue"
_TIMEOUT       = 5.0    # seconds — fast timeout; reporting must not wait on PD


# ── Payload builder ────────────────────────────────────────────────────────────

def _build_payload(
    routing_key: str,
    summary:     str,
    severity:    str,
    source:      str,
    component:   str,
    group:       str,
    details:     Optional[dict[str, Any]],
    dedup_key:   Optional[str],
) -> dict:
    payload: dict[str, Any] = {
        "routing_key":  routing_key,
        "event_action": "trigger",
        "payload": {
            "summary":   summary[:1024],   # PD limit
            "severity":  severity,
            "source":    source,
            "group":     group,
        },
    }
    if component:
        payload["payload"]["component"] = component
    if details:
        payload["payload"]["custom_details"] = details
    if dedup_key:
        payload["dedup_key"] = dedup_key
    return payload


# ── Async variant (use from coroutines) ────────────────────────────────────────

async def fire_alert(
    summary:    str,
    severity:   str = "critical",          # "critical" | "error" | "warning" | "info"
    source:     str = "metrics-report",
    component:  str = "",
    group:      str = "brightmoney-metrics-report",
    details:    Optional[dict[str, Any]] = None,
    dedup_key:  Optional[str] = None,
) -> None:
    """Fire a PagerDuty alert.  Never raises — errors are logged only.

    Args:
        summary:   Human-readable one-liner shown in PD incident title.
        severity:  PD severity level ("critical" for hard failures).
        source:    The URL / host that originated the failure.
        component: Sub-component name (e.g. "vm_client", "trino_client").
        group:     PD grouping label (default: "brightmoney-metrics-report").
        details:   Optional dict of key/value context added to the incident.
        dedup_key: If set, PD will collapse repeated alerts with the same key
                   into one incident instead of opening new ones.
    """
    # Lazy import avoids circular-import risk if config is imported very early
    from .config import settings
    routing_key = settings.pagerduty_routing_key
    if not routing_key:
        log.debug("PD alert suppressed (routing key not set): %s", summary)
        return

    payload = _build_payload(routing_key, summary, severity, source, component, group, details, dedup_key)
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.post(_PD_EVENTS_URL, json=payload)
        if resp.status_code not in (200, 201, 202):
            log.warning("PagerDuty enqueue HTTP %d: %s", resp.status_code, resp.text[:200])
        else:
            log.info("PagerDuty alert fired: %r  dedup=%r", summary, dedup_key)
    except Exception as exc:
        log.warning("PagerDuty alert not delivered: %s", exc)


# ── Sync variant (use from thread-executor code such as trino_client) ──────────

def fire_alert_sync(
    summary:    str,
    severity:   str = "critical",
    source:     str = "metrics-report",
    component:  str = "",
    group:      str = "brightmoney-metrics-report",
    details:    Optional[dict[str, Any]] = None,
    dedup_key:  Optional[str] = None,
) -> None:
    """Synchronous fire-and-forget PagerDuty alert.

    Spawns a daemon thread so the calling thread (Trino executor) is not blocked.
    Never raises.
    """
    from .config import settings
    routing_key = settings.pagerduty_routing_key
    if not routing_key:
        log.debug("PD alert suppressed (routing key not set): %s", summary)
        return

    payload = _build_payload(routing_key, summary, severity, source, component, group, details, dedup_key)

    def _send() -> None:
        try:
            with httpx.Client(timeout=_TIMEOUT) as client:
                resp = client.post(_PD_EVENTS_URL, json=payload)
            if resp.status_code not in (200, 201, 202):
                log.warning("PagerDuty (sync) enqueue HTTP %d: %s", resp.status_code, resp.text[:200])
            else:
                log.info("PagerDuty alert fired (sync): %r  dedup=%r", summary, dedup_key)
        except Exception as exc:
            log.warning("PagerDuty (sync) alert not delivered: %s", exc)

    threading.Thread(target=_send, daemon=True, name="pd-alert").start()
