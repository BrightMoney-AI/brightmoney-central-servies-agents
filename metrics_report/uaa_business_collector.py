"""
uaa_business_collector.py — UAA Services business metrics from Trino/Iceberg.

Add one async function per query block, return list[BusinessMetric] per function,
then call them all inside collect_uaa_business_metrics().
Canvas is skipped automatically when this returns an empty list.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

from .trino_client import execute_query

log = logging.getLogger(__name__)


@dataclass
class BusinessMetric:
    display_name: str
    query_name:   str
    section:      str
    metric_type:  str    # "success_rate" | "failure_count" | "total_count" | "rate" | "provider_comparison"
    value:        float
    details:      list[str] = field(default_factory=list)  # rows for provider_comparison tables


# ── Onboarding Provider Sessions ──────────────────────────────────────────────
# Counts total sessions and successful sessions per provider for D day vs D-1 day.
# Provider is derived from session/event JSON in priority order:
#   1. event response provider_data  2. session accounts.checking aggregator
#   3. session_creation provider     4. routing_service provider_name
# Only AKOYA, PLAID, DL_CAPITALONE are included.

_TRINO_ONBOARDING_PROVIDER_SESSIONS = """
WITH session_events AS (
    SELECT
        s.id               AS session_id,
        s.created_at       AS session_created_at,
        COALESCE(
            json_extract_scalar(e.response,    '$.action_data.provider_data.provider'),
            json_extract_scalar(s.session_data,'$.accounts.checking[0].aggregator'),
            json_extract_scalar(s.session_data,'$.session_creation_on_provider_app_response.provider'),
            json_extract_scalar(s.session_data,'$.routing_service_response.provider_name')
        ) AS provider,
        e.event_name
    FROM iceberg_db.brightmoney_core_uaa__public__alsm_accountlinkingsession__current_view_presto s
    JOIN iceberg_db.brightmoney_core_uaa__public__alsm_accountlinkingeventdata__current_view_presto e
      ON e.account_linking_session_id = s.id
    WHERE json_extract_scalar(s.session_data, '$.flow_data.flow_type')     = 'ONBOARDING'
      AND json_extract_scalar(s.session_data, '$.flow_data.linking_for')   = 'CHECKING'
      AND json_extract_scalar(s.session_data, '$.flow_data.linking_flow')  = 'ADD'
      AND s.created_at >= CURRENT_DATE - INTERVAL '2' DAY
),
sessions_base AS (
    SELECT
        session_id,
        session_created_at,
        provider,
        MAX(CASE WHEN event_name = 'ACCOUNTS_CREATED_IN_ENTITY_MANAGER_APP_EVENT' THEN 1 ELSE 0 END) AS is_success
    FROM session_events
    GROUP BY session_id, session_created_at, provider
),
d_day AS (
    SELECT
        provider,
        COUNT(*)          AS sessions,
        SUM(is_success)   AS success_sessions
    FROM sessions_base
    WHERE DATE(session_created_at) = CURRENT_DATE
      AND provider IN ('AKOYA', 'PLAID', 'DL_CAPITALONE')
    GROUP BY provider
),
d_minus_1 AS (
    SELECT
        provider,
        COUNT(*)          AS sessions,
        SUM(is_success)   AS success_sessions
    FROM sessions_base
    WHERE DATE(session_created_at) = CURRENT_DATE - INTERVAL '1' DAY
      AND provider IN ('AKOYA', 'PLAID', 'DL_CAPITALONE')
    GROUP BY provider
)
SELECT
    COALESCE(d.provider,          d1.provider)          AS provider,
    COALESCE(d.sessions,          0)                    AS d_sessions,
    COALESCE(d.success_sessions,  0)                    AS d_success,
    COALESCE(d1.sessions,         0)                    AS d1_sessions,
    COALESCE(d1.success_sessions, 0)                    AS d1_success
FROM d_day d
FULL OUTER JOIN d_minus_1 d1 ON d1.provider = d.provider
ORDER BY COALESCE(d.provider, d1.provider)
"""


async def _fetch_onboarding_provider_sessions() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_ONBOARDING_PROVIDER_SESSIONS)
    except Exception as exc:
        log.error("Onboarding provider sessions query failed: %s", exc)
        return []

    if not rows:
        log.info("Onboarding provider sessions: no data returned.")
        return []

    details: list[str] = []
    total_d_success = 0

    for row in rows:
        provider    = str(row.get("provider") or "Unknown")
        d_sessions  = int(row.get("d_sessions")  or 0)
        d_success   = int(row.get("d_success")   or 0)
        d1_sessions = int(row.get("d1_sessions") or 0)
        d1_success  = int(row.get("d1_success")  or 0)
        d_pct  = (d_success  / d_sessions  * 100) if d_sessions  else 0.0
        d1_pct = (d1_success / d1_sessions * 100) if d1_sessions else 0.0
        # Pipe-delimited row picked up by the renderer to build a Markdown table
        details.append(
            f"{provider}|{d_sessions}|{d_success} ({d_pct:.1f}%)|{d1_sessions}|{d1_success} ({d1_pct:.1f}%)"
        )
        total_d_success += d_success

    log.info("Onboarding provider sessions: %d provider row(s).", len(details))

    return [BusinessMetric(
        display_name="Total Success Sessions per Provider",
        query_name="onboarding_provider_sessions",
        section="Onboarding",
        metric_type="provider_comparison",
        value=float(total_d_success),
        details=details,
    )]


# ── Public entry point ────────────────────────────────────────────────────────

async def collect_uaa_business_metrics() -> list[BusinessMetric]:
    """Collect all UAA business metrics from Trino concurrently."""
    results = await asyncio.gather(
        _fetch_onboarding_provider_sessions(),
    )
    metrics: list[BusinessMetric] = [m for batch in results for m in batch]
    log.info("UAA business metrics collected: %d metric(s).", len(metrics))
    return metrics
