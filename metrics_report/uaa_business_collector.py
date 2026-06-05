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
    metric_type:  str    # "success_rate" | "failure_count" | "total_count" | "rate"
                         # | "provider_comparison"  — D vs D-1 per provider table
                         # | "source_comparison"    — Today vs Yesterday per source × flow table
                         # | "multi_col_table"      — generic table: details[0]=headers, details[1:]=rows
    value:        float
    details:      list[str] = field(default_factory=list)  # pipe-delimited rows for table metrics


def _fmt_ts(v) -> str:
    """Format a Trino timestamp (datetime obj or string) compactly for tables."""
    if v is None:
        return "-"
    from datetime import datetime
    if isinstance(v, datetime):
        return v.strftime("%b %d %H:%M")
    s = str(v)
    return s[:16]  # "2026-06-05 10:00" from "2026-06-05 10:00:00.000"


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


# ── Successful Account Linkings by Source & Flow ──────────────────────────────
# Counts sessions where ACCOUNTS_CREATED_IN_ENTITY_MANAGER_APP_EVENT fired,
# broken down by client_source (web / android / ios) and flow type
# (Onboarding vs Other), comparing the last 4 hours vs the same 4-hour window
# 24 hours ago (yesterday).

_TRINO_ACCOUNT_LINKING_BY_SOURCE = """
WITH successful_sessions AS (
    SELECT DISTINCT
        s.id           AS session_id,
        s.created_at,
        JSON_EXTRACT_SCALAR(s.session_data, '$.flow_data.client_source') AS client_source,
        CASE
            WHEN JSON_EXTRACT_SCALAR(s.session_data, '$.flow_data.flow_type') = 'ONBOARDING'
            THEN 'Onboarding'
            ELSE 'Other'
        END AS flow_type
    FROM iceberg_db.brightmoney_core_uaa__public__alsm_accountlinkingsession__current_view_presto s
    JOIN iceberg_db.brightmoney_core_uaa__public__alsm_accountlinkingeventdata__current_view_presto e
        ON e.account_linking_session_id = s.id
    WHERE e.event_name = 'ACCOUNTS_CREATED_IN_ENTITY_MANAGER_APP_EVENT'
      AND JSON_EXTRACT_SCALAR(s.session_data, '$.flow_data.client_source') IN ('web', 'android', 'ios')
      AND s.created_at >= NOW() - INTERVAL '28' HOUR
),
today_agg AS (
    SELECT client_source, flow_type, COUNT(*) AS sessions
    FROM successful_sessions
    WHERE created_at >= NOW() - INTERVAL '4' HOUR
    GROUP BY client_source, flow_type
),
yesterday_agg AS (
    SELECT client_source, flow_type, COUNT(*) AS sessions
    FROM successful_sessions
    WHERE created_at >= NOW() - INTERVAL '28' HOUR
      AND created_at <  NOW() - INTERVAL '24' HOUR
    GROUP BY client_source, flow_type
)
SELECT
    COALESCE(t.client_source, y.client_source) AS client_source,
    COALESCE(t.flow_type,     y.flow_type)     AS flow_type,
    COALESCE(t.sessions,      0)               AS today_sessions,
    COALESCE(y.sessions,      0)               AS yesterday_sessions
FROM today_agg t
FULL OUTER JOIN yesterday_agg y
    ON  y.client_source = t.client_source
    AND y.flow_type     = t.flow_type
ORDER BY client_source, flow_type
"""


async def _fetch_account_linking_by_source() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_ACCOUNT_LINKING_BY_SOURCE)
    except Exception as exc:
        log.error("Account linking by source query failed: %s", exc)
        return []

    if not rows:
        log.info("Account linking by source: no data returned.")
        return []

    details: list[str] = []
    total_today = 0

    for row in rows:
        source     = str(row.get("client_source") or "unknown")
        flow       = str(row.get("flow_type")     or "Other")
        today      = int(row.get("today_sessions")     or 0)
        yesterday  = int(row.get("yesterday_sessions") or 0)
        delta      = today - yesterday
        delta_str  = f"+{delta}" if delta >= 0 else str(delta)
        # Pipe-delimited: source | flow | today | yesterday | delta
        details.append(f"{source}|{flow}|{today}|{yesterday}|{delta_str}")
        total_today += today

    log.info("Account linking by source: %d row(s).", len(details))

    return [BusinessMetric(
        display_name="Successful Account Linkings",
        query_name="account_linking_by_source",
        section="Account Linking",
        metric_type="source_comparison",
        value=float(total_today),
        details=details,
    )]


# ── Plaid Batch Refresh: Data Recency ─────────────────────────────────────────
# Computes p50/p75/p90/p95/p99 of hours since last_data_updated_at across all
# accounts in plaid_batch_refresh_metadata. Lower = fresher data.

_TRINO_PLAID_BATCH_RECENCY = """
WITH recency_delta AS (
    SELECT
        DATE_DIFF('hour',
            GREATEST(COALESCE(last_balance_force_fetched, last_data_updated_at), last_data_updated_at),
            current_timestamp
        ) AS delta_hrs
    FROM uaa_db.plaid_batch_refresh_metadata
    WHERE COALESCE(last_balance_force_fetched, last_data_updated_at) IS NOT NULL
)
SELECT
    COUNT(*)                             AS number_of_accounts,
    approx_percentile(delta_hrs, 0.50)  AS p50,
    approx_percentile(delta_hrs, 0.75)  AS p75,
    approx_percentile(delta_hrs, 0.90)  AS p90,
    approx_percentile(delta_hrs, 0.95)  AS p95,
    approx_percentile(delta_hrs, 0.99)  AS p99
FROM recency_delta
"""


async def _fetch_plaid_batch_recency() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_PLAID_BATCH_RECENCY)
    except Exception as exc:
        log.error("Plaid batch recency query failed: %s", exc)
        return []
    if not rows:
        return []
    r = rows[0]
    p50      = r.get("p50")      or 0
    p75      = r.get("p75")      or 0
    p90      = r.get("p90")      or 0
    p95      = r.get("p95")      or 0
    p99      = r.get("p99")      or 0
    accounts = int(r.get("number_of_accounts") or 0)
    return [BusinessMetric(
        display_name="Data Recency (hrs)",
        query_name="plaid_batch_recency",
        section="Plaid Batch Refresh",
        metric_type="multi_col_table",
        value=float(p50),
        details=[
            "Accounts|P50|P75|P90|P95|P99",
            f"{accounts:,}|{p50}|{p75}|{p90}|{p95}|{p99}",
        ],
    )]


# ── Plaid Batch Refresh: Metadata Recency ─────────────────────────────────────
# MIN recency (hours) across all run_timestamp rows — how fresh is the metadata.

_TRINO_PLAID_BATCH_METADATA_RECENCY = """
SELECT
    MIN(DATE_DIFF('minute', run_timestamp, current_timestamp) / 60.0) AS recency_hrs
FROM uaa_db.plaid_batch_refresh_metadata
"""


async def _fetch_plaid_batch_metadata_recency() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_PLAID_BATCH_METADATA_RECENCY)
    except Exception as exc:
        log.error("Plaid batch metadata recency query failed: %s", exc)
        return []
    if not rows or rows[0].get("recency_hrs") is None:
        return []
    return [BusinessMetric(
        display_name="Metadata Recency (hrs)",
        query_name="plaid_batch_metadata_recency",
        section="Plaid Batch Refresh",
        metric_type="total_count",
        value=float(rows[0]["recency_hrs"]),
    )]


# ── Plaid Batch Refresh: Historical Recency ────────────────────────────────────
# Last 2 days of pre-computed recency percentiles from the metrics table.

_TRINO_PLAID_BATCH_HISTORICAL_RECENCY = """
SELECT *
FROM uaa_db.plaid_batch_refresh_recency_metrics
WHERE institution_name = 'Overall'
  AND metric_calculated_time >= date_add('day', -2, current_timestamp)
ORDER BY metric_calculated_time DESC
"""

_RECENCY_COLS = ["metric_calculated_time", "p50", "p75", "p90", "p95", "p99", "number_of_accounts"]


async def _fetch_plaid_batch_historical_recency() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_PLAID_BATCH_HISTORICAL_RECENCY)
    except Exception as exc:
        log.error("Plaid batch historical recency query failed: %s", exc)
        return []
    if not rows:
        return []
    # Use expected columns if present, otherwise fall back to all keys
    cols = [c for c in _RECENCY_COLS if c in rows[0]] or [k for k in rows[0] if k != "institution_name"]
    header = "|".join(c.replace("metric_calculated_time", "Time").replace("number_of_accounts", "Accounts") for c in cols)
    details = [header]
    for r in rows[:24]:
        parts = []
        for c in cols:
            v = r.get(c)
            parts.append(_fmt_ts(v) if c == "metric_calculated_time" else (str(v) if v is not None else "-"))
        details.append("|".join(parts))
    return [BusinessMetric(
        display_name="Historical Recency (Last 2 Days)",
        query_name="plaid_batch_historical_recency",
        section="Plaid Batch Refresh",
        metric_type="multi_col_table",
        value=float(rows[0].get("p50") or 0),
        details=details,
    )]


# ── Plaid Batch Refresh: Error Reasons ────────────────────────────────────────
# Top error reasons (by item count) in the last 24 hours, grouped by hour.

_TRINO_PLAID_BATCH_REFRESH_ERRORS = """
WITH error_data AS (
    SELECT
        item_pid,
        date_trunc('hour', CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP)) AS hour,
        CASE
            WHEN stitch_failed_reason LIKE 'no valid subtype%'                                           THEN 'no valid subtype'
            WHEN stitch_failed_reason LIKE 'BETA item account mapped response%'                          THEN 'BETA item no mapped account'
            WHEN stitch_failed_reason LIKE 'ALPHA item account mapped response%'                         THEN 'ALPHA item no mapped account'
            WHEN stitch_failed_reason LIKE 'failed:\"api error:ITEM_LOGIN_REQUIRED\"%'                   THEN 'ITEM_LOGIN_REQUIRED'
            WHEN stitch_failed_reason LIKE 'failed:\"api error:NO_ACCOUNTS\"%'                           THEN 'NO_ACCOUNTS'
            WHEN stitch_failed_reason LIKE 'failed:\"api error:ITEM_NOT_FOUND\"%'                        THEN 'ITEM_NOT_FOUND'
            WHEN stitch_failed_reason LIKE 'failed:\"api error:INTERNAL_SERVER_ERROR\"'                  THEN 'INTERNAL_SERVER_ERROR'
            WHEN stitch_failed_reason LIKE 'failed:\"api error:ITEM_NOT_SUPPORTED\"'                     THEN 'ITEM_NOT_SUPPORTED'
            WHEN stitch_failed_reason LIKE 'failed:\"api error:MFA_NOT_SUPPORTED\"'                      THEN 'MFA_NOT_SUPPORTED'
            WHEN stitch_failed_reason LIKE 'failed:\"errors: error in fetching Item ITEM_NOT_FOUND\"'    THEN 'ITEM_NOT_FOUND'
            WHEN stitch_failed_reason LIKE 'failed:\"errors: error in fetching Item ITEM_GET_LIMIT\"'    THEN 'ITEM_GET_LIMIT'
            WHEN stitch_failed_reason LIKE 'failed:\"errors: error in fetching Item INTERNAL_SERVER_ERROR\"' THEN 'INTERNAL_SERVER_ERROR'
            WHEN stitch_failed_reason LIKE 'failed:\"last_successful_updated_on < last_balance_force_fetched%' THEN 'already have latest balance'
            WHEN stitch_failed_reason LIKE 'failed:\"no accounts%'                                       THEN 'no accounts from API'
            WHEN stitch_failed_reason LIKE 'escalated%'                                                  THEN 'escalated_accounts'
            ELSE stitch_failed_reason
        END AS reason
    FROM uaa_db.plaid_batch_refresh_error_data
    WHERE CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) >= CURRENT_DATE - INTERVAL '1' DAY
)
SELECT
    hour,
    reason,
    COUNT(DISTINCT item_pid) AS counts
FROM error_data
GROUP BY hour, reason
ORDER BY hour DESC, counts DESC
"""


async def _fetch_plaid_batch_refresh_errors() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_PLAID_BATCH_REFRESH_ERRORS)
    except Exception as exc:
        log.error("Plaid batch refresh errors query failed: %s", exc)
        return []
    if not rows:
        return []
    total = sum(int(r.get("counts") or 0) for r in rows)
    shown = rows[:20]
    details = ["Hour|Reason|Count"]
    for r in shown:
        details.append(f"{_fmt_ts(r.get('hour'))}|{r.get('reason') or '-'}|{r.get('counts') or 0}")
    if len(rows) > 20:
        details.append(f"…|+{len(rows)-20} more rows|")
    return [BusinessMetric(
        display_name="Error Reasons (Last 24h)",
        query_name="plaid_batch_refresh_errors",
        section="Plaid Batch Refresh",
        metric_type="multi_col_table",
        value=float(total),
        details=details,
    )]


# ── Plaid Batch Refresh: Success / Failure Trend ──────────────────────────────
# Hourly success% and error% for the last 24 hours.

_TRINO_PLAID_BATCH_TREND = """
WITH error_data AS (
    SELECT CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) AS ts, 'error' AS status
    FROM uaa_db.plaid_batch_refresh_error_data
    WHERE CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) >= CURRENT_DATE - INTERVAL '1' DAY
),
success_data AS (
    SELECT CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) AS ts, 'success' AS status
    FROM uaa_db.plaid_batch_refresh_data
    WHERE CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) >= CURRENT_DATE - INTERVAL '1' DAY
),
combined AS (SELECT * FROM success_data UNION ALL SELECT * FROM error_data)
SELECT
    date_trunc('hour', ts)                                                       AS metric_hour,
    ROUND(100.0 * SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) / COUNT(*), 2) AS success_pct,
    ROUND(100.0 * SUM(CASE WHEN status = 'error'   THEN 1 ELSE 0 END) / COUNT(*), 2) AS error_pct
FROM combined
GROUP BY date_trunc('hour', ts)
ORDER BY metric_hour DESC
"""


async def _fetch_plaid_batch_trend() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_PLAID_BATCH_TREND)
    except Exception as exc:
        log.error("Plaid batch trend query failed: %s", exc)
        return []
    if not rows:
        return []
    latest_success = float(rows[0].get("success_pct") or 0)
    details = ["Hour|Success %|Error %"]
    for r in rows[:24]:
        details.append(f"{_fmt_ts(r.get('metric_hour'))}|{r.get('success_pct') or 0}%|{r.get('error_pct') or 0}%")
    return [BusinessMetric(
        display_name="Hourly Refresh Health (Last 24h)",
        query_name="plaid_batch_trend",
        section="Plaid Batch Refresh",
        metric_type="multi_col_table",
        value=latest_success,
        details=details,
    )]


# ── Plaid Force Refresh: Daily Metrics ────────────────────────────────────────
# Today's summary: total, rejected, eligible, success, error counts and %.

_TRINO_PLAID_FORCE_REFRESH_DAILY = """
WITH summary AS (
    SELECT state, COUNT(DISTINCT item_pid) AS item_counts
    FROM uaa_db.plaid_force_refresh_metadata
    WHERE run_date = CURRENT_DATE
    GROUP BY state
    UNION ALL
    SELECT 'Total' AS state, COUNT(DISTINCT item_pid) AS item_counts
    FROM uaa_db.plaid_force_refresh_metadata
    WHERE run_date = CURRENT_DATE
),
totals AS (
    SELECT
        MAX(CASE WHEN state = 'Total'                              THEN item_counts END) AS total_items,
        MAX(CASE WHEN state = 'NOT_FOUND_IN_PLAID_METADATA'        THEN item_counts END) AS not_found,
        MAX(CASE WHEN state = 'ELIGIBLE_FOR_FORCE_REFRESH'         THEN item_counts END) AS eligible,
        MAX(CASE WHEN state = 'REJECTED_DUE_TO_RECENCY'            THEN item_counts END) AS rejected_recency,
        MAX(CASE WHEN state = 'REJECTED_DUE_TO_NULL_LAST_UPDATE'   THEN item_counts END) AS rejected_null
    FROM summary
),
success_items AS (
    SELECT DISTINCT item_pid
    FROM uaa_db.plaid_force_refresh_data
    WHERE CAST(from_iso8601_timestamp(bright_updated_at) AS DATE) = CURRENT_DATE
),
error_items AS (
    SELECT DISTINCT item_pid
    FROM uaa_db.plaid_force_refresh_error_data
    WHERE CAST(from_iso8601_timestamp(bright_updated_at) AS DATE) = CURRENT_DATE
),
unique_errors AS (
    SELECT item_pid FROM error_items
    WHERE item_pid NOT IN (SELECT item_pid FROM success_items)
)
SELECT 'Total Items'                                          AS metric,
       total_items                                            AS count,
       CAST(NULL AS DOUBLE)                                   AS percentage
FROM totals
UNION ALL
SELECT 'Rejected (Recency + Null Last Update)',
       (COALESCE(rejected_recency,0) + COALESCE(rejected_null,0)),
       ROUND(((COALESCE(rejected_recency,0) + COALESCE(rejected_null,0)) * 100.0)
             / NULLIF(total_items - COALESCE(not_found,0), 0), 2)
FROM totals
UNION ALL
SELECT 'Eligible for Force Refresh',
       eligible,
       ROUND((COALESCE(eligible,0) * 100.0) / NULLIF(total_items - COALESCE(not_found,0), 0), 2)
FROM totals
UNION ALL
SELECT 'Success',
       COUNT(DISTINCT item_pid),
       ROUND((COUNT(DISTINCT item_pid) * 100.0) / NULLIF((SELECT eligible FROM totals), 0), 2)
FROM success_items
UNION ALL
SELECT 'Error (unique failures)',
       COUNT(DISTINCT item_pid),
       ROUND((COUNT(DISTINCT item_pid) * 100.0) / NULLIF((SELECT eligible FROM totals), 0), 2)
FROM unique_errors
ORDER BY CASE metric
    WHEN 'Total Items'                              THEN 1
    WHEN 'Rejected (Recency + Null Last Update)'   THEN 2
    WHEN 'Eligible for Force Refresh'               THEN 3
    WHEN 'Success'                                  THEN 4
    ELSE 5
END
"""


async def _fetch_plaid_force_refresh_daily() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_PLAID_FORCE_REFRESH_DAILY)
    except Exception as exc:
        log.error("Plaid force refresh daily metrics query failed: %s", exc)
        return []
    if not rows:
        return []
    success_count = 0
    details = ["Metric|Count|%"]
    for r in rows:
        metric = r.get("metric") or "-"
        count  = r.get("count")  or 0
        pct    = r.get("percentage")
        pct_s  = f"{pct}%" if pct is not None else "-"
        details.append(f"{metric}|{count:,}|{pct_s}")
        if "Success" in str(metric):
            success_count = int(count or 0)
    return [BusinessMetric(
        display_name="Daily Metrics (Today)",
        query_name="plaid_force_refresh_daily",
        section="Plaid Force Refresh",
        metric_type="multi_col_table",
        value=float(success_count),
        details=details,
    )]


# ── Plaid Force Refresh: Error Reasons ────────────────────────────────────────
# Top error reasons per day for the last 7 days.

_TRINO_PLAID_FORCE_REFRESH_ERRORS = """
WITH error_data AS (
    SELECT
        item_pid,
        CAST(from_iso8601_timestamp(bright_updated_at) AS DATE) AS bright_date,
        CASE
            WHEN stitch_failed_reason LIKE 'no valid subtype%'                                           THEN 'no valid subtype'
            WHEN stitch_failed_reason LIKE 'failed:\"api error:INSTITUTION_NOT_RESPONDING\"%'            THEN 'INSTITUTION_NOT_RESPONDING'
            WHEN stitch_failed_reason LIKE 'failed:\"api error:ITEM_LOGIN_REQUIRED\"%'                   THEN 'ITEM_LOGIN_REQUIRED'
            WHEN stitch_failed_reason LIKE 'failed:\"api error:NO_ACCOUNTS\"%'                           THEN 'NO_ACCOUNTS'
            WHEN stitch_failed_reason LIKE 'failed:\"api error:ITEM_NOT_FOUND\"%'                        THEN 'ITEM_NOT_FOUND'
            WHEN stitch_failed_reason LIKE 'failed:\"api error:INTERNAL_SERVER_ERROR\"'                  THEN 'INTERNAL_SERVER_ERROR'
            WHEN stitch_failed_reason LIKE 'failed:\"api error:ITEM_NOT_SUPPORTED\"'                     THEN 'ITEM_NOT_SUPPORTED'
            WHEN stitch_failed_reason LIKE 'failed:\"api error:MFA_NOT_SUPPORTED\"'                      THEN 'MFA_NOT_SUPPORTED'
            WHEN stitch_failed_reason LIKE 'failed:\"api error:INVALID_FIELD\"'                          THEN 'INVALID_FIELD'
            WHEN stitch_failed_reason LIKE 'failed:\"api error:ACCESS_NOT_GRANTED\"'                     THEN 'ACCESS_NOT_GRANTED'
            WHEN stitch_failed_reason LIKE 'failed:\"errors: error in fetching Item ITEM_NOT_FOUND\"'    THEN 'ITEM_NOT_FOUND'
            WHEN stitch_failed_reason LIKE 'failed:\"errors: error in fetching Item ITEM_GET_LIMIT\"'    THEN 'ITEM_GET_LIMIT'
            WHEN stitch_failed_reason LIKE 'failed:\"errors: error in fetching Item INTERNAL_SERVER_ERROR\"' THEN 'INTERNAL_SERVER_ERROR'
            WHEN stitch_failed_reason LIKE 'failed:\"no accounts%'                                       THEN 'no accounts from API'
            WHEN stitch_failed_reason LIKE 'escalated%'                                                  THEN 'escalated_accounts'
            ELSE stitch_failed_reason
        END AS reason
    FROM uaa_db.plaid_force_refresh_error_data
    WHERE CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) >= CURRENT_DATE - INTERVAL '7' DAY
      AND CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) >= TIMESTAMP '2025-01-06'
)
SELECT
    bright_date,
    reason,
    COUNT(DISTINCT item_pid) AS counts
FROM error_data
GROUP BY bright_date, reason
ORDER BY bright_date DESC, counts DESC
"""


async def _fetch_plaid_force_refresh_errors() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_PLAID_FORCE_REFRESH_ERRORS)
    except Exception as exc:
        log.error("Plaid force refresh errors query failed: %s", exc)
        return []
    if not rows:
        return []
    total = sum(int(r.get("counts") or 0) for r in rows)
    shown = rows[:20]
    details = ["Date|Reason|Count"]
    for r in shown:
        details.append(f"{r.get('bright_date') or '-'}|{r.get('reason') or '-'}|{r.get('counts') or 0}")
    if len(rows) > 20:
        details.append(f"…|+{len(rows)-20} more rows|")
    return [BusinessMetric(
        display_name="Error Reasons (Last 7 Days)",
        query_name="plaid_force_refresh_errors",
        section="Plaid Force Refresh",
        metric_type="multi_col_table",
        value=float(total),
        details=details,
    )]


# ── Plaid Force Refresh: Success / Failure Trend ──────────────────────────────
# Daily success% and error% for the last 7 days.

_TRINO_PLAID_FORCE_REFRESH_TREND = """
WITH error_raw AS (
    SELECT CAST(from_iso8601_timestamp(bright_updated_at) AS DATE) AS bright_date, item_pid
    FROM uaa_db.plaid_force_refresh_error_data
    WHERE CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) >= CURRENT_DATE - INTERVAL '7' DAY
      AND CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) >= TIMESTAMP '2025-01-09'
),
success_raw AS (
    SELECT CAST(from_iso8601_timestamp(bright_updated_at) AS DATE) AS bright_date, item_pid
    FROM uaa_db.plaid_force_refresh_data
    WHERE CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) >= CURRENT_DATE - INTERVAL '7' DAY
      AND CAST(from_iso8601_timestamp(bright_updated_at) AS TIMESTAMP) >= TIMESTAMP '2025-01-09'
),
unique_errors AS (
    SELECT bright_date, COUNT(DISTINCT item_pid) AS error_count
    FROM error_raw
    WHERE item_pid NOT IN (SELECT item_pid FROM success_raw)
    GROUP BY bright_date
),
unique_success AS (
    SELECT bright_date, COUNT(DISTINCT item_pid) AS success_count
    FROM success_raw
    GROUP BY bright_date
),
eligible AS (
    SELECT run_date AS bright_date, COUNT(DISTINCT item_pid) AS total_count
    FROM uaa_db.plaid_force_refresh_metadata
    WHERE run_date >= CURRENT_DATE - INTERVAL '7' DAY
      AND run_date >= DATE '2025-01-09'
      AND state = 'ELIGIBLE_FOR_FORCE_REFRESH'
    GROUP BY run_date
)
SELECT
    e.bright_date,
    ROUND((COALESCE(us.success_count, 0) * 100.0) / NULLIF(e.total_count, 0), 2) AS success_pct,
    ROUND((COALESCE(ue.error_count,   0) * 100.0) / NULLIF(e.total_count, 0), 2) AS error_pct
FROM eligible e
LEFT JOIN unique_errors  ue ON e.bright_date = ue.bright_date
LEFT JOIN unique_success us ON e.bright_date = us.bright_date
ORDER BY e.bright_date DESC
"""


async def _fetch_plaid_force_refresh_trend() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_PLAID_FORCE_REFRESH_TREND)
    except Exception as exc:
        log.error("Plaid force refresh trend query failed: %s", exc)
        return []
    if not rows:
        return []
    latest_success = float(rows[0].get("success_pct") or 0)
    details = ["Date|Success %|Error %"]
    for r in rows:
        details.append(f"{r.get('bright_date') or '-'}|{r.get('success_pct') or 0}%|{r.get('error_pct') or 0}%")
    return [BusinessMetric(
        display_name="Success / Failure Trend (Last 7 Days)",
        query_name="plaid_force_refresh_trend",
        section="Plaid Force Refresh",
        metric_type="multi_col_table",
        value=latest_success,
        details=details,
    )]


# ── Public entry point ────────────────────────────────────────────────────────

async def collect_uaa_business_metrics() -> list[BusinessMetric]:
    """Collect all UAA business metrics from Trino.

    Existing ALSM/onboarding queries run fully concurrently.
    Plaid queries run with a concurrency cap of 3 to avoid saturating
    the Trino queue (which triggers the 15s–45s retry backoff).
    """
    # Fast ALSM / account-linking queries — fully concurrent
    fast = await asyncio.gather(
        _fetch_onboarding_provider_sessions(),
        _fetch_account_linking_by_source(),
    )

    # Plaid queries — limit to 3 concurrent Trino requests at a time
    sem = asyncio.Semaphore(3)

    async def _limited(coro):
        async with sem:
            return await coro

    plaid = await asyncio.gather(
        _limited(_fetch_plaid_batch_recency()),
        _limited(_fetch_plaid_batch_metadata_recency()),
        _limited(_fetch_plaid_batch_historical_recency()),
        _limited(_fetch_plaid_batch_refresh_errors()),
        _limited(_fetch_plaid_batch_trend()),
        _limited(_fetch_plaid_force_refresh_daily()),
        _limited(_fetch_plaid_force_refresh_errors()),
        _limited(_fetch_plaid_force_refresh_trend()),
    )

    metrics: list[BusinessMetric] = [m for batch in (*fast, *plaid) for m in batch]
    log.info("UAA business metrics collected: %d metric(s).", len(metrics))
    return metrics
