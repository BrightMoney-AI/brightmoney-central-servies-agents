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

from .queries import load_uaa
from .trino_client import execute_query
from .vm_client import VMClient
from .config import settings

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


def _fmt_float(v, decimals: int = 1) -> str:
    if v is None:
        return "N/A"
    try:
        return f"{float(v):.{decimals}f}"
    except (TypeError, ValueError):
        return "N/A"


# ── Onboarding Provider Sessions ──────────────────────────────────────────────
# Counts total sessions and successful sessions per provider for D day vs D-1 day.
# Provider is derived from session/event JSON in priority order:
#   1. event response provider_data  2. session accounts.checking aggregator
#   3. session_creation provider     4. routing_service provider_name
# Only AKOYA, PLAID, DL_CAPITALONE are included.

_TRINO_ONBOARDING_PROVIDER_SESSIONS = load_uaa("onboarding_provider_sessions")


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

_TRINO_ACCOUNT_LINKING_BY_SOURCE = load_uaa("account_linking_by_source")


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
        source    = str(row.get("client_source") or "unknown")
        flow      = str(row.get("flow_type")     or "Other")
        today     = int(row.get("today_sessions")     or 0)
        yesterday = int(row.get("yesterday_sessions") or 0)
        delta     = today - yesterday
        delta_str = f"+{delta}" if delta >= 0 else str(delta)
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

_TRINO_PLAID_BATCH_RECENCY = load_uaa("plaid_batch_recency")


async def _fetch_plaid_batch_recency() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_PLAID_BATCH_RECENCY)
    except Exception as exc:
        log.error("Plaid batch recency query failed: %s", exc)
        return []
    if not rows:
        return []
    r        = rows[0]
    p50      = r.get("p50") or 0
    p75      = r.get("p75") or 0
    p90      = r.get("p90") or 0
    p95      = r.get("p95") or 0
    p99      = r.get("p99") or 0
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

_TRINO_PLAID_BATCH_METADATA_RECENCY = load_uaa("plaid_batch_metadata_recency")


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

_TRINO_PLAID_BATCH_HISTORICAL_RECENCY = load_uaa("plaid_batch_historical_recency")

_RECENCY_COLS = ["metric_calculated_time", "p50", "p75", "p90", "p95", "p99", "number_of_accounts"]


async def _fetch_plaid_batch_historical_recency() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_PLAID_BATCH_HISTORICAL_RECENCY)
    except Exception as exc:
        log.error("Plaid batch historical recency query failed: %s", exc)
        return []
    if not rows:
        return []
    cols   = [c for c in _RECENCY_COLS if c in rows[0]] or [k for k in rows[0] if k != "institution_name"]
    header = "|".join(
        c.replace("metric_calculated_time", "Time").replace("number_of_accounts", "Accounts")
        for c in cols
    )
    details = [header]
    for r in rows[:24]:
        parts = []
        for c in cols:
            v = r.get(c)
            parts.append(_fmt_ts(v) if c == "metric_calculated_time" else (str(v) if v is not None else "N/A"))
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

_TRINO_PLAID_BATCH_REFRESH_ERRORS = load_uaa("plaid_batch_refresh_errors")


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


# ── Plaid Batch Refresh: Hourly Health Trend ──────────────────────────────────
# Hourly success% and error% for the last 24 hours.

_TRINO_PLAID_BATCH_TREND = load_uaa("plaid_batch_trend")


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

_TRINO_PLAID_FORCE_REFRESH_DAILY = load_uaa("plaid_force_refresh_daily")


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
        pct_s  = f"{pct}%" if pct is not None else "N/A"
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

_TRINO_PLAID_FORCE_REFRESH_ERRORS = load_uaa("plaid_force_refresh_errors")


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

_TRINO_PLAID_FORCE_REFRESH_TREND = load_uaa("plaid_force_refresh_trend")


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


# ── ALSM Latency (P99) ────────────────────────────────────────────────────────
# End-to-end latency from LINKING_SUCCESS_EVENT to
# ACCOUNTS_CREATED_IN_ENTITY_MANAGER_APP_EVENT for PLAID and DL_CAPITALONE at P99.
# Value is in seconds (raw metric is milliseconds, divided by 1000).
# Compares today's value vs the same instant 24 hours ago.

def _alsm_latency_promql(aggregator: str) -> str:
    return (
        f'sum('
        f'alsm_event_time_diff_metrics{{'
        f'environment="prod",'
        f'aggregator="{aggregator}",'
        f'quantile="0.99",'
        f'event1="LINKING_SUCCESS_EVENT",'
        f'event2="ACCOUNTS_CREATED_IN_ENTITY_MANAGER_APP_EVENT"'
        f'}}/1000)'
    )


async def _fetch_alsm_latency() -> list[BusinessMetric]:
    aggregators = ["PLAID", "DL_CAPITALONE"]
    try:
        async with VMClient(settings.vm_base_url) as vm:
            queries = []
            for agg in aggregators:
                q = _alsm_latency_promql(agg)
                queries += [vm.query(q), vm.query(f"{q} offset 24h")]
            results = await asyncio.gather(*queries)
    except Exception as exc:
        log.error("ALSM latency query failed: %s", exc)
        return []

    details    = ["Aggregator|Today P99|Yesterday P99|Change"]
    best_today = 0.0

    for i, agg in enumerate(aggregators):
        today_raw, yesterday_raw = results[i * 2], results[i * 2 + 1]
        if today_raw is None and yesterday_raw is None:
            continue
        today_s     = float(today_raw)     if today_raw     is not None else 0.0
        yesterday_s = float(yesterday_raw) if yesterday_raw is not None else 0.0
        delta       = today_s - yesterday_s
        delta_str   = f"+{delta:.1f}s" if delta >= 0 else f"{delta:.1f}s"
        delta_fmt   = f"🔴 {delta_str}" if delta > 0 else f"🟢 {delta_str}"
        details.append(f"{agg}|{today_s:.1f}s|{yesterday_s:.1f}s|{delta_fmt}")
        best_today  = max(best_today, today_s)
        log.info("ALSM latency [%s]: today=%.1fs yesterday=%.1fs", agg, today_s, yesterday_s)

    if len(details) == 1:
        log.info("ALSM latency: no data returned for any aggregator.")
        return []

    return [BusinessMetric(
        display_name="ALSM Latency — P99 (LINKING_SUCCESS → ACCOUNTS_CREATED)",
        query_name="alsm_latency",
        section="ALSM",
        metric_type="multi_col_table",
        value=best_today,
        details=details,
    )]


# ── SAISM Latency (P99) ───────────────────────────────────────────────────────
# End-to-end latency from ACCOUNTS_INGESTION_START_EVENT to
# ACCOUNTS_CREATED_IN_ENTITY_MANAGER_APP_EVENT for CRBAA and BRIGHT at P99.

def _saism_latency_promql(aggregator: str) -> str:
    return (
        f'sum('
        f'saism_event_time_diff_metrics{{'
        f'environment="prod",'
        f'aggregator="{aggregator}",'
        f'quantile="0.99",'
        f'event1="ACCOUNTS_INGESTION_START_EVENT",'
        f'event2="ACCOUNTS_CREATED_IN_ENTITY_MANAGER_APP_EVENT"'
        f'}}/1000)'
    )


async def _fetch_saism_latency() -> list[BusinessMetric]:
    aggregators = ["CRBAA", "BRIGHT"]
    try:
        async with VMClient(settings.vm_base_url) as vm:
            queries = []
            for agg in aggregators:
                q = _saism_latency_promql(agg)
                queries += [vm.query(q), vm.query(f"{q} offset 24h")]
            results = await asyncio.gather(*queries)
    except Exception as exc:
        log.error("SAISM latency query failed: %s", exc)
        return []

    details    = ["Aggregator|Today P99|Yesterday P99|Change"]
    best_today = 0.0

    for i, agg in enumerate(aggregators):
        today_raw, yesterday_raw = results[i * 2], results[i * 2 + 1]
        if today_raw is None and yesterday_raw is None:
            continue
        today_s     = float(today_raw)     if today_raw     is not None else 0.0
        yesterday_s = float(yesterday_raw) if yesterday_raw is not None else 0.0
        delta       = today_s - yesterday_s
        delta_str   = f"+{delta:.1f}s" if delta >= 0 else f"{delta:.1f}s"
        delta_fmt   = f"🔴 {delta_str}" if delta > 0 else f"🟢 {delta_str}"
        details.append(f"{agg}|{today_s:.1f}s|{yesterday_s:.1f}s|{delta_fmt}")
        best_today  = max(best_today, today_s)
        log.info("SAISM latency [%s]: today=%.1fs yesterday=%.1fs", agg, today_s, yesterday_s)

    if len(details) == 1:
        log.info("SAISM latency: no data returned for any aggregator.")
        return []

    return [BusinessMetric(
        display_name="SAISM Latency — P99 (ACCOUNTS_INGESTION_START → ACCOUNTS_CREATED)",
        query_name="saism_latency",
        section="SAISM",
        metric_type="multi_col_table",
        value=best_today,
        details=details,
    )]


# ── Partner Cost Breakdown ────────────────────────────────────────────────────
# Daily snapshot of costs per partner from cost_cube.
# billing_type = ONE_TIME  → daily cost (per-transaction / usage charges)
# billing_type = MONTHLY   → maintenance cost (recurring monthly fees)

_TRINO_PARTNER_COSTS = load_uaa("partner_costs")


async def _fetch_partner_costs() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_PARTNER_COSTS)
    except Exception as exc:
        log.error("Partner cost breakdown query failed: %s", exc)
        return []
    if not rows:
        log.info("Partner costs: no data for today.")
        return []

    total_daily = 0.0
    details = ["Partner|Daily Cost|Maintenance Cost"]
    for row in rows:
        partner     = str(row.get("partner") or "Unknown")
        daily       = float(row.get("daily_cost")       or 0)
        maintenance = float(row.get("maintenance_cost") or 0)
        details.append(f"{partner}|${daily:,.2f}|${maintenance:,.2f}")
        total_daily += daily

    log.info("Partner costs: %d partner(s).", len(rows))
    return [BusinessMetric(
        display_name="Partner Cost Breakdown (Today)",
        query_name="partner_costs",
        section="Partner Costs",
        metric_type="multi_col_table",
        value=total_daily,
        details=details,
    )]


# ── Txn Quality Metrics ───────────────────────────────────────────────────────
# Per account-creation cohort (last 2 days) × provider (All / PLAID / DL_CAPITALONE):
#   - transaction duration avg and P95 (days)
#   - transaction count avg and P95
# Uses a tall/unpivoted layout — one row per (date, provider).

_TRINO_TXN_QUALITY_METRICS = load_uaa("txn_quality_metrics")


async def _fetch_txn_quality_metrics() -> list[BusinessMetric]:
    try:
        rows = await execute_query(_TRINO_TXN_QUALITY_METRICS)
    except Exception as exc:
        log.error("Txn quality metrics query failed: %s", exc)
        return []
    if not rows:
        log.info("Txn quality metrics: no data returned.")
        return []

    details = ["Date|Provider|Avg Dur (days)|P95 Dur (days)|Avg Txn Count|P95 Txn Count"]
    for r in rows:
        date_str = str(r.get("cohort_date") or "N/A")
        provider = str(r.get("provider")    or "N/A")
        avg_dur  = _fmt_float(r.get("avg_txn_duration_days"))
        p95_dur  = _fmt_float(r.get("p95_txn_duration_days"), 0)
        avg_cnt  = _fmt_float(r.get("avg_txn_count"))
        p95_cnt  = _fmt_float(r.get("p95_txn_count"), 0)
        details.append(f"{date_str}|{provider}|{avg_dur}|{p95_dur}|{avg_cnt}|{p95_cnt}")

    log.info("Txn quality metrics: %d row(s) (date × provider).", len(rows))
    return [BusinessMetric(
        display_name="Txn Quality by Account Cohort",
        query_name="txn_quality",
        section="Txn Quality",
        metric_type="multi_col_table",
        value=float(rows[0].get("avg_txn_duration_days") or 0),
        details=details,
    )]


# ── Public entry point ────────────────────────────────────────────────────────

async def collect_uaa_business_metrics() -> list[BusinessMetric]:
    """Collect all UAA business metrics from Trino and VictoriaMetrics.

    Fast queries (HTTP + lightweight Trino) run fully concurrently.
    Plaid/heavy Trino queries run with a concurrency cap of 3 to avoid
    saturating the Trino queue (which triggers 15s–45s retry backoff).
    """
    fast = await asyncio.gather(
        _fetch_onboarding_provider_sessions(),
        _fetch_account_linking_by_source(),
        _fetch_alsm_latency(),
        _fetch_saism_latency(),
    )

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
        _limited(_fetch_partner_costs()),
        _limited(_fetch_txn_quality_metrics()),
    )

    metrics: list[BusinessMetric] = [m for batch in (*fast, *plaid) for m in batch]
    log.info("UAA business metrics collected: %d metric(s).", len(metrics))
    return metrics
