"""
airflow_client.py — Queries the Airflow PostgreSQL metadata DB for DAG run statuses.

Fetches the latest run state for each dp_* DAG so Data Platform canvas can
surface failed/running DAGs and confirm successful ones with a single summary line
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
import sqlalchemy
from sqlalchemy import text

from .models import AirflowDagRun, AirflowHealth, ViewFlowHealth, ViewFlowRun
from .pagerduty import fire_alert

log = logging.getLogger(__name__)

# Latest run per dp_* DAG — DISTINCT ON is PostgreSQL-specific, which is fine here.
_QUERY = text("""
SELECT DISTINCT ON (dag_id)
    dag_id,
    state,
    start_date,
    end_date
FROM dag_run
WHERE dag_id = 'dp_cosmos_flag_debezium_invalid_tables'
ORDER BY dag_id, start_date DESC
""")

# Latest run per (dag_id, IST calendar date) for today + yesterday.
_PIPELINE_QUERY = text("""
SELECT DISTINCT ON (dag_id, ((start_date AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Kolkata')::date)
    dag_id,
    ((start_date AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Kolkata')::date AS run_date,
    state,
    start_date,
    end_date
FROM dag_run
WHERE dag_id IN (
    'transaction_parallel_ingestion_new_emr',
    'cost_cube_pipeline_new_emr'
)
  AND start_date >= NOW() - INTERVAL '2 days'
ORDER BY
    dag_id,
    ((start_date AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Kolkata')::date DESC,
    start_date DESC
""")


def _fetch_sync(db_url: str) -> AirflowHealth:
    engine = sqlalchemy.create_engine(
        db_url,
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        connect_args={"connect_timeout": 10},
    )
    try:
        with engine.connect() as conn:
            rows          = conn.execute(_QUERY).fetchall()
            pipeline_rows = conn.execute(_PIPELINE_QUERY).fetchall()
    finally:
        engine.dispose()

    dag_runs = [
        AirflowDagRun(
            dag_id=row.dag_id,
            state=row.state or "unknown",
            start_date=row.start_date,
            end_date=row.end_date,
        )
        for row in rows
    ]
    pipeline_runs = [
        AirflowDagRun(
            dag_id=row.dag_id,
            state=row.state or "unknown",
            start_date=row.start_date,
            end_date=row.end_date,
            run_date=row.run_date,
        )
        for row in pipeline_rows
    ]
    log.info("Airflow: fetched %d dp_* DAG run(s), %d pipeline run(s).",
             len(dag_runs), len(pipeline_runs))
    return AirflowHealth(dag_runs=dag_runs, pipeline_runs=pipeline_runs)


_VIEW_FLOW_DAG = "dp_cosmos_execute_view_flow"


async def fetch_view_flow_health(
    base_url: str,
    username: str,
    password: str,
    lookback_hours: int = 24,
) -> Optional[ViewFlowHealth]:
    """Fetch last 24h runs of dp_cosmos_execute_view_flow via Airflow REST API."""
    if not base_url:
        return None

    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
    url = f"{base_url.rstrip('/')}/api/v1/dags/{_VIEW_FLOW_DAG}/dagRuns"
    params = {
        "limit": 500,
        "order_by": "-start_date",
        "start_date_gte": cutoff_str,
    }

    try:
        async with httpx.AsyncClient(auth=(username, password), timeout=15.0) as client:
            resp = await client.get(url, params=params)
            if not resp.is_success:
                import asyncio as _asyncio
                _asyncio.create_task(fire_alert(
                    summary=f"Airflow REST API non-200: HTTP {resp.status_code} fetching view-flow DAG runs",
                    severity="critical" if resp.status_code >= 500 else "warning",
                    source=url,
                    component="airflow_client",
                    details={
                        "status_code": resp.status_code,
                        "url": url,
                        "body_preview": resp.text[:300],
                    },
                    dedup_key=f"airflow-api-{resp.status_code}",
                ))
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        log.error("Failed to fetch view flow DAG runs: %s", exc)
        return None

    runs = data.get("dag_runs", [])
    total = len(runs)
    successful = 0
    failed: list[ViewFlowRun] = []
    running: list[ViewFlowRun] = []

    for r in runs:
        conf = r.get("conf") or {}
        table_name = (
            conf.get("base_table_name")
            or conf.get("table_name")
            or conf.get("dataset_name")
            or r.get("dag_run_id", "unknown")
        )
        state = r.get("state", "unknown")
        raw_dt = r.get("start_date")
        start_dt = datetime.fromisoformat(raw_dt.replace("Z", "+00:00")) if raw_dt else None

        vr = ViewFlowRun(table_name=table_name, state=state, start_date=start_dt)
        if state == "success":
            successful += 1
        elif state == "failed":
            failed.append(vr)
        elif state == "running":
            running.append(vr)

    log.info("ViewFlow: %d runs in last %dh — %d success, %d failed, %d running.",
             total, lookback_hours, successful, len(failed), len(running))
    return ViewFlowHealth(total=total, successful=successful, failed=failed, running=running)


async def fetch_airflow_health(db_url: str) -> Optional[AirflowHealth]:
    if not db_url:
        return None
    loop = asyncio.get_running_loop()
    try:
        return await loop.run_in_executor(None, _fetch_sync, db_url)
    except Exception as exc:
        log.error("Failed to query Airflow DB: %s", exc)
        return None
