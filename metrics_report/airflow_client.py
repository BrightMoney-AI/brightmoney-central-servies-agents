"""
airflow_client.py — Queries the Airflow PostgreSQL metadata DB for DAG run statuses.

Fetches the latest run state for each dp_* DAG so Data Platform canvas can
surface failed/running DAGs and confirm successful ones with a single summary line.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import sqlalchemy
from sqlalchemy import text

from .models import AirflowDagRun, AirflowHealth

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
            rows = conn.execute(_QUERY).fetchall()
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
    log.info("Airflow: fetched %d dp_* DAG run(s).", len(dag_runs))
    return AirflowHealth(dag_runs=dag_runs)


async def fetch_airflow_health(db_url: str) -> Optional[AirflowHealth]:
    if not db_url:
        return None
    loop = asyncio.get_running_loop()
    try:
        return await loop.run_in_executor(None, _fetch_sync, db_url)
    except Exception as exc:
        log.error("Failed to query Airflow DB: %s", exc)
        return None
