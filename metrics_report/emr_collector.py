"""
emr_collector.py — EMR / Cosmos cube metrics from Trino/Iceberg.

Runs 9 queries concurrently and returns an EmrReport with one EmrSection per query.
Canvas is skipped when collect_emr_metrics() returns an empty EmrReport.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from .queries import load_dp
from .trino_client import execute_query

log = logging.getLogger(__name__)

# ── SQL ────────────────────────────────────────────────────────────────────────
_Q_CUBE_HEALTH       = load_dp("emr_cube_health")
_Q_STALENESS         = load_dp("emr_staleness")
_Q_MEMORY_TOP10      = load_dp("emr_memory_top10")
_Q_CPU               = load_dp("emr_cpu")
_Q_SCHEDULE_DELAY    = load_dp("emr_schedule_delay")
_Q_LATEST_STALENESS  = load_dp("emr_latest_staleness")
_Q_ROW_GROWTH        = load_dp("emr_row_growth")
_Q_EXECUTION_TIME    = load_dp("emr_execution_time")


# ── data model ─────────────────────────────────────────────────────────────────

@dataclass
class EmrRow:
    cells: list[str]
    flagged: bool = False


@dataclass
class EmrSection:
    title: str
    headers: list[str]
    rows: list[EmrRow]
    failed: bool = False
    flag_count: int = 0

    def __post_init__(self):
        self.flag_count = sum(1 for r in self.rows if r.flagged)


@dataclass
class EmrReport:
    sections: list[EmrSection] = field(default_factory=list)

    @property
    def total_flags(self) -> int:
        return sum(s.flag_count for s in self.sections)


# ── helpers ────────────────────────────────────────────────────────────────────

def _ts(v) -> str:
    if v is None:
        return "—"
    if isinstance(v, datetime):
        return v.strftime("%b %d %H:%M")
    return str(v)[:16]


def _f(v, decimals: int = 2) -> str:
    if v is None:
        return "—"
    try:
        return f"{float(v):.{decimals}f}"
    except (TypeError, ValueError):
        return str(v)


def _n(v) -> str:
    if v is None:
        return "—"
    try:
        return f"{int(v):,}"
    except (TypeError, ValueError):
        return str(v)


async def _fetch(name: str, query: str) -> tuple[str, list[dict[str, Any]]]:
    try:
        rows = await execute_query(query)
        return name, rows
    except Exception as exc:
        log.warning("EMR query failed [%s]: %s", name, exc)
        return name, []


# ── per-query section builders ─────────────────────────────────────────────────

def _build_cube_health(rows: list[dict]) -> EmrSection:
    headers = ["Cube", "Type", "Last Run", "Age (h)", "Recency", "Breach", "Total Rows"]
    emr_rows = []
    for r in rows:
        breach = bool(r.get("recency_breach"))
        emr_rows.append(EmrRow(
            cells=[
                str(r.get("cube_name", "—")),
                str(r.get("ingestion_type", "—")),
                _ts(r.get("last_run_time")),
                _f(r.get("data_age_hrs"), 1),
                _ts(r.get("data_recency")),
                "🔴 YES" if breach else "🟢 no",
                _n(r.get("total_rows")),
            ],
            flagged=breach,
        ))
    return EmrSection(title="Cube Health Overview", headers=headers, rows=emr_rows)


def _build_staleness(rows: list[dict]) -> EmrSection:
    headers = ["Cube", "Last Run", "Data Recency", "Staleness (h)"]
    emr_rows = []
    for r in rows:
        hrs = r.get("staleness_hrs")
        flagged = hrs is not None and float(hrs) > 24
        emr_rows.append(EmrRow(
            cells=[
                str(r.get("cube_name", "—")),
                _ts(r.get("last_run_time")),
                _ts(r.get("data_recency")),
                _f(hrs, 1),
            ],
            flagged=flagged,
        ))
    return EmrSection(title="Staleness (ordered by staleness_hrs)", headers=headers, rows=emr_rows)


def _build_memory_top10(rows: list[dict]) -> EmrSection:
    headers = ["Cube", "P50 Mem (GB)", "P95 Mem (GB)", "P95 Peak Heap (GB)"]
    emr_rows = []
    for r in rows:
        p95 = r.get("p95_memory_used_gb")
        flagged = p95 is not None and float(p95) > 8.0
        emr_rows.append(EmrRow(
            cells=[
                str(r.get("cube_name", "—")),
                _f(r.get("p50_memory_used_gb"), 3),
                _f(r.get("p95_memory_used_gb"), 3),
                _f(r.get("p95_peak_heap_gb"), 3),
            ],
            flagged=flagged,
        ))
    return EmrSection(title="Memory Usage — Top 10", headers=headers, rows=emr_rows)



def _build_cpu(rows: list[dict]) -> EmrSection:
    headers = ["Cube", "P50 CPU %", "P95 CPU %"]
    emr_rows = []
    for r in rows:
        p50 = r.get("p50_cpu_utilization_pct")
        flagged = p50 is not None and float(p50) < 10.0
        emr_rows.append(EmrRow(
            cells=[
                str(r.get("cube_name", "—")),
                _f(p50, 1),
                _f(r.get("p95_cpu_utilization_pct"), 1),
            ],
            flagged=flagged,
        ))
    return EmrSection(title="CPU Utilisation (ordered ASC — low utilisation first)", headers=headers, rows=emr_rows)


def _build_schedule_delay(rows: list[dict]) -> EmrSection:
    headers = ["Cube", "P50 Delay (h)", "P95 Delay (h)"]
    emr_rows = []
    for r in rows:
        p95 = r.get("p95_schedule_delay_hrs")
        flagged = p95 is not None and float(p95) > 1.0
        emr_rows.append(EmrRow(
            cells=[
                str(r.get("cube_name", "—")),
                _f(r.get("p50_schedule_delay_hrs"), 4),
                _f(p95, 4),
            ],
            flagged=flagged,
        ))
    return EmrSection(title="Schedule Delay (active cubes, ordered DESC)", headers=headers, rows=emr_rows)


def _build_latest_staleness(rows: list[dict]) -> EmrSection:
    headers = ["Cube", "Type", "Last Run", "Recency", "Breach", "Staleness (h)"]
    emr_rows = []
    for r in rows:
        breach = bool(r.get("recency_breach"))
        hrs = r.get("staleness_hrs")
        flagged = breach or (hrs is not None and float(hrs) > 24)
        emr_rows.append(EmrRow(
            cells=[
                str(r.get("cube_name", "—")),
                str(r.get("ingestion_type", "—")),
                _ts(r.get("last_run_time")),
                _ts(r.get("data_recency")),
                "🔴 YES" if breach else "🟢 no",
                _f(hrs, 1),
            ],
            flagged=flagged,
        ))
    return EmrSection(title="Latest Staleness with Config (ordered by staleness DESC)", headers=headers, rows=emr_rows)


def _build_row_growth(rows: list[dict]) -> EmrSection:
    headers = ["Cube", "Run Time", "Total Rows", "Prev Rows", "New Rows", "Growth %"]
    emr_rows = []
    for r in rows[:50]:  # cap at 50 rows — can be very large
        new = r.get("new_rows_added")
        pct = r.get("pct_new_rows_added")
        flagged = new is not None and int(new) < 0  # row shrinkage is suspicious
        emr_rows.append(EmrRow(
            cells=[
                str(r.get("cube_name", "—")),
                _ts(r.get("run_time")),
                _n(r.get("total_rows")),
                _n(r.get("prev_total_rows")),
                _n(new),
                _f(pct, 1),
            ],
            flagged=flagged,
        ))
    return EmrSection(title="Row Growth (ordered by new_rows_added DESC, top 50)", headers=headers, rows=emr_rows)


def _build_execution_time(rows: list[dict]) -> EmrSection:
    headers = ["Cube", "P50 Time (h)", "P95 Time (h)"]
    emr_rows = []
    for r in rows:
        p95 = r.get("p95_execution_time_hrs")
        flagged = p95 is not None and float(p95) > 4.0
        emr_rows.append(EmrRow(
            cells=[
                str(r.get("cube_name", "—")),
                _f(r.get("p50_execution_time_hrs"), 4),
                _f(p95, 4),
            ],
            flagged=flagged,
        ))
    return EmrSection(title="Execution Time (active cubes, ordered by P95 DESC)", headers=headers, rows=emr_rows)


# ── main collector ─────────────────────────────────────────────────────────────

async def collect_emr_metrics() -> EmrReport:
    results = await asyncio.gather(
        _fetch("cube_health",      _Q_CUBE_HEALTH),
        _fetch("staleness",        _Q_STALENESS),
        _fetch("memory_top10",     _Q_MEMORY_TOP10),
        _fetch("cpu",              _Q_CPU),
        _fetch("schedule_delay",   _Q_SCHEDULE_DELAY),
        _fetch("latest_staleness", _Q_LATEST_STALENESS),
        _fetch("row_growth",       _Q_ROW_GROWTH),
        _fetch("execution_time",   _Q_EXECUTION_TIME),
    )

    _builders = {
        "cube_health":      _build_cube_health,
        "staleness":        _build_staleness,
        "memory_top10":     _build_memory_top10,
        "cpu":              _build_cpu,
        "schedule_delay":   _build_schedule_delay,
        "latest_staleness": _build_latest_staleness,
        "row_growth":       _build_row_growth,
        "execution_time":   _build_execution_time,
    }

    sections: list[EmrSection] = []
    for name, rows in results:
        builder = _builders[name]
        if rows:
            section = builder(rows)
        else:
            section = EmrSection(title=name.replace("_", " ").title(), headers=[], rows=[], failed=True)
        sections.append(section)

    report = EmrReport(sections=sections)
    log.info("EMR metrics collected: %d sections, %d total flags.", len(sections), report.total_flags)
    return report
