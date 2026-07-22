"""
uks_collector.py — UKS KYC Service real-time metrics from VictoriaMetrics.

Queries custom UKS Prometheus metrics:
  uks_kyc_flow        — overall KYC pass/fail counts
  uks_task            — celery task started/completed counts
  uks_task_duration_ms_bucket — histogram for task latency
  uks_api_incoming    — incoming HTTP request counts by view + status
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional

from .vm_client import VMClient

log = logging.getLogger(__name__)


@dataclass
class UKSTaskMetric:
    name: str
    success_rate: Optional[float] = None   # %
    p99_ms: Optional[float]       = None   # ms


@dataclass
class UKSApiMetric:
    view: str
    success_rate: Optional[float] = None   # % 2xx / total
    req_per_min: Optional[float]  = None


@dataclass
class UKSMetrics:
    kyc_pass_rate: Optional[float]     = None   # %
    kyc_fail_rate: Optional[float]     = None   # %
    kyc_per_min: Optional[float]       = None   # total flows/min
    tasks: list[UKSTaskMetric]         = field(default_factory=list)
    api_views: list[UKSApiMetric]      = field(default_factory=list)

    @property
    def kyc_flagged(self) -> bool:
        return self.kyc_pass_rate is not None and self.kyc_pass_rate < 90.0

    @property
    def any_task_flagged(self) -> bool:
        return any(t.success_rate is not None and t.success_rate < 95.0 for t in self.tasks)


async def collect_uks_metrics(vm: VMClient) -> UKSMetrics:
    """Batch-collect all UKS KYC metrics in one concurrent gather."""
    kyc_pass_q = (
        '100 * sum(rate(uks_kyc_flow{status="passed"}[5m]))'
        ' / sum(rate(uks_kyc_flow{status=~"passed|failed"}[5m]))'
    )
    kyc_total_q = 'sum(rate(uks_kyc_flow{status=~"passed|failed"}[5m])) * 60'

    task_success_q = (
        '100 * sum by (task)(rate(uks_task{event="completed"}[5m]))'
        ' / sum by (task)(rate(uks_task{event="started"}[5m]))'
    )
    task_p99_q = (
        'histogram_quantile(0.99, sum by (task, le)'
        '(rate(uks_task_duration_ms_bucket{event="completed"}[5m])))'
    )
    api_success_q = (
        '100 * sum by (view)(rate(uks_api_incoming{status=~"2.."}[5m]))'
        ' / sum by (view)(rate(uks_api_incoming[5m]))'
    )
    api_rpm_q = 'sum by (view)(rate(uks_api_incoming[5m])) * 60'

    (
        kyc_pass_val,
        kyc_total_val,
        task_success_raw,
        task_p99_raw,
        api_success_raw,
        api_rpm_raw,
    ) = await asyncio.gather(
        _safe_scalar(vm, kyc_pass_q),
        _safe_scalar(vm, kyc_total_q),
        _safe_vec(vm, task_success_q, "task"),
        _safe_vec(vm, task_p99_q, "task"),
        _safe_vec(vm, api_success_q, "view"),
        _safe_vec(vm, api_rpm_q, "view"),
    )

    # Build task metrics
    task_success = dict(task_success_raw)
    task_p99     = dict(task_p99_raw)
    all_tasks    = sorted(set(task_success) | set(task_p99))
    tasks = [
        UKSTaskMetric(
            name=t,
            success_rate=task_success.get(t),
            p99_ms=task_p99.get(t),
        )
        for t in all_tasks
    ]

    # Build API view metrics
    api_success = dict(api_success_raw)
    api_rpm     = dict(api_rpm_raw)
    all_views   = sorted(set(api_success) | set(api_rpm))
    api_views = [
        UKSApiMetric(
            view=v,
            success_rate=api_success.get(v),
            req_per_min=api_rpm.get(v),
        )
        for v in all_views
    ]

    kyc_fail_rate: Optional[float] = None
    if kyc_pass_val is not None:
        kyc_fail_rate = 100.0 - kyc_pass_val

    log.info(
        "UKS collected: kyc_pass=%.1f%%  tasks=%d  api_views=%d",
        kyc_pass_val or 0.0,
        len(tasks),
        len(api_views),
    )
    return UKSMetrics(
        kyc_pass_rate=kyc_pass_val,
        kyc_fail_rate=kyc_fail_rate,
        kyc_per_min=kyc_total_val,
        tasks=tasks,
        api_views=api_views,
    )


async def _safe_scalar(vm: VMClient, promql: str) -> Optional[float]:
    try:
        return await vm.query(promql)
    except Exception as exc:
        log.warning("UKS scalar query failed: %s", exc)
        return None


async def _safe_vec(vm: VMClient, promql: str, label: str) -> list[tuple[str, float]]:
    try:
        return await vm.query_vector(promql, id_label=label)
    except Exception as exc:
        log.warning("UKS vector query failed [label=%s]: %s", label, exc)
        return []
