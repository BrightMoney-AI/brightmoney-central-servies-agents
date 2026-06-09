from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Optional


class Status(str, Enum):
    HEALTHY  = "healthy"
    WARNING  = "warning"
    CRITICAL = "critical"
    UNKNOWN  = "unknown"


@dataclass
class FlaggingThresholds:
    metric_warn_pct:  float = 40.0   # CPU/MEM/Disk warning floor
    metric_crit_pct:  float = 60.0   # CPU/MEM/Disk critical floor
    p99_warn_ms:      float = 1000.0
    p99_crit_ms:      float = 3000.0
    success_warn_pct: float = 99.0
    top_n_unflagged:  int   = 5


@dataclass
class ServerMetrics:
    cpu_pct:  float
    mem_pct:  float
    disk_pct: float


@dataclass
class Server:
    name:    str
    group:   str
    metrics: ServerMetrics
    status:  Status = Status.HEALTHY


@dataclass
class SystemHealth:
    servers: list[Server]

    @property
    def online(self) -> int:
        return sum(1 for s in self.servers if s.status != Status.UNKNOWN)

    @property
    def down(self) -> int:
        return sum(1 for s in self.servers if s.status == Status.UNKNOWN)


@dataclass
class ApiMetrics:
    throughput_rps:              float
    success_rate_pct:            float
    error_rate_pct:              float
    avg_latency_p50_ms:          int
    avg_latency_baseline_ms:     Optional[float] = None  # 7-day baseline
    success_rate_baseline_pct:   Optional[float] = None  # 7-day baseline
    error_rate_baseline_pct:     Optional[float] = None  # 7-day baseline


@dataclass
class Endpoint:
    path:                str
    hits:                int
    success_pct:         float
    errors:              Optional[int]   # None = no data (N/A)
    p99_ms:              float
    p99_baseline_ms:     Optional[float] = None  # 7-day baseline
    success_baseline_pct: Optional[float] = None  # 7-day baseline


@dataclass
class QueueDepth:
    name:    str
    ready:   int
    unacked: int
    total:   int


@dataclass
class QueueHealth:
    queues: list[QueueDepth]


@dataclass
class ConnectorTask:
    id: int
    state: str  # RUNNING, FAILED, UNASSIGNED, PAUSED


@dataclass
class ConnectorStatus:
    name: str
    state: str  # RUNNING, FAILED, PAUSED, UNASSIGNED, RESTARTING, STOPPED
    tasks: list[ConnectorTask]

    @property
    def is_healthy(self) -> bool:
        return self.state == "RUNNING" and all(t.state == "RUNNING" for t in self.tasks)


@dataclass
class KafkaConnectInstance:
    name: str                       # display name, e.g. "Kafka Sink"
    total: int                      # total connector count
    unhealthy: list[ConnectorStatus]


@dataclass
class KafkaConnectHealth:
    instances: list[KafkaConnectInstance]


@dataclass
class AirflowDagRun:
    dag_id: str
    state: str              # success, failed, running, queued, up_for_retry
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    run_date: Optional[date] = None   # IST calendar date of the run

    @property
    def is_healthy(self) -> bool:
        return self.state == "success"


@dataclass
class ViewFlowRun:
    table_name: str
    state: str
    start_date: Optional[datetime]


@dataclass
class ViewFlowHealth:
    total: int
    successful: int
    failed: list[ViewFlowRun]
    running: list[ViewFlowRun]


@dataclass
class AirflowHealth:
    dag_runs: list[AirflowDagRun]
    view_flow: Optional[ViewFlowHealth] = None
    pipeline_runs: list[AirflowDagRun] = field(default_factory=list)  # today + yesterday per pipeline DAG


@dataclass
class L0Report:
    service:              str
    reported_at:          datetime
    status:               Status
    system:               SystemHealth
    api:                  ApiMetrics
    endpoints:            list[Endpoint]
    thresholds:           FlaggingThresholds = field(default_factory=FlaggingThresholds)
    total_endpoint_count: int = 0
    queues:               Optional[QueueHealth] = None
    show_api_metrics:     bool = True
