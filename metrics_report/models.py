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
    avg_latency_p50_ms:          int             # 24h avg — used for spike detection
    avg_latency_baseline_ms:     Optional[float] = None  # 7-day baseline
    avg_latency_current_ms:      Optional[int]   = None  # 1h window — shows live state
    # (start_ist, end_ist, peak_ms) of the anomalous window within the last 24h, or None
    latency_spike_window:        Optional[tuple[str, str, float]] = None
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
class WebhookPipelineMetrics:
    """Size-check + MSK publisher pipeline health (Webhooks/Pipeline namespace)."""
    eb_success_pct:  Optional[float]   # EB publish success % — 24h avg
    msk_success_pct: Optional[float]   # MSK publish success % — 24h avg
    api_dest_pct:    Optional[float]   # EventBridge API-dest delivery % — 24h avg
    overflow_count:  int               # S3 overflow events in 24h
    invalid_count:   int               # MSK unparseable events in 24h


@dataclass
class WebhookInfraMetrics:
    """API Gateway + E2E latency metrics."""
    apigw_5xx_pct:        Optional[float]  # 24h avg 5XX error rate
    apigw_4xx_pct:        Optional[float]  # 24h avg 4XX error rate
    apigw_throughput:     int              # total requests in 24h
    e2e_latency_p99_ms:   Optional[float]  # max E2E p99 latency ms (24h)
    apigw_latency_p99_ms: Optional[float]  # max API GW p99 latency ms (24h)


@dataclass
class WebhookDlqMetrics:
    depth_now:    int            # current visible messages
    age_oldest_s: Optional[int]  # age of oldest message in seconds


@dataclass
class WebhookWafMetrics:
    block_pct:     Optional[float]  # 24h block %
    blocked_total: int              # total blocked requests in 24h
    rules:         dict             # {rule_name: int blocked_count}


@dataclass
class WebhookSlugMetrics:
    slug:            str
    eb_success_pct:  Optional[float]  # 24h avg %
    msk_success_pct: Optional[float]  # 24h avg %


@dataclass
class WebhookLambdaMetrics:
    name:         str             # e.g. "webhook-size-check-prod"
    errors:       int             # total 24h
    throttles:    int             # total 24h
    duration_p99: Optional[float] # avg p99 ms (24h)
    invocations:  int             # total 24h


@dataclass
class WebhookGatewayReport:
    pipeline:    WebhookPipelineMetrics
    infra:       WebhookInfraMetrics
    dlq:         WebhookDlqMetrics
    waf:         WebhookWafMetrics
    slugs:       list[WebhookSlugMetrics]
    lambdas:     list[WebhookLambdaMetrics]
    reported_at: datetime
    status:      Status = Status.UNKNOWN
    failures:    list[str] = field(default_factory=list)


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
