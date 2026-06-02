from dataclasses import dataclass, field
from datetime import datetime
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
    throughput_rps:     float
    success_rate_pct:   float
    error_rate_pct:     float
    avg_latency_p50_ms: int


@dataclass
class Endpoint:
    path:        str
    hits:        int
    success_pct: float
    errors:      Optional[int]  # None = no data (N/A)
    p99_ms:      float


@dataclass
class L0Report:
    service:              str
    reported_at:          datetime
    status:               Status
    system:               SystemHealth
    api:                  ApiMetrics
    endpoints:            list[Endpoint]
    thresholds:           FlaggingThresholds = field(default_factory=FlaggingThresholds)
    total_endpoint_count: int = 0  # total including any not in the list
