"""
dp_l0_collector.py — Data Platform L0: CDC pipeline health for all iceberg CDC sinks.

Checks per sink (driven by kafka_cdc_sinks in services.json):
  1. Coord Lag    (MaxOffsetLag instant)     — high absolute value
  2. Offset Lag   (SumOffsetLag instant)     — current depth
  3. Lag Trend    (delta SumOffsetLag [24h]) — positive = still growing over 24 h
  4. Heartbeat    (rate[5m]*300 msg/min)     — < 50 → stalled connector

Checks per VM (p-iceberg-sink-.* | p-debezium.*):
  5. Disk %       — > 80 % warning, > 90 % critical

All 5 checks run as concurrent batch queries — no per-sink round-trips.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional

from .vm_client import VMClient

log = logging.getLogger(__name__)

# ── thresholds ─────────────────────────────────────────────────────────────────
_COORD_LAG_WARN  = 1_000
_COORD_LAG_CRIT  = 10_000
_LAG_DELTA_NOISE = 100      # ignore tiny positive deltas (sampling jitter)
_LAG_DELTA_CRIT  = 5_000    # grew by this much over 24 h → critical
_HEARTBEAT_MIN   = 5        # total msgs in 5-min window; below this → stalled (normal ~20)
_DISK_WARN_PCT   = 80.0
_DISK_CRIT_PCT   = 90.0

_VM_PATTERN = "p-iceberg-sink-.*|p-debezium.*"


# ── data classes ───────────────────────────────────────────────────────────────

@dataclass
class SinkHealth:
    sink: str
    debezium: Optional[str]           # Debezium connector name (reference only)
    heartbeat_topic: Optional[str]    # exact Kafka topic — None means no heartbeat configured

    coord_lag: Optional[float] = None         # MaxOffsetLag (instant)
    offset_lag: Optional[float] = None        # SumOffsetLag (instant)
    offset_lag_delta: Optional[float] = None  # delta([24h]); positive = still growing
    heartbeat_rate: Optional[float] = None    # msg/5m window

    @property
    def coord_status(self) -> str:
        if self.coord_lag is None:
            return "unknown"
        if self.coord_lag >= _COORD_LAG_CRIT:
            return "critical"
        if self.coord_lag >= _COORD_LAG_WARN:
            return "warning"
        return "ok"

    @property
    def lag_increasing(self) -> bool:
        """True when lag has grown meaningfully over the past 24 h and is still above noise."""
        return (
            self.offset_lag_delta is not None
            and self.offset_lag_delta > _LAG_DELTA_NOISE
            and self.offset_lag is not None
            and self.offset_lag > _COORD_LAG_WARN
        )

    @property
    def lag_delta_status(self) -> str:
        if not self.lag_increasing:
            return "ok"
        if self.offset_lag_delta is not None and self.offset_lag_delta >= _LAG_DELTA_CRIT:
            return "critical"
        return "warning"

    @property
    def heartbeat_status(self) -> str:
        if self.heartbeat_topic is None:
            return "no_connector"
        if self.heartbeat_rate is None:
            return "unknown"
        if self.heartbeat_rate < _HEARTBEAT_MIN:
            return "critical"
        return "ok"

    @property
    def is_flagged(self) -> bool:
        return (
            self.coord_status in ("warning", "critical")
            or self.lag_increasing
            or self.heartbeat_status in ("critical", "unknown")
        )


@dataclass
class VMDiskHealth:
    vm_name: str
    disk_pct: float

    @property
    def status(self) -> str:
        if self.disk_pct >= _DISK_CRIT_PCT:
            return "critical"
        if self.disk_pct >= _DISK_WARN_PCT:
            return "warning"
        return "ok"

    @property
    def is_flagged(self) -> bool:
        return self.status != "ok"


@dataclass
class DPL0Report:
    sinks: list[SinkHealth] = field(default_factory=list)           # CDC sinks
    kafka_sinks: list[SinkHealth] = field(default_factory=list)     # plain Kafka sinks
    vm_disks: list[VMDiskHealth] = field(default_factory=list)

    @property
    def flagged_sinks(self) -> list[SinkHealth]:
        return [s for s in self.sinks if s.is_flagged]

    @property
    def flagged_kafka_sinks(self) -> list[SinkHealth]:
        return [s for s in self.kafka_sinks if s.is_flagged]

    @property
    def flagged_vms(self) -> list[VMDiskHealth]:
        return [v for v in self.vm_disks if v.is_flagged]


# ── main collector ─────────────────────────────────────────────────────────────

async def collect_dp_l0(
    vm: VMClient,
    cdc_sinks: list[dict],
    kafka_sinks: list[str] | None = None,
) -> DPL0Report:
    """Batch-collect CDC + Kafka sink pipeline health in 5 concurrent PromQL queries."""
    sink_debezium = {s["sink"]: s.get("debezium")        for s in cdc_sinks}
    sink_hb_topic = {s["sink"]: s.get("heartbeat_topic") for s in cdc_sinks}
    kafka_sink_names = set(kafka_sinks or [])
    known_sinks   = set(sink_debezium.keys()) | kafka_sink_names

    coord_lag_q = (
        'kafka_consumer_group_ConsumerLagMetrics_Value'
        '{groupId=~"cg-control-.*-coord", name="MaxOffsetLag"}'
    )
    offset_lag_q = (
        'kafka_consumer_group_ConsumerLagMetrics_Value'
        '{groupId=~"cg-control-.*", name="SumOffsetLag"}'
    )
    # delta([24h]) on a gauge = current value − value 24 h ago; positive → lag grew
    offset_delta_q = (
        'delta(kafka_consumer_group_ConsumerLagMetrics_Value'
        '{groupId=~"cg-control-.*", name="SumOffsetLag"}[24h])'
    )
    # Heartbeat topics are CDC event topics: cdc_xxx.schema.debezium_*heartbeat*
    # Match broadly — exact lookup is done in Python against heartbeat_topic field.
    heartbeat_q = (
        'rate(kafka_server_BrokerTopicMetrics_Count'
        '{name="MessagesInPerSec", topic=~"cdc_.*debezium.*"}[5m]) * 300'
    )
    disk_q = (
        f'(node_filesystem_size_bytes{{mountpoint="/", device!~"rootfs",'
        f' name=~"{_VM_PATTERN}", job="system_metrics"}}'
        f' - node_filesystem_free_bytes{{mountpoint="/", device!~"rootfs",'
        f' name=~"{_VM_PATTERN}", job="system_metrics"}})'
        f' / node_filesystem_size_bytes{{mountpoint="/", device!~"rootfs",'
        f' name=~"{_VM_PATTERN}", job="system_metrics"}} * 100'
    )

    coord_raw, offset_raw, delta_raw, heartbeat_raw, disk_raw = await asyncio.gather(
        _safe_vec(vm, coord_lag_q,    "groupId"),
        _safe_vec(vm, offset_lag_q,   "groupId"),
        _safe_vec(vm, offset_delta_q, "groupId"),
        _safe_vec(vm, heartbeat_q,    "topic"),
        _safe_vec(vm, disk_q,         "name"),
    )

    coord_index  = _index_coord(coord_raw,  known_sinks)
    offset_index = _index_offset(offset_raw, known_sinks)
    delta_index  = _index_offset(delta_raw,  known_sinks)
    # Index heartbeat by exact topic name — no fuzzy matching needed
    hb_index: dict[str, float] = dict(heartbeat_raw)

    cdc_sink_health: list[SinkHealth] = []
    for sink_name in sorted(sink_debezium):
        hb_topic = sink_hb_topic.get(sink_name)
        cdc_sink_health.append(SinkHealth(
            sink=sink_name,
            debezium=sink_debezium[sink_name],
            heartbeat_topic=hb_topic,
            coord_lag=coord_index.get(sink_name),
            offset_lag=offset_index.get(sink_name),
            offset_lag_delta=delta_index.get(sink_name),
            heartbeat_rate=hb_index.get(hb_topic) if hb_topic else None,
        ))

    kafka_sink_health: list[SinkHealth] = []
    for sink_name in sorted(kafka_sink_names):
        kafka_sink_health.append(SinkHealth(
            sink=sink_name,
            debezium=None,
            heartbeat_topic=None,
            coord_lag=coord_index.get(sink_name),
            offset_lag=offset_index.get(sink_name),
            offset_lag_delta=delta_index.get(sink_name),
            heartbeat_rate=None,
        ))

    vm_disks = [VMDiskHealth(vm_name=name, disk_pct=pct) for name, pct in disk_raw]
    log.info(
        "DP L0 collected: %d CDC sinks (%d flagged), %d Kafka sinks (%d flagged), %d VMs (%d flagged).",
        len(cdc_sink_health), sum(1 for s in cdc_sink_health if s.is_flagged),
        len(kafka_sink_health), sum(1 for s in kafka_sink_health if s.is_flagged),
        len(vm_disks), sum(1 for v in vm_disks if v.is_flagged),
    )
    return DPL0Report(sinks=cdc_sink_health, kafka_sinks=kafka_sink_health, vm_disks=vm_disks)


# ── label-to-sink-name helpers ─────────────────────────────────────────────────

def _extract_coord_sink(group_id: str) -> str:
    """cg-control-{sink}-coord  →  {sink}"""
    s = group_id.removeprefix("cg-control-")
    return s[:-6] if s.endswith("-coord") else s


def _extract_offset_sink(group_id: str) -> str:
    """cg-control-{sink}  →  {sink}"""
    return group_id.removeprefix("cg-control-")


def _index_coord(raw: list[tuple[str, float]], known: set[str]) -> dict[str, float]:
    out: dict[str, float] = {}
    for group_id, val in raw:
        sink = _extract_coord_sink(group_id)
        if sink in known:
            out[sink] = val
    return out


def _index_offset(raw: list[tuple[str, float]], known: set[str]) -> dict[str, float]:
    """For offset lag (and delta), take the max across any groups that map to the same sink."""
    out: dict[str, float] = {}
    for group_id, val in raw:
        sink = _extract_offset_sink(group_id)
        if sink in known and (sink not in out or val > out[sink]):
            out[sink] = val
    return out




async def _safe_vec(vm: VMClient, promql: str, id_label: str) -> list[tuple[str, float]]:
    try:
        return await vm.query_vector(promql, id_label=id_label)
    except Exception as exc:
        log.warning("DP L0 batch query failed [id_label=%s]: %s", id_label, exc)
        return []
