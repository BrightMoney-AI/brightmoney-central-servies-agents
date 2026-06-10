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
import re
from dataclasses import dataclass, field
from typing import Optional

# Debezium connector names in services.json may carry version suffixes (_v1, _v2, …)
# that are absent from the heartbeat topic name.  Strip them as a fallback.
_VERSION_SUFFIX_RE = re.compile(r"_v\d+$")

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
    debezium: Optional[str]             # None → no active Debezium connector

    coord_lag: Optional[float] = None         # MaxOffsetLag (instant)
    offset_lag: Optional[float] = None        # SumOffsetLag (instant)
    offset_lag_delta: Optional[float] = None  # delta([24h]); positive = still growing
    heartbeat_rate: Optional[float] = None    # msg/min

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
        if self.debezium is None:
            return "no_connector"
        if self.heartbeat_rate is None:
            return "unknown"
        if self.heartbeat_rate < _HEARTBEAT_MIN:
            return "critical"
        return "ok"

    @property
    def is_flagged(self) -> bool:
        return self.coord_status in ("warning", "critical") or self.lag_increasing


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
    sinks: list[SinkHealth] = field(default_factory=list)
    vm_disks: list[VMDiskHealth] = field(default_factory=list)

    @property
    def flagged_sinks(self) -> list[SinkHealth]:
        return [s for s in self.sinks if s.is_flagged]

    @property
    def flagged_vms(self) -> list[VMDiskHealth]:
        return [v for v in self.vm_disks if v.is_flagged]


# ── main collector ─────────────────────────────────────────────────────────────

async def collect_dp_l0(vm: VMClient, cdc_sinks: list[dict]) -> DPL0Report:
    """Batch-collect CDC pipeline health in 5 concurrent PromQL queries."""
    sink_debezium = {s["sink"]: s.get("debezium") for s in cdc_sinks}
    known_sinks   = set(sink_debezium.keys())

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
    # topic format: "__debezium-heartbeat.{connector}.*.*"  (multi-segment, unescaped dot)
    heartbeat_q = (
        'rate(kafka_server_BrokerTopicMetrics_Count'
        '{name="MessagesInPerSec", topic=~"__debezium-heartbeat.*.*.*"}[5m]) * 300'
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
    hb_index     = _index_heartbeat(heartbeat_raw)

    sinks: list[SinkHealth] = []
    for sink_name in sorted(sink_debezium):
        debezium = sink_debezium[sink_name]
        sinks.append(SinkHealth(
            sink=sink_name,
            debezium=debezium,
            coord_lag=coord_index.get(sink_name),
            offset_lag=offset_index.get(sink_name),
            offset_lag_delta=delta_index.get(sink_name),
            heartbeat_rate=_lookup_heartbeat(hb_index, debezium) if debezium else None,
        ))

    vm_disks = [VMDiskHealth(vm_name=name, disk_pct=pct) for name, pct in disk_raw]
    log.info(
        "DP L0 collected: %d sinks (%d flagged), %d VMs (%d flagged).",
        len(sinks), sum(1 for s in sinks if s.is_flagged),
        len(vm_disks), sum(1 for v in vm_disks if v.is_flagged),
    )
    return DPL0Report(sinks=sinks, vm_disks=vm_disks)


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


def _index_heartbeat(raw: list[tuple[str, float]]) -> dict[str, float]:
    """topic=__debezium-heartbeat.{connector}.*.*  →  {connector: summed_rate}

    Multiple sub-topics for the same connector (e.g. different partitions) are summed.
    Connector name is the second dot-separated segment.
    """
    out: dict[str, float] = {}
    for topic, val in raw:
        parts = topic.split(".")
        if len(parts) < 2:
            continue
        connector = parts[1]   # segment after "__debezium-heartbeat"
        out[connector] = out.get(connector, 0.0) + val
    return out


def _lookup_heartbeat(hb_index: dict[str, float], debezium: str) -> Optional[float]:
    """Look up heartbeat rate with three-tier fallback.

    Connector names in services.json can differ from the heartbeat topic name
    in two ways:
      1. Version suffix:  cdc_asset_manager_prod_v1  →  topic cdc_asset_manager_prod
      2. Abbreviated name: cdc_uaa_be_01_v2           →  topic cdc_uaa_be

    Strategy:
      1. Exact match
      2. Strip trailing _vN suffix, exact match
      3. Longest-prefix match — find the topic key K where debezium.startswith(K + "_"),
         picking the longest K to avoid false positives
    """
    # 1. Exact
    if debezium in hb_index:
        return hb_index[debezium]

    # 2. Version suffix stripped (cdc_xxx_v1 → cdc_xxx)
    stripped = _VERSION_SUFFIX_RE.sub("", debezium)
    if stripped != debezium and stripped in hb_index:
        return hb_index[stripped]

    # 3. Longest prefix match (cdc_uaa_be_01_v2 → cdc_uaa_be)
    best_key: Optional[str] = None
    best_len = 0
    for key in hb_index:
        if debezium.startswith(key + "_") and len(key) > best_len:
            best_key, best_len = key, len(key)
    if best_key is not None:
        return hb_index[best_key]

    return None


async def _safe_vec(vm: VMClient, promql: str, id_label: str) -> list[tuple[str, float]]:
    try:
        return await vm.query_vector(promql, id_label=id_label)
    except Exception as exc:
        log.warning("DP L0 batch query failed [id_label=%s]: %s", id_label, exc)
        return []
