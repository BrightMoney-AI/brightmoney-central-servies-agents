"""
dp_l0_collector.py — Data Platform L0: CDC pipeline health for all iceberg CDC sinks.

Health philosophy — trend & baseline relative, not fixed absolute thresholds
---------------------------------------------------------------------------
A sink is judged against *its own normal* and *which way it is moving right
now*, so a busy sink that always carries a large-but-stable backlog is not
flagged, while a quiet sink that suddenly starts climbing is. Concretely:

  • growth_ratio = offset_lag / max(7d-median baseline, floor)   — how far
    above the sink's own normal it currently sits.
  • rising        = 1h slope (delta[1h]) > noise, falling back to 24h delta —
    is the backlog still climbing right now?
  • draining      = slope is negative — recovering, never flagged.
  • stalled       = heartbeat ≈ 0 while a real backlog exists — consumer dead.

Signals collected per sink (driven by kafka_cdc_sinks in services.json):
  1. Coord Lag    (MaxOffsetLag instant)          — informational only
  2. Offset Lag   (SumOffsetLag instant)          — current depth
  3. Lag Δ 24h    (delta SumOffsetLag [24h])      — coarse trend
  4. Lag Δ 1h     (delta SumOffsetLag [1h])       — current slope
  5. Baseline     (median SumOffsetLag [7d])      — the sink's own normal
  6. Heartbeat    (rate[5m]*300 msg/min)          — < min → stalled consumer

Checks per VM (p-iceberg-sink-.* | p-debezium.*):
  7. Disk %       — > 80 % warning, > 90 % critical

All checks run as concurrent batch queries — no per-sink round-trips.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional

from .vm_client import VMClient

log = logging.getLogger(__name__)

# ── thresholds ─────────────────────────────────────────────────────────────────
# Trend & baseline relative — a sink is judged against its own 7d normal and
# whether it is climbing *right now*, not against fixed absolute depths.
_BASELINE_FLOOR  = 1_000    # floor under the 7d-median so a near-zero baseline can't
                            # make growth_ratio explode on tiny absolute lag.
_BACKLOG_FLOOR   = 1_000    # a "real" backlog — stall detection needs at least this.
_GROWTH_WARN     = 2.0      # offset_lag ≥ 2× its own baseline → warning territory
_GROWTH_CRIT     = 4.0      # offset_lag ≥ 4× its own baseline → critical territory
_SLOPE_NOISE     = 500      # |delta[1h]| below this is flat (sampling jitter)
_LAG_DELTA_NOISE = 100      # |delta[24h]| below this is flat
_HEARTBEAT_MIN   = 5        # total msgs in 5-min window; below this → stalled (normal ~20)
# Absolute backstop — used only when a sink has no usable baseline (new sink,
# gap in 7d history) so growth_ratio can't be computed.
_ABS_WARN        = 25_000
_ABS_CRIT        = 100_000

# Coordinator lag is informational only (never flags); kept for display context.
_COORD_LAG_WARN  = 1_000
_COORD_LAG_CRIT  = 10_000

_DISK_WARN_PCT   = 80.0
_DISK_CRIT_PCT   = 90.0

_VM_PATTERN = "p-iceberg-sink-.*|p-debezium.*"


# ── data classes ───────────────────────────────────────────────────────────────

@dataclass
class SinkHealth:
    sink: str
    debezium: Optional[str]           # Debezium connector name (reference only)
    heartbeat_topic: Optional[str]    # exact Kafka topic — None means no heartbeat configured

    coord_lag: Optional[float] = None            # MaxOffsetLag (instant) — informational
    offset_lag: Optional[float] = None           # SumOffsetLag (instant) — current depth
    offset_lag_delta: Optional[float] = None     # delta([24h]); positive = grew over the day
    offset_lag_delta_1h: Optional[float] = None  # delta([1h]);  current slope
    offset_lag_baseline: Optional[float] = None  # median([7d]); the sink's own normal
    heartbeat_rate: Optional[float] = None        # msg/5m window

    # ── raw trend primitives ────────────────────────────────────────────────────
    @property
    def growth_ratio(self) -> Optional[float]:
        """How far above its own 7d normal the sink currently sits (offset / baseline).

        None when there is no offset reading or no usable baseline — callers then
        fall back to the absolute backstop.
        """
        if self.offset_lag is None or self.offset_lag_baseline is None:
            return None
        base = max(self.offset_lag_baseline, _BASELINE_FLOOR)
        return self.offset_lag / base

    @property
    def rising_24h(self) -> bool:
        """Backlog grew meaningfully over the past 24 h."""
        return self.offset_lag_delta is not None and self.offset_lag_delta > _LAG_DELTA_NOISE

    @property
    def _rising(self) -> bool:
        """Climbing *right now*: prefer the 1h slope, fall back to the 24h delta.

        The 1h slope is the freshest signal; when it is missing we defer to the
        coarser 24h trend so a sink still gets gated on direction.
        """
        if self.offset_lag_delta_1h is not None:
            return self.offset_lag_delta_1h > _SLOPE_NOISE
        return self.rising_24h

    @property
    def draining(self) -> bool:
        """Recovering — slope is clearly negative. A draining sink is never flagged."""
        if self.offset_lag_delta_1h is not None:
            return self.offset_lag_delta_1h < -_SLOPE_NOISE
        return self.offset_lag_delta is not None and self.offset_lag_delta < -_LAG_DELTA_NOISE

    @property
    def stalled(self) -> bool:
        """Consumer looks dead: heartbeat ≈ 0 while a real backlog exists."""
        return (
            self.heartbeat_topic is not None
            and self.heartbeat_rate is not None
            and self.heartbeat_rate < _HEARTBEAT_MIN
            and self.offset_lag is not None
            and self.offset_lag > _BACKLOG_FLOOR
        )

    @property
    def _has_data(self) -> bool:
        return self.offset_lag is not None

    # ── authoritative health ────────────────────────────────────────────────────
    @property
    def status(self) -> str:
        """Single source of truth: 'critical' | 'warning' | 'ok' | 'unknown'.

        Order of judgement:
          1. no offset reading at all            → unknown
          2. stalled consumer with real backlog  → critical
          3. far above own normal AND climbing   → critical / warning
          4. (no baseline) absolute backstop     → critical / warning
          5. otherwise                           → ok
        A draining sink is never escalated regardless of depth.
        """
        if not self._has_data:
            return "unknown"

        if self.stalled:
            return "critical"

        ratio = self.growth_ratio
        if ratio is not None:
            if ratio >= _GROWTH_CRIT and self._rising:
                return "critical"
            if ratio >= _GROWTH_WARN and self.rising_24h and not self.draining:
                return "warning"
            return "ok"

        # No usable baseline — judge on absolute depth, still gated on direction.
        assert self.offset_lag is not None
        if self.offset_lag >= _ABS_CRIT and self._rising:
            return "critical"
        if self.offset_lag >= _ABS_WARN and self.rising_24h and not self.draining:
            return "warning"
        return "ok"

    @property
    def is_flagged(self) -> bool:
        return self.status in ("warning", "critical")

    # ── informational / display helpers (do not drive flagging) ──────────────────
    @property
    def coord_status(self) -> str:
        """Coordinator lag band — informational only, never flags a sink."""
        if self.coord_lag is None:
            return "unknown"
        if self.coord_lag >= _COORD_LAG_CRIT:
            return "critical"
        if self.coord_lag >= _COORD_LAG_WARN:
            return "warning"
        return "ok"

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
    def lag_increasing(self) -> bool:
        """Backward-compat alias used by the detailed renderer: rising and above normal."""
        return self.status in ("warning", "critical") and (self._rising or self.rising_24h)


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
    # delta([1h]) = the current slope; the freshest "is it climbing now?" signal
    offset_delta_1h_q = (
        'delta(kafka_consumer_group_ConsumerLagMetrics_Value'
        '{groupId=~"cg-control-.*", name="SumOffsetLag"}[1h])'
    )
    # median over 7d = the sink's own normal, robust to spikes
    offset_baseline_q = (
        'quantile_over_time(0.5, kafka_consumer_group_ConsumerLagMetrics_Value'
        '{groupId=~"cg-control-.*", name="SumOffsetLag"}[7d])'
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

    _sem = asyncio.Semaphore(4)

    async def _lim(coro):
        async with _sem:
            return await coro

    (
        coord_raw, offset_raw, delta_raw, delta_1h_raw,
        baseline_raw, heartbeat_raw, disk_raw,
    ) = await asyncio.gather(
        _lim(_safe_vec(vm, coord_lag_q,        "groupId")),
        _lim(_safe_vec(vm, offset_lag_q,       "groupId")),
        _lim(_safe_vec(vm, offset_delta_q,     "groupId")),
        _lim(_safe_vec(vm, offset_delta_1h_q,  "groupId")),
        _lim(_safe_vec(vm, offset_baseline_q,  "groupId")),
        _lim(_safe_vec(vm, heartbeat_q,        "topic")),
        _lim(_safe_vec(vm, disk_q,             "name")),
    )

    coord_index    = _index_coord(coord_raw,     known_sinks)
    offset_index   = _index_offset(offset_raw,   known_sinks)
    delta_index    = _index_offset(delta_raw,    known_sinks)
    delta_1h_index = _index_offset(delta_1h_raw, known_sinks)
    baseline_index = _index_offset(baseline_raw, known_sinks)
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
            offset_lag_delta_1h=delta_1h_index.get(sink_name),
            offset_lag_baseline=baseline_index.get(sink_name),
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
            offset_lag_delta_1h=delta_1h_index.get(sink_name),
            offset_lag_baseline=baseline_index.get(sink_name),
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
    s = group_id[len("cg-control-"):] if group_id.startswith("cg-control-") else group_id
    return s[:-6] if s.endswith("-coord") else s


def _extract_offset_sink(group_id: str) -> str:
    """cg-control-{sink}  →  {sink}"""
    return group_id[len("cg-control-"):] if group_id.startswith("cg-control-") else group_id


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
