"""
uaa_kafka_collector.py — Transaction Insight Kafka emission metrics.

Pulls all TI Kafka metrics from VictoriaMetrics using the exact queries
from the Transactions Insights Grafana dashboard (uid: oBYJfM5nk).

Consumed by hl_canvas_renderer:
  • L0 — only rendered when one or more metrics are flagged (failures > 0,
         lag above threshold, success rate degraded).
  • L1 — always rendered in full (full producer/consumer/lag/broker tables).
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional

from .config import settings
from .vm_client import VMClient

log = logging.getLogger(__name__)

# ── Consumer groups tracked in the dashboard ──────────────────────────────────
_CONSUMER_GROUPS = [
    "consumer_group",
    "horizon-enrichment-consumer",
    "enriched_transaction_ingestion_consumer_group",
]

# Short display names for the three consumer groups (same order as above)
_GROUP_LABELS = [
    "consumer_group",
    "horizon-enrichment",
    "enriched-ingestion",
]

# ── Broker topics to track ────────────────────────────────────────────────────
_BROKER_TOPICS = [
    "HORIZON_TO_UAA_ENRICHED_RESPONSE",
    "UAA_TO_HORIZON_ENRICH_REQUEST",
    "ENRICHED_TRANSACTION_INGESTION",
]


@dataclass
class TIKafkaMetrics:
    # ── Producer ──────────────────────────────────────────────────────────────
    producer_success_rate: Optional[float]  # 0–100 %; None = no data
    pub_fail_per_min:       float           # publishing failures/min  (rate × 60)
    producer_ack_fail_1h:   float           # increase in ack failures over 1h
    publishing_fail_1h:     float           # increase in publishing failures over 1h
    enrichment_err_per_min: float           # enrichment service errors/min

    # ── Consumer throughput (per topic_name) ──────────────────────────────────
    consumer_msg_rates:   list[tuple[str, float]] = field(default_factory=list)
    producer_throughputs: list[tuple[str, float]] = field(default_factory=list)

    # ── Consumer lag  [(group_label, max_lag), ...] ───────────────────────────
    consumer_max_lags: list[tuple[str, float]] = field(default_factory=list)
    consumer_sum_lags: list[tuple[str, float]] = field(default_factory=list)

    # ── Broker topic msg/sec  [(topic, rate), ...] ────────────────────────────
    broker_topic_rates: list[tuple[str, float]] = field(default_factory=list)

    # ── CDC API RR logs ───────────────────────────────────────────────────────
    rr_success_pct:    Optional[float] = None   # % of RR logs that succeeded
    rr_vs_total_pct:   Optional[float] = None   # % of API RR logs vs total logs

    # ── Derived flags (populated in collect()) ───────────────────────────────
    @property
    def is_flagged(self) -> bool:
        """Return True if any metric is outside acceptable bounds."""
        return bool(self._flag_items())

    def _flag_items(self) -> list[tuple[str, str]]:
        """Return [(severity, description)] for each breach.  severity ∈ {'warn','crit'}."""
        items: list[tuple[str, str]] = []

        # Producer success rate
        if self.producer_success_rate is not None:
            if self.producer_success_rate < 95.0:
                items.append(("crit", f"producer success {self.producer_success_rate:.1f}%"))
            elif self.producer_success_rate < 99.0:
                items.append(("warn", f"producer success {self.producer_success_rate:.1f}%"))

        # Failure counts — any non-zero is worth surfacing
        if self.pub_fail_per_min > 0:
            items.append(("warn", f"pub failures {self.pub_fail_per_min:.1f}/min"))
        if self.producer_ack_fail_1h > 0:
            items.append(("warn", f"ACK failures {self.producer_ack_fail_1h:.0f} (1h)"))
        if self.publishing_fail_1h > 0:
            items.append(("warn", f"pub failures {self.publishing_fail_1h:.0f} (1h)"))
        if self.enrichment_err_per_min > 0:
            items.append(("warn", f"enrichment errors {self.enrichment_err_per_min:.1f}/min"))

        # Consumer lag — max_lag > 1 000 → warn; > 10 000 → crit
        for label, lag in self.consumer_max_lags:
            if lag > 10_000:
                items.append(("crit", f"consumer lag [{label}] max={lag:.0f}"))
            elif lag > 1_000:
                items.append(("warn", f"consumer lag [{label}] max={lag:.0f}"))

        # CDC API RR success rate
        if self.rr_success_pct is not None:
            if self.rr_success_pct < 80.0:
                items.append(("crit", f"CDC RR success {self.rr_success_pct:.1f}%"))
            elif self.rr_success_pct < 95.0:
                items.append(("warn", f"CDC RR success {self.rr_success_pct:.1f}%"))

        return items


async def collect_ti_kafka_metrics() -> Optional[TIKafkaMetrics]:
    """Collect all TI Kafka metrics from VictoriaMetrics.

    Returns None on total failure (VM unreachable), or a TIKafkaMetrics
    with zero/None values for individual metrics that couldn't be fetched.
    """
    try:
        async with VMClient(settings.vm_base_url, headers=settings.vm_headers) as vm:
            return await _collect(vm)
    except Exception as exc:
        log.error("TI Kafka metrics collection failed entirely: %s", exc)
        return None


async def _collect(vm: VMClient) -> TIKafkaMetrics:
    # ── Scalar queries (run concurrently) ─────────────────────────────────────
    scalar_coros = [
        # [0]  Producer success rate (0–100 %)
        vm.query(
            "(\n"
            "  rate(bm_kafka_producer_ack_success_count[5m])\n"
            "  /\n"
            "  (\n"
            "    rate(bm_kafka_producer_ack_success_count[5m])\n"
            "    + rate(bm_kafka_producer_ack_failed_count[5m])\n"
            "  )\n"
            ") * 100 OR on() vector(100)\n"
        ),
        # [1]  Publishing failures per minute
        vm.query("rate(bm_kafka_publishing_failed_messages_count[5m]) * 60"),
        # [2]  ACK failures last 1h
        vm.query("sum(increase(bm_kafka_producer_ack_failed_count[1h]))"),
        # [3]  Publishing failures last 1h
        vm.query("sum(increase(bm_kafka_publishing_failed_messages_count[1h]))"),
        # [4]  Enrichment service errors per minute
        vm.query("rate(publish_batch_to_enrichment_service_error[5m]) * 60"),
        # [5]  CDC API RR success %
        vm.query(
            'sum(service_api_rr_log_success{environment="prod", exported_job="TRANSACTION_INSIGHT"})'
            ' / sum(total_service_api_rr_log_requests{environment="prod", exported_job="TRANSACTION_INSIGHT"})'
            ' * 100'
        ),
        # [6]  CDC API RR logs vs total logs %
        vm.query(
            'sum(total_service_api_rr_log_requests{environment="prod", exported_job="TRANSACTION_INSIGHT"})'
            ' / sum(total_log_requests{environment="prod", exported_job="TRANSACTION_INSIGHT"})'
            ' * 100'
        ),
        # [7–9]  Consumer max lag per group
        *[
            vm.query(f'max(kafka_consumer_group_ConsumerLagMetrics_Value{{groupId="{g}", name="MaxOffsetLag"}})')
            for g in _CONSUMER_GROUPS
        ],
        # [10–12]  Consumer sum lag per group
        *[
            vm.query(f'sum(kafka_consumer_group_ConsumerLagMetrics_Value{{groupId="{g}", name="SumOffsetLag"}})')
            for g in _CONSUMER_GROUPS
        ],
        # [13–15]  Broker topic message rates
        *[
            vm.query(f'sum(rate(kafka_server_BrokerTopicMetrics_Count{{name="MessagesInPerSec", topic=~"{t}"}}[5m]))')
            for t in _BROKER_TOPICS
        ],
    ]

    # Per-topic vector queries
    vector_coros = [
        # Consumer message rate per topic_name
        vm.query_vector('rate(bm_kafka_consumer_received_count{name=~"p-.*"}[5m])', id_label="topic_name"),
        # Producer throughput per topic_name
        vm.query_vector('rate(bm_kafka_producer_published_count{name=~"p-.*"}[5m])', id_label="topic_name"),
    ]

    # ── Concurrency cap ───────────────────────────────────────────────────────
    # All 18 queries fire at the same time as dozens of other VM queries from
    # ALSM/SAISM/central-business collectors → causes 429 bursts.
    # Cap at 4 concurrent to spread load without noticeably slowing the run.
    _sem = asyncio.Semaphore(4)

    async def _limited(coro):
        async with _sem:
            return await coro

    scalar_results, vector_results = await asyncio.gather(
        asyncio.gather(*[_limited(c) for c in scalar_coros], return_exceptions=True),
        asyncio.gather(*[_limited(c) for c in vector_coros], return_exceptions=True),
    )

    def _s(idx: int) -> float:
        """Safe scalar extractor — returns 0.0 on error/None."""
        v = scalar_results[idx]
        if isinstance(v, Exception) or v is None:
            return 0.0
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    def _so(idx: int) -> Optional[float]:
        """Safe scalar extractor — returns None on error/None."""
        v = scalar_results[idx]
        if isinstance(v, Exception) or v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    def _vec(idx: int) -> list[tuple[str, float]]:
        v = vector_results[idx]
        if isinstance(v, Exception) or not v:
            return []
        return [(label, float(val)) for label, val in v]

    consumer_max_lags = [
        (_GROUP_LABELS[i], _s(7 + i)) for i in range(len(_CONSUMER_GROUPS))
    ]
    consumer_sum_lags = [
        (_GROUP_LABELS[i], _s(10 + i)) for i in range(len(_CONSUMER_GROUPS))
    ]
    broker_topic_rates = [
        (_BROKER_TOPICS[i], _s(13 + i)) for i in range(len(_BROKER_TOPICS))
    ]

    metrics = TIKafkaMetrics(
        producer_success_rate  = _so(0),
        pub_fail_per_min       = _s(1),
        producer_ack_fail_1h   = _s(2),
        publishing_fail_1h     = _s(3),
        enrichment_err_per_min = _s(4),
        rr_success_pct         = _so(5),
        rr_vs_total_pct        = _so(6),
        consumer_msg_rates     = _vec(0),
        producer_throughputs   = _vec(1),
        consumer_max_lags      = consumer_max_lags,
        consumer_sum_lags      = consumer_sum_lags,
        broker_topic_rates     = broker_topic_rates,
    )

    log.info(
        "TI Kafka metrics: producer_success=%.1f%% pub_fail/min=%.2f "
        "ack_fail_1h=%.0f enrichment_err/min=%.2f rr_success=%.1f%%",
        metrics.producer_success_rate or 0,
        metrics.pub_fail_per_min,
        metrics.producer_ack_fail_1h,
        metrics.enrichment_err_per_min,
        metrics.rr_success_pct or 0,
    )

    return metrics
