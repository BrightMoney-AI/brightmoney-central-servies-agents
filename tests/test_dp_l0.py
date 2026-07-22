"""Tests for the trend/baseline-relative CDC sink health model (dp_l0_collector)."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from metrics_report.dp_l0_collector import SinkHealth


def _sink(**kw) -> SinkHealth:
    base = dict(sink="s", debezium="d", heartbeat_topic="cdc_s.debezium_heartbeat")
    base.update(kw)
    return SinkHealth(**base)


# ── zero / near-zero lag is always healthy regardless of history ───────────────

def test_zero_lag_is_ok():
    """fiserv-style: no backlog, so nothing to flag even with a big baseline."""
    s = _sink(offset_lag=0, offset_lag_baseline=0, offset_lag_delta=0,
              offset_lag_delta_1h=0, heartbeat_rate=20)
    assert s.status == "ok"
    assert not s.is_flagged


# ── stalled consumer with real backlog → critical ─────────────────────────────

def test_stalled_with_backlog_is_critical():
    s = _sink(offset_lag=50_000, offset_lag_baseline=40_000,
              offset_lag_delta=0, offset_lag_delta_1h=0, heartbeat_rate=0)
    assert s.stalled
    assert s.status == "critical"


def test_stalled_but_no_backlog_is_ok():
    """Heartbeat quiet is fine when there's no backlog to drain."""
    s = _sink(offset_lag=10, offset_lag_baseline=5,
              offset_lag_delta=0, offset_lag_delta_1h=0, heartbeat_rate=0)
    assert not s.stalled
    assert s.status == "ok"


# ── stable-high backlog (busy sink at its own normal) → ok ─────────────────────

def test_stable_high_is_ok():
    """referral-service style: always ~43k, flat. Its own normal → not flagged."""
    s = _sink(offset_lag=43_000, offset_lag_baseline=42_000,
              offset_lag_delta=200, offset_lag_delta_1h=50, heartbeat_rate=25)
    # ratio ~1.02, not rising fast → ok
    assert s.status == "ok"


# ── growing far above normal & climbing now → critical ─────────────────────────

def test_growing_above_normal_is_critical():
    s = _sink(offset_lag=200_000, offset_lag_baseline=40_000,
              offset_lag_delta=160_000, offset_lag_delta_1h=20_000, heartbeat_rate=25)
    assert s.growth_ratio is not None and s.growth_ratio >= 4.0
    assert s._rising
    assert s.status == "critical"


# ── moderately above normal & rising over 24h → warning ────────────────────────

def test_moderate_rising_is_warning():
    s = _sink(offset_lag=100_000, offset_lag_baseline=40_000,
              offset_lag_delta=60_000, offset_lag_delta_1h=None, heartbeat_rate=25)
    # ratio 2.5 (>=2, <4), rising_24h True, not draining → warning
    assert s.status == "warning"


# ── draining is never flagged even if currently deep ───────────────────────────

def test_draining_is_ok():
    s = _sink(offset_lag=200_000, offset_lag_baseline=40_000,
              offset_lag_delta=-50_000, offset_lag_delta_1h=-30_000, heartbeat_rate=25)
    assert s.draining
    assert s.status == "ok"


def test_high_but_flat_1h_not_critical():
    """Deep vs normal but slope has gone flat — not climbing now, so not critical."""
    s = _sink(offset_lag=200_000, offset_lag_baseline=40_000,
              offset_lag_delta=160_000, offset_lag_delta_1h=0, heartbeat_rate=25)
    # ratio>=4 but _rising False (1h slope flat) → not critical; rising_24h True → warning
    assert not s._rising
    assert s.status == "warning"


# ── no data → unknown (not flagged red) ────────────────────────────────────────

def test_no_offset_data_is_unknown():
    s = _sink(offset_lag=None, offset_lag_baseline=None,
              offset_lag_delta=None, offset_lag_delta_1h=None, heartbeat_rate=None)
    assert s.status == "unknown"
    assert not s.is_flagged


# ── absolute backstop when no baseline exists ──────────────────────────────────

def test_absolute_backstop_critical_when_no_baseline():
    s = _sink(offset_lag=150_000, offset_lag_baseline=None,
              offset_lag_delta=120_000, offset_lag_delta_1h=10_000, heartbeat_rate=25)
    assert s.growth_ratio is None
    assert s.status == "critical"


def test_absolute_backstop_warning_when_no_baseline():
    s = _sink(offset_lag=30_000, offset_lag_baseline=None,
              offset_lag_delta=20_000, offset_lag_delta_1h=None, heartbeat_rate=25)
    assert s.growth_ratio is None
    assert s.status == "warning"


def test_low_absolute_no_baseline_is_ok():
    s = _sink(offset_lag=5_000, offset_lag_baseline=None,
              offset_lag_delta=3_000, offset_lag_delta_1h=None, heartbeat_rate=25)
    assert s.status == "ok"


# ── coord lag is informational only — never flags ──────────────────────────────

def test_coord_lag_does_not_flag():
    s = _sink(coord_lag=50_000, offset_lag=100, offset_lag_baseline=100,
              offset_lag_delta=0, offset_lag_delta_1h=0, heartbeat_rate=25)
    assert s.coord_status == "critical"
    assert s.status == "ok"
    assert not s.is_flagged


# ── baseline floor prevents tiny-baseline ratio explosions ─────────────────────

def test_baseline_floor_prevents_false_critical():
    """Baseline ~10 but current 2000: without the floor ratio=200×; with floor=2×."""
    s = _sink(offset_lag=2_000, offset_lag_baseline=10,
              offset_lag_delta=1_900, offset_lag_delta_1h=100, heartbeat_rate=25)
    # base floored to 1000 → ratio 2.0, rising_24h, 1h slope 100 (<500 noise) not _rising
    assert s.growth_ratio == 2.0
    assert s.status == "warning"  # warn (>=2 & rising_24h), not critical
