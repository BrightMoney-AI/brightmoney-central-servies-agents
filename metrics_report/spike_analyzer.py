"""
spike_analyzer.py — Spike detection over 30-minute bucketed time series.

Algorithm:
  1. 24h is chunked into 48 × 30-minute buckets via query_range (step=30m).
  2. CONSECUTIVE SPIKE: bucket[i] >= SPIKE_MULTIPLIER × bucket[i-1].
     Catches sudden jumps: 10 → 20 rps, 1% → 2% error rate, etc.
  3. MAX/AVG RATIO: max_val / avg_val.
     Catches gradual elevation that never hits a 2× consecutive jump,
     e.g. CPU steady at 40% then 70% for 2h — ratio = 1.75, flagged as elevated.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

SPIKE_MULTIPLIER   = 2.0   # bucket[i] / bucket[i-1] threshold for a spike
ELEVATED_RATIO     = 1.5   # max/avg threshold to flag as "elevated" (not a spike)


@dataclass
class SpikeResult:
    metric_name:   str
    display_name:  str
    unit:          str
    max_val:       float
    avg_val:       float
    spike_count:   int    # how many consecutive 2× jumps were found
    worst_jump:    float  # highest bucket[i]/bucket[i-1] ratio seen
    max_avg_ratio: float  # max / avg — catches the 40 → 70 case

    @property
    def is_spiked(self) -> bool:
        """True when at least one consecutive 2× jump was found."""
        return self.spike_count > 0

    @property
    def is_elevated(self) -> bool:
        """True when peak was >= 1.5× average without a 2× consecutive jump."""
        return not self.is_spiked and self.max_avg_ratio >= ELEVATED_RATIO

    def fmt(self, v: float) -> str:
        if self.unit == "%":
            return f"{v:.1f}%"
        if self.unit == "ms":
            return f"{v:.0f}ms"
        if self.unit == "rps":
            return f"{v:.2f} rps"
        return f"{v:.1f}"

    def fmt_max(self) -> str:
        return self.fmt(self.max_val)

    def fmt_avg(self) -> str:
        return self.fmt(self.avg_val)


def analyze(
    metric_name:  str,
    display_name: str,
    unit:         str,
    buckets:      list[float],
) -> Optional[SpikeResult]:
    """Analyze a list of 30-minute bucket values for spikes.

    Zero buckets are excluded from avg/max so APIs that are silent for most of
    the day (many zero buckets) don't produce a distorted max/avg ratio.

    Consecutive spike check only fires between adjacent non-zero buckets —
    a 0 → X transition is not a spike, it's the metric starting to receive traffic.

    Returns None if fewer than 2 non-zero buckets exist.
    """
    non_zero = [v for v in buckets if v > 0]
    if len(non_zero) < 2:
        return None

    avg_val       = sum(non_zero) / len(non_zero)
    max_val       = max(non_zero)
    max_avg_ratio = (max_val / avg_val) if avg_val > 0 else 0.0

    spike_count = 0
    worst_jump  = 0.0
    for i in range(1, len(buckets)):
        prev, curr = buckets[i - 1], buckets[i]
        if prev > 0 and curr > 0:          # both must be non-zero
            ratio = curr / prev
            if ratio > worst_jump:
                worst_jump = ratio
            if ratio >= SPIKE_MULTIPLIER:
                spike_count += 1

    return SpikeResult(
        metric_name   = metric_name,
        display_name  = display_name,
        unit          = unit,
        max_val       = max_val,
        avg_val       = avg_val,
        spike_count   = spike_count,
        worst_jump    = worst_jump,
        max_avg_ratio = max_avg_ratio,
    )
