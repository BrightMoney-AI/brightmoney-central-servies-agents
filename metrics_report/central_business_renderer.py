"""
Renders Central Services business metrics into Slack Canvas markdown.

Layout per section:
  - All checks healthy  → single ✅ summary line
  - Has flagged items   → table of flagged metrics, then collapsed healthy line
  - Info metrics (totals/rates) shown only when section has issues

Per-metric thresholds can be set in central_business.json:
  warn_below / crit_below  — success_rate (flag when value drops below)
  warn_above / crit_above  — failure_count (flag when value exceeds)
"""
from __future__ import annotations

from collections import defaultdict

from .central_business_collector import BusinessMetric

_SECTION_ORDER = [
    "Mixpanel",
    "CleverTap",
    "Email",
    "Email Forwarder",
    "Facebook",
    "Singular",
    "Snap",
    "Firestore",
    "Webhook Gateway",
]

_RATE_WARN = 99.0   # default success_rate warn (flag below this)
_RATE_CRIT = 95.0   # default success_rate critical (red below this)
_FAIL_WARN = 0.0    # default failure_count warn (flag above this)
_FAIL_CRIT = 100.0  # default failure_count critical (red above this)


def _rate_thresholds(m: BusinessMetric) -> tuple[float, float]:
    warn = m.warn_below if m.warn_below is not None else _RATE_WARN
    crit = m.crit_below if m.crit_below is not None else _RATE_CRIT
    return warn, crit


def _failure_thresholds(m: BusinessMetric) -> tuple[float, float]:
    warn = m.warn_above if m.warn_above is not None else _FAIL_WARN
    crit = m.crit_above if m.crit_above is not None else _FAIL_CRIT
    return warn, crit


# ── helpers ───────────────────────────────────────────────────────────────────

def _rate_emoji(m: BusinessMetric) -> str:
    warn, crit = _rate_thresholds(m)
    v = m.value
    if v >= warn:
        return "🟢"
    if v >= crit:
        return "🟡"
    return "🔴"


def _failure_emoji(m: BusinessMetric) -> str:
    warn, crit = _failure_thresholds(m)
    v = m.value
    if v <= warn:
        return "🟢"
    if v <= crit:
        return "🟡"
    return "🔴"


def _fmt_value(m: BusinessMetric) -> str:
    if m.metric_type == "success_rate":
        return f"{_rate_emoji(m)} {m.value:.2f}%"
    if m.metric_type == "failure_count":
        return f"{_failure_emoji(m)} {m.value:.0f}"
    if m.metric_type == "rate":
        return f"{m.value:.3f}"
    return f"{m.value:.0f}"


def _is_flagged(m: BusinessMetric) -> bool:
    if m.metric_type == "success_rate":
        warn, _ = _rate_thresholds(m)
        return m.value < warn
    if m.metric_type == "failure_count":
        warn, _ = _failure_thresholds(m)
        return m.value > warn
    return False


def _is_critical(m: BusinessMetric) -> bool:
    if m.metric_type == "success_rate":
        _, crit = _rate_thresholds(m)
        return m.value < crit
    if m.metric_type == "failure_count":
        _, crit = _failure_thresholds(m)
        return m.value > crit
    return False


def _section_worst_emoji(metrics: list[BusinessMetric]) -> str:
    flagged = [m for m in metrics if _is_flagged(m)]
    if not flagged:
        return "✅"
    return "🔴" if any(_is_critical(m) for m in flagged) else "🟡"


# ── section renderer ──────────────────────────────────────────────────────────

def _render_section(section: str, items: list[BusinessMetric]) -> str:
    flagged = [m for m in items if _is_flagged(m)]
    check_metrics = [m for m in items if m.metric_type in ("success_rate", "failure_count")]
    healthy_checks = [m for m in check_metrics if not _is_flagged(m)]
    info_metrics = [m for m in items if m.metric_type in ("total_count", "rate")]

    lines: list[str] = []

    if not flagged:
        n = len(check_metrics)
        label = f"all {n} checks healthy" if n > 0 else "no checks"
        lines.append(f"### ✅ {section} · {label}")
        lines.append("")
        return "\n".join(lines)

    sec_emoji = _section_worst_emoji(items)
    lines.append(f"### {sec_emoji} {section} · {len(flagged)} flagged")
    lines.append("")

    lines.append("| Metric | Value |")
    lines.append("|---|---|")
    for m in flagged:
        lines.append(f"| {m.display_name} | {_fmt_value(m)} |")
    lines.append("")

    if healthy_checks:
        names = ", ".join(m.display_name for m in healthy_checks[:4])
        extra = len(healthy_checks) - 4
        suffix = f" +{extra} more" if extra > 0 else ""
        lines.append(f"_🟢 {names}{suffix}_")
        lines.append("")

    if info_metrics:
        lines.append("| Metric | Value |")
        lines.append("|---|---|")
        for m in info_metrics:
            lines.append(f"| {m.display_name} | {_fmt_value(m)} |")
        lines.append("")

    return "\n".join(lines)


# ── public entry point ────────────────────────────────────────────────────────

def render_business_canvas(metrics: list[BusinessMetric], title: str = "") -> str:
    """Return full Slack Canvas markdown for the business metrics report."""
    if not metrics:
        return "No business metrics data available."

    grouped: dict[str, list[BusinessMetric]] = defaultdict(list)
    for m in metrics:
        grouped[m.section].append(m)

    ordered = [s for s in _SECTION_ORDER if s in grouped]
    ordered += [s for s in grouped if s not in _SECTION_ORDER]

    sections = [_render_section(s, grouped[s]) for s in ordered]

    header = f"# {title}\n\n" if title else ""
    footer = (
        "\n\n---\n\n"
        "🟢 healthy   🟡 warning   🔴 critical   ·   "
        "defaults: success ≥99% / ≥95%   failure ≤0 / ≤100   ·   "
        "per-metric overrides in central_business.json   ·   "
        "brightmoney observability"
    )
    return header + "\n\n---\n\n".join(sections) + footer
