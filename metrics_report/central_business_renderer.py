"""
Renders Central Services business metrics into Slack Canvas markdown.

Layout per section:
  - All checks healthy  → single ✅ summary line
  - Has flagged items   → table of flagged metrics, then collapsed healthy line
  - Info metrics (totals/rates) shown only when section has issues
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

_RATE_CRIT = 95.0   # below this → 🔴
_RATE_WARN = 99.0   # below this → 🟡


# ── helpers ───────────────────────────────────────────────────────────────────

def _rate_emoji(v: float) -> str:
    if v >= _RATE_WARN:
        return "🟢"
    if v >= _RATE_CRIT:
        return "🟡"
    return "🔴"


def _failure_emoji(v: float) -> str:
    if v == 0:
        return "🟢"
    if v < 100:
        return "🟡"
    return "🔴"


def _fmt_value(m: BusinessMetric) -> str:
    if m.metric_type == "success_rate":
        return f"{_rate_emoji(m.value)} {m.value:.2f}%"
    if m.metric_type == "failure_count":
        return f"{_failure_emoji(m.value)} {m.value:.0f}"
    if m.metric_type == "rate":
        return f"{m.value:.3f}"
    return f"{m.value:.0f}"


def _is_flagged(m: BusinessMetric) -> bool:
    if m.metric_type == "success_rate":
        return m.value < _RATE_WARN
    if m.metric_type == "failure_count":
        return m.value > 0
    return False


def _section_worst_emoji(metrics: list[BusinessMetric]) -> str:
    flagged = [m for m in metrics if _is_flagged(m)]
    if not flagged:
        return "✅"
    is_crit = any(
        (m.metric_type == "success_rate" and m.value < _RATE_CRIT)
        or (m.metric_type == "failure_count" and m.value >= 100)
        for m in flagged
    )
    return "🔴" if is_crit else "🟡"


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

    # Flagged metrics table
    lines.append("| Metric | Value |")
    lines.append("|---|---|")
    for m in flagged:
        lines.append(f"| {m.display_name} | {_fmt_value(m)} |")
    lines.append("")

    # Collapsed healthy checks
    if healthy_checks:
        names = ", ".join(m.display_name for m in healthy_checks[:4])
        extra = len(healthy_checks) - 4
        suffix = f" +{extra} more" if extra > 0 else ""
        lines.append(f"_🟢 {names}{suffix}_")
        lines.append("")

    # Info metrics table (only when there are issues — gives context)
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
        "🟢 ≥99%   🟡 95–99%   🔴 <95%   ·   "
        "failure_count: 🟢 0   🟡 1–99   🔴 ≥100   ·   brightmoney observability"
    )
    return header + "\n\n---\n\n".join(sections) + footer
