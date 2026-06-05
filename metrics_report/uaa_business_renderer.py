"""
uaa_business_renderer.py — Renders UAA Services business metrics into Slack Canvas markdown.

Layout per section:
  - All checks healthy  → single ✅ summary line
  - Has flagged items   → table of flagged metrics, then collapsed healthy line
  - provider_comparison → always rendered as a D vs D-1 comparison table
"""
from __future__ import annotations

from collections import defaultdict

from .uaa_business_collector import BusinessMetric

# Section display order
_SECTION_ORDER: list[str] = [
    "Onboarding",
    "Account Linking",
    "ALSM",
    "SAISM",
    "Plaid Batch Refresh",
    "Plaid Force Refresh",
    "Partner Costs",
]

# For latency metrics: lower is better so delta coloring is inverted vs counts

_RATE_CRIT = 95.0   # success_rate below this → 🔴
_RATE_WARN = 99.0   # success_rate below this → 🟡


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
    return False   # provider_comparison, total_count, rate are informational


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


# ── comparison table renderers ────────────────────────────────────────────────

def _render_provider_comparison(m: BusinessMetric) -> str:
    """Render a provider_comparison metric as a D vs D-1 Markdown table."""
    lines: list[str] = [
        f"**{m.display_name}**",
        "",
        "| Provider | Today Sessions | Today Success | Yesterday Sessions | Yesterday Success |",
        "|---|---|---|---|---|",
    ]
    for detail in m.details:
        parts = [p.strip() for p in detail.split("|")]
        if len(parts) == 5:
            lines.append(f"| {' | '.join(parts)} |")
    lines.append("")
    return "\n".join(lines)


def _render_multi_col_table(m: BusinessMetric) -> str:
    """Render a multi_col_table metric. details[0] = pipe-delimited headers, details[1:] = rows."""
    if len(m.details) < 2:
        return f"**{m.display_name}** — no data\n"
    headers = [h.strip() for h in m.details[0].split("|")]
    sep     = "|".join("---" for _ in headers)
    lines   = [
        f"**{m.display_name}**",
        "",
        "| " + " | ".join(headers) + " |",
        f"|{sep}|",
    ]
    for row in m.details[1:]:
        parts = [p.strip() for p in row.split("|")]
        # Pad or trim to match header column count
        while len(parts) < len(headers):
            parts.append("N/A")
        lines.append("| " + " | ".join(parts[:len(headers)]) + " |")
    lines.append("")
    return "\n".join(lines)


def _render_source_comparison(m: BusinessMetric) -> str:
    """Render a source_comparison metric as a Today vs Yesterday breakdown table.

    Details rows are pipe-delimited: source | flow | today | yesterday | delta
    """
    lines: list[str] = [
        f"**{m.display_name}**  _(last 4h today vs same 4h window yesterday)_",
        "",
        "| Source | Flow | Today | Yesterday | Change |",
        "|---|---|---|---|---|",
    ]
    for detail in m.details:
        parts = [p.strip() for p in detail.split("|")]
        if len(parts) == 5:
            source, flow, today, yesterday, delta = parts
            # Colour the delta: green if positive/zero, red if negative
            delta_fmt = f"🟢 {delta}" if not delta.startswith("-") else f"🔴 {delta}"
            lines.append(f"| {source} | {flow} | {today} | {yesterday} | {delta_fmt} |")
    lines.append("")
    return "\n".join(lines)


# ── section renderer ──────────────────────────────────────────────────────────

def _render_section(section: str, items: list[BusinessMetric]) -> str:
    flagged            = [m for m in items if _is_flagged(m)]
    check_metrics      = [m for m in items if m.metric_type in ("success_rate", "failure_count")]
    healthy_checks     = [m for m in check_metrics if not _is_flagged(m)]
    info_metrics       = [m for m in items if m.metric_type in ("total_count", "rate")]
    comparison_metrics = [m for m in items if m.metric_type in ("provider_comparison", "source_comparison")]
    table_metrics      = [m for m in items if m.metric_type == "multi_col_table"]

    lines: list[str] = []

    if not flagged:
        n = len(check_metrics)
        if n > 0:
            label = f"all {n} check{'s' if n != 1 else ''} healthy"
        elif comparison_metrics or table_metrics:
            label = "overview"
        else:
            label = "no checks"
        lines.append(f"### ✅ {section} · {label}")
        lines.append("")
    else:
        sec_emoji = _section_worst_emoji(items)
        lines.append(f"### {sec_emoji} {section} · {len(flagged)} flagged")
        lines.append("")

        lines.append("| Metric | Value |")
        lines.append("|---|---|")
        for m in flagged:
            lines.append(f"| {m.display_name} | {_fmt_value(m)} |")
        lines.append("")

        if healthy_checks:
            names  = ", ".join(m.display_name for m in healthy_checks[:4])
            extra  = len(healthy_checks) - 4
            suffix = f" +{extra} more" if extra > 0 else ""
            lines.append(f"_🟢 {names}{suffix}_")
            lines.append("")

        if info_metrics:
            lines.append("| Metric | Value |")
            lines.append("|---|---|")
            for m in info_metrics:
                lines.append(f"| {m.display_name} | {_fmt_value(m)} |")
            lines.append("")

    # Comparison tables always rendered regardless of section flag status
    for m in comparison_metrics:
        if m.metric_type == "source_comparison":
            lines.extend(_render_source_comparison(m).splitlines())
        else:
            lines.extend(_render_provider_comparison(m).splitlines())
        lines.append("")

    # Generic multi-column tables always rendered
    for m in table_metrics:
        lines.extend(_render_multi_col_table(m).splitlines())
        lines.append("")

    return "\n".join(lines)


# ── public entry point ────────────────────────────────────────────────────────

def render_uaa_business_canvas(metrics: list[BusinessMetric], title: str = "") -> str:
    if not metrics:
        return "No UAA business metrics data available."

    grouped: dict[str, list[BusinessMetric]] = defaultdict(list)
    for m in metrics:
        grouped[m.section].append(m)

    ordered  = [s for s in _SECTION_ORDER if s in grouped]
    ordered += [s for s in grouped if s not in _SECTION_ORDER]

    sections = [_render_section(s, grouped[s]) for s in ordered]

    header = f"# {title}\n\n" if title else ""
    footer = (
        "\n\n---\n\n"
        "Today = current date   Yesterday = previous day   ·   "
        "🟢 ≥99%   🟡 95–99%   🔴 <95%   ·   brightmoney observability"
    )
    return header + "\n\n---\n\n".join(sections) + footer
