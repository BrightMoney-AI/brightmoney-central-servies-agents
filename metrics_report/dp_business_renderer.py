"""
dp_business_renderer.py — Renders Data Platform business metrics into Slack Canvas markdown.

Layout per section:
  - All checks pass (count = 0) → ✅ single line
  - Any failures                → count summary + bulleted list of affected tables
"""
from __future__ import annotations

from collections import defaultdict

from .dp_business_collector import BusinessMetric

_SECTION_ORDER = [
    "Table Recency",
    "CDC Health",
    "Validation",
    "View Health",
    "Compaction",
]

_RATE_CRIT = 95.0   # kept for scheduler summary block compatibility
_RATE_WARN = 99.0


# ── helpers ───────────────────────────────────────────────────────────────────

def _count_emoji(v: float) -> str:
    return "🟢" if v == 0 else "🔴"


def _fmt_value(m: BusinessMetric) -> str:
    if m.metric_type == "failure_count":
        e = _count_emoji(m.value)
        n = int(m.value)
        noun = "table" if n == 1 else "tables"
        return f"{e} {n} {noun}"
    if m.metric_type == "total_count":
        return f"{int(m.value):,}"
    return f"{m.value:.0f}"


def _is_flagged(m: BusinessMetric) -> bool:
    return m.metric_type == "failure_count" and m.value > 0


def _section_emoji(items: list[BusinessMetric]) -> str:
    return "🔴" if any(_is_flagged(m) for m in items) else "✅"


# ── section renderer ──────────────────────────────────────────────────────────

def _render_section(section: str, items: list[BusinessMetric]) -> str:
    flagged   = [m for m in items if _is_flagged(m)]
    healthy   = [m for m in items if not _is_flagged(m)]
    lines: list[str] = []

    if not flagged:
        n = len(items)
        lines.append(f"### ✅ {section} · all {n} check{'s' if n != 1 else ''} healthy")
        lines.append("")
        return "\n".join(lines)

    lines.append(f"### 🔴 {section} · {len(flagged)} issue{'s' if len(flagged) != 1 else ''}")
    lines.append("")

    for m in flagged:
        n = int(m.value)
        lines.append(f"**{m.display_name}** — {n} affected")
        if m.details:
            shown = m.details[:10]
            lines.extend(f"- `{item}`" for item in shown)
            if len(m.details) > 10:
                lines.append(f"_+{len(m.details) - 10} more_")
        lines.append("")

    if healthy:
        names = ", ".join(m.display_name for m in healthy)
        lines.append(f"_🟢 {names}_")
        lines.append("")

    return "\n".join(lines)


# ── public entry point ────────────────────────────────────────────────────────

def render_dp_business_canvas(metrics: list[BusinessMetric], title: str = "") -> str:
    if not metrics:
        return "No Data Platform business metrics data available."

    grouped: dict[str, list[BusinessMetric]] = defaultdict(list)
    for m in metrics:
        grouped[m.section].append(m)

    ordered  = [s for s in _SECTION_ORDER if s in grouped]
    ordered += [s for s in grouped if s not in _SECTION_ORDER]

    sections = [_render_section(s, grouped[s]) for s in ordered]

    header = f"# {title}\n\n" if title else ""
    footer = "\n\n---\n\n🟢 0 issues   🔴 any issue   ·   brightmoney observability"
    return header + "\n\n---\n\n".join(sections) + footer
