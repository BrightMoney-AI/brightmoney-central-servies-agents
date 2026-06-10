"""
emr_renderer.py — Renders EmrReport as Slack Canvas markdown.

Each section is rendered as a compact markdown table.
Flagged rows are shown first with 🔴; healthy rows follow collapsed to a count.
"""
from __future__ import annotations

from .emr_collector import EmrReport, EmrSection, EmrRow

_MAX_HEALTHY_ROWS = 10   # unflagged rows shown per section before collapsing


def _render_table(headers: list[str], rows: list[EmrRow], max_healthy: int = _MAX_HEALTHY_ROWS) -> str:
    if not headers:
        return "_no data_\n"

    flagged   = [r for r in rows if r.flagged]
    healthy   = [r for r in rows if not r.flagged]

    lines: list[str] = []

    # header row
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "|".join("---" for _ in headers) + "|")

    for row in flagged:
        lines.append("| 🔴 " + " | ".join(row.cells) + " |")

    shown_healthy = healthy[:max_healthy]
    for row in shown_healthy:
        lines.append("| " + " | ".join(row.cells) + " |")

    if len(healthy) > max_healthy:
        lines.append(f"_+{len(healthy) - max_healthy} more healthy rows not shown_")

    return "\n".join(lines) + "\n"


def _render_section(section: EmrSection) -> str:
    lines: list[str] = []

    if section.failed:
        lines.append(f"### ⚪ {section.title}")
        lines.append("")
        lines.append("_Query failed — no data available_")
        lines.append("")
        return "\n".join(lines)

    flag_label = f"  ·  🔴 {section.flag_count} flagged" if section.flag_count else ""
    lines.append(f"### {section.title}{flag_label}")
    lines.append("")
    lines.append(_render_table(section.headers, section.rows))
    return "\n".join(lines)


def render_emr_canvas(report: EmrReport, title: str = "") -> str:
    if not report.sections:
        return "No EMR metrics data available."

    lines: list[str] = []

    if title:
        lines += [f"# {title}", ""]

    total_flags = report.total_flags
    failed      = sum(1 for s in report.sections if s.failed)

    if total_flags == 0 and failed == 0:
        overall = "✅ All checks healthy"
    elif total_flags > 0:
        overall = f"🔴 {total_flags} flagged item(s)"
    else:
        overall = f"⚠️ {failed} query failure(s)"

    lines.append(f"**{overall}**  ·  {len(report.sections)} sections")
    lines.append("")

    for section in report.sections:
        lines.append(_render_section(section))
        lines.append("---")
        lines.append("")

    lines.append(
        "_Flags: recency_breach=true · staleness >24h · "
        "p95 mem >8 GB · RSS util <30% · p50 CPU <10% · "
        "p95 schedule delay >1h · p95 exec time >4h · row shrinkage_"
    )

    return "\n".join(lines)
