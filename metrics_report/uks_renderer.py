"""
uks_renderer.py — Renders the UKS KYC metrics canvas for the regular detailed channel.

Covers:
  - KYC flow pass/fail rate
  - Celery task success rates and P99 latency
  - Incoming API success rates and request volumes
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .uks_collector import UKSMetrics


def render_uks_canvas(metrics: "UKSMetrics", title: str = "") -> str:
    lines: list[str] = []
    if title:
        lines += [f"# {title}", ""]

    # ── KYC Flow ──────────────────────────────────────────────────────────────
    lines += ["## KYC Flow", ""]
    if metrics.kyc_pass_rate is not None:
        pass_icon = ("🔴" if metrics.kyc_pass_rate < 90
                     else "🟡" if metrics.kyc_pass_rate < 95 else "🟢")
        fail_icon = ("🔴" if (metrics.kyc_fail_rate or 0) > 10
                     else "🟡" if (metrics.kyc_fail_rate or 0) > 5 else "🟢")
        rpm_str = f"  ·  {metrics.kyc_per_min:.1f} flows/min" if metrics.kyc_per_min else ""
        lines += [
            "| Metric | Value | Status |",
            "|---|---|---|",
            f"| Pass Rate | {metrics.kyc_pass_rate:.1f}%{rpm_str} | {pass_icon} |",
            f"| Fail Rate | {metrics.kyc_fail_rate:.1f}% | {fail_icon} |" if metrics.kyc_fail_rate is not None else "| Fail Rate | — | ⚪ |",
            "",
        ]
    else:
        lines += ["_No KYC flow data_", ""]

    # ── Celery Tasks ──────────────────────────────────────────────────────────
    if metrics.tasks:
        lines += ["## Celery Tasks", ""]
        lines += ["| Task | Success Rate | P99 Latency | Status |", "|---|---|---|---|"]
        for t in sorted(metrics.tasks, key=lambda x: x.name):
            suc  = f"{t.success_rate:.1f}%" if t.success_rate is not None else "—"
            p99  = f"{t.p99_ms:.0f} ms"     if t.p99_ms is not None else "—"
            icon = ("🔴" if t.success_rate is not None and t.success_rate < 90
                    else "🟡" if t.success_rate is not None and t.success_rate < 95
                    else "🟢")
            lines.append(f"| `{t.name}` | {suc} | {p99} | {icon} |")
        lines.append("")

    # ── Incoming API ──────────────────────────────────────────────────────────
    if metrics.api_views:
        lines += ["## Incoming API — By View", ""]
        lines += ["| View | Success Rate | Req/min | Status |", "|---|---|---|---|"]
        for v in sorted(metrics.api_views, key=lambda x: -(x.req_per_min or 0)):
            suc = f"{v.success_rate:.1f}%" if v.success_rate is not None else "—"
            rpm = f"{v.req_per_min:.1f}"   if v.req_per_min is not None else "—"
            icon = ("🔴" if v.success_rate is not None and v.success_rate < 90
                    else "🟡" if v.success_rate is not None and v.success_rate < 95
                    else "🟢")
            lines.append(f"| `{v.view}` | {suc} | {rpm} | {icon} |")
        lines.append("")

    lines += [
        "---",
        "",
        "🟢 ≥ 95%   🟡 90–95%   🔴 < 90%   ·   brightmoney observability",
    ]
    return "\n".join(lines)
