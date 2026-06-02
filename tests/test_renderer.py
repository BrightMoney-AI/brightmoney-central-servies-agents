"""Basic tests for renderer helpers and render()."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime, timezone

from metrics_report.models import (
    ApiMetrics, Endpoint, FlaggingThresholds, L0Report,
    Server, ServerMetrics, Status, SystemHealth,
)
from metrics_report.renderer import (
    _endpoint_is_flagged, _fmt_hits, _fmt_p99, _short_name, render,
)


def _make_report(endpoints=None, status=Status.HEALTHY):
    return L0Report(
        service="Test Service",
        reported_at=datetime(2026, 6, 2, 10, 3, tzinfo=timezone.utc),
        status=status,
        system=SystemHealth(servers=[
            Server("p-uaa-entity-manager-01", "em", ServerMetrics(10.0, 45.0, 55.0)),
        ]),
        api=ApiMetrics(
            throughput_rps=10.0,
            success_rate_pct=100.0,
            error_rate_pct=0.0,
            avg_latency_p50_ms=200,
        ),
        endpoints=endpoints or [],
        total_endpoint_count=len(endpoints) if endpoints else 0,
    )


def test_fmt_hits():
    assert _fmt_hits(1_467_544) == "1.5M"
    assert _fmt_hits(199_817)   == "200K"
    assert _fmt_hits(55)        == "55"


def test_fmt_p99():
    assert _fmt_p99(3629)  == "3.6s"
    assert _fmt_p99(356)   == "356ms"
    assert _fmt_p99(0.6)   == "0.6ms"


def test_endpoint_is_flagged_errors():
    t  = FlaggingThresholds()
    ep = Endpoint("/api/foo", 100, 100.0, errors=35, p99_ms=200)
    assert _endpoint_is_flagged(ep, t) is True


def test_endpoint_is_flagged_p99():
    t  = FlaggingThresholds()
    ep = Endpoint("/api/foo", 100, 100.0, errors=0, p99_ms=4523)
    assert _endpoint_is_flagged(ep, t) is True


def test_endpoint_is_flagged_success():
    t  = FlaggingThresholds()
    ep = Endpoint("/api/foo", 100, 89.1, errors=0, p99_ms=200)
    assert _endpoint_is_flagged(ep, t) is True


def test_endpoint_not_flagged():
    t  = FlaggingThresholds()
    ep = Endpoint("/api/foo", 100, 100.0, errors=0, p99_ms=200)
    assert _endpoint_is_flagged(ep, t) is False


def test_render_keys():
    report  = _make_report()
    payload = render(report)
    assert "text" in payload
    assert "blocks" in payload


def test_render_block_count():
    eps = [
        Endpoint(f"/api/ep-{i}", hits=1000 - i, success_pct=100.0, errors=0, p99_ms=200)
        for i in range(25)
    ]
    report  = _make_report(endpoints=eps)
    payload = render(report)
    assert len(payload["blocks"]) <= 50


def test_short_name_celery():
    assert _short_name("p-uaa-em-celery-05") == "cel-05"


def test_short_name_em():
    assert _short_name("p-uaa-entity-manager-10") == "em-10"


def test_critical_status_in_blocks():
    report  = _make_report(status=Status.CRITICAL)
    payload = render(report)
    joined  = " ".join(
        b.get("text", {}).get("text", "")
        for b in payload["blocks"]
        if b.get("type") == "section"
    )
    assert "🔴" in joined
