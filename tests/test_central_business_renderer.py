"""Tests for per-metric business metric thresholds."""
from metrics_report.central_business_collector import BusinessMetric
from metrics_report.central_business_renderer import _is_critical, _is_flagged, _rate_emoji


def _metric(**kwargs) -> BusinessMetric:
    defaults = dict(
        display_name="test",
        query_name="test",
        section="Test",
        metric_type="success_rate",
        value=100.0,
    )
    defaults.update(kwargs)
    return BusinessMetric(**defaults)


def test_mixpanel_single_events_failed_below_10k_healthy():
    m = _metric(
        display_name="Mixpanel Single Events Failed",
        metric_type="failure_count",
        value=7914,
        warn_above=10000,
        crit_above=10000,
    )
    assert not _is_flagged(m)
    assert not _is_critical(m)


def test_mixpanel_single_events_failed_above_10k_flagged():
    m = _metric(
        display_name="Mixpanel Single Events Failed",
        metric_type="failure_count",
        value=10001,
        warn_above=10000,
        crit_above=10000,
    )
    assert _is_flagged(m)
    assert _is_critical(m)


def test_snap_success_rates_above_50_healthy():
    for value in (57.1, 62.5, 71.4):
        m = _metric(
            display_name="App Event Success Rate",
            section="Snap",
            value=value,
            warn_below=50,
            crit_below=50,
        )
        assert not _is_flagged(m)
        assert _rate_emoji(m) == "🟢"


def test_snap_success_rate_below_50_critical():
    m = _metric(
        display_name="Web Event Success Rate",
        section="Snap",
        value=49.9,
        warn_below=50,
        crit_below=50,
    )
    assert _is_flagged(m)
    assert _is_critical(m)


def test_clevertap_single_event_success_rate_96_6_healthy():
    for value in (96.6, 96.5):
        m = _metric(
            display_name="Single Event Success Rate",
            section="CleverTap",
            value=value,
            warn_below=96,
            crit_below=90,
        )
        assert not _is_flagged(m)
        assert _rate_emoji(m) == "🟢"


def test_email_send_success_rate_94_7_healthy():
    m = _metric(
        display_name="Email Send Success Rate",
        section="Email Forwarder",
        value=94.7,
        warn_below=94,
        crit_below=90,
    )
    assert not _is_flagged(m)
    assert _rate_emoji(m) == "🟢"


def test_email_forwarder_not_processed_below_500_healthy():
    m = _metric(
        display_name="Notification Events Not Processed",
        section="Email Forwarder",
        metric_type="failure_count",
        value=193,
        warn_above=500,
        crit_above=500,
    )
    assert not _is_flagged(m)
    assert not _is_critical(m)


def test_default_success_rate_still_strict():
    m = _metric(value=96.0)
    assert _is_flagged(m)
    assert not _is_critical(m)
