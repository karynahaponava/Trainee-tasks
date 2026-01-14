import pytest
import pandas as pd
from src.rules import FatalErrorTimeRule, BundleFatalErrorRule


@pytest.fixture
def empty_df():
    """Returns an empty DataFrame with expected columns."""
    return pd.DataFrame(columns=["date", "severity", "bundle_id"])


@pytest.fixture
def boundary_data():
    """Exactly 10 errors (should NOT trigger an alert per requirement > 10)."""
    return pd.DataFrame(
        {
            "date": pd.date_range(start="2023-01-01 10:00:00", periods=10, freq="1s"),
            "severity": ["Fatal"] * 10,
            "bundle_id": ["com.test.boundary"] * 10,
        }
    )


@pytest.fixture
def trigger_data():
    """11 errors (SHOULD trigger an alert)."""
    return pd.DataFrame(
        {
            "date": pd.date_range(start="2023-01-01 10:00:00", periods=11, freq="1s"),
            "severity": ["Fatal"] * 11,
            "bundle_id": ["com.test.trigger"] * 11,
        }
    )


@pytest.fixture
def mixed_severity_data():
    """Many 'Error' severity logs, but 0 'Fatal'. Should be ignored."""
    return pd.DataFrame(
        {
            "date": pd.date_range(start="2023-01-01 10:00:00", periods=50, freq="1s"),
            "severity": ["Error"] * 50,
            "bundle_id": ["com.test.mixed"] * 50,
        }
    )


# --- TESTS FOR RULE 2.1 (Time Spike) ---


def test_rule_2_1_empty(empty_df):
    """Ensure rule handles empty input gracefully."""
    rule = FatalErrorTimeRule()
    assert len(rule.check(empty_df)) == 0


def test_rule_2_1_boundary(boundary_data):
    """Verify that exactly 10 errors do not trigger an alert."""
    rule = FatalErrorTimeRule()
    alerts = rule.check(boundary_data)
    assert len(alerts) == 0


def test_rule_2_1_trigger(trigger_data):
    """Verify that 11 errors trigger the alert."""
    rule = FatalErrorTimeRule()
    alerts = rule.check(trigger_data)
    assert len(alerts) > 0
    assert "[RULE 2.1]" in alerts[0]


def test_rule_2_1_ignore_non_fatal(mixed_severity_data):
    """Verify that non-Fatal errors are ignored."""
    rule = FatalErrorTimeRule()
    alerts = rule.check(mixed_severity_data)
    assert len(alerts) == 0


# --- TESTS FOR RULE 2.2 (Bundle Spike) ---


def test_rule_2_2_trigger(trigger_data):
    """Verify Bundle rule triggers correctly."""
    rule = BundleFatalErrorRule()
    alerts = rule.check(trigger_data)
    assert len(alerts) > 0
    assert "com.test.trigger" in alerts[0]


def test_rule_2_2_ignore_non_fatal(mixed_severity_data):
    """Verify Bundle rule ignores non-Fatal errors."""
    rule = BundleFatalErrorRule()
    alerts = rule.check(mixed_severity_data)
    assert len(alerts) == 0
