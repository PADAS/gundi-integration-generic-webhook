import importlib

import pytest

from app.settings import integration

VALID = {
    "OUTBOUND_REQUEST_TIMEOUT_SECONDS": 30.0,
    "OUTBOUND_FLUSH_LOCK_SECONDS": 300,
    "OUTBOUND_BATCH_PROGRESS_TTL_SECONDS": 90000,
    "OUTBOUND_BUFFER_MAX_RECORDS": 10000,
    "OUTBOUND_BACKOFF_INITIAL_SECONDS": 30,
    "OUTBOUND_BACKOFF_MAX_SECONDS": 900,
    "OUTBOUND_OVERFLOW_REPORT_SECONDS": 600,
    "OUTBOUND_INVALID_CONFIG_WARNING_SECONDS": 3600,
}


def test_the_defaults_are_valid():
    integration.validate_outbound_settings(VALID)
    integration.validate_outbound_settings(vars(integration))


@pytest.mark.parametrize("name", [n for n in VALID if n != "OUTBOUND_REQUEST_TIMEOUT_SECONDS"])
@pytest.mark.parametrize("bad", [0, -5, True, 1.5])
def test_counts_and_durations_must_be_positive_integers(name, bad):
    with pytest.raises(ValueError, match=rf"^{name} must be a positive integer; got {bad!r}\."):
        integration.validate_outbound_settings({**VALID, name: bad})


@pytest.mark.parametrize("bad", [0, -1.0, True])
def test_the_request_timeout_must_be_positive(bad):
    with pytest.raises(ValueError, match=rf"^OUTBOUND_REQUEST_TIMEOUT_SECONDS must be a positive number; got {bad!r}\."):
        integration.validate_outbound_settings({**VALID, "OUTBOUND_REQUEST_TIMEOUT_SECONDS": bad})


def test_the_initial_backoff_must_not_exceed_the_max():
    with pytest.raises(ValueError, match=r"OUTBOUND_BACKOFF_INITIAL_SECONDS \(901\) must not exceed"):
        integration.validate_outbound_settings({**VALID, "OUTBOUND_BACKOFF_INITIAL_SECONDS": 901})


def test_the_flush_lock_must_outlast_two_request_timeouts():
    with pytest.raises(ValueError, match=r"OUTBOUND_FLUSH_LOCK_SECONDS \(60\) must be greater than"):
        integration.validate_outbound_settings({**VALID, "OUTBOUND_FLUSH_LOCK_SECONDS": 60})


@pytest.mark.parametrize("env_name, env_value", [
    ("OUTBOUND_BUFFER_MAX_RECORDS", "0"),
    ("OUTBOUND_BATCH_PROGRESS_TTL_SECONDS", "-1"),
    ("OUTBOUND_BACKOFF_MAX_SECONDS", "10"),  # below the default initial backoff
])
def test_bad_environment_values_fail_at_import(monkeypatch, env_name, env_value):
    monkeypatch.setenv(env_name, env_value)
    try:
        with pytest.raises(ValueError, match=env_name):
            importlib.reload(integration)
    finally:
        monkeypatch.delenv(env_name)
        importlib.reload(integration)  # leave the module as the rest of the suite expects
    assert integration.OUTBOUND_BUFFER_MAX_RECORDS == 10000
