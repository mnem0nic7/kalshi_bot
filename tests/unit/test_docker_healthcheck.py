from datetime import UTC, datetime

from kalshi_bot.docker_healthcheck import _env_int, daemon_threshold_components, parse_heartbeat_at


def test_parse_heartbeat_at_accepts_json_string() -> None:
    parsed = parse_heartbeat_at('{"heartbeat_at": "2026-05-06T19:29:50.695401+00:00"}')

    assert parsed == datetime(2026, 5, 6, 19, 29, 50, 695401, tzinfo=UTC)


def test_parse_heartbeat_at_rejects_missing_or_invalid_payload() -> None:
    assert parse_heartbeat_at({}) is None
    assert parse_heartbeat_at("{not json") is None


def test_env_int_uses_default_for_missing_or_invalid_values(monkeypatch) -> None:
    monkeypatch.delenv("DAEMON_HEARTBEAT_INTERVAL_SECONDS", raising=False)
    assert _env_int("DAEMON_HEARTBEAT_INTERVAL_SECONDS", 60) == 60

    monkeypatch.setenv("DAEMON_HEARTBEAT_INTERVAL_SECONDS", "not-an-int")
    assert _env_int("DAEMON_HEARTBEAT_INTERVAL_SECONDS", 60) == 60

    monkeypatch.setenv("DAEMON_HEARTBEAT_INTERVAL_SECONDS", "15")
    assert _env_int("DAEMON_HEARTBEAT_INTERVAL_SECONDS", 60) == 15


def test_daemon_threshold_components_include_default_grace(monkeypatch) -> None:
    monkeypatch.delenv("DAEMON_HEARTBEAT_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("DAEMON_HEARTBEAT_UNHEALTHY_GRACE_SECONDS", raising=False)

    assert daemon_threshold_components() == {
        "heartbeat_interval_seconds": 60,
        "base_seconds": 135,
        "grace_seconds": 45,
        "threshold_seconds": 180,
    }


def test_daemon_threshold_components_accept_env_override(monkeypatch) -> None:
    monkeypatch.setenv("DAEMON_HEARTBEAT_INTERVAL_SECONDS", "30")
    monkeypatch.setenv("DAEMON_HEARTBEAT_UNHEALTHY_GRACE_SECONDS", "90")

    assert daemon_threshold_components() == {
        "heartbeat_interval_seconds": 30,
        "base_seconds": 75,
        "grace_seconds": 90,
        "threshold_seconds": 165,
    }


def test_daemon_threshold_components_fallback_for_invalid_env(monkeypatch) -> None:
    monkeypatch.setenv("DAEMON_HEARTBEAT_INTERVAL_SECONDS", "bad")
    monkeypatch.setenv("DAEMON_HEARTBEAT_UNHEALTHY_GRACE_SECONDS", "-5")

    assert daemon_threshold_components() == {
        "heartbeat_interval_seconds": 60,
        "base_seconds": 135,
        "grace_seconds": 0,
        "threshold_seconds": 135,
    }
