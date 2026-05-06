from datetime import UTC, datetime

from kalshi_bot.docker_healthcheck import parse_heartbeat_at


def test_parse_heartbeat_at_accepts_json_string() -> None:
    parsed = parse_heartbeat_at('{"heartbeat_at": "2026-05-06T19:29:50.695401+00:00"}')

    assert parsed == datetime(2026, 5, 6, 19, 29, 50, 695401, tzinfo=UTC)


def test_parse_heartbeat_at_rejects_missing_or_invalid_payload() -> None:
    assert parse_heartbeat_at({}) is None
    assert parse_heartbeat_at("{not json") is None
