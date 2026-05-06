from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import UTC, datetime
from typing import Any


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def parse_heartbeat_at(payload: Any) -> datetime | None:
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return None
    if not isinstance(payload, dict):
        return None
    raw = payload.get("heartbeat_at")
    if not isinstance(raw, str) or not raw.strip():
        return None
    value = raw.strip()
    if value.endswith("Z"):
        value = f"{value[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


async def _daemon_health() -> int:
    import asyncpg

    kalshi_env = os.getenv("KALSHI_ENV", "demo")
    color = os.getenv("APP_COLOR", "blue")
    heartbeat_interval_seconds = _env_int("DAEMON_HEARTBEAT_INTERVAL_SECONDS", 60)
    threshold_seconds = (heartbeat_interval_seconds * 2) + 15
    stream_name = f"daemon_heartbeat:{kalshi_env}:{color}"

    conn = await asyncpg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres_demo"),
        port=_env_int("POSTGRES_PORT", 5432),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB", "kalshi_bot"),
        timeout=5,
    )
    try:
        payload = await conn.fetchval(
            "select payload from checkpoints where stream_name = $1",
            stream_name,
        )
    finally:
        await conn.close()

    now = datetime.now(UTC)
    heartbeat_at = parse_heartbeat_at(payload)
    heartbeat_age_seconds = (
        (now - heartbeat_at).total_seconds() if heartbeat_at is not None else None
    )
    healthy = heartbeat_age_seconds is not None and heartbeat_age_seconds <= threshold_seconds
    result = {
        "kalshi_env": kalshi_env,
        "color": color,
        "healthy": healthy,
        "reason": "heartbeat fresh" if healthy else "heartbeat missing or stale",
        "heartbeat_at": heartbeat_at.isoformat() if heartbeat_at is not None else None,
        "heartbeat_age_seconds": heartbeat_age_seconds,
        "threshold_seconds": threshold_seconds,
    }
    print(json.dumps(result, separators=(",", ":")))
    return 0 if healthy else 1


def main() -> int:
    command = sys.argv[1] if len(sys.argv) > 1 else "daemon"
    if command != "daemon":
        print(f"unknown healthcheck command: {command}", file=sys.stderr)
        return 2
    return asyncio.run(_daemon_health())


if __name__ == "__main__":
    raise SystemExit(main())
