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


def daemon_threshold_components() -> dict[str, int]:
    heartbeat_interval_seconds = _env_int("DAEMON_HEARTBEAT_INTERVAL_SECONDS", 60)
    grace_seconds = max(0, _env_int("DAEMON_HEARTBEAT_UNHEALTHY_GRACE_SECONDS", 45))
    base_seconds = (heartbeat_interval_seconds * 2) + 15
    return {
        "heartbeat_interval_seconds": heartbeat_interval_seconds,
        "base_seconds": base_seconds,
        "grace_seconds": grace_seconds,
        "threshold_seconds": base_seconds + grace_seconds,
    }


def normalize_heartbeat_role(role: str | None) -> str:
    normalized = "".join(ch if ch.isalnum() else "_" for ch in str(role or "daemon").strip().lower())
    normalized = "_".join(part for part in normalized.split("_") if part)
    return normalized or "daemon"


def daemon_heartbeat_stream_name(kalshi_env: str, color: str, role: str | None = None) -> str:
    normalized_role = normalize_heartbeat_role(role)
    base = f"daemon_heartbeat:{kalshi_env}:{color}"
    return base if normalized_role == "daemon" else f"{base}:{normalized_role}"


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
    role = normalize_heartbeat_role(os.getenv("DAEMON_HEARTBEAT_ROLE", "daemon"))
    threshold_components = daemon_threshold_components()
    threshold_seconds = threshold_components["threshold_seconds"]
    stream_name = daemon_heartbeat_stream_name(kalshi_env, color, role)

    conn = await asyncpg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres_demo"),
        port=_env_int("POSTGRES_PORT", 5432),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB", "kalshi_bot"),
        timeout=5,
        command_timeout=5,
    )
    try:
        payload = await conn.fetchval(
            "select payload from checkpoints where stream_name = $1",
            stream_name,
        )
    finally:
        try:
            await asyncio.wait_for(conn.close(), timeout=1)
        except Exception:
            conn.terminate()

    now = datetime.now(UTC)
    heartbeat_at = parse_heartbeat_at(payload)
    heartbeat_age_seconds = (
        (now - heartbeat_at).total_seconds() if heartbeat_at is not None else None
    )
    healthy = heartbeat_age_seconds is not None and heartbeat_age_seconds <= threshold_seconds
    result = {
        "kalshi_env": kalshi_env,
        "color": color,
        "daemon_role": role,
        "stream_name": stream_name,
        "healthy": healthy,
        "reason": "heartbeat fresh" if healthy else "heartbeat missing or stale",
        "heartbeat_at": heartbeat_at.isoformat() if heartbeat_at is not None else None,
        "heartbeat_age_seconds": heartbeat_age_seconds,
        "threshold_seconds": threshold_seconds,
        "threshold_components": {
            key: value
            for key, value in threshold_components.items()
            if key != "threshold_seconds"
        },
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
