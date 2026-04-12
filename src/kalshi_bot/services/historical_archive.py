from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from kalshi_bot.config import Settings
from kalshi_bot.weather.scoring import extract_current_temp_f, extract_forecast_high_f


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _as_utc(value)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _quantize_two(value: float | None) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value)).quantize(Decimal("0.01"))


def weather_bundle_archive_metadata(payload: dict[str, Any], *, captured_at: datetime | None = None) -> dict[str, Any] | None:
    mapping = payload.get("mapping") or {}
    station_id = str(mapping.get("station_id") or "").strip()
    if not station_id:
        return None
    timezone_name = str(mapping.get("timezone_name") or "UTC")
    observation_ts = _parse_iso(((payload.get("observation") or {}).get("properties") or {}).get("timestamp"))
    forecast_updated_ts = _parse_iso(((payload.get("forecast") or {}).get("properties") or {}).get("updated"))
    effective_captured_at = _as_utc(captured_at) or datetime.now(UTC)
    zone = ZoneInfo(timezone_name)
    local_market_day = (observation_ts or forecast_updated_ts or effective_captured_at).astimezone(zone).date().isoformat()
    asof_ts = max(item for item in (observation_ts, forecast_updated_ts, effective_captured_at) if item is not None)
    return {
        "station_id": station_id,
        "series_ticker": mapping.get("series_ticker"),
        "local_market_day": local_market_day,
        "timezone_name": timezone_name,
        "asof_ts": asof_ts,
        "observation_ts": observation_ts,
        "forecast_updated_ts": forecast_updated_ts,
        "forecast_high_f": _quantize_two(extract_forecast_high_f(payload.get("forecast") or {})),
        "current_temp_f": _quantize_two(extract_current_temp_f(payload.get("observation") or {})),
    }


def append_weather_bundle_archive(
    settings: Settings,
    payload: dict[str, Any],
    *,
    source_id: str,
    archive_source: str,
    captured_at: datetime | None = None,
) -> dict[str, Any] | None:
    metadata = weather_bundle_archive_metadata(payload, captured_at=captured_at)
    if metadata is None:
        return None
    archive_root = Path(settings.historical_weather_archive_path)
    archive_path = archive_root / metadata["station_id"] / f"{metadata['local_market_day']}.jsonl"
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    archived_payload = dict(payload)
    archived_payload["_archive"] = {
        "source_id": source_id,
        "archive_source": archive_source,
        "captured_at": metadata["asof_ts"].isoformat(),
        "local_market_day": metadata["local_market_day"],
        "station_id": metadata["station_id"],
    }
    with archive_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(archived_payload, default=str))
        handle.write("\n")
    return {
        "archive_path": str(archive_path),
        **metadata,
    }
