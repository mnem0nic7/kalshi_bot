from __future__ import annotations

from prometheus_client import Counter, Gauge


ROOM_RUNS_TOTAL = Counter("kalshi_bot_room_runs_total", "Completed room workflows", ["status"])
ORDERS_TOTAL = Counter("kalshi_bot_orders_total", "Orders attempted", ["status"])
ACTIVE_ROOMS = Gauge("kalshi_bot_active_rooms", "Rooms currently active")
FEED_FRESHNESS_SECONDS = Gauge("kalshi_bot_feed_freshness_seconds", "Age of last feed update", ["feed"])

