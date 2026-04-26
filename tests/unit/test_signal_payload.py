from __future__ import annotations

import pytest

from kalshi_bot.core.signal_payload import capital_bucket_from_signal_payload


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (None, "risky"),
        ({}, "risky"),
        ({"capital_bucket": "safe"}, "safe"),
        ({"capital_bucket": "RISKY"}, "risky"),
        ({"capital_bucket": "unknown", "trade_regime": "standard"}, "safe"),
        ({"trade_regime": "near_threshold"}, "risky"),
        ({"trade_regime": "longshot_yes"}, "risky"),
        ({"trade_regime": "longshot_no"}, "risky"),
    ],
)
def test_capital_bucket_from_signal_payload(payload, expected: str) -> None:
    assert capital_bucket_from_signal_payload(payload) == expected
