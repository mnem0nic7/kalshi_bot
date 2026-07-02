"""v1→v2 order payload translation (Kalshi deprecated POST /portfolio/orders).

V2 (POST /portfolio/events/orders) uses a single YES book: side is bid/ask and
price is the yes price. Buying NO == selling YES at the same yes price, so:
  (buy,  yes) -> bid    (buy,  no) -> ask
  (sell, yes) -> ask    (sell, no) -> bid
Found live 2026-07-02: v1 returns 410 deprecated_v1_order_endpoint — every
production order (weather + crypto + pilot) was broken until this remap."""
from __future__ import annotations

from kalshi_bot.integrations.kalshi import order_payload_v2


def _v1(**kw):
    base = {
        "ticker": "KXBNB15M-26JUL021130-30",
        "side": "yes",
        "action": "buy",
        "client_order_id": "room:abc",
        "count_fp": "1.00",
        "yes_price_dollars": "0.3700",
        "time_in_force": "immediate_or_cancel",
        "self_trade_prevention_type": "taker_at_cross",
    }
    base.update(kw)
    return base


class TestOrderPayloadV2:
    def test_buy_yes_maps_to_bid(self):
        p = order_payload_v2(_v1(side="yes", action="buy"))
        assert p["side"] == "bid"
        assert p["price"] == "0.3700"
        assert p["count"] == "1.00"
        assert p["ticker"] == "KXBNB15M-26JUL021130-30"
        assert p["time_in_force"] == "immediate_or_cancel"
        assert p["self_trade_prevention_type"] == "taker_at_cross"
        assert p["client_order_id"] == "room:abc"
        assert "count_fp" not in p and "yes_price_dollars" not in p and "action" not in p

    def test_buy_no_maps_to_ask(self):
        p = order_payload_v2(_v1(side="no", action="buy"))
        assert p["side"] == "ask"
        assert p["price"] == "0.3700"

    def test_sell_yes_maps_to_ask(self):
        p = order_payload_v2(_v1(side="yes", action="sell"))
        assert p["side"] == "ask"

    def test_sell_no_maps_to_bid(self):
        p = order_payload_v2(_v1(side="no", action="sell"))
        assert p["side"] == "bid"

    def test_subaccount_becomes_int(self):
        p = order_payload_v2(_v1(subaccount="3"))
        assert p["subaccount"] == 3

    def test_no_subaccount_key_when_absent(self):
        p = order_payload_v2(_v1())
        assert "subaccount" not in p
