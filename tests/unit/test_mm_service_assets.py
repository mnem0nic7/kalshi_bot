"""Asset resolution for the MM research service.

crypto_model_nightly_assets is a comma-separated STRING ("BTC,ETH,..."); naively
list()-ing it splits into characters. resolve_mm_assets must parse it into proper
asset symbols.
"""
from __future__ import annotations

from kalshi_bot.mm.service import resolve_mm_assets


def test_parses_comma_separated_string():
    assert resolve_mm_assets("BTC,ETH,SOL") == ["BTC", "ETH", "SOL"]


def test_handles_whitespace_and_blanks():
    assert resolve_mm_assets("btc, eth ,, sol ") == ["BTC", "ETH", "SOL"]


def test_accepts_list_input():
    assert resolve_mm_assets(["BTC", "eth"]) == ["BTC", "ETH"]


def test_falls_back_to_default_when_empty():
    assert resolve_mm_assets("") == ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "HYPE"]
    assert resolve_mm_assets(None) == ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "HYPE"]
