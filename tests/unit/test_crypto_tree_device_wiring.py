"""Integration of device resolution into the real XGBoost/LightGBM fitters.

These run CPU-only (CI-safe): they assert the size gate downgrades a GPU request
on small data and that the device actually used is recorded in the artifact, so
operators can see whether a model trained on GPU or fell back.
"""
from __future__ import annotations

from decimal import Decimal

from kalshi_bot.crypto.services import _fit_crypto_model_candidates


def _rows(n: int = 24) -> list[dict]:
    rows = []
    for i in range(n):
        label = 1 if i % 2 == 0 else 0
        mid = 0.4 + (i % 5) * 0.03
        rows.append(
            {
                "asset_symbol": "BTC",
                "label_yes": label,
                "mid_yes_dollars": Decimal(str(mid)),
                "spot_distance_dollars": Decimal("0.05"),
                "spot_current_dollars": Decimal("100"),
                "contract_close_yes_dollars": Decimal(str(mid + 0.05)),
                "time_to_close_seconds": 600 + i,
            }
        )
    return rows


def test_xgboost_size_gate_downgrades_small_gpu_request_to_cpu(monkeypatch) -> None:
    monkeypatch.setenv("CRYPTO_XGBOOST_DEVICE", "cuda")
    monkeypatch.setenv("CRYPTO_GPU_MIN_ROWS", "999999")  # force size downgrade

    result = _fit_crypto_model_candidates(_rows())["xgboost_classifier"]

    assert result["status"] == "available", result.get("reason")
    assert result["model"]["xgboost"]["device"] == "cpu"


def test_lightgbm_records_device_and_defaults_cpu(monkeypatch) -> None:
    monkeypatch.delenv("CRYPTO_LIGHTGBM_DEVICE", raising=False)

    result = _fit_crypto_model_candidates(_rows())["lightgbm_classifier"]

    assert result["status"] == "available", result.get("reason")
    assert result["model"]["lightgbm"]["device"] == "cpu"
