"""Integration of device resolution into the real XGBoost/LightGBM fitters.

These run CPU-only (CI-safe): they assert the size gate downgrades a GPU request
on small data and that the device actually used is recorded in the artifact, so
operators can see whether a model trained on GPU or fell back.
"""
from __future__ import annotations

import threading
from decimal import Decimal

import pytest

import kalshi_bot.crypto.services as crypto_services
from kalshi_bot.crypto.services import (
    _crypto_resolved_xgb_device,
    _fit_crypto_model_candidates,
    _fit_tree_with_device_fallback,
)


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


def test_resolved_xgb_device_honors_gpu_request_above_min_rows(monkeypatch) -> None:
    monkeypatch.setenv("CRYPTO_XGBOOST_DEVICE", "cuda")
    monkeypatch.setenv("CRYPTO_GPU_MIN_ROWS", "100")

    assert _crypto_resolved_xgb_device(100) == "cuda"
    assert _crypto_resolved_xgb_device(99) == "cpu"


def test_resolved_xgb_device_defaults_cpu(monkeypatch) -> None:
    monkeypatch.delenv("CRYPTO_XGBOOST_DEVICE", raising=False)

    assert _crypto_resolved_xgb_device(10**6) == "cpu"


def test_device_fallback_retries_gpu_failure_on_cpu() -> None:
    calls: list[str] = []

    def build_and_fit(device: str) -> str:
        calls.append(device)
        if device == "cuda":
            raise RuntimeError("no cuda runtime")
        return "fitted"

    fitted, device = _fit_tree_with_device_fallback(build_and_fit, "cuda", model_label="xgboost")

    assert (fitted, device) == ("fitted", "cpu")
    assert calls == ["cuda", "cpu"]


def test_device_fallback_does_not_mask_cpu_failures() -> None:
    def build_and_fit(device: str) -> str:
        raise RuntimeError("bad data")

    with pytest.raises(RuntimeError, match="bad data"):
        _fit_tree_with_device_fallback(build_and_fit, "cpu", model_label="xgboost")


def test_gpu_path_overlaps_xgboost_with_cpu_fits(monkeypatch) -> None:
    fit_threads: dict[str, str] = {}

    def _stub(name: str) -> dict:
        fit_threads[name] = threading.current_thread().name
        return {"name": name, "status": "available", "model": {"model_type": name}, "dependency_version": None}

    monkeypatch.setattr(crypto_services, "_crypto_resolved_xgb_device", lambda n_rows: "cuda")
    monkeypatch.setattr(crypto_services, "_fit_crypto_logistic_model", lambda *a, **k: _stub("sklearn_logistic"))
    monkeypatch.setattr(crypto_services, "_fit_crypto_xgboost_model", lambda *a, **k: _stub("xgboost_classifier"))
    monkeypatch.setattr(crypto_services, "_fit_crypto_lightgbm_model", lambda *a, **k: _stub("lightgbm_classifier"))

    result = _fit_crypto_model_candidates(_rows())

    main_thread = threading.current_thread().name
    assert fit_threads["xgboost_classifier"].startswith("crypto-xgb-fit")
    assert fit_threads["sklearn_logistic"] == main_thread
    assert fit_threads["lightgbm_classifier"] == main_thread
    for name in ("sklearn_logistic", "xgboost_classifier", "lightgbm_classifier"):
        assert result[name]["name"] == name


def test_parallel_and_sequential_paths_produce_identical_models(monkeypatch) -> None:
    monkeypatch.delenv("CRYPTO_XGBOOST_DEVICE", raising=False)
    monkeypatch.delenv("CRYPTO_LIGHTGBM_DEVICE", raising=False)
    rows = _rows(48)

    sequential = _fit_crypto_model_candidates(rows)
    # Force the overlap branch while the fits themselves stay on CPU: the
    # orchestration must not change any artifact.
    monkeypatch.setattr(crypto_services, "_crypto_resolved_xgb_device", lambda n_rows: "cuda")
    parallel = _fit_crypto_model_candidates(rows)

    for name in ("sklearn_logistic", "xgboost_classifier", "lightgbm_classifier"):
        assert parallel[name]["status"] == "available", parallel[name].get("reason")
        assert parallel[name]["model"] == sequential[name]["model"]
