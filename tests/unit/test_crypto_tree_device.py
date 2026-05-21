"""Tests for GPU/CPU device resolution + fallback for crypto tree models.

Covers the size-aware device choice (GPU only pays off above a row threshold on
this hardware) and the logged CPU fallback that keeps a candidate from being
silently dropped when CUDA is unavailable or OOMs.
"""
from __future__ import annotations

import pytest

from kalshi_bot.crypto.services import (
    _fit_tree_with_device_fallback,
    _resolve_tree_device,
)


# --- _resolve_tree_device: size-aware GPU gating ---------------------------

def test_cpu_request_stays_cpu() -> None:
    device, downgrade = _resolve_tree_device("cpu", n_rows=1_000_000, gpu_min_rows=20_000)
    assert device == "cpu"
    assert downgrade is None


def test_gpu_kept_when_rows_meet_threshold() -> None:
    device, downgrade = _resolve_tree_device("cuda", n_rows=20_000, gpu_min_rows=20_000)
    assert device == "cuda"
    assert downgrade is None


def test_gpu_downgraded_to_cpu_below_threshold() -> None:
    device, downgrade = _resolve_tree_device("cuda", n_rows=500, gpu_min_rows=20_000)
    assert device == "cpu"
    assert downgrade is not None  # reason recorded for observability


def test_threshold_disabled_honors_gpu_at_any_size() -> None:
    # gpu_min_rows<=0 means "no size gate" — used by LightGBM, which has no
    # crossover where GPU wins, so an explicit operator request is honored as-is.
    device, downgrade = _resolve_tree_device("gpu", n_rows=10, gpu_min_rows=0)
    assert device == "gpu"
    assert downgrade is None


def test_blank_request_defaults_to_cpu() -> None:
    device, downgrade = _resolve_tree_device("   ", n_rows=50_000, gpu_min_rows=20_000)
    assert device == "cpu"
    assert downgrade is None


# --- _fit_tree_with_device_fallback: never silently drop the model ---------

def test_returns_result_and_device_on_success() -> None:
    result, used = _fit_tree_with_device_fallback(
        lambda dev: f"fit-on-{dev}", "cuda", model_label="xgboost"
    )
    assert result == "fit-on-cuda"
    assert used == "cuda"


def test_falls_back_to_cpu_when_gpu_fit_raises() -> None:
    calls: list[str] = []

    def build(dev: str) -> str:
        calls.append(dev)
        if dev != "cpu":
            raise RuntimeError("no CUDA device / out of memory")
        return "fit-on-cpu"

    result, used = _fit_tree_with_device_fallback(build, "cuda", model_label="xgboost")
    assert result == "fit-on-cpu"
    assert used == "cpu"
    assert calls == ["cuda", "cpu"]  # tried GPU first, then fell back


def test_does_not_retry_when_already_cpu() -> None:
    calls: list[str] = []

    def build(dev: str) -> str:
        calls.append(dev)
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        _fit_tree_with_device_fallback(build, "cpu", model_label="xgboost")
    assert calls == ["cpu"]  # no pointless retry


def test_raises_when_cpu_fallback_also_fails() -> None:
    def build(dev: str) -> str:
        raise RuntimeError(f"fail-on-{dev}")

    with pytest.raises(RuntimeError):
        _fit_tree_with_device_fallback(build, "cuda", model_label="lightgbm")
