"""Per-frequency fit-row cap (Leg 2b): bounds the 1h candidate-fit sample so
the trainer stays inside its 32g cgroup; 15m is unaffected."""
from types import SimpleNamespace

from kalshi_bot.crypto.services import _crypto_train_fit_row_limit


def _s(base=500_000, cap_1h=150_000):
    return SimpleNamespace(crypto_train_max_snapshots=base,
                           crypto_train_max_fit_rows_1h=cap_1h)


def test_1h_capped_15m_not():
    assert _crypto_train_fit_row_limit(_s(), "1h") == 150_000
    assert _crypto_train_fit_row_limit(_s(), "15m") == 500_000


def test_cap_never_raises_above_base():
    assert _crypto_train_fit_row_limit(_s(base=100_000), "1h") == 100_000


def test_none_disables_cap():
    assert _crypto_train_fit_row_limit(_s(cap_1h=None), "1h") == 500_000
