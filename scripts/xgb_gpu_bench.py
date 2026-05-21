"""Benchmark XGBoost cuda vs cpu at the dimensions our crypto models actually use.

Mirrors _fit_crypto_xgboost_model hyperparameters (80 trees, depth 3, ~20 feats).
Verifies device="cuda" works on this host and shows whether GPU is worth it
for the small datasets the 15m / 1h crypto models train on.
"""
import time
import numpy as np
import xgboost as xgb

print(f"xgboost {xgb.__version__}")

def make(n, d=20, seed=17):
    rng = np.random.default_rng(seed)
    X = rng.standard_normal((n, d)).astype(np.float32)
    w = rng.standard_normal(d)
    logits = X @ w + 0.3 * rng.standard_normal(n)
    y = (logits > np.median(logits)).astype(int)
    return X, y

def fit(device, X, y, reps=5):
    # warm up (first cuda call pays context-init cost we don't want to measure)
    xgb.XGBClassifier(n_estimators=80, max_depth=3, learning_rate=0.05,
                      subsample=0.9, colsample_bytree=0.9, eval_metric="logloss",
                      random_state=17, n_jobs=1, tree_method="hist",
                      device=device).fit(X, y)
    t = []
    for _ in range(reps):
        t0 = time.perf_counter()
        xgb.XGBClassifier(n_estimators=80, max_depth=3, learning_rate=0.05,
                          subsample=0.9, colsample_bytree=0.9, eval_metric="logloss",
                          random_state=17, n_jobs=1, tree_method="hist",
                          device=device).fit(X, y)
        t.append(time.perf_counter() - t0)
    return min(t)  # best of N — least noisy

print(f"{'rows':>8} | {'cpu (ms)':>10} | {'cuda (ms)':>10} | winner")
print("-" * 48)
for n in (250, 1000, 5000, 20000, 100000):
    X, y = make(n)
    cpu = fit("cpu", X, y)
    try:
        cuda = fit("cuda", X, y)
        win = "cuda" if cuda < cpu else "cpu"
        print(f"{n:>8} | {cpu*1e3:>10.1f} | {cuda*1e3:>10.1f} | {win} ({max(cpu,cuda)/min(cpu,cuda):.1f}x)")
    except Exception as exc:
        print(f"{n:>8} | {cpu*1e3:>10.1f} | {'FAILED':>10} | {type(exc).__name__}: {str(exc)[:60]}")
