#!/usr/bin/env python3
"""Unit tests for `edge_validation/btc_15m_validator.py`."""
from __future__ import annotations

import pathlib
import sys

import numpy as np
import pandas as pd

REPO = pathlib.Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from edge_validation.btc_15m_validator import (
    _book_depth_metrics,
    check_30d_15m_structural_persistence,
    evaluate_15m_window_signal,
    run_dual_engine_depth_mc,
)


def test_book_depth_metrics_and_15m_signal_math() -> None:
    """Verify +1c/+2c/+3c USD depth and causal 15m TWAP pricing + 5% fee deduction."""
    asks = [(0.75, 100.0), (0.76, 200.0), (0.77, 300.0), (0.79, 500.0)]
    bids = [(0.74, 150.0), (0.72, 100.0)]
    dm = _book_depth_metrics(asks, bids)
    assert abs(dm["spread"] - 0.01) < 1e-6
    assert abs(dm["ask_d2_usd"] - (75.0 + 152.0 + 231.0)) < 1e-4

    w = 1795080600
    bn_1m = {w - k * 60: (64000.0, 64000.0 + (2.0 if k % 2 == 0 else -1.5)) for k in range(1, 35)}
    bn_1m[w] = (64000.0, 64010.0)
    for m_off in range(1, 15):
        bn_1m[w + m_off * 60] = (64050.0, 64110.0)
    hist = [{"t": w + m_off * 60 + 30, "p": 0.76} for m_off in range(10, 14)]
    mkt = {"window_ts": w, "slug": f"btc-updown-15m-{w}", "volume_usd": 12000.0, "resolved_outcome": "UP", "history": hist}

    sig = evaluate_15m_window_signal(mkt, bn_1m, (12, 13, 14), max_ask=0.82, spread_pad=0.01)
    assert sig is not None
    assert sig["side"] == "UP"
    assert sig["ask_proxy"] == 0.77
    assert sig["won"] is True
    assert sig["pnl_per_share"] > 0.20


def test_30d_structural_and_depth_mc_invariants(tmp_path: pathlib.Path) -> None:
    """Verify offline 15m structural persistence calculation and monotone MC win-rate ordering."""
    base_ms = 1790000100_000  # divisible by 900_000
    rows = []
    px = 60000.0
    for i in range(60 * 3):
        open_p = px
        close_p = px + (30.0 if (i % 6 < 3) else -2.0)
        rows.append({"open_time": base_ms + i * 300_000, "open": open_p, "high": max(open_p, close_p) + 1, "low": min(open_p, close_p) - 1, "close": close_p})
        px = close_p
    pq = tmp_path / "btc_syn.parquet"
    pd.DataFrame(rows).to_parquet(pq)
    st = check_30d_15m_structural_persistence(pq, days=1)
    assert st["signals"] >= 1
    assert 0.0 <= float(st["wr_pct"]) <= 100.0

    mc_low = run_dual_engine_depth_mc("low", 0.80, 20.0, n_paths=500, seed=7)
    mc_high = run_dual_engine_depth_mc("high", 0.87, 25.0, n_paths=500, seed=7)
    assert mc_high["p_10k_180d_pct"] >= mc_low["p_10k_180d_pct"]
    assert np.isfinite(mc_high["p95_mdd_pct"])
