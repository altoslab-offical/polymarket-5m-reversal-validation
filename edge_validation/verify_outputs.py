#!/usr/bin/env python3
"""
Independent verification suite for Phase A deliverables:
1. Unit tests on Wilson CI, bootstrap CI, shared state machine cooldown/bust logic, and PnL formulas.
2. Post-run audit of api_notes.md, windows.csv, and summary.md completeness and invariants.
"""
from __future__ import annotations

import pathlib
import sys
import numpy as np
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent
PROJECT_ROOT = ROOT.parent
sys.path.insert(0, str(ROOT))

from build_windows_and_summary import wilson_ci, bootstrap_mean_ci, run_state_machine, compute_pnl_columns


def test_math_and_state_machine() -> None:
    # 1. Wilson CI sanity: 60 / 100 -> center ~ 0.60, symmetric-ish [0.502, 0.691]
    p, lo, hi = wilson_ci(60, 100)
    assert abs(p - 0.60) < 1e-12
    assert 0.50 < lo < 0.51 and 0.68 < hi < 0.70

    # 2. Bootstrap deterministic & monotonic with mean
    rng_vals = np.array([0.46, -0.54] * 25 + [0.46] * 50, dtype=np.float64)
    m, blo, bhi = bootstrap_mean_ci(rng_vals, n_boot=2000, seed=42)
    assert abs(m - np.mean(rng_vals)) < 1e-12
    assert blo < m < bhi

    # 3. State machine: loss triggers 2-bar cooldown ONLY on losing coin, increments round; round 6 loss resets & busts
    # Create 7 windows T = 0, 300, ..., 1800
    rows = []
    for k in range(7):
        t = k * 300
        for c in ("BTC", "ETH", "SOL"):
            sig = 1 if (c == "BTC" or (k in (1, 2) and c == "ETH")) else 0
            rsi = 20.0 if c == "BTC" else 22.0  # BTC has higher |rsi7 - 50| = 30 vs ETH = 28
            out = "Down"  # Force every trade to LOSE so BTC hits cooldown at k=1,2 and ETH steps in
            rows.append({
                "coin": c, "T": t, "rsi7": rsi, "sig_dir": sig, "sig_dir_core": sig,
                "pm_outcome": out, "up_vwap_pre30": 0.50, "down_vwap_pre30": 0.50,
                "up_vwap_0_30": 0.50, "down_vwap_0_30": 0.50,
                "up_first_px": 0.50, "down_first_px": 0.50,
                "up_vwap_0_60": 0.50, "down_vwap_0_60": 0.50,
                "taker_fee_rate": 0.07,
            })
    df = pd.DataFrame(rows)
    sm = run_state_machine(df)
    sel = sm[sm["sm_selected"] == 1].sort_values("T")
    # k=0: BTC loses at round 1 -> cooldown BTC for k=1,2!
    # k=1,2: BTC is blocked, ETH selected at round 2 (losses -> ETH cooled for k=2,3) -> at k=2 both BTC and ETH blocked!
    assert list(zip(sel["T"], sel["coin"], sel["sm_round"])) == [
        (0, "BTC", 1),
        (300, "ETH", 2),
        (900, "BTC", 3),
        (1200, "BTC", 4),
    ] or len(sel) >= 3  # Check k=1200 blocked too: loss at 900 cools BTC at 1200 & 1500 -> next BTC is T=1800!
    assert list(zip(sel["T"], sel["coin"], sel["sm_round"])) == [
        (0, "BTC", 1),
        (300, "ETH", 2),
        (900, "BTC", 3),
        (1800, "BTC", 4),
    ], f"Got {list(zip(sel['T'], sel['coin'], sel['sm_round']))}"

    # 4. Fee formula check: p=0.50, rate=0.07 -> cost = 0.50 + 0.07 * 0.25 = 0.5175; win -> pnl = +0.4825
    pnl_df = compute_pnl_columns(pd.DataFrame([{
        "sig_dir_core": 1, "pm_outcome": "Up",
        "up_vwap_pre30": 0.50, "down_vwap_pre30": 0.50,
        "up_vwap_0_30": np.nan, "down_vwap_0_30": np.nan,
        "up_first_px": np.nan, "down_first_px": np.nan,
        "up_vwap_0_60": 0.55, "down_vwap_0_60": 0.45,
        "taker_fee_rate": 0.07,
    }]))
    assert abs(float(pnl_df["cost_main"].iloc[0]) - 0.5175) < 1e-12
    assert abs(float(pnl_df["pnl_main"].iloc[0]) - 0.4825) < 1e-12
    assert abs(float(pnl_df["pnl_assumed054"].iloc[0]) - 0.46) < 1e-12
    print("[Unit Tests] ALL PASSED")


def verify_deliverables() -> None:
    api_md = PROJECT_ROOT / "api_notes.md"
    win_csv = PROJECT_ROOT / "windows.csv"
    sum_md = PROJECT_ROOT / "summary.md"

    assert api_md.exists() and api_md.stat().st_size > 2000, "api_notes.md missing or too small"
    if not win_csv.exists() or not sum_md.exists():
        print("[Post-Run Audit] SKIPPED (windows.csv / summary.md not yet generated)")
        return

    df = pd.read_csv(win_csv, keep_default_na=True, na_values=["NA"], low_memory=False)
    assert len(df) == 83_019, f"Expected 83,019 rows, got {len(df)}"
    assert set(df["coin"].unique()) == {"BTC", "ETH", "SOL"}
    assert int(df["T"].min()) == 1786665600 and int(df["T"].max()) == 1794967200
    assert (df["regime"] == "twap_60s").all()
    assert set(df["split"].unique()) == {"IS", "OOS"}

    # State machine: at most 1 selected coin per timestamp T
    sel_per_t = df.groupby("T")["sm_selected"].sum()
    assert (sel_per_t <= 1).all(), "Multiple coins selected at same T!"

    # Verify required tables T1..T8 and verdict exist in summary.md
    txt = sum_md.read_text(encoding="utf-8")
    for hdr in ("## T1.", "## T2.", "## T3.", "## T4.", "## T5.", "## T6.", "## T7.", "## T8.", "最終判定"):
        assert hdr in txt, f"Missing section {hdr} in summary.md"
    print(f"[Post-Run Audit] ALL PASSED (windows.csv rows={len(df):,}, cols={df.shape[1]})")


if __name__ == "__main__":
    test_math_and_state_machine()
    verify_deliverables()
