#!/usr/bin/env python3
"""Unit and parity tests for `edge_validation/exploration_grid.py`."""
from __future__ import annotations

from dataclasses import asdict
import pathlib
import sys
from types import SimpleNamespace
from typing import Any, Dict, List, Tuple

import pandas as pd

REPO = pathlib.Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from edge_validation.exploration_grid import (
    DEFAULT_FEE_MAP,
    CausalAuxTrackers,
    GridVariant,
    _eval_shared_fv,
    _eval_variant_symbol,
    _variant_cfg_for,
    build_e4_diff_table,
    build_grid_variants,
    compute_base_min_net_ev,
    compute_paired_stats,
    simulate_grid,
    summarize_variant,
    verify_all_primary_parity,
    verify_base_recorder_parity,
)
from edge_validation.live_recorder_42 import NOV21_UTC_TS
from edge_validation.run_daily_bundle import evaluate_day10_early_stop
from engine.oracle_sniper import TWAP_V2_FILTERED_CONFIG, taker_fee_per_share
from engine.signal_core import MarketInputs, evaluate as evaluate_signal


def _make_hist(base: float = 100.0, n: int = 1805) -> tuple[float, ...]:
    """Deterministic slightly volatile 1s price history passing sigma floor cleanly."""
    vals: List[float] = []
    cur = base
    for i in range(n):
        step = 0.004 if (i % 2 == 0) else -0.002
        cur = cur + step
        vals.append(round(cur, 6))
    return tuple(vals)


def _make_inp(
    symbol: str = "BTCUSDT",
    window_ts: int = 1795220100,
    elapsed: int = 210,
    cl_spot: float = 100.048,
    bn_px: float = 100.048,
    up_ask: float = 0.72,
    up_bid: float = 0.71,
) -> MarketInputs:
    ages = {"cl_twap": 0.4, "cl_spot": 0.4, "bn": 0.2, "book": 0.2}
    return MarketInputs(
        symbol=symbol, window_ts=window_ts, now_ts=float(window_ts + elapsed),
        strike_twap=100.0, cl_twap=cl_spot, cl_spot=cl_spot, bn_close_1s=bn_px,
        vol_hist=_make_hist(),
        up_asks=[(up_ask, 100.0)], down_asks=[(0.29, 100.0)],
        up_bids=[(up_bid, 100.0)], down_bids=[(0.28, 100.0)],
        ages=ages, orderflow_score=0.25, cvd_imbalance=0.20, oracle_gap_bps=0.0,
        realized_twap_240=(None, 0.0),
    )


def test_24_variants_and_base_present() -> None:
    """Ensure exact count (1 BASE + 24 variants) and unique variant codes."""
    variants = build_grid_variants()
    assert len(variants) == 25
    codes = [v.code for v in variants]
    assert codes[0] == "BASE"
    assert len(set(codes)) == 25
    expected = {
        "BASE", "A1", "A2", "A3", "A4", "B1", "B2", "B3", "C1", "C2", "D1", "D2",
        "E1", "E2", "E3", "E4", "F1", "F2", "F3", "G1", "G2", "G3", "G4", "H1", "H2",
    }
    assert set(codes) == expected


def test_e4_blocks_lagged_chainlink_when_binance_reversed() -> None:
    """E4 uses causal 60s basis-adjusted Binance price to block lagged Chainlink signals."""
    by_code = {v.code: v for v in build_grid_variants()}
    aux = CausalAuxTrackers()
    w = 1795220100
    # Warm 60s basis with bn - cl = +0.02 consistently
    for sec in range(w + 150, w + 210):
        inp_w = _make_inp(window_ts=w, elapsed=sec - w, cl_spot=100.048, bn_px=100.068)
        aux.update("BTCUSDT", sec, inp_w, sigma=0.000035)

    # At t = w+210: Chainlink still stale-high at 100.048, while Binance already dumped to 100.02!
    inp_lag = _make_inp(window_ts=w, elapsed=210, cl_spot=100.048, bn_px=100.02, up_ask=0.72, up_bid=0.71)
    shared = _eval_shared_fv({"BTCUSDT": inp_lag}, need_fv=True)
    cfg = _variant_cfg_for(by_code["BASE"], 0.05)

    dec_base = _eval_variant_symbol(by_code["BASE"], inp_lag, cfg, shared["BTCUSDT"], aux)
    dec_e4 = _eval_variant_symbol(by_code["E4"], inp_lag, cfg, shared["BTCUSDT"], aux)
    assert dec_base.action == "ENTER"
    assert dec_e4.action == "SKIP" and dec_e4.reason in ("E4_BN_Z_LOW", "PROB_LOW", "EV_LOW")


def test_e_g_h_gates_behave_deterministically() -> None:
    """Verify E1/E2/E3 reverse filters, G1 symbol filter, H2 spread filter, and H1 top-10% sigma filter."""
    by_code = {v.code: v for v in build_grid_variants()}
    aux = CausalAuxTrackers()
    w = 1795220400
    # Populate past 2000 sigma samples (low = 0.00004, 10% = 0.00010) strictly before entry tick
    for i in range(2000):
        sig_v = 0.00004 if i < 1800 else 0.00012
        aux.sigma_24h["SOLUSDT"].append((w - 2100 + i, sig_v))
    # Populate BN history 2s, 3s, 5s prior to tick t_now = w + 210
    t_now = w + 210
    aux.bn_hist["SOLUSDT"][t_now - 2] = 100.08  # 2s ago higher -> adverse for UP
    aux.bn_hist["SOLUSDT"][t_now - 3] = 100.06  # 3s ago slightly aligned (<0.5 sigma)
    aux.bn_hist["SOLUSDT"][t_now - 5] = 100.02  # 5s ago lower -> aligned for UP

    inp_sol = _make_inp(symbol="SOLUSDT", window_ts=w, elapsed=210, cl_spot=100.048, bn_px=100.068, up_ask=0.74, up_bid=0.71)
    shared = _eval_shared_fv({"SOLUSDT": inp_sol}, need_fv=True)
    # G1 filters out SOL
    d_g1 = _eval_variant_symbol(by_code["G1"], inp_sol, _variant_cfg_for(by_code["G1"], 0.01), shared["SOLUSDT"], aux)
    assert d_g1.action == "SKIP" and d_g1.reason == "SYMBOL_FILTERED"
    # H2 blocks 3c spread (0.74 - 0.71)
    d_h2 = _eval_variant_symbol(by_code["H2"], inp_sol, _variant_cfg_for(by_code["H2"], 0.01), shared["SOLUSDT"], aux)
    assert d_h2.action == "SKIP" and d_h2.reason == "SPREAD_WIDE"
    # E1 (2s adverse) blocks 100.068 < 100.08; E2 (5s aligned) allows 100.068 > 100.02
    d_e1 = _eval_variant_symbol(by_code["E1"], inp_sol, _variant_cfg_for(by_code["E1"], 0.01), shared["SOLUSDT"], aux)
    d_e2 = _eval_variant_symbol(by_code["E2"], inp_sol, _variant_cfg_for(by_code["E2"], 0.01), shared["SOLUSDT"], aux)
    assert d_e1.action == "SKIP" and d_e1.reason == "BN_ADVERSE_2S"
    assert d_e2.action == "ENTER"
    # E3 threshold is 0.5*sigma*sqrt(3); large reverse triggers BN_ADVERSE_3S
    aux.bn_hist["SOLUSDT"][t_now - 3] = 100.30
    d_e3 = _eval_variant_symbol(by_code["E3"], inp_sol, _variant_cfg_for(by_code["E3"], 0.01), shared["SOLUSDT"], aux)
    assert d_e3.action == "SKIP" and d_e3.reason == "BN_ADVERSE_3S"
    # H1 blocks high sigma (force high sigma in shared tuple)
    fv_only = shared["SOLUSDT"][1]
    d_h1 = _eval_variant_symbol(by_code["H1"], inp_sol, _variant_cfg_for(by_code["H1"], 0.01), (0.00015, fv_only, None), aux)
    assert d_h1.action == "SKIP" and d_h1.reason == "HIGH_SIGMA_TOP10"


def _build_synthetic_windows() -> Tuple[List[int], Dict[Any, pd.DataFrame], Dict[Tuple[int, str], Any]]:
    """Construct 8 clean 5m synthetic windows (~1800s warmup + 2 active windows) with full books."""
    import json
    base_w = 1795219200
    n_wins = 8
    settled_ts = [base_w + i * 300 for i in range(n_wins)]
    snap_rows: List[dict[str, Any]] = []
    w_meta: Dict[Tuple[int, str], Any] = {}
    syms = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    for w_idx, w in enumerate(settled_ts):
        for sym in syms:
            strike = 100.0 if sym != "ETHUSDT" else 2000.0
            out = "UP" if (w_idx != 7) else "DOWN"
            w_meta[(w, sym)] = SimpleNamespace(window_ts=w, symbol=sym, twap_strike=strike, resolved_outcome=out)
            for el in range(300):
                t = w + el
                drift = 0.0012 * strike if (w_idx >= 6 and el >= 180) else 0.0002 * ((el % 5) - 2)
                if w_idx == 7 and el >= 235:
                    drift = -0.0018 * strike  # reversal in last window to exercise SL / F1 / F2 exits
                px = strike + drift + (0.0001 * strike if (el % 2 == 0) else -0.00005 * strike)
                u_ask, d_ask = (0.72, 0.29) if drift > 0.0008 * strike else (0.52, 0.49)
                u_bid, d_bid = round(u_ask - 0.01, 2), round(d_ask - 0.01, 2)
                if w_idx == 7 and el >= 235:
                    u_bid, u_ask = 0.30, 0.32
                snap_rows.append({
                    "ts_sec": t, "window_ts": w, "symbol": sym, "elapsed_sec": el,
                    "cl_spot": px, "cl_twap60": px, "bn_close": px,
                    "bn_vol": 2.0, "bn_buy_vol": 1.4 if drift >= 0 else 0.3,
                    "bn_sell_vol": 0.6 if drift >= 0 else 1.7, "bn_trades": 3,
                    "up_bids_json": json.dumps([[u_bid, 100.0]]), "up_asks_json": json.dumps([[u_ask, 100.0]]),
                    "down_bids_json": json.dumps([[d_bid, 100.0]]), "down_asks_json": json.dumps([[d_ask, 100.0]]),
                    "up_bid1": u_bid, "up_bid1_sz": 100.0, "up_ask1": u_ask, "up_ask1_sz": 100.0,
                    "down_bid1": d_bid, "down_bid1_sz": 100.0, "down_ask1": d_ask, "down_ask1_sz": 100.0,
                    "cl_twap_age_sec": 0.4, "cl_spot_age_sec": 0.4, "bn_age_sec": 0.2,
                    "up_book_age_sec": 0.2, "down_book_age_sec": 0.2,
                    "cl_twap_age_ms": 400.0, "cl_spot_age_ms": 400.0, "bn_age_ms": 200.0,
                    "up_book_age_ms": 200.0, "down_book_age_ms": 200.0,
                    "cl_twap_lag_ms": 500.0, "cl_spot_lag_ms": 500.0, "bn_lag_ms": 20.0,
                    "up_book_lag_ms": 20.0, "down_book_lag_ms": 20.0,
                })
    df_s = pd.DataFrame(snap_rows)
    snap_by_wt_sym = {k: g.sort_values("elapsed_sec") for k, g in df_s.groupby(["window_ts", "symbol"])}
    return settled_ts, snap_by_wt_sym, w_meta


def test_base_100pct_parity_with_live_recorder() -> None:
    """All 3 primary models (v2_filtered, v2_pure, v1) must match live_recorder_42 100.0%."""
    settled_ts, snap_by_wt_sym, w_meta = _build_synthetic_windows()
    variants = build_grid_variants()
    dfs = simulate_grid(variants, DEFAULT_FEE_MAP, settled_ts, snap_by_wt_sym, w_meta)
    parity = verify_base_recorder_parity(dfs["BASE"], DEFAULT_FEE_MAP, settled_ts, snap_by_wt_sym, w_meta)
    assert parity["frozen_ok"] is True
    assert parity["parity_pct"] == 100.0, parity
    all_p = verify_all_primary_parity(dfs["BASE"], DEFAULT_FEE_MAP, settled_ts, snap_by_wt_sym, w_meta)
    for model_name in ("v2_filtered", "v2_pure", "v1"):
        assert all_p[model_name]["frozen_ok"] is True
        assert all_p[model_name]["parity_pct"] == 100.0, (model_name, all_p[model_name])
    assert dfs["F1"]["sl_triggered"].sum() == 0 if len(dfs["F1"]) else True


def test_e4_diff_table_captures_filter_and_delta() -> None:
    """E4 vs BASE diff forensics table highlights filtered windows and PnL delta cleanly."""
    df_b = pd.DataFrame([
        {"window_ts": 100, "symbol": "BTCUSDT", "side": "UP", "elapsed_sec": 210, "filled_1s": True, "miss_reason": "", "resolved_outcome": "UP", "pnl_per_share": 0.20},
        {"window_ts": 400, "symbol": "ETHUSDT", "side": "UP", "elapsed_sec": 215, "filled_1s": True, "miss_reason": "", "resolved_outcome": "DOWN", "pnl_per_share": -0.75},
    ])
    df_e = pd.DataFrame([
        {"window_ts": 100, "symbol": "BTCUSDT", "side": "UP", "elapsed_sec": 210, "filled_1s": True, "miss_reason": "", "resolved_outcome": "UP", "pnl_per_share": 0.20},
    ])
    diffs = build_e4_diff_table(df_b, df_e)
    assert len(diffs) == 1
    assert diffs[0]["window_ts"] == 400
    assert diffs[0]["e4_status"] == "SKIPPED"
    assert abs(diffs[0]["pnl_delta"] - 0.75) < 1e-6


def test_paired_stats_and_194se_math() -> None:
    """Verify paired difference, paired t-stat, and 1.94*SE null benchmark calculation."""
    df_b = pd.DataFrame([
        {"window_ts": 100, "filled_1s": True, "cost_1s": 0.80, "pnl_per_share": 0.20},
        {"window_ts": 400, "filled_1s": True, "cost_1s": 0.80, "pnl_per_share": -0.50},
        {"window_ts": 700, "filled_1s": True, "cost_1s": 0.80, "pnl_per_share": 0.20},
    ])
    df_v = pd.DataFrame([
        {"window_ts": 100, "filled_1s": True, "cost_1s": 0.78, "pnl_per_share": 0.22},
        {"window_ts": 400, "filled_1s": True, "cost_1s": 0.75, "pnl_per_share": -0.45},
        {"window_ts": 700, "filled_1s": True, "cost_1s": 0.79, "pnl_per_share": 0.21},
    ])
    p_st = compute_paired_stats(df_v, df_b)
    # Diffs = [+0.02, +0.05, +0.01] -> mean = 0.0266667, std(ddof=1) = 0.0208167, se = 0.0120185
    assert p_st["n_paired"] == 3
    assert abs(p_st["mean_diff"] - 0.02667) < 1e-4
    assert abs(p_st["paired_t"] - 2.219) < 0.02

    v = GridVariant("TEST", "T", "desc")
    sm = summarize_variant(v, df_v, df_b, span_days=1.0)
    assert abs(sm["null_194se"] - 1.94 * sm["se_pnl_sh"]) < 1e-4


def test_f2_annotation_and_base_min_net_ev() -> None:
    """Verify `F2` carries duplicate tag and `compute_base_min_net_ev` handles empty & mixed fills."""
    by_code = {v.code: v for v in build_grid_variants()}
    assert "與 BASE 相同" in by_code["F2"].description
    empty_res = compute_base_min_net_ev(pd.DataFrame())
    assert empty_res["n_all"] == 0 and empty_res["min_net_ev_all"] is None and empty_res["min_net_ev_filled"] is None

    df_b = pd.DataFrame([
        {"window_ts": 100, "symbol": "BTCUSDT", "side": "UP", "ask_t0": 0.82, "entry_p_true": 0.901, "entry_z": 1.30, "fee_rate": 0.05, "filled_1s": False},
        {"window_ts": 400, "symbol": "ETHUSDT", "side": "DOWN", "ask_t0": 0.80, "entry_p_true": 0.910, "entry_z": 1.35, "fee_rate": 0.01, "filled_1s": True},
    ])
    ev_res = compute_base_min_net_ev(df_b)
    exp_all = round(0.901 - (0.82 + taker_fee_per_share(0.82, 0.05)), 6)
    exp_fill = round(0.910 - (0.80 + taker_fee_per_share(0.80, 0.01)), 6)
    assert ev_res["n_all"] == 2 and ev_res["n_filled"] == 1
    assert abs(ev_res["min_net_ev_all"] - exp_all) < 1e-6
    assert abs(ev_res["min_net_ev_filled"] - exp_fill) < 1e-6


def test_day10_early_stop_fail_only_rule() -> None:
    """Verify Day-10 check triggers fail-only stop strictly when all 3 `CI_hi < +2c` after Day 10."""
    low_cis = {"post_nov21": {
        "v2_pure": {"filled_1s": 120, "mean_pnl_1s": 0.005, "ci98_3_bonferroni_1s": [-0.02, 0.019]},
        "v2_filtered": {"filled_1s": 90, "mean_pnl_1s": 0.001, "ci98_3_bonferroni_1s": [-0.03, 0.015]},
        "v1": {"filled_1s": 30, "mean_pnl_1s": -0.01, "ci98_3_bonferroni_1s": [-0.05, 0.018]},
    }}
    pre = evaluate_day10_early_stop(low_cis, now_ts=NOV21_UTC_TS + 9.5 * 86400)
    assert pre["status"] == "PRE_DAY10"
    stop = evaluate_day10_early_stop(low_cis, now_ts=NOV21_UTC_TS + 10.1 * 86400)
    assert stop["status"] == "EARLY_STOP_INSUFFICIENT_EDGE"

    one_strong = {"post_nov21": {
        **low_cis["post_nov21"],
        "v2_filtered": {"filled_1s": 100, "mean_pnl_1s": 0.12, "ci98_3_bonferroni_1s": [0.05, 0.20]},
    }}
    cont = evaluate_day10_early_stop(one_strong, now_ts=NOV21_UTC_TS + 10.5 * 86400)
    assert cont["status"] == "CONTINUE_TO_300"

    missing = {"post_nov21": {"v2_pure": low_cis["post_nov21"]["v2_pure"], "v2_filtered": {}, "v1": low_cis["post_nov21"]["v1"]}}
    cont_missing = evaluate_day10_early_stop(missing, now_ts=NOV21_UTC_TS + 11.0 * 86400)
    assert cont_missing["status"] == "CONTINUE_TO_300"
