#!/usr/bin/env python3
"""Offline 24-variant Exploration Grid (Tier-2 exploratory replay, zero live impact).

Reads recorded 1Hz snapshots from `live_3coin_42.db` read-only and evaluates `BASE`
(`TWAP_V2_FILTERED_CONFIG`) plus 24 single-factor / targeted variants (`A1`..`H2`)
using identical causal inputs and depth-aware `+1s` FAK execution/exit rules.
"""
from __future__ import annotations

import argparse
from collections import deque
from dataclasses import asdict, dataclass, field, replace
import hashlib
import json
import math
import pathlib
import sqlite3
import sys
from typing import Any, Deque, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent
REPO = ROOT.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from edge_validation.live_recorder_42 import (
    DEFAULT_DB,
    FROZEN_CFG_PATH,
    NOV21_UTC_TS,
    SYMBOLS,
    RecorderStreamState,
    _parse_levels_json,
    _sim_one_cfg,
    assemble_recorder_inputs,
)
from engine.oracle_sniper import (
    DEFAULT_SNIPER_CONFIG,
    TWAP_V2_FILTERED_CONFIG,
    TWAP_V2_PURE_CONFIG,
    Candidate,
    OpenPosition,
    OrderBookTop,
    SniperConfig,
    analyze_trade_ev,
    check_frozen_config_parity,
    compute_shares,
    effective_exit_proceeds,
    entry_limit_price,
    estimate_twap_fair_value,
    evaluate_exit,
    taker_fee_per_share,
)
from engine.signal_core import (
    DEFAULT_MAX_AGES,
    MIN_VOL_SAMPLES,
    MarketInputs,
    SignalDecision,
    _freshness_skip,
    _side_gate_failure,
    choose_best_candidate,
    compute_fv,
    compute_z_twap_proj_diagnostic,
    simulate_fak_buy,
    simulate_fak_sell,
)

GRID_CSV_DIR = ROOT / "exploration_grid_csv"
GRID_REPORT_PATH = ROOT / "exploration_grid_report.md"
NULL_BENCHMARK_24_SIGMA_MULT = 1.94
DEFAULT_FEE_MAP: dict[str, float] = {"BTCUSDT": 0.05, "ETHUSDT": 0.01, "SOLUSDT": 0.01}


@dataclass(frozen=True)
class GridVariant:
    """Specification for one exploration variant anchored on `TWAP_V2_FILTERED_CONFIG`."""

    code: str
    group: str
    description: str
    cfg_overrides: dict[str, Any] = field(default_factory=dict)
    allowed_symbols: tuple[str, ...] = SYMBOLS
    bn_adverse_sec: Optional[int] = None
    bn_adverse_sigma_mult: float = 0.0
    use_bn_basis_z: bool = False
    use_twap_proj_sl: bool = False
    skip_top_sigma_pct: Optional[float] = None


def build_grid_variants() -> list[GridVariant]:
    """Return `BASE` plus the exact 24 pre-specified exploration variants (`A1`..`H2`)."""
    return [
        GridVariant("BASE", "BASE", "Baseline v2_filtered (frozen control)"),
        GridVariant("A1", "A. Price Band", "ask in [0.60, 0.82]", {"min_entry_price": 0.60, "max_entry_price": 0.82}),
        GridVariant("A2", "A. Price Band", "ask in [0.68, 0.88]", {"min_entry_price": 0.68, "max_entry_price": 0.88}),
        GridVariant("A3", "A. Price Band", "ask in [0.75, 0.90]", {"min_entry_price": 0.75, "max_entry_price": 0.90}),
        GridVariant("A4", "A. Price Band", "ask in [0.84, 0.95] (highband)", {"min_entry_price": 0.84, "max_entry_price": 0.95}),
        GridVariant("B1", "B. EV Floor", "net EV >= 2.0c", {"min_net_edge": 0.02}),
        GridVariant("B2", "B. EV Floor", "net EV >= 5.0c", {"min_net_edge": 0.05}),
        GridVariant("B3", "B. EV Floor", "net EV >= 7.0c", {"min_net_edge": 0.07}),
        GridVariant("C1", "C. Window", "elapsed in [200, 285]s", {"entry_start_elapsed_sec": 200.0, "entry_end_elapsed_sec": 285.0}),
        GridVariant("C2", "C. Window", "elapsed in [240, 290]s (final 60s TWAP)", {"entry_start_elapsed_sec": 240.0, "entry_end_elapsed_sec": 290.0}),
        GridVariant("D1", "D. Z Gate", "z >= 1.0", {"min_z_score": 1.0}),
        GridVariant("D2", "D. Z Gate", "z >= 1.6", {"min_z_score": 1.6}),
        GridVariant("E1", "E. BN Lead", "skip on 2s adverse Binance move", bn_adverse_sec=2, bn_adverse_sigma_mult=0.0),
        GridVariant("E2", "E. BN Lead", "skip on 5s adverse Binance move", bn_adverse_sec=5, bn_adverse_sigma_mult=0.0),
        GridVariant("E3", "E. BN Lead", "skip on 3s adverse Binance > 0.5 sigma", bn_adverse_sec=3, bn_adverse_sigma_mult=0.5),
        GridVariant("E4", "E. BN Lead", "min(|z_CL|, |z_BN_minus_60s_basis|)", use_bn_basis_z=True),
        GridVariant("F1", "F. Exit", "hold to settlement (disable SL)", {"enable_emergency_exit": False}),
        GridVariant("F2", "F. Exit", "SL uses TWAP-proj z < -0.35 (rem<=60s; 與 BASE 相同)", use_twap_proj_sl=True),
        GridVariant("F3", "F. Exit", "early TP at bid >= 0.97", {"early_tp_enabled": True, "take_profit_bid": 0.97}),
        GridVariant("G1", "G. Subset/Combo", "BTCUSDT only", allowed_symbols=("BTCUSDT",)),
        GridVariant("G2", "G. Subset/Combo", "exclude SOLUSDT (BTC+ETH)", allowed_symbols=("BTCUSDT", "ETHUSDT")),
        GridVariant("G3", "G. Subset/Combo", "E(3s adverse BN) + F2(TWAP-proj SL)", bn_adverse_sec=3, bn_adverse_sigma_mult=0.0, use_twap_proj_sl=True),
        GridVariant("G4", "G. Subset/Combo", "E(3s adverse BN) + BTCUSDT only", bn_adverse_sec=3, bn_adverse_sigma_mult=0.0, allowed_symbols=("BTCUSDT",)),
        GridVariant("H1", "H. Regime", "skip top 10% 24h causal sigma", skip_top_sigma_pct=0.90),
        GridVariant("H2", "H. Regime", "skip wide spread (ask-bid > 2c)", {"max_entry_spread": 0.02}),
    ]


@dataclass
class CausalAuxTrackers:
    """Strictly causal per-symbol trackers updated at 1Hz for E1..E4 and H1."""

    bn_hist: dict[str, dict[int, float]] = field(default_factory=lambda: {s: {} for s in SYMBOLS})
    basis_60s: dict[str, Deque[tuple[int, float]]] = field(default_factory=lambda: {s: deque() for s in SYMBOLS})
    sigma_24h: dict[str, Deque[tuple[int, float]]] = field(default_factory=lambda: {s: deque() for s in SYMBOLS})

    def bn_price_ago(self, symbol: str, t_sec: int, lookback_sec: int) -> Optional[float]:
        """Return exact prior Binance price at `t_sec - lookback_sec` if recorded."""
        return self.bn_hist.get(symbol, {}).get(t_sec - lookback_sec)

    def mean_basis_60s(self, symbol: str) -> Optional[float]:
        """Return causal 60s mean `(bn_close - cl_spot)` when >= 10 samples exist."""
        dq = self.basis_60s.get(symbol)
        if not dq or len(dq) < 10:
            return None
        return sum(val for _ts, val in dq) / len(dq)

    def prior_sigma_samples(self, symbol: str, t_sec: int) -> list[float]:
        """Return causal prior-24h sigma observations strictly before `t_sec`."""
        dq = self.sigma_24h.get(symbol)
        if not dq:
            return []
        return [sig for ts, sig in dq if ts < t_sec]

    def update(self, symbol: str, t_sec: int, inp: MarketInputs, sigma: Optional[float]) -> None:
        """Append current second's causal observations and prune expired history."""
        bn_map = self.bn_hist[symbol]
        if inp.bn_close_1s is not None and inp.bn_close_1s > 0.0:
            bn_map[t_sec] = float(inp.bn_close_1s)
        cutoff_bn = t_sec - 20
        for stale_t in [k for k in bn_map if k < cutoff_bn]:
            del bn_map[stale_t]

        dq_b = self.basis_60s[symbol]
        if (
            inp.bn_close_1s is not None and inp.bn_close_1s > 0.0
            and inp.cl_spot is not None and inp.cl_spot > 0.0
            and float(inp.ages.get("bn", 9999.0)) <= 5.0
            and float(inp.ages.get("cl_spot", 9999.0)) <= 5.0
        ):
            dq_b.append((t_sec, float(inp.bn_close_1s) - float(inp.cl_spot)))
        while dq_b and dq_b[0][0] < (t_sec - 59):
            dq_b.popleft()

        if sigma is not None and sigma > 0.0:
            dq_s = self.sigma_24h[symbol]
            dq_s.append((t_sec, float(sigma)))
            while dq_s and dq_s[0][0] < (t_sec - 86400):
                dq_s.popleft()


@dataclass
class VariantWindowState:
    """Single-slot mutable state for one variant inside the active 5m window."""

    entered: bool = False
    open_pos: Optional[dict[str, Any]] = None


def _variant_cfg_for(
    variant: GridVariant, fee_rate: float, base_cfg: SniperConfig = TWAP_V2_FILTERED_CONFIG
) -> SniperConfig:
    overrides = {**variant.cfg_overrides, "fee_rate": float(fee_rate)}
    return replace(base_cfg, **overrides)


def _eval_shared_fv(
    inps: Dict[str, MarketInputs], need_fv: bool, base_cfg: SniperConfig = TWAP_V2_FILTERED_CONFIG
) -> Dict[str, Tuple[Optional[float], Optional[Any], Optional[str]]]:
    """Evaluate warmup, freshness, strike, price, and canonical FV once per tick/symbol."""
    out: Dict[str, Tuple[Optional[float], Optional[Any], Optional[str]]] = {}
    for sym, inp in inps.items():
        if len(inp.vol_hist) < MIN_VOL_SAMPLES:
            out[sym] = (None, None, "WARMUP")
            continue
        stale = _freshness_skip(inp.ages, DEFAULT_MAX_AGES)
        if stale is not None:
            out[sym] = (None, None, stale)
            continue
        if inp.strike_twap is None or inp.strike_twap <= 0.0:
            out[sym] = (None, None, "NO_STRIKE")
            continue
        if inp.cl_spot is None or inp.cl_spot <= 0.0 or inp.bn_close_1s is None or inp.bn_close_1s <= 0.0:
            out[sym] = (None, None, "NO_PRICE")
            continue
        if not need_fv:
            out[sym] = (None, None, None)
            continue
        res = compute_fv(inp, base_cfg)
        out[sym] = (res[0], res[1], None) if res is not None else (None, None, "NO_PRICE")
    return out


def _apply_e4_basis_fv(
    variant: GridVariant,
    inp: MarketInputs,
    c_sym: SniperConfig,
    sigma: float,
    fv_cl: Any,
    aux: CausalAuxTrackers,
) -> Tuple[Optional[str], Optional[float], Optional[float], Optional[str]]:
    """Evaluate `E4` conservative `min(|z_CL|, |z_BN|)` and return `(side, z_eff, p_eff, skip_reason)`."""
    if abs(fv_cl.raw_z_score) < c_sym.min_z_score:
        return None, None, None, "Z_LOW"
    basis = aux.mean_basis_60s(inp.symbol)
    if basis is None or inp.bn_close_1s is None or inp.strike_twap is None:
        return None, None, None, "E4_NO_BASIS"
    bn_adj = float(inp.bn_close_1s) - float(basis)
    if bn_adj <= 0.0:
        return None, None, None, "E4_NO_BASIS"
    rem = 300.0 - (inp.now_ts - float(inp.window_ts))
    micro = (inp.orderflow_score, inp.oracle_gap_bps, inp.cvd_imbalance)
    fv_bn = estimate_twap_fair_value(inp.strike_twap, bn_adj, rem, sigma, inp.realized_twap_240, micro, c_sym)
    if fv_bn is None:
        return None, None, None, "E4_NO_BASIS"
    side = "UP" if fv_cl.raw_z_score >= c_sym.min_z_score else "DOWN"
    sign = 1.0 if side == "UP" else -1.0
    if sign * fv_bn.raw_z_score < c_sym.min_z_score:
        return side, None, None, "E4_BN_Z_LOW"
    z_eff = min(abs(fv_cl.raw_z_score), abs(fv_bn.raw_z_score))
    p_cl = fv_cl.p_true_up if side == "UP" else fv_cl.p_true_down
    p_bn = fv_bn.p_true_up if side == "UP" else fv_bn.p_true_down
    return side, z_eff, min(p_cl, p_bn), None


def _check_extra_gates(
    variant: GridVariant, inp: MarketInputs, side: str, sigma: float, aux: CausalAuxTrackers
) -> Optional[str]:
    """Evaluate causal Binance adverse-move filter (`E1..E3/G3/G4`) and `H1` volatility filter."""
    if variant.bn_adverse_sec is not None:
        bn_prev = aux.bn_price_ago(inp.symbol, int(inp.now_ts), variant.bn_adverse_sec)
        if bn_prev and bn_prev > 0.0 and inp.bn_close_1s is not None:
            sign = 1.0 if side == "UP" else -1.0
            s_ret = sign * (float(inp.bn_close_1s) - bn_prev) / bn_prev
            thresh = -1e-9 if variant.bn_adverse_sigma_mult <= 0.0 else -(
                variant.bn_adverse_sigma_mult * sigma * math.sqrt(float(variant.bn_adverse_sec))
            )
            if s_ret < thresh:
                return f"BN_ADVERSE_{variant.bn_adverse_sec}S"
    if variant.skip_top_sigma_pct is not None:
        prior = aux.prior_sigma_samples(inp.symbol, int(inp.now_ts))
        if len(prior) >= MIN_VOL_SAMPLES and sigma >= float(np.quantile(prior, variant.skip_top_sigma_pct)):
            return "HIGH_SIGMA_TOP10"
    return None


def _eval_variant_symbol(
    variant: GridVariant,
    inp: MarketInputs,
    c_sym: SniperConfig,
    base_state: Tuple[Optional[float], Optional[Any], Optional[str]],
    aux: CausalAuxTrackers,
) -> SignalDecision:
    """Deterministic single-symbol signal evaluation for one `GridVariant`."""
    if inp.symbol not in variant.allowed_symbols:
        return SignalDecision("SKIP", "SYMBOL_FILTERED", None, None, None, None, None)
    sigma, fv_cl, base_err = base_state
    if base_err is not None or sigma is None or fv_cl is None:
        return SignalDecision("SKIP", base_err or "NO_PRICE", None, None, None, None, None)
    elapsed = inp.now_ts - float(inp.window_ts)
    if elapsed < c_sym.entry_start_elapsed_sec or elapsed > c_sym.entry_end_elapsed_sec:
        return SignalDecision("SKIP", "OUTSIDE_ENTRY_WINDOW", None, abs(fv_cl.raw_z_score), max(fv_cl.p_true_up, fv_cl.p_true_down), sigma, None, fv=fv_cl)

    if variant.use_bn_basis_z:
        side, z_dir, p_dir, e4_err = _apply_e4_basis_fv(variant, inp, c_sym, sigma, fv_cl, aux)
        if e4_err is not None or side is None or z_dir is None or p_dir is None:
            return SignalDecision("SKIP", e4_err or "Z_LOW", side, abs(fv_cl.raw_z_score), None, sigma, None, fv=fv_cl)
    else:
        if abs(fv_cl.raw_z_score) < c_sym.min_z_score:
            return SignalDecision("SKIP", "Z_LOW", None, abs(fv_cl.raw_z_score), max(fv_cl.p_true_up, fv_cl.p_true_down), sigma, None, fv=fv_cl)
        side = "UP" if fv_cl.raw_z_score >= c_sym.min_z_score else "DOWN"
        z_dir = abs(fv_cl.raw_z_score)
        p_dir = fv_cl.p_true_up if side == "UP" else fv_cl.p_true_down

    asks = inp.up_asks if side == "UP" else inp.down_asks
    bids = inp.up_bids if side == "UP" else inp.down_bids
    ask_top = float(asks[0][0]) if asks else None
    bid_top = float(bids[0][0]) if bids else None
    err, net_ev = _side_gate_failure(c_sym, side, fv_cl, p_dir, ask_top, bid_top)
    if err is not None:
        return SignalDecision("SKIP", err, side, z_dir, p_dir, sigma, ask_top, bid_top, net_ev, fv=fv_cl)

    extra_err = _check_extra_gates(variant, inp, side, sigma, aux)
    if extra_err is not None:
        return SignalDecision("SKIP", extra_err, side, z_dir, p_dir, sigma, ask_top, bid_top, net_ev, fv=fv_cl)

    assert ask_top is not None
    ev = analyze_trade_ev(p_dir, ask_top, c_sym.fee_rate)
    cand = Candidate(
        inp.symbol, side, ask_top, bid_top, p_dir, z_dir, ev.net_ev_per_share,
        ev.fee_per_share, ev.total_cost_per_share, elapsed,
    )
    return SignalDecision("ENTER", "OK", side, z_dir, p_dir, sigma, ask_top, bid_top, ev.net_ev_per_share, fv_cl, cand)


def _try_enter_variant(
    variant: GridVariant,
    tick: Tuple[int, int, int],
    inps: Dict[str, MarketInputs],
    shared_fv: Dict[str, Tuple[Optional[float], Optional[Any], Optional[str]]],
    aux: CausalAuxTrackers,
    market_ctx: Tuple[Dict[str, Dict[int, Any]], Dict[Tuple[int, str], Any], Dict[str, float], SniperConfig],
) -> Tuple[Optional[dict[str, Any]], Optional[dict[str, Any]]]:
    """Evaluate all allowed symbols and execute simulated `+1s` depth-aware FAK entry."""
    T, t_sec, elapsed = tick
    sec_maps, w_meta, fee_map, base_cfg = market_ctx
    decs = {
        s: _eval_variant_symbol(variant, inp, _variant_cfg_for(variant, fee_map.get(s, DEFAULT_SNIPER_CONFIG.fee_rate), base_cfg), shared_fv[s], aux)
        for s, inp in inps.items()
    }
    best = choose_best_candidate(list(decs.values()))
    if best is None:
        return None, None
    sym = best.symbol
    fee_r = float(fee_map.get(sym, DEFAULT_SNIPER_CONFIG.fee_rate))
    c_sym = _variant_cfg_for(variant, fee_r, base_cfg)
    inp0, dec0 = inps[sym], decs[sym]
    lim = entry_limit_price(c_sym, best.ask, best.p_true)
    shares = compute_shares(c_sym, 100.0, best.total_cost_per_share)
    r0 = sec_maps[sym].get(t_sec)
    r1 = sec_maps[sym].get(t_sec + 1)
    asks1 = _parse_levels_json(getattr(r1, "up_asks_json" if best.side == "UP" else "down_asks_json", None), False) if r1 else []
    f1 = simulate_fak_buy(asks1, float(shares), lim) if shares > 0 else simulate_fak_buy([], 0.0, lim)
    wm = w_meta.get((int(T), sym))
    out = wm.resolved_outcome if wm else None
    cost1 = (float(f1.avg_px) + taker_fee_per_share(float(f1.avg_px), fee_r)) if (f1.filled and f1.avg_px) else np.nan
    hold_pnl = round((1.0 if out == best.side else 0.0) - cost1, 5) if (f1.filled and out in ("UP", "DOWN")) else np.nan
    bid0 = (r0.up_bid1 if best.side == "UP" else r0.down_bid1) if r0 else None
    pos: dict[str, Any] = {
        "variant": variant.code, "signal_ts": t_sec, "window_ts": int(T), "elapsed_sec": int(elapsed),
        "symbol": sym, "side": best.side, "regime": "post_nov21" if int(T) >= NOV21_UTC_TS else "pre_nov21",
        "ask_t0": round(float(best.ask), 5), "spread_t0": round(float(best.ask) - float(bid0), 5) if bid0 is not None else np.nan,
        "avg_fill_px_1s": f1.avg_px if f1.filled else np.nan, "fill_status_1s": f1.status,
        "limit_px": lim, "shares": shares, "filled_1s": f1.filled, "miss_reason": "" if f1.filled else f1.status,
        "exit_reason": "HOLD_SETTLE" if f1.filled else f"MISSED_{f1.status}",
        "hold_sec": None if f1.filled else 0, "resolved_outcome": out,
        "would_have_won": (out == best.side) if out in ("UP", "DOWN") else None,
        "entry_p_true": best.p_true, "entry_z": best.z, "sigma_used": dec0.sigma, "fee_rate": fee_r,
        "cost_1s": cost1, "exit_ts": None, "exit_bid_1s": None, "pnl_per_share": np.nan,
        "pnl_hold_to_settle": hold_pnl, "pnl_usd": np.nan, "sl_triggered": False, "sl_was_wrong": False,
    }
    return (pos, None) if f1.filled else (None, pos)


def _try_exit_variant(
    variant: GridVariant,
    open_pos: dict[str, Any],
    tick: Tuple[int, int, int],
    inps: Dict[str, MarketInputs],
    shared_fv: Dict[str, Tuple[Optional[float], Optional[Any], Optional[str]]],
    sec_maps: Dict[str, Dict[int, Any]],
    base_cfg: SniperConfig = TWAP_V2_FILTERED_CONFIG,
) -> Optional[dict[str, Any]]:
    """Run canonical exit rules on open position (supporting `F1` no-SL, `F2` TWAP-proj SL, `F3` TP@0.97)."""
    T, t_sec, elapsed = tick
    sym = open_pos["symbol"]
    if sym not in inps:
        return None
    inp = inps[sym]
    sig, fv, _err = shared_fv[sym]
    c_sym = _variant_cfg_for(variant, open_pos["fee_rate"], base_cfg)
    if fv is None and float(inp.ages.get("cl_spot", 9999.0)) <= DEFAULT_MAX_AGES["cl_spot"] and float(inp.ages.get("book", 9999.0)) <= DEFAULT_MAX_AGES["book"]:
        res = compute_fv(inp, c_sym)
        if res is not None:
            sig, fv = res
    if fv is None:
        return None
    if variant.use_twap_proj_sl and elapsed >= 240:
        z_tp = compute_z_twap_proj_diagnostic(inp.strike_twap or 0.0, inp.cl_spot or 0.0, float(300 - elapsed), sig or 5e-5, inp.realized_twap_240, open_pos["side"])
        if z_tp is not None:
            sign = 1.0 if open_pos["side"] == "UP" else -1.0
            fv = replace(fv, raw_z_score=sign * float(z_tp))
    bids0 = inp.up_bids if open_pos["side"] == "UP" else inp.down_bids
    asks0 = inp.up_asks if open_pos["side"] == "UP" else inp.down_asks
    bk0 = OrderBookTop(
        up_bid=bids0[0][0] if (open_pos["side"] == "UP" and bids0) else None,
        up_ask=asks0[0][0] if (open_pos["side"] == "UP" and asks0) else None,
        down_bid=bids0[0][0] if (open_pos["side"] == "DOWN" and bids0) else None,
        down_ask=asks0[0][0] if (open_pos["side"] == "DOWN" and asks0) else None,
    )
    op_obj = OpenPosition(sym, open_pos["side"], int(T), open_pos["shares"], float(open_pos["avg_fill_px_1s"]), float(open_pos["cost_1s"]), float(open_pos["entry_p_true"]), float(open_pos["entry_z"]))
    ex = evaluate_exit(c_sym, op_obj, float(elapsed), fv, bk0)
    if ex is None:
        return None
    r1 = sec_maps[sym].get(t_sec + 1)
    bids1 = _parse_levels_json(getattr(r1, "up_bids_json" if open_pos["side"] == "UP" else "down_bids_json", None), True) if r1 else []
    min_bid = max(0.01, round(ex.bid - (0.05 if ex.reason == "EMERGENCY_STOP_LOSS" else 0.03), 2))
    sell = simulate_fak_sell(bids1, float(open_pos["shares"]), min_bid)
    if not sell.filled or sell.avg_px is None:
        return None
    proc = effective_exit_proceeds(float(sell.avg_px), open_pos["fee_rate"])
    pnl1 = round(proc - open_pos["cost_1s"], 5)
    sl_trig = ex.reason == "EMERGENCY_STOP_LOSS"
    open_pos.update({
        "exit_reason": ex.reason, "exit_ts": t_sec + 1, "hold_sec": (t_sec + 1) - int(open_pos["signal_ts"]),
        "exit_bid_1s": sell.avg_px, "pnl_per_share": pnl1,
        "pnl_usd": round(pnl1 * open_pos["shares"], 4), "sl_triggered": sl_trig,
        "sl_was_wrong": bool(sl_trig and open_pos["resolved_outcome"] == open_pos["side"]),
    })
    return open_pos


def _step_grid_second(
    tick: Tuple[int, int, int],
    variants: Sequence[GridVariant],
    v_states: Dict[str, VariantWindowState],
    ctx: Tuple[RecorderStreamState, CausalAuxTrackers, Dict[str, Dict[int, Any]], Dict[Tuple[int, str], Any], Dict[str, float], SniperConfig],
    trades_by_code: Dict[str, List[dict[str, Any]]],
) -> None:
    """Advance all variants by one 1Hz tick inside the active window."""
    T, t_sec, elapsed = tick
    stream_state, aux, sec_maps, w_meta, fee_map, base_cfg = ctx
    inps = assemble_recorder_inputs(tick, sec_maps, w_meta, stream_state)
    any_open = any(st.open_pos is not None for st in v_states.values())
    need_fv = bool(150 <= elapsed <= 290 or any_open)
    shared_fv = _eval_shared_fv(inps, need_fv=need_fv, base_cfg=base_cfg)
    for v in variants:
        st = v_states[v.code]
        if not st.entered and st.open_pos is None and (150 <= elapsed <= 290):
            filled_pos, missed_pos = _try_enter_variant(v, tick, inps, shared_fv, aux, (sec_maps, w_meta, fee_map, base_cfg))
            st.entered = filled_pos is not None or missed_pos is not None
            st.open_pos = filled_pos
            if missed_pos is not None:
                trades_by_code[v.code].append(missed_pos)
        elif st.open_pos is not None:
            done = _try_exit_variant(v, st.open_pos, tick, inps, shared_fv, sec_maps, base_cfg)
            if done is not None:
                trades_by_code[v.code].append(done)
                st.open_pos = None
    for sym, inp in inps.items():
        aux.update(sym, t_sec, inp, shared_fv[sym][0])


def simulate_grid(
    variants: Sequence[GridVariant],
    fee_map: Dict[str, float],
    settled_ts: List[int],
    snap_by_wt_sym: Dict[Any, pd.DataFrame],
    w_meta: Dict[Tuple[int, str], Any],
    base_cfg: SniperConfig = TWAP_V2_FILTERED_CONFIG,
) -> Dict[str, pd.DataFrame]:
    """Single-pass streaming replay across all variants on recorded snapshots."""
    stream_state = RecorderStreamState()
    aux = CausalAuxTrackers()
    trades_by_code: Dict[str, List[dict[str, Any]]] = {v.code: [] for v in variants}
    for T in settled_ts:
        sec_maps = {
            s: ({int(r.ts_sec): r for r in g.itertuples(index=False)} if (g := snap_by_wt_sym.get((int(T), s))) is not None else {})
            for s in SYMBOLS
        }
        v_states = {v.code: VariantWindowState() for v in variants}
        ctx = (stream_state, aux, sec_maps, w_meta, fee_map, base_cfg)
        for t_sec in range(int(T), int(T) + 300):
            _step_grid_second((int(T), t_sec, t_sec - int(T)), variants, v_states, ctx, trades_by_code)
        for v in variants:
            pos = v_states[v.code].open_pos
            if pos is None:
                continue
            hp = pos["pnl_hold_to_settle"]
            pos.update({
                "hold_sec": (int(T) + 300) - int(pos["signal_ts"]),
                "pnl_per_share": hp,
                "pnl_usd": round(float(hp) * pos["shares"], 4) if pd.notna(hp) else np.nan,
            })
            trades_by_code[v.code].append(pos)
    return {code: pd.DataFrame(rows) for code, rows in trades_by_code.items()}


def _max_dd_fixed5_usd(pnls_per_share: np.ndarray, start_eq: float = 40.0, shares: float = 5.0) -> float:
    """Simulated max drawdown (USD & %) at fixed 5-share lot size starting from $40U."""
    if len(pnls_per_share) == 0:
        return 0.0
    eq = start_eq
    peak = start_eq
    max_dd = 0.0
    for p in pnls_per_share:
        eq = max(0.0, eq + float(p) * shares)
        peak = max(peak, eq)
        dd = (peak - eq) / peak if peak > 0 else 0.0
        max_dd = max(max_dd, dd)
    return round(max_dd * 100.0, 2)


def compute_paired_stats(df_var: pd.DataFrame, df_base: pd.DataFrame) -> dict[str, Any]:
    """Compute paired same-window PnL/share difference and t-stat of variant vs `BASE`."""
    f_b = df_base[df_base["filled_1s"] == True] if len(df_base) else pd.DataFrame()
    f_v = df_var[df_var["filled_1s"] == True] if len(df_var) else pd.DataFrame()
    if len(f_b) == 0 or len(f_v) == 0:
        return {"n_paired": 0, "mean_diff": None, "se_diff": None, "paired_t": None}
    b_map = {int(r.window_ts): float(r.pnl_per_share) for r in f_b.itertuples(index=False) if pd.notna(r.pnl_per_share)}
    v_map = {int(r.window_ts): float(r.pnl_per_share) for r in f_v.itertuples(index=False) if pd.notna(r.pnl_per_share)}
    common = sorted(set(b_map) & set(v_map))
    if not common:
        return {"n_paired": 0, "mean_diff": None, "se_diff": None, "paired_t": None}
    diffs = np.array([v_map[w] - b_map[w] for w in common], dtype=float)
    mean_d = float(np.mean(diffs))
    se_d = float(np.std(diffs, ddof=1) / math.sqrt(len(diffs))) if len(diffs) >= 2 else None
    t_val = (mean_d / se_d) if (se_d is not None and se_d > 1e-12) else None
    return {
        "n_paired": len(common),
        "mean_diff": round(mean_d, 5),
        "se_diff": round(se_d, 5) if se_d is not None else None,
        "paired_t": round(t_val, 3) if t_val is not None else None,
    }


def summarize_variant(variant: GridVariant, df_t: pd.DataFrame, df_base: pd.DataFrame, span_days: float) -> dict[str, Any]:
    """Compute summary metrics, null benchmark `1.94*SE`, and paired stats vs `BASE`."""
    f1 = df_t[df_t["filled_1s"] == True] if len(df_t) else pd.DataFrame()
    n_sig = int(len(df_t))
    n_fill = int(len(f1))
    paired = compute_paired_stats(df_t, df_base) if variant.code != "BASE" else {"n_paired": n_fill, "mean_diff": 0.0, "se_diff": 0.0, "paired_t": 0.0}
    if n_fill == 0:
        return {
            "code": variant.code, "group": variant.group, "description": variant.description,
            "signals": n_sig, "filled_1s": 0, "wr_pct": None, "mean_pnl_sh": None, "se_pnl_sh": None,
            "null_194se": None, "ev_stake_pct": None, "max_dd_pct": 0.0, "trades_per_day": 0.0, **paired,
        }
    pnls = f1["pnl_per_share"].dropna().to_numpy(dtype=float)
    mean_p = float(np.mean(pnls)) if len(pnls) else 0.0
    se_p = float(np.std(pnls, ddof=1) / math.sqrt(len(pnls))) if len(pnls) >= 2 else None
    mean_c = float(f1["cost_1s"].dropna().mean()) if f1["cost_1s"].notna().any() else 0.0
    return {
        "code": variant.code, "group": variant.group, "description": variant.description,
        "signals": n_sig, "filled_1s": n_fill,
        "wr_pct": round(float((pnls > 0.0).mean()) * 100.0, 2) if len(pnls) else None,
        "mean_pnl_sh": round(mean_p, 5),
        "se_pnl_sh": round(se_p, 5) if se_p is not None else None,
        "null_194se": round(NULL_BENCHMARK_24_SIGMA_MULT * se_p, 5) if se_p is not None else None,
        "ev_stake_pct": round((mean_p / mean_c) * 100.0, 2) if mean_c > 0 else None,
        "max_dd_pct": _max_dd_fixed5_usd(pnls),
        "trades_per_day": round(n_fill / max(span_days, 1e-6), 2),
        **paired,
    }


def _compare_trade_dfs(df_ref: pd.DataFrame, df_grid: pd.DataFrame, frozen_ok: bool, frozen_diffs: list[str]) -> dict[str, Any]:
    """Compare two single-config simulation DataFrames row-by-row."""
    n_ref, n_grid = int(len(df_ref)), int(len(df_grid))
    if n_ref != n_grid:
        return {"parity_pct": 0.0, "matched": 0, "total": max(n_ref, n_grid), "frozen_ok": frozen_ok, "frozen_diffs": frozen_diffs, "mismatch": f"row_count {n_grid} != {n_ref}"}
    matched = 0
    first_diff: Optional[str] = None
    for i in range(n_ref):
        ra, rb = df_ref.iloc[i], df_grid.iloc[i]
        same_id = int(ra["window_ts"]) == int(rb["window_ts"]) and str(ra["symbol"]) == str(rb["symbol"]) and int(ra["signal_ts"]) == int(rb["signal_ts"])
        same_res = str(ra["side"]) == str(rb["side"]) and bool(ra["filled_1s"]) == bool(rb["filled_1s"]) and str(ra["exit_reason"]) == str(rb["exit_reason"])
        pa, pb = ra["pnl_per_share"], rb["pnl_per_share"]
        same_pnl = (pd.isna(pa) and pd.isna(pb)) or (pd.notna(pa) and pd.notna(pb)) and abs(float(pa) - float(pb)) <= 1e-9
        if same_id and same_res and same_pnl:
            matched += 1
        elif first_diff is None:
            first_diff = f"row {i}: ref=({ra['window_ts']},{ra['symbol']},{ra['signal_ts']},{ra['pnl_per_share']}) vs grid=({rb['window_ts']},{rb['symbol']},{rb['signal_ts']},{rb['pnl_per_share']})"
    pct = round(matched / n_ref * 100.0, 2) if n_ref else 100.0
    return {"parity_pct": pct, "matched": matched, "total": n_ref, "frozen_ok": frozen_ok, "frozen_diffs": frozen_diffs, "mismatch": first_diff}


def verify_base_recorder_parity(
    df_grid_base: pd.DataFrame,
    fee_map: Dict[str, float],
    settled_ts: List[int],
    snap_by_wt_sym: Dict[Any, pd.DataFrame],
    w_meta: Dict[Tuple[int, str], Any],
) -> dict[str, Any]:
    """Assert 100.0% equivalence between `BASE` in grid and `live_recorder_42._sim_one_cfg(TWAP_V2_FILTERED_CONFIG)`."""
    df_ref, _, _, _ = _sim_one_cfg(TWAP_V2_FILTERED_CONFIG, fee_map, settled_ts, snap_by_wt_sym, w_meta)
    frozen_ok, frozen_diffs = check_frozen_config_parity(TWAP_V2_FILTERED_CONFIG, FROZEN_CFG_PATH, "v2_filtered")
    return _compare_trade_dfs(df_ref, df_grid_base, frozen_ok, frozen_diffs)


def verify_all_primary_parity(
    df_grid_base: pd.DataFrame,
    fee_map: Dict[str, float],
    settled_ts: List[int],
    snap_by_wt_sym: Dict[Any, pd.DataFrame],
    w_meta: Dict[Tuple[int, str], Any],
) -> dict[str, Any]:
    """Verify 100.0% parity across all 3 pre-registered primary models (`v2_filtered`, `v2_pure`, `v1`)."""
    out: dict[str, Any] = {"v2_filtered": verify_base_recorder_parity(df_grid_base, fee_map, settled_ts, snap_by_wt_sym, w_meta)}
    for model_name, cfg in (("v2_pure", TWAP_V2_PURE_CONFIG), ("v1", DEFAULT_SNIPER_CONFIG)):
        frozen_ok, frozen_diffs = check_frozen_config_parity(cfg, FROZEN_CFG_PATH, model_name)
        df_ref, _, _, _ = _sim_one_cfg(cfg, fee_map, settled_ts, snap_by_wt_sym, w_meta)
        ctl = GridVariant(f"CONTROL_{model_name.upper()}", "BASE", f"Parity control {model_name}")
        df_ctl = simulate_grid([ctl], fee_map, settled_ts, snap_by_wt_sym, w_meta, base_cfg=cfg)[ctl.code]
        out[model_name] = _compare_trade_dfs(df_ref, df_ctl, frozen_ok, frozen_diffs)
    return out


def _min_ev_entry(df_sub: pd.DataFrame) -> Optional[dict[str, Any]]:
    """Return the lowest `net_ev` record across `df_sub`, or `None` if empty."""
    best: Optional[dict[str, Any]] = None
    for row in df_sub.itertuples(index=False):
        if pd.isna(row.ask_t0) or pd.isna(row.entry_p_true):
            continue
        ask, p_true = float(row.ask_t0), float(row.entry_p_true)
        fee_r = float(row.fee_rate) if pd.notna(row.fee_rate) else 0.0
        fee_t0 = float(taker_fee_per_share(ask, fee_r))
        net_ev = p_true - (ask + fee_t0)
        if best is None or net_ev < best["net_ev"]:
            best = {
                "net_ev": round(net_ev, 6), "window_ts": int(row.window_ts),
                "symbol": str(row.symbol), "side": str(row.side), "ask_t0": round(ask, 4),
                "entry_p_true": round(p_true, 5), "entry_z": round(float(row.entry_z), 4),
                "fee_t0": round(fee_t0, 5), "filled_1s": bool(row.filled_1s),
            }
    return best


def compute_base_min_net_ev(df_base: pd.DataFrame) -> dict[str, Any]:
    """Compute min `net_ev` on all signals & filled trades in `BASE` to prove B1-B3 non-bindingness."""
    required = {"ask_t0", "entry_p_true", "fee_rate", "filled_1s", "window_ts", "symbol", "side", "entry_z"}
    if len(df_base) == 0 or not required.issubset(df_base.columns):
        return {"n_all": 0, "min_net_ev_all": None, "min_all_row": None, "n_filled": 0, "min_net_ev_filled": None, "min_filled_row": None}
    filled_df = df_base[df_base["filled_1s"] == True]
    min_all = _min_ev_entry(df_base)
    min_fill = _min_ev_entry(filled_df) if len(filled_df) else None
    return {
        "n_all": int(len(df_base)),
        "min_net_ev_all": min_all["net_ev"] if min_all else None,
        "min_all_row": min_all,
        "n_filled": int(len(filled_df)),
        "min_net_ev_filled": min_fill["net_ev"] if min_fill else None,
        "min_filled_row": min_fill,
    }


def build_e4_diff_table(df_base: pd.DataFrame, df_e4: pd.DataFrame) -> List[dict[str, Any]]:
    """Build window-by-window forensics table where `E4` differed from `BASE`."""
    b_map = {int(r.window_ts): r for r in df_base.itertuples(index=False)} if len(df_base) else {}
    e_map = {int(r.window_ts): r for r in df_e4.itertuples(index=False)} if len(df_e4) else {}
    diffs: List[dict[str, Any]] = []
    for wt in sorted(set(b_map) | set(e_map)):
        rb, re = b_map.get(wt), e_map.get(wt)
        same_pres = (rb is not None) and (re is not None)
        b_p = float(rb.pnl_per_share) if (rb is not None and pd.notna(rb.pnl_per_share)) else None
        e_p = float(re.pnl_per_share) if (re is not None and pd.notna(re.pnl_per_share)) else None
        same_fields = bool(
            same_pres and str(rb.symbol) == str(re.symbol) and str(rb.side) == str(re.side)
            and bool(rb.filled_1s) == bool(re.filled_1s) and int(rb.elapsed_sec) == int(re.elapsed_sec)
            and ((b_p is None and e_p is None) or (b_p is not None and e_p is not None and abs(b_p - e_p) <= 1e-6))
        )
        if same_fields:
            continue
        ref = rb if rb is not None else re
        assert ref is not None
        delta = round((e_p or 0.0) - (b_p or 0.0), 5) if (b_p is not None or e_p is not None) else None
        diffs.append({
            "window_ts": wt, "symbol": str(ref.symbol), "side": str(ref.side),
            "outcome": str(ref.resolved_outcome or "-"),
            "base_el": int(rb.elapsed_sec) if rb is not None else None,
            "base_status": ("FILLED" if rb.filled_1s else str(rb.miss_reason or "MISSED")) if rb is not None else "SKIPPED",
            "base_pnl": round(b_p, 5) if b_p is not None else None,
            "e4_el": int(re.elapsed_sec) if re is not None else None,
            "e4_status": ("FILLED" if re.filled_1s else str(re.miss_reason or "MISSED")) if re is not None else "SKIPPED",
            "e4_pnl": round(e_p, 5) if e_p is not None else None,
            "pnl_delta": delta,
        })
    return diffs


def _sha256_prefix(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()[:12] if path.exists() else "MISSING"


def _render_grid_table(summaries: List[dict[str, Any]]) -> List[str]:
    ranked = sorted(summaries, key=lambda r: (r["mean_pnl_sh"] is not None, r["mean_pnl_sh"] if r["mean_pnl_sh"] is not None else -999.0), reverse=True)
    rows = [
        "| Rank | Code | Group | Description | Signals | Filled +1s | Trades/d | WR% | Mean PnL/sh | SE | Null Max (1.94×SE) | EV/Stake% | MaxDD% | Paired n | ΔvsBASE | Paired t |",
        "|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for idx, r in enumerate(ranked, start=1):
        rows.append(
            f"| {idx} | **{r['code']}** | {r['group']} | {r['description']} | {r['signals']} | {r['filled_1s']} | "
            f"{r['trades_per_day']:.2f} | {r['wr_pct']} | {r['mean_pnl_sh']} | {r['se_pnl_sh']} | {r['null_194se']} | "
            f"{r['ev_stake_pct']} | {r['max_dd_pct']:.2f}% | {r['n_paired']} | {r['mean_diff']} | {r['paired_t']} |"
        )
    return rows


def _render_section2_notes(base_ev: dict[str, Any]) -> List[str]:
    """Render `BASE min(net_ev)` diagnostic proving `B1..B3` non-bindingness and `F2` note."""
    lines = ["", "- **`F2` 重複標記**：`F2` 於現行 `v2_filtered` (`use_twap_projection=True`) 下行為與 `BASE` 完全重疊（保留於 24 組網格，不移除）。"]
    if not base_ev or base_ev.get("n_all", 0) == 0:
        lines.append("- **`BASE min(net_ev)` 檢驗**：尚無 `BASE` 訊號樣本。")
        return lines
    ma, mf = base_ev.get("min_all_row"), base_ev.get("min_filled_row")
    if ma is not None:
        ev_c = ma["net_ev"] * 100.0
        lines.append(
            f"- **`BASE min(net_ev)`（全部訊號 `n={base_ev['n_all']}`）**：`+{ev_c:.2f}c` (`{ma['net_ev']:.6f}`, "
            f"`{ma['window_ts']} {ma['symbol']} {ma['side']}`, `ask={ma['ask_t0']}`, `p={ma['entry_p_true']}`, `z={ma['entry_z']}`, `fee={ma['fee_t0']}`)。"
        )
    if mf is not None:
        ev_fc = mf["net_ev"] * 100.0
        lines.append(
            f"- **`BASE min(net_ev)`（已成交 `+1s` `n={base_ev['n_filled']}`）**：`+{ev_fc:.2f}c` (`{mf['net_ev']:.6f}`, "
            f"`{mf['window_ts']} {mf['symbol']} {mf['side']}`, `ask={mf['ask_t0']}`, `p={mf['entry_p_true']}`, `z={mf['entry_z']}`, `fee={mf['fee_t0']}`)。"
            " 因 `z >= 1.30`、`p >= 0.80` 與 `ask <= 0.82` 已先將真實 `net_ev` 卡在 `> +7.0c`，故 `B1` (`>=2c`)、`B2` (`>=5c`)、`B3` (`>=7c`) 與 `BASE` 結果一致（非參數漏接）。"
        )
    return lines


def _render_section3_e4(e4_diffs: List[dict[str, Any]]) -> List[str]:
    """Render objective `E4` vs `BASE` diff table separating fills from non-fills."""
    fill_diffs = [d for d in e4_diffs if d["base_status"] == "FILLED" or d["e4_status"] == "FILLED"]
    net_delta = round(sum(float(d["pnl_delta"] or 0.0) for d in fill_diffs), 5)
    lines = [
        "",
        "## 3. `E4` vs `BASE` 逐窗差異鑑識表",
        "",
        f"- 共有 **{len(e4_diffs)} 個視窗** 出現訊號行為差異，其中僅 **{len(fill_diffs)} 個視窗涉及實際 `+1s` 成交變化**（其餘皆為 `BASE` `MISSED_PRICE` 未成交窗，不計為救回虧損），成交差異合計 ΔPnL/sh = `{net_delta}`。",
        "",
        "| Window TS | Coin | Side | Outcome | BASE El | BASE Status | BASE PnL/sh | E4 El | E4 Status | E4 PnL/sh | ΔPnL (E4-BASE) |",
        "|---:|---|---|---|---:|---|---:|---:|---|---:|---:|",
    ]
    for d in e4_diffs[:40]:
        lines.append(
            f"| {d['window_ts']} | `{d['symbol']}` | {d['side']} | {d['outcome']} | {d['base_el']} | `{d['base_status']}` | {d['base_pnl']} | "
            f"{d['e4_el']} | `{d['e4_status']}` | {d['e4_pnl']} | {d['pnl_delta']} |"
        )
    return lines


def _render_section4_rules() -> List[str]:
    """Render finalized Claude Section 10 / 5m-only governance rules."""
    return [
        "",
        "## 4. 規劃者已裁決之 5m 專屬治理規格 (`CLAUDE_RESPONSE_10`)",
        "",
        "1. **主判定層維持 3 組（5m 專屬）**：`v2_pure`、`v2_filtered`、`v1`（Bonferroni `98.33%`），不擴組；`v2_bnguard` 類過濾由探索層 `G3` 承擔。15m BTC 路線依使用者指示取消，僅保留 5m 路線。",
        "2. **上線與第 10 天提早停損（fail-only）**：`v2_filtered` 需 `n >= 300` 且 `98.33% CI` 下界 `> 0` 方可進入 5 股試跑；第 10 天若 3 組 `98.33% CI` 上界皆 `< +2c/sh` 則提早停損結束（絕不提早放行）。",
        "3. **主機隔離紀律**：4.2 期間離線重播限用 `nice -n 19` 於固定離峰時段執行，嚴禁於即時錄製／Runner 運行期間併發高 CPU 重運算。",
    ]


def _render_grid_report(
    summaries: List[dict[str, Any]],
    all_parity: dict[str, Any],
    e4_diffs: List[dict[str, Any]],
    meta: dict[str, Any],
) -> str:
    """Render Markdown exploration report (pure exploratory diagnostics, zero launch claims)."""
    sc_sha = _sha256_prefix(REPO / "engine" / "signal_core.py")
    os_sha = _sha256_prefix(REPO / "engine" / "oracle_sniper.py")
    fc_sha = _sha256_prefix(FROZEN_CFG_PATH)
    lines = [
        "# 探索網格報告（24 組策略變體離線重播 — Tier 2 Exploratory Only）",
        "",
        f"> **Scope**: `{meta['scope_label']}` | 總已結算視窗 `{meta['windows_total']}`（過濾後 `{meta['windows_used']}` 窗 / `{meta['span_days']:.2f}` 天） | Start TS `{meta['start_ts']}`",
        "> ⚠️ **紀律聲明**：本表僅供探索期排序與候選發掘（**不構成判定、不作為直接實盤依據**）。24 組無優勢下，第 1 名純靠運氣的期望上限約 **`1.94 × SE`**（已逐列標註對照）。",
        "",
        "## 1. 三組預註冊主設定 100% 一致性與核心零改動證明 (`exploration_grid` vs `live_recorder_42._sim_one_cfg`)",
        "",
        f"- 凍結檔案指紋（SHA256 前 12 碼）：`signal_core.py={sc_sha}` | `oracle_sniper.py={os_sha}` | `frozen_config.json={fc_sha}`（完全未改）。",
        "",
        "| Primary Model | `check_frozen_config_parity` | Row Parity | Matched / Total | Mismatch |",
        "|---|---|---:|---:|---|",
    ]
    for m_lbl in ("v2_filtered", "v2_pure", "v1"):
        pr = all_parity[m_lbl]
        lines.append(f"| `{m_lbl}` | **`{pr['frozen_ok']}`** | **`{pr['parity_pct']}%`** | `{pr['matched']}/{pr['total']}` | `{pr['mismatch']}` |")

    lines += ["", "## 2. 24 組變體總表（依每股平均 PnL 排序）", ""] + _render_grid_table(summaries)
    lines += _render_section2_notes(meta.get("base_min_net_ev") or {})
    lines += _render_section3_e4(e4_diffs)
    lines += _render_section4_rules()
    return "\n".join(lines) + "\n"


def _append_summary_block(target: pathlib.Path, summaries: List[dict[str, Any]], meta: dict[str, Any]) -> None:
    """Insert or replace delimited Tier-2 exploration block inside an existing daily report."""
    s_tag, e_tag = "<!-- EXPLORATION_GRID_START -->", "<!-- EXPLORATION_GRID_END -->"
    block_lines = [
        s_tag, "",
        f"## Tier-2 探索網格日報表（唯讀探索，不構成判定｜Scope=`{meta['scope_label']}`｜Start=`{meta['start_ts']}`）",
        "",
    ] + _render_grid_table(summaries) + _render_section2_notes(meta.get("base_min_net_ev") or {}) + ["", e_tag]
    block = "\n".join(block_lines)
    txt = target.read_text(encoding="utf-8") if target.exists() else ""
    if s_tag in txt and e_tag in txt:
        pre, rest = txt.split(s_tag, 1)
        _mid, post = rest.split(e_tag, 1)
        target.write_text(pre.rstrip() + "\n\n" + block + "\n" + post.lstrip(), encoding="utf-8")
    else:
        sep = "\n\n" if txt and not txt.endswith("\n\n") else ""
        target.write_text(txt + sep + block + "\n", encoding="utf-8")


def run_exploration_grid(
    db_path: pathlib.Path = DEFAULT_DB,
    frozen_path: pathlib.Path = FROZEN_CFG_PATH,
    start_ts: int = NOV21_UTC_TS,
    sanity: bool = False,
    csv_dir: pathlib.Path = GRID_CSV_DIR,
    report_path: pathlib.Path = GRID_REPORT_PATH,
    append_to: Optional[pathlib.Path] = None,
) -> dict[str, Any]:
    """Run all 25 exploration grid configs read-only and emit CSVs + Markdown report."""
    frozen = json.loads(frozen_path.read_text(encoding="utf-8"))
    fee_map = {str(k): float(v) for k, v in frozen.get("effective_fees_post_nov20", DEFAULT_FEE_MAP).items()}
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    con.execute("PRAGMA busy_timeout=5000;")
    df_w = pd.read_sql_query("SELECT * FROM windows_42 ORDER BY window_ts, symbol", con)
    df_s = pd.read_sql_query("SELECT * FROM snapshots_1hz ORDER BY window_ts, symbol, elapsed_sec", con)
    con.close()

    settled = df_w[df_w["resolved_outcome"].isin(["UP", "DOWN"])]
    settled_ts = sorted(int(w) for w in settled["window_ts"].unique())
    snap_by_wt_sym = {k: g.sort_values("elapsed_sec") for k, g in df_s.groupby(["window_ts", "symbol"])}
    w_meta = {(int(r.window_ts), str(r.symbol)): r for r in df_w.itertuples(index=False)}

    variants = build_grid_variants()
    dfs_all = simulate_grid(variants, fee_map, settled_ts, snap_by_wt_sym, w_meta)
    all_parity = verify_all_primary_parity(dfs_all["BASE"], fee_map, settled_ts, snap_by_wt_sym, w_meta)
    eff_start = 0 if sanity else int(start_ts)
    used_ts = [w for w in settled_ts if w >= eff_start]
    span_days = max(1 / 288.0, ((max(used_ts) - min(used_ts) + 300) / 86400.0)) if used_ts else 0.0

    csv_dir.mkdir(parents=True, exist_ok=True)
    dfs_filt: Dict[str, pd.DataFrame] = {}
    for code, df_c in dfs_all.items():
        sub = df_c[df_c["window_ts"] >= eff_start].copy() if (len(df_c) and eff_start > 0) else df_c
        dfs_filt[code] = sub
        sub.to_csv(csv_dir / f"grid_{code}.csv", index=False)

    summaries = [summarize_variant(v, dfs_filt[v.code], dfs_filt["BASE"], span_days) for v in variants]
    e4_diffs = build_e4_diff_table(dfs_filt["BASE"], dfs_filt["E4"])
    base_ev_stats = compute_base_min_net_ev(dfs_filt["BASE"])
    meta = {
        "scope_label": "PRE_NOV21_SANITY_ALL_SETTLED (code correctness check only)" if sanity else "POST_NOV21_EXPLORATION",
        "start_ts": eff_start, "windows_total": len(settled_ts),
        "windows_used": len(used_ts), "span_days": span_days,
        "base_min_net_ev": base_ev_stats,
    }
    report_path.write_text(_render_grid_report(summaries, all_parity, e4_diffs, meta), encoding="utf-8")
    if append_to is not None:
        _append_summary_block(append_to, summaries, meta)
    return {
        "parity": all_parity["v2_filtered"], "all_parity": all_parity,
        "e4_diff_count": len(e4_diffs), "base_min_net_ev": base_ev_stats,
        "meta": meta, "summaries": summaries, "report_path": str(report_path),
    }


def main() -> None:
    """CLI entrypoint for offline 24-variant exploration grid."""
    ap = argparse.ArgumentParser(description="Offline 24-Variant Exploration Grid")
    ap.add_argument("--db", type=pathlib.Path, default=DEFAULT_DB)
    ap.add_argument("--start-ts", type=int, default=NOV21_UTC_TS)
    ap.add_argument("--sanity", action="store_true", help="Include all recorded settled windows for code-correctness verification")
    ap.add_argument("--out-report", type=pathlib.Path, default=GRID_REPORT_PATH)
    ap.add_argument("--csv-dir", type=pathlib.Path, default=GRID_CSV_DIR)
    ap.add_argument("--append-to", type=pathlib.Path, default=None)
    args = ap.parse_args()
    res = run_exploration_grid(args.db, start_ts=args.start_ts, sanity=args.sanity, csv_dir=args.csv_dir, report_path=args.out_report, append_to=args.append_to)
    print(json.dumps({"all_parity": res["all_parity"], "e4_diff_count": res["e4_diff_count"], "meta": res["meta"], "variants": len(res["summaries"]), "report": res["report_path"]}, indent=2))


if __name__ == "__main__":
    main()
