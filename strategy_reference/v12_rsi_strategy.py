"""V12.0 Hammer-Reclaim Multi-Period RSI Exhaustion Reversal Strategy (Full 5m Candle T=0s->300s)."""
from __future__ import annotations

from dataclasses import dataclass, field
import math
from typing import Any

import pandas as pd

COINS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
POST_NOV20_TAKER_FEES: dict[str, float] = {
    "BTCUSDT": 0.05,
    "ETHUSDT": 0.01,
    "SOLUSDT": 0.01,
}


@dataclass(frozen=True)
class V12RSIConfig:
    """
    Production V12.0_HAMMER_RECLAIM_RSI Specification:
    1) Replaces V11's waterfall-chasing A_CPOS (<0.15 marubozu crash closes) with a strict
       Wick-Rejection Floor (close_pos_in_bar >= 0.15 for LONG / <= 0.85 for SHORT), keeping
       signals in the 51-54c sweet spot instead of chasing >57c bounces or 44c crashes.
    2) Removes toxic D_WYCKOFF_2LEG (41.5% WR) & overfit fallback rules, preserving C_BB / B_BACC
       multi-period RSI(2/6/14), 1h trend guard, counter-TBR & 3-bar CVD iceberg gates.
    3) Enforces Chainlink TWAP60 anti-waterfall check (basis >= -3.0 bps) and a tight
       5-second 2-stage CLOB execution cap ([0.48, 0.52] Maker -> 0.53/0.515 FAK Taker).
    4) Replaces 4-step Martingale ruin risk with 1-Shot Quarter-Kelly sizing (opt-in 2-step cap).
    """

    name: str = "V12.0 Hammer-Reclaim 多週期 RSI + TWAP60 反瀑布護欄 + 緊縮凱利策略"
    version: str = "V12.0_HAMMER_RECLAIM_RSI"
    atr_min_pct: float = 0.09
    streak_min: int = 2
    streak_max: int = 5
    macro_max_ret_1h: float = 1.4
    macro_max_ret_4h: float = 2.8
    reclaim_close_pos_min: float = 0.15
    rsi_branch_b: float = 24.0
    body_to_atr_ratio: float = 1.25
    rsi_branch_c: float = 24.0
    bb_pct_limit: float = 0.12
    rsi2_single_branch_max: float = 14.5
    rsi_term_enabled: bool = True
    rsi_term_streak_max: int = 4
    rsi_term_rsi2_limit: float = 4.5
    rsi_term_rsi6_limit: float = 19.5
    rsi_term_rsi14_floor: float = 25.0
    rsi_term_min_spread: float = 10.0
    rsi_term_min_bacc: float = 1.15
    rsi_term_max_tbr: float = 0.45
    max_counter_tbr: float = 0.52
    single_branch_max_tbr: float = 0.475
    single_branch_min_wick_exempt: float = 0.20
    single_branch_min_bacc_exempt: float = 1.10
    trend_guard_enabled: bool = True
    trend_guard_adverse_1h: float = 0.60
    trend_guard_min_wick: float = 0.15
    trend_guard_max_rsi: float = 17.0
    iceberg_cvd3_min_streak: int = 5
    iceberg_max_cvd3: float = 0.06
    iceberg_climax_tbr_exempt: float = 0.30

    # Oracle basis & 5s CLOB Execution Limits
    max_adverse_twap_basis_bps: float = 3.0
    min_entry_price: float = 0.48
    max_maker_price: float = 0.52
    max_eth_sol_taker_price: float = 0.53
    max_btc_taker_price: float = 0.515
    max_opening_mid_price: float = 0.55
    maker_max_rest_sec: float = 5.0

    # Position Sizing & Anti-Ruin Capital Control
    max_martingale_steps: int = 1
    min_exchange_shares: float = 5.0
    tier_s_plus_micro_shares: float = 6.0
    flat_equity_ceiling_u: float = 85.0
    kelly_fraction: float = 0.25
    prior_win_prob: float = 0.555
    max_risk_pct: float = 0.05
    dd_compress_threshold: float = 0.85
    dd_compress_multiplier: float = 0.55
    strict_2step_max_ask: float = 0.52
    taker_fees: dict[str, float] = field(default_factory=lambda: dict(POST_NOV20_TAKER_FEES))


V12_CONFIG = V12RSIConfig()


def _sf(df: pd.DataFrame, col: str, idx: int, default: float = 0.0) -> float:
    np_map = getattr(df, "_np_cache", None)
    if np_map is not None:
        arr = np_map.get(col)
        if arr is None:
            return default
        val = float(arr[idx])
        return default if val != val else val
    if col not in df.columns:
        return default
    val = df[col].iat[idx]
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return default
    return float(val)


def _norm_features(df: pd.DataFrame, idx: int, is_long: bool) -> dict[str, float]:
    r2 = _sf(df, "rsi_2", idx, 50.0)
    r6 = _sf(df, "rsi_6", idx, 50.0)
    r14 = _sf(df, "rsi_14", idx, 50.0)
    bb = _sf(df, "bb_pct_b", idx, 0.5)
    tbr = _sf(df, "taker_buy_ratio", idx, 0.5)
    cvd3 = _sf(df, "cvd_3", idx, 2.0 * tbr - 1.0)
    ret1h = _sf(df, "ret_12bars_pct", idx, 0.0)
    ret4h = _sf(df, "ret_48bars_pct", idx, 0.0)
    cpos = _sf(df, "close_pos_in_bar", idx, 0.5)
    if is_long:
        return {
            "streak": _sf(df, "streak_down", idx, 0.0),
            "r2": r2, "r6": r6, "r14": r14, "bb": bb, "tbr": tbr, "cvd3": cvd3,
            "ret1h": ret1h, "ret4h": ret4h, "cpos": cpos,
            "wick": _sf(df, "lower_wick_ratio", idx, 0.0),
            "bacc": _sf(df, "body_accel", idx, 0.0),
            "vol_r": _sf(df, "vol_ratio", idx, 1.0),
            "atr": _sf(df, "atr_pct", idx, 0.0),
        }
    return {
        "streak": _sf(df, "streak_up", idx, 0.0),
        "r2": 100.0 - r2, "r6": 100.0 - r6, "r14": 100.0 - r14,
        "bb": 1.0 - bb, "tbr": 1.0 - tbr, "cvd3": -cvd3,
        "ret1h": -ret1h, "ret4h": -ret4h, "cpos": 1.0 - cpos,
        "wick": _sf(df, "upper_wick_ratio", idx, 0.0),
        "bacc": _sf(df, "body_accel", idx, 0.0),
        "vol_r": _sf(df, "vol_ratio", idx, 1.0),
        "atr": _sf(df, "atr_pct", idx, 0.0),
    }


def _passes_micro_gates(f: dict[str, float], sync_cnt: int, cfg: V12RSIConfig) -> list[str] | None:
    if f["atr"] < cfg.atr_min_pct or f["streak"] < cfg.streak_min or f["streak"] > cfg.streak_max:
        return None
    if f["ret1h"] < -cfg.macro_max_ret_1h or f["ret4h"] < -cfg.macro_max_ret_4h:
        return None
    if cfg.trend_guard_enabled and f["ret1h"] < -cfg.trend_guard_adverse_1h:
        if f["wick"] < cfg.trend_guard_min_wick or f["r6"] > cfg.trend_guard_max_rsi:
            return None
    if f["tbr"] >= cfg.max_counter_tbr or f["cpos"] <= cfg.reclaim_close_pos_min:
        return None

    br_b = (f["r6"] <= cfg.rsi_branch_b) and (f["bacc"] >= cfg.body_to_atr_ratio)
    br_c = (f["streak"] >= 3) and (f["r6"] <= cfg.rsi_branch_c) and (f["bb"] <= cfg.bb_pct_limit)
    if not (br_b or br_c):
        return None
    br_e = (
        cfg.rsi_term_enabled and f["streak"] <= cfg.rsi_term_streak_max
        and f["r2"] <= cfg.rsi_term_rsi2_limit and f["r6"] <= cfg.rsi_term_rsi6_limit
        and f["r14"] >= cfg.rsi_term_rsi14_floor and (f["r14"] - f["r6"]) >= cfg.rsi_term_min_spread
        and f["bacc"] >= cfg.rsi_term_min_bacc and f["tbr"] <= cfg.rsi_term_max_tbr
    )
    branches = [b for b, ok in (("B_BACC", br_b), ("C_BB", br_c), ("E_RSI_TERM", br_e)) if ok]
    n_br = len(branches)
    if (f["streak"] > 5 and n_br < 2) or (n_br == 1 and f["r2"] > cfg.rsi2_single_branch_max):
        return None
    if n_br == 1 and (0.455 < f["tbr"] < 0.545) and sync_cnt < 2 and f["vol_r"] < 1.15:
        return None
    if f["streak"] >= cfg.iceberg_cvd3_min_streak and f["cvd3"] > cfg.iceberg_max_cvd3 and f["tbr"] > cfg.iceberg_climax_tbr_exempt:
        return None
    if n_br == 1 and (not br_e) and f["tbr"] > cfg.single_branch_max_tbr and f["wick"] < cfg.single_branch_min_wick_exempt and f["bacc"] < cfg.single_branch_min_bacc_exempt:
        return None
    return branches


def _score_candidate(
    sym: str, df: pd.DataFrame, idx: int, is_long: bool, sync_cnt: int, mean_r6: float, cfg: V12RSIConfig
) -> dict[str, Any] | None:
    f = _norm_features(df, idx, is_long)
    branches = _passes_micro_gates(f, sync_cnt, cfg)
    if branches is None:
        return None
    n_br = len(branches)
    is_s_plus = (n_br >= 3) or ("E_RSI_TERM" in branches and f["r2"] <= 2.5 and (f["r14"] - f["r6"]) >= 11.5) or (n_br >= 2 and f["bacc"] >= 1.35 and f["vol_r"] >= 2.0 and f["tbr"] <= 0.42)
    is_s = is_s_plus or ("E_RSI_TERM" in branches) or (n_br >= 2 and f["tbr"] <= 0.43 and sync_cnt >= 2 and f["vol_r"] >= 1.15)
    tier = "TIER_S+" if is_s_plus else ("TIER_S" if is_s else "STANDARD")
    score = (100.0 - f["r6"]) + f["streak"] * 5.0 + f["bacc"] * 4.0 + n_br * 3.0 + (12.0 if is_s_plus else (8.0 if is_s else 0.0))
    return {
        "symbol": sym,
        "direction": "LONG_BOUNCE" if is_long else "SHORT_PULLBACK",
        "polymarket_side": "BUY UP" if is_long else "BUY DOWN",
        "price": _sf(df, "close", idx, 0.0),
        "rsi_2": round(_sf(df, "rsi_2", idx, 50.0), 2),
        "rsi_6": round(_sf(df, "rsi_6", idx, 50.0), 2),
        "rsi_14": round(_sf(df, "rsi_14", idx, 50.0), 2),
        "mean_rsi_6": round(mean_r6, 2),
        "streak": int(f["streak"]),
        "wick_ratio": round(f["wick"], 3),
        "vol_ratio": round(f["vol_r"], 2),
        "taker_buy_ratio": round(_sf(df, "taker_buy_ratio", idx, 0.5), 3),
        "cvd_3": round(_sf(df, "cvd_3", idx, 0.0), 4),
        "bb_pct_b": round(_sf(df, "bb_pct_b", idx, 0.5), 3),
        "close_pos_in_bar": round(_sf(df, "close_pos_in_bar", idx, 0.5), 3),
        "body_accel": round(f["bacc"], 2),
        "coins_sync": sync_cnt,
        "tier": tier,
        "is_tier_s": is_s,
        "is_tier_s_plus": is_s_plus,
        "trigger_branch": "+".join(branches),
        "score": round(score, 2),
    }


def evaluate_v12_signal_at_index(
    dfs: dict[str, pd.DataFrame],
    idx: int,
    cfg: V12RSIConfig = V12_CONFIG,
    twap_basis_bps_map: dict[str, float] | None = None,
) -> dict[str, Any] | None:
    """Select the highest-scoring V12.0 Hammer-Reclaim RSI signal across BTC/ETH/SOL at bar `idx`."""
    if idx < 48:
        return None
    r6s = [_sf(dfs[s], "rsi_6", idx, 50.0) for s in COINS if s in dfs and idx < len(dfs[s])]
    mean_r6 = sum(r6s) / max(1, len(r6s))
    sync_dn = sum(1 for s in COINS if s in dfs and idx < len(dfs[s]) and _sf(dfs[s], "streak_down", idx, 0) >= 2)
    sync_up = sum(1 for s in COINS if s in dfs and idx < len(dfs[s]) and _sf(dfs[s], "streak_up", idx, 0) >= 2)

    best: dict[str, Any] | None = None
    for sym in COINS:
        df = dfs.get(sym)
        if df is None or idx >= len(df):
            continue
        for is_long, sync_cnt in ((True, sync_dn), (False, sync_up)):
            cand = _score_candidate(sym, df, idx, is_long, sync_cnt, mean_r6, cfg)
            if cand is None:
                continue
            if twap_basis_bps_map and sym in twap_basis_bps_map and twap_basis_bps_map[sym] < -cfg.max_adverse_twap_basis_bps:
                continue
            if best is None or cand["score"] > best["score"]:
                best = cand
    return best


def compute_v12_order_plan(
    symbol: str,
    tier: str,
    best_bid: float | None,
    best_ask: float | None,
    active_equity_u: float,
    active_peak_u: float,
    step: int = 1,
    prior_cost_u: float = 0.0,
    cfg: V12RSIConfig = V12_CONFIG,
) -> dict[str, Any]:
    """Build price-capped Maker/Taker execution & Quarter-Kelly stake plan for one 5m entry."""
    if best_ask is None or best_ask < cfg.min_entry_price or best_ask > cfg.max_opening_mid_price:
        return {"action": "SKIP", "reason": f"ask={best_ask} outside [{cfg.min_entry_price}, {cfg.max_opening_mid_price}]"}
    fee_rate = cfg.taker_fees.get(symbol, 0.01)
    max_taker = cfg.max_btc_taker_price if symbol == "BTCUSDT" else cfg.max_eth_sol_taker_price
    maker_px = round(min(cfg.max_maker_price, max(cfg.min_entry_price, (best_ask - 0.01) if best_bid is not None else cfg.max_maker_price)), 2)

    if step >= 2 and (cfg.max_martingale_steps < 2 or best_ask > cfg.strict_2step_max_ask):
        return {"action": "SKIP", "reason": f"2-step blocked (max_steps={cfg.max_martingale_steps}, ask={best_ask})"}

    dd_scale = cfg.dd_compress_multiplier if active_equity_u < cfg.dd_compress_threshold * active_peak_u else 1.0
    if step >= 2 and prior_cost_u > 0.0:
        taker_fee_per_sh = fee_rate * best_ask * (1.0 - best_ask)
        shares = float(math.ceil((prior_cost_u + 0.50) / max(0.20, 1.0 - best_ask - taker_fee_per_sh)))
    elif active_equity_u < cfg.flat_equity_ceiling_u:
        shares = cfg.tier_s_plus_micro_shares if tier == "TIER_S+" else cfg.min_exchange_shares
    else:
        f_full = max(0.0, (cfg.prior_win_prob - maker_px) / max(0.10, 1.0 - maker_px))
        risk_pct = min(cfg.max_risk_pct, cfg.kelly_fraction * f_full) * dd_scale
        shares = max(cfg.min_exchange_shares, float(round((active_equity_u * risk_pct) / maker_px)))

    return {
        "action": "ENTER",
        "maker_limit_px": maker_px,
        "maker_max_rest_sec": cfg.maker_max_rest_sec,
        "fallback_taker_max_px": max_taker,
        "taker_eligible": bool(best_ask <= max_taker),
        "taker_fee_rate": fee_rate,
        "shares": shares,
        "est_maker_cost_u": round(shares * maker_px, 2),
    }
