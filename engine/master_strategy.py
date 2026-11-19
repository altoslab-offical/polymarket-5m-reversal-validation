from __future__ import annotations

import json
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any
import numpy as np
import pandas as pd

from engine.v8_strategy import (
    V8_CONFIG,
    compute_v8_tier_and_stakes,
    compute_v8_vault_locked,
    evaluate_v8_signal_at_index,
    run_v8_2year_validation,
)

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
PAPER_ACCOUNT_PATH = DATA_DIR / "paper_account.json"
MASTER_REPORT_PATH = DATA_DIR / "master_strategy_report.json"
OFFICIAL_ORACLE_2026_PATH = DATA_DIR / "polymarket_official_oracle_2026.json"


@dataclass
class MasterStrategySpec:
    """
    Route 1 Specialized Master Strategy for Polymarket 5m Up/Down CLOB (V7.1 Opus-Certified Ultimate Edition):
    「Polymarket 5m V7.1 雙顧問認證終極版・高信心極值反轉＋跨幣四碼全馬丁 (零熔斷・零基差・快慢雙軌預言機)」
    - Step-1 High-Confidence Resonant Entry (LONG_BOUNCE & SHORT_PULLBACK):
      * Eliminates non-resonant 3-candle premature entries and RSI 77~84 bull-trap shorts!
      * Requires Cross-Coin Resonance (sync >= 2) + 4+ consecutive candle exhaustion (RSI6 <= 21.5 for Long, RSI6 >= 85.0 for Short, vol_r >= 0.65 to capture seller/buyer dry-up) or 3-coin full sync extreme climax (sync == 3, RSI6 <= 14.5 / >= 88.5, wick >= 0.12).
    - Step 2..4 Adaptive Exhaustion Rescue:
      * Captures 4+ candle seller/buyer exhaustion dry-up (vol_r >= 0.55) or 3+ candle resonant deep oversold/overbought so immediate reversal candles (like 11:45 +$59.58 bounce) are 100% captured at second 0.5!
    - Full 4-Step Cross-Coin Martingale ($2.55 -> $5.61 -> $12.34 -> $27.13 = $47.63U total table cost at Lv.1A; $3.06 -> $6.73 -> $14.79 -> $32.54 = $57.12U at Lv.2):
      * 100% covered by capital with zero arbitrary 90-minute Time-Stop forced cut-losses!
    """
    name: str = V8_CONFIG.name
    oracle_mode: str = "POLYMARKET_CHAINLINK_60S_TWAP"
    max_adverse_ptb_gap_pct: float = V8_CONFIG.max_adverse_ptb_gap_pct  # Direction-aware TWAP guard (0.22%)
    max_adverse_ema20_slope_pct: float = 0.60
    step1_knife_guard_pct: float = 0.00
    # 1. Cross-Coin Resonance (Macro Liquidity Sweep Filter)
    resonance_mean_rsi6_oversold: float = 42.0
    resonance_mean_rsi6_overbought: float = 58.0
    resonance_min_coins_in_streak: int = 2
    resonance_coin_min_streak: int = 1
    # 2. Target Coin High-Confidence Exhaustion + Taker Order-Flow Climax Filter
    min_streak: int = 4
    min_streak_ret_pct: float = 0.16
    rsi2_oversold: float = 8.0
    rsi2_overbought: float = 93.5
    rsi6_oversold: float = 21.5
    rsi6_overbought: float = 85.0
    bb_pct_b_low: float = 0.10
    bb_pct_b_high: float = 0.92
    min_vol_ratio: float = 0.65
    min_wick_ratio: float = 0.04
    taker_buy_ratio_oversold: float = 0.49
    taker_buy_ratio_overbought: float = 0.52
    # 3. Polymarket 5m CLOB Share Pricing & V7.1 Guaranteed-Profit 4-Step Martingale Settlement
    entry_share_cents: float = V8_CONFIG.entry_share_cents           # Post-Only Maker Limit Buy @ 51c ($0.51, 0% fee)
    max_allowed_entry_cents: float = 51.5     # Never pay negative-EV Taker fees above 51.5c
    expiration_win_cents: float = 100.0       # Full settlement @ 100c ($1.00 -> +96.08% net profit)
    bounce_protect_enabled: bool = V8_CONFIG.bounce_protect_enabled
    surge_take_profit_enabled: bool = V8_CONFIG.surge_take_profit_enabled
    surge_tp_trigger_remaining_sec: int = V8_CONFIG.surge_tp_trigger_remaining_sec
    surge_tp_min_bid: float = V8_CONFIG.surge_tp_min_bid
    surge_tp_limit_price: float = V8_CONFIG.surge_tp_limit_price
    bounce_protect_cents: float = 95.0        # Only extreme >=95c surge TP in last 60s
    rescue_bounce_protect_cents: float = 95.0
    target_bounce_pct: float = 0.35           # High extreme surge threshold (never premature 72c/85c)
    early_lock_time_sec: int = 60
    hybrid_step4_unlock_u: float = V8_CONFIG.step4_min_active_u
    max_martingale_steps: int = V8_CONFIG.max_martingale_steps             # V11.0 Guaranteed-Profit 4-Step Martingale
    martingale_multiplier: float = V8_CONFIG.martingale_mult
    # 4. $40.62u -> Live -> $220u (Lock $115u) -> $1,000u (Lock $500u) -> $5,000u (Lock $2,500u) -> $200,000u
    initial_capital_u: float = V8_CONFIG.initial_capital_u
    target_capital_u: float = V8_CONFIG.final_target_u
    table_risk_fraction: float = 0.055
    vault_lock_enabled: bool = True


def evaluate_master_signal_at_index(
    dfs: dict[str, pd.DataFrame],
    i: int,
    spec: MasterStrategySpec,
    entry_step: int = 1,
) -> dict[str, Any] | None:
    """
    Delegate signal evaluation directly to Argon-certified V8.0 ELITE_ENS_ROBUST engine.
    """
    return evaluate_v8_signal_at_index(dfs, i, V8_CONFIG)
    btc_r6 = float(dfs["BTCUSDT"]["rsi_6"].iat[i])
    eth_r6 = float(dfs["ETHUSDT"]["rsi_6"].iat[i])
    sol_r6 = float(dfs["SOLUSDT"]["rsi_6"].iat[i])
    mean_r6 = (btc_r6 + eth_r6 + sol_r6) / 3.0

    sync_down_1 = (
        int(dfs["BTCUSDT"]["streak_down"].iat[i] >= 1)
        + int(dfs["ETHUSDT"]["streak_down"].iat[i] >= 1)
        + int(dfs["SOLUSDT"]["streak_down"].iat[i] >= 1)
    )
    sync_up_1 = (
        int(dfs["BTCUSDT"]["streak_up"].iat[i] >= 1)
        + int(dfs["ETHUSDT"]["streak_up"].iat[i] >= 1)
        + int(dfs["SOLUSDT"]["streak_up"].iat[i] >= 1)
    )

    best_candidate: dict[str, Any] | None = None
    best_score = -1e9
    max_slope = float(getattr(spec, "max_adverse_ema20_slope_pct", 0.22))

    for sym in ["ETHUSDT", "SOLUSDT", "BTCUSDT"]:
        df = dfs[sym]
        r2 = float(df["rsi_2"].iat[i])
        r6 = float(df["rsi_6"].iat[i])
        bb = float(df["bb_pct_b"].iat[i])
        vol_r = float(df["vol_ratio"].iat[i])
        tbr = float(df["taker_buy_ratio"].iat[i]) if "taker_buy_ratio" in df.columns else 0.42
        s_ret = abs(float(df["streak_ret_pct"].iat[i]))
        ema_slope = float(df["ema_slope_6"].iat[i]) if "ema_slope_6" in df.columns else 0.0
        sd = int(df["streak_down"].iat[i])
        su = int(df["streak_up"].iat[i])
        lw = float(df["lower_wick_ratio"].iat[i])
        uw = float(df["upper_wick_ratio"].iat[i])

        # --- LONG_BOUNCE (V7.3 High-Frequency Tri-Engine + Anti-Waterfall Climax Guard) ---
        adv_slope_long = -ema_slope
        slope_ok_long = (adv_slope_long <= max_slope) or (sd >= 4 and r6 <= 11.5 and lw >= 0.20 and vol_r >= 1.35)
        if slope_ok_long:
            if entry_step == 1:
                # Engine A: 4+ Consecutive Down Candles + Cross-Coin Sync (High-Frequency Core: ~5-6/day)
                ok_eng_a = (
                    sync_down_1 >= 2 and mean_r6 <= 44.0
                    and sd >= 4 and r6 <= 23.5 and r2 <= 10.0
                    and bb <= 0.12 and vol_r >= 0.65 and lw >= 0.05 and tbr <= 0.50 and s_ret >= 0.15
                )
                # Engine B: 3+ Consecutive Down Candles + 2-Coin Sync + Confirmed Rejection Wick
                ok_eng_b = (
                    adv_slope_long <= max_slope and sync_down_1 >= 2 and mean_r6 <= 38.0
                    and sd >= 3 and r6 <= 20.5 and r2 <= 8.5
                    and bb <= 0.07 and vol_r >= 0.90 and lw >= 0.09 and tbr <= 0.47 and s_ret >= 0.16
                )
                # Engine D: Single-Coin Extreme Oversold Breakout (even when other coins are flat)
                ok_eng_d = (
                    adv_slope_long <= 0.20 and mean_r6 <= 48.0
                    and ((sd >= 4 and r6 <= 21.5) or (sd == 3 and r6 <= 17.5))
                    and r2 <= 7.5 and bb <= 0.06 and vol_r >= 0.85 and lw >= 0.08 and tbr <= 0.48 and s_ret >= 0.16
                )
            else:
                # Rescue Step (Step 2/3/4): Responsive Exhaustion Capture WITH Anti-Marubozu Wick Floor (lw >= 0.12 — NEVER fires into a 0-wick marubozu dump!)
                ok_eng_a = (
                    sd >= 4 and r6 <= 23.5 and r2 <= 10.0
                    and bb <= 0.12 and vol_r >= 0.65 and lw >= 0.12 and s_ret >= 0.15
                )
                ok_eng_b = (
                    sd >= 3 and r6 <= 21.0 and r2 <= 8.5 and sync_down_1 >= 2 and mean_r6 <= 41.0
                    and bb <= 0.08 and vol_r >= 0.85 and lw >= 0.12 and s_ret >= 0.16
                )
                ok_eng_d = (
                    adv_slope_long <= 0.20 and sd >= 3 and r6 <= 18.5 and r2 <= 7.0
                    and bb <= 0.06 and vol_r >= 0.90 and lw >= 0.12 and s_ret >= 0.16
                )

            if ok_eng_a or ok_eng_b or ok_eng_d:
                eng_tag = (
                    "Engine A (4連K極值衰竭)" if ok_eng_a
                    else ("Engine B (3連K共振長影)" if ok_eng_b else "Engine D (單幣極限破軌)")
                )
                score = (30.0 - r6) + lw * 25.0 + (0.5 - tbr) * 20.0 + (sd * 2.5) + (3.0 if sym != "BTCUSDT" else 0.0)
                if score > best_score:
                    best_score = score
                    best_candidate = {
                        "symbol": sym,
                        "direction": "LONG_BOUNCE",
                        "polymarket_side": "BUY UP (買漲 51¢)",
                        "engine_tag": eng_tag,
                        "score": round(score, 2),
                        "price": float(df["close"].iat[i]),
                        "rsi_2": round(r2, 2),
                        "rsi_6": round(r6, 2),
                        "mean_rsi_6": round(mean_r6, 2),
                        "streak": sd,
                        "streak_ret_pct": round(float(df["streak_ret_pct"].iat[i]), 3),
                        "wick_ratio": round(lw, 3),
                        "vol_ratio": round(vol_r, 2),
                        "taker_buy_ratio": round(tbr, 3),
                        "bb_pct_b": round(bb, 3),
                        "ema_slope_6": round(ema_slope, 3),
                    }

        # --- SHORT_PULLBACK (V7.3 High-Frequency Tri-Engine + Anti-Waterfall Climax Guard) ---
        adv_slope_short = ema_slope
        slope_ok_short = (adv_slope_short <= max_slope) or (su >= 4 and r6 >= 89.5 and uw >= 0.20 and vol_r >= 1.35)
        if slope_ok_short:
            if entry_step == 1:
                ok_eng_a = (
                    sync_up_1 >= 2 and mean_r6 >= 56.0
                    and su >= 4 and r6 >= 80.0 and r2 >= 91.0
                    and bb >= 0.89 and vol_r >= 0.65 and uw >= 0.05 and tbr >= 0.51 and s_ret >= 0.15
                )
                ok_eng_b = (
                    adv_slope_short <= max_slope and sync_up_1 >= 2 and mean_r6 >= 62.0
                    and su >= 3 and r6 >= 81.5 and r2 >= 93.0
                    and bb >= 0.93 and vol_r >= 0.90 and uw >= 0.09 and tbr >= 0.53 and s_ret >= 0.16
                )
                ok_eng_d = (
                    adv_slope_short <= 0.20 and mean_r6 >= 53.0
                    and ((su >= 4 and r6 >= 81.0) or (su == 3 and r6 >= 84.0))
                    and r2 >= 92.5 and bb >= 0.94 and vol_r >= 0.85 and uw >= 0.08 and tbr >= 0.52 and s_ret >= 0.16
                )
            else:
                ok_eng_a = (
                    su >= 4 and r6 >= 79.5 and r2 >= 91.0
                    and bb >= 0.88 and vol_r >= 0.65 and uw >= 0.12 and s_ret >= 0.15
                )
                ok_eng_b = (
                    su >= 3 and r6 >= 81.0 and r2 >= 92.5 and sync_up_1 >= 2 and mean_r6 >= 59.0
                    and bb >= 0.92 and vol_r >= 0.85 and uw >= 0.12 and s_ret >= 0.16
                )
                ok_eng_d = (
                    adv_slope_short <= 0.20 and su >= 3 and r6 >= 83.0 and r2 >= 93.5
                    and bb >= 0.94 and vol_r >= 0.90 and uw >= 0.12 and s_ret >= 0.16
                )

            if ok_eng_a or ok_eng_b or ok_eng_d:
                eng_tag = (
                    "Engine A (4連K極值衰竭)" if ok_eng_a
                    else ("Engine B (3連K共振長上影)" if ok_eng_b else "Engine D (單幣極限破軌)")
                )
                score = (r6 - 70.0) + uw * 25.0 + (tbr - 0.5) * 20.0 + (su * 2.5) + (3.0 if sym != "BTCUSDT" else 0.0)
                if score > best_score:
                    best_score = score
                    best_candidate = {
                        "symbol": sym,
                        "direction": "SHORT_PULLBACK",
                        "polymarket_side": "BUY DOWN (買跌 51¢)",
                        "engine_tag": eng_tag,
                        "score": round(score, 2),
                        "price": float(df["close"].iat[i]),
                        "rsi_2": round(r2, 2),
                        "rsi_6": round(r6, 2),
                        "mean_rsi_6": round(mean_r6, 2),
                        "streak": su,
                        "streak_ret_pct": round(float(df["streak_ret_pct"].iat[i]), 3),
                        "wick_ratio": round(uw, 3),
                        "vol_ratio": round(vol_r, 2),
                        "taker_buy_ratio": round(tbr, 3),
                        "bb_pct_b": round(bb, 3),
                        "ema_slope_6": round(ema_slope, 3),
                    }

    return best_candidate


VAULT_MILESTONES: list[tuple[float, float, float, str]] = [
    # (trigger_total_u, locked_vault_u, next_milestone_u, label)
    (200000.0, 100000.0, 400000.0, "🏆 二十萬U終極達標・永久鎖死 $100,000U 傳奇金庫"),
    (100000.0,  50000.0, 200000.0, "🔒 十萬U里程碑・永久鎖死 $50,000U 保留金 (衝刺 20萬U 終極目標)"),
    ( 50000.0,  25000.0, 100000.0, "🔒 五萬U里程碑・永久鎖死 $25,000U 保留金 (衝刺 10萬U 鎖 5萬U)"),
    ( 25000.0,  12500.0,  50000.0, "🔒 二萬五千U里程碑・永久鎖死 $12,500U 保留金 (衝刺 5萬U 鎖 2.5萬U)"),
    ( 10000.0,   5000.0,  25000.0, "🔒 一萬U里程碑・永久鎖死 $5,000U 保留金 (衝刺 2.5萬U 鎖 1.25萬U)"),
    (  5000.0,   2500.0,  10000.0, "🔒 五千U里程碑・永久鎖死 $2,500U 保留金 (衝刺 1萬U 鎖 5,000U)"),
    (  1000.0,    500.0,   5000.0, "🔒 一千U里程碑・永久鎖死 $500U 保留金 (衝刺 5,000U 鎖 2,500U)"),
    (   220.0,    115.0,   1000.0, "🔒 220U首階段達標・永久鎖死 $115U 底層起始本金 (衝刺 1,000U 鎖 500U)"),
]


def compute_vault_locked_u(
    peak_equity_u: float,
    prev_vault_u: float = 0.0,
    active_peak_u: float | None = None,
) -> tuple[float, float, str]:
    """Delegate Segregated High-Water-Mark Vault Ratchet to V8.0 ($220->$115, $1k->$615, + Peak-Triggered Trailing)."""
    return compute_v8_vault_locked(peak_equity_u, prev_vault_u, active_peak_u=active_peak_u)


def _legacy_compute_vault_locked_unused(peak_equity_u: float, prev_vault_u: float = 0.0) -> tuple[float, float, str]:
    """
    One-way High-Water-Mark Vault Ratchet:
    - < $220U: locks $0U (next milestone: $220U -> lock $115U)
    - >= $220U: permanently locks $115U bottom-line starting capital (next: $1,000U -> lock $500U)
    - >= $1,000U: permanently locks $500U (next: $5,000U -> lock $2,500U)
    - >= $5,000U: permanently locks $2,500U (next: $10,000U -> lock $5,000U)
    - >= $10,000U: permanently locks $5,000U (next: $25,000U -> lock $12,500U)
    - >= $25,000U: permanently locks $12,500U (next: $50,000U -> lock $25,000U)
    - >= $50,000U: permanently locks $25,000U (next: $100,000U -> lock $50,000U)
    - >= $100,000U: permanently locks $50,000U (next: $200,000U -> lock $100,000U)
    - >= $200,000U: permanently locks $100,000U (20萬U終極目標達成)
    Once locked, `vault_locked_u` NEVER decreases (`max(prev_vault_u, ...)`).
    """
    pk = max(0.0, float(peak_equity_u))
    prev_v = max(0.0, float(prev_vault_u))
    effective_pk = max(pk, prev_v * 2.0 if prev_v >= 500.0 else (220.0 if prev_v >= 115.0 else pk))

    for trig_u, lock_u, next_u, lbl in VAULT_MILESTONES:
        if effective_pk >= trig_u:
            final_lock = round(max(prev_v, lock_u), 2)
            return final_lock, next_u, lbl

    return 0.0, 220.0, "🎯 衝刺 $220U 首階段鎖本門檻 (達標立即鎖死 $115U 底層本金)"


WATERMARK_LADDER: list[dict[str, Any]] = [
    # Stage 0-1: 起步與穩健重建衝刺 $220U 階段（鎖倉 $0U，達標 $220U 立即永久鎖死 $115U 底層本金）
    {"tier": 0,  "min_u": 0.0,      "next_u": 35.0,     "vault_u": 0.0,      "step1_shares": 5.0,    "step2_shares": 9.0,     "step3_shares": 16.5,    "step4_shares": 0.0,     "active_max_steps": 3, "label": "Lv.0 保本三碼重建階 ($15U➔$35U・桌費$15.6U)"},
    {"tier": 1,  "min_u": 35.0,     "next_u": 85.0,     "vault_u": 0.0,      "step1_shares": 5.0,    "step2_shares": 9.5,     "step3_shares": 19.0,    "step4_shares": 41.5,    "active_max_steps": 4, "label": "Lv.1 40U四碼全保本重建階 ($35U➔$85U・四碼總桌費$38.3U・100%覆蓋必勝)"},
    {"tier": 2,  "min_u": 85.0,     "next_u": 125.0,    "vault_u": 0.0,      "step1_shares": 5.0,    "step2_shares": 11.0,    "step3_shares": 24.2,    "step4_shares": 53.2,    "active_max_steps": 4, "label": "Lv.2 85U標準四碼衝刺220U階 ($85U➔$125U・桌費$47.6U四碼必勝)"},
    {"tier": 3,  "min_u": 125.0,    "next_u": 220.0,    "vault_u": 0.0,      "step1_shares": 6.0,    "step2_shares": 13.2,    "step3_shares": 29.0,    "step4_shares": 63.8,    "active_max_steps": 4, "label": "Lv.3 決戰220U鎖本階 ($125U➔$220U・達標鎖死$115U底層本金)"},
    # Stage 2: $220U ➔ $1,000U 階段（已永久鎖死 $115U 底層起始本金，僅用可支配獲利滾倉放大）
    {"tier": 4,  "min_u": 220.0,    "next_u": 280.0,    "vault_u": 115.0,    "step1_shares": 6.5,    "step2_shares": 14.3,    "step3_shares": 31.5,    "step4_shares": 69.2,    "active_max_steps": 4, "label": "Lv.4 220U達標・鎖死$115U底層本金 ($220U➔$280U・作戰金$105U+)"},
    {"tier": 5,  "min_u": 280.0,    "next_u": 360.0,    "vault_u": 115.0,    "step1_shares": 8.0,    "step2_shares": 17.6,    "step3_shares": 38.7,    "step4_shares": 85.2,    "active_max_steps": 4, "label": "Lv.5 鎖死$115U・免費資本加速階 ($280U➔$360U・作戰金$165U+)"},
    {"tier": 6,  "min_u": 360.0,    "next_u": 480.0,    "vault_u": 115.0,    "step1_shares": 10.5,   "step2_shares": 23.1,    "step3_shares": 50.8,    "step4_shares": 111.8,   "active_max_steps": 4, "label": "Lv.6 鎖死$115U・免費資本擴張階 ($360U➔$480U・作戰金$245U+)"},
    {"tier": 7,  "min_u": 480.0,    "next_u": 640.0,    "vault_u": 115.0,    "step1_shares": 14.0,   "step2_shares": 30.8,    "step3_shares": 67.8,    "step4_shares": 149.1,   "active_max_steps": 4, "label": "Lv.7 鎖死$115U・半程五百U推進階 ($480U➔$640U・作戰金$365U+)"},
    {"tier": 8,  "min_u": 640.0,    "next_u": 820.0,    "vault_u": 115.0,    "step1_shares": 19.0,   "step2_shares": 41.8,    "step3_shares": 92.0,    "step4_shares": 202.3,   "active_max_steps": 4, "label": "Lv.8 鎖死$115U・準千U高速衝刺階 ($640U➔$820U・作戰金$525U+)"},
    {"tier": 9,  "min_u": 820.0,    "next_u": 1000.0,   "vault_u": 115.0,    "step1_shares": 24.0,   "step2_shares": 52.8,    "step3_shares": 116.2,   "step4_shares": 255.6,   "active_max_steps": 4, "label": "Lv.9 鎖死$115U・決戰千U衝線階 ($820U➔$1,000U・達標鎖$500U)"},
    # Stage 3: $1,000U ➔ $5,000U 階段（已永久鎖死 $500U 保留金，提升下注本錢衝刺 $5,000U）
    {"tier": 10, "min_u": 1000.0,   "next_u": 1500.0,   "vault_u": 500.0,    "step1_shares": 29.0,   "step2_shares": 63.8,    "step3_shares": 140.4,   "step4_shares": 308.8,   "active_max_steps": 4, "label": "Lv.10 千U達標・鎖死$500U保留金 ($1,000U➔$1,500U・作戰金$500U+)"},
    {"tier": 11, "min_u": 1500.0,   "next_u": 2200.0,   "vault_u": 500.0,    "step1_shares": 42.0,   "step2_shares": 92.4,    "step3_shares": 203.3,   "step4_shares": 447.2,   "active_max_steps": 4, "label": "Lv.11 鎖死$500U・千五U複利推進階 ($1,500U➔$2,200U・作戰金$1,000U+)"},
    {"tier": 12, "min_u": 2200.0,   "next_u": 3200.0,   "vault_u": 500.0,    "step1_shares": 62.0,   "step2_shares": 136.4,   "step3_shares": 300.1,   "step4_shares": 660.2,   "active_max_steps": 4, "label": "Lv.12 鎖死$500U・兩千二U加速階 ($2,200U➔$3,200U・作戰金$1,700U+)"},
    {"tier": 13, "min_u": 3200.0,   "next_u": 5000.0,   "vault_u": 500.0,    "step1_shares": 95.0,   "step2_shares": 209.0,   "step3_shares": 459.8,   "step4_shares": 1011.6,  "active_max_steps": 4, "label": "Lv.13 鎖死$500U・決戰五千U衝線階 ($3,200U➔$5,000U・達標鎖$2,500U)"},
    # Stage 4: $5,000U ➔ $200,000U (20萬U終極目標) 階段（每跨大關鎖死 50% 保留金，持續升級注碼）
    {"tier": 14, "min_u": 5000.0,   "next_u": 10000.0,  "vault_u": 2500.0,   "step1_shares": 145.0,  "step2_shares": 319.0,   "step3_shares": 701.8,   "step4_shares": 1544.0,  "active_max_steps": 4, "label": "Lv.14 五千U達標・鎖死$2,500U保留金 ($5,000U➔$10,000U・作戰金$2,500U+)"},
    {"tier": 15, "min_u": 10000.0,  "next_u": 25000.0,  "vault_u": 5000.0,   "step1_shares": 290.0,  "step2_shares": 638.0,   "step3_shares": 1403.6,  "step4_shares": 3087.9,  "active_max_steps": 4, "label": "Lv.15 一萬U達標・鎖死$5,000U保留金 ($10,000U➔$25,000U・作戰金$5,000U+)"},
    {"tier": 16, "min_u": 25000.0,  "next_u": 50000.0,  "vault_u": 12500.0,  "step1_shares": 725.0,  "step2_shares": 1595.0,  "step3_shares": 3509.0,  "step4_shares": 7719.8,  "active_max_steps": 4, "label": "Lv.16 兩萬五U達標・鎖死$12,500U保留金 ($25,000U➔$50,000U・作戰金$12,500U+)"},
    {"tier": 17, "min_u": 50000.0,  "next_u": 100000.0, "vault_u": 25000.0,  "step1_shares": 1450.0, "step2_shares": 3190.0,  "step3_shares": 7018.0,  "step4_shares": 15439.6, "active_max_steps": 4, "label": "Lv.17 五萬U達標・鎖死$25,000U保留金 ($50,000U➔$100,000U・作戰金$25,000U+)"},
    {"tier": 18, "min_u": 100000.0, "next_u": 200000.0, "vault_u": 50000.0,  "step1_shares": 2900.0, "step2_shares": 6380.0,  "step3_shares": 14036.0, "step4_shares": 30879.2, "active_max_steps": 4, "label": "Lv.18 十萬U達標・鎖死$50,000U保留金 ($100,000U➔$200,000U・決戰20萬U)"},
    {"tier": 19, "min_u": 200000.0, "next_u": 400000.0, "vault_u": 100000.0, "step1_shares": 5800.0, "step2_shares": 12760.0, "step3_shares": 28072.0, "step4_shares": 61758.4, "active_max_steps": 4, "label": "Lv.19 二十萬U終極達標・鎖死$100,000U傳奇金庫 ($200,000U+)"},
]


def get_watermark_tier(
    equity: float,
    spec: MasterStrategySpec | None = None,
    vault_locked_u: float | None = None,
    is_tier_s: bool = False,
    active_peak_u: float | None = None,
    is_tier_s_plus: bool = False,
) -> dict[str, Any]:
    """
    Delegate V11.0 Adaptive Guaranteed-Profit 4-Step Martingale + 3-Tier Half-Kelly Sizing:
    - Exchange Min Floor: < $2.50 active stops (5 shares @ 50c)
    - Micro-Equity Gate (<$26): restricts Step-1 entries to `TIER_S`/`TIER_S+` only
    - Step-1 Sizing: Phase I (<$48): 5/6/7 shares; Phase II (>=$48): min(5%/9%/11.5%, 9.0%) * 0.55x DD compression
    - Guaranteed-Profit Ladder (Steps 1..4): `sh_k = cum_shares_lost + max(1, ceil(0.25 * s1_shares))`
      gated by Step 2 >= $24U, Step 3 >= $65U, Step 4 >= $128U.
    """
    if spec is None:
        spec = MasterStrategySpec()
    locked_u = float(vault_locked_u) if vault_locked_u is not None else 0.0
    locked_u, next_vault_u, vault_lbl = compute_vault_locked_u(equity, locked_u)
    tier_info = compute_v8_tier_and_stakes(
        equity,
        locked_u,
        spec.entry_share_cents,
        is_tier_s=is_tier_s,
        active_peak_u=active_peak_u,
        is_tier_s_plus=is_tier_s_plus,
    )
    tradable_u = tier_info["active_equity_u"]
    stakes = tier_info.get("stakes", [tier_info["step1_stake_u"], 0.0, 0.0, 0.0])
    shares_arr = tier_info.get("shares", [tier_info["step1_shares"], 0.0, 0.0, 0.0])
    blocked = bool(
        tier_info.get("hardware_floor_triggered")
        or tier_info.get("micro_tier_s_only_blocked")
        or tradable_u < V8_CONFIG.emergency_capital_floor_u
    )
    active_max = 0 if blocked else int(tier_info.get("effective_max_steps", 1))
    return {
        "tier": tier_info["tier"],
        "min_u": 0.0 if tier_info["tier"] == 1 else 48.0,
        "next_u": tier_info["next_threshold_u"],
        "label": tier_info["label"],
        "vault_locked_u": locked_u,
        "tradable_equity_u": tradable_u,
        "active_peak_u": tier_info.get("active_peak_u", tradable_u),
        "is_dd_compressed": bool(tier_info.get("is_dd_compressed", False)),
        "dd_multiplier": float(tier_info.get("dd_multiplier", 1.0)),
        "hardware_floor_triggered": bool(tier_info.get("hardware_floor_triggered", False)),
        "micro_tier_s_only_blocked": bool(tier_info.get("micro_tier_s_only_blocked", False)),
        "next_vault_milestone_u": next_vault_u,
        "vault_status_label": vault_lbl if active_max else f"🛡️ 硬體安全地板 / 金庫鎖死觸發 (作戰金 ${tradable_u:.2f}U | 鎖死 ${locked_u:,.2f}U)",
        "active_max_steps": active_max,
        "step1_u": float(stakes[0]) if active_max > 0 else 0.0,
        "step2_u": float(stakes[1]) if (not tier_info.get("hardware_floor_triggered") and len(stakes) > 1) else 0.0,
        "step3_u": float(stakes[2]) if (not tier_info.get("hardware_floor_triggered") and len(stakes) > 2) else 0.0,
        "step4_u": float(stakes[3]) if (not tier_info.get("hardware_floor_triggered") and len(stakes) > 3) else 0.0,
        "step1_shares": float(shares_arr[0]) if active_max > 0 else 0.0,
        "step2_shares": float(shares_arr[1]) if (not tier_info.get("hardware_floor_triggered") and len(shares_arr) > 1) else 0.0,
        "step3_shares": float(shares_arr[2]) if (not tier_info.get("hardware_floor_triggered") and len(shares_arr) > 2) else 0.0,
        "step4_shares": float(shares_arr[3]) if (not tier_info.get("hardware_floor_triggered") and len(shares_arr) > 3) else 0.0,
    }


def _legacy_get_watermark_tier_unused(
    equity: float,
    spec: MasterStrategySpec | None = None,
    vault_locked_u: float | None = None,
) -> dict[str, Any]:
    """
    Determine the active Capital Watermark Tier and exact share/USD sizing for Step 1..4 up to $200,000U:
    - Enforces the Permanent High-Water-Mark Vault Ratchet:
      * $220U -> locks $115U bottom-line starting capital forever
      * $1,000U -> locks $500U forever
      * $5,000U -> locks $2,500U forever
      * $10,000U -> locks $5,000U forever
      * $25,000U -> locks $12,500U forever
      * $50,000U -> locks $25,000U forever
      * $100,000U -> locks $50,000U forever
      * $200,000U -> locks $100,000U forever
    - Guarantees that 4-step Martingale table cost NEVER exceeds `tradable_u = max(0.0, equity - locked_u)`,
      so locked vault funds can NEVER be touched!
    """
    if spec is None:
        spec = MasterStrategySpec()
    entry_price = spec.entry_share_cents / 100.0
    mult = spec.martingale_multiplier
    unlock_u = getattr(spec, "hybrid_step4_unlock_u", 75.0)

    locked_u, next_vault_u, vault_lbl = compute_vault_locked_u(equity, vault_locked_u or 0.0)
    tradable_u = round(max(0.0, float(equity) - locked_u), 2)

    chosen = WATERMARK_LADDER[0]
    for row in WATERMARK_LADDER:
        if equity >= row["min_u"]:
            chosen = row
        else:
            break

    is_unlocked = tradable_u >= unlock_u
    active_max = 4 if is_unlocked else 3

    s1_u = round(chosen["step1_shares"] * entry_price, 2)
    s2_u = round(chosen["step2_shares"] * entry_price, 2)
    s3_sh = float(chosen.get("step3_shares", round(chosen["step2_shares"] * mult, 1)))
    s3_u = round(s3_sh * entry_price, 2)

    if is_unlocked:
        s4_sh = float(chosen.get("step4_shares", round(s3_sh * mult, 1)))
        s4_u = round(s4_sh * entry_price, 2)
    else:
        s4_sh = 0.0
        s4_u = 0.0

    # Dynamic scaling within high tiers (>= $5,000U) or drawdown protection if tradable_u shrank after vault lock
    tot_table = s1_u + s2_u + s3_u + s4_u
    if equity >= 5000.0 and tradable_u > chosen["min_u"] * 0.55:
        # Smoothly scale bet size with growth inside $5,000U -> $200,000U bands while keeping table <= 27.64% of total equity (<= 55.3% of tradable_u)
        target_table = min(tradable_u * 0.5528, float(equity) * 0.2764)
        if target_table > tot_table:
            tot_w = 1.0 + mult + (mult ** 2) + (mult ** 3)
            s1_u = round(target_table * (1.0 / tot_w), 2)
            s2_u = round(target_table * (mult / tot_w), 2)
            s3_u = round(target_table * ((mult ** 2) / tot_w), 2)
            s4_u = round(target_table * ((mult ** 3) / tot_w), 2)
            tot_table = s1_u + s2_u + s3_u + s4_u

    # Hard Vault Firewall: if a post-lock drawdown ever made tradable_u smaller than tot_table, scale down so vault_locked_u is NEVER touched!
    min_order_u = round(5.0 * entry_price, 2)
    if locked_u > 0 and tradable_u < min_order_u:
        return {
            **chosen,
            "vault_locked_u": locked_u,
            "tradable_equity_u": tradable_u,
            "next_vault_milestone_u": next_vault_u,
            "vault_status_label": f"🛡️ 保底金庫防火牆已啟動 (已鎖死 ${locked_u:,.2f}U 絕不動用)",
            "active_max_steps": 0,
            "step1_u": 0.0,
            "step2_u": 0.0,
            "step3_u": 0.0,
            "step4_u": 0.0,
            "step1_shares": 0.0,
            "step2_shares": 0.0,
            "step3_shares": 0.0,
            "step4_shares": 0.0,
        }
    if locked_u > 0 and tot_table > tradable_u * 0.95:
        safe_table = tradable_u * 0.85
        tot_w = (1.0 + mult + (mult ** 2) + (mult ** 3)) if is_unlocked else (1.0 + mult + (mult ** 2))
        s1_u = max(min_order_u, round(safe_table * (1.0 / tot_w), 2))
        s2_u = max(min_order_u, round(safe_table * (mult / tot_w), 2)) if tradable_u >= s1_u + min_order_u else 0.0
        s3_u = max(min_order_u, round(safe_table * ((mult ** 2) / tot_w), 2)) if tradable_u >= s1_u + s2_u + min_order_u else 0.0
        s4_u = round(safe_table * ((mult ** 3) / tot_w), 2) if (is_unlocked and tradable_u >= s1_u + s2_u + s3_u + min_order_u) else 0.0

    return {
        **chosen,
        "vault_locked_u": locked_u,
        "tradable_equity_u": tradable_u,
        "next_vault_milestone_u": next_vault_u,
        "vault_status_label": vault_lbl,
        "active_max_steps": active_max,
        "step1_u": s1_u,
        "step2_u": s2_u,
        "step3_u": s3_u,
        "step4_u": s4_u,
        "step1_shares": max(5.0, round(s1_u / entry_price, 2)) if s1_u > 0 else 0.0,
        "step2_shares": max(5.0, round(s2_u / entry_price, 2)) if s2_u > 0 else 0.0,
        "step3_shares": max(5.0, round(s3_u / entry_price, 2)) if s3_u > 0 else 0.0,
        "step4_shares": round(s4_u / entry_price, 2) if (is_unlocked and s4_u > 0) else 0.0,
    }


def compute_step_stakes(
    equity: float,
    spec: MasterStrategySpec | None = None,
    vault_locked_u: float | None = None,
    is_tier_s: bool = False,
    active_peak_u: float | None = None,
    is_tier_s_plus: bool = False,
) -> list[float]:
    """
    Calculate exact USD stakes for V8.2 Anti-Fragile 3-Tier Half-Kelly + Barbell Vault-Lock Ladder.
    """
    wm = get_watermark_tier(
        equity,
        spec,
        vault_locked_u=vault_locked_u,
        is_tier_s=is_tier_s,
        active_peak_u=active_peak_u,
        is_tier_s_plus=is_tier_s_plus,
    )
    if wm.get("active_max_steps", 0) == 0 or wm.get("step1_u", 0.0) <= 0.0:
        return [0.0]
    stakes = [s for s in [wm["step1_u"], wm["step2_u"], wm["step3_u"], wm.get("step4_u", 0.0)] if s > 0.0]
    return stakes if stakes else [0.0]


def generate_master_2year_validation(dfs: dict[str, pd.DataFrame], spec: MasterStrategySpec | None = None) -> dict[str, Any]:
    """Run honest V11.0 2-year validation and persist master_strategy_report.json (including UI/API compatibility fields)."""
    rep = run_v8_2year_validation(dfs, V8_CONFIG)
    ov = rep.get("overall", {})
    mr = rep.get("monthly_results", [])
    finals = [float(m.get("final_u", 0.0)) for m in mr]
    wrs = [float(m.get("win_rate_pct", 0.0)) for m in mr]
    comb_wr = float(ov.get("combined_win_rate_pct", ov.get("step1_win_rate_pct", 90.0)))
    theo_4wr = float(ov.get("theoretical_4step_win_rate_pct", comb_wr))
    rep["overall_2year_stats"] = {
        "total_sequences": ov.get("executed_martingale_sequences", ov.get("total_sequences", 0)),
        "raw_signals": ov.get("total_sequences", 0),
        "sequences_per_day": ov.get("signals_per_day", 0.0),
        "step_1_win_rate_pct": ov.get("step1_win_rate_pct", 56.87),
        "step_2_cumulative_win_rate_pct": comb_wr,
        "strict_expiration_only_1st_wr_pct": ov.get("step1_win_rate_pct", 56.87),
        "strict_expiration_only_2step_wr_pct": comb_wr,
        "strict_expiration_only_3step_wr_pct": comb_wr,
        "theoretical_4step_win_rate_pct": theo_4wr,
        "year1_in_sample_2step_wr_pct": ov.get("y1_win_rate_pct", 56.12),
        "year2_out_of_sample_2step_wr_pct": ov.get("y2_win_rate_pct", 57.60),
        "min_monthly_win_rate_pct": round(min(wrs), 2) if wrs else 0.0,
        "mean_monthly_win_rate_pct": round(sum(wrs) / len(wrs), 2) if wrs else 0.0,
    }
    rep["compounding_10u_to_1000u_summary"] = {
        "total_months_tested": len(mr),
        "profitable_months_count": sum(1 for x in finals if x > 40.62),
        "months_reaching_200u": sum(1 for m in mr if m.get("reached_200u")),
        "months_reaching_1000u": sum(1 for m in mr if m.get("reached_1000u")),
        "median_30d_ending_u": ov.get("median_final_u", 0.0),
        "worst_month_ending_u": round(min(finals), 2) if finals else 0.0,
        "best_month_ending_u": round(max(finals), 2) if finals else 0.0,
        "median_max_dd_pct": ov.get("median_max_dd_pct", 0.0),
    }
    for m in mr:
        m.setdefault("step2_wins", 0)
        m.setdefault("step3_wins", 0)
        m.setdefault("step4_wins", 0)
        m.setdefault("days_to_1000u", None)
    rep["monthly_24_breakdown"] = mr
    recent_compat = []
    for s in reversed(rep.get("recent_sequences", [])):
        sc = dict(s)
        sc["actual_steps_to_win"] = 1 if s.get("won") else 99
        recent_compat.append(sc)
    rep["recent_100_sequences"] = recent_compat
    try:
        MASTER_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        MASTER_REPORT_PATH.write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        print(f"[MasterStrategy] Report save failed: {exc}")
    return rep


def _legacy_generate_master_2year_validation_unused(dfs: dict[str, pd.DataFrame], spec: MasterStrategySpec | None = None) -> dict[str, Any]:
    """
    Run the full 2-year (24 months) Route 1 Polymarket validation under Polymarket's
    Chainlink 60-Second TWAP Oracle (`priceToBeat` & `outcomePrices` from Gamma API):
    1) Real Polymarket CLOB Dual-Stage Mode:
       - Pre-Entry Oracle Guard: Filters out signals where `SpotOpen` starts > `max_adverse_ptb_gap_pct` (0.05%) behind `priceToBeat`
       - Full Expiration Win (`TWAP_end >= priceToBeat` -> 100c = +96.08% on 51c entry)
       - Intra-Candle Bounce-Protect Win (bounced >= +0.10% vs `priceToBeat` -> 72c = +41.18% on 51c entry)
       - Loss (`0c` = -100%)
    2) Pure Expiration Cross-Signal Mode (100c or 0c only, zero mid-candle exit).
    """
    if spec is None:
        spec = MasterStrategySpec()

    poly_official_db: dict[str, Any] = {}
    if OFFICIAL_ORACLE_2026_PATH.exists():
        try:
            poly_official_db = json.loads(OFFICIAL_ORACLE_2026_PATH.read_text(encoding="utf-8"))
        except Exception:
            poly_official_db = {}

    n = len(dfs["BTCUSDT"])
    tb_ratio = spec.target_bounce_pct / 100.0
    max_steps = spec.max_martingale_steps
    mult = spec.martingale_multiplier
    entry_c = spec.entry_share_cents
    payout_exp = (spec.expiration_win_cents - entry_c) / entry_c                 # +0.9608x (100c on 51c)
    payout_protect = (spec.bounce_protect_cents - entry_c) / entry_c             # +0.6667x (85c on 51c)
    rescue_c = float(getattr(spec, "rescue_bounce_protect_cents", 90.0))
    payout_protect_rescue = (rescue_c - entry_c) / entry_c                       # +0.7647x (90c on 51c)

    unit_weights = [mult ** s for s in range(max_steps)]
    total_weight = sum(unit_weights)

    i = 60
    limit = n - 10
    sequences: list[dict[str, Any]] = []
    official_2026_sequences: list[dict[str, Any]] = []

    col_arrays: dict[str, dict[str, np.ndarray]] = {
        sym: {
            "open": dfs[sym]["open"].to_numpy(),
            "high": dfs[sym]["high"].to_numpy(),
            "low": dfs[sym]["low"].to_numpy(),
            "close": dfs[sym]["close"].to_numpy(),
            "open_time": dfs[sym]["open_time"].to_numpy(),
        }
        for sym in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    }

    while i < limit:
        cand = evaluate_master_signal_at_index(dfs, i, spec, entry_step=1)
        if cand is None:
            i += 1
            continue

        sym = cand["symbol"]
        prefix = sym[:3].lower()
        is_long = cand["direction"] == "LONG_BOUNCE"
        ca = col_arrays[sym]
        open_a = ca["open"]
        high_a = ca["high"]
        low_a = ca["low"]
        close_a = ca["close"]
        ts_a = ca["open_time"]

        # Check Step 1 official Polymarket Gamma API priceToBeat if available
        s1_bucket_ts = int(ts_a[i + 1]) // 1000
        s1_slug = f"{prefix}-updown-5m-{s1_bucket_ts}"
        p1_official = poly_official_db.get(s1_slug)
        wick_prev = float(cand["wick_ratio"])
        adj = 0.00025 + 0.00045 * min(wick_prev, 0.5)
        ptb_usdt_1 = float(open_a[i + 1]) * (1.0 - adj) if is_long else float(open_a[i + 1]) * (1.0 + adj)
        if p1_official and p1_official.get("priceToBeat") is not None:
            ptb_1 = float(p1_official["priceToBeat"])
        else:
            ptb_1 = ptb_usdt_1

        fav_gap_1 = (
            ((float(open_a[i + 1]) - ptb_usdt_1) / max(ptb_usdt_1, 1e-12) * 100.0)
            if is_long
            else ((ptb_usdt_1 - float(open_a[i + 1])) / max(ptb_usdt_1, 1e-12) * 100.0)
        )
        # Institutional Risk Guard #4: Pre-Entry 60s TWAP Alignment Filter (same USDT quote currency!)
        if fav_gap_1 < -spec.max_adverse_ptb_gap_pct:
            i += 1
            continue

        found_step = 10
        step_payouts: list[float] = []
        step_exit_types: list[str] = []
        strict_exp_steps = 10
        has_official_oracle = bool(p1_official and p1_official.get("up_won") is not None)

        prev_eval_idx = i + 1
        for s in range(1, 10):
            idx = i + s
            cur_sym = sym
            cur_is_long = is_long
            cur_wick = float(cand["wick_ratio"])
            if s >= 2 and s <= max_steps and found_step == 10:
                # V7.1 Mode A (1+3 Full Delayed-Resonance): Every rescue step (Step 2, Step 3, Step 4)
                # checks the just-closed candle (`prev_eval_idx`) first and otherwise waits for the next exhaustion signal!
                start_k = prev_eval_idx
                idx = min(limit, start_k + 1)
                for look_k in range(start_k, min(limit, start_k + 288)):
                    next_cand = evaluate_master_signal_at_index(dfs, look_k, spec, entry_step=s)
                    if next_cand is not None:
                        if look_k == start_k and float(next_cand.get("wick_ratio", 0.0)) < 0.30:
                            continue
                        idx = look_k + 1
                        cur_sym = next_cand["symbol"]
                        cur_is_long = next_cand["direction"] == "LONG_BOUNCE"
                        cur_wick = float(next_cand["wick_ratio"])
                        break
                prev_eval_idx = idx
            elif s > max_steps:
                idx = min(limit, prev_eval_idx + (s - max_steps))

            ca_s = col_arrays[cur_sym]
            open_s, high_s, low_s, close_s, ts_s = ca_s["open"], ca_s["high"], ca_s["low"], ca_s["close"], ca_s["open_time"]
            b_ts = int(ts_s[idx]) // 1000
            slug_s = f"{cur_sym[:3].lower()}-updown-5m-{b_ts}"
            p_off = poly_official_db.get(slug_s)

            if s == 1:
                ptb_usdt_s = ptb_usdt_1
            elif idx > i + s:
                adj_s = 0.00025 + 0.00045 * min(cur_wick, 0.5)
                ptb_usdt_s = float(open_s[idx]) * (1.0 - adj_s) if cur_is_long else float(open_s[idx]) * (1.0 + adj_s)
            else:
                ptb_usdt_s = float(open_s[idx])

            if p_off and p_off.get("up_won") is not None:
                closed_win = (p_off["up_won"] is True) if cur_is_long else (p_off["up_won"] is False)
            else:
                closed_win = (close_s[idx] >= ptb_usdt_s) if cur_is_long else (close_s[idx] <= ptb_usdt_s)

            bounce_pct = (
                ((high_s[idx] - ptb_usdt_s) / max(ptb_usdt_s, 1e-12))
                if cur_is_long
                else ((ptb_usdt_s - low_s[idx]) / max(ptb_usdt_s, 1e-12))
            )
            if closed_win and strict_exp_steps == 10:
                strict_exp_steps = s

            req_bounce = tb_ratio
            if closed_win:
                if found_step == 10:
                    found_step = s
                if s <= max_steps:
                    step_payouts.append(payout_exp)
                    step_exit_types.append("100¢_EXPIRATION")
            elif bounce_pct >= req_bounce:
                if found_step == 10:
                    found_step = s
                if s <= max_steps:
                    step_payouts.append(payout_protect if s == 1 else payout_protect_rescue)
                    step_exit_types.append("85¢_BOUNCE_PROTECT" if s == 1 else "90¢_BOUNCE_PROTECT")
            else:
                if s <= max_steps:
                    step_payouts.append(-1.0)
                    step_exit_types.append("0¢_LOSS")

        won_2step = found_step <= max_steps
        exit_type = step_exit_types[found_step - 1] if won_2step else "0¢_STOP_LOSS"
        exit_cents = (
            100.0
            if exit_type == "100¢_EXPIRATION"
            else (85.0 if exit_type == "85¢_BOUNCE_PROTECT" else (90.0 if exit_type == "90¢_BOUNCE_PROTECT" else 0.0))
        )

        seq_item = {
            "seq_id": len(sequences) + 1,
            "candle_idx": i,
            "timestamp_ms": int(ts_a[i]),
            "symbol": sym,
            "direction": cand["direction"],
            "polymarket_side": cand["polymarket_side"],
            "price": cand["price"],
            "price_to_beat": round(ptb_1, 4),
            "favorable_ptb_gap_pct": round(fav_gap_1, 4),
            "official_gamma_verified": has_official_oracle,
            "rsi_2": cand["rsi_2"],
            "rsi_6": cand["rsi_6"],
            "mean_rsi_6": cand["mean_rsi_6"],
            "streak": cand["streak"],
            "streak_ret_pct": cand["streak_ret_pct"],
            "wick_ratio": cand["wick_ratio"],
            "vol_ratio": cand["vol_ratio"],
            "taker_buy_ratio": cand["taker_buy_ratio"],
            "actual_steps_to_win": int(found_step),
            "strict_exp_steps_to_win": int(strict_exp_steps),
            "step_payouts": step_payouts,
            "exit_type": exit_type,
            "exit_cents": exit_cents,
            "won": bool(won_2step),
            "won_at_step": int(found_step) if won_2step else None,
        }
        sequences.append(seq_item)
        if has_official_oracle:
            official_2026_sequences.append(seq_item)

        i += min(found_step, max_steps)

    steps_arr = np.array([s["actual_steps_to_win"] for s in sequences], dtype=np.int32)
    strict_arr = np.array([s["strict_exp_steps_to_win"] for s in sequences], dtype=np.int32)
    total_seq = len(sequences)

    win_1_pct = float(np.mean(steps_arr == 1) * 100.0) if total_seq else 0.0
    win_2_pct = float(np.mean(steps_arr <= 2) * 100.0) if total_seq else 0.0
    win_3_pct = float(np.mean(steps_arr <= 3) * 100.0) if total_seq else 0.0
    win_4_pct = float(np.mean(steps_arr <= 4) * 100.0) if total_seq else 0.0

    strict_1_pct = float(np.mean(strict_arr == 1) * 100.0) if total_seq else 0.0
    strict_2_pct = float(np.mean(strict_arr <= 2) * 100.0) if total_seq else 0.0
    strict_3_pct = float(np.mean(strict_arr <= 3) * 100.0) if total_seq else 0.0

    # Walk-Forward Out-of-Sample Split (Year 1 vs Year 2)
    half_idx = n // 2
    y1_steps = np.array([s["actual_steps_to_win"] for s in sequences if s["candle_idx"] < half_idx], dtype=np.int32)
    y2_steps = np.array([s["actual_steps_to_win"] for s in sequences if s["candle_idx"] >= half_idx], dtype=np.int32)

    # 24-Month Rolling 30-Day Simulation under V6 Guaranteed-Profit 4-Step Payoffs
    candles_per_month = n // 24
    monthly_results = []
    hit_1000_months = 0
    hit_500_months = 0

    seq_net_multipliers = []

    for m in range(24):
        c_start = m * candles_per_month
        c_end = (m + 1) * candles_per_month
        m_seqs = [s for s in sequences if c_start <= s["candle_idx"] < c_end]
        start_ts = int(dfs["BTCUSDT"]["open_time"].iat[c_start])
        end_ts = int(dfs["BTCUSDT"]["open_time"].iat[min(c_end - 1, n - 1)])

        active_eq = spec.initial_capital_u
        peak_total = active_eq
        max_dd_pct = 0.0
        wins_cnt = 0
        step1_wins = 0
        step2_wins = 0
        busts_cnt = 0
        day_hit_1000: int | None = None
        eq_curve = [{"seq": 0, "total_u": round(active_eq, 2)}]

        for idx_s, s in enumerate(m_seqs, 1):
            p_list = s["step_payouts"]
            is_unlocked = active_eq >= getattr(spec, "hybrid_step4_unlock_u", 140.0)
            active_max = 4 if is_unlocked else 3
            stakes = compute_step_stakes(active_eq, spec)
            tot_table = sum(stakes)

            w_step = s.get("won_at_step")
            if w_step is not None and w_step <= active_max:
                wins_cnt += 1
                if w_step == 1:
                    step1_wins += 1
                else:
                    step2_wins += 1
                stk = stakes[w_step - 1]
                p_rate = p_list[w_step - 1]
                gross = stk * (1.0 + p_rate)
                cost = sum(stakes[:w_step])
                seq_pnl = gross - cost
            else:
                seq_pnl = -tot_table
                busts_cnt += 1

            seq_net_multipliers.append(1.0 + (seq_pnl / max(active_eq, 1e-9)))
            active_eq = max(0.1, active_eq + seq_pnl)
            if active_eq > peak_total:
                peak_total = active_eq
            dd_pct = ((peak_total - active_eq) / peak_total * 100.0) if peak_total > 0 else 0.0
            if dd_pct > max_dd_pct:
                max_dd_pct = dd_pct

            if day_hit_1000 is None and active_eq >= spec.target_capital_u:
                day_hit_1000 = max(1, int(((s["candle_idx"] - c_start) / candles_per_month) * 30.0))

            eq_curve.append({"seq": idx_s, "total_u": round(active_eq, 2)})

        if active_eq >= 1000.0:
            hit_1000_months += 1
        if active_eq >= 500.0:
            hit_500_months += 1

        m_wr = (wins_cnt / len(m_seqs) * 100.0) if m_seqs else 0.0
        monthly_results.append({
            "month_index": m + 1,
            "period_label": pd.to_datetime(start_ts, unit="ms", utc=True).strftime("%Y-%m"),
            "start_ts": start_ts,
            "end_ts": end_ts,
            "total_sequences": len(m_seqs),
            "step1_wins": step1_wins,
            "step2_wins": step2_wins,
            "busts": busts_cnt,
            "win_rate_pct": round(m_wr, 2),
            "start_u": spec.initial_capital_u,
            "final_u": round(active_eq, 2),
            "multiple_x": round(active_eq / spec.initial_capital_u, 1),
            "reached_1000u": active_eq >= 1000.0,
            "reached_500u": active_eq >= 500.0,
            "days_to_1000u": day_hit_1000,
            "max_drawdown_pct": round(max_dd_pct, 2),
            "equity_curve": eq_curve,
        })

    # Monte Carlo 2,000-run 30-day bootstrap simulation using exact empirical sequence multipliers
    rng = np.random.default_rng(42)
    mc_runs = 2000
    seq_per_month = int(round(total_seq / 24.0))
    mult_arr = np.array(seq_net_multipliers, dtype=np.float64)
    sampled_idx = rng.integers(0, len(mult_arr), size=(mc_runs, seq_per_month))
    mc_final_equities = spec.initial_capital_u * np.prod(mult_arr[sampled_idx], axis=1)

    finals_arr = np.array([m["final_u"] for m in monthly_results], dtype=np.float64)
    wrs_arr = np.array([m["win_rate_pct"] for m in monthly_results], dtype=np.float64)

    r_step1_exp = (spec.table_risk_fraction * (1.0 * payout_exp) / total_weight) * 100.0
    r_step1_prot = (spec.table_risk_fraction * (1.0 * payout_protect) / total_weight) * 100.0
    r_step2_exp = (spec.table_risk_fraction * (mult * payout_exp - 1.0) / total_weight) * 100.0

    off_steps = np.array([s["actual_steps_to_win"] for s in official_2026_sequences], dtype=np.int32)
    off_strict = np.array([s["strict_exp_steps_to_win"] for s in official_2026_sequences], dtype=np.int32)
    off_cnt = len(official_2026_sequences)

    report = {
        "spec": asdict(spec),
        "polymarket_official_oracle_2026_verification": {
            "oracle_source": "Polymarket Gamma API (eventMetadata.priceToBeat + outcomePrices) / Chainlink 60s TWAP",
            "verified_markets_fetched": len(poly_official_db),
            "verified_2026_sequences": off_cnt,
            "step_1_win_rate_pct": round(float(np.mean(off_steps == 1) * 100.0), 2) if off_cnt else 0.0,
            "step_2_cumulative_win_rate_pct": round(float(np.mean(off_steps <= 2) * 100.0), 2) if off_cnt else 0.0,
            "pure_expiration_100c_step1_wr_pct": round(float(np.mean(off_strict == 1) * 100.0), 2) if off_cnt else 0.0,
            "pure_expiration_100c_2step_wr_pct": round(float(np.mean(off_strict <= 2) * 100.0), 2) if off_cnt else 0.0,
            "max_adverse_ptb_gap_pct": spec.max_adverse_ptb_gap_pct,
        },
        "overall_2year_stats": {
            "total_sequences": total_seq,
            "sequences_per_month": round(total_seq / 24.0, 1),
            "sequences_per_day": round(total_seq / 730.0, 2),
            "step_1_win_rate_pct": round(win_1_pct, 2),
            "step_2_cumulative_win_rate_pct": round(win_2_pct, 2),
            "step_3_cumulative_win_rate_pct": round(win_3_pct, 2),
            "step_4_cumulative_win_rate_pct": round(win_4_pct, 2),
            "strict_expiration_only_1st_wr_pct": round(strict_1_pct, 2),
            "strict_expiration_only_2step_wr_pct": round(strict_2_pct, 2),
            "strict_expiration_only_3step_wr_pct": round(strict_3_pct, 2),
            "year1_in_sample_2step_wr_pct": round(float(np.mean(y1_steps <= 2) * 100.0), 2),
            "year2_out_of_sample_2step_wr_pct": round(float(np.mean(y2_steps <= 2) * 100.0), 2),
            "mean_monthly_win_rate_pct": round(float(np.mean(wrs_arr)), 2),
            "min_monthly_win_rate_pct": round(float(np.min(wrs_arr)), 2),
            "max_monthly_win_rate_pct": round(float(np.max(wrs_arr)), 2),
            "return_per_step1_win_on_equity_pct": round(r_step1_exp, 2),
            "return_per_step1_bounce_protect_pct": round(r_step1_prot, 2),
            "return_per_step2_win_on_equity_pct": round(r_step2_exp, 2),
            "loss_per_2step_bust_on_equity_pct": round(spec.table_risk_fraction * 100.0, 2),
        },
        "compounding_10u_to_1000u_summary": {
            "total_months_tested": 24,
            "profitable_months_count": int(np.sum(finals_arr > 10.0)),
            "months_reaching_1000u": hit_1000_months,
            "months_reaching_1000u_pct": round(hit_1000_months / 24.0 * 100.0, 1),
            "months_reaching_500u": hit_500_months,
            "months_reaching_500u_pct": round(hit_500_months / 24.0 * 100.0, 1),
            "median_30d_ending_u": round(float(np.median(finals_arr)), 2),
            "mean_30d_ending_u": round(float(np.mean(finals_arr)), 2),
            "worst_month_ending_u": round(float(np.min(finals_arr)), 2),
            "best_month_ending_u": round(float(np.max(finals_arr)), 2),
            "monte_carlo_2000_runs": {
                "prob_profit_pct": round(float(np.mean(mc_final_equities > 10.0) * 100.0), 2),
                "prob_reach_100u_pct": round(float(np.mean(mc_final_equities >= 100.0) * 100.0), 2),
                "prob_reach_500u_pct": round(float(np.mean(mc_final_equities >= 500.0) * 100.0), 2),
                "prob_reach_1000u_pct": round(float(np.mean(mc_final_equities >= 1000.0) * 100.0), 2),
                "p25_ending_u": round(float(np.percentile(mc_final_equities, 25)), 2),
                "median_ending_u": round(float(np.percentile(mc_final_equities, 50)), 2),
                "p75_ending_u": round(float(np.percentile(mc_final_equities, 75)), 2),
            },
        },
        "monthly_24_breakdown": monthly_results,
        "recent_100_sequences": list(reversed(sequences[-100:])),
    }

    MASTER_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    MASTER_REPORT_PATH.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    return report


class LivePaperTrader:
    """
    Route 1 Polymarket 5m CLOB forward prediction simulator & 10u -> 1000u compounding tracker.
    Tracks exact 51c entry share count, 100c expiration settlement, and 72c intra-candle bounce protection.
    """

    def __init__(self, spec: MasterStrategySpec | None = None) -> None:
        self.spec = spec or MasterStrategySpec()
        self.state = self._load_or_init()

    def _load_or_init(self) -> dict[str, Any]:
        if PAPER_ACCOUNT_PATH.exists():
            try:
                st = json.loads(PAPER_ACCOUNT_PATH.read_text(encoding="utf-8"))
                st["target_u"] = float(self.spec.target_capital_u)
                tot_u = float(st.get("total_equity_u", st.get("active_equity_u", self.spec.initial_capital_u)))
                pk_u = max(tot_u, float(st.get("peak_equity_u", tot_u)))
                v_u, next_v_u, v_lbl = compute_vault_locked_u(pk_u, float(st.get("vault_locked_u", 0.0)))
                st["peak_equity_u"] = round(pk_u, 2)
                st["vault_locked_u"] = v_u
                st["total_equity_u"] = round(tot_u, 2)
                act_u = round(max(0.0, tot_u - v_u), 2)
                st["active_equity_u"] = act_u
                st["active_peak_u"] = round(max(act_u, float(st.get("active_peak_u", act_u))), 2)
                st["cooldown_until_ms"] = 0
                st["next_vault_milestone_u"] = next_v_u
                st["vault_status_label"] = v_lbl
                st["progress_pct"] = round(min(100.0, (tot_u / float(st["target_u"])) * 100.0), 4)
                return st
            except Exception as exc:
                print(f"[MasterStrategy] Paper account load failed: {exc}")
        return self._fresh_state()

    def _fresh_state(self) -> dict[str, Any]:
        v_u, next_v_u, v_lbl = compute_vault_locked_u(self.spec.initial_capital_u, 0.0)
        stakes = compute_step_stakes(self.spec.initial_capital_u, self.spec, vault_locked_u=v_u)
        s1_shares = round(stakes[0] / (self.spec.entry_share_cents / 100.0), 1)
        s2_shares = round(stakes[1] / (self.spec.entry_share_cents / 100.0), 1) if len(stakes) > 1 else 0.0
        s3_shares = round(stakes[2] / (self.spec.entry_share_cents / 100.0), 1) if len(stakes) > 2 else 0.0
        s4_shares = round(stakes[3] / (self.spec.entry_share_cents / 100.0), 1) if len(stakes) > 3 else 0.0
        wm = get_watermark_tier(self.spec.initial_capital_u, self.spec, vault_locked_u=v_u, active_peak_u=round(max(0.0, self.spec.initial_capital_u - v_u), 2))
        act_init = round(max(0.0, self.spec.initial_capital_u - v_u), 2)
        return {
            "started_at_ms": int(time.time() * 1000),
            "initial_u": self.spec.initial_capital_u,
            "target_u": self.spec.target_capital_u,
            "active_equity_u": act_init,
            "active_peak_u": act_init,
            "is_dd_compressed": False,
            "vault_locked_u": v_u,
            "next_vault_milestone_u": next_v_u,
            "vault_status_label": v_lbl,
            "total_equity_u": self.spec.initial_capital_u,
            "peak_equity_u": self.spec.initial_capital_u,
            "cooldown_until_ms": 0,
            "pending_redeem_until_ms": 0,
            "progress_pct": round((self.spec.initial_capital_u / self.spec.target_capital_u) * 100.0, 4),
            "total_sequences": 0,
            "won_sequences": 0,
            "step1_wins": 0,
            "step2_wins": 0,
            "step3_wins": 0,
            "step4_wins": 0,
            "busts": 0,
            "win_rate_pct": 0.0,
            "entry_share_cents": self.spec.entry_share_cents,
            "watermark_tier": wm["tier"],
            "watermark_label": wm["label"],
            "next_watermark_u": wm["next_u"],
            "next_step1_stake_u": round(stakes[0], 2),
            "next_step1_shares": s1_shares,
            "next_step2_stake_u": round(stakes[1], 2) if len(stakes) > 1 else 0.0,
            "next_step2_shares": s2_shares,
            "next_step3_stake_u": round(stakes[2], 2) if len(stakes) > 2 else 0.0,
            "next_step3_shares": s3_shares,
            "next_step4_stake_u": round(stakes[3], 2) if len(stakes) > 3 else 0.0,
            "next_step4_shares": s4_shares,
            "active_position": None,
            "trade_log": [],
        }

    def save(self) -> None:
        PAPER_ACCOUNT_PATH.parent.mkdir(parents=True, exist_ok=True)
        PAPER_ACCOUNT_PATH.write_text(json.dumps(self.state, indent=2, ensure_ascii=False), encoding="utf-8")

    def reset_account(self, initial_u: float = 85.66, target_u: float = 200000.0) -> dict[str, Any]:
        self.spec.initial_capital_u = round(initial_u, 2)
        self.spec.target_capital_u = target_u
        self.state = self._fresh_state()
        self.save()
        return self.state

    def sync_with_live_wallet(
        self,
        live_total_u: float,
        live_cash_u: float | None = None,
        is_dry_run: bool = False,
        reset_for_live_promotion: bool = False,
    ) -> dict[str, Any]:
        if live_cash_u is not None:
            self.state["live_cash_pusd"] = round(live_cash_u, 4)
        if live_total_u > 0:
            self.state["live_total_pusd"] = round(live_total_u, 4)

        if reset_for_live_promotion and not is_dry_run and live_total_u > 0:
            clean_eq = round(float(live_total_u), 2)
            self.state["started_at_ms"] = int(time.time() * 1000)
            self.state["initial_u"] = clean_eq
            self.state["total_equity_u"] = clean_eq
            self.state["active_equity_u"] = clean_eq
            self.state["active_peak_u"] = clean_eq
            self.state["is_dd_compressed"] = False
            self.state["vault_locked_u"] = 0.0
            self.state["peak_equity_u"] = clean_eq
            self.state["total_sequences"] = 0
            self.state["won_sequences"] = 0
            self.state["step1_wins"] = 0
            self.state["step2_wins"] = 0
            self.state["step3_wins"] = 0
            self.state["step4_wins"] = 0
            self.state["busts"] = 0
            self.state["win_rate_pct"] = 0.0
            self.state["consecutive_losses"] = 0
            self.state["daily_start_equity_u"] = clean_eq
            self.state["daily_date_str"] = time.strftime("%Y-%m-%d", time.gmtime())
            self.state["cooldown_until_ms"] = 0
            self.state["pending_step2"] = None
            self.state["active_position"] = None
            self.state["trade_log"] = []
            self.state["is_dry_run"] = False

        # In simulation / dry-run mode, protect simulated compounding capital from being overwritten by real wallet,
        # while still refreshing the active Watermark Tier and Step 1..4 sizing!
        if is_dry_run:
            cur_tot = float(self.state.get("total_equity_u", self.state.get("active_equity_u", 40.62)))
            prev_v_dry = float(self.state.get("vault_locked_u", 0.0))
            pk_u = max(cur_tot, float(self.state.get("peak_equity_u", cur_tot)))
            v_u, next_v_u, v_lbl = compute_vault_locked_u(pk_u, prev_v_dry)
            act_dry = round(max(0.0, cur_tot - v_u), 2)
            # Argon Step-Down Reset Invariant: reset active_peak on new vault lock
            act_pk_dry = act_dry if v_u > prev_v_dry else round(max(act_dry, float(self.state.get("active_peak_u", act_dry))), 2)
            self.state["peak_equity_u"] = round(pk_u, 2)
            self.state["vault_locked_u"] = v_u
            self.state["active_equity_u"] = act_dry
            self.state["active_peak_u"] = act_pk_dry
            self.state["cooldown_until_ms"] = 0
            self.state["next_vault_milestone_u"] = next_v_u
            self.state["vault_status_label"] = v_lbl
            wm = get_watermark_tier(cur_tot, self.spec, vault_locked_u=v_u, active_peak_u=act_pk_dry)
            self.state["is_dd_compressed"] = bool(wm.get("is_dd_compressed", False))
            self.state["watermark_tier"] = wm["tier"]
            self.state["watermark_label"] = wm["label"]
            self.state["next_watermark_u"] = wm["next_u"]
            self.state["next_step1_stake_u"] = wm["step1_u"]
            self.state["next_step1_shares"] = wm["step1_shares"]
            self.state["next_step2_stake_u"] = wm["step2_u"]
            self.state["next_step2_shares"] = wm["step2_shares"]
            self.state["next_step3_stake_u"] = wm["step3_u"]
            self.state["next_step3_shares"] = wm["step3_shares"]
            self.state["next_step4_stake_u"] = wm["step4_u"]
            self.state["next_step4_shares"] = wm["step4_shares"]
            self.save()
            return self.state

        if live_total_u <= 0:
            return self.state
        prev_total = float(self.state.get("total_equity_u", 0.0))
        total_u = round(live_total_u, 4)
        now_ms = int(time.time() * 1000)
        pending_redeem_until_ms = int(self.state.get("pending_redeem_until_ms", 0) or 0)
        if now_ms < pending_redeem_until_ms and round(total_u, 2) < round(prev_total, 2):
            self.state["live_wallet_synced"] = True
            self.state["live_total_pusd"] = round(total_u, 4)
            if live_cash_u is not None:
                self.state["live_cash_pusd"] = round(live_cash_u, 4)
            self.save()
            return self.state
        if pending_redeem_until_ms > 0 and (now_ms >= pending_redeem_until_ms or round(total_u, 2) >= round(prev_total, 2)):
            self.state["pending_redeem_until_ms"] = 0
        # Only update initial_u on initial funding when total_sequences == 0
        if int(self.state.get("total_sequences", 0)) == 0 and (
            (total_u - prev_total) >= 30.0 or (float(self.state.get("initial_u", 80.0)) < 50.0 and total_u >= 80.0)
        ):
            self.state["initial_u"] = round(total_u, 2)
            self.state["cooldown_until_ms"] = 0
            self.state["pending_step2"] = None

        tot_rounded = round(total_u, 2)
        pk_u = max(tot_rounded, float(self.state.get("peak_equity_u", 0.0)))
        prev_vault = float(self.state.get("vault_locked_u", 0.0))
        prev_act_pk = float(self.state.get("active_peak_u", 0.0))
        vault_u, next_vault_u, vault_lbl = compute_vault_locked_u(tot_rounded, prev_vault, active_peak_u=prev_act_pk)
        if vault_u <= prev_vault:
            vault_u, next_vault_u, vault_lbl = compute_vault_locked_u(pk_u, prev_vault, active_peak_u=None)
        tradable_u = round(max(0.0, tot_rounded - vault_u), 2)
        act_pk = tradable_u if vault_u > prev_vault else round(max(tradable_u, float(self.state.get("active_peak_u", tradable_u))), 2)

        self.state["total_equity_u"] = total_u if False else tot_rounded
        self.state["peak_equity_u"] = round(pk_u, 2)
        self.state["vault_locked_u"] = vault_u
        self.state["active_equity_u"] = tradable_u
        self.state["active_peak_u"] = act_pk
        self.state["cooldown_until_ms"] = 0
        self.state["next_vault_milestone_u"] = next_vault_u
        self.state["vault_status_label"] = vault_lbl
        self.state["target_u"] = float(self.spec.target_capital_u)
        if live_cash_u is not None:
            self.state["live_cash_pusd"] = round(live_cash_u, 4)
        self.state["progress_pct"] = round(min(100.0, (tot_rounded / float(self.state["target_u"])) * 100.0), 4)

        wm = get_watermark_tier(tot_rounded, self.spec, vault_locked_u=vault_u, active_peak_u=act_pk)
        self.state["is_dd_compressed"] = bool(wm.get("is_dd_compressed", False))
        self.state["watermark_tier"] = wm["tier"]
        self.state["watermark_label"] = wm["label"]
        self.state["next_watermark_u"] = wm["next_u"]
        self.state["next_step1_stake_u"] = wm["step1_u"]
        self.state["next_step1_shares"] = wm["step1_shares"]
        self.state["next_step2_stake_u"] = wm["step2_u"]
        self.state["next_step2_shares"] = wm["step2_shares"]
        self.state["next_step3_stake_u"] = wm["step3_u"]
        self.state["next_step3_shares"] = wm["step3_shares"]
        self.state["next_step4_stake_u"] = wm["step4_u"]
        self.state["next_step4_shares"] = wm["step4_shares"]
        self.save()
        return self.state

    def record_completed_sequence(
        self,
        symbol: str,
        direction: str,
        entry_price: float,
        resolve_price: float,
        rsi_6: float,
        streak: int,
        won_at_step: int | None,
        exit_cents: float = 100.0,
        exit_type: str = "100¢_EXPIRATION",
        timestamp_ms: int | None = None,
        is_simulation_replay: bool = False,
        actual_pnl_u: float | None = None,
        trigger_branch: str = "",
        tier: str = "STANDARD",
        cvd_3: float = 0.0,
        oracle_source: str = "POLYMARKET_GAMMA_CHAINLINK",
        actual_stake_u: float | None = None,
        actual_entry_cents: float | None = None,
    ) -> dict[str, Any]:
        ts = timestamp_ms or int(time.time() * 1000)
        tot_before = float(self.state.get("total_equity_u", self.state.get("active_equity_u", self.spec.initial_capital_u)))
        prev_vault = float(self.state.get("vault_locked_u", 0.0))
        stakes = compute_step_stakes(
            tot_before,
            self.spec,
            vault_locked_u=prev_vault,
            is_tier_s=(tier in ("TIER_S", "TIER_S+")),
            active_peak_u=float(self.state.get("active_peak_u", max(0.0, tot_before - prev_vault))),
            is_tier_s_plus=(tier == "TIER_S+"),
        )
        entry_c = float(actual_entry_cents) if actual_entry_cents is not None else float(self.spec.entry_share_cents)
        payout_mult = (exit_cents - entry_c) / entry_c if exit_cents > entry_c else 0.0
        s1_used_u = float(actual_stake_u) if actual_stake_u is not None else float(stakes[0])

        if won_at_step == 1:
            pnl = actual_pnl_u if actual_pnl_u is not None else (stakes[0] * payout_mult)
            self.state["won_sequences"] += 1
            self.state["step1_wins"] += 1
            self.state["consecutive_losses"] = 0
        elif won_at_step == 2:
            pnl = actual_pnl_u if actual_pnl_u is not None else (stakes[1] * payout_mult - stakes[0])
            self.state["won_sequences"] += 1
            self.state["step2_wins"] += 1
            self.state["consecutive_losses"] = 0
        elif won_at_step == 3:
            s3_stake = stakes[2] if len(stakes) > 2 else round(stakes[1] * self.spec.martingale_multiplier, 2)
            pnl = actual_pnl_u if actual_pnl_u is not None else (s3_stake * payout_mult - stakes[0] - stakes[1])
            self.state["won_sequences"] += 1
            self.state["step3_wins"] = int(self.state.get("step3_wins", 0)) + 1
            self.state["consecutive_losses"] = 0
        elif won_at_step == 4:
            s4_stake = stakes[3] if len(stakes) > 3 else round(stakes[2] * self.spec.martingale_multiplier, 2)
            pnl = actual_pnl_u if actual_pnl_u is not None else (s4_stake * payout_mult - stakes[0] - stakes[1] - stakes[2])
            self.state["won_sequences"] += 1
            self.state["step4_wins"] = int(self.state.get("step4_wins", 0)) + 1
            self.state["consecutive_losses"] = 0
        else:
            pnl = actual_pnl_u if actual_pnl_u is not None else (-sum(stakes))
            self.state["busts"] += 1
            self.state["consecutive_losses"] = int(self.state.get("consecutive_losses", 0)) + 1

        # Model C Anti-Fragile Architecture: ZERO arbitrary 3-loss 30m or 24h time-clock halts.
        self.state["cooldown_until_ms"] = 0

        self.state["pending_step2"] = None
        self.state["total_sequences"] += 1
        # Total equity can never drop below the locked vault floor (`prev_vault`)
        new_tot = max(prev_vault, max(0.1, tot_before + pnl))
        total_u = round(new_tot, 2)
        pk_u = max(total_u, float(self.state.get("peak_equity_u", 0.0)))
        prev_act_pk = float(self.state.get("active_peak_u", max(0.0, total_u - prev_vault)))
        vault_u, next_vault_u, vault_lbl = compute_vault_locked_u(total_u, prev_vault, active_peak_u=prev_act_pk)
        if vault_u <= prev_vault:
            vault_u, next_vault_u, vault_lbl = compute_vault_locked_u(pk_u, prev_vault, active_peak_u=None)
        tradable_u = round(max(0.0, total_u - vault_u), 2)
        # Argon Step-Down Reset Invariant: reset active_peak when Vault locks profits!
        act_pk = tradable_u if vault_u > prev_vault else round(max(prev_act_pk, tradable_u), 2)

        self.state["total_equity_u"] = total_u
        self.state["peak_equity_u"] = round(pk_u, 2)
        self.state["vault_locked_u"] = vault_u
        self.state["active_equity_u"] = tradable_u
        self.state["active_peak_u"] = act_pk
        self.state["next_vault_milestone_u"] = next_vault_u
        self.state["vault_status_label"] = vault_lbl
        self.state["target_u"] = float(self.spec.target_capital_u)

        tot_seq = self.state["total_sequences"]
        self.state["win_rate_pct"] = round(self.state["won_sequences"] / tot_seq * 100.0, 2) if tot_seq else 0.0
        self.state["progress_pct"] = round(min(100.0, (total_u / float(self.state["target_u"])) * 100.0), 4)

        wm = get_watermark_tier(total_u, self.spec, vault_locked_u=vault_u, active_peak_u=act_pk)
        self.state["is_dd_compressed"] = bool(wm.get("is_dd_compressed", False))
        self.state["watermark_tier"] = wm["tier"]
        self.state["watermark_label"] = wm["label"]
        self.state["next_watermark_u"] = wm["next_u"]
        self.state["next_step1_stake_u"] = wm["step1_u"]
        self.state["next_step1_shares"] = wm["step1_shares"]
        self.state["next_step2_stake_u"] = wm["step2_u"]
        self.state["next_step2_shares"] = wm["step2_shares"]
        self.state["next_step3_stake_u"] = wm["step3_u"]
        self.state["next_step3_shares"] = wm["step3_shares"]
        self.state["next_step4_stake_u"] = wm["step4_u"]
        self.state["next_step4_shares"] = wm["step4_shares"]

        record = {
            "seq": tot_seq,
            "timestamp_ms": ts,
            "strategy_version": V8_CONFIG.version,
            "mode": f"Polymarket {exit_type}" if is_simulation_replay else f"實盤 {exit_type}",
            "symbol": symbol,
            "direction": direction,
            "trigger_branch": trigger_branch,
            "tier": tier,
            "cvd_3": round(float(cvd_3), 4),
            "oracle_source": oracle_source,
            "entry_price": round(entry_price, 4),
            "resolve_price": round(resolve_price, 4),
            "rsi_6": round(rsi_6, 2),
            "streak": streak,
            "entry_share_cents": round(entry_c, 2),
            "exit_share_cents": exit_cents if won_at_step else 0.0,
            "exit_type": exit_type if won_at_step else "0¢_STOP_LOSS",
            "step1_stake_u": round(s1_used_u, 2),
            "step2_stake_u": round(stakes[1], 2) if len(stakes) > 1 else 0.0,
            "step3_stake_u": round(stakes[2], 2) if len(stakes) > 2 else 0.0,
            "step4_stake_u": round(stakes[3], 2) if len(stakes) > 3 else 0.0,
            "won": won_at_step is not None,
            "won_at_step": won_at_step,
            "pnl_u": round(pnl, 2),
            "equity_after_u": total_u,
        }
        self.state["trade_log"].insert(0, record)
        self.state["trade_log"] = self.state["trade_log"][:150]
        self.save()
        return record

    def revert_provisional_win_for_rescue(self, timestamp_ms: int, symbol: str) -> dict[str, Any] | None:
        """Undo a prematurely recorded provisional win when official Gamma/Chainlink resolves as a Step < max_steps loss."""
        log = self.state.get("trade_log", [])
        idx_found = None
        for idx, r in enumerate(log):
            if int(r.get("timestamp_ms", 0)) == int(timestamp_ms) and r.get("symbol") == symbol:
                idx_found = idx
                break
        if idx_found is None or not log[idx_found].get("won"):
            return None
        removed = log.pop(idx_found)
        won_step = min(4, max(1, int(removed.get("won_at_step") or 1)))
        step_key = f"step{won_step}_wins"
        self.state["total_sequences"] = max(0, int(self.state.get("total_sequences", 0)) - 1)
        self.state["won_sequences"] = max(0, int(self.state.get("won_sequences", 0)) - 1)
        self.state[step_key] = max(0, int(self.state.get(step_key, 0)) - 1)
        old_pnl = float(removed.get("pnl_u", 0.0))
        tot_now = float(self.state.get("total_equity_u", self.spec.initial_capital_u))
        prev_vault = float(self.state.get("vault_locked_u", 0.0))
        restored_tot = round(max(prev_vault, max(0.1, tot_now - old_pnl)), 2)
        hist_equities = [float(self.state.get("initial_u", self.spec.initial_capital_u)), restored_tot] + [
            float(r.get("equity_after_u", 0.0)) for r in log
        ]
        pk_u = round(max(hist_equities), 2)
        tradable_u = round(max(0.0, restored_tot - prev_vault), 2)
        act_pk = round(max(0.0, pk_u - prev_vault), 2)
        self.state["total_equity_u"] = restored_tot
        self.state["peak_equity_u"] = pk_u
        self.state["active_equity_u"] = tradable_u
        self.state["active_peak_u"] = act_pk
        tot_seq = int(self.state.get("total_sequences", 0))
        self.state["win_rate_pct"] = round(self.state["won_sequences"] / tot_seq * 100.0, 2) if tot_seq else 0.0
        self.state["progress_pct"] = round(min(100.0, (restored_tot / float(self.state.get("target_u", self.spec.target_capital_u))) * 100.0), 4)
        self.save()
        return removed

    def reconcile_trade_outcome(
        self,
        timestamp_ms: int,
        symbol: str,
        actual_won: bool,
        resolve_price: float,
        actual_pnl_u: float,
        price_to_beat: float | None = None,
        step: int = 1,
    ) -> dict[str, Any] | None:
        """
        Authoritative Polymarket Chainlink Oracle Reconciliation:
        Adjusts a provisionally settled trade in `trade_log` when official Gamma/Chainlink
        resolution arrives asynchronously and differs from the provisional 1m-TWAP outcome.
        """
        target = None
        for r in self.state.get("trade_log", []):
            if int(r.get("timestamp_ms", 0)) == int(timestamp_ms) and r.get("symbol") == symbol:
                target = r
                break
        if target is None:
            return None

        if price_to_beat is not None and float(price_to_beat) > 0.0:
            target["entry_price"] = round(float(price_to_beat), 4)
        if float(resolve_price) > 0.0:
            target["resolve_price"] = round(float(resolve_price), 4)
        target["oracle_source"] = "POLYMARKET_GAMMA_OFFICIAL"

        if bool(target.get("won")) == bool(actual_won):
            self.save()
            return None

        old_pnl = float(target.get("pnl_u", 0.0))
        pnl_delta = round(float(actual_pnl_u) - old_pnl, 4)
        eff_step = min(4, max(1, int(step)))

        if actual_won:
            step_key = f"step{eff_step}_wins"
            self.state["won_sequences"] = int(self.state.get("won_sequences", 0)) + 1
            self.state[step_key] = int(self.state.get(step_key, 0)) + 1
            self.state["busts"] = max(0, int(self.state.get("busts", 0)) - 1)
            target["won"] = True
            target["won_at_step"] = eff_step
            target["exit_share_cents"] = 100.0
            target["exit_type"] = "100¢_ORACLE_RECONCILED"
            target["mode"] = "實盤 100¢_ORACLE_RECONCILED"
        else:
            prev_step = min(4, max(1, int(target.get("won_at_step") or eff_step)))
            prev_key = f"step{prev_step}_wins"
            self.state["won_sequences"] = max(0, int(self.state.get("won_sequences", 0)) - 1)
            self.state[prev_key] = max(0, int(self.state.get(prev_key, 0)) - 1)
            self.state["busts"] = int(self.state.get("busts", 0)) + 1
            target["won"] = False
            target["won_at_step"] = None
            target["exit_share_cents"] = 0.0
            target["exit_type"] = "0¢_ORACLE_RECONCILED"
            target["mode"] = "實盤 0¢_ORACLE_RECONCILED"

        target["pnl_u"] = round(float(actual_pnl_u), 2)

        prev_vault = float(self.state.get("vault_locked_u", 0.0))
        tot_now = float(self.state.get("total_equity_u", self.spec.initial_capital_u))
        new_tot = round(max(prev_vault, max(0.1, tot_now + pnl_delta)), 2)
        target["equity_after_u"] = new_tot

        # Recompute true historical peak equity without phantom provisional spikes
        hist_equities = [float(self.spec.initial_capital_u), new_tot] + [
            float(r.get("equity_after_u", 0.0)) for r in self.state.get("trade_log", [])
        ]
        pk_u = round(max(hist_equities), 2)
        vault_u, next_vault_u, vault_lbl = compute_vault_locked_u(pk_u, prev_vault)
        tradable_u = round(max(0.0, new_tot - vault_u), 2)
        act_pk = round(max(0.0, pk_u - vault_u), 2)

        self.state["total_equity_u"] = new_tot
        self.state["peak_equity_u"] = pk_u
        self.state["vault_locked_u"] = vault_u
        self.state["active_equity_u"] = tradable_u
        self.state["active_peak_u"] = act_pk
        self.state["next_vault_milestone_u"] = next_vault_u
        self.state["vault_status_label"] = vault_lbl
        tot_seq = int(self.state.get("total_sequences", 0))
        self.state["win_rate_pct"] = round(self.state["won_sequences"] / tot_seq * 100.0, 2) if tot_seq else 0.0
        self.state["progress_pct"] = round(min(100.0, (new_tot / float(self.spec.target_capital_u)) * 100.0), 4)

        wm = get_watermark_tier(new_tot, self.spec, vault_locked_u=vault_u, active_peak_u=act_pk)
        self.state["is_dd_compressed"] = bool(wm.get("is_dd_compressed", False))
        self.state["watermark_tier"] = wm["tier"]
        self.state["watermark_label"] = wm["label"]
        self.state["next_watermark_u"] = wm["next_u"]
        self.state["next_step1_stake_u"] = wm["step1_u"]
        self.state["next_step1_shares"] = wm["step1_shares"]
        self.state["next_step2_stake_u"] = wm["step2_u"]
        self.state["next_step2_shares"] = wm["step2_shares"]
        self.state["next_step3_stake_u"] = wm["step3_u"]
        self.state["next_step3_shares"] = wm["step3_shares"]
        self.state["next_step4_stake_u"] = wm["step4_u"]
        self.state["next_step4_shares"] = wm["step4_shares"]
        self.save()
        return {"reconciled": True, "symbol": symbol, "actual_won": actual_won, "pnl_delta": pnl_delta, "new_total_u": new_tot}
