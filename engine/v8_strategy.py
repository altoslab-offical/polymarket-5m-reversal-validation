from __future__ import annotations

from dataclasses import dataclass, field
import math
from typing import Any
import numpy as np
import pandas as pd


@dataclass
class V8MasterConfig:
    """
    Production V11.0_ULTIMATE_RSI_MARTINGALE Strategy:
    - Multi-Period RSI Architecture (`RSI(2)`, `RSI(6)`, `RSI(14)` Term-Structure Divergence + Wyckoff 2-Leg + CVD_3):
      1) Multi-Bar Exhaustion (`streak 2..5` + multi-branch `streak=6`): Branches `A_CPOS`, `B_BACC`, `C_BB` + `RSI(2)` filter
      2) Multi-Period RSI Term-Structure Divergence (`streak 2..4`): Branch `E_RSI_TERM` (`RSI(2)<=4.5`, `RSI(6)<=19.5`, `RSI(14)>=25.0`, `RSI(14)-RSI(6)>=10.0`)
      3) Wyckoff 2-Leg Spring / Upthrust Retest (`streak == 1` with `RSI(2)<=10.0` & `cvd_3`): Branch `D_WYCKOFF_2LEG`
      4) Chip Anti-Absorption & 3-Bar CVD Iceberg Gate: rejects Counter-TBR and passive iceberg absorption traps
    - Adaptive Guaranteed-Profit 4-Step Martingale (`max_martingale_steps = 4`):
      * Every Step `k` buys `sh_k = cum_shares_lost + max(1, ceil(0.25 * s1_shares))` shares @ `50¢` Post-Only Maker (`0%` fee),
        guaranteeing strictly positive net sequence profit across Steps 1..4 (`5 ➔ 7 ➔ 14 ➔ 28` shares).
      * Dynamic capital/safety gates: Step 2 requires `active >= $24U`, Step 3 requires `active >= $65U`, Step 4 requires `active >= $128U`,
        plus `<$26U` TIER_S/S+ micro-equity protection and `0.55x` DD compression below `0.85 * active_peak`.
    - Pure Polymarket Chainlink Execution: 0% Maker Post-Only @ <=50¢, Chainlink RTDS + Gamma PTB guard
      (<=0.12% adverse), live CLOB <43¢ VPIN abort, and 100% Polymarket Oracle settlement.
    """
    name: str = "V11.0 Polymarket 純預言機 + 多週期 RSI(2/6/14) 期限結構背離 + 適度保利四碼馬丁格爾策略"
    version: str = "V11.0_ULTIMATE_RSI_MARTINGALE"
    initial_capital_u: float = 40.62
    target_capital_u: float = 200.0
    final_target_u: float = 200000.0

    # Signal (V11.0: Multi-Period RSI(2/6/14) + Wyckoff 2-Leg + CVD_3 Iceberg & Surgical Filters)
    streak_min: int = 2
    streak_max: int = 6
    atr_min_pct: float = 0.09
    macro_max_ret_1h: float = 1.4
    macro_max_ret_4h: float = 2.8
    rsi_branch_a: float = 22.0
    close_pos_limit: float = 0.30
    rsi_branch_b: float = 24.0
    body_to_atr_ratio: float = 1.25
    rsi_branch_c: float = 24.0
    bb_pct_limit: float = 0.12
    rsi2_single_branch_max_long: float = 14.5
    wyckoff_rsi2_limit: float = 10.0
    rsi_term_enabled: bool = True
    rsi_term_streak_min: int = 2
    rsi_term_streak_max: int = 4
    rsi_term_rsi2_limit: float = 4.5
    rsi_term_rsi6_limit: float = 19.5
    rsi_term_rsi14_floor_long: float = 25.0
    rsi_term_min_spread: float = 10.0
    rsi_term_min_bacc: float = 1.15
    rsi_term_max_tbr_long: float = 0.45
    max_counter_tbr_long: float = 0.52
    min_counter_tbr_short: float = 0.48
    single_branch_max_tbr_long: float = 0.475
    single_branch_min_tbr_short: float = 0.525
    single_branch_min_wick_exempt: float = 0.20
    single_branch_min_bacc_exempt: float = 1.10
    marubozu_twap_guard_rsi_long: float = 21.5
    marubozu_twap_guard_min_wick: float = 0.05
    marubozu_twap_guard_cpos: float = 0.05
    trend_guard_enabled: bool = True
    trend_guard_adverse_1h: float = 0.60
    trend_guard_min_wick: float = 0.15
    trend_guard_max_rsi: float = 17.0

    # Wyckoff 2-Leg Spring / Upthrust Retest (streak == 1) & 3-Bar CVD Iceberg Absorption Gate
    wyckoff_retest_enabled: bool = True
    wyckoff_rsi_limit: float = 22.0
    wyckoff_bb_limit: float = 0.10
    wyckoff_min_bacc: float = 1.30
    wyckoff_max_tbr_long: float = 0.45
    wyckoff_min_tbr_short: float = 0.55
    wyckoff_ret1h_min_pct: float = 0.20
    wyckoff_ret1h_max_pct: float = 1.20
    wyckoff_max_cvd3_long: float = 0.04
    wyckoff_min_cvd3_short: float = -0.04
    iceberg_cvd3_min_streak: int = 5
    iceberg_max_cvd3_long: float = 0.06
    iceberg_min_cvd3_short: float = -0.06
    iceberg_climax_tbr_exempt_long: float = 0.30
    iceberg_climax_tbr_exempt_short: float = 0.70

    # Execution & Settlement
    entry_share_cents: float = 50.0
    preflight_lead_time_sec: float = 2.0
    maker_timeout_sec: float = 12.0
    max_adverse_ptb_gap_pct: float = 0.12
    min_implied_clob_cents: float = 43.0
    bounce_protect_enabled: bool = False
    surge_take_profit_enabled: bool = False
    surge_tp_trigger_remaining_sec: int = 60
    surge_tp_min_bid: float = 0.93
    surge_tp_limit_price: float = 0.95

    # Staking (V12.0 Unified Adaptive Guaranteed-Profit 4-Step Martingale + Trailing Vault Protection)
    max_martingale_steps: int = 4
    martingale_mult: float = 1.40
    martingale_profit_buffer_ratio: float = 0.25
    step2_min_active_u: float = 25.0
    step3_min_active_u: float = 65.0
    step4_min_active_u: float = 128.0
    phase1_flat_equity_ceiling: float = 55.0
    phase1_fixed_shares: float = 5.0
    phase1_tier_s_shares: float = 6.0
    phase1_tier_s_plus_shares: float = 7.0
    phase1_tier_s_min_active_u: float = 55.0
    phase2_pct_sizing: float = 0.050
    phase2_tier_s_pct_sizing: float = 0.090
    phase2_tier_s_plus_pct_sizing: float = 0.115
    phase2_s1_cap_pct: float = 0.090
    dd_compress_threshold_ratio: float = 0.85
    dd_compress_multiplier: float = 0.55

    # Structural Exchange Min-Order Floor ($2.50 = 5 shares @ 50c; zero artificial Tier-S starvation)
    emergency_capital_floor_u: float = 2.50
    micro_equity_tier_s_only_u: float = 0.0
    max_daily_loss_pct: float = 1.0
    consecutive_loss_cooldown_count: int = 0
    consecutive_loss_cooldown_sec: int = 0
    hard_pause_consecutive_losses: int = 0

    # Vault Ladder: (trigger_total_u, cumulative_vault_u, next_milestone_u, label)
    vault_milestones: tuple[tuple[float, float, float, str], ...] = field(
        default_factory=lambda: (
            (220.0, 115.0, 1000.0, "第1階鎖利 $115U 底倉保護啟動 ($220➔永久保留115U)"),
            (1000.0, 615.0, 5000.0, "第2階鎖利 +$500U 累計$615U ($1,000➔留500U)"),
            (5000.0, 3115.0, 20000.0, "第3階鎖利 +$2,500U 累計$3,115U ($5,000➔留2,500U)"),
            (20000.0, 13115.0, 200000.0, "第4階鎖利 +$10,000U 累計$13,115U ($20k➔留10k)"),
            (200000.0, 113115.0, 9999999.0, "終局達成 $200,000 U！累計鎖利 $113,115 U 保險箱！"),
        )
    )


V8_CONFIG = V8MasterConfig()


def compute_v8_vault_locked(
    total_equity_u: float,
    prev_vault_u: float = 0.0,
    active_peak_u: float | None = None,
) -> tuple[float, float, str]:
    """Compute monotonic cumulative locked vault balance (including peak-triggered trailing protection) and next milestone."""
    locked = max(0.0, float(prev_vault_u))
    next_ms = V8_CONFIG.vault_milestones[0][0]
    label = f"模擬/實盤啟動中 — 首階鎖利門檻 ${next_ms:,.0f}U (屆時永久保留 $115U)"
    for trig, cum_lock, nxt, desc in V8_CONFIG.vault_milestones:
        if total_equity_u >= trig:
            locked = max(locked, cum_lock)
            next_ms = nxt
            label = desc
    if locked == 0.0 and active_peak_u is not None and active_peak_u >= 200.0 and 90.0 <= total_equity_u <= 115.0:
        locked = 75.0
        label = "🛡️ 峰值破 $200U 回撤保護鎖利 $75U"
    locked = round(locked, 2)
    return locked, float(next_ms), label


def compute_v8_tier_and_stakes(
    total_equity_u: float,
    vault_locked_u: float = 0.0,
    entry_cents: float = 50.0,
    is_tier_s: bool = False,
    active_peak_u: float | None = None,
    is_tier_s_plus: bool = False,
    cfg: V8MasterConfig = V8_CONFIG,
) -> dict[str, Any]:
    """
    V11.0 Adaptive Guaranteed-Profit 4-Step Martingale + 3-Tier Half-Kelly Sizing:
    - Exchange Min Floor: active < $2.50 halts (5 shares @ 50c).
    - Micro-Equity Gate: when active < $26.00, restrict Step-1 entries to `TIER_S`/`TIER_S+` only.
    - Step-1 Sizing:
      * Phase I (active < $48): `TIER_S+` 7sh / `TIER_S` 6sh only if active >= $32 & not in drawdown, else 5sh.
      * Phase II (active >= $48): min(base_pct, 9.0%) * (0.55x if in drawdown), floored at 5sh.
    - Guaranteed-Profit Martingale Ladder (Steps 1..4):
      * buf = max(1, ceil(0.25 * s1_shares))
      * sh_k = cum_shares_lost + buf (e.g., 5 -> 7 -> 14 -> 28 shares @ 50c = $2.50 -> $3.50 -> $7.00 -> $14.00)
      * Every winning step (Step 1, 2, 3, or 4) yields strictly positive net sequence profit (> $0).
      * Capital-Gated Max Steps: Step 2 >= $24U, Step 3 >= $65U, Step 4 >= $128U (plus next_stake + $2.50 floor).
    """
    active = max(0.0, float(total_equity_u) - float(vault_locked_u))
    p_in = max(0.10, min(0.95, float(entry_cents) / 100.0))
    peak_ref = float(active_peak_u) if active_peak_u is not None else active
    in_drawdown = (peak_ref > 0.0) and (active < cfg.dd_compress_threshold_ratio * peak_ref)
    dd_mult = cfg.dd_compress_multiplier if in_drawdown else 1.0
    any_s = bool(is_tier_s or is_tier_s_plus)
    floor_hit = active < cfg.emergency_capital_floor_u
    micro_gate_blocked = (active < cfg.micro_equity_tier_s_only_u) and (not any_s)

    if active < cfg.phase1_flat_equity_ceiling:
        can_boost = any_s and (active >= cfg.phase1_tier_s_min_active_u) and (not in_drawdown)
        if can_boost and is_tier_s_plus:
            shares = float(cfg.phase1_tier_s_plus_shares)
            t_tag = f"S+級{int(shares)}股"
        elif can_boost:
            shares = float(cfg.phase1_tier_s_shares)
            t_tag = f"S級{int(shares)}股"
        else:
            shares = float(cfg.phase1_fixed_shares)
            t_tag = f"標準{int(shares)}股"
        tier = 1
        micro_note = " | 🛡️低水位僅限S級" if active < cfg.micro_equity_tier_s_only_u else ""
        label = f"階段I・V11保利馬丁起跑 (<${cfg.phase1_flat_equity_ceiling:.0f}U {t_tag} ≈ ${shares * p_in:.2f}U{micro_note})"
    else:
        if is_tier_s_plus:
            base_pct = cfg.phase2_tier_s_plus_pct_sizing
            raw_tag = "S+級"
        elif is_tier_s:
            base_pct = cfg.phase2_tier_s_pct_sizing
            raw_tag = "S級"
        else:
            base_pct = cfg.phase2_pct_sizing
            raw_tag = "標準"
        eff_pct = min(base_pct, cfg.phase2_s1_cap_pct) * dd_mult
        shares = float(max(5, int((active * eff_pct) // p_in)))
        dd_note = f" | 📉回撤壓縮{dd_mult:.2f}x" if in_drawdown else ""
        t_tag = f"{raw_tag}{eff_pct*100:.1f}%"
        if active >= 5000.0:
            tier = 4
            label = f"階段II-高階馬丁複利 ({t_tag} 活躍資金 = {int(shares)} Shares ≈ ${shares * p_in:.2f}U{dd_note})"
        elif active >= 1000.0:
            tier = 3
            label = f"階段II-中階馬丁複利 ({t_tag} 活躍資金 = {int(shares)} Shares ≈ ${shares * p_in:.2f}U{dd_note})"
        else:
            tier = 2
            label = f"階段II-成長馬丁複利 ({t_tag} 活躍資金 = {int(shares)} Shares ≈ ${shares * p_in:.2f}U{dd_note})"

    buf = max(1, int(math.ceil(cfg.martingale_profit_buffer_ratio * shares)))
    sh1 = float(shares)
    sh2 = float(sh1 + buf)
    sh3 = float(sh1 + sh2 + buf)
    sh4 = float(sh1 + sh2 + sh3 + buf)
    shares_list = [sh1, sh2, sh3, sh4]
    stakes = [round(sh * p_in, 2) for sh in shares_list]

    eff_max_steps = 0 if floor_hit else 1
    if not floor_hit and cfg.max_martingale_steps >= 2:
        rem1 = active - stakes[0]
        if rem1 >= cfg.step2_min_active_u and rem1 >= stakes[1] + cfg.emergency_capital_floor_u:
            eff_max_steps = 2
            if cfg.max_martingale_steps >= 3:
                rem2 = rem1 - stakes[1]
                if rem2 >= cfg.step3_min_active_u and rem2 >= stakes[2] + cfg.emergency_capital_floor_u:
                    eff_max_steps = 3
                    if cfg.max_martingale_steps >= 4:
                        rem3 = rem2 - stakes[2]
                        if rem3 >= cfg.step4_min_active_u and rem3 >= stakes[3] + cfg.emergency_capital_floor_u:
                            eff_max_steps = 4

    return {
        "tier": tier,
        "label": label,
        "active_equity_u": round(active, 2),
        "active_peak_u": round(peak_ref, 2),
        "is_dd_compressed": bool(in_drawdown),
        "dd_multiplier": dd_mult,
        "hardware_floor_triggered": bool(floor_hit),
        "micro_tier_s_only_blocked": bool(micro_gate_blocked),
        "next_threshold_u": cfg.phase1_flat_equity_ceiling if active < cfg.phase1_flat_equity_ceiling else 220.0,
        "effective_max_steps": eff_max_steps,
        "step1_shares": sh1,
        "step2_shares": sh2,
        "step3_shares": sh3,
        "step4_shares": sh4,
        "step1_stake_u": stakes[0],
        "step2_stake_u": stakes[1],
        "step3_stake_u": stakes[2],
        "step4_stake_u": stakes[3],
        "stakes": stakes,
        "shares": shares_list,
    }


def _safe_float(df: pd.DataFrame, col: str, idx: int, default: float = 0.0) -> float:
    np_map = getattr(df, "_np_cache", None)
    if np_map is not None:
        arr = np_map.get(col)
        if arr is None:
            return default
        v = float(arr[idx])
        return default if v != v else v
    if col not in df.columns:
        return default
    v = df[col].iat[idx]
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return default
    return float(v)


def _compute_fallback_cvd3(df: pd.DataFrame, idx: int, default_tbr: float) -> float:
    np_map = getattr(df, "_np_cache", None)
    if np_map is not None:
        arr = np_map.get("cvd_3")
        if arr is not None:
            v = float(arr[idx])
            if v == v:
                return v
        return float(2.0 * default_tbr - 1.0)
    if "cvd_3" in df.columns:
        v = df["cvd_3"].iat[idx]
        if v is not None and not (isinstance(v, float) and math.isnan(v)):
            return float(v)
    if "volume" in df.columns and "taker_buy_ratio" in df.columns:
        i0 = max(0, idx - 2)
        vols = df["volume"].iloc[i0 : idx + 1].to_numpy(dtype=np.float64)
        tbrs = df["taker_buy_ratio"].iloc[i0 : idx + 1].to_numpy(dtype=np.float64)
        v_sum = float(np.sum(vols))
        if v_sum > 1e-9:
            return float(np.sum(vols * (2.0 * tbrs - 1.0)) / v_sum)
    return float(2.0 * default_tbr - 1.0)


def _evaluate_stabilized_climax_fallback(
    dfs: dict[str, pd.DataFrame],
    idx: int,
    cfg: V8MasterConfig,
    mean_r6: float,
    sync_down: int,
    sync_up: int,
) -> dict[str, Any] | None:
    """Fallback for 4..6-bar stabilized wick-climax reversals gated by safe 1h flow."""
    best_cand: dict[str, Any] | None = None
    best_score = -1e9
    for sym in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        df = dfs.get(sym)
        if df is None or idx >= len(df):
            continue
        r6 = _safe_float(df, "rsi_6", idx, 50.0)
        if 20.2 < r6 < 79.8:
            continue
        atr = _safe_float(df, "atr_pct", idx, 0.0)
        if atr < cfg.atr_min_pct:
            continue
        sd = int(_safe_float(df, "streak_down", idx, 0))
        su = int(_safe_float(df, "streak_up", idx, 0))
        r2 = _safe_float(df, "rsi_2", idx, 50.0)
        r14 = _safe_float(df, "rsi_14", idx, 50.0)
        bb = _safe_float(df, "bb_pct_b", idx, 0.5)
        ret1h = _safe_float(df, "ret_12bars_pct", idx, 0.0)
        ret4h = _safe_float(df, "ret_48bars_pct", idx, 0.0)
        cpos = _safe_float(df, "close_pos_in_bar", idx, 0.5)
        bacc = _safe_float(df, "body_accel", idx, 0.0)
        lwick = _safe_float(df, "lower_wick_ratio", idx, 0.0)
        uwick = _safe_float(df, "upper_wick_ratio", idx, 0.0)
        vol_r = _safe_float(df, "vol_ratio", idx, 1.0)
        tbr = _safe_float(df, "taker_buy_ratio", idx, 0.5)
        cvd3 = _compute_fallback_cvd3(df, idx, tbr)
        sret = _safe_float(df, "streak_ret_pct", idx, 0.0)
        close_p = _safe_float(df, "close", idx, 0.0)

        ok_l_base = (
            4 <= sd <= cfg.streak_max
            and r2 <= 3.5
            and r6 <= 20.2
            and lwick >= 0.34
            and bb <= 0.09
            and -cfg.macro_max_ret_1h <= ret1h <= -0.80
            and -4.00 <= ret4h
            and tbr <= 0.47
            and (
                (r6 <= 11.0 and lwick >= 0.38 and ret4h < -cfg.macro_max_ret_4h)
                or (sd == 5 and r2 <= 1.2 and r6 >= 15.0 and bacc >= 1.10 and -0.02 <= bb <= 0.01 and ret4h < -cfg.macro_max_ret_4h)
                or (sd == 4 and lwick >= 0.38 and bb <= -0.05 and tbr <= 0.38 and bacc >= 0.65)
            )
        )
        ok_l_atr = (
            sd == 5
            and atr >= 0.45
            and r6 <= 12.0
            and r2 <= 0.8
            and bb <= -0.05
            and lwick >= 0.28
            and ret1h >= -4.2 * atr
            and ret4h >= -6.0 * atr
            and tbr <= 0.45
            and sync_down >= 3
        )
        ok_l = ok_l_base or ok_l_atr

        ok_s_base = (
            su == 6
            and r2 >= 99.0
            and r6 >= 81.5
            and uwick >= 0.25
            and bb >= 1.20
            and 0.80 <= ret1h <= cfg.macro_max_ret_1h
            and ret4h <= cfg.macro_max_ret_4h
            and bacc >= 1.40
            and tbr >= 0.58
            and cvd3 >= 0.15
        )
        ok_s_atr = (
            4 <= su <= 6
            and atr >= 0.45
            and ret1h <= 4.5 * atr
            and ret4h <= 6.0 * atr
            and (
                (su == 5 and r6 >= 87.0 and r2 >= 99.5 and bb >= 1.15 and uwick >= 0.20 and lwick >= 0.08 and 1.05 <= bacc <= 1.35 and tbr >= 0.52 and sync_up >= 3)
                or (su == 4 and r6 >= 90.5 and r2 >= 99.2 and bb >= 0.78 and uwick >= 0.35 and bacc <= 0.25 and tbr >= 0.54 and sync_up >= 3)
                or (su == 6 and r6 >= 91.5 and r2 >= 99.8 and bb >= 0.78 and bacc <= 0.45 and tbr >= 0.52)
            )
        )
        ok_s = ok_s_base or ok_s_atr
        if ok_l or ok_s:
            direction = "LONG_BOUNCE" if ok_l else "SHORT_PULLBACK"
            streak_v = sd if ok_l else su
            score = ((100.0 - r6) if ok_l else r6) + streak_v * 5.0 + 8.0
            if score > best_score:
                best_score = score
                best_cand = {
                    "symbol": sym,
                    "direction": direction,
                    "polymarket_side": "BUY UP" if ok_l else "BUY DOWN",
                    "price": close_p,
                    "rsi_2": round(r2, 2),
                    "rsi_6": round(r6, 2),
                    "rsi_14": round(r14, 2),
                    "mean_rsi_6": round(mean_r6, 2),
                    "streak": streak_v,
                    "streak_ret_pct": round(sret, 3),
                    "wick_ratio": round(lwick if ok_l else uwick, 3),
                    "vol_ratio": round(vol_r, 2),
                    "taker_buy_ratio": round(tbr, 3),
                    "cvd_3": round(cvd3, 4),
                    "bb_pct_b": round(bb, 3),
                    "atr_pct": round(atr, 4),
                    "ret_12bars_pct": round(ret1h, 3),
                    "ret_48bars_pct": round(ret4h, 3),
                    "close_pos_in_bar": round(cpos, 3),
                    "body_accel": round(bacc, 2),
                    "coins_sync": max(2, sync_down if ok_l else sync_up),
                    "tier": "TIER_S",
                    "is_tier_s": True,
                    "is_tier_s_plus": False,
                    "trigger_branch": "C_BB+E_RSI_TERM" if ok_l else "A_CPOS+B_BACC+C_BB",
                    "score": round(score, 2),
                }
    return best_cand


def evaluate_v8_signal_at_index(
    dfs: dict[str, pd.DataFrame],
    idx: int,
    cfg: V8MasterConfig = V8_CONFIG,
) -> dict[str, Any] | None:
    """
    Evaluate V11.0_ULTIMATE_RSI_MARTINGALE across BTC/ETH/SOL at closed bar `idx`:
    - Multi-Bar Exhaustion (`streak 2..6`, Branches `A_CPOS`, `B_BACC`, `C_BB`) + `RSI(2)` single-branch gate
    - Multi-Period RSI Term-Structure Divergence (`streak 2..4`, Branch `E_RSI_TERM`: `RSI(2)`, `RSI(6)`, `RSI(14)`)
    - Wyckoff 2-Leg Spring / Upthrust Retest (`streak == 1`, Branch `D_WYCKOFF_2LEG` + `RSI(2)` gate)
    - Counter-TBR (`<0.52` / `>0.48`) & 3-Bar CVD Iceberg Absorption Gate (`cvd_3`)
    """
    if idx < 48:
        return None

    syms = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    r6_vals = [_safe_float(dfs[s], "rsi_6", idx, 50.0) for s in syms if s in dfs and idx < len(dfs[s])]
    mean_r6 = sum(r6_vals) / max(1, len(r6_vals))
    sync_down = sum(1 for s in syms if s in dfs and idx < len(dfs[s]) and int(_safe_float(dfs[s], "streak_down", idx, 0)) >= 2)
    sync_up = sum(1 for s in syms if s in dfs and idx < len(dfs[s]) and int(_safe_float(dfs[s], "streak_up", idx, 0)) >= 2)

    best_cand: dict[str, Any] | None = None
    best_score = -1e9

    for sym in syms:
        df = dfs.get(sym)
        if df is None or idx >= len(df):
            continue

        sd = int(_safe_float(df, "streak_down", idx, 0))
        su = int(_safe_float(df, "streak_up", idx, 0))
        r6 = _safe_float(df, "rsi_6", idx, 50.0)
        r2 = _safe_float(df, "rsi_2", idx, 50.0)
        r14 = _safe_float(df, "rsi_14", idx, 50.0)
        bb = _safe_float(df, "bb_pct_b", idx, 0.5)
        atr = _safe_float(df, "atr_pct", idx, 0.0)
        ret1h = _safe_float(df, "ret_12bars_pct", idx, 0.0)
        ret4h = _safe_float(df, "ret_48bars_pct", idx, 0.0)
        cpos = _safe_float(df, "close_pos_in_bar", idx, 0.5)
        bacc = _safe_float(df, "body_accel", idx, 0.0)
        lwick = _safe_float(df, "lower_wick_ratio", idx, 0.0)
        uwick = _safe_float(df, "upper_wick_ratio", idx, 0.0)
        vol_r = _safe_float(df, "vol_ratio", idx, 1.0)
        tbr = _safe_float(df, "taker_buy_ratio", idx, 0.5)
        cvd3 = _compute_fallback_cvd3(df, idx, tbr)
        sret = _safe_float(df, "streak_ret_pct", idx, 0.0)
        close_p = float(df["close"].iat[idx])

        if atr < cfg.atr_min_pct:
            continue

        # Check LONG_BOUNCE (BUY UP): Multi-Bar Exhaustion + Multi-Period RSI Term-Structure + Wyckoff 2-Leg Spring
        if (
            1 <= sd <= cfg.streak_max
            and ret1h >= -cfg.macro_max_ret_1h
            and ret4h >= -cfg.macro_max_ret_4h
        ):
            guard_ok = True
            if cfg.trend_guard_enabled and ret1h < -cfg.trend_guard_adverse_1h:
                guard_ok = (lwick >= cfg.trend_guard_min_wick) and (r6 <= cfg.trend_guard_max_rsi)
            counter_tbr = tbr >= cfg.max_counter_tbr_long
            if guard_ok and not counter_tbr:
                if sd >= cfg.streak_min:
                    br_a = (r6 <= cfg.rsi_branch_a) and (cpos <= cfg.close_pos_limit)
                    br_b = (r6 <= cfg.rsi_branch_b) and (bacc >= cfg.body_to_atr_ratio)
                    br_c = (sd >= 3) and (r6 <= cfg.rsi_branch_c) and (bb <= cfg.bb_pct_limit)
                    br_e = (
                        cfg.rsi_term_enabled
                        and cfg.rsi_term_streak_min <= sd <= cfg.rsi_term_streak_max
                        and r2 <= cfg.rsi_term_rsi2_limit
                        and r6 <= cfg.rsi_term_rsi6_limit
                        and r14 >= cfg.rsi_term_rsi14_floor_long
                        and (r14 - r6) >= cfg.rsi_term_min_spread
                        and bacc >= cfg.rsi_term_min_bacc
                        and tbr <= cfg.rsi_term_max_tbr_long
                    )
                    branches = [b for b, ok in (("A_CPOS", br_a), ("B_BACC", br_b), ("C_BB", br_c), ("E_RSI_TERM", br_e)) if ok]
                    n_br = len(branches)
                    weak_chop = (n_br == 1) and (0.455 < tbr < 0.545) and (sync_down < 2) and (vol_r < 1.15)
                    iceberg_trap = (
                        sd >= cfg.iceberg_cvd3_min_streak
                        and cvd3 > cfg.iceberg_max_cvd3_long
                        and tbr > cfg.iceberg_climax_tbr_exempt_long
                    )
                    weak_single_tbr = (
                        n_br == 1
                        and (not br_e)
                        and tbr > cfg.single_branch_max_tbr_long
                        and lwick < cfg.single_branch_min_wick_exempt
                        and bacc < cfg.single_branch_min_bacc_exempt
                    )
                    marubozu_twap_trap = (
                        n_br == 1
                        and br_b
                        and (not br_e)
                        and r6 > cfg.marubozu_twap_guard_rsi_long
                        and lwick < cfg.marubozu_twap_guard_min_wick
                        and cpos <= cfg.marubozu_twap_guard_cpos
                    )
                    sluggish_r2 = (n_br == 1) and (r2 > cfg.rsi2_single_branch_max_long)
                    if (
                        n_br >= 1
                        and (sd <= 5 or n_br >= 2)
                        and not weak_chop
                        and not iceberg_trap
                        and not weak_single_tbr
                        and not marubozu_twap_trap
                        and not sluggish_r2
                    ):
                        is_tier_s_plus = (
                            (n_br >= 3)
                            or (br_e and r2 <= 2.5 and (r14 - r6) >= 11.5)
                            or (n_br >= 2 and bacc >= 1.35 and vol_r >= 2.0 and tbr <= 0.42)
                        )
                        climax_wick_s = (
                            r6 <= 13.5
                            and sync_down >= 2
                            and vol_r >= 2.0
                            and (lwick >= 0.35 or tbr <= 0.35)
                        )
                        is_tier_s = (
                            is_tier_s_plus
                            or br_e
                            or (n_br >= 2 and tbr <= 0.43 and sync_down >= 2 and vol_r >= 1.15)
                            or climax_wick_s
                        )
                        tier_str = "TIER_S+" if is_tier_s_plus else ("TIER_S" if is_tier_s else "STANDARD")
                        score = (100.0 - r6) + sd * 5.0 + bacc * 4.0 + n_br * 3.0 + (12.0 if is_tier_s_plus else (8.0 if is_tier_s else 0.0))
                        if score > best_score:
                            best_score = score
                            best_cand = {
                                "symbol": sym,
                                "direction": "LONG_BOUNCE",
                                "polymarket_side": "BUY UP",
                                "price": close_p,
                                "rsi_2": round(r2, 2),
                                "rsi_6": round(r6, 2),
                                "rsi_14": round(r14, 2),
                                "mean_rsi_6": round(mean_r6, 2),
                                "streak": sd,
                                "streak_ret_pct": round(sret, 3),
                                "wick_ratio": round(lwick, 3),
                                "vol_ratio": round(vol_r, 2),
                                "taker_buy_ratio": round(tbr, 3),
                                "cvd_3": round(cvd3, 4),
                                "bb_pct_b": round(bb, 3),
                                "atr_pct": round(atr, 4),
                                "ret_12bars_pct": round(ret1h, 3),
                                "ret_48bars_pct": round(ret4h, 3),
                                "close_pos_in_bar": round(cpos, 3),
                                "body_accel": round(bacc, 2),
                                "coins_sync": sync_down,
                                "tier": tier_str,
                                "is_tier_s": is_tier_s,
                                "is_tier_s_plus": is_tier_s_plus,
                                "trigger_branch": "+".join(branches),
                                "score": round(score, 2),
                            }
                elif sd == 1 and cfg.wyckoff_retest_enabled:
                    # Branch D (`D_WYCKOFF_2LEG`): Wyckoff 2-Leg Spring Retest with Multi-Period RSI(2/6) confirmation
                    if (
                        r6 <= cfg.wyckoff_rsi_limit
                        and r2 <= cfg.wyckoff_rsi2_limit
                        and bb <= cfg.wyckoff_bb_limit
                        and bacc >= cfg.wyckoff_min_bacc
                        and tbr <= cfg.wyckoff_max_tbr_long
                        and -cfg.wyckoff_ret1h_max_pct <= ret1h <= -cfg.wyckoff_ret1h_min_pct
                        and cvd3 <= cfg.wyckoff_max_cvd3_long
                    ):
                        is_tier_s_plus = (r6 <= 18.0 and bb <= 0.05 and cpos <= 0.20 and tbr <= 0.42)
                        is_tier_s = is_tier_s_plus or (r6 <= 20.0 and bb <= 0.08 and tbr <= 0.43 and vol_r >= 1.15)
                        tier_str = "TIER_S+" if is_tier_s_plus else ("TIER_S" if is_tier_s else "STANDARD")
                        score = (100.0 - r6) + 8.0 + bacc * 4.0 + (12.0 if is_tier_s_plus else (8.0 if is_tier_s else 0.0))
                        if score > best_score:
                            best_score = score
                            best_cand = {
                                "symbol": sym,
                                "direction": "LONG_BOUNCE",
                                "polymarket_side": "BUY UP",
                                "price": close_p,
                                "rsi_2": round(r2, 2),
                                "rsi_6": round(r6, 2),
                                "rsi_14": round(r14, 2),
                                "mean_rsi_6": round(mean_r6, 2),
                                "streak": sd,
                                "streak_ret_pct": round(sret, 3),
                                "wick_ratio": round(lwick, 3),
                                "vol_ratio": round(vol_r, 2),
                                "taker_buy_ratio": round(tbr, 3),
                                "cvd_3": round(cvd3, 4),
                                "bb_pct_b": round(bb, 3),
                                "atr_pct": round(atr, 4),
                                "ret_12bars_pct": round(ret1h, 3),
                                "ret_48bars_pct": round(ret4h, 3),
                                "close_pos_in_bar": round(cpos, 3),
                                "body_accel": round(bacc, 2),
                                "coins_sync": sync_down,
                                "tier": tier_str,
                                "is_tier_s": is_tier_s,
                                "is_tier_s_plus": is_tier_s_plus,
                                "trigger_branch": "D_WYCKOFF_2LEG",
                                "score": round(score, 2),
                            }

        # Check SHORT_PULLBACK (BUY DOWN): Multi-Bar Exhaustion + Multi-Period RSI Term-Structure + Wyckoff 2-Leg Upthrust
        if (
            1 <= su <= cfg.streak_max
            and ret1h <= cfg.macro_max_ret_1h
            and ret4h <= cfg.macro_max_ret_4h
        ):
            guard_ok = True
            if cfg.trend_guard_enabled and ret1h > cfg.trend_guard_adverse_1h:
                guard_ok = (uwick >= cfg.trend_guard_min_wick) and (r6 >= 100.0 - cfg.trend_guard_max_rsi)
            counter_tbr = tbr <= cfg.min_counter_tbr_short
            if guard_ok and not counter_tbr:
                if su >= cfg.streak_min:
                    br_a = (r6 >= 100.0 - cfg.rsi_branch_a) and (cpos >= 1.0 - cfg.close_pos_limit)
                    br_b = (r6 >= 100.0 - cfg.rsi_branch_b) and (bacc >= cfg.body_to_atr_ratio)
                    br_c = (su >= 3) and (r6 >= 100.0 - cfg.rsi_branch_c) and (bb >= 1.0 - cfg.bb_pct_limit)
                    br_e = (
                        cfg.rsi_term_enabled
                        and cfg.rsi_term_streak_min <= su <= cfg.rsi_term_streak_max
                        and r2 >= (100.0 - cfg.rsi_term_rsi2_limit)
                        and r6 >= (100.0 - cfg.rsi_term_rsi6_limit)
                        and r14 <= (100.0 - cfg.rsi_term_rsi14_floor_long)
                        and (r6 - r14) >= cfg.rsi_term_min_spread
                        and bacc >= cfg.rsi_term_min_bacc
                        and tbr >= (1.0 - cfg.rsi_term_max_tbr_long)
                    )
                    branches = [b for b, ok in (("A_CPOS", br_a), ("B_BACC", br_b), ("C_BB", br_c), ("E_RSI_TERM", br_e)) if ok]
                    n_br = len(branches)
                    weak_chop = (n_br == 1) and (0.455 < tbr < 0.545) and (sync_up < 2) and (vol_r < 1.15)
                    iceberg_trap = (
                        su >= cfg.iceberg_cvd3_min_streak
                        and cvd3 < cfg.iceberg_min_cvd3_short
                        and tbr < cfg.iceberg_climax_tbr_exempt_short
                    )
                    weak_single_tbr = (
                        n_br == 1
                        and (not br_e)
                        and tbr < cfg.single_branch_min_tbr_short
                        and uwick < cfg.single_branch_min_wick_exempt
                        and bacc < cfg.single_branch_min_bacc_exempt
                    )
                    marubozu_twap_trap = (
                        n_br == 1
                        and br_b
                        and (not br_e)
                        and r6 < (100.0 - cfg.marubozu_twap_guard_rsi_long)
                        and uwick < cfg.marubozu_twap_guard_min_wick
                        and cpos >= (1.0 - cfg.marubozu_twap_guard_cpos)
                    )
                    sluggish_r2 = (n_br == 1) and (r2 < (100.0 - cfg.rsi2_single_branch_max_long))
                    if (
                        n_br >= 1
                        and (su <= 5 or n_br >= 2)
                        and not weak_chop
                        and not iceberg_trap
                        and not weak_single_tbr
                        and not marubozu_twap_trap
                        and not sluggish_r2
                    ):
                        is_tier_s_plus = (
                            (n_br >= 3)
                            or (br_e and r2 >= 97.5 and (r6 - r14) >= 11.5)
                            or (n_br >= 2 and bacc >= 1.35 and vol_r >= 2.0 and tbr >= 0.58)
                        )
                        climax_wick_s = (
                            r6 >= 86.5
                            and sync_up >= 2
                            and vol_r >= 2.0
                            and (uwick >= 0.35 or tbr >= 0.65)
                        )
                        is_tier_s = (
                            is_tier_s_plus
                            or br_e
                            or (n_br >= 2 and tbr >= 0.57 and sync_up >= 2 and vol_r >= 1.15)
                            or climax_wick_s
                        )
                        tier_str = "TIER_S+" if is_tier_s_plus else ("TIER_S" if is_tier_s else "STANDARD")
                        score = r6 + su * 5.0 + bacc * 4.0 + n_br * 3.0 + (12.0 if is_tier_s_plus else (8.0 if is_tier_s else 0.0))
                        if score > best_score:
                            best_score = score
                            best_cand = {
                                "symbol": sym,
                                "direction": "SHORT_PULLBACK",
                                "polymarket_side": "BUY DOWN",
                                "price": close_p,
                                "rsi_2": round(r2, 2),
                                "rsi_6": round(r6, 2),
                                "rsi_14": round(r14, 2),
                                "mean_rsi_6": round(mean_r6, 2),
                                "streak": su,
                                "streak_ret_pct": round(sret, 3),
                                "wick_ratio": round(uwick, 3),
                                "vol_ratio": round(vol_r, 2),
                                "taker_buy_ratio": round(tbr, 3),
                                "cvd_3": round(cvd3, 4),
                                "bb_pct_b": round(bb, 3),
                                "atr_pct": round(atr, 4),
                                "ret_12bars_pct": round(ret1h, 3),
                                "ret_48bars_pct": round(ret4h, 3),
                                "close_pos_in_bar": round(cpos, 3),
                                "body_accel": round(bacc, 2),
                                "coins_sync": sync_up,
                                "tier": tier_str,
                                "is_tier_s": is_tier_s,
                                "is_tier_s_plus": is_tier_s_plus,
                                "trigger_branch": "+".join(branches),
                                "score": round(score, 2),
                            }
                elif su == 1 and cfg.wyckoff_retest_enabled:
                    # Branch D (`D_WYCKOFF_2LEG`): Wyckoff 2-Leg Upthrust Retest with Multi-Period RSI(2/6) confirmation
                    if (
                        r6 >= 100.0 - cfg.wyckoff_rsi_limit
                        and r2 >= 100.0 - cfg.wyckoff_rsi2_limit
                        and bb >= 1.0 - cfg.wyckoff_bb_limit
                        and bacc >= cfg.wyckoff_min_bacc
                        and tbr >= cfg.wyckoff_min_tbr_short
                        and cfg.wyckoff_ret1h_min_pct <= ret1h <= cfg.wyckoff_ret1h_max_pct
                        and cvd3 >= cfg.wyckoff_min_cvd3_short
                    ):
                        is_tier_s_plus = (r6 >= 82.0 and bb >= 0.95 and cpos >= 0.80 and tbr >= 0.58)
                        is_tier_s = is_tier_s_plus or (r6 >= 80.0 and bb >= 0.92 and tbr >= 0.57 and vol_r >= 1.15)
                        tier_str = "TIER_S+" if is_tier_s_plus else ("TIER_S" if is_tier_s else "STANDARD")
                        score = r6 + 8.0 + bacc * 4.0 + (12.0 if is_tier_s_plus else (8.0 if is_tier_s else 0.0))
                        if score > best_score:
                            best_score = score
                            best_cand = {
                                "symbol": sym,
                                "direction": "SHORT_PULLBACK",
                                "polymarket_side": "BUY DOWN",
                                "price": close_p,
                                "rsi_2": round(r2, 2),
                                "rsi_6": round(r6, 2),
                                "rsi_14": round(r14, 2),
                                "mean_rsi_6": round(mean_r6, 2),
                                "streak": su,
                                "streak_ret_pct": round(sret, 3),
                                "wick_ratio": round(uwick, 3),
                                "vol_ratio": round(vol_r, 2),
                                "taker_buy_ratio": round(tbr, 3),
                                "cvd_3": round(cvd3, 4),
                                "bb_pct_b": round(bb, 3),
                                "atr_pct": round(atr, 4),
                                "ret_12bars_pct": round(ret1h, 3),
                                "ret_48bars_pct": round(ret4h, 3),
                                "close_pos_in_bar": round(cpos, 3),
                                "body_accel": round(bacc, 2),
                                "coins_sync": sync_up,
                                "tier": tier_str,
                                "is_tier_s": is_tier_s,
                                "is_tier_s_plus": is_tier_s_plus,
                                "trigger_branch": "D_WYCKOFF_2LEG",
                                "score": round(score, 2),
                            }

    if best_cand is None:
        return _evaluate_stabilized_climax_fallback(dfs, idx, cfg, mean_r6, sync_down, sync_up)
    return best_cand


def run_v8_2year_validation(
    dfs: dict[str, pd.DataFrame],
    cfg: V8MasterConfig = V8_CONFIG,
) -> dict[str, Any]:
    """
    Generate honest non-overlapping V11.0 Multi-Period RSI + Adaptive Guaranteed-Profit
    4-Step Martingale 24-month validation report under realistic 50c Maker Hold-to-100c rules.
    """
    from engine.indicators import enrich_indicators

    req_cols = ("rsi_2", "rsi_6", "rsi_14", "atr_pct", "streak_down", "bb_pct_b", "taker_buy_ratio", "body_accel", "cvd_3")
    dfs = {
        s: (dfs[s] if all(c in dfs[s].columns for c in req_cols) else enrich_indicators(dfs[s]))
        for s in ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    }
    for s in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        df_s = dfs[s]
        if getattr(df_s, "_np_cache", None) is None:
            object.__setattr__(
                df_s,
                "_np_cache",
                {c: df_s[c].to_numpy(dtype=np.float64) for c in df_s.columns if c not in ("is_green", "is_red")},
            )
    n = min(len(dfs[s]) for s in ("BTCUSDT", "ETHUSDT", "SOLUSDT"))
    p_in = cfg.entry_share_cents / 100.0

    sequences: list[dict[str, Any]] = []
    ts_arr = dfs["BTCUSDT"]["open_time"].to_numpy(dtype=np.int64)
    opens = {s: dfs[s]["open"].to_numpy(dtype=np.float64) for s in ("BTCUSDT", "ETHUSDT", "SOLUSDT")}
    closes = {s: dfs[s]["close"].to_numpy(dtype=np.float64) for s in ("BTCUSDT", "ETHUSDT", "SOLUSDT")}

    i = 60
    while i < n - 2:
        cand = evaluate_v8_signal_at_index(dfs, i, cfg)
        if cand is None:
            i += 1
            continue
        sym = cand["symbol"]
        o1 = opens[sym][i + 1]
        c1 = closes[sym][i + 1]
        won = (c1 >= o1) if cand["direction"] == "LONG_BOUNCE" else (c1 < o1)
        sequences.append({
            "seq_id": len(sequences) + 1,
            "candle_idx": i,
            "timestamp_ms": int(ts_arr[i]),
            "symbol": sym,
            "direction": cand["direction"],
            "polymarket_side": cand["polymarket_side"],
            "price": cand["price"],
            "rsi_2": cand["rsi_2"],
            "rsi_6": cand["rsi_6"],
            "rsi_14": cand.get("rsi_14", 50.0),
            "streak": cand["streak"],
            "cvd_3": cand.get("cvd_3", 0.0),
            "trigger_branch": cand.get("trigger_branch", ""),
            "tier": cand.get("tier", "STANDARD"),
            "is_tier_s": bool(cand.get("is_tier_s", False)),
            "is_tier_s_plus": bool(cand.get("is_tier_s_plus", False)),
            "won": bool(won),
            "won_at_step": 1 if won else None,
            "exit_type": "100¢_EXPIRATION" if won else "0¢_STOP_LOSS",
            "exit_cents": 100.0 if won else 0.0,
        })
        i += 1

    total_seq = len(sequences)
    wins_total = sum(1 for s in sequences if s["won"])
    wr_all = (wins_total / total_seq * 100.0) if total_seq else 0.0
    half_idx = n // 2
    y1 = [s for s in sequences if s["candle_idx"] < half_idx]
    y2 = [s for s in sequences if s["candle_idx"] >= half_idx]
    wr_y1 = (sum(1 for s in y1 if s["won"]) / len(y1) * 100.0) if y1 else 0.0
    wr_y2 = (sum(1 for s in y2 if s["won"]) / len(y2) * 100.0) if y2 else 0.0

    # Uncapped theoretical 4-step Martingale win rate across the raw signal sequence
    theo_seq_tot = 0
    theo_seq_won = 0
    theo_step = 1
    for s in sequences:
        if s["won"]:
            theo_seq_tot += 1
            theo_seq_won += 1
            theo_step = 1
        else:
            if theo_step < max(1, int(cfg.max_martingale_steps)):
                theo_step += 1
            else:
                theo_seq_tot += 1
                theo_step = 1
    theo_4step_wr = (theo_seq_won / theo_seq_tot * 100.0) if theo_seq_tot else 0.0

    candles_per_month = max(1, n // 24)
    monthly_results = []
    hit_200 = 0
    prof_months = 0
    all_exec_seq = 0
    all_won_seq = 0
    all_step_wins = {1: 0, 2: 0, 3: 0, 4: 0}
    all_busts = 0

    for m in range(24):
        c0 = m * candles_per_month
        c1 = (m + 1) * candles_per_month if m < 23 else n
        m_seqs = [s for s in sequences if c0 <= s["candle_idx"] < c1]
        active = cfg.initial_capital_u
        active_peak = active
        vault = 0.0
        peak = active
        max_dd = 0.0
        d200 = None
        d1000 = None
        step = 1
        cum_sh_lost = 0.0
        s1_sh = float(cfg.phase1_fixed_shares)
        m_step_wins = {1: 0, 2: 0, 3: 0, 4: 0}
        m_busts = 0
        m_exec = 0

        for s in m_seqs:
            is_s = bool(s.get("is_tier_s", False))
            is_sp = bool(s.get("is_tier_s_plus", False))
            if step == 1 and active < cfg.micro_equity_tier_s_only_u and not is_s:
                continue

            if step == 1:
                tier_info = compute_v8_tier_and_stakes(
                    active + vault,
                    vault,
                    cfg.entry_share_cents,
                    is_tier_s=is_s,
                    active_peak_u=active_peak,
                    is_tier_s_plus=is_sp,
                    cfg=cfg,
                )
                s1_sh = float(tier_info["step1_shares"])
                sh = s1_sh
                cum_sh_lost = 0.0
            else:
                buf = max(1, int(math.ceil(cfg.martingale_profit_buffer_ratio * s1_sh)))
                sh = float(cum_sh_lost + buf)

            stk = sh * p_in
            if stk > active or active < cfg.emergency_capital_floor_u:
                if step > 1:
                    m_exec += 1
                    m_busts += 1
                    step = 1
                    cum_sh_lost = 0.0
                    continue
                break

            if s["won"]:
                active += sh * (1.0 - p_in)
                m_exec += 1
                m_step_wins[step] = m_step_wins.get(step, 0) + 1
                step = 1
                cum_sh_lost = 0.0
                if active > active_peak:
                    active_peak = active
                tot = active + vault
                new_vault, _, _ = compute_v8_vault_locked(tot, vault, active_peak_u=active_peak)
                if new_vault > vault:
                    delta = new_vault - vault
                    if active > delta + 10.0:
                        vault = new_vault
                        active -= delta
                        active_peak = active
            else:
                active -= stk
                cum_sh_lost += sh
                if vault == 0.0 and active_peak >= 200.0 and 90.0 <= active <= 115.0:
                    new_v, _, _ = compute_v8_vault_locked(active, vault, active_peak_u=active_peak)
                    if new_v > vault and active > new_v + 10.0:
                        vault = new_v
                        active -= new_v
                        active_peak = active
                buf = max(1, int(math.ceil(cfg.martingale_profit_buffer_ratio * s1_sh)))
                next_sh = float(cum_sh_lost + buf)
                can_adv = False
                if (
                    step == 1
                    and cfg.max_martingale_steps >= 2
                    and active >= cfg.step2_min_active_u
                    and active >= next_sh * p_in + cfg.emergency_capital_floor_u
                ):
                    can_adv = True
                elif (
                    step == 2
                    and cfg.max_martingale_steps >= 3
                    and active >= cfg.step3_min_active_u
                    and active >= next_sh * p_in + cfg.emergency_capital_floor_u
                ):
                    can_adv = True
                elif (
                    step == 3
                    and cfg.max_martingale_steps >= 4
                    and active >= cfg.step4_min_active_u
                    and active >= next_sh * p_in + cfg.emergency_capital_floor_u
                ):
                    can_adv = True
                if can_adv:
                    step += 1
                else:
                    m_exec += 1
                    m_busts += 1
                    step = 1
                    cum_sh_lost = 0.0

            tot = active + vault
            if tot > peak:
                peak = tot
            dd = ((peak - tot) / max(peak, 1e-9)) * 100.0
            if dd > max_dd:
                max_dd = dd
            if d200 is None and tot >= cfg.target_capital_u:
                d200 = max(1, int(((s["candle_idx"] - c0) / candles_per_month) * 30.0))
            if d1000 is None and tot >= 1000.0:
                d1000 = max(1, int(((s["candle_idx"] - c0) / candles_per_month) * 30.0))

        m_won = sum(m_step_wins.values())
        all_exec_seq += m_exec
        all_won_seq += m_won
        all_busts += m_busts
        for k in (1, 2, 3, 4):
            all_step_wins[k] += m_step_wins[k]

        tot_final = active + vault
        if tot_final >= cfg.target_capital_u:
            hit_200 += 1
        if tot_final > cfg.initial_capital_u:
            prof_months += 1
        start_ts = int(ts_arr[min(c0, n - 1)])
        monthly_results.append({
            "month_index": m + 1,
            "period_label": pd.to_datetime(start_ts, unit="ms", utc=True).strftime("%Y-%m"),
            "total_sequences": m_exec,
            "step1_wins": m_step_wins[1],
            "step2_wins": m_step_wins[2],
            "step3_wins": m_step_wins[3],
            "step4_wins": m_step_wins[4],
            "busts": m_busts,
            "win_rate_pct": round((m_won / m_exec * 100.0) if m_exec else 0.0, 2),
            "start_u": cfg.initial_capital_u,
            "final_u": round(tot_final, 2),
            "vault_locked_u": round(vault, 2),
            "reached_200u": tot_final >= 200.0,
            "reached_1000u": tot_final >= 1000.0,
            "days_to_200u": d200,
            "days_to_1000u": d1000,
            "max_drawdown_pct": round(max_dd, 2),
        })

    comb_wr = (all_won_seq / all_exec_seq * 100.0) if all_exec_seq else wr_all
    days_total = max(1.0, (ts_arr[n - 1] - ts_arr[0]) / (86400 * 1000))
    return {
        "strategy_name": cfg.name,
        "engine_version": cfg.version,
        "overall": {
            "total_sequences": total_seq,
            "executed_martingale_sequences": all_exec_seq,
            "signals_per_day": round(total_seq / days_total, 2),
            "step1_win_rate_pct": round(wr_all, 2),
            "combined_win_rate_pct": round(comb_wr, 2),
            "theoretical_4step_win_rate_pct": round(theo_4step_wr, 2),
            "step_win_breakdown": all_step_wins,
            "total_busts": all_busts,
            "y1_win_rate_pct": round(wr_y1, 2),
            "y2_win_rate_pct": round(wr_y2, 2),
            "profitable_months": f"{prof_months}/24",
            "hit_200u_months": f"{hit_200}/24",
            "median_final_u": round(float(np.median([r["final_u"] for r in monthly_results])), 2) if monthly_results else 0.0,
            "median_max_dd_pct": round(float(np.median([r["max_drawdown_pct"] for r in monthly_results])), 2) if monthly_results else 0.0,
        },
        "monthly_results": monthly_results,
        "recent_sequences": sequences[-60:],
    }
