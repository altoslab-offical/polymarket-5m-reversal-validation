#!/usr/bin/env python3
"""
Merge Binance features and Polymarket market/trade aggregates, simulate the
shared 3-coin state machine (no Martingale bet sizing in PnL; fixed 1-share stake),
export windows.csv, and compute Tables T1..T8 + Section 5 GO/NO-GO verdict in summary.md.
"""
from __future__ import annotations

import datetime
import pathlib
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent
PROJECT_ROOT = ROOT.parent
CACHE_DIR = ROOT / "cache"

WINDOWS_CSV = PROJECT_ROOT / "windows.csv"
SUMMARY_MD = PROJECT_ROOT / "summary.md"

COINS = ["BTC", "ETH", "SOL"]
BOOTSTRAP_ITERS = 10_000
BOOTSTRAP_SEED = 42
Z_95 = 1.959963984540054


def wilson_ci(wins: int, n: int) -> Tuple[float, float, float]:
    """Return (rate, low, high) in [0, 1] using 95% Wilson score interval."""
    if n <= 0:
        return (float("nan"), float("nan"), float("nan"))
    p = wins / n
    z2 = Z_95 * Z_95
    denom = 1.0 + z2 / n
    center = (p + z2 / (2.0 * n)) / denom
    half = (Z_95 * np.sqrt((p * (1.0 - p) / n) + (z2 / (4.0 * n * n)))) / denom
    return (p, max(0.0, float(center - half)), min(1.0, float(center + half)))


def bootstrap_mean_ci(values: np.ndarray, n_boot: int = BOOTSTRAP_ITERS, seed: int = BOOTSTRAP_SEED) -> Tuple[float, float, float]:
    """Return (mean, ci_low, ci_high) using deterministic numpy bootstrap."""
    arr = values[np.isfinite(values)]
    n = len(arr)
    if n == 0:
        return (float("nan"), float("nan"), float("nan"))
    mean_val = float(np.mean(arr))
    if n == 1:
        return (mean_val, mean_val, mean_val)
    rng = np.random.default_rng(seed)
    # Chunked vectorized bootstrap to keep memory tiny even if n is large
    boot_means = np.empty(n_boot, dtype=np.float64)
    chunk = 1000
    for i in range(0, n_boot, chunk):
        b = min(chunk, n_boot - i)
        idx = rng.integers(0, n, size=(b, n))
        boot_means[i : i + b] = arr[idx].mean(axis=1)
    lo, hi = np.percentile(boot_means, [2.5, 97.5])
    return (mean_val, float(lo), float(hi))


def summarize_subset(
    df_sub: pd.DataFrame,
    win_col: str = "win",
    pnl_col: str = "pnl_main",
    cost_col: str = "cost_main",
) -> Dict[str, Any]:
    valid = df_sub[df_sub[win_col].notna() & df_sub[pnl_col].notna()]
    n = int(len(valid))
    if n == 0:
        return {
            "n_raw": int(len(df_sub)),
            "n": 0,
            "wins": 0,
            "wr": float("nan"),
            "wr_lo": float("nan"),
            "wr_hi": float("nan"),
            "mean_cost": float("nan"),
            "mean_pnl": float("nan"),
            "pnl_lo": float("nan"),
            "pnl_hi": float("nan"),
        }
    wins = int(valid[win_col].sum())
    wr, wr_lo, wr_hi = wilson_ci(wins, n)
    mean_pnl, pnl_lo, pnl_hi = bootstrap_mean_ci(valid[pnl_col].to_numpy(dtype=np.float64))
    mean_cost = float(valid[cost_col].mean()) if cost_col in valid.columns and valid[cost_col].notna().any() else float("nan")
    return {
        "n_raw": int(len(df_sub)),
        "n": n,
        "wins": wins,
        "wr": wr,
        "wr_lo": wr_lo,
        "wr_hi": wr_hi,
        "mean_cost": mean_cost,
        "mean_pnl": mean_pnl,
        "pnl_lo": pnl_lo,
        "pnl_hi": pnl_hi,
    }


def fmt_pct(x: float) -> str:
    return "NA" if not np.isfinite(x) else f"{100.0 * x:.2f}%"


def fmt_usd(x: float) -> str:
    return "NA" if not np.isfinite(x) else f"{x:+.4f}"


def fmt_ci_pct(lo: float, hi: float) -> str:
    if not (np.isfinite(lo) and np.isfinite(hi)):
        return "NA"
    return f"[{100.0 * lo:.2f}%, {100.0 * hi:.2f}%]"


def fmt_ci_usd(lo: float, hi: float) -> str:
    if not (np.isfinite(lo) and np.isfinite(hi)):
        return "NA"
    return f"[{lo:+.4f}, {hi:+.4f}]"


def fmt_row(label: str, st: Dict[str, Any]) -> str:
    cost_s = "NA" if not np.isfinite(st["mean_cost"]) else f"${st['mean_cost']:.4f}"
    return (
        f"| {label} | {st['n']:,} | {fmt_pct(st['wr'])} | {fmt_ci_pct(st['wr_lo'], st['wr_hi'])} | "
        f"{cost_s} | {fmt_usd(st['mean_pnl'])} | {fmt_ci_usd(st['pnl_lo'], st['pnl_hi'])} |"
    )


def run_state_machine(df: pd.DataFrame) -> pd.DataFrame:
    """
    Section 2.5 shared state machine over strictly ordered windows T.
    Note: df must already contain T, coin, rsi7, sig_dir, pm_outcome.
    """
    df = df.sort_values(["T", "coin"]).reset_index(drop=True)
    n = len(df)
    sm_selected = np.zeros(n, dtype=np.int8)
    sm_round = np.full(n, np.nan, dtype=np.float64)
    sm_bust = np.full(n, np.nan, dtype=np.float64)
    sm_cooldown_blocked = np.zeros(n, dtype=np.int8)

    cooldown_until: Dict[str, int] = {c: -1 for c in COINS}
    cur_round = 1

    # Group row indices by T (3 rows per T)
    Ts = df["T"].to_numpy()
    coins = df["coin"].to_numpy()
    sigs = df["sig_dir"].to_numpy()
    rsis = df["rsi7"].to_numpy()
    outcomes = df["pm_outcome"].to_numpy()

    i = 0
    while i < n:
        cur_T = int(Ts[i])
        j = i
        while j < n and int(Ts[j]) == cur_T:
            j += 1

        cands: List[Tuple[float, int, int]] = []
        for idx in range(i, j):
            c_name = str(coins[idx])
            s = int(sigs[idx])
            if s != 0:
                if cur_T <= cooldown_until[c_name]:
                    sm_cooldown_blocked[idx] = 1
                else:
                    cands.append((abs(float(rsis[idx]) - 50.0), -COINS.index(c_name), idx))

        if cands:
            cands.sort(reverse=True)
            sel_idx = cands[0][2]
            sel_coin = str(coins[sel_idx])
            sel_sig = int(sigs[sel_idx])
            sel_outcome = outcomes[sel_idx]

            sm_selected[sel_idx] = 1
            sm_round[sel_idx] = float(cur_round)

            bet_side = "Up" if sel_sig == 1 else "Down"
            if sel_outcome in ("Up", "Down"):
                won = (sel_outcome == bet_side)
                if won:
                    sm_bust[sel_idx] = 0.0
                    cur_round = 1
                else:
                    # Cooldown 2 bars (T+300 and T+600 blocked for this coin)
                    cooldown_until[sel_coin] = cur_T + 600
                    if cur_round >= 6:
                        sm_bust[sel_idx] = 1.0
                        cur_round = 1
                    else:
                        sm_bust[sel_idx] = 0.0
                        cur_round += 1

        i = j

    df["sm_selected"] = sm_selected
    df["sm_round"] = pd.Series(sm_round, dtype="Int64")
    df["sm_bust"] = pd.Series(sm_bust, dtype="Int64")
    df["sm_cooldown_blocked"] = sm_cooldown_blocked
    return df


def compute_pnl_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    is_up = df["sig_dir_core"] == 1
    is_dn = df["sig_dir_core"] == -1
    has_core = is_up | is_dn

    df["bet_side"] = np.where(is_up, "Up", np.where(is_dn, "Down", None))

    def _pick(up_col: str, dn_col: str) -> np.ndarray:
        return np.where(
            is_up, df[up_col].to_numpy(dtype=np.float64),
            np.where(is_dn, df[dn_col].to_numpy(dtype=np.float64), np.nan),
        )

    pre30 = _pick("up_vwap_pre30", "down_vwap_pre30")
    v030 = _pick("up_vwap_0_30", "down_vwap_0_30")
    first = _pick("up_first_px", "down_first_px")
    v060 = _pick("up_vwap_0_60", "down_vwap_0_60")

    entry_main = np.where(np.isfinite(pre30), pre30, np.where(np.isfinite(v030), v030, first))
    entry_src = np.where(
        ~has_core, None,
        np.where(np.isfinite(pre30), "vwap_pre30",
        np.where(np.isfinite(v030), "vwap_0_30",
        np.where(np.isfinite(first), "first_px", "NA"))),
    )

    outcome = df["pm_outcome"].to_numpy()
    bet = df["bet_side"].to_numpy()
    valid_outcome = np.isin(outcome, ["Up", "Down"]) & has_core
    win = np.where(valid_outcome, (outcome == bet).astype(np.float64), np.nan)

    fee_rate = df["taker_fee_rate"].to_numpy(dtype=np.float64)
    cost_main = np.where(
        np.isfinite(entry_main) & np.isfinite(fee_rate),
        entry_main + fee_rate * entry_main * (1.0 - entry_main),
        np.nan,
    )
    pnl_main = win - cost_main
    pnl_assumed054 = np.where(valid_outcome, win - 0.54, np.nan)

    cost_060 = np.where(
        np.isfinite(v060) & np.isfinite(fee_rate),
        v060 + fee_rate * v060 * (1.0 - v060),
        np.nan,
    )
    pnl_060 = win - cost_060

    df["entry_px_pre30"] = pre30
    df["entry_px_main"] = entry_main
    df["entry_px_main_source"] = entry_src
    df["win"] = win
    df["cost_main"] = cost_main
    df["pnl_main"] = pnl_main
    df["pnl_assumed054"] = pnl_assumed054
    df["cost_assumed054"] = np.where(valid_outcome, 0.54, np.nan)
    df["entry_px_0_60"] = v060
    df["cost_vwap_0_60"] = cost_060
    df["pnl_vwap_0_60"] = pnl_060
    return df


def main() -> None:
    df_bn = pd.read_parquet(CACHE_DIR / "binance_features.parquet")
    df_gm = pd.read_parquet(CACHE_DIR / "gamma_markets.parquet")
    df_hs = pd.read_parquet(CACHE_DIR / "clob_hist_px.parquet")
    df_tr = pd.read_parquet(CACHE_DIR / "pm_trades_agg.parquet")

    # Merge on (coin, T)
    df = (
        df_bn
        .merge(df_gm, on=["coin", "T"], how="left")
        .merge(df_hs, on=["coin", "T"], how="left")
        .merge(df_tr, on=["coin", "T"], how="left")
    )

    unique_Ts = np.sort(df["T"].unique())
    n_Ts = len(unique_Ts)
    # 2/3 IS, 1/3 OOS strictly by time
    n_is = (2 * n_Ts) // 3
    oos_start_T = int(unique_Ts[n_is])

    df["utc_time"] = pd.to_datetime(df["T"], unit="s", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["month"] = pd.to_datetime(df["T"], unit="s", utc=True).dt.strftime("%Y-%m")
    df["utc_hour"] = (df["T"] % 86400) // 3600
    df["regime"] = "twap_60s"
    df["split"] = np.where(df["T"] < oos_start_T, "IS", "OOS")

    df = run_state_machine(df)
    df = compute_pnl_columns(df)

    # Strict column ordering for windows.csv
    ordered_cols = [
        "coin", "symbol", "T", "utc_time", "regime", "split",
        "slug", "found", "condition_id", "up_token_id", "down_token_id", "pm_outcome", "bn_next_up",
        "taker_fee_rate", "maker_fee_rate", "order_min_size", "is_twap60",
        "rsi7", "rsi2", "pctb", "lower_wick_ratio", "upper_wick_ratio",
        "taker_buy_ratio", "twap60_proxy", "twap60_source", "close_vs_twap",
        "consec_up", "consec_down", "adx14", "atr_ratio", "vol_z",
        "d_oi_5m_pct", "d_oi_15m_pct", "oi_is_na",
        "is_extreme", "sig_dir_core", "micro_1", "micro_2", "micro_3", "micro_4", "micro_score", "sig_dir",
        "sm_selected", "sm_round", "sm_bust", "sm_cooldown_blocked",
        "up_vwap_pre30", "down_vwap_pre30",
        "up_vwap_0_30", "down_vwap_0_30",
        "up_vwap_0_60", "down_vwap_0_60",
        "up_first_px", "down_first_px",
        "up_n_trades_0_60", "down_n_trades_0_60",
        "up_usdc_0_60", "down_usdc_0_60",
        "up_hist_px_T", "down_hist_px_T",
        "bet_side", "entry_px_pre30", "entry_px_main", "entry_px_main_source",
        "win", "cost_main", "pnl_main", "pnl_assumed054",
        "entry_px_0_60", "cost_vwap_0_60", "pnl_vwap_0_60",
    ]
    df_out = df[ordered_cols].copy()
    df_out.to_csv(WINDOWS_CSV, index=False, na_rep="NA")
    print(f"Exported {WINDOWS_CSV} ({len(df_out):,} rows, {len(ordered_cols)} cols)", flush=True)

    # ================= Build Summary Tables T1..T8 =================
    lines: List[str] = []
    lines.append("# Polymarket 5m 反轉訊號 — Phase A 優勢驗證報告 (`summary.md`)\n")
    lines.append(f"- **驗證期間（純 60 秒 TWAP 體制）**：`{df['utc_time'].iloc[0]}` (`T={int(unique_Ts[0])}`) 至 `{df['utc_time'].iloc[-1]}` (`T={int(unique_Ts[-1])}`)，共 **{n_Ts:,}** 個 5 分鐘視窗 × 3 幣（BTC / ETH / SOL）= **{len(df):,}** 列。")
    lines.append(f"- **樣本內 / 樣本外時間切分**：前 2/3 為樣本內 `IS`（`T < {oos_start_T}`, {n_is:,} 視窗 / `{pd.to_datetime(unique_Ts[0], unit='s', utc=True).strftime('%Y-%m-%d %H:%MZ')} ~ {pd.to_datetime(unique_Ts[n_is-1], unit='s', utc=True).strftime('%Y-%m-%d %H:%MZ')}`），後 1/3 為樣本外 `OOS`（`T >= {oos_start_T}`, {n_Ts - n_is:,} 視窗 / `{pd.to_datetime(oos_start_T, unit='s', utc=True).strftime('%Y-%m-%d %H:%MZ')} ~ {pd.to_datetime(unique_Ts[-1], unit='s', utc=True).strftime('%Y-%m-%d %H:%MZ')}`）。")
    lines.append("- **注額與成本口徑**：固定每單 **1 股**；主口徑取 `vwap_pre30`（缺值依序遞補 `vwap_0_30` → `first_px`），計入市場實測 `taker_fee_rate`（`0.07 × p × (1 - p)`）。")
    lines.append("- **信賴區間口徑**：勝率為 95% Wilson Score CI；平均每股 `pnl` 為 `10,000` 次 Bootstrap 95% CI（`seed=42`）。")
    lines.append("- **研究方法已知簡化與近似註記**：")
    lines.append("  1. **收盤近似（第 2.2 節）**：歷史特徵以 `[T-300, T)` 整根收盤價計算（原規格為 `T` 前 3–5 秒判斷），這會**略微高估**訊號品質與即時反應時間。")
    lines.append("  2. **TWAP 代理（第 2.3 節）**：本次區間內 `twap60_proxy` **100% 採用 Binance 1s K 線最後 60 根收盤均價**（`1m_ohlc4_fallback` 數為 `0`）。")
    lines.append("  3. **狀態機結算假設（第 2.5 節）**：假設視窗 `T` 的結果於 `T+300` 瞬間完成結算並供 `T+300` 視窗更新冷卻/關卡狀態（僅影響 T6 連輸關卡統計，不影響 T1–T5、T7 固定注額損益）。\n")

    # T1
    lines.append("## T1. 主結果（共享狀態機 `sm_selected = 1`，固定注額 1 股）\n")
    lines.append("| 區段 / 口徑 | 筆數 n | 勝率 | 勝率 Wilson 95% CI | 平均進場成本 | 平均每股 PnL | PnL Bootstrap 95% CI |")
    lines.append("|---|---:|---:|---|---:|---:|---|")
    sm = df[df["sm_selected"] == 1]
    t1_full = summarize_subset(sm, "win", "pnl_main", "cost_main")
    t1_is = summarize_subset(sm[sm["split"] == "IS"], "win", "pnl_main", "cost_main")
    t1_oos = summarize_subset(sm[sm["split"] == "OOS"], "win", "pnl_main", "cost_main")
    lines.append(fmt_row("Full（主口徑 pre30 + taker fee）", t1_full))
    lines.append(fmt_row("IS 前 2/3（主口徑 pre30 + taker fee）", t1_is))
    lines.append(fmt_row("**OOS 後 1/3（主口徑 pre30 + taker fee）**", t1_oos))
    lines.append(fmt_row("Full 對照：假設 $0.54 零手續費 (`pnl_assumed054`)", summarize_subset(sm, "win", "pnl_assumed054", "cost_assumed054")))
    lines.append(fmt_row("OOS 對照：假設 $0.54 零手續費 (`pnl_assumed054`)", summarize_subset(sm[sm["split"] == "OOS"], "win", "pnl_assumed054", "cost_assumed054")))
    lines.append(fmt_row("Full 對照：開盤後 60s VWAP (`pnl_vwap_0_60`)", summarize_subset(sm, "win", "pnl_vwap_0_60", "cost_vwap_0_60")))
    lines.append(fmt_row("OOS 對照：開盤後 60s VWAP (`pnl_vwap_0_60`)", summarize_subset(sm[sm["split"] == "OOS"], "win", "pnl_vwap_0_60", "cost_vwap_0_60")))
    lines.append("")

    # T2
    lines.append("## T2. 獨立訊號表現（所有 `sig_dir != 0`，不經狀態機，依幣種）\n")
    lines.append("| 幣種 / 區段 | 筆數 n | 勝率 | 勝率 Wilson 95% CI | 平均進場成本 | 平均每股 PnL | PnL Bootstrap 95% CI |")
    lines.append("|---|---:|---:|---|---:|---:|---|")
    sig_all = df[df["sig_dir"] != 0]
    t2_full_coins: Dict[str, Dict[str, Any]] = {}
    t2_oos_coins: Dict[str, Dict[str, Any]] = {}
    for c in COINS:
        sub_c = sig_all[sig_all["coin"] == c]
        t2_full_coins[c] = summarize_subset(sub_c)
        t2_oos_coins[c] = summarize_subset(sub_c[sub_c["split"] == "OOS"])
        lines.append(fmt_row(f"{c} (Full)", t2_full_coins[c]))
        lines.append(fmt_row(f"{c} (IS)", summarize_subset(sub_c[sub_c["split"] == "IS"])))
        lines.append(fmt_row(f"{c} (OOS)", t2_oos_coins[c]))
    lines.append(fmt_row("三幣合計 (Full)", summarize_subset(sig_all)))
    lines.append(fmt_row("三幣合計 (OOS)", summarize_subset(sig_all[sig_all["split"] == "OOS"])))
    lines.append("")

    # T3 Ablation
    lines.append("## T3. 訊號消融分析（全樣本獨立訊號，主口徑 PnL）\n")
    lines.append("| 條件階層 / 單項測試 | 筆數 n | 勝率 | 勝率 Wilson 95% CI | 平均進場成本 | 平均每股 PnL | PnL Bootstrap 95% CI |")
    lines.append("|---|---:|---:|---|---:|---:|---|")
    c_core = df[df["sig_dir_core"] != 0]
    c_non_ext = df[(df["sig_dir_core"] != 0) & (df["is_extreme"] == 0)]
    c_full_sig = df[df["sig_dir"] != 0]
    lines.append(fmt_row("1. 僅核心條件 (`sig_dir_core != 0`)", summarize_subset(c_core)))
    lines.append(fmt_row("2. 核心 + 非極端過濾 (`is_extreme == 0`)", summarize_subset(c_non_ext)))
    lines.append(fmt_row("3. 核心 + 非極端 + `micro_score >= 2` (`sig_dir != 0`)", summarize_subset(c_full_sig)))
    lines.append(fmt_row("4a. 核心 + 非極端 + 單獨 micro_1 (`RSI2` 極值)", summarize_subset(c_non_ext[c_non_ext["micro_1"] == 1])))
    lines.append(fmt_row("4b. 核心 + 非極端 + 單獨 micro_2 (`%B` 突破)", summarize_subset(c_non_ext[c_non_ext["micro_2"] == 1])))
    lines.append(fmt_row("4c. 核心 + 非極端 + 單獨 micro_3 (`wick_ratio >= 0.12`)", summarize_subset(c_non_ext[c_non_ext["micro_3"] == 1])))
    lines.append(fmt_row("4d. 核心 + 非極端 + 單獨 micro_4 (`taker_buy` + `TWAP`)", summarize_subset(c_non_ext[c_non_ext["micro_4"] == 1])))
    lines.append(fmt_row("5a. 僅核心 + 單獨 micro_1 (`RSI2` 極值)", summarize_subset(c_core[c_core["micro_1"] == 1])))
    lines.append(fmt_row("5b. 僅核心 + 單獨 micro_2 (`%B` 突破)", summarize_subset(c_core[c_core["micro_2"] == 1])))
    lines.append(fmt_row("5c. 僅核心 + 單獨 micro_3 (`wick_ratio >= 0.12`)", summarize_subset(c_core[c_core["micro_3"] == 1])))
    lines.append(fmt_row("5d. 僅核心 + 單獨 micro_4 (`taker_buy` + `TWAP`)", summarize_subset(c_core[c_core["micro_4"] == 1])))
    lines.append("")

    # T4 Calibration across ALL windows
    lines.append("## T4. 全市場價格校準（所有視窗，依 `up_vwap_pre30` 每 0.05 分桶 vs 實際 `Up` 勝率）\n")
    lines.append("| `up_vwap_pre30` 區間 | 筆數 n | 平均報價 | 實際 Up 勝率 | 勝率 Wilson 95% CI | 買 Up 平均 PnL (含 fee) | PnL Bootstrap 95% CI |")
    lines.append("|---|---:|---:|---:|---|---:|---|")
    cal_df = df[df["up_vwap_pre30"].notna() & df["pm_outcome"].isin(["Up", "Down"]) & df["taker_fee_rate"].notna()].copy()
    cal_df["cal_win"] = (cal_df["pm_outcome"] == "Up").astype(np.float64)
    p_cal = cal_df["up_vwap_pre30"].to_numpy(dtype=np.float64)
    f_cal = cal_df["taker_fee_rate"].to_numpy(dtype=np.float64)
    cal_df["cal_cost"] = p_cal + f_cal * p_cal * (1.0 - p_cal)
    cal_df["cal_pnl"] = cal_df["cal_win"] - cal_df["cal_cost"]
    edges = [round(i * 0.05, 2) for i in range(21)]
    for b_idx in range(20):
        lo_b, hi_b = edges[b_idx], edges[b_idx + 1]
        if b_idx < 19:
            mask = (cal_df["up_vwap_pre30"] >= lo_b) & (cal_df["up_vwap_pre30"] < hi_b)
            lbl = f"[{lo_b:.2f}, {hi_b:.2f})"
        else:
            mask = (cal_df["up_vwap_pre30"] >= lo_b) & (cal_df["up_vwap_pre30"] <= hi_b)
            lbl = f"[{lo_b:.2f}, {hi_b:.2f}]"
        sub_b = cal_df[mask]
        if len(sub_b) == 0:
            lines.append(f"| {lbl} | 0 | NA | NA | NA | NA | NA |")
        else:
            st_b = summarize_subset(sub_b, "cal_win", "cal_pnl", "cal_cost")
            avg_q = float(sub_b["up_vwap_pre30"].mean())
            lines.append(
                f"| {lbl} | {st_b['n']:,} | ${avg_q:.4f} | {fmt_pct(st_b['wr'])} | {fmt_ci_pct(st_b['wr_lo'], st_b['wr_hi'])} | "
                f"{fmt_usd(st_b['mean_pnl'])} | {fmt_ci_usd(st_b['pnl_lo'], st_b['pnl_hi'])} |"
            )
    lines.append("")

    # T5 Signal price vs non-signal price
    lines.append("## T5. 訊號視窗與非訊號視窗的進場價比較\n")
    lines.append("| 比較組別 | 樣本數 n (非NA) | 平均價格 | Bootstrap 95% CI | 中位數 |")
    lines.append("|---|---:|---:|---|---:|")
    up_sig = df[df["sig_dir"] == 1]["up_vwap_pre30"].dropna()
    up_nosig = df[df["sig_dir"] == 0]["up_vwap_pre30"].dropna()
    dn_sig = df[df["sig_dir"] == -1]["down_vwap_pre30"].dropna()
    dn_nosig = df[df["sig_dir"] == 0]["down_vwap_pre30"].dropna()

    # Main-hierarchy entry price (pre30 -> 0_30 -> first_px) for Up / Down on non-signal windows too
    up_main_all = np.where(df["up_vwap_pre30"].notna(), df["up_vwap_pre30"], np.where(df["up_vwap_0_30"].notna(), df["up_vwap_0_30"], df["up_first_px"]))
    dn_main_all = np.where(df["down_vwap_pre30"].notna(), df["down_vwap_pre30"], np.where(df["down_vwap_0_30"].notna(), df["down_vwap_0_30"], df["down_first_px"]))
    sig_main_px = df[df["sig_dir"] != 0]["entry_px_main"].dropna()
    nosig_both_px = pd.Series(np.concatenate([up_main_all[df["sig_dir"] == 0], dn_main_all[df["sig_dir"] == 0]])).dropna()

    for label, s in [
        ("押 Up 訊號 (`sig_dir == +1`) 之 `up_vwap_pre30`", up_sig),
        ("非訊號視窗 (`sig_dir == 0`) 之 `up_vwap_pre30`", up_nosig),
        ("押 Down 訊號 (`sig_dir == -1`) 之 `down_vwap_pre30`", dn_sig),
        ("非訊號視窗 (`sig_dir == 0`) 之 `down_vwap_pre30`", dn_nosig),
        ("所有訊號 (`sig_dir != 0`) 之押注側主口徑進場價 (`entry_px_main`)", sig_main_px),
        ("非訊號視窗 (`sig_dir == 0`) 全體 Up+Down 主口徑價格", nosig_both_px),
    ]:
        arr = s.to_numpy(dtype=np.float64)
        m, lo, hi = bootstrap_mean_ci(arr)
        med = float(np.median(arr)) if len(arr) else float("nan")
        lines.append(f"| {label} | {len(arr):,} | ${m:.4f} | [${lo:.4f}, ${hi:.4f}] | ${med:.4f} |")
    lines.append("")

    # T6 Consecutive losses & round distribution in State Machine
    lines.append("## T6. 狀態機連輸分布與獨立假設對照（基於 `sm_selected = 1` 且已結算單）\n")
    sm_res = sm[sm["win"].notna()].sort_values("T")
    sm_wins_arr = sm_res["win"].to_numpy(dtype=np.int64)
    n_sm_res = len(sm_wins_arr)
    w_emp = float(sm_wins_arr.mean()) if n_sm_res > 0 else float("nan")
    q_emp = 1.0 - w_emp

    # Count run-lengths of consecutive 0s (losses) in the state machine trade stream
    # Also report state-machine rounds 1..6 counts & bust count
    loss_runs: List[int] = []
    cur_loss = 0
    for v in sm_wins_arr:
        if v == 0:
            cur_loss += 1
        else:
            if cur_loss > 0:
                loss_runs.append(cur_loss)
                cur_loss = 0
    if cur_loss > 0:
        loss_runs.append(cur_loss)

    n_loss_runs = len(loss_runs)
    n_busts = int((sm_res["sm_bust"] == 1).sum())
    n_sequences = int((sm_res["sm_round"] == 1).sum())
    lines.append(f"- 狀態機已結算總筆數：`{n_sm_res:,}`；實測單關勝率 `w = {100*w_emp:.2f}%`（`1 - w = {100*q_emp:.2f}%`）；獨立連輸事件總數（由敗局起算）：`{n_loss_runs:,}`；馬丁輪次總數（`sm_round == 1`）：`{n_sequences:,}`；**6 關爆倉次數**：`{n_busts}`（每輪爆倉率 `{100*n_busts/max(n_sequences,1):.3f}%` vs 獨立假設 `(1-w)^6 = {100*(q_emp**6):.3f}%`）。\n")
    lines.append("| 連輸門檻 `k` | `sm_round` 連輸累積達 `k` 敗輪數 (佔總輪數) | 獨立理論 `(1-w)^k` | 連續負場數 >= `k` 次 | 連輸延續條件比率 (`>=k / 總連輸事件`) vs `(1-w)^(k-1)` |")
    lines.append("|---|---:|---:|---:|---:|")
    for k in range(1, 8):
        cnt_ge_k = sum(1 for r in loss_runs if r >= k)
        emp_cond = cnt_ge_k / max(n_loss_runs, 1)
        theo_cond = q_emp ** (k - 1) if np.isfinite(q_emp) else float("nan")
        theo_k = (q_emp ** k) if np.isfinite(q_emp) else float("nan")
        if k <= 6:
            # Count rounds reaching sm_round == k AND losing at round k
            lost_at_k = int(((sm_res["sm_round"] == k) & (sm_res["win"] == 0)).sum())
            seq_rate = lost_at_k / max(n_sequences, 1)
            seq_col = f"{lost_at_k:,} / {n_sequences:,} ({fmt_pct(seq_rate)})"
        else:
            seq_col = "— (第6關止步歸1)"
        lines.append(
            f"| k = {k} | {seq_col} | {fmt_pct(theo_k)} | {cnt_ge_k:,} | "
            f"實測 {fmt_pct(emp_cond)} vs 獨立 {fmt_pct(theo_cond)} |"
        )
    lines.append("\n| 狀態機關卡 `sm_round` | 筆數 n | 該關勝率 | 該關爆倉數 |")
    lines.append("|---|---:|---:|---:|")
    for r_num in range(1, 7):
        sub_r = sm_res[sm_res["sm_round"] == r_num]
        nr = len(sub_r)
        wr_r = float(sub_r["win"].mean()) if nr > 0 else float("nan")
        br_r = int((sub_r["sm_bust"] == 1).sum())
        lines.append(f"| 第 {r_num} 關 | {nr:,} | {fmt_pct(wr_r)} | {br_r} |")
    lines.append("")

    # T7 Slices
    lines.append("## T7. 時間與行情切片（`sm_selected = 1`，主口徑）\n")
    lines.append("### 7a. 依月份切片\n")
    lines.append("| 月份 (所屬區間) | 筆數 n | 勝率 | 勝率 Wilson 95% CI | 平均進場成本 | 平均每股 PnL | PnL Bootstrap 95% CI |")
    lines.append("|---|---:|---:|---|---:|---:|---|")
    for mo in sorted(df["month"].unique()):
        sub_m = sm[sm["month"] == mo]
        # Annotate whether month is IS, OOS, or boundary
        splits_in_mo = sorted(df[df["month"] == mo]["split"].unique())
        tag = "+".join(splits_in_mo)
        lines.append(fmt_row(f"{mo} ({tag})", summarize_subset(sub_m)))
    # Also split OOS explicitly by month for pre-registered Rule 4 check!
    lines.append("\n**OOS 區間內逐月切片（供第 5 節條件 4 檢驗）**\n")
    lines.append("| OOS 月份 | OOS T1 筆數 n | T1 勝率 | T1 平均 PnL | T1 PnL 95% CI | OOS 獨立訊號(T2) n | T2 平均 PnL |")
    lines.append("|---|---:|---:|---:|---|---:|---:|")
    sm_oos = sm[sm["split"] == "OOS"]
    sig_oos = sig_all[sig_all["split"] == "OOS"]
    oos_month_min_t1 = float("inf")
    oos_month_min_t2 = float("inf")
    for mo in sorted(sm_oos["month"].unique()):
        st_m1 = summarize_subset(sm_oos[sm_oos["month"] == mo])
        st_m2 = summarize_subset(sig_oos[sig_oos["month"] == mo])
        if np.isfinite(st_m1["mean_pnl"]):
            oos_month_min_t1 = min(oos_month_min_t1, st_m1["mean_pnl"])
        if np.isfinite(st_m2["mean_pnl"]):
            oos_month_min_t2 = min(oos_month_min_t2, st_m2["mean_pnl"])
        lines.append(
            f"| {mo} | {st_m1['n']:,} | {fmt_pct(st_m1['wr'])} | {fmt_usd(st_m1['mean_pnl'])} | "
            f"{fmt_ci_usd(st_m1['pnl_lo'], st_m1['pnl_hi'])} | {st_m2['n']:,} | {fmt_usd(st_m2['mean_pnl'])} |"
        )

    lines.append("\n### 7b. 依 UTC 小時（每 4 小時一組）\n")
    lines.append("| UTC 時段 | 筆數 n | 勝率 | 勝率 Wilson 95% CI | 平均進場成本 | 平均每股 PnL | PnL Bootstrap 95% CI |")
    lines.append("|---|---:|---:|---|---:|---:|---|")
    for h0 in range(0, 24, 4):
        sub_h = sm[(sm["utc_hour"] >= h0) & (sm["utc_hour"] < h0 + 4)]
        lines.append(fmt_row(f"{h0:02d}:00–{h0+4:02d}:00 UTC", summarize_subset(sub_h)))

    lines.append("\n### 7c. 依極端行情過濾是否觸發（對照核心 + micro>=2 下被過濾 vs 放行）\n")
    lines.append("| 切片 | 筆數 n | 勝率 | 勝率 Wilson 95% CI | 平均進場成本 | 平均每股 PnL | PnL Bootstrap 95% CI |")
    lines.append("|---|---:|---:|---|---:|---:|---|")
    lines.append(fmt_row("T1 主結果 (`sm_selected = 1`, 均為非極端)", t1_full))
    cand_pool = df[(df["sig_dir_core"] != 0) & (df["micro_score"] >= 2)]
    lines.append(fmt_row("獨立候選・非極端放行 (`is_extreme == 0`)", summarize_subset(cand_pool[cand_pool["is_extreme"] == 0])))
    lines.append(fmt_row("獨立候選・極端過濾阻擋 (`is_extreme == 1`)", summarize_subset(cand_pool[cand_pool["is_extreme"] == 1])))
    lines.append("")

    # T8 Data Quality
    lines.append("## T8. 資料品質檢查\n")
    n_total = len(df)
    n_not_found = int((df["found"] == 0).sum())
    n_pm_na = int((~df["pm_outcome"].isin(["Up", "Down"])).sum())
    comp_pm_bn = df[df["pm_outcome"].isin(["Up", "Down"]) & df["bn_next_up"].notna()].copy()
    comp_pm_bn["pm_up"] = (comp_pm_bn["pm_outcome"] == "Up").astype(int)
    n_disagree_all = int((comp_pm_bn["pm_up"] != comp_pm_bn["bn_next_up"].astype(int)).sum())

    comp_sig = comp_pm_bn[comp_pm_bn["sig_dir"] != 0]
    n_disagree_sig = int((comp_sig["pm_up"] != comp_sig["bn_next_up"].astype(int)).sum())

    up_pre30_na = int(df["up_vwap_pre30"].isna().sum())
    dn_pre30_na = int(df["down_vwap_pre30"].isna().sum())
    sig_pre30_na = int(sig_all["entry_px_pre30"].isna().sum())
    sig_main_na = int(sig_all["entry_px_main"].isna().sum())
    sm_pre30_na = int(sm["entry_px_pre30"].isna().sum())
    sm_main_na = int(sm["entry_px_main"].isna().sum())
    oi_na_cnt = int(df["oi_is_na"].sum())

    lines.append("| 品質指標 | 數量 | 佔比 / 比率 |")
    lines.append("|---|---:|---:|")
    lines.append(f"| 總列數（三幣 × {n_Ts:,} 視窗） | {n_total:,} | 100.00% |")
    lines.append(f"| 找不到市場數 (`found == 0`) | {n_not_found:,} | {100*n_not_found/n_total:.4f}% |")
    lines.append(f"| `pm_outcome = NA` 數（含找不到或未結算） | {n_pm_na:,} | {100*n_pm_na/n_total:.4f}% |")
    lines.append(f"| `bn_next_up` 與 `pm_outcome` 不一致（全視窗可比 {len(comp_pm_bn):,} 筆） | {n_disagree_all:,} | {100*n_disagree_all/max(len(comp_pm_bn),1):.2f}% |")
    lines.append(f"| `bn_next_up` 與 `pm_outcome` 不一致（`sig_dir != 0` 訊號視窗 {len(comp_sig):,} 筆） | {n_disagree_sig:,} | {100*n_disagree_sig/max(len(comp_sig),1):.2f}% |")
    lines.append(f"| 全視窗 `up_vwap_pre30` 缺值率 | {up_pre30_na:,} | {100*up_pre30_na/n_total:.2f}% |")
    lines.append(f"| 全視窗 `down_vwap_pre30` 缺值率 | {dn_pre30_na:,} | {100*dn_pre30_na/n_total:.2f}% |")
    lines.append(f"| 訊號視窗 (`sig_dir != 0`, {len(sig_all):,} 筆) 押注側 `vwap_pre30` 缺值率 | {sig_pre30_na:,} | {100*sig_pre30_na/max(len(sig_all),1):.2f}% |")
    lines.append(f"| 訊號視窗主口徑 `entry_px_main` 仍缺值（連 `vwap_0_30`、`first_px` 都無） | {sig_main_na:,} | {100*sig_main_na/max(len(sig_all),1):.2f}% |")
    lines.append(f"| 狀態機選單 (`sm_selected = 1`, {len(sm):,} 筆) `vwap_pre30` 缺值率 | {sm_pre30_na:,} | {100*sm_pre30_na/max(len(sm),1):.2f}% |")
    lines.append(f"| 狀態機選單主口徑 `entry_px_main` 仍缺值 | {sm_main_na:,} | {100*sm_main_na/max(len(sm),1):.2f}% |")
    lines.append(f"| OI (`d_oi_5m_pct` / `d_oi_15m_pct`) 為 NA 數 | {oi_na_cnt:,} | {100*oi_na_cnt/n_total:.4f}% |")
    lines.append("")

    # Section 5 Pre-registered Verdict
    cond1 = (t1_oos["n"] >= 300)
    cond2 = bool(np.isfinite(t1_oos["pnl_lo"]) and t1_oos["pnl_lo"] > 0.0)
    n_pos_oos_coins = sum(1 for c in COINS if np.isfinite(t2_oos_coins[c]["mean_pnl"]) and t2_oos_coins[c]["mean_pnl"] > 0.0)
    n_pos_full_coins = sum(1 for c in COINS if np.isfinite(t2_full_coins[c]["mean_pnl"]) and t2_full_coins[c]["mean_pnl"] > 0.0)
    cond3_oos = (n_pos_oos_coins >= 2)
    cond3_full = (n_pos_full_coins >= 2)
    cond4 = bool(np.isfinite(oos_month_min_t1) and oos_month_min_t1 >= -0.03)

    oos_coin_str = ", ".join(f"{c}:{fmt_usd(t2_oos_coins[c]['mean_pnl'])}" for c in COINS)
    full_coin_str = ", ".join(f"{c}:{fmt_usd(t2_full_coins[c]['mean_pnl'])}" for c in COINS)

    lines.append("## 第 5 節：預先登記判定規則檢驗與最終結論\n")
    lines.append("| 條件 | 要求 | 實測數值 | 判定 |")
    lines.append("|---|---|---|---|")
    lines.append(f"| 條件 1 | 樣本外 `OOS` T1 `n >= 300` | `n = {t1_oos['n']:,}` | {'PASS' if cond1 else 'FAIL'} |")
    lines.append(f"| 條件 2 | 樣本外 `OOS` T1 平均 PnL Bootstrap 95% CI **下界 > 0** | 平均 `{fmt_usd(t1_oos['mean_pnl'])}` / 下界 `{fmt_usd(t1_oos['pnl_lo'])}` | {'PASS' if cond2 else 'FAIL'} |")
    lines.append(
        f"| 條件 3 | 三個幣中至少兩個 T2 平均 PnL > 0 | "
        f"OOS `{n_pos_oos_coins}/3` ({oos_coin_str})；"
        f"Full `{n_pos_full_coins}/3` ({full_coin_str}) | "
        f"{'PASS' if (cond3_oos and cond3_full) else ('PARTIAL' if (cond3_oos or cond3_full) else 'FAIL')} |"
    )
    lines.append(f"| 條件 4 | 樣本外期間無任何月份平均 PnL < -0.03 | OOS 各月最低 T1 PnL = `{fmt_usd(oos_month_min_t1)}`（T2 最低 `{fmt_usd(oos_month_min_t2)}`） | {'PASS' if cond4 else 'FAIL'} |")
    lines.append("")

    if not cond1:
        verdict = "**樣本不足，無法判定**（OOS T1 n < 300）。"
    elif cond1 and cond2 and cond3_oos and cond4:
        verdict = "**GO（值得進入 Phase B）**"
    else:
        verdict = "**NO-GO：目前資料不支持訊號有優勢**"
    lines.append(f"### 最終判定：{verdict}\n")

    SUMMARY_MD.write_text("\n".join(lines), encoding="utf-8")
    print(f"Exported {SUMMARY_MD}", flush=True)


if __name__ == "__main__":
    main()
