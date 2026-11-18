#!/usr/bin/env python3
"""
Complete Unbiased Backtest Suite for TWAP Sniper V2:
  1. Real 1s-CLOB Replay on `live_btc_5m.db` (1,013 BTC windows = 3.517 days, 100% Gamma TWAP
     backfill, conservative +1s & +2s execution delay) for V1, V2_FILTERED, V2_PURE, and
     V2_HIGHBAND_SHADOW ([0.84, 0.95] data reference).
  2. Honest Frequency, Win-Rate, and Monthly Projection Matrix anchored to `4.1` / `1s-CLOB`
     (explicitly marking `L4` $43->$192 compounding retracted & CI pre-4.2).
"""
from __future__ import annotations

from dataclasses import asdict
import math
import pathlib
import sqlite3
import sys
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent
REPO = ROOT.parent
sys.path.insert(0, str(REPO))

from engine.oracle_sniper import (
    DEFAULT_SNIPER_CONFIG,
    TWAP_V2_FILTERED_CONFIG,
    TWAP_V2_HIGHBAND_SHADOW_CONFIG,
    TWAP_V2_PURE_CONFIG,
    FairValueEstimate,
    OpenPosition,
    OrderBookTop,
    SniperConfig,
    compute_shares,
    effective_exit_proceeds,
    entry_limit_price,
    estimate_fair_value,
    estimate_twap_fair_value,
    evaluate_entry,
    evaluate_exit,
    taker_fee_per_share,
)

DB_PATH = REPO / "data/oracle_sniper/live_btc_5m.db"
GAMMA_BACKFILL_PQ = ROOT / "cache/live_btc_5m_gamma_backfill.parquet"
P41_PQ = ROOT / "phase41_intrawindow.parquet"
OUT_MD = ROOT / "strategy_v2_report.md"
OUT_TRADES_CSV = ROOT / "v2_backtest_trades_clob1s.csv"


def wilson_ci(wins: int, n: int, z: float = 1.95996398454) -> Tuple[float, float]:
    """Wilson 95% proportion CI."""
    if n <= 0:
        return (np.nan, np.nan)
    p = wins / n
    den = 1.0 + z * z / n
    ctr = (p + z * z / (2.0 * n)) / den
    half = (z / den) * math.sqrt((p * (1.0 - p) + z * z / (4.0 * n)) / n)
    return (max(0.0, ctr - half), min(1.0, ctr + half))


def bootstrap_ci(vals: np.ndarray, n_boot: int = 1000, seed: int = 42) -> Tuple[float, float]:
    """Bootstrap 95% mean CI."""
    if len(vals) < 2:
        m = float(np.mean(vals)) if len(vals) else np.nan
        return (m, m)
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(vals), size=(n_boot, len(vals)))
    lo, hi = np.percentile(vals[idx].mean(axis=1), [2.5, 97.5])
    return (float(lo), float(hi))


def df_to_md(df: pd.DataFrame) -> str:
    """GitHub Markdown table formatter."""
    cols = list(df.columns)
    hdr = "| " + " | ".join(str(c) for c in cols) + " |"
    sep = "| " + " | ".join("---" for _ in cols) + " |"
    body = [
        "| " + " | ".join("" if (isinstance(v, float) and math.isnan(v)) else str(v) for v in r) + " |"
        for r in df.itertuples(index=False, name=None)
    ]
    return "\n".join([hdr, sep, *body])


def load_1s_clob_windows() -> Tuple[List[tuple[int, str, float, str, np.ndarray, Dict[int, int], pd.DataFrame]], int]:
    """Load 1,013 real 1s-CLOB BTC windows with 100% Gamma TWAP backfill and trailing 30m vol."""
    df_w = pd.read_parquet(GAMMA_BACKFILL_PQ)
    con = sqlite3.connect(DB_PATH)
    df_s = pd.read_sql_query(
        "SELECT ts, window_ts, elapsed_sec, chainlink_price, binance_price, coinbase_price, vol_1s, cvd_imb, obi, up_bid, up_ask, down_bid, down_ask FROM snapshots ORDER BY window_ts, elapsed_sec",
        con,
    )
    con.close()
    df = df_s.merge(df_w[["window_ts", "gm_ptb", "gm_out"]], on="window_ts", how="inner")
    df = df[df["gm_out"].isin(["UP", "DOWN"]) & df["gm_ptb"].notna()].sort_values(["window_ts", "elapsed_sec"]).reset_index(drop=True)

    in_tw = df["elapsed_sec"] >= 240.0
    df["cl_240"] = np.where(in_tw & df["chainlink_price"].notna(), df["chainlink_price"], 0.0)
    df["n_240"] = np.where(in_tw & df["chainlink_price"].notna(), 1.0, 0.0)
    df["A_240"] = np.where(
        df.groupby("window_ts")["n_240"].cumsum() > 0,
        df.groupby("window_ts")["cl_240"].cumsum() / np.maximum(df.groupby("window_ts")["n_240"].cumsum(), 1.0),
        df["chainlink_price"],
    )
    bn = df["binance_price"].ffill().to_numpy(dtype=np.float64)
    dt = np.diff(df["ts"].to_numpy(dtype=np.float64), prepend=df["ts"].iloc[0] - 1.0)
    ret = np.where((dt > 0) & (dt <= 5), np.log(bn / np.roll(bn, 1)) / np.sqrt(np.maximum(dt, 1.0)), np.nan)
    ret[0] = np.nan
    df["sig_30m"] = np.maximum(pd.Series(ret).rolling(1800, min_periods=120).std().bfill().fillna(0.00005).to_numpy(), 0.000035)

    uniq_t = np.sort(df["window_ts"].unique())
    t_split = int(uniq_t[(2 * len(uniq_t)) // 3])
    packed = []
    for w_ts, g in df.groupby("window_ts"):
        g_r = g.reset_index(drop=True)
        ts_int = np.round(g_r["ts"].to_numpy()).astype(np.int64)
        idx_map = {int(t): i for i, t in enumerate(ts_int)}
        packed.append((int(w_ts), "IS" if w_ts < t_split else "OOS", float(g_r["gm_ptb"].iloc[0]), str(g_r["gm_out"].iloc[0]), ts_int, idx_map, g_r))
    return packed, t_split


def _compute_tick_fv(cfg: SniperConfig, row: tuple[Any, ...], s0: float, gap_bps: float) -> Optional[FairValueEstimate]:
    _, el, cl_px, _, _, v1_raw, v30, A_val, cvd, obi = row
    if not np.isfinite(cl_px) or cl_px <= 0:
        return None
    cvd_f = 0.0 if not np.isfinite(cvd) else float(cvd)
    obi_f = 0.0 if not np.isfinite(obi) else float(obi)
    flow = max(-1.0, min(1.0, 0.70 * cvd_f + 0.30 * obi_f))
    rem = max(0.5, 300.0 - float(el))
    if cfg.use_twap_projection:
        a_sec = max(0.0, float(el) - 240.0) if el >= 240.0 else 0.0
        return estimate_twap_fair_value(s0, float(cl_px), rem, float(v30), (float(A_val), a_sec), (flow, gap_bps, cvd_f), cfg)
    v1 = max(cfg.min_1s_vol_floor, float(v1_raw) if np.isfinite(v1_raw) else 0.000055)
    return estimate_fair_value(s0, float(cl_px), rem, v1, flow, gap_bps, cfg.student_t_df, cvd_f)


def _check_exit_delay(
    cfg: SniperConfig, pos: OpenPosition, row_ctx: tuple[int, float, FairValueEstimate, OrderBookTop],
    delay_ctx: tuple[int, np.ndarray, Dict[int, int], np.ndarray, np.ndarray], meta: tuple[int, str, str, float],
) -> Optional[Dict[str, Any]]:
    i, elapsed, fv, bk = row_ctx
    delay_sec, ts_int, idx_map, ub, db = delay_ctx
    w_ts, sp, name, fee_rate = meta
    ex = evaluate_exit(cfg, pos, elapsed, fv, bk)
    j_ex = idx_map.get(int(ts_int[i]) + delay_sec) if ex is not None else None
    if ex is None or j_ex is None:
        return None
    b1 = float(ub[j_ex] if pos.side == "UP" else db[j_ex])
    if not np.isfinite(b1) or b1 < 0.12 or (ex.reason != "EMERGENCY_STOP_LOSS" and b1 < cfg.take_profit_bid):
        return None
    proc = effective_exit_proceeds(b1, fee_rate)
    return {
        "model": name, "delay": delay_sec, "fee": fee_rate, "split": sp, "window_ts": w_ts,
        "side": pos.side, "ask": pos.entry_price, "cost": pos.total_cost_per_share,
        "pnl": proc - pos.total_cost_per_share, "win": 1.0 if proc > pos.total_cost_per_share else 0.0, "exit": ex.reason,
    }


def _replay_one_window(
    win: tuple[int, str, float, str, np.ndarray, Dict[int, int], pd.DataFrame],
    name: str, cfg: SniperConfig, delay_sec: int, fee_rate: float, ewma_basis: Optional[float],
) -> Tuple[List[Dict[str, Any]], Optional[float]]:
    w_ts, sp, s0, out, ts_int, idx_map, g = win
    ub, ua = g["up_bid"].to_numpy(), g["up_ask"].to_numpy()
    db, da = g["down_bid"].to_numpy(), g["down_ask"].to_numpy()
    cols = ["ts", "elapsed_sec", "chainlink_price", "binance_price", "coinbase_price", "vol_1s", "sig_30m", "A_240", "cvd_imb", "obi"]
    pos: Optional[OpenPosition] = None
    entry_j = -1
    out_trades: List[Dict[str, Any]] = []

    for i, row in enumerate(g[cols].itertuples(index=False, name=None)):
        cl_px, bn_px, cb_px = row[2], row[3], row[4]
        gap_bps = 0.0
        if np.isfinite(cl_px) and cl_px > 0:
            cex = (0.55 * cb_px + 0.45 * bn_px) if (np.isfinite(cb_px) and np.isfinite(bn_px)) else (bn_px if np.isfinite(bn_px) else cl_px)
            raw_bps = ((cex - cl_px) / cl_px) * 10000.0
            ewma_basis = raw_bps if ewma_basis is None else (0.015 * raw_bps + 0.985 * ewma_basis)
            gap_bps = raw_bps - ewma_basis
        fv = _compute_tick_fv(cfg, row, s0, gap_bps)
        if fv is None:
            continue
        bk = OrderBookTop(ub[i] if np.isfinite(ub[i]) else None, ua[i] if np.isfinite(ua[i]) else None, db[i] if np.isfinite(db[i]) else None, da[i] if np.isfinite(da[i]) else None)
        if pos is None:
            cand = evaluate_entry("BTCUSDT", cfg, float(row[1]), fv, bk)
            j = idx_map.get(int(ts_int[i]) + delay_sec) if cand is not None else None
            if cand is None:
                continue
            if j is None:
                break
            ask1 = float(ua[j] if cand.side == "UP" else da[j])
            if not np.isfinite(ask1) or ask1 > entry_limit_price(cfg, cand.ask, cand.p_true):
                break
            pos = OpenPosition("BTCUSDT", cand.side, w_ts, 5.0, ask1, ask1 + taker_fee_per_share(ask1, fee_rate), cand.p_true, cand.z)
            entry_j = j
            if not cfg.early_tp_enabled and not cfg.enable_emergency_exit:
                break
        elif i > entry_j:
            done = _check_exit_delay(cfg, pos, (i, float(row[1]), fv, bk), (delay_sec, ts_int, idx_map, ub, db), (w_ts, sp, name, fee_rate))
            if done is not None:
                out_trades.append(done)
                pos = None
                break
    if pos is not None:
        win_f = 1.0 if out == pos.side else 0.0
        out_trades.append({"model": name, "delay": delay_sec, "fee": fee_rate, "split": sp, "window_ts": w_ts, "side": pos.side, "ask": pos.entry_price, "cost": pos.total_cost_per_share, "pnl": win_f - pos.total_cost_per_share, "win": win_f, "exit": "SETTLE"})
    return out_trades, ewma_basis


def replay_1s_clob(
    windows: List[tuple[int, str, float, str, np.ndarray, Dict[int, int], pd.DataFrame]],
    name: str, base_cfg: SniperConfig, delay_sec: int, fee_rate: float,
) -> pd.DataFrame:
    """Replay exact production `evaluate_entry` / `evaluate_exit` logic on 1s CLOB orderbooks with `+delay_sec`."""
    cfg = SniperConfig(**{**asdict(base_cfg), "fee_rate": fee_rate})
    trades: List[Dict[str, Any]] = []
    ewma_basis: Optional[float] = None
    for win in windows:
        w_trades, ewma_basis = _replay_one_window(win, name, cfg, delay_sec, fee_rate, ewma_basis)
        trades.extend(w_trades)
    return pd.DataFrame(trades)


def _split_summary_rows(tdf: pd.DataFrame, name: str, delay: int, fee: float) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    span_days = {"IS": 1013 * 2 / 3 / 288.0, "OOS": 1013 / 3 / 288.0, "FULL": 1013 / 288.0}
    for sp in ("IS", "OOS", "FULL"):
        s = tdf if sp == "FULL" else tdf[tdf["split"] == sp]
        n = len(s)
        if n == 0:
            continue
        wins = int(s["win"].sum())
        wlo, whi = wilson_ci(wins, n)
        plo, phi = bootstrap_ci(s["pnl"].to_numpy(dtype=np.float64))
        rows.append({
            "model": name, "delay_s": delay, "fee": fee, "split": sp, "n": n,
            "per_day_btc": round(n / span_days[sp], 1),
            "wr": round(wins / n, 4), "wr_ci95": f"[{wlo:.3f},{whi:.3f}]",
            "mean_cost": round(float(s["cost"].mean()), 4),
            "pnl_per_sh": round(float(s["pnl"].mean()), 5),
            "pnl_ci95": f"[{plo:+.4f},{phi:+.4f}]",
        })
    return rows


def summarize_clob_1s(windows: List[Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Run V1, V2_FILTERED, V2_PURE, and V2_HIGHBAND_SHADOW across +1s/+2s delay & {0.07, 0.05} fees."""
    configs: List[Tuple[str, SniperConfig]] = [
        ("V1_SPOT_DEFAULT", DEFAULT_SNIPER_CONFIG),
        ("TWAP_V2_FILTERED_SETTLE", TWAP_V2_FILTERED_CONFIG),
        ("TWAP_V2_PURE_SETTLE", TWAP_V2_PURE_CONFIG),
        ("V2_HIGHBAND_SHADOW", TWAP_V2_HIGHBAND_SHADOW_CONFIG),
    ]
    all_trades: List[pd.DataFrame] = []
    summary_rows: List[Dict[str, Any]] = []
    for name, cfg in configs:
        for delay in (1, 2):
            for fee in (0.07, 0.05):
                tdf = replay_1s_clob(windows, name, cfg, delay, fee)
                if name == "TWAP_V2_FILTERED_SETTLE" and delay == 1 and fee == 0.05:
                    all_trades.append(tdf.copy())
                summary_rows.extend(_split_summary_rows(tdf, name, delay, fee))
    out_csv = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()
    return pd.DataFrame(summary_rows), out_csv


def build_monthly_expectation_table() -> pd.DataFrame:
    """Transparent monthly expectancy bounds at 5 shares vs 5% sizing (Section 4.2 gate pending)."""
    rows: List[Dict[str, Any]] = []
    scenarios = [
        ("v2_filtered (+1s ref: ~13.4/d BTC, ~30-40/d 3-coin)", 30.0, 0.000, "保守無優勢 (CI跨0下緣)"),
        ("v2_filtered (+1s ref)", 30.0, 0.020, "保守正優勢 (+2¢/股)"),
        ("v2_filtered (+1s ref)", 30.0, 0.030, "96d因果均值 (+3¢/股)"),
        ("v2_filtered (+1s ref)", 30.0, 0.037, "Claude 1s盤口均值 (+3.7¢/股)"),
        ("v2_pure (+1s ref: ~33/d BTC, ~75/d 3-coin)", 75.0, 0.020, "高頻保守 (+2¢/股)"),
        ("v2_pure (+1s ref)", 75.0, 0.030, "高頻均值 (+3¢/股)"),
    ]
    for label, trades_day_3c, edge_sh, note in scenarios:
        sh_5 = 5.0
        sh_100u = round(100.0 * 0.05 / 0.79, 2)
        rows.append({
            "scenario": label,
            "edge_per_sh": f"{edge_sh:+.3f}",
            "est_trades_3coin_day": int(trades_day_3c),
            "est_trades_30d": int(trades_day_3c * 30),
            "monthly_usd_5sh": round(trades_day_3c * 30 * sh_5 * edge_sh, 2),
            "monthly_usd_at_100u_5pct": round(trades_day_3c * 30 * sh_100u * edge_sh, 2),
            "note": note,
        })
    return pd.DataFrame(rows)


def main() -> None:
    """Execute unbiased 1s-CLOB backtest (In-Sample Ref) + expectation sensitivity report."""
    windows, t_split = load_1s_clob_windows()
    clob_summary, clob_trades = summarize_clob_1s(windows)
    clob_trades.to_csv(OUT_TRADES_CSV, index=False)
    exp_df = build_monthly_expectation_table()
    lines = [
        "# TWAP Sniper V2 策略實作與回測說明 (`strategy_v2_report.md`)",
        "",
        "> ⚠️ **Round-3 稽核規範聲明**：",
        "> 1. `live_btc_5m.db`（`1,013` BTC 窗 = `3.52` 天）為 **V2 設計樣本內參考（Reference / In-Sample Only）**。",
        "> 2. **原第 2 節 96 天組合複利模擬（`$43.10 -> $192.89U`）正式作廢（`L4 Retracted`）**。",
        "> 3. 實盤與期望值裁決以 Section 4.2（`>= 2026-11-21T00:00:00Z`，`n >= 300`，**Bonferroni 98.3% CI 下界 > 0**）為唯一依據。",
        "",
        f"- 核心策略模組：`engine/oracle_sniper.py` (`TWAP_V2_FILTERED_CONFIG`, `TWAP_V2_PURE_CONFIG`, `TWAP_V2_HIGHBAND_SHADOW_CONFIG`)",
        f"- 真實 1s-CLOB 回測基準：`1,013` BTC 窗（100% Gamma TWAP 回填；IS `T < {t_split}` `675` 窗 / OOS `338` 窗）",
        "",
        "## 1. 真實 1 秒 CLOB 盤口回測（含 `+1s` / `+2s` 延遲懲罰；V2 僅作設計參考）",
        "",
        df_to_md(clob_summary),
        "",
        "## 2. [RETRACTED / 已作廢] 原 96 天三幣複利模擬 (`L4` 暫不採用)",
        "",
        "> 因基於 L1–L3 修正前資料且未計入 1Hz 錯過率與問價深度，`$43.10 -> $192.89U` 曲線已全面作廢；上線判定改以 4.2 實盤盤口為準。",
        "",
        "## 3. 誠實敏感度推估（僅在 4.2 通過 `98.3% CI > 0` 後生效）",
        "",
        df_to_md(exp_df),
        "",
        "- **重要誠實提示**：在真實 1Hz 盤口 (`live_btc_5m.db`, `n=339`) 上，95% CI 為 `+0.037 ± 0.042`（下界仍跨過 `0`）。因此若 `4.2`（11/21 新費率後）實測淨優勢落在 `<= 0¢/股`（做市商把 ETH/SOL 降費優勢吃光），則策略**不會上線**；只有通過 `4.2` 且確認淨正優勢（例如 `+2¢ ~ +3.7¢/股`）後，上表月期望值才會成立。",
        "",
    ]
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    print(f"[V2 Backtest] Saved {OUT_MD} & {OUT_TRADES_CSV} ({len(clob_trades)} rows)")


if __name__ == "__main__":
    main()
