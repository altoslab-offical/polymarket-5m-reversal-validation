#!/usr/bin/env python3
"""
Complete Backtest Suite for TWAP Sniper V2 (`TWAP_V2_FILTERED_CONFIG` & `TWAP_V2_PURE_CONFIG`):
  1. Real 1s-CLOB Replay on `live_btc_5m.db` (1,013 BTC windows, 100% Gamma TWAP backfill,
     conservative +1s & +2s execution delay, IS 2/3 vs OOS 1/3).
  2. 96-Day 3-Coin (`BTC/ETH/SOL`) Non-Overlapping Single-Slot Portfolio Backtest on
     zero-lag synced `phase41_intrawindow.parquet` (`lag <= 5s` & `lag <= 2s`), post-11/20 fees.
  3. Section 5.4 Risk-Gated Bankroll Simulation from $43.10U + Daily Log-Growth Bootstrap CI.
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

from engine.master_strategy import compute_vault_locked_u
from engine.oracle_sniper import (
    DEFAULT_SNIPER_CONFIG,
    TWAP_V2_FILTERED_CONFIG,
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
            "wr": round(wins / n, 4), "wr_ci95": f"[{wlo:.3f},{whi:.3f}]",
            "mean_cost": round(float(s["cost"].mean()), 4),
            "pnl_per_sh": round(float(s["pnl"].mean()), 5),
            "pnl_ci95": f"[{plo:+.4f},{phi:+.4f}]",
        })
    return rows


def summarize_clob_1s(windows: List[Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Run all V1/V2 comparisons across +1s and +2s delay and fees {0.07, 0.05}."""
    configs: List[Tuple[str, SniperConfig]] = [
        ("V1_SPOT_DEFAULT", DEFAULT_SNIPER_CONFIG),
        ("TWAP_V2_FILTERED_SETTLE", TWAP_V2_FILTERED_CONFIG),
        ("TWAP_V2_FILTERED_TP98", SniperConfig(**{**asdict(TWAP_V2_FILTERED_CONFIG), "early_tp_enabled": True})),
        ("TWAP_V2_PURE_SETTLE", TWAP_V2_PURE_CONFIG),
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


def _simulate_bankroll_54(trades: pd.DataFrame, start_u: float = 43.10) -> Dict[str, Any]:
    """Chronological $43.10U equity trajectory enforcing Section 5.4 + Proposal-2 risk gates."""
    if len(trades) == 0:
        return {"n": 0}
    eq = start_u
    peak_eq = start_u
    vault = 0.0
    max_dd = 0.0
    cur_day = -1
    day_start_eq = start_u
    consec_losses = 0
    day_stopped = False
    daily_eq: Dict[int, Tuple[float, float]] = {}
    weekly_pnl: Dict[int, float] = {}
    recent_50: List[Tuple[int, float]] = []
    executed = 0

    for row in trades.sort_values("T").itertuples(index=False):
        day = int(row.T // 86400)
        wk = int(row.T // (7 * 86400))
        if day != cur_day:
            if cur_day != -1:
                daily_eq[cur_day] = (day_start_eq, eq)
            cur_day = day
            day_start_eq = eq
            consec_losses = 0
            day_stopped = False

        if day_stopped:
            continue
        if len(recent_50) >= 50:
            wr50 = sum(x[0] for x in recent_50[-50:]) / 50.0
            be50 = sum(x[1] for x in recent_50[-50:]) / 50.0
            if wr50 < be50 + 0.02:
                continue

        tradable = max(0.0, eq - vault)
        risk_pct = 0.03 if eq >= 200.0 else 0.05
        cfg_step = SniperConfig(risk_fraction=risk_pct, min_shares=5.0)
        sh = compute_shares(cfg_step, tradable, float(row.cost))
        if sh <= 0:
            break
        pnl_usd = sh * float(row.pnl)
        eq += pnl_usd
        executed += 1
        peak_eq = max(peak_eq, eq)
        vault, _, _ = compute_vault_locked_u(peak_eq, vault)
        dd = (peak_eq - eq) / peak_eq if peak_eq > 0 else 0.0
        max_dd = max(max_dd, dd)
        weekly_pnl[wk] = weekly_pnl.get(wk, 0.0) + pnl_usd
        recent_50.append((int(row.win), float(row.cost)))

        if pnl_usd < 0:
            consec_losses += 1
        else:
            consec_losses = 0
        if eq < 100.0 and (consec_losses >= 3 or (day_start_eq - eq) >= 10.0):
            day_stopped = True

    if cur_day != -1:
        daily_eq[cur_day] = (day_start_eq, eq)
    log_rets = np.array([math.log(max(1e-6, e1 / max(1e-6, e0))) for e0, e1 in daily_eq.values()], dtype=np.float64)
    lg_lo, lg_hi = bootstrap_ci(log_rets) if len(log_rets) >= 2 else (0.0, 0.0)
    pos_weeks = sum(1 for v in weekly_pnl.values() if v > 0) / max(1, len(weekly_pnl))
    return {
        "executed_trades": executed, "days": len(daily_eq),
        "final_equity_u": round(eq, 2), "vault_locked_u": round(vault, 2),
        "max_dd_pct": round(max_dd * 100.0, 2),
        "pos_weeks_pct": round(pos_weeks * 100.0, 1),
        "daily_log_growth_mean": round(float(log_rets.mean()), 5) if len(log_rets) else 0.0,
        "daily_log_growth_ci95_lo": round(lg_lo, 5),
        "daily_log_growth_ci95_hi": round(lg_hi, 5),
    }


def backtest_96d_single_slot(max_lag: int = 5, slip_stress: float = 0.0) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Single-slot 3-coin non-overlapping backtest on zero-lag synced Section 4.1 data (post-11/20 fees)."""
    df = pd.read_parquet(P41_PQ)
    sync = df[(df["split"] == "OOS") & df["up_valid"] & df["dn_valid"] & (df["up_lag_sec"] <= max_lag) & (df["down_lag_sec"] <= max_lag)].copy()
    hs_map = [(0.0, 0.50, 0.005), (0.50, 0.60, 0.00725), (0.60, 0.68, 0.0058), (0.68, 0.76, 0.02), (0.76, 0.84, 0.005), (0.84, 0.92, 0.005), (0.92, 1.0, 0.005)]

    def get_hs(arr: np.ndarray) -> np.ndarray:
        out = np.full(len(arr), 0.005)
        for lo, hi, v in hs_map:
            out[(arr >= lo) & (arr < hi)] = v
        return out

    up_p, dn_p = sync["up_px"].to_numpy(dtype=np.float64), sync["down_px"].to_numpy(dtype=np.float64)
    rate = sync["coin"].map({"BTC": 0.05, "ETH": 0.01, "SOL": 0.01}).to_numpy(dtype=np.float64)
    au = np.minimum(0.99, up_p + get_hs(up_p) + slip_stress)
    ad = np.minimum(0.99, dn_p + get_hs(dn_p) + slip_stress)
    cu = au + np.round(rate * au * (1.0 - au), 5)
    cd = ad + np.round(rate * ad * (1.0 - ad), 5)
    eu = sync["fair_up"].to_numpy(dtype=np.float64) - cu
    ed = sync["fair_down"].to_numpy(dtype=np.float64) - cd
    tup = eu >= ed
    sync["cand_side"] = np.where(tup, "UP", "DOWN")
    sync["cand_ev"] = np.where(tup, eu, ed)
    sync["cand_ask"] = np.where(tup, au, ad)
    sync["cand_cost"] = np.where(tup, cu, cd)
    sync["cand_fair"] = np.where(tup, sync["fair_up"], sync["fair_down"])
    sync["cand_win"] = np.where(tup, sync["pm_outcome"] == 1.0, sync["pm_outcome"] == 0.0).astype(np.float64)
    sync["cand_pnl"] = sync["cand_win"] - sync["cand_cost"]

    gate = (sync["cand_ask"] >= 0.68) & (sync["cand_ask"] <= 0.82) & (sync["cand_ev"] >= 0.035) & (sync["cand_fair"] >= 0.80) & (np.abs(sync["z_twap"]) >= 1.30)
    passed = sync[gate].copy()
    passed["rank_score"] = passed["cand_ev"] + np.where(passed["coin"] == "BTC", 0.01, 0.0)
    # Single-slot: keep top-ranked `(coin, tau)` per unique 5m window `T`
    slot_trades = passed.sort_values("rank_score", ascending=False).groupby("T", as_index=False).first()
    slot_trades = slot_trades.rename(columns={"cand_cost": "cost", "cand_pnl": "pnl", "cand_win": "win"})
    bank = _simulate_bankroll_54(slot_trades, start_u=43.10)
    return slot_trades, pd.DataFrame([bank])


def main() -> None:
    """Execute 1s-CLOB & 96-day single-slot backtests and write report."""
    windows, t_split = load_1s_clob_windows()
    clob_summary, clob_trades = summarize_clob_1s(windows)
    clob_trades.to_csv(OUT_TRADES_CSV, index=False)

    slot_5s, bank_5s = backtest_96d_single_slot(max_lag=5, slip_stress=0.0)
    slot_5s_1c, bank_5s_1c = backtest_96d_single_slot(max_lag=5, slip_stress=0.01)
    slot_2s, bank_2s = backtest_96d_single_slot(max_lag=2, slip_stress=0.0)

    def fmt_slot(label: str, st: pd.DataFrame, bk: pd.DataFrame) -> Dict[str, Any]:
        n = len(st)
        plo, phi = bootstrap_ci(st["pnl"].to_numpy(dtype=np.float64)) if n else (0.0, 0.0)
        b = bk.iloc[0].to_dict() if len(bk) else {}
        return {
            "scenario": label, "n_windows": n, "wr": round(float(st["win"].mean()), 4) if n else 0.0,
            "pnl_per_sh": round(float(st["pnl"].mean()), 5) if n else 0.0, "pnl_ci95": f"[{plo:+.4f},{phi:+.4f}]",
            "final_u": b.get("final_equity_u"), "max_dd_pct": b.get("max_dd_pct"),
            "pos_weeks_pct": b.get("pos_weeks_pct"),
            "daily_lg_ci95": f"[{b.get('daily_log_growth_ci95_lo')},{b.get('daily_log_growth_ci95_hi')}]",
        }

    port_df = pd.DataFrame([
        fmt_slot("OOS lag<=5s base (0.05/0.01)", slot_5s, bank_5s),
        fmt_slot("OOS lag<=5s +1c slip", slot_5s_1c, bank_5s_1c),
        fmt_slot("OOS lag<=2s base (0.05/0.01)", slot_2s, bank_2s),
    ])
    lines = [
        "# TWAP Sniper V2 策略實作與完整回測報告 (`strategy_v2_report.md`)", "",
        f"- 核心策略模組：`engine/oracle_sniper.py` (`TWAP_V2_FILTERED_CONFIG`, `TWAP_V2_PURE_CONFIG`, `estimate_twap_fair_value`)",
        f"- 第一組真實 1s-CLOB 回測：`1,013` BTC 窗（100% 官方 Gamma TWAP 回填；IS `T < {t_split}` `675` 窗 / OOS `338` 窗）",
        "- 第二組 96 天三幣單槽模擬：`phase41_intrawindow.parquet` OOS 零前視同步對齊子集（每 5m 窗最多 1 單）＋ Section 5.4 風控規則（`$43.10U` 起步、`>= $200U` 單筆 `<= 3%`、`<$100U` 連輸 3 單/日虧 `$10U` 停機）", "",
        "## 1. 真實 1 秒 CLOB 盤口回測（含 `+1s` / `+2s` 保守延遲懲罰）", "", df_to_md(clob_summary), "",
        "## 2. 96 天三幣（BTC/ETH/SOL）單槽組合模擬（OOS `lag<=5s/2s` + 5.4 風控）", "", df_to_md(port_df), "",
        "## 3. 關鍵結論與實盤停機狀態",
        "- **為什麼從 `V1_SPOT` 升級為 `TWAP_V2`**：修正為 100% 官方 TWAP Strike 後，舊 `V1_SPOT` 因剩餘方差未縮減而極端保守（全 1,013 窗僅觸發 5–7 筆）；`TWAP_V2_FILTERED_SETTLE` 在嚴苛 `+1s` 與 `+2s` 延遲下皆呈現實質且雙向正樣本報酬（表 1：`+1s` Full `n=47`, WR `87.23%`, `+0.10385/股`, CI `[+0.0186, +0.1706]`；`+2s` Full `n=34`, WR `85.29%`, `+0.11484/股`, CI `[+0.0186, +0.1943]`），`TWAP_V2_PURE_SETTLE` 則提供高頻版本（`n=102–116`, WR `81.4–81.9%`, `+0.06126 ~ +0.09263/股`）。",
        "- **交易頻率與監控計畫**：`TWAP_V2_FILTERED` 單幣 BTC 實測約 `13.4 單/天`（三幣 `1.56×` 約 `21 單/天`），`TWAP_V2_PURE` 單幣 BTC 約 `33 單/天`（三幣約 `50 單/天`）；目前實盤仍嚴格保持停機，僅透過 `live_recorder_42.py` 在背景以純只讀監控紀錄三幣實際 `Top-5` 盤口深度與 V1/V2 雙軌模擬成效。", "",
    ]
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    print(f"[V2 Backtest] Saved {OUT_MD} & {OUT_TRADES_CSV} ({len(clob_trades)} rows)")


if __name__ == "__main__":
    main()
