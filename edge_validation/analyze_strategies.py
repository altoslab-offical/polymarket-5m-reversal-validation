#!/usr/bin/env python3
"""
Empirical strategy comparison & Monte Carlo compounding analysis (read-only):
Part A: Real 1-second CLOB snapshots on live_btc_5m.db (1,007 BTC 5m windows, 84.4h)
  - Passive Maker simulations (Claude's A.5 concepts) vs Intrawindow Chainlink Sniper
  - Fee sensitivity: 0.07 (current) vs 0.05 (Nov 20 BTC) vs 0.01 (Nov 20 ETH/SOL)
  - Chronological 60% IS / 40% OOS validation + 10,000-path Monte Carlo from $42.53U
Part B: 96-day (82,944 settled windows) BTC/ETH/SOL cross-coin late-window lock-in audit
  using raw_1m_{SYM}.parquet + real Polymarket pm_outcome from windows.csv.
"""
from __future__ import annotations

import pathlib
import sqlite3
import sys
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent
PROJECT_ROOT = ROOT.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(ROOT))

from engine.oracle_sniper import (
    DEFAULT_SNIPER_CONFIG,
    FairValueEstimate,
    OpenPosition,
    OrderBookTop,
    RollingMicrostructureTracker,
    SniperConfig,
    effective_exit_proceeds,
    evaluate_entry,
    evaluate_exit,
    taker_fee_per_share,
)
from verify_oracle_sniper import _book_from_row, _effective_strike, _tick_fv
from build_windows_and_summary import wilson_ci, bootstrap_mean_ci

LIVE_DB = PROJECT_ROOT / "data" / "oracle_sniper" / "live_btc_5m.db"
REPORT_MD = ROOT / "strategy_comparison_report.md"


def load_live_windows(db_path: pathlib.Path) -> List[Tuple[int, float, str, List[tuple]]]:
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    windows = cur.execute(
        "SELECT window_ts, strike_price, strike_source, close_price, resolved_outcome FROM windows "
        "WHERE resolved_outcome IS NOT NULL ORDER BY window_ts ASC"
    ).fetchall()
    out: List[Tuple[int, float, str, List[tuple]]] = []
    for w_ts, db_strike, strike_src, _close, outcome in windows:
        rows = cur.execute(
            "SELECT ts, elapsed_sec, chainlink_price, cex_lead_price, cvd_imb, obi, "
            "up_bid, up_ask, down_bid, down_ask "
            "FROM snapshots WHERE window_ts=? ORDER BY ts ASC",
            (w_ts,),
        ).fetchall()
        strike = _effective_strike(rows, db_strike, strike_src)
        if strike is None or strike <= 0:
            continue
        out.append((int(w_ts), float(strike), str(outcome), rows))
    conn.close()
    return out


def precompute_fv_stream(
    windows: List[Tuple[int, float, str, List[tuple]]],
    cfg: SniperConfig,
) -> List[Tuple[int, float, str, List[Tuple[tuple, Optional[FairValueEstimate], OrderBookTop]]]]:
    """Run RollingMicrostructureTracker once chronologically (strictly causal, zero look-ahead)."""
    tracker = RollingMicrostructureTracker(vol_lookback_sec=cfg.vol_lookback_sec)
    stream = []
    for w_ts, strike, outcome, rows in windows:
        ticks = []
        for row in rows:
            if row[2] is None:
                continue
            fv = _tick_fv(tracker, cfg, strike, row)
            book = _book_from_row(row)
            ticks.append((row, fv, book))
        stream.append((w_ts, strike, outcome, ticks))
    return stream


def sim_sniper_on_stream(
    stream: List[Tuple[int, float, str, List[Tuple[tuple, Optional[FairValueEstimate], OrderBookTop]]]],
    cfg: SniperConfig,
    early_tp_sell_at_end: bool = False,
) -> pd.DataFrame:
    """Exact chronological replay matching verify_oracle_sniper, tracking exact entry/exit seconds."""
    trades: List[Dict[str, Any]] = []
    for w_ts, _strike, outcome, ticks in stream:
        pos: Optional[OpenPosition] = None
        tr: Optional[Dict[str, Any]] = None
        for row, fv, book in ticks:
            if fv is None:
                continue
            el = float(row[1])
            if pos is not None and tr is not None and tr["exit_reason"] is None:
                dec = evaluate_exit(cfg, pos, el, fv, book)
                if dec is not None:
                    tr["exit_reason"] = dec.reason
                    tr["exit_el"] = el
                    tr["payout"] = dec.net_proceeds_per_share
                continue
            if tr is None:
                cand = evaluate_entry("BTCUSDT", cfg, el, fv, book)
                if cand is not None:
                    pos = OpenPosition(
                        "BTCUSDT", cand.side, w_ts, 5.0, cand.ask,
                        cand.total_cost_per_share, cand.p_true, cand.z,
                    )
                    tr = {
                        "window_ts": w_ts,
                        "side": cand.side,
                        "entry_px": cand.ask,
                        "cost": cand.total_cost_per_share,
                        "entry_el": el,
                        "exit_el": None,
                        "exit_reason": None,
                        "payout": None,
                        "p_true": cand.p_true,
                        "z": cand.z,
                    }
        if tr is not None:
            if tr["exit_reason"] is None:
                won = (outcome == tr["side"])
                tr["exit_reason"] = f"SETTLED_{outcome}"
                tr["payout"] = 1.0 if won else 0.0
                # Assume 17s onchain resolution lag after T+300 when held to settlement (observed median 17s)
                tr["lock_sec"] = (300.0 - tr["entry_el"]) + (10.0 if (early_tp_sell_at_end and won) else 17.0)
            else:
                tr["lock_sec"] = tr["exit_el"] - tr["entry_el"]
            tr["won"] = 1 if tr["payout"] > tr["cost"] else 0
            tr["pnl"] = tr["payout"] - tr["cost"]
            trades.append(tr)
    return pd.DataFrame(trades)


def sim_passive_maker_on_stream(
    stream: List[Tuple[int, float, str, List[Tuple[tuple, Optional[FairValueEstimate], OrderBookTop]]]],
    mode: str,
    cfg: SniperConfig,
) -> pd.DataFrame:
    """
    Simulate passive Maker strategies on real 1s CLOB bid/ask snapshots (0% maker fee, 0% rebate after Nov 20).
    Fill rule (conservative): limit bid at L fills ONLY if a subsequent tick has best_ask <= L.
    - 't0_contrarian_48': Fade previous window's outcome at T=3s with resting bid @ min(0.48, prev_bid) up to el<=150s
    - 't0_dip_45': Place resting bid @ 0.45 at T=3s on direction of current spot lead (or contrarian) up to el<=240s
    - 'sniper_maker_pass1c': When Sniper signal fires, post post_only bid at (ask - 0.01) instead of crossing spread;
       fills only if future tick up_ask/down_ask <= limit_px while signal window remains active (el <= 285s).
    """
    records: List[Dict[str, Any]] = []
    prev_outcome: Optional[str] = None
    for w_ts, _strike, outcome, ticks in stream:
        filled_rec: Optional[Dict[str, Any]] = None
        if mode in ("t0_contrarian_48", "t0_dip_45"):
            if prev_outcome in ("UP", "DOWN"):
                side = "DOWN" if prev_outcome == "UP" else "UP"
                target_px = 0.48 if mode == "t0_contrarian_48" else 0.45
                max_wait = 150.0 if mode == "t0_contrarian_48" else 240.0
                posted = False
                for row, fv, book in ticks:
                    el = float(row[1])
                    if el < 3.0 or el > max_wait:
                        continue
                    ask = book.up_ask if side == "UP" else book.down_ask
                    bid = book.up_bid if side == "UP" else book.down_bid
                    if not posted:
                        if bid is not None and ask is not None and target_px < ask:
                            posted = True
                        elif ask is not None and ask <= target_px:
                            # Already below target, would be cross/wait
                            continue
                    elif ask is not None and ask <= target_px:
                        won = 1 if (outcome == side) else 0
                        filled_rec = {
                            "window_ts": w_ts, "side": side, "entry_px": target_px,
                            "cost": target_px, "entry_el": el, "lock_sec": (300.0 - el) + 17.0,
                            "won": won, "pnl": (1.0 if won else 0.0) - target_px,
                        }
                        break
        elif mode == "sniper_maker_pass1c":
            limit_side: Optional[str] = None
            limit_px: float = 0.0
            pos: Optional[OpenPosition] = None
            for row, fv, book in ticks:
                if fv is None:
                    continue
                el = float(row[1])
                if pos is not None and filled_rec is not None and filled_rec.get("exit_reason") is None:
                    dec = evaluate_exit(cfg, pos, el, fv, book)
                    if dec is not None:
                        filled_rec["exit_reason"] = dec.reason
                        filled_rec["payout"] = dec.net_proceeds_per_share
                        filled_rec["lock_sec"] = el - filled_rec["entry_el"]
                    continue
                if limit_side is not None and pos is None and el <= 285.0:
                    ask = book.up_ask if limit_side == "UP" else book.down_ask
                    if ask is not None and ask <= limit_px:
                        # Passive maker filled at 0 fee!
                        pos = OpenPosition("BTCUSDT", limit_side, w_ts, 5.0, limit_px, limit_px, fv.p_true_up if limit_side == "UP" else fv.p_true_down, abs(fv.raw_z_score))
                        filled_rec = {
                            "window_ts": w_ts, "side": limit_side, "entry_px": limit_px,
                            "cost": limit_px, "entry_el": el, "exit_reason": None, "payout": None,
                        }
                elif limit_side is None:
                    cand = evaluate_entry("BTCUSDT", cfg, el, fv, book)
                    if cand is not None:
                        limit_side = cand.side
                        limit_px = round(cand.ask - 0.01, 2)
            if filled_rec is not None:
                if filled_rec["exit_reason"] is None:
                    won = (outcome == filled_rec["side"])
                    filled_rec["payout"] = 1.0 if won else 0.0
                    filled_rec["lock_sec"] = (300.0 - filled_rec["entry_el"]) + 17.0
                filled_rec["won"] = 1 if filled_rec["payout"] > filled_rec["cost"] else 0
                filled_rec["pnl"] = filled_rec["payout"] - filled_rec["cost"]

        prev_outcome = outcome
        if filled_rec is not None:
            records.append(filled_rec)
    return pd.DataFrame(records)


def summarize_trades(df: pd.DataFrame, hours_span: float, label: str) -> Dict[str, Any]:
    days = hours_span / 24.0
    n = int(len(df))
    if n == 0:
        return {"label": label, "n": 0, "tpd": 0.0, "wr": float("nan"), "wr_lo": float("nan"), "wr_hi": float("nan"),
                "avg_cost": float("nan"), "pnl": float("nan"), "pnl_lo": float("nan"), "pnl_hi": float("nan"), "lock_med": float("nan")}
    wins = int(df["won"].sum())
    wr, wr_lo, wr_hi = wilson_ci(wins, n)
    pnl_m, pnl_lo, pnl_hi = bootstrap_mean_ci(df["pnl"].to_numpy(dtype=np.float64))
    return {
        "label": label,
        "n": n,
        "tpd": n / max(days, 1e-6),
        "wr": wr, "wr_lo": wr_lo, "wr_hi": wr_hi,
        "avg_cost": float(df["cost"].mean()),
        "pnl": pnl_m, "pnl_lo": pnl_lo, "pnl_hi": pnl_hi,
        "lock_med": float(df["lock_sec"].median()),
    }


def monte_carlo_growth(
    df_trades: pd.DataFrame,
    trades_per_day: float,
    start_u: float = 42.53,
    horizon_days: int = 90,
    n_paths: int = 10_000,
    seed: int = 42,
) -> Dict[str, Any]:
    """
    Simulate Section 5.4 capital ladder from $42.53U using real empirical per-trade (cost, payout) distribution:
      - equity < $100: fixed 5 shares
      - $100 <= equity < $200: floor(0.03 * equity / cost) rounded to multiple of 5 (min 5)
      - equity >= $200: 0.05 * equity / cost (conservative ~0.35x-0.5x Kelly for binary sniper), capped at 80 shares (capacity guardrail)
      - halt if equity < $15 or cannot afford 5 shares
    """
    costs = df_trades["cost"].to_numpy(dtype=np.float64)
    pnls = df_trades["pnl"].to_numpy(dtype=np.float64)
    n_tr = len(costs)
    rng = np.random.default_rng(seed)

    tpd_int = max(1, int(round(trades_per_day)))
    total_steps = horizon_days * tpd_int

    end_equities = np.empty(n_paths, dtype=np.float64)
    max_dds = np.empty(n_paths, dtype=np.float64)
    days_100: List[float] = []
    days_200: List[float] = []
    weekly_pos_rates: List[float] = []
    ruined = 0

    for p_idx in range(n_paths):
        eq = start_u
        peak = start_u
        max_dd = 0.0
        d100 = None
        d200 = None
        week_eq_start = eq
        pos_weeks = 0
        tot_weeks = 0

        picks = rng.integers(0, n_tr, size=total_steps)
        for step in range(total_steps):
            idx = int(picks[step])
            c = costs[idx]
            pnl_sh = pnls[idx]
            if eq < 5.0 * c + 0.05:
                ruined += 1
                break
            if eq < 100.0:
                sh = 5.0
            elif eq < 200.0:
                raw_sh = (0.03 * eq) / c
                sh = max(5.0, float(int(raw_sh // 5) * 5))
            else:
                sh = min(80.0, max(5.0, round((0.05 * eq) / c, 2)))
            eq += sh * pnl_sh
            if eq > peak:
                peak = eq
            dd = (peak - eq) / peak if peak > 0 else 0.0
            if dd > max_dd:
                max_dd = dd
            day_cur = (step + 1) / tpd_int
            if d100 is None and eq >= 100.0:
                d100 = day_cur
            if d200 is None and eq >= 200.0:
                d200 = day_cur
            if (step + 1) % (7 * tpd_int) == 0:
                tot_weeks += 1
                if eq > week_eq_start:
                    pos_weeks += 1
                week_eq_start = eq

        end_equities[p_idx] = eq
        max_dds[p_idx] = max_dd
        if d100 is not None:
            days_100.append(d100)
        if d200 is not None:
            days_200.append(d200)
        if tot_weeks > 0:
            weekly_pos_rates.append(pos_weeks / tot_weeks)

    daily_log_g = np.log(np.maximum(end_equities, 1e-6) / start_u) / horizon_days
    return {
        "tpd": tpd_int,
        "p_reach_100_90d": len(days_100) / n_paths,
        "days_to_100_p25": float(np.percentile(days_100, 25)) if days_100 else float("nan"),
        "days_to_100_p50": float(np.percentile(days_100, 50)) if days_100 else float("nan"),
        "days_to_100_p75": float(np.percentile(days_100, 75)) if days_100 else float("nan"),
        "p_reach_200_90d": len(days_200) / n_paths,
        "days_to_200_p50": float(np.percentile(days_200, 50)) if days_200 else float("nan"),
        "end_30d_p50": float(np.percentile(end_equities, 50)) if horizon_days == 30 else float("nan"),
        "end_eq_p25": float(np.percentile(end_equities, 25)),
        "end_eq_p50": float(np.percentile(end_equities, 50)),
        "end_eq_p75": float(np.percentile(end_equities, 75)),
        "max_dd_p50": float(np.percentile(max_dds, 50)),
        "max_dd_p95": float(np.percentile(max_dds, 95)),
        "weekly_wr_mean": float(np.mean(weekly_pos_rates)) if weekly_pos_rates else float("nan"),
        "daily_log_g_p50": float(np.percentile(daily_log_g, 50)),
        "ruin_rate": ruined / n_paths,
    }


def analyze_cross_coin_96days() -> pd.DataFrame:
    """
    Use 96 days of 1m bars + real Polymarket pm_outcome (windows.csv, 82,944 settled markets)
    to verify whether ETH and SOL share BTC's late-window lock-in physics in both IS and OOS.
    """
    df_win = pd.read_csv(
        PROJECT_ROOT / "windows.csv",
        usecols=["coin", "T", "split", "pm_outcome"],
        keep_default_na=True,
        na_values=["NA"],
    )
    df_win = df_win[df_win["pm_outcome"].isin(["Up", "Down"])].copy()

    rows_all: List[pd.DataFrame] = []
    for coin, sym in (("BTC", "BTCUSDT"), ("ETH", "ETHUSDT"), ("SOL", "SOLUSDT")):
        df_1m = pd.read_parquet(ROOT / "cache" / f"raw_1m_{sym}.parquet").sort_values("ts").reset_index(drop=True)
        # 1m log returns and trailing 30-bar std of 1m returns (strictly causal)
        c = df_1m["close"].to_numpy(dtype=np.float64)
        lr = np.zeros_like(c)
        lr[1:] = np.log(np.maximum(c[1:], 1e-12) / np.maximum(c[:-1], 1e-12))
        sigma_1m = pd.Series(lr).rolling(30, min_periods=15).std(ddof=1).to_numpy(dtype=np.float64)
        # Map 1m ts -> (open, close, sigma_1m)
        ts_arr = df_1m["ts"].to_numpy(dtype=np.int64)
        open_map = dict(zip(ts_arr, df_1m["open"].to_numpy(dtype=np.float64)))
        close_map = dict(zip(ts_arr, c))
        sig_map = dict(zip(ts_arr, sigma_1m))

        sub = df_win[df_win["coin"] == coin].copy()
        Ts = sub["T"].to_numpy(dtype=np.int64)
        # tau=180s -> bar T+120 close (known at T+180s), 2 min left
        # tau=240s -> bar T+180 close (known at T+240s), 1 min left
        o0 = np.array([open_map.get(int(t), np.nan) for t in Ts], dtype=np.float64)
        c180 = np.array([close_map.get(int(t) + 120, np.nan) for t in Ts], dtype=np.float64)
        s180 = np.array([sig_map.get(int(t) + 120, np.nan) for t in Ts], dtype=np.float64)
        c240 = np.array([close_map.get(int(t) + 180, np.nan) for t in Ts], dtype=np.float64)
        s240 = np.array([sig_map.get(int(t) + 180, np.nan) for t in Ts], dtype=np.float64)

        sub["z180"] = np.log(c180 / o0) / (np.maximum(s180, 1e-5) * np.sqrt(2.0))
        sub["z240"] = np.log(c240 / o0) / (np.maximum(s240, 1e-5) * np.sqrt(1.0))
        rows_all.append(sub)

    return pd.concat(rows_all, ignore_index=True)


def main() -> None:
    windows = load_live_windows(LIVE_DB)
    n_win = len(windows)
    split_idx = int(0.60 * n_win)  # 60% IS (~602 windows, 50.6h), 40% OOS (~402 windows, 33.8h)
    is_win = windows[:split_idx]
    oos_win = windows[split_idx:]
    is_hours = (is_win[-1][0] - is_win[0][0] + 300) / 3600.0
    oos_hours = (oos_win[-1][0] - oos_win[0][0] + 300) / 3600.0
    full_hours = (windows[-1][0] - windows[0][0] + 300) / 3600.0

    print(f"Loaded {n_win} BTC windows ({full_hours:.2f}h): IS={len(is_win)} ({is_hours:.2f}h), OOS={len(oos_win)} ({oos_hours:.2f}h)", flush=True)

    stream_full = precompute_fv_stream(windows, DEFAULT_SNIPER_CONFIG)
    stream_is = stream_full[:split_idx]
    stream_oos = stream_full[split_idx:]

    lines: List[str] = []
    lines.append("# 策略對比實證與資金曲線分析報告 (`strategy_comparison_report.md`)\n")
    lines.append(f"- **Part A 資料集（`live_btc_5m.db` 真實逐秒 CLOB 盤口 + Chainlink）**：`{n_win}` 個完整結算 BTC 5m 視窗（`{full_hours:.2f}` 小時）；前 60% `IS` `{len(is_win)}` 窗（`{is_hours:.2f}h`），後 40% `OOS` `{len(oos_win)}` 窗（`{oos_hours:.2f}h`）。\n")

    # 1. Maker vs Sniper comparison across full & OOS
    lines.append("## 1. 被動 Maker vs 視窗內 Chainlink TWAP Sniper（真實逐秒盤口）\n")
    lines.append("| 策略 / 費率情境 | 區段 | 筆數 n | 日均筆數(BTC) | 勝率 (Wilson 95% CI) | 平均成本 | 平均每股 PnL (95% Boot CI) | 中位鎖倉(秒) |")
    lines.append("|---|---|---:|---:|---|---:|---|---:|")

    configs: List[Tuple[str, str, Any]] = [
        ("Maker-A1: T=0s 反向掛 48¢ (至 150s)", "maker", ("t0_contrarian_48", DEFAULT_SNIPER_CONFIG)),
        ("Maker-A2: T=0s 反向掛 45¢ (至 240s)", "maker", ("t0_dip_45", DEFAULT_SNIPER_CONFIG)),
        ("Maker-A3: Sniper 訊號後退 1¢ 掛 Maker", "maker", ("sniper_maker_pass1c", DEFAULT_SNIPER_CONFIG)),
        ("Sniper-現行 (0.07 fee, [0.68,0.82])", "sniper", DEFAULT_SNIPER_CONFIG),
        ("Sniper-11/20 BTC (0.05 fee)", "sniper", SniperConfig(fee_rate=0.05)),
        ("Sniper-11/20 ETH/SOL 口徑 (0.01 fee)", "sniper", SniperConfig(fee_rate=0.01)),
    ]

    def fmt_st_row(st: Dict[str, Any], seg: str) -> str:
        if st["n"] == 0:
            return f"| {st['label']} | {seg} | 0 | 0.0 | NA | NA | NA | NA |"
        return (
            f"| {st['label']} | {seg} | {st['n']} | {st['tpd']:.1f} | "
            f"{100*st['wr']:.2f}% [{100*st['wr_lo']:.1f}%, {100*st['wr_hi']:.1f}%] | "
            f"`${st['avg_cost']:.4f}` | **{st['pnl']:+.4f}** [{st['pnl_lo']:+.4f}, {st['pnl_hi']:+.4f}] | {st['lock_med']:.0f}s |"
        )

    sniper_df_nov20_btc = None
    sniper_df_nov20_es = None
    for lbl, kind, param in configs:
        if kind == "maker":
            mode, c_obj = param
            df_f = sim_passive_maker_on_stream(stream_full, mode, c_obj)
            df_o = sim_passive_maker_on_stream(stream_oos, mode, c_obj)
        else:
            df_f = sim_sniper_on_stream(stream_full, param)
            df_o = sim_sniper_on_stream(stream_oos, param)
            if abs(param.fee_rate - 0.05) < 1e-6:
                sniper_df_nov20_btc = df_f
            elif abs(param.fee_rate - 0.01) < 1e-6:
                sniper_df_nov20_es = df_f
        lines.append(fmt_st_row(summarize_trades(df_f, full_hours, lbl), "Full"))
        lines.append(fmt_st_row(summarize_trades(df_o, oos_hours, lbl), "OOS (40%)"))

    # 2. Check IS-only expansion grid (to test whether ETH/SOL 0.01 fee unlocks wider price/Z band without overfitting)
    lines.append("\n## 2. 頻率與 EV 擴展測試（嚴格僅在 IS 60% 挑選，OOS 40% 單次凍結驗證，套用 11/20 新費率 `0.05`/`0.01`）\n")
    grid_cands = [
        ("預設 [150,255] Z>=1.30 ask[0.68,0.82] EV>=3.5¢", DEFAULT_SNIPER_CONFIG),
        ("放寬價格 ask[0.62,0.85] EV>=3.0¢", SniperConfig(min_entry_price=0.62, max_entry_price=0.85, min_net_edge=0.030, fee_rate=0.05)),
        ("提早窗口 [120,260] ask[0.64,0.85] Z>=1.20 EV>=3.0¢", SniperConfig(entry_start_elapsed_sec=120.0, entry_end_elapsed_sec=260.0, min_z_score=1.20, min_entry_price=0.64, max_entry_price=0.85, min_net_edge=0.030, fee_rate=0.05)),
        ("ETH/SOL 0.01費率 + [120,260] Z>=1.20 ask[0.64,0.86] EV>=3.0¢", SniperConfig(entry_start_elapsed_sec=120.0, entry_end_elapsed_sec=260.0, min_z_score=1.20, min_entry_price=0.64, max_entry_price=0.86, min_net_edge=0.030, fee_rate=0.01)),
    ]
    lines.append("| 參數組 | 區段 | 筆數 n | 日均筆數(BTC) | 勝率 (Wilson 95% CI) | 平均成本 | 平均每股 PnL (95% Boot CI) | 中位鎖倉 |")
    lines.append("|---|---|---:|---:|---|---:|---|---:|")
    tuned_df_005 = None
    tuned_df_001 = None
    for idx_g, (lbl, cfg_g) in enumerate(grid_cands):
        if idx_g == 0:
            cfg_g = SniperConfig(fee_rate=0.05)
        df_i = sim_sniper_on_stream(stream_is, cfg_g)
        df_o = sim_sniper_on_stream(stream_oos, cfg_g)
        df_f = sim_sniper_on_stream(stream_full, cfg_g)
        if idx_g == 2:
            tuned_df_005 = df_f
        elif idx_g == 3:
            tuned_df_001 = df_f
        lines.append(fmt_st_row(summarize_trades(df_i, is_hours, lbl), "IS (60%)"))
        lines.append(fmt_st_row(summarize_trades(df_o, oos_hours, lbl), "OOS (40%)"))

    # 3. 96-day cross-coin validation (BTC/ETH/SOL)
    lines.append("\n## 3. 跨幣種 96 天（`2026-08-14 ~ 2026-11-18`，82,944 實盤結算）鎖定勝率與開單量驗證\n")
    df_cc = analyze_cross_coin_96days()
    lines.append("以 `raw_1m` 嚴格無未來函數計算進行中第 `τ = 180s` 與 `τ = 240s` 時之 `|Z| >= 1.30`（`raw_z >= 1.30` 押 Up、`raw_z <= -1.30` 押 Down），直接對照 **Polymarket 真實 `pm_outcome`（60s Chainlink TWAP 結算）**：\n")
    lines.append("| 幣種 / 檢驗點 / 區段 | 觸發數 n | 日均觸發/幣 | 實際 `pm_outcome` 勝率 (Wilson 95% CI) |")
    lines.append("|---|---:|---:|---|")
    t_sets_180: Dict[str, set] = {}
    for tau_lbl, z_col in (("τ = 180s (剩 2 分)", "z180"), ("τ = 240s (剩 1 分)", "z240")):
        for coin in ("BTC", "ETH", "SOL"):
            sub = df_cc[(df_cc["coin"] == coin) & df_cc[z_col].notna()].copy()
            trig = sub[np.abs(sub[z_col]) >= 1.30].copy()
            trig["pred"] = np.where(trig[z_col] > 0, "Up", "Down")
            trig["won"] = (trig["pred"] == trig["pm_outcome"]).astype(int)
            if tau_lbl.startswith("τ = 180s"):
                t_sets_180[coin] = set(trig["T"].astype(int))
            for seg_lbl, seg_df in (("Full (96d)", trig), ("OOS (後1/3, 32d)", trig[trig["split"] == "OOS"])):
                days_s = 96.1 if seg_lbl.startswith("Full") else 32.0
                n_s = len(seg_df)
                w_s = int(seg_df["won"].sum())
                wr, lo, hi = wilson_ci(w_s, n_s)
                lines.append(f"| {coin} @ {tau_lbl} ({seg_lbl}) | {n_s:,} | {n_s/days_s:.1f} | **{100*wr:.2f}%** [{100*lo:.2f}%, {100*hi:.2f}%] |")

    union_180 = len(t_sets_180["BTC"] | t_sets_180["ETH"] | t_sets_180["SOL"])
    btc_180 = len(t_sets_180["BTC"])
    multi_factor = union_180 / max(btc_180, 1)
    lines.append(f"\n- **跨幣同窗單槽疊加係數（`τ=180s` 96 天實測）**：BTC 觸發 `{btc_180:,}` 窗，BTC∪ETH∪SOL 聯集達 `{union_180:,}` 窗 → **三幣單槽不重複有效開單倍率 = `{multi_factor:.2f}×`**（即保守估計三幣單槽會讓開單數是單一 BTC 的 `{multi_factor:.2f}` 倍，無重疊風險）。")

    # 4. Monte Carlo from $42.53U
    lines.append("\n## 4. Monte Carlo 資金達成時間模擬（起跑 `$42.53U`，10,000 條路徑，嚴格照 5.4 資金階梯）\n")
    lines.append("- 階梯規則：`<$100` 固定 5 股；`$100–$200` 取 3% 資金（5 股倍數）；`>=$200` 取 5% 資金（上限 80 股）。")
    lines.append("- 組合日均單數（依 `11/20` 後 0.05 BTC / 0.01 ETH+SOL 費率實測池合併估算）：\n")

    # Combine 1/3 BTC (0.05) + 2/3 ETH/SOL (0.01) proxy for multi-coin blended EV
    assert sniper_df_nov20_btc is not None and sniper_df_nov20_es is not None
    blend_default = pd.concat([sniper_df_nov20_btc, sniper_df_nov20_es, sniper_df_nov20_es], ignore_index=True)
    assert tuned_df_005 is not None and tuned_df_001 is not None
    blend_tuned = pd.concat([tuned_df_005, tuned_df_001, tuned_df_001], ignore_index=True)

    btc_tpd_def = len(sniper_df_nov20_btc) / (full_hours / 24.0)
    btc_tpd_tuned = len(tuned_df_005) / (full_hours / 24.0)

    mc_scenarios = [
        ("A. 現行預設 Sniper (僅 BTC 1 幣, 11/20 fee=0.05)", sniper_df_nov20_btc, btc_tpd_def),
        ("B. 現行預設 Sniper (三幣單槽 BTC+ETH+SOL, 11/20 fee)", blend_default, btc_tpd_def * min(multi_factor, 2.0)),
        ("C. 擴展窗口 [120,260] Z>=1.20 (三幣單槽, 11/20 fee)", blend_tuned, btc_tpd_tuned * min(multi_factor, 2.0)),
        ("D. 保守折扣情境 (預設三幣假設實盤滑價再減 -1¢/股)", blend_default.assign(pnl=blend_default["pnl"] - 0.01, cost=blend_default["cost"] + 0.01), btc_tpd_def * min(multi_factor, 2.0)),
    ]

    lines.append("| 情境 | 模擬日單數 | 平均每股 PnL | 90天內達 `$100U` 機率 | 達 `$100U` 天數 (P25 / **P50** / P75) | 達 `$200U` P50天數 | 90天終值 (P25 / **P50** / P75) | 週勝率 | 最大回撤 (P50 / P95) |")
    lines.append("|---|---:|---:|---:|---|---:|---|---:|---|")
    for name, dff, tpd_est in mc_scenarios:
        mc = monte_carlo_growth(dff, tpd_est, start_u=42.53, horizon_days=90, n_paths=10_000, seed=42)
        d100_s = f"{mc['days_to_100_p25']:.1f} / **{mc['days_to_100_p50']:.1f}** / {mc['days_to_100_p75']:.1f} 天" if np.isfinite(mc["days_to_100_p50"]) else "未達"
        d200_s = f"**{mc['days_to_200_p50']:.1f}** 天 ({100*mc['p_reach_200_90d']:.0f}%)" if np.isfinite(mc["days_to_200_p50"]) else f"未達 ({100*mc['p_reach_200_90d']:.0f}%)"
        lines.append(
            f"| {name} | {mc['tpd']} | `{dff['pnl'].mean():+.4f}` | {100*mc['p_reach_100_90d']:.1f}% | "
            f"{d100_s} | {d200_s} | `${mc['end_eq_p25']:.0f}` / **`${mc['end_eq_p50']:.0f}`** / `${mc['end_eq_p75']:.0f}` | "
            f"{100*mc['weekly_wr_mean']:.1f}% | {100*mc['max_dd_p50']:.1f}% / {100*mc['max_dd_p95']:.1f}% |"
        )

    REPORT_MD.write_text("\n".join(lines), encoding="utf-8")
    print(f"Saved {REPORT_MD}", flush=True)


if __name__ == "__main__":
    main()
