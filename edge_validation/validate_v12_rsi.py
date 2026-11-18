"""Side-by-side 96-day Official Polymarket, 2-Year Multi-Regime & Monte Carlo validation for V12.0 RSI."""
from __future__ import annotations

import math
from pathlib import Path
import sys
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.indicators import enrich_indicators  # noqa: E402
from engine.v12_rsi_strategy import COINS, POST_NOV20_TAKER_FEES, V12_CONFIG, evaluate_v12_signal_at_index  # noqa: E402
from engine.v8_strategy import V8_CONFIG, evaluate_v8_signal_at_index  # noqa: E402

CACHE_DIR = ROOT / "edge_validation" / "cache"
OUT_CSV = ROOT / "edge_validation" / "v12_vs_v11_validation.csv"


def _attach_np_cache(df: pd.DataFrame) -> pd.DataFrame:
    cache = {c: df[c].to_numpy(dtype=np.float64) for c in df.columns if c not in ("is_green", "is_red")}
    object.__setattr__(df, "_np_cache", cache)
    return df


def _load_96d_context() -> tuple[
    dict[str, pd.DataFrame],
    dict[tuple[str, int], str],
    dict[tuple[str, int], float],
    dict[tuple[str, int], tuple[float, float]],
    dict[tuple[str, int], tuple[float, float, int]],
    int,
]:
    gm = pd.read_parquet(CACHE_DIR / "gamma_markets.parquet")
    bf = pd.read_parquet(CACHE_DIR / "binance_features.parquet")
    ch = pd.read_parquet(CACHE_DIR / "clob_hist_px.parquet")
    pm = pd.read_parquet(CACHE_DIR / "pm_trades_agg.parquet")
    oos_cut = int(gm["T"].max()) - 30 * 86400

    dfs: dict[str, pd.DataFrame] = {}
    for sym in COINS:
        m1 = pd.read_parquet(CACHE_DIR / f"raw_1m_{sym}.parquet").sort_values("ts")
        m1["b5"] = (m1["ts"] // 300) * 300_000
        g5 = m1.groupby("b5", as_index=False).agg(
            open_time=("b5", "first"), open=("open", "first"), high=("high", "max"),
            low=("low", "min"), close=("close", "last"), volume=("volume", "sum"),
            taker_buy_base=("taker_buy_base", "sum"), n1m=("open", "count"),
        )
        dfs[sym] = _attach_np_cache(enrich_indicators(g5[g5["n1m"] == 5].reset_index(drop=True)))

    gm_map = {(f"{r.coin.upper()}USDT", int(r.T)): str(r.pm_outcome).upper() for r in gm.itertuples() if str(r.pm_outcome).upper() in ("UP", "DOWN")}
    bf_map = {(str(r.symbol), int(r.T)): float(r.close_vs_twap) for r in bf.itertuples() if pd.notna(r.close_vs_twap)}
    ch_map = {(f"{r.coin.upper()}USDT", int(r.T)): (float(r.up_hist_px_T), float(r.down_hist_px_T)) for r in ch.itertuples()}
    pm_map = {
        (f"{r.coin.upper()}USDT", int(r.T)): (
            float(r.up_first_px),
            float(r.down_first_px),
            int((0.0 if pd.isna(r.up_n_trades_0_60) else float(r.up_n_trades_0_60)) + (0.0 if pd.isna(r.down_n_trades_0_60) else float(r.down_n_trades_0_60))),
        )
        for r in pm.itertuples()
    }
    return dfs, gm_map, bf_map, ch_map, pm_map, oos_cut


def _extract_96d_row(
    sig: dict[str, Any],
    dfs: dict[str, pd.DataFrame],
    idx: int,
    ctx: tuple[Any, ...],
) -> dict[str, Any] | None:
    _, gm_map, bf_map, ch_map, pm_map, oos_cut = ctx
    sym = str(sig["symbol"])
    nxt = dfs[sym].iloc[idx + 1]
    t_open = int(nxt["open_time"] // 1000)
    out = gm_map.get((sym, t_open))
    if out not in ("UP", "DOWN"):
        return None
    want = "UP" if sig["direction"] == "LONG_BOUNCE" else "DOWN"
    cvt = bf_map.get((sym, t_open), 1.0)
    basis_bps = (cvt - 1.0) * 10000.0 if want == "UP" else (1.0 - cvt) * 10000.0
    ch_p = ch_map.get((sym, t_open), (float("nan"), float("nan")))
    pm_p = pm_map.get((sym, t_open), (float("nan"), float("nan"), 0))
    px_1st = pm_p[0] if want == "UP" else pm_p[1]
    bn_win = int((nxt["close"] >= nxt["open"]) if want == "UP" else (nxt["close"] < nxt["open"]))
    return {
        "ts": t_open, "sym": sym, "is_oos": int(t_open >= oos_cut), "want": want,
        "basis_bps": basis_bps, "mid_T": ch_p[0] if want == "UP" else ch_p[1],
        "px_1st": px_1st, "n_tr": pm_p[2], "bn_w": bn_win, "pm_w": int(out == want),
    }


def _build_basis_map(dfs: dict[str, pd.DataFrame], idx: int, bf_map: dict[tuple[str, int], float]) -> dict[str, float]:
    out_map: dict[str, float] = {}
    for s in COINS:
        t_s = int(dfs[s].iloc[idx + 1]["open_time"] // 1000)
        cvt = bf_map.get((s, t_s), 1.0)
        if dfs[s]["streak_down"].iat[idx] >= 2:
            out_map[s] = (cvt - 1.0) * 10000.0
        elif dfs[s]["streak_up"].iat[idx] >= 2:
            out_map[s] = (1.0 - cvt) * 10000.0
        else:
            out_map[s] = 0.0
    return out_map


def collect_96d_signals(
    ctx: tuple[dict[str, pd.DataFrame], dict[tuple[str, int], str], dict[tuple[str, int], float], dict[tuple[str, int], tuple[float, float]], dict[tuple[str, int], tuple[float, float, int]], int],
    use_v12: bool,
    apply_twap_mid_guard: bool,
) -> pd.DataFrame:
    """Evaluate V11 or V12 over aligned 96d candles matched to official Gamma outcomes."""
    dfs, gm_map, bf_map, ch_map, pm_map, oos_cut = ctx
    rows: list[dict[str, Any]] = []
    n_bars = min(len(dfs[s]) for s in COINS)
    for i in range(185, n_bars - 2):
        basis_arg = _build_basis_map(dfs, i, bf_map) if (use_v12 and apply_twap_mid_guard) else None
        sig = evaluate_v12_signal_at_index(dfs, i, V12_CONFIG, twap_basis_bps_map=basis_arg) if use_v12 else evaluate_v8_signal_at_index(dfs, i, V8_CONFIG)
        if not sig:
            continue
        row = _extract_96d_row(sig, dfs, i, ctx)
        if row is None:
            continue
        if apply_twap_mid_guard and (row["basis_bps"] < -V12_CONFIG.max_adverse_twap_basis_bps or (not math.isnan(row["mid_T"]) and row["mid_T"] > V12_CONFIG.max_opening_mid_price)):
            continue
        rows.append(row)
    return pd.DataFrame(rows)


def summarize_96d(df: pd.DataFrame, model_name: str) -> list[dict[str, Any]]:
    """Summarize IS (66d), OOS (30d), and FULL (96d) stats including capped-band EV."""
    out: list[dict[str, Any]] = []
    for split, sub, days in (("IS_66D", df[df["is_oos"] == 0], 66.0), ("OOS_30D", df[df["is_oos"] == 1], 30.0), ("FULL_96D", df, 96.0)):
        liq = sub[sub["px_1st"].notna()].copy()
        ask = liq["px_1st"] + 0.005
        fee = liq["sym"].map(POST_NOV20_TAKER_FEES).fillna(0.01) * ask * (1.0 - ask)
        band = liq[(ask >= 0.48) & (ask <= 0.53)]
        b_ask = band["px_1st"] + 0.005
        b_fee = band["sym"].map(POST_NOV20_TAKER_FEES).fillna(0.01) * b_ask * (1.0 - b_ask)
        out.append(
            {
                "dataset": "96D_OFFICIAL_PM", "model": model_name, "split": split,
                "signals": int(len(sub)), "per_day": round(len(sub) / days, 2),
                "bn_wr_pct": round(float(sub["bn_w"].mean()) * 100.0, 2),
                "pm_wr_pct": round(float(sub["pm_w"].mean()) * 100.0, 2),
                "ev_at_50c_sh": round(float(sub["pm_w"].mean()) - 0.50, 4),
                "mean_ask_1s": round(float(ask.mean()), 4),
                "net_taker_ev_all": round(float((liq["pm_w"] - ask - fee).mean()), 4),
                "band_48_53_n": int(len(band)),
                "band_48_53_pm_wr": round(float(band["pm_w"].mean()) * 100.0, 2) if len(band) else float("nan"),
                "band_48_53_maker_ev": round(float((band["pm_w"] - band["px_1st"]).mean()), 4) if len(band) else float("nan"),
                "band_48_53_taker_ev": round(float((band["pm_w"] - b_ask - b_fee).mean()), 4) if len(band) else float("nan"),
            }
        )
    return out


def validate_2year() -> list[dict[str, Any]]:
    """Run 2-year Year-1 vs Year-2 Binance stability check for V11.0 vs V12.0."""
    dfs = {s: _attach_np_cache(enrich_indicators(pd.read_parquet(ROOT / "data" / f"{s}_5m.parquet").sort_values("open_time").reset_index(drop=True))) for s in COINS}
    n_bars = min(len(dfs[s]) for s in COINS)
    half = n_bars // 2
    opens = {s: dfs[s]["open"].to_numpy(dtype=np.float64) for s in COINS}
    closes = {s: dfs[s]["close"].to_numpy(dtype=np.float64) for s in COINS}

    results: list[dict[str, Any]] = []
    for name, fn, cfg in (("V11.0_Baseline", evaluate_v8_signal_at_index, V8_CONFIG), ("V12.0_Hammer_RSI", evaluate_v12_signal_at_index, V12_CONFIG)):
        recs: list[tuple[int, int, int]] = []
        for i in range(185, n_bars - 2):
            sig = fn(dfs, i, cfg)
            if not sig:
                continue
            sym = str(sig["symbol"])
            want_up = sig["direction"] == "LONG_BOUNCE"
            w1 = int((closes[sym][i + 1] >= opens[sym][i + 1]) if want_up else (closes[sym][i + 1] < opens[sym][i + 1]))
            w2 = int((closes[sym][i + 2] >= opens[sym][i + 2]) if want_up else (closes[sym][i + 2] < opens[sym][i + 2]))
            recs.append((1 if i < half else 2, w1, int(w1 == 1 or w2 == 1)))
        df_r = pd.DataFrame(recs, columns=["yr", "w1", "seq2"])
        results.append(
            {
                "dataset": "2Y_BINANCE", "model": name, "split": "ALL_2Y",
                "signals": int(len(df_r)), "per_day": round(len(df_r) / 730.0, 2),
                "bn_wr_pct": round(float(df_r["w1"].mean()) * 100.0, 2),
                "pm_wr_pct": float("nan"), "ev_at_50c_sh": float("nan"), "mean_ask_1s": float("nan"),
                "net_taker_ev_all": float("nan"), "band_48_53_n": 0, "band_48_53_pm_wr": float("nan"),
                "band_48_53_maker_ev": float("nan"), "band_48_53_taker_ev": float("nan"),
                "y1_bn_wr_pct": round(float(df_r[df_r["yr"] == 1]["w1"].mean()) * 100.0, 2),
                "y2_bn_wr_pct": round(float(df_r[df_r["yr"] == 2]["w1"].mean()) * 100.0, 2),
                "seq2_bn_wr_pct": round(float(df_r["seq2"].mean()) * 100.0, 2),
            }
        )
    return results


def _sim_path(rng: np.random.Generator, wr: float, px: float, fee: float, max_steps: int, n_trades: int) -> tuple[float, float]:
    eq, peak, max_dd, step, cum_cost = 50.0, 50.0, 0.0, 1, 0.0
    for _ in range(n_trades):
        if eq < 2.6:
            break
        cost_u = min(eq - cum_cost if max_steps > 1 else eq, max(2.55, round(eq * 0.05, 2) if step == 1 else cum_cost * 1.35))
        if cost_u < 2.0:
            step, cum_cost = 1, 0.0
            continue
        sh = cost_u / (px + fee * px * (1.0 - px))
        cum_cost += cost_u
        if rng.random() < wr:
            eq += (sh - cum_cost)
            step, cum_cost = 1, 0.0
        elif step >= max_steps or cum_cost >= eq * 0.55:
            eq -= cum_cost
            step, cum_cost = 1, 0.0
        else:
            step += 1
        peak = max(peak, eq)
        max_dd = max(max_dd, (peak - eq) / peak * 100.0 if peak > 0 else 100.0)
    return eq, max_dd


def run_monte_carlo() -> pd.DataFrame:
    """Bootstrap 1,000 30-day paths from $50U comparing V11 4-step vs observed V12 IS/OOS/Full profiles."""
    rng = np.random.default_rng(42)
    configs = [
        ("V11_4Step_OOS (49.6% @ 50.4c)", 0.4964, 0.5042, 0.01, 4, 480),
        ("V12_Strict_2Step_Base (53.4% @ 51.0c)", 0.5342, 0.5100, 0.01, 2, 160),
        ("V12_1Shot_Kelly_OOS_Modest (53.4% @ 51.0c)", 0.5342, 0.5100, 0.01, 1, 160),
        ("V12_1Shot_Kelly_96d_Mean (56.4% @ 51.5c)", 0.5644, 0.5150, 0.01, 1, 160),
    ]
    rows: list[dict[str, Any]] = []
    for name, wr, px, fee, steps, n_tr in configs:
        res = [_sim_path(rng, wr, px, fee, steps, n_tr) for _ in range(1000)]
        eqs = np.array([r[0] for r in res])
        dds = np.array([r[1] for r in res])
        rows.append(
            {
                "sizing_policy": name, "oos_wr_pct": round(wr * 100.0, 2), "entry_px": px, "max_steps": steps,
                "p5_end_u": round(float(np.percentile(eqs, 5)), 2),
                "median_end_u": round(float(np.median(eqs)), 2),
                "p95_end_u": round(float(np.percentile(eqs, 95)), 2),
                "p50_max_dd_pct": round(float(np.median(dds)), 2),
                "p95_max_dd_pct": round(float(np.percentile(dds, 95)), 2),
                "ruin_below_15u_pct": round(float(np.mean(eqs < 15.0)) * 100.0, 2),
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    """Execute full validation suite and save summary CSV."""
    ctx = _load_96d_context()
    df_v11 = collect_96d_signals(ctx, use_v12=False, apply_twap_mid_guard=False)
    df_v12_core = collect_96d_signals(ctx, use_v12=True, apply_twap_mid_guard=False)
    df_v12_full = collect_96d_signals(ctx, use_v12=True, apply_twap_mid_guard=True)

    summary = pd.DataFrame(
        summarize_96d(df_v11, "V11.0_Baseline")
        + summarize_96d(df_v12_core, "V12.0_Signal_Core")
        + summarize_96d(df_v12_full, "V12.0_Full_Guarded")
        + validate_2year()
    )
    summary.to_csv(OUT_CSV, index=False)
    mc_df = run_monte_carlo()

    print("=== 96-DAY OFFICIAL POLYMARKET & 2-YEAR STABILITY COMPARISON ===")
    print(summary.to_string(index=False))
    print("\n=== MONTE CARLO 30-DAY BOOTSTRAP FROM $50.00U (1,000 PATHS) ===")
    print(mc_df.to_string(index=False))


if __name__ == "__main__":
    main()
