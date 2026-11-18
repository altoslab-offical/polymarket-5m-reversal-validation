"""Audit V11.0 full-candle (T=0s->300s) signals against 91-day official Polymarket outcomes and CLOB prices."""
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
from engine.v8_strategy import V8_CONFIG, evaluate_v8_signal_at_index  # noqa: E402

CACHE_DIR = ROOT / "edge_validation" / "cache"
OUT_SUMMARY_CSV = ROOT / "edge_validation" / "v11_full_candle_91d_summary.csv"
OUT_BRANCH_CSV = ROOT / "edge_validation" / "v11_full_candle_91d_branches.csv"
COINS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")


def _norm_sym(coin: Any) -> str:
    raw = str(coin).upper()
    return raw if raw.endswith("USDT") else f"{raw}USDT"


def load_data() -> tuple[
    dict[str, pd.DataFrame],
    dict[tuple[str, int], str],
    dict[tuple[str, int], tuple[float, float, float, float]],
    int,
]:
    """Load 91-day Gamma official outcomes, Polymarket CLOB trade VWAPs, and aligned 5m Binance klines."""
    gm = pd.read_parquet(CACHE_DIR / "gamma_markets.parquet")
    pm = pd.read_parquet(CACHE_DIR / "pm_trades_agg.parquet")
    min_t, max_t = int(gm["T"].min()), int(gm["T"].max())

    dfs: dict[str, pd.DataFrame] = {}
    for sym in COINS:
        df = pd.read_parquet(ROOT / "data" / f"{sym}_5m.parquet")
        df = df[(df["open_time"] >= (min_t - 86400) * 1000) & (df["open_time"] <= (max_t + 3600) * 1000)]
        dfs[sym] = enrich_indicators(df.reset_index(drop=True))

    gm_map = {
        (_norm_sym(r.coin), int(r.T)): str(r.pm_outcome).upper()
        for r in gm.itertuples()
        if str(r.pm_outcome).upper() in ("UP", "DOWN")
    }
    pm_map = {
        (_norm_sym(r.coin), int(r.T)): (
            float(r.up_vwap_pre30),
            float(r.down_vwap_pre30),
            float(r.up_vwap_0_30),
            float(r.down_vwap_0_30),
        )
        for r in pm.itertuples()
    }
    return dfs, gm_map, pm_map, max_t - 30 * 86400


def _pick_side_px(pm_tuple: tuple[float, float, float, float] | None, want: str) -> tuple[float, float]:
    if pm_tuple is None:
        return float("nan"), float("nan")
    up_pre, dn_pre, up_030, dn_030 = pm_tuple
    return (up_pre, up_030) if want == "UP" else (dn_pre, dn_030)


def collect_signals(
    dfs: dict[str, pd.DataFrame],
    gm_map: dict[tuple[str, int], str],
    pm_map: dict[tuple[str, int], tuple[float, float, float, float]],
    oos_cut: int,
) -> pd.DataFrame:
    """Evaluate V11.0 signals across all aligned 5m bars matched to official Polymarket resolutions."""
    rows: list[dict[str, Any]] = []
    n_bars = min(len(dfs[c]) for c in COINS)
    for i in range(185, n_bars - 5):
        sig = evaluate_v8_signal_at_index(dfs, i, V8_CONFIG)
        if not sig:
            continue
        sym = str(sig["symbol"])
        nxt_row = dfs[sym].iloc[i + 1]
        nxt_ts = int(nxt_row["open_time"] // 1000)
        out1 = gm_map.get((sym, nxt_ts))
        if out1 not in ("UP", "DOWN"):
            continue
        want = "UP" if sig["direction"] == "LONG_BOUNCE" else "DOWN"
        bn_win = int((nxt_row["close"] >= nxt_row["open"]) if want == "UP" else (nxt_row["close"] < nxt_row["open"]))
        out2 = gm_map.get((sym, nxt_ts + 300))
        w1 = int(out1 == want)
        w2 = float(out2 == want) if out2 in ("UP", "DOWN") else float("nan")
        px_pre, px_030 = _pick_side_px(pm_map.get((sym, nxt_ts)), want)
        rows.append(
            {
                "ts": nxt_ts,
                "coin": sym.replace("USDT", ""),
                "sym": sym,
                "want": want,
                "branch": str(sig.get("trigger_branch", "UNKNOWN")),
                "tier": str(sig.get("tier", "STANDARD")),
                "bn_win": bn_win,
                "pm_win": w1,
                "seq2_win": float(w1 == 1 or w2 == 1.0) if not math.isnan(w2) else float("nan"),
                "px_pre30": px_pre,
                "px_0_30": px_030,
                "is_oos": int(nxt_ts >= oos_cut),
            }
        )
    return pd.DataFrame(rows)


def _nov20_taker_ev(wr: float, px: float) -> float:
    if math.isnan(wr) or math.isnan(px):
        return float("nan")
    return wr - px - 0.01 * px * (1.0 - px)


def summarize_slice(sub: pd.DataFrame, group_name: str, slice_name: str, px_col: str = "px_0_30") -> dict[str, Any]:
    """Compute exact win-rate, real CLOB price, 0%-fee Maker EV, and Nov-20 0.01 Taker EV for a subset."""
    n = int(len(sub))
    valid_px = sub[sub[px_col].notna()]
    mean_px = float(valid_px[px_col].mean()) if len(valid_px) > 0 else float("nan")
    pm_wr = float(sub["pm_win"].mean()) if n > 0 else float("nan")
    pm_wr_px = float(valid_px["pm_win"].mean()) if len(valid_px) > 0 else float("nan")
    maker_ev = float((valid_px["pm_win"] - valid_px[px_col]).mean()) if len(valid_px) > 0 else float("nan")
    return {
        "group": group_name,
        "slice": slice_name,
        "n": n,
        "n_with_px": int(len(valid_px)),
        "bn_wr_pct": round(float(sub["bn_win"].mean()) * 100.0, 2) if n > 0 else float("nan"),
        "pm_wr_pct": round(pm_wr * 100.0, 2) if n > 0 else float("nan"),
        "gap_pct": round((pm_wr - float(sub["bn_win"].mean())) * 100.0, 2) if n > 0 else float("nan"),
        "mean_px": round(mean_px, 4) if not math.isnan(mean_px) else float("nan"),
        "maker_0fee_ev_sh": round(maker_ev, 4) if not math.isnan(maker_ev) else float("nan"),
        "nov20_01_taker_ev_sh": round(_nov20_taker_ev(pm_wr_px, mean_px), 4) if not math.isnan(mean_px) else float("nan"),
    }


def _append_cap_rows(rows: list[dict[str, Any]], df: pd.DataFrame, prefix: str, px_col: str) -> None:
    for cap in (0.48, 0.50, 0.51, 0.52, 0.55):
        rows.append(summarize_slice(df[df[px_col] <= cap], f"price_cap_{px_col}", f"{prefix}<={cap:.2f}", px_col=px_col))
    rows.append(summarize_slice(df[df[px_col] > 0.52], f"price_cap_{px_col}", f"{prefix}>0.52", px_col=px_col))


def build_summary_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Assemble overall, IS/OOS, per-coin, per-tier, price-cap, and pruned-branch candidate rows."""
    rows: list[dict[str, Any]] = [
        summarize_slice(df, "overall", "ALL_91D"),
        summarize_slice(df[df["is_oos"] == 0], "overall", "IS_61D"),
        summarize_slice(df[df["is_oos"] == 1], "overall", "OOS_30D"),
    ]
    for coin in ("BTC", "ETH", "SOL"):
        rows.append(summarize_slice(df[df["coin"] == coin], "coin", coin))
    for tier in ("STANDARD", "TIER_S", "TIER_S+"):
        rows.append(summarize_slice(df[df["tier"] == tier], "tier", tier))
    _append_cap_rows(rows, df, "px030", "px_0_30")
    _append_cap_rows(rows, df[df["is_oos"] == 1], "OOS_px030", "px_0_30")
    _append_cap_rows(rows, df, "pxpre", "px_pre30")

    core_df = df[df["branch"].str.contains("C_BB|B_BACC", regex=True) & ~df["branch"].isin(["D_WYCKOFF_2LEG"])]
    for tag, sub in (("CORE_CBB_BBACC_ALL", core_df), ("CORE_OOS", core_df[core_df["is_oos"] == 1]), ("C_BB_ONLY", df[df["branch"] == "C_BB"])):
        rows.append(summarize_slice(sub, "pruned_candidate", tag))
        rows.append(summarize_slice(sub[sub["px_0_30"] <= 0.51], "pruned_candidate", f"{tag}_px<=0.51"))
        rows.append(summarize_slice(sub[sub["px_0_30"] <= 0.50], "pruned_candidate", f"{tag}_px<=0.50"))
    return pd.DataFrame(rows)


def build_branch_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Build IS vs OOS statistics for every V11.0 trigger branch."""
    rows: list[dict[str, Any]] = []
    for branch, grp in sorted(df.groupby("branch"), key=lambda item: -len(item[1])):
        g_is = grp[grp["is_oos"] == 0]
        g_oos = grp[grp["is_oos"] == 1]
        valid = grp[grp["px_0_30"].notna()]
        rows.append(
            {
                "branch": branch,
                "n": int(len(grp)),
                "bn_wr_pct": round(float(grp["bn_win"].mean()) * 100.0, 2),
                "pm_wr_pct": round(float(grp["pm_win"].mean()) * 100.0, 2),
                "is_n": int(len(g_is)),
                "is_pm_wr_pct": round(float(g_is["pm_win"].mean()) * 100.0, 2) if len(g_is) else float("nan"),
                "oos_n": int(len(g_oos)),
                "oos_pm_wr_pct": round(float(g_oos["pm_win"].mean()) * 100.0, 2) if len(g_oos) else float("nan"),
                "mean_px_pre30": round(float(grp["px_pre30"].mean()), 4),
                "mean_px_0_30": round(float(valid["px_0_30"].mean()), 4) if len(valid) else float("nan"),
                "maker_0fee_ev_sh": round(float((valid["pm_win"] - valid["px_0_30"]).mean()), 4) if len(valid) else float("nan"),
            }
        )
    return pd.DataFrame(rows)


def _step_shares_sequence(step: int) -> float:
    return (5.0, 7.0, 14.0, 28.0)[step - 1]


def _resolve_sequence(
    row: Any,
    gm_map: dict[tuple[str, int], str],
    pm_map: dict[tuple[str, int], tuple[float, float, float, float]],
    equity: float,
    step_cap: int,
) -> tuple[float, int, bool, int]:
    cum_cost = 0.0
    end_ts = int(row.ts)
    for k in range(1, step_cap + 1):
        ts_k = int(row.ts) + (k - 1) * 300
        out_k = gm_map.get((str(row.sym), ts_k))
        if out_k not in ("UP", "DOWN"):
            return cum_cost, end_ts, False, k - 1
        _, px_k = _pick_side_px(pm_map.get((str(row.sym), ts_k)), str(row.want))
        px_eff = px_k if (not math.isnan(px_k) and 0.05 < px_k < 0.95) else 0.50
        sh_k = _step_shares_sequence(k)
        cost_k = sh_k * px_eff
        if cum_cost + cost_k > equity:
            return -cum_cost, end_ts, False, max(1, k - 1)
        cum_cost += cost_k
        end_ts = ts_k + 300
        if out_k == str(row.want):
            return sh_k * 1.0 - cum_cost, end_ts, True, k
    return -cum_cost, end_ts, False, step_cap


def simulate_bankroll(
    df: pd.DataFrame,
    gm_map: dict[tuple[str, int], str],
    pm_map: dict[tuple[str, int], tuple[float, float, float, float]],
    name: str,
    step_cap: int,
    px_cap: float | None = None,
    branches_filter: set[str] | None = None,
) -> dict[str, Any]:
    """Simulate non-overlapping sequences from $43.00U on chronological 91-day official Polymarket outcomes."""
    eq, peak, max_dd, next_free_ts = 43.0, 43.0, 0.0, 0
    seqs, wins, busts = 0, 0, 0
    for row in df.sort_values("ts").itertuples():
        if int(row.ts) < next_free_ts or eq < 2.50:
            continue
        if branches_filter is not None and str(row.branch) not in branches_filter:
            continue
        if px_cap is not None and (math.isnan(float(row.px_0_30)) or float(row.px_0_30) > px_cap):
            continue
        pnl, end_ts, won, _ = _resolve_sequence(row, gm_map, pm_map, eq, step_cap)
        if end_ts == int(row.ts):
            continue
        eq += pnl
        peak = max(peak, eq)
        max_dd = max(max_dd, (peak - eq) / peak * 100.0 if peak > 0 else 0.0)
        next_free_ts = end_ts
        seqs += 1
        wins += int(won)
        busts += int(not won)
    return {
        "policy": name,
        "step_cap": step_cap,
        "seqs": seqs,
        "wins": wins,
        "busts": busts,
        "seq_wr_pct": round(wins / seqs * 100.0, 2) if seqs else 0.0,
        "end_equity_u": round(eq, 2),
        "net_pnl_u": round(eq - 43.0, 2),
        "max_dd_pct": round(max_dd, 2),
    }


def main() -> None:
    """Run full 91-day audit, save summary CSVs, and print observed metrics."""
    dfs, gm_map, pm_map, oos_cut = load_data()
    df = collect_signals(dfs, gm_map, pm_map, oos_cut)
    summary_df = build_summary_rows(df)
    branch_df = build_branch_rows(df)
    summary_df.to_csv(OUT_SUMMARY_CSV, index=False)
    branch_df.to_csv(OUT_BRANCH_CSV, index=False)

    print("=== SUMMARY (91D OFFICIAL POLYMARKET PM_OUTCOME) ===")
    print(summary_df.to_string(index=False))
    print("\n=== BRANCH IS VS OOS ===")
    print(branch_df.to_string(index=False))
    print("\n=== CHRONOLOGICAL $43U SIMULATION (REAL PM_OUTCOME + VWAP_0_30, 0% MAKER) ===")
    sims = [
        simulate_bankroll(df, gm_map, pm_map, "ALL_4STEP_UNFILTERED", step_cap=4),
        simulate_bankroll(df, gm_map, pm_map, "ALL_2STEP_UNFILTERED", step_cap=2),
        simulate_bankroll(df, gm_map, pm_map, "ALL_1SHOT_5SH_UNFILTERED", step_cap=1),
        simulate_bankroll(df, gm_map, pm_map, "ALL_1SHOT_5SH_PX<=0.51", step_cap=1, px_cap=0.51),
        simulate_bankroll(df, gm_map, pm_map, "ALL_2STEP_PX<=0.51", step_cap=2, px_cap=0.51),
        simulate_bankroll(df, gm_map, pm_map, "C_BB_ONLY_1SHOT_ALLPX", step_cap=1, branches_filter={"C_BB"}),
        simulate_bankroll(df, gm_map, pm_map, "C_BB_ONLY_1SHOT_PX<=0.52", step_cap=1, px_cap=0.52, branches_filter={"C_BB"}),
        simulate_bankroll(df, gm_map, pm_map, "C_BB_ONLY_2STEP_ALLPX", step_cap=2, branches_filter={"C_BB"}),
    ]
    print(pd.DataFrame(sims).to_string(index=False))


if __name__ == "__main__":
    main()
