#!/usr/bin/env python3
"""
Section 4.1 Historical Verification (`L1 + L2 + L3` zero-lookahead fixes, `2026-08-14 .. 2026-11-18`):
  - L1: Binance 1s klines strictly indexed at `idx_T + tau - 1` (bar closing at `T + tau`).
  - L2: Post-signal execution prices taken strictly AFTER `tau` (`first point in [tau+1, tau+5]`)
        AND pre-tau lag-bucket breakdown (`<=1s, 1-5s, 5-10s, 10-20s, 20-40s, 40-70s`).
  - L3: Conditional half-spread measured on `live_btc_5m.db` specifically when `model EV >= 0.035`.
"""
from __future__ import annotations

import math
import pathlib
import sqlite3
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent
REPO = ROOT.parent
CACHE = ROOT / "cache"
DB_PATH = REPO / "data/oracle_sniper/live_btc_5m.db"

GAMMA_PQ = CACHE / "gamma_markets.parquet"
GAMMA_BACKFILL_PQ = CACHE / "live_btc_5m_gamma_backfill.parquet"
CLOB_PQ = CACHE / "clob_hist_l2.parquet"
BN_PQ = CACHE / "bn_1s_tau_features_l1.parquet"

OUT_RAW_PQ = ROOT / "phase41_intrawindow.parquet"
OUT_MF_CSV = ROOT / "phase41_model_free_summary.csv"
OUT_WM_CSV = ROOT / "phase41_with_model_summary.csv"
OUT_L2_CSV = ROOT / "phase41_l2_post5_summary.csv"
OUT_SUMMARY_MD = ROOT / "summary_41.md"
OUT_DEVIATIONS_MD = ROOT / "deviations.md"

TAUS = [150, 180, 210, 240, 255]
ASK_BRACKETS: List[Tuple[str, float, float]] = [
    ("[0.60,0.68)", 0.60, 0.68),
    ("[0.68,0.76)", 0.68, 0.76),
    ("[0.76,0.84)", 0.76, 0.84),
    ("[0.84,0.92)", 0.84, 0.92),
    ("[0.68,0.82]", 0.68, 0.82001),
    ("ALL_PRICES", 0.00, 1.00),
]
ALL_SPREAD_BANDS: List[Tuple[str, float, float]] = [
    ("[0.00,0.50)", 0.00, 0.50),
    ("[0.50,0.60)", 0.50, 0.60),
    ("[0.60,0.68)", 0.60, 0.68),
    ("[0.68,0.76)", 0.68, 0.76),
    ("[0.76,0.84)", 0.76, 0.84),
    ("[0.84,0.92)", 0.84, 0.92),
    ("[0.92,1.00)", 0.92, 1.00),
]


def norm_cdf_vec(arr: np.ndarray) -> np.ndarray:
    """Standard normal CDF via C-level math.erf."""
    inv_sqrt2 = 0.7071067811865475
    erf = math.erf
    return np.fromiter(
        (0.5 * (1.0 + erf(x * inv_sqrt2)) if math.isfinite(x) else float("nan") for x in arr),
        dtype=np.float64,
        count=len(arr),
    )


def df_to_md(df: pd.DataFrame) -> str:
    """GitHub Markdown table formatter."""
    cols = list(df.columns)
    hdr = "| " + " | ".join(str(c) for c in cols) + " |"
    sep = "| " + " | ".join("---" for _ in cols) + " |"
    rows = [
        "| " + " | ".join("" if (isinstance(v, float) and math.isnan(v)) else str(v) for v in row) + " |"
        for row in df.itertuples(index=False, name=None)
    ]
    return "\n".join([hdr, sep, *rows])


def wilson_ci(wins: int, n: int, z: float = 1.95996398454) -> Tuple[float, float]:
    """Wilson score 95% proportion CI."""
    if n <= 0:
        return (np.nan, np.nan)
    p = wins / n
    den = 1.0 + z * z / n
    ctr = (p + z * z / (2.0 * n)) / den
    half = (z / den) * math.sqrt((p * (1.0 - p) + z * z / (4.0 * n)) / n)
    return (max(0.0, ctr - half), min(1.0, ctr + half))


def bootstrap_ci(vals: np.ndarray, n_boot: int = 800, seed: int = 42) -> Tuple[float, float]:
    """Chunked bootstrap 95% mean CI."""
    if len(vals) < 2:
        m = float(np.mean(vals)) if len(vals) else np.nan
        return (m, m)
    rng = np.random.default_rng(seed)
    n = len(vals)
    means = np.empty(n_boot, dtype=np.float64)
    bs = 100
    for i in range(0, n_boot, bs):
        k = min(bs, n_boot - i)
        means[i : i + k] = vals[rng.integers(0, n, size=(k, n))].mean(axis=1)
    lo, hi = np.percentile(means, [2.5, 97.5])
    return (float(lo), float(hi))


def taker_fee_vec(px: np.ndarray, rate: np.ndarray | float) -> np.ndarray:
    """Vectorized Polymarket crypto taker fee per share."""
    valid = (px > 0.0) & (px < 1.0)
    raw = np.where(valid, np.round(rate * px * (1.0 - px), 5), 0.0)
    return np.where(raw >= 1e-5, raw, 0.0)


def _compute_db_twap_fairs(df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
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
    sig = np.maximum(pd.Series(ret).rolling(1800, min_periods=120).std().bfill().fillna(0.00005).to_numpy(), 0.000035)
    el, cl, s0 = df["elapsed_sec"].to_numpy(dtype=np.float64), df["chainlink_price"].ffill().to_numpy(dtype=np.float64), df["gm_ptb"].to_numpy(dtype=np.float64)
    r = np.maximum(1.0, 300.0 - el)
    z = np.empty(len(df), dtype=np.float64)
    le240 = el <= 240.0
    z[le240] = ((cl[le240] - s0[le240]) / s0[le240]) / (sig[le240] * np.sqrt(np.maximum(1.0, r[le240] - 40.0)))
    gt240 = ~le240
    mean_tw = ((el[gt240] - 240.0) * df["A_240"].to_numpy()[gt240] + r[gt240] * cl[gt240]) / 60.0
    sd_tw = np.maximum((r[gt240] / 60.0) * sig[gt240] * np.sqrt(np.maximum(1.0, r[gt240]) / 3.0), 1e-7)
    z[gt240] = ((mean_tw - s0[gt240]) / s0[gt240]) / sd_tw
    fu = norm_cdf_vec(z)
    return fu, 1.0 - fu


def estimate_half_spreads_l3() -> Tuple[pd.DataFrame, Dict[str, float]]:
    """L3 Fix: Measure empirical half-spread `(ask - bid)/2` conditioned on `model EV >= 0.035`."""
    df_w = pd.read_parquet(GAMMA_BACKFILL_PQ)
    con = sqlite3.connect(DB_PATH)
    df_s = pd.read_sql_query(
        "SELECT ts, window_ts, elapsed_sec, chainlink_price, binance_price, up_bid, up_ask, down_bid, down_ask "
        "FROM snapshots WHERE elapsed_sec BETWEEN 145.0 AND 260.0 ORDER BY window_ts, elapsed_sec",
        con,
    )
    con.close()
    df = df_s.merge(df_w[["window_ts", "gm_ptb"]], on="window_ts", how="inner")
    df = df[df["gm_ptb"].notna()].sort_values(["window_ts", "elapsed_sec"]).reset_index(drop=True)
    fair_up, fair_dn = _compute_db_twap_fairs(df)

    mids_l, hss_l, ev_mid_l, ev_ask_l = [], [], [], []
    for b_c, a_c, f_arr in (("up_bid", "up_ask", fair_up), ("down_bid", "down_ask", fair_dn)):
        b, a = df[b_c].to_numpy(dtype=np.float64), df[a_c].to_numpy(dtype=np.float64)
        ok = np.isfinite(b) & np.isfinite(a) & (b > 0.0) & (a < 1.0) & (a > b)
        mid = 0.5 * (b[ok] + a[ok])
        mids_l.append(mid)
        hss_l.append(0.5 * (a[ok] - b[ok]))
        ev_mid_l.append(f_arr[ok] - (mid + 0.07 * mid * (1.0 - mid)))
        ev_ask_l.append(f_arr[ok] - (a[ok] + 0.07 * a[ok] * (1.0 - a[ok])))

    mids, hss = np.concatenate(mids_l), np.concatenate(hss_l)
    ev_mid, ev_ask = np.concatenate(ev_mid_l), np.concatenate(ev_ask_l)
    rows: List[Dict[str, Any]] = []
    lookup: Dict[str, float] = {}
    for label, lo, hi in ALL_SPREAD_BANDS:
        m = (mids >= lo) & (mids < hi)
        mc_mid = m & (ev_mid >= 0.035)
        mc_ask = m & (ev_ask >= 0.035)
        cond = hss[mc_ask] if mc_ask.any() else hss[m]
        med = float(np.median(cond)) if len(cond) else 0.01
        rows.append({
            "bracket": label, "uncond_n": int(m.sum()), "uncond_med": round(float(np.median(hss[m])), 5),
            "ev_mid_n": int(mc_mid.sum()), "ev_mid_med": round(float(np.median(hss[mc_mid])), 5) if mc_mid.any() else np.nan,
            "ev_mid_mean": round(float(np.mean(hss[mc_mid])), 5) if mc_mid.any() else np.nan,
            "ev_ask_n": int(mc_ask.sum()), "ev_ask_med": round(med, 5),
            "ev_ask_mean": round(float(np.mean(cond)), 5) if len(cond) else 0.01,
            "ev_ask_p75": round(float(np.percentile(cond, 75)), 5) if len(cond) else 0.015,
        })
        lookup[label] = med
    return pd.DataFrame(rows), lookup


def map_half_spread(px: np.ndarray, lookup: Dict[str, float]) -> np.ndarray:
    """Map price vector to conditional half-spread vector."""
    hs = np.full(len(px), 0.01, dtype=np.float64)
    for label, lo, hi in ALL_SPREAD_BANDS:
        hs[(px >= lo) & (px < hi)] = lookup[label]
    return hs


def compute_brier(probs: np.ndarray, outcomes: np.ndarray) -> float:
    """Mean squared probability error."""
    ok = np.isfinite(probs) & np.isfinite(outcomes)
    return float(np.mean((probs[ok] - outcomes[ok]) ** 2)) if ok.any() else np.nan


def build_base_dataset(spread_lookup: Dict[str, float]) -> Tuple[pd.DataFrame, int, int, int]:
    """Load L1+L2 caches and attach model probabilities & pre/post5 cost arrays."""
    df_gm = pd.read_parquet(GAMMA_PQ)
    df_clob = pd.read_parquet(CLOB_PQ)
    df_bn = pd.read_parquet(BN_PQ)
    gm_ok = df_gm[(df_gm["found"] == 1) & (df_gm["pm_outcome"].isin(["Up", "Down"]))].copy()
    gm_ok["pm_outcome"] = (gm_ok["pm_outcome"] == "Up").astype(np.float64)
    uniq_t = np.sort(df_gm["T"].unique())
    t_split = int(uniq_t[(2 * len(uniq_t)) // 3])

    bn_rows = []
    for tau in TAUS:
        sub = df_bn[["coin", "T", "S0", "twap60_close", "A_255"]].copy()
        sub["tau"] = tau
        sub["spot"] = df_bn[f"spot_{tau}"]
        sub["sigma_log_1s"] = df_bn[f"sigma_{tau}"]
        bn_rows.append(sub)
    df = (
        df_clob.merge(gm_ok[["coin", "T", "pm_outcome", "is_twap60"]], on=["coin", "T"], how="inner")
        .merge(pd.concat(bn_rows, ignore_index=True), on=["coin", "T", "tau"], how="inner")
    )
    df["split"] = np.where(df["T"] < t_split, "IS", "OOS")
    _attach_model_and_prices(df, spread_lookup)
    return df, len(df_gm), len(gm_ok), t_split


def _attach_model_and_prices(df: pd.DataFrame, spread_lookup: Dict[str, float]) -> None:
    tau_arr = df["tau"].to_numpy(dtype=np.float64)
    up_px, dn_px = df["up_px"].to_numpy(dtype=np.float64), df["down_px"].to_numpy(dtype=np.float64)
    up_lag, dn_lag = df["up_lag_sec"].to_numpy(dtype=np.float64), df["down_lag_sec"].to_numpy(dtype=np.float64)
    df["up_valid"] = np.isfinite(up_px) & (up_lag >= 0.0) & (up_lag <= tau_arr) & (up_px > 0.0) & (up_px < 1.0)
    df["dn_valid"] = np.isfinite(dn_px) & (dn_lag >= 0.0) & (dn_lag <= tau_arr) & (dn_px > 0.0) & (dn_px < 1.0)

    up_p5, dn_p5 = df["up_post5_px"].to_numpy(dtype=np.float64), df["down_post5_px"].to_numpy(dtype=np.float64)
    df["up_post5_valid"] = np.isfinite(up_p5) & (up_p5 > 0.0) & (up_p5 < 1.0)
    df["dn_post5_valid"] = np.isfinite(dn_p5) & (dn_p5 > 0.0) & (dn_p5 < 1.0)

    sum_px = up_px + dn_px
    df["mkt_up_mid"] = np.where(df["up_valid"] & df["dn_valid"] & (sum_px > 0.5) & (sum_px < 1.5), up_px / sum_px, np.where(df["up_valid"], up_px, 1.0 - dn_px))

    spot, s0 = df["spot"].to_numpy(dtype=np.float64), df["S0"].to_numpy(dtype=np.float64)
    sig_usd = np.maximum(s0 * df["sigma_log_1s"].to_numpy(dtype=np.float64), s0 * 1e-6)
    r, a_255 = 300.0 - tau_arr, df["A_255"].to_numpy(dtype=np.float64)
    z = np.empty(len(df), dtype=np.float64)
    le240 = tau_arr <= 240.0
    z[le240] = (spot[le240] - s0[le240]) / (sig_usd[le240] * np.sqrt(r[le240] - 40.0))
    gt240 = ~le240
    mean_tw = ((tau_arr[gt240] - 240.0) * a_255[gt240] + r[gt240] * spot[gt240]) / 60.0
    sd_tw = (r[gt240] / 60.0) * sig_usd[gt240] * np.sqrt(r[gt240] / 3.0)
    z[gt240] = (mean_tw - s0[gt240]) / sd_tw
    fu = norm_cdf_vec(z)
    df["z_twap"], df["fair_up"], df["fair_down"] = z.astype("float32"), fu.astype("float32"), (1.0 - fu).astype("float32")


def _summarize_cell(wins_arr: np.ndarray, cost_arr: np.ndarray, pnl_arr: np.ndarray, meta: Dict[str, Any]) -> Dict[str, Any]:
    n = len(pnl_arr)
    if n == 0:
        return {**meta, "n": 0, "wins": 0, "wr": np.nan, "wr_ci95": "", "mean_cost": np.nan, "mean_pnl": np.nan, "ci95_lo": np.nan, "ci95_hi": np.nan}
    w = int(wins_arr.sum())
    wlo, whi = wilson_ci(w, n)
    plo, phi = bootstrap_ci(pnl_arr)
    return {
        **meta, "n": n, "wins": w, "wr": round(w / n, 4), "wr_ci95": f"[{wlo:.3f},{whi:.3f}]",
        "mean_cost": round(float(cost_arr.mean()), 4), "mean_pnl": round(float(pnl_arr.mean()), 5),
        "ci95_lo": round(plo, 5), "ci95_hi": round(phi, 5),
    }


def _compute_side_choice(
    up_p: np.ndarray, dn_p: np.ndarray, up_ok: np.ndarray, dn_ok: np.ndarray,
    fair_up: np.ndarray, fair_dn: np.ndarray, rate: np.ndarray | float, spread_lookup: Dict[str, float], slip: float = 0.0,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    au = np.minimum(0.99, up_p + map_half_spread(np.nan_to_num(up_p, nan=0.5), spread_lookup) + slip)
    ad = np.minimum(0.99, dn_p + map_half_spread(np.nan_to_num(dn_p, nan=0.5), spread_lookup) + slip)
    cu, cd = au + taker_fee_vec(au, rate), ad + taker_fee_vec(ad, rate)
    eu = np.where(up_ok, fair_up - cu, -999.0)
    ed = np.where(dn_ok, fair_dn - cd, -999.0)
    tup = eu >= ed
    return tup, np.where(tup, eu, ed), np.where(tup, au, ad), np.where(tup, cu, cd)


def build_l2_post5_table(df: pd.DataFrame, spread_lookup: Dict[str, float]) -> pd.DataFrame:
    """Claude L2 spec: model uses <= tau (`L1` fixed), execution uses first point in `[tau+1, tau+5]`, listed by coin x split x ask x fee."""
    rows: List[Dict[str, Any]] = []
    up_p5, dn_p5 = df["up_post5_px"].to_numpy(dtype=np.float64), df["down_post5_px"].to_numpy(dtype=np.float64)
    up_ok5, dn_ok5 = df["up_post5_valid"].to_numpy(), df["dn_post5_valid"].to_numpy()
    fu, fd = df["fair_up"].to_numpy(dtype=np.float64), df["fair_down"].to_numpy(dtype=np.float64)
    y_up = df["pm_outcome"].to_numpy(dtype=np.float64)
    coins, splits = df["coin"].to_numpy(), df["split"].to_numpy()
    new_rate = df["coin"].map({"BTC": 0.05, "ETH": 0.01, "SOL": 0.01}).to_numpy(dtype=np.float64)

    for fee_lbl, rate_v in (("0.07", 0.07), ("new(0.05/0.01)", new_rate)):
        tup, ev5, ask5, cost5 = _compute_side_choice(up_p5, dn_p5, up_ok5, dn_ok5, fu, fd, rate_v, spread_lookup)
        won5 = np.where(tup, y_up == 1.0, y_up == 0.0).astype(np.float64)
        pnl5 = won5 - cost5
        valid_ev = (ev5 >= 0.035) & np.isfinite(pnl5)
        for sp in ("IS", "OOS"):
            for cn in ("ALL", "BTC", "ETH", "SOL"):
                m_base = valid_ev & (splits == sp) & (True if cn == "ALL" else (coins == cn))
                for br_lbl, lo, hi in ASK_BRACKETS:
                    m = m_base & (ask5 >= lo) & (ask5 < hi)
                    rows.append(_summarize_cell(won5[m], cost5[m], pnl5[m], {"coin": cn, "split": sp, "ask_bracket": br_lbl, "fee": fee_lbl}))
    return pd.DataFrame(rows)


def build_l2_causal_fill_table(df: pd.DataFrame, spread_lookup: Dict[str, float]) -> pd.DataFrame:
    """Strictly causal L2: signal triggered on pre-tau quote (`<= tau`), filled at `[tau+1, tau+5]`."""
    rows: List[Dict[str, Any]] = []
    up_pre, dn_pre = df["up_px"].to_numpy(dtype=np.float64), df["down_px"].to_numpy(dtype=np.float64)
    up_p5, dn_p5 = df["up_post5_px"].to_numpy(dtype=np.float64), df["down_post5_px"].to_numpy(dtype=np.float64)
    up_ok = df["up_valid"].to_numpy() & df["up_post5_valid"].to_numpy()
    dn_ok = df["dn_valid"].to_numpy() & df["dn_post5_valid"].to_numpy()
    fu, fd = df["fair_up"].to_numpy(dtype=np.float64), df["fair_down"].to_numpy(dtype=np.float64)
    y_up, coins, splits = df["pm_outcome"].to_numpy(dtype=np.float64), df["coin"].to_numpy(), df["split"].to_numpy()
    new_rate = df["coin"].map({"BTC": 0.05, "ETH": 0.01, "SOL": 0.01}).to_numpy(dtype=np.float64)

    for fee_lbl, rate_v in (("0.07", 0.07), ("new(0.05/0.01)", new_rate)):
        tup, ev_pre, ask_pre, _ = _compute_side_choice(up_pre, dn_pre, up_ok, dn_ok, fu, fd, rate_v, spread_lookup)
        p_post = np.where(tup, up_p5, dn_p5)
        ask_post = np.minimum(0.99, p_post + map_half_spread(np.nan_to_num(p_post, nan=0.5), spread_lookup))
        cost_post = ask_post + taker_fee_vec(ask_post, rate_v)
        won = np.where(tup, y_up == 1.0, y_up == 0.0).astype(np.float64)
        pnl = won - cost_post
        m_sig = (ev_pre >= 0.035) & (ask_pre >= 0.68) & (ask_pre <= 0.82) & np.isfinite(pnl)
        for sp in ("IS", "OOS"):
            for cn in ("ALL", "BTC", "ETH", "SOL"):
                m = m_sig & (splits == sp) & (True if cn == "ALL" else (coins == cn))
                rows.append(_summarize_cell(won[m], cost_post[m], pnl[m], {"rule": "causal_pre_ev>=3.5c_post5_fill", "coin": cn, "split": sp, "fee": fee_lbl}))
    return pd.DataFrame(rows)


def build_lag_bucket_table(df: pd.DataFrame, spread_lookup: Dict[str, float]) -> pd.DataFrame:
    """Preserved lag-bucket sensitivity on L1+L3 OOS `[0.68, 0.82]`, EV >= 3.5c."""
    oos = df[df["split"] == "OOS"]
    up_pre, dn_pre = oos["up_px"].to_numpy(dtype=np.float64), oos["down_px"].to_numpy(dtype=np.float64)
    up_ok, dn_ok = oos["up_valid"].to_numpy(), oos["dn_valid"].to_numpy()
    fu, fd = oos["fair_up"].to_numpy(dtype=np.float64), oos["fair_down"].to_numpy(dtype=np.float64)
    y_up = oos["pm_outcome"].to_numpy(dtype=np.float64)
    new_rate = oos["coin"].map({"BTC": 0.05, "ETH": 0.01, "SOL": 0.01}).to_numpy(dtype=np.float64)
    tup, ev, ask, cost = _compute_side_choice(up_pre, dn_pre, up_ok, dn_ok, fu, fd, new_rate, spread_lookup)
    lag_side = np.where(tup, oos["up_lag_sec"].to_numpy(), oos["down_lag_sec"].to_numpy())
    won = np.where(tup, y_up == 1.0, y_up == 0.0).astype(np.float64)
    pnl = won - cost
    base = (ev >= 0.035) & (ask >= 0.68) & (ask <= 0.82)
    rows: List[Dict[str, Any]] = []
    for lbl, lo, hi in (("lag<=1s", -0.1, 1.0), ("(1,5]s", 1.0, 5.0), ("(5,10]s", 5.0, 10.0), ("(10,20]s", 10.0, 20.0), ("(20,40]s", 20.0, 40.0), ("(40,70]s", 40.0, 70.0)):
        m = base & (lag_side > lo) & (lag_side <= hi)
        rows.append(_summarize_cell(won[m], cost[m], pnl[m], {"lag_bucket": lbl, "fee": "new(0.05/0.01)", "ask_band": "[0.68,0.82]"}))
    return pd.DataFrame(rows)


def build_is_brier_and_mf(df: pd.DataFrame, spread_lookup: Dict[str, float]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Recompute L1-corrected IS Brier vs market mid and Model-Free OOS summary."""
    is_v = df[(df["split"] == "IS") & (df["up_valid"] | df["dn_valid"])]
    b_rows = []
    for tau in TAUS:
        st = is_v[is_v["tau"] == tau]
        for cn in ("ALL", "BTC", "ETH", "SOL"):
            sc = st if cn == "ALL" else st[st["coin"] == cn]
            bm, bk = compute_brier(sc["fair_up"].to_numpy(), sc["pm_outcome"].to_numpy()), compute_brier(sc["mkt_up_mid"].to_numpy(), sc["pm_outcome"].to_numpy())
            b_rows.append({"coin": cn, "tau": tau, "n": len(sc), "brier_model_L1": round(bm, 5), "brier_market": round(bk, 5)})

    # Model-Free using post5 execution price & L3 conditional spread (OOS)
    oos = df[df["split"] == "OOS"]
    up_p5, dn_p5 = oos["up_post5_px"].to_numpy(dtype=np.float64), oos["down_post5_px"].to_numpy(dtype=np.float64)
    up_ok5, dn_ok5 = oos["up_post5_valid"].to_numpy(), oos["dn_post5_valid"].to_numpy()
    fav_up = np.where(up_ok5 & dn_ok5, up_p5 >= dn_p5, up_ok5)
    fav_px = np.where(fav_up, up_p5, dn_p5)
    fav_ok = np.where(fav_up, up_ok5, dn_ok5) & (fav_px >= 0.60) & (fav_px < 0.92)
    ask = np.minimum(0.99, fav_px + map_half_spread(np.nan_to_num(fav_px, nan=0.5), spread_lookup))
    new_rate = oos["coin"].map({"BTC": 0.05, "ETH": 0.01, "SOL": 0.01}).to_numpy(dtype=np.float64)
    won = np.where(fav_up, oos["pm_outcome"] == 1.0, oos["pm_outcome"] == 0.0).astype(np.float64)
    mf_rows: List[Dict[str, Any]] = []
    for flbl, r_v in (("0.07", 0.07), ("new(0.05/0.01)", new_rate)):
        cost = ask + taker_fee_vec(ask, r_v)
        pnl = won - cost
        for br_lbl, lo, hi in ASK_BRACKETS[:4]:
            m = fav_ok & (fav_px >= lo) & (fav_px < hi)
            mf_rows.append(_summarize_cell(won[m], cost[m], pnl[m], {"bracket": br_lbl, "fee": flbl, "split": "OOS_post5"}))
    return pd.DataFrame(b_rows), pd.DataFrame(mf_rows)


def main() -> None:
    """Run L1 + L2 + L3 corrected Section 4.1 verification and save reports."""
    spread_df, spread_lookup = estimate_half_spreads_l3()
    df, n_tot, n_ok, t_split = build_base_dataset(spread_lookup)
    df.to_parquet(OUT_RAW_PQ, index=False)

    df_l2 = build_l2_post5_table(df, spread_lookup)
    df_l2_causal = build_l2_causal_fill_table(df, spread_lookup)
    df_lag = build_lag_bucket_table(df, spread_lookup)
    df_brier, df_mf = build_is_brier_and_mf(df, spread_lookup)

    df_l2.to_csv(OUT_L2_CSV, index=False)
    df_mf.to_csv(OUT_MF_CSV, index=False)
    df_l2_causal.to_csv(OUT_WM_CSV, index=False)

    lines = [
        "# Section 4.1 修正報告：`L1 + L2 + L3` 零前視重新驗證 (`2026-08-14 .. 2026-11-18`)", "",
        f"- 總視窗：`{n_tot:,}`（已結算 `{n_ok:,}`）；IS `T < {t_split}` / OOS `T >= {t_split}`",
        "- **L1 修正**：`spot_τ = series[idx_T + τ - 1]`（結束於 `T + τ` 之 1s bar），`S0` 與 `A_255` 右端點同樣減 1。",
        "- **L2 修正**：進場價改取 `t ∈ [τ+1, τ+5]` 的第一個價格點（模型嚴格只用 `<= τ` 資訊）；另保留前照 `lag` 分組表與因果預判→後成交表。",
        "- **L3 修正**：以 `live_btc_5m.db` 在 **模型 `EV >= 3.5¢` 成立時刻** 的實際半價差中位數估算成本，取代全樣本無條件中位數。", "",
        "## 1. L3 條件半價差表（無條件 vs 模型 `EV >= 3.5¢`）", "", df_to_md(spread_df), "",
        "## 2. L2 核心表格：`t ∈ [τ+1, τ+5]` 成交 × Ask 區間 × 幣別 × IS/OOS × 費率 (`EV >= 3.5¢`)", "",
        df_to_md(df_l2[df_l2["n"] > 0]), "",
        "## 3. L2 嚴格因果板：`τ` 時點依預盤口觸發 → `[τ+1, τ+5]` 成交 (`ask[0.68,0.82]`, `EV_pre >= 3.5¢`)", "",
        df_to_md(df_l2_causal), "",
        "## 4. Model-Free `[0.60, 0.92)` 同用 `[τ+1, τ+5]` + L3 半價差 (OOS)", "",
        df_to_md(df_mf), "",
        "## 5. L1 修正後：OOS 依 `lag` 分組敏感度 (`ask[0.68,0.82]`, `EV>=3.5¢`, `new fee`)", "",
        df_to_md(df_lag), "",
        "## 6. L1 修正後：IS Brier Score vs 市場中價", "",
        df_to_md(df_brier[df_brier["coin"] == "ALL"]), "",
    ]
    OUT_SUMMARY_MD.write_text("\n".join(lines), encoding="utf-8")

    dev = [
        "# Deviations from Specification (`deviations.md`)", "",
        "## 1. 本輪已完全修正的三項偏差（L1 / L2 / L3）",
        "- **L1（1 秒索引修正）**：Binance 1s klines 以 `open_time` 為索引，`open_time = s` 代表 `[s, s+1)`，收盤於 `s+1`。初版取 `idx_T + τ` 多偷看未來 1 秒，本版已改為 `idx_T + τ - 1`（`S0 = [T-60, T-1]`、`twap60 = [T+240, T+299]`、`A_255 = [T+240, T+254]`）。",
        "- **L2（`[τ+1, τ+5]` 成交修正）**：`prices-history (fidelity=1)` 約每 60s 抽樣一次，前照 `lag > 0` 在 `τ` 前數十秒定格會造成 Binance 領先膨脹。本版實取 `t ∈ [τ+1, τ+5]` 的第一個價格點作為成交價（模型嚴格只用 `<= τ`），並完整保存前照 `lag` 分組表供比對。",
        "- **L3（條件價差修正）**：已改用 `live_btc_5m.db` 在模型 `EV >= 3.5¢` 時刻實測的半價差分布；唯 ETH / SOL 在 `11/20` 降費後的真實 `ask_size` 仍待 Section 4.2 現場錄製驗證。", "",
        "## 2. L4 組合模擬暫緩說明",
        "- 舊版 `$43 -> $192` 組合模擬因基於修正前之 L1–L3 資料，依 Claude 回覆已標記為作廢/暫不採用；策略過關評估以 Section 4.2 即時 1Hz Top-5 錄製為準。", "",
        "## 3. R1 / R2 官方 Chainlink 60s TWAP 串接",
        "- 使用免費無 Key 的 RTDS `crypto_prices_twap_sixty`（與 PolyBolt `price.crypto.twap` 在重複時間戳最大差異 `4.4e-11 USD`），以 Gamma `eventMetadata` 回填 `live_btc_5m.db` 全數 `1,013/1,013` 窗達 **100.00%** 一致率。", "",
    ]
    OUT_DEVIATIONS_MD.write_text("\n".join(dev), encoding="utf-8")
    print(f"[4.1 L1+L2+L3] Done: raw={len(df):,}, l2_rows={len(df_l2):,}, causal_rows={len(df_l2_causal):,}")


if __name__ == "__main__":
    main()
