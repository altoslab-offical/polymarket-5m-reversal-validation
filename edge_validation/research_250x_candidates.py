#!/usr/bin/env python3
"""Track 2 Research: 2-Year IS/OOS Perpetual Screening & Polymarket 15m/1h Structural Probe.

Evaluates 3 distinct linear-perp strategy families across 755 days of 5m Binance data
(strict chronological 50/50 IS/OOS split, bar-i close signal -> bar-(i+1) open fill,
worst-case SL-before-TP intrabar resolution, 14 bps RT primary / 8 bps sensitivity)
and empirically probes live Polymarket 5m vs 15m vs 1h recurring markets ($0 free APIs).
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import math
import pathlib
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
import urllib.error
import urllib.request

import numpy as np
import pandas as pd

REPO = pathlib.Path(__file__).resolve().parent.parent
DATA_DIR = REPO / "data"
OUT_CSV = REPO / "edge_validation" / "research_250x_summary.csv"
OUT_MD = REPO / "edge_validation" / "NEW_STRATEGY_250X_RESEARCH.md"
SYMBOLS: tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
PRIMARY_COST_RT = 0.0014   # 14 bps round-trip (10 bps taker fee + 4 bps slippage)
SENS_COST_RT = 0.0008      # 8 bps round-trip (maker entry + taker exit)
ANN_DAYS = 365.0


@dataclass(frozen=True)
class PerpTrade:
    """Single closed perpetual futures trade with ATR stop-risk normalization."""

    symbol: str
    entry_ts_ms: int
    exit_ts_ms: int
    side: int  # +1 long, -1 short
    entry_px: float
    exit_px: float
    stop_pct: float
    gross_ret: float
    split: str  # "IS" | "OOS"


def _wilder_rsi(close: np.ndarray, period: int = 6) -> np.ndarray:
    """Vectorized Wilder RSI with exact zero look-ahead."""
    n = len(close)
    out = np.full(n, 50.0, dtype=float)
    if n <= period + 1:
        return out
    diff = np.diff(close, prepend=close[0])
    gain = np.where(diff > 0.0, diff, 0.0)
    loss = np.where(diff < 0.0, -diff, 0.0)
    avg_g = float(np.mean(gain[1 : period + 1]))
    avg_l = float(np.mean(loss[1 : period + 1]))
    alpha = 1.0 / period
    for i in range(period + 1, n):
        avg_g = (1.0 - alpha) * avg_g + alpha * float(gain[i])
        avg_l = (1.0 - alpha) * avg_l + alpha * float(loss[i])
        rs = avg_g / avg_l if avg_l > 1e-12 else 1e6
        out[i] = 100.0 - (100.0 / (1.0 + rs))
    return out


def _atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> np.ndarray:
    """Wilder ATR(14) computed strictly up to bar i."""
    prev_c = np.roll(close, 1)
    prev_c[0] = close[0]
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_c), np.abs(low - prev_c)))
    return pd.Series(tr).ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean().to_numpy(dtype=float)


def _resample_ohlcv(df5: pd.DataFrame, tf_min: int) -> pd.DataFrame:
    """Resample 5m bars deterministically onto exact closed `tf_min` buckets."""
    if tf_min == 5:
        df = df5.copy()
        df["tbr"] = (df["taker_buy_base"] / df["volume"].replace(0.0, np.nan)).fillna(0.5)
        return df.reset_index(drop=True)
    bucket_ms = tf_min * 60 * 1000
    g_key = (df5["open_time"].astype(np.int64) // bucket_ms) * bucket_ms
    agg = df5.groupby(g_key, sort=True).agg(
        open_time=("open_time", "first"),
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
        taker_buy_base=("taker_buy_base", "sum"),
        bars=("close", "count"),
    )
    need = tf_min // 5
    agg = agg[agg["bars"] == need].reset_index(drop=True)
    agg["tbr"] = (agg["taker_buy_base"] / agg["volume"].replace(0.0, np.nan)).fillna(0.5)
    return agg


def _macro_bull_mask_by_ts(btc_1h: pd.DataFrame) -> Dict[int, bool]:
    """Causal BTC 1h EMA(200) bullish state keyed by 1h close time (`open_time + 3600s`)."""
    ema200 = btc_1h["close"].ewm(span=200, adjust=False, min_periods=200).mean()
    out: Dict[int, bool] = {}
    for ot, cl, em in zip(btc_1h["open_time"], btc_1h["close"], ema200):
        if pd.notna(em):
            out[int(ot) + 3600_000] = bool(float(cl) > float(em))
    return out


def _lookup_macro_bull(ts_ms: np.ndarray, bull_by_close_ms: Dict[int, bool]) -> np.ndarray:
    """Map strictly already-closed BTC 1h trend state onto lower-timeframe open timestamps."""
    keys = np.array(sorted(bull_by_close_ms.keys()), dtype=np.int64)
    vals = np.array([bull_by_close_ms[k] for k in keys], dtype=bool)
    out = np.ones(len(ts_ms), dtype=bool)
    if len(keys) == 0:
        return out
    idx = np.searchsorted(keys, ts_ms, side="right") - 1
    valid = idx >= 0
    out[valid] = vals[idx[valid]]
    return out


def _simulate_atr_trades(
    df: pd.DataFrame,
    sig_dir: np.ndarray,
    atr_arr: np.ndarray,
    sl_mult: float,
    tp_mult: float,
    max_hold: int,
    symbol: str,
    split_ts_ms: int,
) -> List[PerpTrade]:
    """Execute signal[i] at open[i+1] with conservative SL-before-TP intrabar check."""
    opens = df["open"].to_numpy(dtype=float)
    highs = df["high"].to_numpy(dtype=float)
    lows = df["low"].to_numpy(dtype=float)
    closes = df["close"].to_numpy(dtype=float)
    ts_arr = df["open_time"].to_numpy(dtype=np.int64)
    n = len(df)
    trades: List[PerpTrade] = []
    i = 1
    while i < n - 2:
        sd = int(sig_dir[i])
        if sd == 0 or not np.isfinite(atr_arr[i]) or atr_arr[i] <= 0.0:
            i += 1
            continue
        ent_idx = i + 1
        ent_px = float(opens[ent_idx])
        atr_i = float(atr_arr[i])
        sl_dist = sl_mult * atr_i
        tp_dist = tp_mult * atr_i
        sl_px = ent_px - sd * sl_dist
        tp_px = ent_px + sd * tp_dist
        exit_idx = min(n - 1, ent_idx + max_hold - 1)
        exit_px = float(closes[exit_idx])
        for j in range(ent_idx, exit_idx + 1):
            hit_sl = (lows[j] <= sl_px) if sd > 0 else (highs[j] >= sl_px)
            hit_tp = (highs[j] >= tp_px) if sd > 0 else (lows[j] <= tp_px)
            if hit_sl:
                exit_idx, exit_px = j, sl_px
                break
            if hit_tp:
                exit_idx, exit_px = j, tp_px
                break
        gross = sd * ((exit_px - ent_px) / ent_px)
        stop_pct = max(1e-5, sl_dist / ent_px)
        split = "IS" if int(ts_arr[ent_idx]) < split_ts_ms else "OOS"
        trades.append(PerpTrade(symbol, int(ts_arr[ent_idx]), int(ts_arr[exit_idx]), sd, ent_px, exit_px, stop_pct, gross, split))
        i = exit_idx + 1
    return trades


def _gen_c1_squeeze_signals(
    df: pd.DataFrame, bull_map: Dict[int, bool], macro_gate: bool
) -> Tuple[np.ndarray, np.ndarray]:
    """C1: Volatility compression (`ATR14/SMA96 < 0.80`) + 20-bar breakout + Taker flow."""
    highs = df["high"].to_numpy(dtype=float)
    lows = df["low"].to_numpy(dtype=float)
    closes = df["close"].to_numpy(dtype=float)
    tbr = df["tbr"].to_numpy(dtype=float)
    ts_ms = df["open_time"].to_numpy(dtype=np.int64)
    atr14 = _atr(highs, lows, closes, 14)
    atr_sma = pd.Series(atr14).rolling(96, min_periods=96).mean().shift(1).to_numpy(dtype=float)
    atr_ratio = np.where(atr_sma > 0, atr14 / atr_sma, 1.0)
    squeeze_prev = pd.Series(atr_ratio < 0.80).rolling(4, min_periods=1).max().shift(1).fillna(0.0).to_numpy(dtype=float) > 0.5
    hi20 = pd.Series(highs).rolling(20, min_periods=20).max().shift(1).to_numpy(dtype=float)
    lo20 = pd.Series(lows).rolling(20, min_periods=20).min().shift(1).to_numpy(dtype=float)
    bull = _lookup_macro_bull(ts_ms, bull_map)
    long_sig = squeeze_prev & (closes > hi20) & (tbr >= 0.56) & (bull if macro_gate else True)
    short_sig = squeeze_prev & (closes < lo20) & (tbr <= 0.44) & ((~bull) if macro_gate else True)
    sig = np.where(long_sig, 1, np.where(short_sig, -1, 0))
    sig[:120] = 0
    return sig, atr14


def _gen_c2_climax_signals(
    df: pd.DataFrame, bull_map: Dict[int, bool], regime_filtered: bool
) -> Tuple[np.ndarray, np.ndarray]:
    """C2: RSI(6) + volume surge + taker exhaustion + rejection wick mean-reversion on perp."""
    opens = df["open"].to_numpy(dtype=float)
    highs = df["high"].to_numpy(dtype=float)
    lows = df["low"].to_numpy(dtype=float)
    closes = df["close"].to_numpy(dtype=float)
    vols = df["volume"].to_numpy(dtype=float)
    tbr = df["tbr"].to_numpy(dtype=float)
    ts_ms = df["open_time"].to_numpy(dtype=np.int64)
    atr14 = _atr(highs, lows, closes, 14)
    rsi6 = _wilder_rsi(closes, 6)
    vol_sma = pd.Series(vols).rolling(20, min_periods=20).mean().shift(1).to_numpy(dtype=float)
    vol_ratio = np.where(vol_sma > 0, vols / vol_sma, 0.0)
    rng = np.maximum(1e-12, highs - lows)
    l_wick = (np.minimum(opens, closes) - lows) / rng
    u_wick = (highs - np.maximum(opens, closes)) / rng
    bull = _lookup_macro_bull(ts_ms, bull_map)
    long_sig = (rsi6 <= 15.0) & (vol_ratio >= 1.5) & (tbr <= 0.44) & (l_wick >= 0.15) & (bull if regime_filtered else True)
    short_sig = (rsi6 >= 85.0) & (vol_ratio >= 1.5) & (tbr >= 0.56) & (u_wick >= 0.15) & ((~bull) if regime_filtered else True)
    sig = np.where(long_sig, 1, np.where(short_sig, -1, 0))
    sig[:60] = 0
    return sig, atr14


def _gen_c3_btc_lead_signals(
    df_alt: pd.DataFrame, df_btc: pd.DataFrame, contrarian: bool = False
) -> Tuple[pd.DataFrame, np.ndarray, np.ndarray]:
    """C3: BTC impulse bar with strong directional taker flow while ETH/SOL lag (< 0.6x)."""
    merged = pd.merge(
        df_alt,
        df_btc[["open_time", "open", "high", "low", "close", "tbr"]].rename(
            columns={"open": "b_open", "high": "b_high", "low": "b_low", "close": "b_close", "tbr": "b_tbr"}
        ),
        on="open_time", how="inner",
    ).sort_values("open_time").reset_index(drop=True)
    b_atr = _atr(merged["b_high"].to_numpy(float), merged["b_low"].to_numpy(float), merged["b_close"].to_numpy(float), 14)
    b_atr_prev = np.roll(b_atr, 1)
    b_atr_pct = np.where(merged["b_open"].to_numpy(float) > 0, b_atr_prev / merged["b_open"].to_numpy(float), 1.0)
    b_ret = (merged["b_close"].to_numpy(float) - merged["b_open"].to_numpy(float)) / np.maximum(1e-9, merged["b_open"].to_numpy(float))
    a_ret = (merged["close"].to_numpy(float) - merged["open"].to_numpy(float)) / np.maximum(1e-9, merged["open"].to_numpy(float))
    b_tbr = merged["b_tbr"].to_numpy(float)
    a_atr = _atr(merged["high"].to_numpy(float), merged["low"].to_numpy(float), merged["close"].to_numpy(float), 14)

    up_imp = (b_ret >= 2.0 * b_atr_pct) & (b_tbr >= 0.58) & (a_ret < 0.6 * b_ret)
    dn_imp = (b_ret <= -2.0 * b_atr_pct) & (b_tbr <= 0.42) & (a_ret > 0.6 * b_ret)
    raw = np.where(up_imp, 1, np.where(dn_imp, -1, 0))
    sig = -raw if contrarian else raw
    sig[:60] = 0
    return merged, sig, a_atr


def _evaluate_split_stats(trades: List[PerpTrade], span_days: float, cost_rt: float) -> dict[str, Any]:
    """Compute WR, PF, Expectancy R, Daily Sharpe (at 1% fixed risk/trade), and MaxDD%."""
    if not trades:
        return {"n": 0, "per_day": 0.0, "wr_pct": None, "pf": None, "exp_r": None, "mean_bps": None, "sharpe": None, "max_dd_pct": 0.0}
    r_mults = np.array([(t.gross_ret - cost_rt) / t.stop_pct for t in trades], dtype=float)
    net_bps = np.array([(t.gross_ret - cost_rt) * 10000.0 for t in trades], dtype=float)
    wins = r_mults[r_mults > 0.0]
    losses = r_mults[r_mults < 0.0]
    pf = float(wins.sum() / abs(losses.sum())) if len(losses) and abs(losses.sum()) > 1e-12 else (99.0 if len(wins) else 0.0)

    # Constant 1% risk per trade equity & daily calendar Sharpe
    risk_f = 0.01
    day_rets: Dict[int, float] = {}
    eq = 1.0
    peak = 1.0
    max_dd = 0.0
    for t, r_val in sorted(zip(trades, r_mults), key=lambda x: x[0].exit_ts_ms):
        pnl_pct = risk_f * float(r_val)
        day_k = int(t.exit_ts_ms // 86_400_000)
        day_rets[day_k] = day_rets.get(day_k, 0.0) + pnl_pct
        eq = max(0.0, eq * (1.0 + pnl_pct))
        peak = max(peak, eq)
        max_dd = max(max_dd, (peak - eq) / peak if peak > 0 else 0.0)
    n_days = max(1, int(round(span_days)))
    min_d = min(day_rets.keys())
    daily_series = np.array([day_rets.get(min_d + k, 0.0) for k in range(n_days)], dtype=float)
    sd_d = float(np.std(daily_series, ddof=1)) if len(daily_series) >= 2 else 0.0
    sharpe = (float(np.mean(daily_series)) / sd_d) * math.sqrt(ANN_DAYS) if sd_d > 1e-12 else 0.0
    return {
        "n": len(trades), "per_day": round(len(trades) / max(span_days, 1.0), 2),
        "wr_pct": round(float((r_mults > 0.0).mean()) * 100.0, 2),
        "pf": round(pf, 3), "exp_r": round(float(np.mean(r_mults)), 4),
        "mean_bps": round(float(np.mean(net_bps)), 2),
        "sharpe": round(sharpe, 2), "max_dd_pct": round(max_dd * 100.0, 2),
    }


def _mc_250x(trades_oos: List[PerpTrade], span_days: float, kelly_mult: float, cost_rt: float = PRIMARY_COST_RT, n_paths: int = 10000) -> dict[str, Any]:
    """Bootstrap 10,000 paths ($40 -> $10,000, max 10x leverage, $5 min notional) over 180 days."""
    if not trades_oos:
        return {"kelly_mult": kelly_mult, "p_hit_10k_pct": 0.0, "p50_days": None, "p_dd50_pct": 0.0}
    rng = np.random.default_rng(42 + int(round(kelly_mult * 100)))
    r_arr = np.array([(t.gross_ret - cost_rt) / t.stop_pct for t in trades_oos], dtype=float)
    stop_arr = np.array([t.stop_pct for t in trades_oos], dtype=float)
    p_win = float((r_arr > 0.0).mean())
    avg_win = float(r_arr[r_arr > 0.0].mean()) if (r_arr > 0.0).any() else 0.0
    avg_loss = float(abs(r_arr[r_arr < 0.0].mean())) if (r_arr < 0.0).any() else 1.0
    full_kelly = max(0.0, (p_win / max(avg_loss, 1e-6)) - ((1.0 - p_win) / max(avg_win, 1e-6))) if avg_win > 0 else 0.0
    risk_frac = min(0.05, max(0.005, full_kelly * kelly_mult))
    trades_per_day = max(0.5, len(trades_oos) / max(span_days, 1.0))
    n_trades_180d = max(1, int(round(trades_per_day * 180.0)))

    hit_10k = 0
    dd50 = 0
    hit_days: List[float] = []
    for _ in range(n_paths):
        eq = 40.0
        peak = 40.0
        max_dd = 0.0
        idxs = rng.integers(0, len(r_arr), size=n_trades_180d)
        hit_at: Optional[float] = None
        for step_i, idx in enumerate(idxs, start=1):
            notional = min(eq * 10.0, (eq * risk_frac) / max(1e-4, stop_arr[idx]))
            if notional < 5.0:
                continue
            pnl = notional * stop_arr[idx] * r_arr[idx]
            eq = max(0.0, eq + pnl)
            peak = max(peak, eq)
            max_dd = max(max_dd, (peak - eq) / peak if peak > 0 else 0.0)
            if eq < 2.0:
                break
            if eq >= 10000.0 and hit_at is None:
                hit_at = step_i / trades_per_day
                break
        if hit_at is not None:
            hit_10k += 1
            hit_days.append(hit_at)
        if max_dd >= 0.50:
            dd50 += 1
    return {
        "kelly_mult": kelly_mult, "risk_frac_pct": round(risk_frac * 100.0, 2),
        "p_hit_10k_pct": round(hit_10k / n_paths * 100.0, 2),
        "p50_days": round(float(np.median(hit_days)), 1) if hit_days else None,
        "p_dd50_pct": round(dd50 / n_paths * 100.0, 2),
    }


def _http_get_json(url: str) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            return json.loads(r.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        return {"_error": str(exc)}


def probe_polymarket_non5m() -> List[dict[str, Any]]:
    """Probe live Polymarket 5m vs 15m vs 1h recurring markets (`$0` public Gamma/CLOB APIs)."""
    now = int(time.time())
    targets = [
        ("5m", "btc", (now // 300) * 300), ("5m", "eth", (now // 300) * 300), ("5m", "sol", (now // 300) * 300),
        ("15m", "btc", (now // 900) * 900), ("15m", "eth", (now // 900) * 900), ("15m", "sol", (now // 900) * 900),
        ("1h", "btc", (now // 3600) * 3600), ("1h", "eth", (now // 3600) * 3600), ("1h", "sol", (now // 3600) * 3600),
    ]
    rows: List[dict[str, Any]] = []
    for tf, coin, bucket in targets:
        slug = f"{coin}-updown-{tf}-{bucket}"
        m = _http_get_json(f"https://gamma-api.polymarket.com/markets/slug/{slug}")
        if not isinstance(m, dict) or "_error" in m or not m.get("clobTokenIds"):
            rows.append({"tf": tf, "coin": coin.upper(), "slug": slug, "active": False, "depth_2c_usd": 0.0})
            continue
        tids = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
        up_tid = str(tids[0]) if tids else ""
        bk = _http_get_json(f"https://clob.polymarket.com/book?token_id={up_tid}") if up_tid else {}
        fee = _http_get_json(f"https://clob.polymarket.com/fee-rate?token_id={up_tid}") if up_tid else {}
        asks = sorted(((float(a["price"]), float(a["size"])) for a in (bk.get("asks") or [])), key=lambda x: x[0]) if isinstance(bk, dict) else []
        top_a = asks[0][0] if asks else None
        depth_2c = sum(px * sz for px, sz in asks if top_a is not None and px <= top_a + 0.0201)
        desc = str(m.get("description") or "").replace("\n", " ")
        twap_rule = "60s_TWAP" if "60" in desc or "last 60" in desc.lower() else ("FULL_RANGE_TWAP" if "time-weighted average price" in desc.lower() else desc[:80])
        rows.append({
            "tf": tf, "coin": coin.upper(), "slug": slug, "active": True,
            "volume_usd": round(float(m.get("volumeNum") or 0.0), 2),
            "liquidity_usd": round(float(m.get("liquidityNum") or 0.0), 2),
            "up_top_ask": top_a, "depth_2c_usd": round(depth_2c, 2),
            "base_fee": fee.get("base_fee") if isinstance(fee, dict) else None,
            "rule_snippet": desc[:140], "twap_rule": twap_rule,
        })
    return rows


def _run_candidate_pool(
    candidates: List[Tuple[str, Callable[[], List[PerpTrade]]]],
    is_days: float,
    oos_days: float,
) -> Tuple[List[dict[str, Any]], Dict[str, List[PerpTrade]]]:
    """Evaluate every strategy across IS/OOS splits and return summary rows + OOS trade pools."""
    summary_rows: List[dict[str, Any]] = []
    oos_pool: Dict[str, List[PerpTrade]] = {}
    for name, fn in candidates:
        trades = fn()
        tr_is = [t for t in trades if t.split == "IS"]
        tr_oos = [t for t in trades if t.split == "OOS"]
        oos_pool[name] = tr_oos
        s_is = _evaluate_split_stats(tr_is, is_days, PRIMARY_COST_RT)
        s_oos = _evaluate_split_stats(tr_oos, oos_days, PRIMARY_COST_RT)
        s_oos_maker = _evaluate_split_stats(tr_oos, oos_days, SENS_COST_RT)
        gate_pass = bool(
            s_oos["n"] >= 200 and (s_oos["pf"] or 0.0) >= 1.25 and (s_oos["sharpe"] or -99.0) >= 1.2
            and s_oos["max_dd_pct"] < 30.0 and (s_is["exp_r"] or -1.0) > 0.0 and (s_oos["exp_r"] or -1.0) > 0.0
        )
        summary_rows.append({
            "strategy": name, "gate": "PASS" if gate_pass else "REJECTED (OOS FAIL)",
            "is_n": s_is["n"], "is_wr": s_is["wr_pct"], "is_pf": s_is["pf"], "is_exp_r": s_is["exp_r"], "is_sharpe": s_is["sharpe"],
            "oos_n": s_oos["n"], "oos_per_day": s_oos["per_day"], "oos_wr": s_oos["wr_pct"], "oos_pf": s_oos["pf"],
            "oos_exp_r": s_oos["exp_r"], "oos_bps": s_oos["mean_bps"], "oos_sharpe": s_oos["sharpe"], "oos_max_dd_pct": s_oos["max_dd_pct"],
            "oos_8bps_pf": s_oos_maker["pf"], "oos_8bps_sharpe": s_oos_maker["sharpe"],
        })
    return summary_rows, oos_pool


def run_screening() -> dict[str, Any]:
    """Run all perp strategy candidates across 2-year IS/OOS splits plus Polymarket probe."""
    raw5 = {
        s: _resample_ohlcv(pd.read_parquet(DATA_DIR / f"{s}_5m.parquet").sort_values("open_time").reset_index(drop=True), 5)
        for s in SYMBOLS
    }
    raw15 = {s: _resample_ohlcv(raw5[s], 15) for s in SYMBOLS}
    raw1h = {s: _resample_ohlcv(raw5[s], 60) for s in SYMBOLS}
    bull_map = _macro_bull_mask_by_ts(raw1h["BTCUSDT"])
    t_min = int(raw5["BTCUSDT"]["open_time"].iloc[0])
    t_max = int(raw5["BTCUSDT"]["open_time"].iloc[-1])
    split_ms = (t_min + t_max) // 2
    is_days = (split_ms - t_min) / 86_400_000.0
    oos_days = (t_max - split_ms) / 86_400_000.0

    def _pool(tf_map: Dict[str, pd.DataFrame], gen_fn: Callable[[pd.DataFrame], Tuple[np.ndarray, np.ndarray]], sl_m: float, tp_m: float, hold: int) -> List[PerpTrade]:
        all_t: List[PerpTrade] = []
        for s in SYMBOLS:
            sig, atr_a = gen_fn(tf_map[s])
            all_t.extend(_simulate_atr_trades(tf_map[s], sig, atr_a, sl_m, tp_m, hold, s, split_ms))
        return all_t

    def _c3_run(tf_map: Dict[str, pd.DataFrame], hold: int, contrarian: bool) -> List[PerpTrade]:
        all_t: List[PerpTrade] = []
        for s in ("ETHUSDT", "SOLUSDT"):
            df_m, sig, atr_a = _gen_c3_btc_lead_signals(tf_map[s], tf_map["BTCUSDT"], contrarian=contrarian)
            all_t.extend(_simulate_atr_trades(df_m, sig, atr_a, 1.5, 2.5, hold, s, split_ms))
        return all_t

    candidates: List[Tuple[str, Callable[[], List[PerpTrade]]]] = [
        ("C1_15m_Squeeze_Breakout_Macro", lambda: _pool(raw15, lambda d: _gen_c1_squeeze_signals(d, bull_map, True), 2.0, 4.0, 48)),
        ("C1_15m_Squeeze_Breakout_AllDir", lambda: _pool(raw15, lambda d: _gen_c1_squeeze_signals(d, bull_map, False), 2.0, 4.0, 48)),
        ("C1_1h_Squeeze_Breakout_Macro", lambda: _pool(raw1h, lambda d: _gen_c1_squeeze_signals(d, bull_map, True), 2.0, 4.0, 48)),
        ("C2_5m_Climax_MR_All", lambda: _pool(raw5, lambda d: _gen_c2_climax_signals(d, bull_map, False), 1.5, 2.5, 12)),
        ("C2_15m_Climax_MR_All", lambda: _pool(raw15, lambda d: _gen_c2_climax_signals(d, bull_map, False), 1.5, 2.5, 12)),
        ("C2_15m_Climax_MR_MacroPullback", lambda: _pool(raw15, lambda d: _gen_c2_climax_signals(d, bull_map, True), 1.5, 2.5, 12)),
        ("C3_5m_BTC_Lead_Follow", lambda: _c3_run(raw5, 6, False)),
        ("C3_15m_BTC_Lead_Follow", lambda: _c3_run(raw15, 4, False)),
        ("C3_5m_BTC_Lead_Fade", lambda: _c3_run(raw5, 6, True)),
    ]
    summary_rows, oos_pool = _run_candidate_pool(candidates, is_days, oos_days)
    pd.DataFrame(summary_rows).to_csv(OUT_CSV, index=False)

    passed = [r["strategy"] for r in summary_rows if r["gate"] == "PASS"]
    best_name = max(summary_rows, key=lambda r: r["oos_sharpe"] if r["oos_sharpe"] is not None else -999.0)["strategy"]
    mc_targets = passed if passed else [best_name]
    mc_results = [
        {"strategy": tn, "tag": "PASS" if tn in passed else "REJECTED - Reference Only",
         "k025": _mc_250x(oos_pool[tn], oos_days, 0.25), "k050": _mc_250x(oos_pool[tn], oos_days, 0.50)}
        for tn in mc_targets
    ]
    poly_probe = probe_polymarket_non5m()
    _write_research_md(summary_rows, mc_results, poly_probe, (is_days, oos_days, t_min, split_ms, t_max))
    return {"summary_rows": summary_rows, "mc_results": mc_results, "poly_probe_count": len(poly_probe), "report_path": str(OUT_MD)}


def _write_research_md(
    summary_rows: List[dict[str, Any]],
    mc_results: List[dict[str, Any]],
    poly_probe: List[dict[str, Any]],
    span_info: Tuple[float, float, int, int, int],
) -> None:
    is_days, oos_days, t_min, split_ms, t_max = span_info
    lines = [
        "# Track 2：`\\$40U → \\$10,000U`（250x）替代賽道探索與兩年 Walk-Forward 篩選",
        "",
        f"> **數據區間**：`data/{{BTC,ETH,SOL}}USDT_5m.parquet`（`217,650` 根/幣 ≈ `{is_days + oos_days:.1f}` 天） | IS 前半（`{is_days:.1f}` 天） vs OOS 後半（`{oos_days:.1f}` 天）",
        "> **執行紀律**：Bar `i` 收盤產生訊號 → Bar `i+1` 開盤進場（零前視）；單根同時觸及 SL/TP 視為 **先損後賺**；主口徑來回成本 **`14 bps`**（10 bps taker + 4 bps 滑價），另附 `8 bps` 敏感度。",
        "> **Pass Gate**：`OOS n >= 200`、`OOS PF >= 1.25`、`OOS Sharpe >= 1.2`、`OOS MaxDD < 30%`、IS 與 OOS 均正期望值。",
        "",
        "## 1. Part A：線性合約三大策略家族 2 年嚴格 IS/OOS 篩選結果",
        "",
        "| Strategy | Gate | IS n | IS PF | IS Sharpe | OOS n | OOS /d | OOS WR% | OOS PF | OOS Exp(R) | OOS bps | OOS Sharpe | OOS MaxDD% | OOS @8bps PF/Sharpe |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for r in summary_rows:
        lines.append(
            f"| `{r['strategy']}` | **{r['gate']}** | {r['is_n']} | {r['is_pf']} | {r['is_sharpe']} | "
            f"{r['oos_n']} | {r['oos_per_day']} | {r['oos_wr']}% | {r['oos_pf']} | {r['oos_exp_r']} | "
            f"{r['oos_bps']} | {r['oos_sharpe']} | {r['oos_max_dd_pct']}% | {r['oos_8bps_pf']} / {r['oos_8bps_sharpe']} |"
        )
    lines += [
        "",
        "## 2. `$40U → $10,000U` 180 天分數凱利 Monte Carlo（`10,000` 路徑，上限 10x 槓桿，最小名目 `$5U`）",
        "",
        "| Strategy | Status | Kelly 倍數 (Risk%/單) | 180天達 `$10k` 機率 | 達標中位天數 (P50) | 回撤 >=50% 機率 |",
        "|---|---|---|---:|---:|---:|",
    ]
    for m in mc_results:
        for k_key in ("k025", "k050"):
            kd = m[k_key]
            lines.append(
                f"| `{m['strategy']}` | `{m['tag']}` | `{kd['kelly_mult']}x` (`{kd['risk_frac_pct']}%`) | "
                f"**{kd['p_hit_10k_pct']}%** | {kd['p50_days']} 天 | {kd['p_dd50_pct']}% |"
            )
    lines += [
        "",
        "## 3. Part B：Polymarket 非 5m 加密盤口即時探測（`5m` vs `15m` vs `1h`）",
        "",
        "> 即時呼叫公開免費 Gamma/CLOB API，比較 5m 與 15m/1h 的 `+2¢` 盤口深度、費率與 TWAP 結算規則。",
        "",
        "| TF | Coin | Active | 當前 Volume | Liquidity | Up Ask | `+2¢` Ask Depth (USD) | `base_fee` | TWAP Rule |",
        "|---|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for p in poly_probe:
        lines.append(
            f"| `{p['tf']}` | `{p['coin']}` | {p['active']} | ${p.get('volume_usd', 0):.0f} | ${p.get('liquidity_usd', 0):.0f} | "
            f"{p.get('up_top_ask')} | **${p.get('depth_2c_usd', 0):.2f}** | `{p.get('base_fee')}` | `{p.get('twap_rule', '')}` |"
        )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    print(json.dumps(run_screening(), indent=2))
