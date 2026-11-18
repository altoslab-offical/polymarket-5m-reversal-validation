#!/usr/bin/env python3
"""
Download, cache, and compute Binance Spot (1m, 1s) + Futures UM OI (5m metrics)
features aligned strictly without look-ahead to Polymarket 5m windows.

Warmup range: 2026-08-07T00:00:00Z (T=1786060800) .. 2026-11-18T02:05:00Z
Output range: T in [1786665600 (2026-08-14T00:00:00Z), 1794967200 (2026-11-18T02:00:00Z)]
"""
from __future__ import annotations

import asyncio
import datetime
import io
import json
import pathlib
import sys
import zipfile
from typing import Dict, List, Tuple

import httpx
import numpy as np
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent
CACHE_DIR = ROOT / "cache"
RAW_DIR = CACHE_DIR / "raw_binance"
RAW_DIR.mkdir(parents=True, exist_ok=True)

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
COIN_SHORT = {"BTCUSDT": "BTC", "ETHUSDT": "ETH", "SOLUSDT": "SOL"}

T_WARMUP_START = 1786060800  # 2026-08-07T00:00:00Z (7d before T_START)
T_START = 1786665600         # 2026-08-14T00:00:00Z (60s TWAP effective)
T_END = 1794967200           # 2026-11-18T02:00:00Z (latest settled window at validation start)

MONTHS_1M = ["2026-08", "2026-09", "2026-10"]
MONTHS_1S = ["2026-08", "2026-09", "2026-10"]
VISION_DAILY_START = datetime.date(2026, 11, 1)
VISION_DAILY_END = datetime.date(2026, 11, 16)
METRICS_START = datetime.date(2026, 8, 6)
METRICS_END = datetime.date(2026, 11, 16)

REST_1M_START_TS = 1794873600  # 2026-11-17T00:00:00Z
REST_END_TS = T_END + 300      # 1794967500 (includes [T_END, T_END+300) for bn_next_up)


def _norm_ts_sec(ts_raw: np.ndarray) -> np.ndarray:
    """Convert Binance Vision microsecond (>=1e14) or millisecond (>=1e11) timestamps to UTC integer seconds."""
    ts = ts_raw.astype(np.int64)
    if len(ts) == 0:
        return ts
    mx = int(ts.max())
    if mx >= 10**14:
        return ts // 1_000_000
    if mx >= 10**11:
        return ts // 1_000
    return ts


async def fetch_with_retry(
    client: httpx.AsyncClient,
    url: str,
    sem: asyncio.Semaphore,
    allow_404: bool = False,
) -> bytes | None:
    for attempt in range(6):
        try:
            async with sem:
                resp = await client.get(url, timeout=120.0)
            if resp.status_code == 200:
                return resp.content
            if allow_404 and resp.status_code == 404:
                return None
            if resp.status_code in (429, 418, 500, 502, 503, 504):
                await asyncio.sleep(min(2.0 ** attempt, 15.0))
                continue
            raise RuntimeError(f"HTTP {resp.status_code} for {url}: {resp.text[:200]}")
        except (httpx.HTTPError, asyncio.TimeoutError) as exc:
            if attempt == 5:
                raise RuntimeError(f"Failed {url} after retries: {exc}") from exc
            await asyncio.sleep(min(1.5 * (attempt + 1), 10.0))
    return None


async def load_1m_symbol(client: httpx.AsyncClient, sem: asyncio.Semaphore, sym: str) -> pd.DataFrame:
    out_pq = CACHE_DIR / f"raw_1m_{sym}.parquet"
    if out_pq.exists():
        return pd.read_parquet(out_pq)

    urls: List[str] = []
    for m in MONTHS_1M:
        urls.append(f"https://data.binance.vision/data/spot/monthly/klines/{sym}/1m/{sym}-1m-{m}.zip")
    d = VISION_DAILY_START
    while d <= VISION_DAILY_END:
        urls.append(f"https://data.binance.vision/data/spot/daily/klines/{sym}/1m/{sym}-1m-{d.isoformat()}.zip")
        d += datetime.timedelta(days=1)

    contents = await asyncio.gather(*(fetch_with_retry(client, u, sem) for u in urls))
    frames: List[pd.DataFrame] = []
    cols = ["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_vol", "trades", "taker_buy_base", "taker_buy_quote", "ignore"]
    usecols = [0, 1, 2, 3, 4, 5, 9]
    for content in contents:
        assert content is not None
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            with zf.open(zf.namelist()[0]) as f:
                df_part = pd.read_csv(f, header=None, usecols=usecols, names=[cols[i] for i in usecols])
        df_part["ts"] = _norm_ts_sec(df_part["open_time"].to_numpy())
        df_part = df_part[(df_part["ts"] >= T_WARMUP_START - 600) & (df_part["ts"] < REST_1M_START_TS)]
        if not df_part.empty:
            frames.append(df_part[["ts", "open", "high", "low", "close", "volume", "taker_buy_base"]])

    # Fetch REST 1m tail [REST_1M_START_TS, REST_END_TS)
    cur_ms = REST_1M_START_TS * 1000
    end_ms = REST_END_TS * 1000 - 1
    rest_rows = []
    while cur_ms <= end_ms:
        u = f"https://api.binance.com/api/v3/klines?symbol={sym}&interval=1m&startTime={cur_ms}&endTime={end_ms}&limit=1000"
        b = await fetch_with_retry(client, u, sem)
        arr = json.loads(b.decode())
        if not arr:
            break
        for r in arr:
            rest_rows.append((
                int(r[0]) // 1000,
                float(r[1]),
                float(r[2]),
                float(r[3]),
                float(r[4]),
                float(r[5]),
                float(r[9]),
            ))
        next_ms = int(arr[-1][0]) + 60_000
        if next_ms <= cur_ms:
            break
        cur_ms = next_ms
    if rest_rows:
        frames.append(pd.DataFrame(rest_rows, columns=["ts", "open", "high", "low", "close", "volume", "taker_buy_base"]))

    df = pd.concat(frames, ignore_index=True)
    for col in ("open", "high", "low", "close", "volume", "taker_buy_base"):
        df[col] = df[col].astype(np.float64)
    df = df.drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)
    df.to_parquet(out_pq, index=False)
    print(f"[1m] {sym}: {len(df):,} rows cached ({df['ts'].min()} .. {df['ts'].max()})", flush=True)
    return df


async def _fetch_1s_zip_twap(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    url: str,
    min_open_ts: int,
    max_open_ts: int,
) -> Dict[int, Tuple[float, int]]:
    """Download one 1s zip and extract sum_close + count for seconds in [T-60, T) (i.e. 5m open_ts % 300 >= 240)."""
    content = await fetch_with_retry(client, url, sem)
    assert content is not None
    # Process in chunks to keep RAM modest even on 70MB zips
    out: Dict[int, Tuple[float, int]] = {}
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        with zf.open(zf.namelist()[0]) as f:
            for chunk in pd.read_csv(f, header=None, usecols=[0, 4], names=["open_time", "close"], chunksize=500_000):
                ts = _norm_ts_sec(chunk["open_time"].to_numpy())
                c = chunk["close"].to_numpy(dtype=np.float64)
                mask = (ts >= min_open_ts) & (ts < max_open_ts) & ((ts % 300) >= 240)
                if not np.any(mask):
                    continue
                ts_m = ts[mask]
                c_m = c[mask]
                # bar_T is the upcoming Polymarket window start T = ((ts_m // 300) + 1) * 300
                bar_T = ((ts_m // 300) + 1) * 300
                # Aggregate per bar_T inside chunk
                uniq, idx = np.unique(bar_T, return_inverse=True)
                sums = np.bincount(idx, weights=c_m)
                cnts = np.bincount(idx)
                for k, s_val, n_val in zip(uniq, sums, cnts):
                    ki = int(k)
                    prev_s, prev_n = out.get(ki, (0.0, 0))
                    out[ki] = (prev_s + float(s_val), prev_n + int(n_val))
    return out


async def load_1s_twap_symbol(client: httpx.AsyncClient, sem_zip: asyncio.Semaphore, sem_rest: asyncio.Semaphore, sym: str) -> pd.DataFrame:
    out_pq = CACHE_DIR / f"raw_twap60_1s_{sym}.parquet"
    if out_pq.exists():
        return pd.read_parquet(out_pq)

    urls: List[str] = []
    for m in MONTHS_1S:
        urls.append(f"https://data.binance.vision/data/spot/monthly/klines/{sym}/1s/{sym}-1s-{m}.zip")
    d = VISION_DAILY_START
    while d <= VISION_DAILY_END:
        urls.append(f"https://data.binance.vision/data/spot/daily/klines/{sym}/1s/{sym}-1s-{d.isoformat()}.zip")
        d += datetime.timedelta(days=1)

    # Bounded download+parse across zips
    agg: Dict[int, Tuple[float, int]] = {}
    results = await asyncio.gather(*(_fetch_1s_zip_twap(client, sem_zip, u, T_START - 300, REST_1M_START_TS) for u in urls))
    for part in results:
        for k, (s_val, n_val) in part.items():
            ps, pn = agg.get(k, (0.0, 0))
            agg[k] = (ps + s_val, pn + n_val)

    # Fetch REST 1s only for the tail [REST_1M_START_TS, T_END] (strictly [T-60, T) per 5m window)
    # Query exact 60s window [T-60, T) per window: 313 windows -> fast with Semaphore(20)
    tail_Ts = list(range(REST_1M_START_TS, T_END + 1, 300))

    async def fetch_tail_1s(T_val: int) -> Tuple[int, float, int]:
        st_ms = (T_val - 60) * 1000
        en_ms = T_val * 1000 - 1
        u = f"https://api.binance.com/api/v3/klines?symbol={sym}&interval=1s&startTime={st_ms}&endTime={en_ms}&limit=60"
        b = await fetch_with_retry(client, u, sem_rest)
        arr = json.loads(b.decode())
        if not arr:
            return T_val, 0.0, 0
        closes = [float(r[4]) for r in arr if (T_val - 60) <= (int(r[0]) // 1000) < T_val]
        return T_val, float(sum(closes)), len(closes)

    tail_res = await asyncio.gather(*(fetch_tail_1s(tv) for tv in tail_Ts))
    for T_val, s_val, n_val in tail_res:
        if n_val > 0:
            ps, pn = agg.get(T_val, (0.0, 0))
            agg[T_val] = (ps + s_val, pn + n_val)

    rows = [
        (k, s_val / n_val, n_val)
        for k, (s_val, n_val) in sorted(agg.items())
        if n_val > 0
    ]
    df = pd.DataFrame(rows, columns=["T", "twap60_1s", "n_1s"])
    df.to_parquet(out_pq, index=False)
    print(f"[1s-twap] {sym}: {len(df):,} windows cached (min n_1s={df['n_1s'].min() if len(df) else 0})", flush=True)
    return df


async def load_oi_symbol(client: httpx.AsyncClient, sem: asyncio.Semaphore, sym: str) -> pd.DataFrame:
    out_pq = CACHE_DIR / f"raw_oi_{sym}.parquet"
    if out_pq.exists():
        return pd.read_parquet(out_pq)

    urls: List[str] = []
    d = METRICS_START
    while d <= METRICS_END:
        urls.append(f"https://data.binance.vision/data/futures/um/daily/metrics/{sym}/{sym}-metrics-{d.isoformat()}.zip")
        d += datetime.timedelta(days=1)

    contents = await asyncio.gather(*(fetch_with_retry(client, u, sem, allow_404=True) for u in urls))
    oi_map: Dict[int, float] = {}
    for content in contents:
        if content is None:
            continue
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            with zf.open(zf.namelist()[0]) as f:
                df_m = pd.read_csv(f, usecols=["create_time", "sum_open_interest"])
        # Note: create_time in CSV is NOT monotonically sorted -> parse UTC explicitly in integer seconds
        # Pandas 3.0 defaults to datetime64[us, UTC], so always normalize unit via .dt.as_unit("s")!
        ts_sec = pd.to_datetime(df_m["create_time"], utc=True).dt.as_unit("s").astype("int64").to_numpy()
        vals = df_m["sum_open_interest"].to_numpy(dtype=np.float64)
        for t, v in zip(ts_sec, vals):
            if np.isfinite(v) and v > 0:
                oi_map[int(t)] = float(v)

    # Fill Nov 17..18 via fapi openInterestHist (last 500 x 5m points covers ~41.6h from 2026-11-16T09:00Z to 2026-11-18T02:35Z)
    b = await fetch_with_retry(client, f"https://fapi.binance.com/futures/data/openInterestHist?symbol={sym}&period=5m&limit=500", sem)
    for row in json.loads(b.decode()):
        ts = int(row["timestamp"]) // 1000
        val = float(row["sumOpenInterest"])
        if np.isfinite(val) and val > 0:
            oi_map[ts] = val

    df = pd.DataFrame(sorted(oi_map.items()), columns=["ts", "sum_open_interest"])
    assert len(df) > 25_000 and int(df["ts"].min()) >= 1_700_000_000, f"Invalid OI range for {sym}: len={len(df)}, min={df['ts'].min()}"
    df.to_parquet(out_pq, index=False)
    print(f"[OI] {sym}: {len(df):,} 5m rows cached ({df['ts'].min()} .. {df['ts'].max()})", flush=True)
    return df


def compute_wilder_rsi(close: np.ndarray, period: int) -> np.ndarray:
    n = len(close)
    rsi = np.full(n, np.nan, dtype=np.float64)
    if n <= period:
        return rsi
    delta = np.diff(close, prepend=np.nan)
    gains = np.where(delta > 0, delta, 0.0)
    losses = np.where(delta < 0, -delta, 0.0)

    avg_gain = np.mean(gains[1 : period + 1])
    avg_loss = np.mean(losses[1 : period + 1])
    alpha = 1.0 / period
    one_minus = 1.0 - alpha

    for i in range(period, n):
        if i > period:
            avg_gain = avg_gain * one_minus + gains[i] * alpha
            avg_loss = avg_loss * one_minus + losses[i] * alpha
        if avg_loss == 0.0:
            rsi[i] = 100.0 if avg_gain > 0.0 else 50.0
        else:
            rs = avg_gain / avg_loss
            rsi[i] = 100.0 - (100.0 / (1.0 + rs))
    return rsi


def compute_wilder_atr_and_adx(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> Tuple[np.ndarray, np.ndarray]:
    n = len(close)
    atr = np.full(n, np.nan, dtype=np.float64)
    adx = np.full(n, np.nan, dtype=np.float64)
    if n <= 2 * period:
        return atr, adx

    prev_c = np.roll(close, 1)
    prev_h = np.roll(high, 1)
    prev_l = np.roll(low, 1)

    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_c), np.abs(low - prev_c)))
    tr[0] = np.nan

    up_move = high - prev_h
    dn_move = prev_l - low
    plus_dm = np.where((up_move > dn_move) & (up_move > 0.0), up_move, 0.0)
    minus_dm = np.where((dn_move > up_move) & (dn_move > 0.0), dn_move, 0.0)
    plus_dm[0] = np.nan
    minus_dm[0] = np.nan

    sm_tr = float(np.mean(tr[1 : period + 1]))
    sm_pdm = float(np.mean(plus_dm[1 : period + 1]))
    sm_mdm = float(np.mean(minus_dm[1 : period + 1]))

    alpha = 1.0 / period
    one_minus = 1.0 - alpha

    dx = np.full(n, np.nan, dtype=np.float64)
    for i in range(period, n):
        if i > period:
            sm_tr = sm_tr * one_minus + tr[i] * alpha
            sm_pdm = sm_pdm * one_minus + plus_dm[i] * alpha
            sm_mdm = sm_mdm * one_minus + minus_dm[i] * alpha
        atr[i] = sm_tr
        if sm_tr > 0.0:
            pdi = 100.0 * (sm_pdm / sm_tr)
            mdi = 100.0 * (sm_mdm / sm_tr)
            denom = pdi + mdi
            dx[i] = 100.0 * abs(pdi - mdi) / denom if denom > 0.0 else 0.0
        else:
            dx[i] = 0.0

    # Seed ADX at index 2*period - 1 using mean of dx[period : 2*period]
    seed_end = 2 * period
    cur_adx = float(np.mean(dx[period:seed_end]))
    adx[seed_end - 1] = cur_adx
    for i in range(seed_end, n):
        cur_adx = cur_adx * one_minus + dx[i] * alpha
        adx[i] = cur_adx

    return atr, adx


def build_symbol_features(
    sym: str,
    df_1m: pd.DataFrame,
    df_1s: pd.DataFrame,
    df_oi: pd.DataFrame,
) -> pd.DataFrame:
    # Build complete 5m grid from T_WARMUP_START to T_END + 300 (so each Polymarket T uses bar [T-300, T), plus [T, T+300) for bn_next_up)
    df_1m = df_1m.copy()
    df_1m["bar_open_ts"] = (df_1m["ts"] // 300) * 300
    df_1m["ohlc4"] = (df_1m["open"] + df_1m["high"] + df_1m["low"] + df_1m["close"]) / 4.0

    # Order 1m strictly by timestamp within each 5m bar
    df_1m = df_1m.sort_values("ts")
    agg_5m = df_1m.groupby("bar_open_ts", as_index=False).agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
        taker_buy_base=("taker_buy_base", "sum"),
        last_1m_ohlc4=("ohlc4", "last"),
        n_1m=("ts", "count"),
    )

    grid_open_ts = np.arange(T_WARMUP_START - 300, T_END + 301, 300, dtype=np.int64)
    bars = pd.DataFrame({"bar_open_ts": grid_open_ts}).merge(agg_5m, on="bar_open_ts", how="left")
    # Identify complete 5m bars
    bars["bar_valid"] = (bars["n_1m"] == 5) & bars["close"].notna()

    # Verify zero missing 1m bars inside range before indicator calculation
    missing_bars = int((~bars["bar_valid"]).sum())
    if missing_bars > 0:
        print(f"[WARN] {sym}: {missing_bars} incomplete 5m bars in warmup/test grid; ffill/skip handling.", flush=True)

    o = bars["open"].to_numpy(dtype=np.float64)
    h = bars["high"].to_numpy(dtype=np.float64)
    l = bars["low"].to_numpy(dtype=np.float64)
    c = bars["close"].to_numpy(dtype=np.float64)
    v = bars["volume"].to_numpy(dtype=np.float64)
    tbb = bars["taker_buy_base"].to_numpy(dtype=np.float64)

    rsi7 = compute_wilder_rsi(c, 7)
    rsi2 = compute_wilder_rsi(c, 2)

    s_c = pd.Series(c)
    sma20 = s_c.rolling(20, min_periods=20).mean().to_numpy(dtype=np.float64)
    std20 = s_c.rolling(20, min_periods=20).std(ddof=0).to_numpy(dtype=np.float64)
    bb_upper = sma20 + 1.9 * std20
    bb_lower = sma20 - 1.9 * std20
    bb_width = bb_upper - bb_lower
    pctb = np.where(bb_width > 0.0, (c - bb_lower) / bb_width, np.nan)

    rng = h - l
    lower_wick_ratio = np.where(rng > 0.0, (np.minimum(o, c) - l) / rng, np.nan)
    upper_wick_ratio = np.where(rng > 0.0, (h - np.maximum(o, c)) / rng, np.nan)
    taker_buy_ratio = np.where(v > 0.0, tbb / v, np.nan)

    # Consecutive up/down streaks
    n = len(c)
    consec_up = np.zeros(n, dtype=np.int32)
    consec_down = np.zeros(n, dtype=np.int32)
    cu = 0
    cd = 0
    for i in range(n):
        if not np.isfinite(o[i]) or not np.isfinite(c[i]):
            cu = 0
            cd = 0
        elif c[i] > o[i]:
            cu += 1
            cd = 0
        elif c[i] < o[i]:
            cd += 1
            cu = 0
        else:
            cu = 0
            cd = 0
        consec_up[i] = cu
        consec_down[i] = cd

    atr14, adx14 = compute_wilder_atr_and_adx(h, l, c, 14)
    atr_sma100 = pd.Series(atr14).rolling(100, min_periods=100).mean().to_numpy(dtype=np.float64)
    atr_ratio = np.where(atr_sma100 > 0.0, atr14 / atr_sma100, np.nan)

    s_v = pd.Series(v)
    vol_sma100 = s_v.rolling(100, min_periods=100).mean().to_numpy(dtype=np.float64)
    vol_std100 = s_v.rolling(100, min_periods=100).std(ddof=0).to_numpy(dtype=np.float64)
    vol_z = np.where(vol_std100 > 0.0, (v - vol_sma100) / vol_std100, np.nan)

    # OI aligned by exact timestamp ts == bar_open_ts (i.e. bar [T-300, T) uses OI ts = T - 300)
    oi_series = pd.Series(df_oi["sum_open_interest"].to_numpy(), index=df_oi["ts"].to_numpy())
    oi_cur = pd.Series(grid_open_ts).map(oi_series).to_numpy(dtype=np.float64)
    oi_5m_ago = pd.Series(grid_open_ts - 300).map(oi_series).to_numpy(dtype=np.float64)
    oi_15m_ago = pd.Series(grid_open_ts - 900).map(oi_series).to_numpy(dtype=np.float64)
    d_oi_5m_pct = np.where((oi_cur > 0.0) & (oi_5m_ago > 0.0), (oi_cur / oi_5m_ago - 1.0) * 100.0, np.nan)
    d_oi_15m_pct = np.where((oi_cur > 0.0) & (oi_15m_ago > 0.0), (oi_cur / oi_15m_ago - 1.0) * 100.0, np.nan)

    # Polymarket window T corresponds to current bar ending at T -> bar_open_ts = T - 300
    # So T = bar_open_ts + 300
    bars["T"] = bars["bar_open_ts"] + 300
    bars["bn_close"] = c
    bars["rsi7"] = rsi7
    bars["rsi2"] = rsi2
    bars["pctb"] = pctb
    bars["lower_wick_ratio"] = lower_wick_ratio
    bars["upper_wick_ratio"] = upper_wick_ratio
    bars["taker_buy_ratio"] = taker_buy_ratio
    bars["consec_up"] = consec_up
    bars["consec_down"] = consec_down
    bars["adx14"] = adx14
    bars["atr_ratio"] = atr_ratio
    bars["vol_z"] = vol_z
    bars["d_oi_5m_pct"] = d_oi_5m_pct
    bars["d_oi_15m_pct"] = d_oi_15m_pct

    # bn_next_up on [T, T+300) (the next bar where bar_open_ts == T)
    next_up_map = pd.Series(
        np.where(bars["bar_valid"], (c >= o).astype(np.float64), np.nan),
        index=bars["bar_open_ts"].to_numpy(),
    )
    bars["bn_next_up"] = bars["T"].map(next_up_map)

    # Filter strictly to evaluation windows T in [T_START, T_END]
    out = bars[(bars["T"] >= T_START) & (bars["T"] <= T_END)].copy()

    # Merge 1s TWAP proxy for [T-60, T); fallback to last 1m (O+H+L+C)/4 if 1s missing
    out = out.merge(df_1s[["T", "twap60_1s", "n_1s"]], on="T", how="left")
    use_1s = out["n_1s"].notna() & (out["n_1s"] >= 60) & out["twap60_1s"].notna()
    out["twap60_proxy"] = np.where(use_1s, out["twap60_1s"], out["last_1m_ohlc4"])
    out["twap60_source"] = np.where(use_1s, "1s", "1m_ohlc4_fallback")
    out["close_vs_twap"] = np.where(out["twap60_proxy"] > 0.0, out["bn_close"] / out["twap60_proxy"], np.nan)

    # Compute extreme filter & signals strictly per Section 2.4
    oi_na = out["d_oi_5m_pct"].isna() | out["d_oi_15m_pct"].isna()
    ext_oi_5m = np.where(out["d_oi_5m_pct"].notna(), (out["d_oi_5m_pct"] > 0.32) | (out["d_oi_5m_pct"] < -0.38), False)
    ext_oi_15m = np.where(out["d_oi_15m_pct"].notna(), np.abs(out["d_oi_15m_pct"]) > 0.75, False)
    ext_adx = np.where(out["adx14"].notna(), out["adx14"] > 25.0, False)
    ext_atr = np.where(out["atr_ratio"].notna(), out["atr_ratio"] > 1.55, False)
    ext_volz = np.where(out["vol_z"].notna(), out["vol_z"] > 2.30, False)

    out["oi_is_na"] = oi_na.astype(np.int8)
    out["is_extreme"] = (ext_oi_5m | ext_oi_15m | ext_adx | ext_atr | ext_volz).astype(np.int8)

    # Core direction
    core_up = (out["rsi7"] <= 26.0) & (out["consec_down"] >= 2)
    core_dn = (out["rsi7"] >= 74.0) & (out["consec_up"] >= 2)
    sig_dir_core = np.where(core_up, 1, np.where(core_dn, -1, 0))
    out["sig_dir_core"] = sig_dir_core.astype(np.int8)

    # Individual microstructure conditions (evaluated in direction of core, or computed symmetrically for ablation)
    # When core_dn (sig_dir_core == -1), use Down rules; otherwise use Up rules when core_up
    m1_up = out["rsi2"] <= 15.0
    m2_up = out["pctb"] <= 0.12
    m3_up = out["lower_wick_ratio"] >= 0.12
    m4_up = (out["taker_buy_ratio"] >= 0.42) & (out["bn_close"] >= out["twap60_proxy"] * 0.9995)

    m1_dn = out["rsi2"] >= 85.0
    m2_dn = out["pctb"] >= 0.88
    m3_dn = out["upper_wick_ratio"] >= 0.12
    m4_dn = (out["taker_buy_ratio"] <= 0.58) & (out["bn_close"] <= out["twap60_proxy"] * 1.0005)

    out["micro_1"] = np.where(core_up, m1_up, np.where(core_dn, m1_dn, False)).astype(np.int8)
    out["micro_2"] = np.where(core_up, m2_up, np.where(core_dn, m2_dn, False)).astype(np.int8)
    out["micro_3"] = np.where(core_up, m3_up, np.where(core_dn, m3_dn, False)).astype(np.int8)
    out["micro_4"] = np.where(core_up, m4_up, np.where(core_dn, m4_dn, False)).astype(np.int8)
    out["micro_score"] = (out["micro_1"] + out["micro_2"] + out["micro_3"] + out["micro_4"]).astype(np.int8)

    valid_sig = (out["is_extreme"] == 0) & (out["sig_dir_core"] != 0) & (out["micro_score"] >= 2)
    out["sig_dir"] = np.where(valid_sig, out["sig_dir_core"], 0).astype(np.int8)
    out["coin"] = COIN_SHORT[sym]
    out["symbol"] = sym

    keep_cols = [
        "coin", "symbol", "T", "bar_valid", "bn_next_up",
        "rsi7", "rsi2", "pctb", "lower_wick_ratio", "upper_wick_ratio",
        "taker_buy_ratio", "twap60_proxy", "twap60_source", "close_vs_twap",
        "consec_up", "consec_down", "adx14", "atr_ratio", "vol_z",
        "d_oi_5m_pct", "d_oi_15m_pct", "oi_is_na",
        "is_extreme", "sig_dir_core", "micro_1", "micro_2", "micro_3", "micro_4",
        "micro_score", "sig_dir",
    ]
    return out[keep_cols].reset_index(drop=True)


async def main() -> None:
    limits = httpx.Limits(max_connections=30, max_keepalive_connections=20)
    sem_1m = asyncio.Semaphore(12)
    sem_1s_zip = asyncio.Semaphore(3)  # limit concurrent 70MB 1s downloads+parses
    sem_rest = asyncio.Semaphore(20)
    sem_oi = asyncio.Semaphore(15)

    all_frames: List[pd.DataFrame] = []
    async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}, limits=limits) as client:
        for sym in SYMBOLS:
            print(f"=== Processing Binance data for {sym} ===", flush=True)
            df_1m, df_1s, df_oi = await asyncio.gather(
                load_1m_symbol(client, sem_1m, sym),
                load_1s_twap_symbol(client, sem_1s_zip, sem_rest, sym),
                load_oi_symbol(client, sem_oi, sym),
            )
            feat = build_symbol_features(sym, df_1m, df_1s, df_oi)
            all_frames.append(feat)
            print(
                f"[Done {sym}] rows={len(feat):,}, core={int((feat['sig_dir_core']!=0).sum())}, "
                f"sig_dir={int((feat['sig_dir']!=0).sum())}, oi_na={int(feat['oi_is_na'].sum())}, "
                f"twap_fallback={int((feat['twap60_source']!='1s').sum())}",
                flush=True,
            )

    full = pd.concat(all_frames, ignore_index=True).sort_values(["T", "coin"]).reset_index(drop=True)
    out_path = CACHE_DIR / "binance_features.parquet"
    full.to_parquet(out_path, index=False)
    print(f"Saved {out_path} ({len(full):,} rows)", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
