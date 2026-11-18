#!/usr/bin/env python3
"""
Download & cache Section 4.1 historical datasets (`2026-08-14 .. 2026-11-18`, BTC/ETH/SOL):
  1. `clob_hist_intrawindow.parquet`: CLOB `batch-prices-history` (`fidelity=1`) snapshots
     at tau in {150, 180, 210, 240, 255} for BOTH Up and Down tokens across 83,019 windows.
  2. `bn_1s_tau_features.parquet`: Exact Binance 1s features `[T-60, T+300)` without look-ahead:
     - `S0`: `[T-60, T]` mean 1s close price
     - `spot_150..255`: last 1s close <= T + tau
     - `sigma_log_150..255`: std of trailing 1,800 1s log-returns <= T + tau
     - `A_255`: realized partial TWAP mean over `[T+240, T+255]` (`a = 15s`)
     - `twap60_close`: true `[T+240, T+300]` 60s TWAP for oracle cross-check.
"""
from __future__ import annotations

import asyncio
import datetime
import io
import json
import pathlib
import time
import zipfile
from typing import Any, Dict, List, Tuple

import httpx
import numpy as np
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent
CACHE_DIR = ROOT / "cache"
GAMMA_PARQUET = CACHE_DIR / "gamma_markets.parquet"
CLOB_OUT = CACHE_DIR / "clob_hist_intrawindow.parquet"
BN_OUT = CACHE_DIR / "bn_1s_tau_features.parquet"

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
COIN_SHORT = {"BTCUSDT": "BTC", "ETHUSDT": "ETH", "SOLUSDT": "SOL"}
COINS = ["BTC", "ETH", "SOL"]
TAUS = (150, 180, 210, 240, 255)

T_START = 1786665600       # 2026-08-14T00:00:00Z
T_END = 1794967200         # 2026-11-18T02:00:00Z
REST_START_TS = 1794871200 # 2026-11-16T23:20:00Z (includes 40m warmup before 11-17 00:00Z)

MONTHS_1S = ["2026-08", "2026-09", "2026-10"]
VISION_DAILY_START = datetime.date(2026, 11, 1)
VISION_DAILY_END = datetime.date(2026, 11, 16)


class RatePacer:
    def __init__(self, max_per_10s: float, max_conc: int) -> None:
        self.interval = 10.0 / max_per_10s
        self.sem = asyncio.Semaphore(max_conc)
        self.lock = asyncio.Lock()
        self.next_ts = 0.0

    async def acquire(self) -> None:
        await self.sem.acquire()
        async with self.lock:
            now = time.monotonic()
            wait = max(0.0, self.next_ts - now)
            self.next_ts = max(now, self.next_ts) + self.interval
        if wait > 0:
            await asyncio.sleep(wait)

    def release(self) -> None:
        self.sem.release()


async def post_json_retry(
    client: httpx.AsyncClient, pacer: RatePacer, url: str, body: dict[str, Any]
) -> Any:
    for attempt in range(7):
        await pacer.acquire()
        try:
            r = await client.post(url, json=body, timeout=30.0)
        except (httpx.HTTPError, asyncio.TimeoutError):
            pacer.release()
            await asyncio.sleep(min(2.0 ** attempt, 12.0))
            continue
        pacer.release()
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 418, 500, 502, 503, 504):
            await asyncio.sleep(min(1.5 * (2 ** attempt), 15.0))
            continue
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
    raise RuntimeError(f"Failed POST {url}")


async def fetch_bytes_retry(
    client: httpx.AsyncClient, sem: asyncio.Semaphore, url: str
) -> bytes:
    for attempt in range(6):
        try:
            async with sem:
                r = await client.get(url, timeout=180.0)
            if r.status_code == 200:
                return r.content
            if r.status_code in (429, 418, 500, 502, 503, 504):
                await asyncio.sleep(min(2.0 ** attempt, 15.0))
                continue
            raise RuntimeError(f"HTTP {r.status_code} {url}")
        except (httpx.HTTPError, asyncio.TimeoutError):
            if attempt == 5:
                raise
            await asyncio.sleep(min(2.0 * (attempt + 1), 12.0))
    raise RuntimeError(f"Failed {url}")


async def fetch_clob_intrawindow(client: httpx.AsyncClient, df_gamma: pd.DataFrame) -> pd.DataFrame:
    if CLOB_OUT.exists():
        df_old = pd.read_parquet(CLOB_OUT)
        if len(df_old) == len(df_gamma) * len(TAUS):
            print(f"[4.1 CLOB] Cache hit: {len(df_old):,} rows", flush=True)
            return df_old

    pacer = RatePacer(max_per_10s=460.0, max_conc=36)
    valid = df_gamma[(df_gamma["found"] == 1) & df_gamma["up_token_id"].notna() & df_gamma["down_token_id"].notna()].copy()

    batches: List[pd.DataFrame] = []
    for coin in COINS:
        sub = valid[valid["coin"] == coin].sort_values("T")
        for i in range(0, len(sub), 10):
            batches.append(sub.iloc[i : i + 10])

    # Result dict keyed by (coin, T, tau) -> (up_px, up_lag, dn_px, dn_lag)
    results: Dict[Tuple[str, int, int], Tuple[float, float, float, float]] = {}
    lock = asyncio.Lock()

    async def process_batch(sub: pd.DataFrame) -> None:
        toks: List[str] = []
        meta: List[Tuple[str, int, str, str]] = []
        for row in sub.itertuples(index=False):
            toks.append(row.up_token_id)
            toks.append(row.down_token_id)
            meta.append((row.coin, int(row.T), row.up_token_id, row.down_token_id))
        t_min = int(sub["T"].min())
        t_max = int(sub["T"].max())
        payload = {
            "markets": toks,
            "start_ts": t_min - 180,
            "end_ts": t_max + 310,
            "fidelity": 1,
        }
        res = await post_json_retry(client, pacer, "https://clob.polymarket.com/batch-prices-history", payload)
        hist_map = res.get("history", {}) if isinstance(res, dict) else {}
        local: Dict[Tuple[str, int, int], Tuple[float, float, float, float]] = {}
        for coin, T, up_tok, dn_tok in meta:
            up_pts = sorted((int(x["t"]), float(x["p"])) for x in (hist_map.get(up_tok) or []) if "t" in x and "p" in x)
            dn_pts = sorted((int(x["t"]), float(x["p"])) for x in (hist_map.get(dn_tok) or []) if "t" in x and "p" in x)
            up_ts = [x[0] for x in up_pts]
            dn_ts = [x[0] for x in dn_pts]
            for tau in TAUS:
                target = T + tau
                up_px, up_lag = np.nan, np.nan
                dn_px, dn_lag = np.nan, np.nan
                # bisect right for last <= target
                import bisect
                ui = bisect.bisect_right(up_ts, target) - 1
                if ui >= 0:
                    up_px = up_pts[ui][1]
                    up_lag = float(target - up_pts[ui][0])
                di = bisect.bisect_right(dn_ts, target) - 1
                if di >= 0:
                    dn_px = dn_pts[di][1]
                    dn_lag = float(target - dn_pts[di][0])
                local[(coin, T, tau)] = (up_px, up_lag, dn_px, dn_lag)
        async with lock:
            results.update(local)

    t0 = time.time()
    print(f"[4.1 CLOB] Fetching batch-prices-history [T, T+300] in {len(batches):,} calls...", flush=True)
    for start in range(0, len(batches), 400):
        await asyncio.gather(*(process_batch(b) for b in batches[start : start + 400]))
        done = min(start + 400, len(batches))
        if done % 1600 == 0 or done == len(batches):
            print(f"  [4.1 CLOB] {done:,}/{len(batches):,} ({time.time() - t0:.1f}s)", flush=True)

    rows = []
    for row in df_gamma[["coin", "T"]].itertuples(index=False):
        c, T = str(row.coin), int(row.T)
        for tau in TAUS:
            up_px, up_lag, dn_px, dn_lag = results.get((c, T, tau), (np.nan, np.nan, np.nan, np.nan))
            rows.append((c, T, tau, up_px, up_lag, dn_px, dn_lag))
    df_out = pd.DataFrame(rows, columns=["coin", "T", "tau", "up_px", "up_lag_sec", "down_px", "down_lag_sec"])
    for col in ("up_px", "up_lag_sec", "down_px", "down_lag_sec"):
        df_out[col] = df_out[col].astype("float32")
    df_out.to_parquet(CLOB_OUT, index=False)
    print(f"[4.1 CLOB] Saved {CLOB_OUT}: rows={len(df_out):,}", flush=True)
    return df_out


def _norm_ts_sec(ts_raw: np.ndarray) -> np.ndarray:
    ts = ts_raw.astype(np.int64)
    if len(ts) == 0:
        return ts
    mx = int(ts.max())
    if mx >= 10**14:
        return ts // 1_000_000
    if mx >= 10**11:
        return ts // 1_000
    return ts


def _compute_sym_features(sym: str, closes_by_sec: np.ndarray, ts0: int, valid_ts_end: int) -> pd.DataFrame:
    """Vectorized extraction of S0 ([T-60, T]), spot(tau), trailing 30m 1s log-return sigma, A_255, twap60_close."""
    coin = COIN_SHORT[sym]
    # Forward-fill tiny missing seconds (<30s) safely before log-returns so 1s timeline is contiguous
    n_tot = len(closes_by_sec)
    series = closes_by_sec.copy()
    isnan = np.isnan(series)
    if isnan.any():
        # standard forward fill using numpy
        idx = np.where(~isnan, np.arange(n_tot), 0)
        np.maximum.accumulate(idx, out=idx)
        series = series[idx]

    log_ret = np.empty(n_tot, dtype=np.float64)
    log_ret[0] = np.nan
    with np.errstate(divide="ignore", invalid="ignore"):
        log_ret[1:] = np.log(series[1:] / series[:-1])
    log_ret[~np.isfinite(log_ret)] = np.nan

    # Prefix sums for exact O(1) trailing 1800s mean & std of log_ret (ddof=1)
    lr_valid = np.isfinite(log_ret).astype(np.int64)
    lr_zero = np.where(np.isfinite(log_ret), log_ret, 0.0)
    csum_n = np.concatenate(([0], np.cumsum(lr_valid)))
    csum_x = np.concatenate(([0.0], np.cumsum(lr_zero)))
    csum_x2 = np.concatenate(([0.0], np.cumsum(lr_zero * lr_zero)))

    # Prefix sums for O(1) price TWAP integrals
    px_valid = np.isfinite(series).astype(np.int64)
    px_zero = np.where(np.isfinite(series), series, 0.0)
    csum_pn = np.concatenate(([0], np.cumsum(px_valid)))
    csum_p = np.concatenate(([0.0], np.cumsum(px_zero)))

    Ts = np.arange(T_START, T_END + 1, 300, dtype=np.int64)
    idx_T = Ts - ts0

    def mean_range(a_idx: np.ndarray, b_idx_incl: np.ndarray) -> np.ndarray:
        a = np.clip(a_idx, 0, n_tot - 1)
        b = np.clip(b_idx_incl + 1, 0, n_tot)
        s = csum_p[b] - csum_p[a]
        c = csum_pn[b] - csum_pn[a]
        return np.where(c > 0, s / np.maximum(c, 1), np.nan)

    def sigma_trailing(end_idx_incl: np.ndarray, win_sec: int = 1800) -> np.ndarray:
        b = np.clip(end_idx_incl + 1, 0, n_tot)
        a = np.clip(end_idx_incl + 1 - win_sec, 0, n_tot)
        cnt = csum_n[b] - csum_n[a]
        sx = csum_x[b] - csum_x[a]
        sx2 = csum_x2[b] - csum_x2[a]
        var = (sx2 - (sx * sx) / np.maximum(cnt, 1)) / np.maximum(cnt - 1, 1)
        return np.where(cnt >= 60, np.sqrt(np.maximum(var, 0.0)), np.nan)

    out: Dict[str, Any] = {
        "coin": np.full(len(Ts), coin, dtype=object),
        "T": Ts,
        # S0 = mean over [T-60, T] (inclusive of T, i.e., 61s, plus store S0_excl [T-60, T-1] for parity check)
        "S0": mean_range(idx_T - 60, idx_T),
        "S0_excl": mean_range(idx_T - 60, idx_T - 1),
        "twap60_close": mean_range(idx_T + 240, idx_T + 300),
        "A_255": mean_range(idx_T + 240, idx_T + 255),
    }
    for tau in TAUS:
        ti = np.clip(idx_T + tau, 0, n_tot - 1)
        out[f"spot_{tau}"] = series[ti]
        out[f"sigma_{tau}"] = sigma_trailing(idx_T + tau, 1800)
    return pd.DataFrame(out)


async def fetch_bn_1s_features(client: httpx.AsyncClient) -> pd.DataFrame:
    if BN_OUT.exists():
        df_old = pd.read_parquet(BN_OUT)
        if len(df_old) == 3 * ((T_END - T_START) // 300 + 1):
            print(f"[4.1 BN-1s] Cache hit: {len(df_old):,} rows", flush=True)
            return df_old

    sem_zip = asyncio.Semaphore(3)
    sem_rest = asyncio.Semaphore(16)
    ts0 = T_START - 2400  # extra 40m warmup before T_START
    ts1 = T_END + 305
    n_secs = ts1 - ts0 + 1

    urls_template: List[str] = []
    for m in MONTHS_1S:
        urls_template.append("https://data.binance.vision/data/spot/monthly/klines/{SYM}/1s/{SYM}-1s-" + f"{m}.zip")
    d = VISION_DAILY_START
    while d <= VISION_DAILY_END:
        urls_template.append("https://data.binance.vision/data/spot/daily/klines/{SYM}/1s/{SYM}-1s-" + f"{d.isoformat()}.zip")
        d += datetime.timedelta(days=1)

    frames: List[pd.DataFrame] = []
    for sym in SYMBOLS:
        t0 = time.time()
        print(f"[4.1 BN-1s] Processing {sym} ({len(urls_template)} zips + REST Nov17-18)...", flush=True)
        arr_px = np.full(n_secs, np.nan, dtype=np.float64)

        # Process zips sequentially or bounded batches of 4 to keep peak RAM < 150MB
        urls = [u.format(SYM=sym) for u in urls_template]
        for i in range(0, len(urls), 4):
            batch_u = urls[i : i + 4]
            contents = await asyncio.gather(*(fetch_bytes_retry(client, sem_zip, u) for u in batch_u))
            for content in contents:
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    with zf.open(zf.namelist()[0]) as f:
                        for chunk in pd.read_csv(f, header=None, usecols=[0, 4], names=["ts", "c"], chunksize=600_000):
                            ts_s = _norm_ts_sec(chunk["ts"].to_numpy())
                            c = chunk["c"].to_numpy(dtype=np.float64)
                            m = (ts_s >= ts0) & (ts_s <= ts1)
                            if np.any(m):
                                arr_px[ts_s[m] - ts0] = c[m]
            print(f"  [{sym}] zips {min(i+4, len(urls))}/{len(urls)} ({time.time()-t0:.1f}s)", flush=True)

        # Fetch REST tail [2026-11-17T00:00:00Z .. T_END+305] in 1000-sec chunks
        rest_start = 1794873540  # slight overlap
        starts = list(range(rest_start, ts1 + 1, 1000))

        async def fetch_rest(s_sec: int) -> List[Tuple[int, float]]:
            e_sec = min(s_sec + 999, ts1)
            u = f"https://api.binance.com/api/v3/klines?symbol={sym}&interval=1s&startTime={s_sec*1000}&endTime={e_sec*1000}&limit=1000"
            b = await fetch_bytes_retry(client, sem_rest, u)
            return [(int(r[0]) // 1000, float(r[4])) for r in json.loads(b.decode())]

        rest_parts = await asyncio.gather(*(fetch_rest(s) for s in starts))
        for part in rest_parts:
            for t_s, c_v in part:
                if ts0 <= t_s <= ts1:
                    arr_px[t_s - ts0] = c_v

        df_sym = _compute_sym_features(sym, arr_px, ts0, ts1)
        miss = int(np.isnan(arr_px[2400:]).sum())
        print(f"[4.1 BN-1s] {sym} done in {time.time()-t0:.1f}s (raw missing 1s ticks={miss:,})", flush=True)
        frames.append(df_sym)

    df_all = pd.concat(frames, ignore_index=True).sort_values(["T", "coin"]).reset_index(drop=True)
    df_all.to_parquet(BN_OUT, index=False)
    print(f"[4.1 BN-1s] Saved {BN_OUT}: rows={len(df_all):,}", flush=True)
    return df_all


async def main() -> None:
    df_gamma = pd.read_parquet(GAMMA_PARQUET)
    limits = httpx.Limits(max_connections=64, max_keepalive_connections=64)
    async with httpx.AsyncClient(http2=False, limits=limits, headers={"User-Agent": "poly-41-val/1.0"}) as client:
        # Run CLOB history fetch first (~3 min), then Binance 1s zips (~4-6 min)
        await fetch_clob_intrawindow(client, df_gamma)
        await fetch_bn_1s_features(client)


if __name__ == "__main__":
    asyncio.run(main())
