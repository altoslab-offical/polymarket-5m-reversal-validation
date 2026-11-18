#!/usr/bin/env python3
"""
Fetch and aggregate Polymarket 5m market metadata, CLOB historical price series,
and Data API v2 trade-by-trade fills for BTC, ETH, and SOL over the pure 60s TWAP period.

Range: T in [1786665600 (2026-08-14T00:00:00Z), 1794967200 (2026-11-18T02:00:00Z)]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import pathlib
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx
import numpy as np
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent
CACHE_DIR = ROOT / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

T_START = 1786665600  # 2026-08-14T00:00:00Z
T_END = 1794967200    # 2026-11-18T02:00:00Z
COINS = ["BTC", "ETH", "SOL"]

GAMMA_PARQUET = CACHE_DIR / "gamma_markets.parquet"
HIST_PARQUET = CACHE_DIR / "clob_hist_px.parquet"
TRADES_PARQUET = CACHE_DIR / "pm_trades_agg.parquet"
TRADES_SQLITE = CACHE_DIR / "pm_trades_ckpt.sqlite"


class RatePacer:
    """Async sliding-window request pacer + global cooldown on 429."""

    def __init__(self, max_per_10s: float, max_conc: int) -> None:
        self.sem = asyncio.Semaphore(max_conc)
        self.min_interval = 10.0 / max_per_10s
        self._lock = asyncio.Lock()
        self._next_ts = 0.0
        self._cooldown_until = 0.0

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            if now < self._cooldown_until:
                wait = self._cooldown_until - now
                await asyncio.sleep(wait)
                now = time.monotonic()
            target = max(now, self._next_ts)
            wait = target - now
            self._next_ts = target + self.min_interval
        if wait > 0:
            await asyncio.sleep(wait)

    async def backoff(self, sec: float) -> None:
        async with self._lock:
            target = time.monotonic() + sec
            if target > self._cooldown_until:
                self._cooldown_until = target


async def get_json_with_retry(
    client: httpx.AsyncClient,
    pacer: RatePacer,
    method: str,
    url: str,
    json_body: Any = None,
    allow_404: bool = False,
) -> Any:
    for attempt in range(8):
        await pacer.acquire()
        try:
            async with pacer.sem:
                if method == "GET":
                    resp = await client.get(url, timeout=25.0)
                else:
                    resp = await client.post(url, json=json_body, timeout=25.0)
            if resp.status_code == 200:
                return resp.json()
            if allow_404 and resp.status_code == 404:
                return None
            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                wait = float(ra) if ra and ra.replace(".", "", 1).isdigit() else min(2.0 ** attempt, 20.0)
                await pacer.backoff(wait + 0.5)
                continue
            if resp.status_code in (500, 502, 503, 504):
                await asyncio.sleep(min(1.5 * (attempt + 1), 12.0))
                continue
            raise RuntimeError(f"HTTP {resp.status_code} for {url}: {resp.text[:250]}")
        except (httpx.HTTPError, asyncio.TimeoutError, json.JSONDecodeError) as exc:
            if attempt == 7:
                raise RuntimeError(f"Failed {url} after 8 attempts: {exc}") from exc
            await asyncio.sleep(min(1.2 * (attempt + 1), 10.0))
    return None


def _parse_json_field(val: Any) -> Any:
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return None
    return val


def _extract_market_row(coin: str, T: int, m: Dict[str, Any]) -> Dict[str, Any]:
    slug = f"{coin.lower()}-updown-5m-{T}"
    cond_id = m.get("conditionId")
    tokens = _parse_json_field(m.get("clobTokenIds")) or []
    outcomes = _parse_json_field(m.get("outcomes")) or []
    prices = _parse_json_field(m.get("outcomePrices")) or []

    up_idx, dn_idx = None, None
    for idx, label in enumerate(outcomes):
        lbl = str(label).strip().lower()
        if lbl == "up":
            up_idx = idx
        elif lbl == "down":
            dn_idx = idx
    if up_idx is None and len(tokens) >= 2:
        up_idx, dn_idx = 0, 1

    up_token = str(tokens[up_idx]) if up_idx is not None and up_idx < len(tokens) else None
    dn_token = str(tokens[dn_idx]) if dn_idx is not None and dn_idx < len(tokens) else None

    pm_outcome: Optional[str] = None
    if m.get("closed") is True and len(prices) >= 2 and up_idx is not None and dn_idx is not None:
        try:
            p_up = float(prices[up_idx])
            p_dn = float(prices[dn_idx])
            if p_up >= 0.99 and p_dn <= 0.01:
                pm_outcome = "Up"
            elif p_dn >= 0.99 and p_up <= 0.01:
                pm_outcome = "Down"
        except Exception:
            pm_outcome = None

    fees_enabled = bool(m.get("feesEnabled", False))
    fs = m.get("feeSchedule")
    taker_fee_rate = np.nan
    maker_fee_rate = np.nan
    if not fees_enabled:
        taker_fee_rate = 0.0
        maker_fee_rate = 0.0
    elif isinstance(fs, dict) and "rate" in fs:
        taker_fee_rate = float(fs["rate"])
        maker_fee_rate = 0.0 if fs.get("takerOnly", True) else taker_fee_rate

    desc = str(m.get("description") or "")
    is_twap60 = "twap-60s" in desc.lower()

    return {
        "coin": coin,
        "T": int(T),
        "slug": slug,
        "found": 1,
        "condition_id": cond_id,
        "up_token_id": up_token,
        "down_token_id": dn_token,
        "pm_outcome": pm_outcome,
        "taker_fee_rate": taker_fee_rate,
        "maker_fee_rate": maker_fee_rate,
        "order_min_size": float(m.get("orderMinSize")) if m.get("orderMinSize") is not None else np.nan,
        "is_twap60": int(is_twap60),
    }


async def stage_gamma(client: httpx.AsyncClient) -> pd.DataFrame:
    if GAMMA_PARQUET.exists():
        df_old = pd.read_parquet(GAMMA_PARQUET)
        if len(df_old) == 3 * ((T_END - T_START) // 300 + 1):
            print(f"[Gamma] Cache hit: {len(df_old):,} markets", flush=True)
            return df_old

    Ts = list(range(T_START, T_END + 1, 300))
    all_pairs = [(coin, t) for coin in COINS for t in Ts]
    # Gamma /markets allows at most 100 slug params per call
    pacer = RatePacer(max_per_10s=240.0, max_conc=20)
    batches = [all_pairs[i : i + 100] for i in range(0, len(all_pairs), 100)]

    by_slug: Dict[str, Dict[str, Any]] = {}
    lock = asyncio.Lock()

    async def fetch_batch(batch: List[Tuple[str, int]]) -> None:
        qs = "&".join(f"slug={c.lower()}-updown-5m-{t}" for c, t in batch)
        url = f"https://gamma-api.polymarket.com/markets?closed=any&limit=100&{qs}"
        arr = await get_json_with_retry(client, pacer, "GET", url)
        if isinstance(arr, list):
            async with lock:
                for m in arr:
                    if isinstance(m, dict) and m.get("slug"):
                        by_slug[m["slug"]] = m

    t0 = time.time()
    print(f"[Gamma] Fetching {len(all_pairs):,} markets in {len(batches)} batches...", flush=True)
    for start in range(0, len(batches), 100):
        await asyncio.gather(*(fetch_batch(b) for b in batches[start : start + 100]))
        if (start + 100) % 300 == 0 or start + 100 >= len(batches):
            print(f"  [Gamma] {min(start+100, len(batches))}/{len(batches)} batches ({time.time()-t0:.1f}s), got {len(by_slug):,}", flush=True)

    # Verify any missing slug via individual lookup /markets/slug/<slug> before marking not found
    missing_pairs = [(c, t) for (c, t) in all_pairs if f"{c.lower()}-updown-5m-{t}" not in by_slug]
    if missing_pairs:
        print(f"[Gamma] Double-checking {len(missing_pairs)} missing slugs individually...", flush=True)
        async def check_one(c: str, t: int) -> None:
            sl = f"{c.lower()}-updown-5m-{t}"
            m = await get_json_with_retry(client, pacer, "GET", f"https://gamma-api.polymarket.com/markets/slug/{sl}", allow_404=True)
            if isinstance(m, dict) and m.get("slug"):
                async with lock:
                    by_slug[m["slug"]] = m
        await asyncio.gather(*(check_one(c, t) for c, t in missing_pairs))

    rows: List[Dict[str, Any]] = []
    for coin, T in all_pairs:
        sl = f"{coin.lower()}-updown-5m-{T}"
        m = by_slug.get(sl)
        if m is not None:
            rows.append(_extract_market_row(coin, T, m))
        else:
            rows.append({
                "coin": coin, "T": int(T), "slug": sl, "found": 0,
                "condition_id": None, "up_token_id": None, "down_token_id": None,
                "pm_outcome": None, "taker_fee_rate": np.nan, "maker_fee_rate": np.nan,
                "order_min_size": np.nan, "is_twap60": 0,
            })

    df = pd.DataFrame(rows).sort_values(["T", "coin"]).reset_index(drop=True)
    df.to_parquet(GAMMA_PARQUET, index=False)
    print(
        f"[Gamma] Saved {GAMMA_PARQUET}: total={len(df):,}, found={int(df['found'].sum()):,}, "
        f"settled={int(df['pm_outcome'].notna().sum()):,}, twap60={int(df['is_twap60'].sum()):,}",
        flush=True,
    )
    return df


async def stage_hist(client: httpx.AsyncClient, df_gamma: pd.DataFrame) -> pd.DataFrame:
    if HIST_PARQUET.exists():
        df_old = pd.read_parquet(HIST_PARQUET)
        if len(df_old) == len(df_gamma):
            print(f"[Hist] Cache hit: {len(df_old):,} rows", flush=True)
            return df_old

    # Limit for CLOB /prices-history is 1000 req / 10s. /batch-prices-history takes max 20 tokens (10 markets).
    pacer = RatePacer(max_per_10s=450.0, max_conc=35)
    valid = df_gamma[(df_gamma["found"] == 1) & df_gamma["up_token_id"].notna() & df_gamma["down_token_id"].notna()].copy()

    # Build batches of up to 10 consecutive windows of same coin (20 tokens total)
    batches: List[pd.DataFrame] = []
    for coin in COINS:
        sub = valid[valid["coin"] == coin].sort_values("T")
        for i in range(0, len(sub), 10):
            batches.append(sub.iloc[i : i + 10])

    tok_px: Dict[Tuple[str, int], Tuple[float, float]] = {}
    lock = asyncio.Lock()

    async def fetch_hist_batch(sub: pd.DataFrame) -> None:
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
            "start_ts": t_min - 1800,
            "end_ts": t_max + 60,
            "fidelity": 1,
        }
        res = await get_json_with_retry(
            client, pacer, "POST", "https://clob.polymarket.com/batch-prices-history", json_body=payload
        )
        hist_map = res.get("history", {}) if isinstance(res, dict) else {}
        local: Dict[Tuple[str, int], Tuple[float, float]] = {}
        for coin, T, up_tok, dn_tok in meta:
            up_pts = [x for x in (hist_map.get(up_tok) or []) if x.get("t", 10**12) <= T]
            dn_pts = [x for x in (hist_map.get(dn_tok) or []) if x.get("t", 10**12) <= T]
            up_px = float(max(up_pts, key=lambda x: x["t"])["p"]) if up_pts else np.nan
            dn_px = float(max(dn_pts, key=lambda x: x["t"])["p"]) if dn_pts else np.nan
            local[(coin, T)] = (up_px, dn_px)
        async with lock:
            tok_px.update(local)

    t0 = time.time()
    print(f"[Hist] Fetching CLOB batch-prices-history in {len(batches):,} calls...", flush=True)
    for start in range(0, len(batches), 400):
        await asyncio.gather(*(fetch_hist_batch(b) for b in batches[start : start + 400]))
        if (start + 400) % 1600 == 0 or start + 400 >= len(batches):
            print(f"  [Hist] {min(start+400, len(batches)):,}/{len(batches):,} ({time.time()-t0:.1f}s)", flush=True)

    rows = []
    for row in df_gamma[["coin", "T"]].itertuples(index=False):
        up_px, dn_px = tok_px.get((row.coin, int(row.T)), (np.nan, np.nan))
        rows.append((row.coin, int(row.T), up_px, dn_px))

    df_hist = pd.DataFrame(rows, columns=["coin", "T", "up_hist_px_T", "down_hist_px_T"])
    df_hist.to_parquet(HIST_PARQUET, index=False)
    print(f"[Hist] Saved {HIST_PARQUET}: up_notna={int(df_hist['up_hist_px_T'].notna().sum()):,}", flush=True)
    return df_hist


def _init_trades_sqlite() -> sqlite3.Connection:
    con = sqlite3.connect(TRADES_SQLITE)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS agg_trades (
            coin TEXT NOT NULL,
            T INTEGER NOT NULL,
            up_vwap_pre30 REAL,
            down_vwap_pre30 REAL,
            up_vwap_0_30 REAL,
            down_vwap_0_30 REAL,
            up_vwap_0_60 REAL,
            down_vwap_0_60 REAL,
            up_first_px REAL,
            down_first_px REAL,
            up_n_trades_0_60 INTEGER NOT NULL,
            down_n_trades_0_60 INTEGER NOT NULL,
            up_usdc_0_60 REAL NOT NULL,
            down_usdc_0_60 REAL NOT NULL,
            PRIMARY KEY (coin, T)
        )
        """
    )
    con.commit()
    return con


def _agg_market_trades(
    T: int,
    up_tok: str,
    dn_tok: str,
    rows_desc: List[Tuple[str, int, float, float]],
) -> Tuple[Any, ...]:
    """
    rows_desc must be in chronological DESC order (newest first, as returned by /v2/trades).
    Tuple elements: (token_id, ts, price, size).
    """
    up_pre30_usdc = 0.0
    up_pre30_sh = 0.0
    dn_pre30_usdc = 0.0
    dn_pre30_sh = 0.0

    up_030_usdc = 0.0
    up_030_sh = 0.0
    dn_030_usdc = 0.0
    dn_030_sh = 0.0

    up_060_usdc = 0.0
    up_060_sh = 0.0
    up_060_n = 0
    dn_060_usdc = 0.0
    dn_060_sh = 0.0
    dn_060_n = 0

    # Chronologically earliest trade with ts >= T is the last one seen with smallest ts >= T in DESC stream
    up_first_ts: Optional[int] = None
    up_first_px: Optional[float] = None
    dn_first_ts: Optional[int] = None
    dn_first_px: Optional[float] = None

    t_pre = T - 30
    t_30 = T + 30
    t_60 = T + 60

    for tok, ts, px, sz in rows_desc:
        if sz <= 0.0:
            continue
        if tok == up_tok:
            if ts >= T:
                if up_first_ts is None or ts <= up_first_ts:
                    up_first_ts = ts
                    up_first_px = px
                if ts < t_30:
                    up_030_usdc += px * sz
                    up_030_sh += sz
                if ts < t_60:
                    up_060_usdc += px * sz
                    up_060_sh += sz
                    up_060_n += 1
            elif ts >= t_pre:
                up_pre30_usdc += px * sz
                up_pre30_sh += sz
        elif tok == dn_tok:
            if ts >= T:
                if dn_first_ts is None or ts <= dn_first_ts:
                    dn_first_ts = ts
                    dn_first_px = px
                if ts < t_30:
                    dn_030_usdc += px * sz
                    dn_030_sh += sz
                if ts < t_60:
                    dn_060_usdc += px * sz
                    dn_060_sh += sz
                    dn_060_n += 1
            elif ts >= t_pre:
                dn_pre30_usdc += px * sz
                dn_pre30_sh += sz

    return (
        up_pre30_usdc / up_pre30_sh if up_pre30_sh > 0 else None,
        dn_pre30_usdc / dn_pre30_sh if dn_pre30_sh > 0 else None,
        up_030_usdc / up_030_sh if up_030_sh > 0 else None,
        dn_030_usdc / dn_030_sh if dn_030_sh > 0 else None,
        up_060_usdc / up_060_sh if up_060_sh > 0 else None,
        dn_060_usdc / dn_060_sh if dn_060_sh > 0 else None,
        up_first_px,
        dn_first_px,
        up_060_n,
        dn_060_n,
        up_060_usdc,
        dn_060_usdc,
    )


async def stage_trades(client: httpx.AsyncClient, df_gamma: pd.DataFrame) -> pd.DataFrame:
    con = _init_trades_sqlite()
    done_pairs = set(con.execute("SELECT coin, T FROM agg_trades").fetchall())
    total_valid = int((df_gamma["found"] == 1).sum())
    print(f"[Trades] Already checkpointed: {len(done_pairs):,}/{total_valid:,}", flush=True)

    todo = df_gamma[(df_gamma["found"] == 1) & df_gamma["condition_id"].notna()].copy()
    todo = todo[~todo.apply(lambda r: (r["coin"], int(r["T"])) in done_pairs, axis=1)]

    if not todo.empty:
        # Align batches by identical window T so stop_ts = T - 31 stops all conditions immediately:
        # - BTC alone: [BTC_T]
        # - ETH + SOL at same T combined: [ETH_T, SOL_T] (~380 rows -> usually 1 HTTP call for both coins)
        batches: List[List[Tuple[str, int, str, str, str]]] = []
        todo = todo.sort_values(["T", "coin"])
        for t_val, grp in todo.groupby("T", sort=True):
            eth_sol: List[Tuple[str, int, str, str, str]] = []
            for row in grp.itertuples(index=False):
                item = (row.coin, int(row.T), row.condition_id, row.up_token_id, row.down_token_id)
                if row.coin == "BTC":
                    batches.append([item])
                else:
                    eth_sol.append(item)
            if eth_sol:
                batches.append(eth_sol)

        pacer = RatePacer(max_per_10s=550.0, max_conc=45)

        async def fetch_trade_batch(batch: List[Tuple[str, int, str, str, str]]) -> List[Tuple[Any, ...]]:
            cids = ",".join(x[2] for x in batch)
            t_win = batch[0][1]
            cid_to_rows: Dict[str, List[Tuple[str, int, float, float]]] = {x[2]: [] for x in batch}
            cur: Optional[str] = None
            stop_ts = t_win - 31  # Strict margin past [T-30, T)

            for _page in range(400):
                url = f"https://data-api.polymarket.com/v2/trades?condition={cids}&limit=1000"
                if cur:
                    url += f"&cursor={cur}"
                res = await get_json_with_retry(client, pacer, "GET", url)
                rows = (res.get("data") or []) if isinstance(res, dict) else []
                for r in rows:
                    cid = r.get("condition_id")
                    if cid in cid_to_rows:
                        cid_to_rows[cid].append((
                            str(r["token_id"]),
                            int(r["timestamp"]),
                            float(r["price"]),
                            float(r["size"]),
                        ))
                pag = res.get("pagination") or {} if isinstance(res, dict) else {}
                if not rows or int(rows[-1]["timestamp"]) < stop_ts or not pag.get("has_more") or not pag.get("next_cursor"):
                    break
                cur = str(pag["next_cursor"])

            out_records: List[Tuple[Any, ...]] = []
            for coin, T, cid, up_tok, dn_tok in batch:
                agg = _agg_market_trades(T, up_tok, dn_tok, cid_to_rows[cid])
                out_records.append((coin, T, *agg))
            return out_records

        t0 = time.time()
        total_todo = len(todo)
        print(f"[Trades] Remaining markets={total_todo:,} in {len(batches):,} T-aligned batches (45 workers)...", flush=True)

        queue: asyncio.Queue[Optional[List[Tuple[str, int, str, str, str]]]] = asyncio.Queue()
        for b in batches:
            queue.put_nowait(b)
        n_workers = 45
        for _ in range(n_workers):
            queue.put_nowait(None)

        buffer: List[Tuple[Any, ...]] = []
        buf_lock = asyncio.Lock()
        done_cnt = 0

        def _flush_records(recs: List[Tuple[Any, ...]]) -> None:
            con.executemany(
                """
                INSERT OR REPLACE INTO agg_trades (
                    coin, T, up_vwap_pre30, down_vwap_pre30,
                    up_vwap_0_30, down_vwap_0_30, up_vwap_0_60, down_vwap_0_60,
                    up_first_px, down_first_px, up_n_trades_0_60, down_n_trades_0_60,
                    up_usdc_0_60, down_usdc_0_60
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                recs,
            )
            con.commit()

        async def worker() -> None:
            nonlocal done_cnt
            while True:
                batch = await queue.get()
                if batch is None:
                    queue.task_done()
                    break
                try:
                    recs = await fetch_trade_batch(batch)
                    to_flush: Optional[List[Tuple[Any, ...]]] = None
                    async with buf_lock:
                        buffer.extend(recs)
                        if len(buffer) >= 600:
                            to_flush = list(buffer)
                            buffer.clear()
                    if to_flush is not None:
                        _flush_records(to_flush)
                        async with buf_lock:
                            done_cnt += len(to_flush)
                            dc = done_cnt
                        elapsed = time.time() - t0
                        rate = dc / max(elapsed, 0.01)
                        eta_min = (total_todo - dc) / max(rate, 0.01) / 60.0
                        print(
                            f"  [Trades] +{dc:,}/{total_todo:,} ({100*dc/total_todo:.1f}%) "
                            f"in {elapsed:.0f}s ({rate:.1f} mkts/s, ETA {eta_min:.1f}m)",
                            flush=True,
                        )
                finally:
                    queue.task_done()

        await asyncio.gather(*(worker() for _ in range(n_workers)))
        if buffer:
            _flush_records(buffer)
            done_cnt += len(buffer)
            buffer.clear()
            print(f"  [Trades] +{done_cnt:,}/{total_todo:,} (100.0%) in {time.time()-t0:.0f}s", flush=True)

    df_tr = pd.read_sql_query("SELECT * FROM agg_trades", con)
    con.close()
    # Left join against full grid from df_gamma so missing markets also appear as NA rows
    base = df_gamma[["coin", "T"]].merge(df_tr, on=["coin", "T"], how="left").sort_values(["T", "coin"]).reset_index(drop=True)
    base.to_parquet(TRADES_PARQUET, index=False)
    print(f"[Trades] Saved {TRADES_PARQUET}: total={len(base):,}, up_pre30_notna={int(base['up_vwap_pre30'].notna().sum()):,}", flush=True)
    return base


async def run_stages(stage: str) -> None:
    limits = httpx.Limits(max_connections=80, max_keepalive_connections=50)
    async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}, limits=limits) as client:
        df_gamma = await stage_gamma(client)
        if stage in ("all", "hist"):
            await stage_hist(client, df_gamma)
        if stage in ("all", "trades"):
            await stage_trades(client, df_gamma)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", choices=["all", "gamma", "hist", "trades"], default="all")
    args = ap.parse_args()
    asyncio.run(run_stages(args.stage))


if __name__ == "__main__":
    main()
