#!/usr/bin/env python3
"""
Fetch 2 full years (730 days) of 5-minute klines from Binance for BTCUSDT, ETHUSDT, and SOLUSDT.
Uses aiohttp and asyncio with concurrency limit, retry logic, and fallback endpoint.
Saves parsed data to Parquet files in data/ directory.
"""

import asyncio
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import aiohttp
import numpy as np
import pandas as pd

PRIMARY_URL = "https://data-api.binance.vision/api/v3/klines"
FALLBACK_URL = "https://api.binance.com/api/v3/klines"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
INTERVAL = "5m"
INTERVAL_MS = 5 * 60 * 1000  # 300,000 ms
CHUNK_SIZE = 1000  # Binance max limit
CHUNK_SPAN_MS = CHUNK_SIZE * INTERVAL_MS  # 300,000,000 ms (~3.47 days)
DAYS_BACK = 730
CONCURRENCY_LIMIT = 10
DATA_DIR = Path("/Users/tommy/Desktop/預測市場交易/data")


def get_time_chunks(start_ms: int, end_ms: int) -> List[Tuple[int, int]]:
    """Pre-calculate non-overlapping chunk ranges (start_ms, end_ms) for 1000-candle intervals."""
    chunks = []
    curr = start_ms
    while curr < end_ms:
        c_end = min(curr + CHUNK_SPAN_MS - 1, end_ms)
        chunks.append((curr, c_end))
        curr += CHUNK_SPAN_MS
    return chunks


async def fetch_chunk(
    session: aiohttp.ClientSession,
    symbol: str,
    start_ms: int,
    end_ms: int,
    sem: asyncio.Semaphore,
    max_retries: int = 5,
) -> list:
    """Fetch a single chunk of klines with semaphore throttling and fallback endpoints."""
    params = {
        "symbol": symbol,
        "interval": INTERVAL,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": CHUNK_SIZE,
    }
    headers = {"User-Agent": "Mozilla/5.0 (compatible; PredictionMarketDataFetcher/1.0)"}
    endpoints = [PRIMARY_URL, FALLBACK_URL]

    async with sem:
        for attempt in range(1, max_retries + 1):
            for url in endpoints:
                try:
                    async with session.get(
                        url,
                        params=params,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=15, connect=5),
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json(content_type=None)
                            if isinstance(data, list):
                                return data
                        elif resp.status in (429, 418):
                            retry_after = float(resp.headers.get("Retry-After", 2.0))
                            await asyncio.sleep(retry_after + 0.5 * attempt)
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    pass
            await asyncio.sleep(0.3 * (2 ** (attempt - 1)))

        raise RuntimeError(
            f"Failed to fetch {symbol} [{start_ms}, {end_ms}] after {max_retries} retries across endpoints."
        )


async def fetch_symbol_chunks(
    session: aiohttp.ClientSession,
    symbol: str,
    chunks: List[Tuple[int, int]],
    sem: asyncio.Semaphore,
    progress_callback,
) -> list:
    """Launch concurrent fetch tasks for all chunks of a symbol."""
    async def fetch_with_callback(s: int, e: int):
        data = await fetch_chunk(session, symbol, s, e, sem)
        progress_callback(symbol)
        return data

    tasks = [fetch_with_callback(s, e) for s, e in chunks]
    results = await asyncio.gather(*tasks)
    flat_rows = [row for chunk_result in results for row in chunk_result]
    return flat_rows


def parse_klines_to_df(rows: list, max_close_time_ms: int) -> pd.DataFrame:
    """
    Parse raw Binance klines to DataFrame with requested types and column ordering:
    - open_time (int64 ms)
    - timestamp (datetime64[ns, UTC])
    - open, high, low, close, volume, quote_volume, taker_buy_base, taker_buy_quote (float64)
    - trades (int64)
    - close_time (int64 ms)
    """
    if not rows:
        return pd.DataFrame()

    raw_cols = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "trades",
        "taker_buy_base",
        "taker_buy_quote",
        "ignore",
    ]
    df = pd.DataFrame(rows, columns=raw_cols)

    # Cast integer fields
    df["open_time"] = df["open_time"].astype("int64")
    df["close_time"] = df["close_time"].astype("int64")
    df["trades"] = df["trades"].astype("int64")

    # Cast UTC timestamp
    df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True).astype("datetime64[ns, UTC]")

    # Cast float fields
    float_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "taker_buy_base",
        "taker_buy_quote",
    ]
    for col in float_cols:
        df[col] = df[col].astype("float64")

    target_columns = [
        "open_time",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "taker_buy_base",
        "taker_buy_quote",
        "trades",
        "close_time",
    ]
    df = df[target_columns]

    # Deduplicate by open_time and sort ascending
    df = df.drop_duplicates(subset=["open_time"]).sort_values("open_time").reset_index(drop=True)

    # Drop unclosed candle
    df = df[df["close_time"] <= max_close_time_ms].reset_index(drop=True)

    return df


def calculate_streak_stats(df: pd.DataFrame) -> dict:
    """Calculate streak and candle statistics."""
    total_rows = len(df)
    if total_rows == 0:
        return {}

    start_date = df["timestamp"].iloc[0].strftime("%Y-%m-%d %H:%M:%S UTC")
    end_date = df["timestamp"].iloc[-1].strftime("%Y-%m-%d %H:%M:%S UTC")

    is_up = df["close"] > df["open"]
    is_down = df["close"] < df["open"]
    is_flat = df["close"] == df["open"]

    # Calculate up streaks
    up_blocks = (is_up != is_up.shift()).cumsum()
    up_runs = is_up.groupby(up_blocks).sum()
    up_runs = up_runs[up_runs > 0]
    max_up_streak = int(up_runs.max()) if not up_runs.empty else 0
    avg_up_streak = float(up_runs.mean()) if not up_runs.empty else 0.0

    # Calculate down streaks
    down_blocks = (is_down != is_down.shift()).cumsum()
    down_runs = is_down.groupby(down_blocks).sum()
    down_runs = down_runs[down_runs > 0]
    max_down_streak = int(down_runs.max()) if not down_runs.empty else 0
    avg_down_streak = float(down_runs.mean()) if not down_runs.empty else 0.0

    # Candle returns
    returns = ((df["close"] / df["open"]) - 1.0) * 100.0

    return {
        "row_count": total_rows,
        "start_date": start_date,
        "end_date": end_date,
        "up_candles": int(is_up.sum()),
        "down_candles": int(is_down.sum()),
        "flat_candles": int(is_flat.sum()),
        "max_up_streak": max_up_streak,
        "avg_up_streak": round(avg_up_streak, 2),
        "max_down_streak": max_down_streak,
        "avg_down_streak": round(avg_down_streak, 2),
        "max_5m_gain_pct": round(float(returns.max()), 2),
        "max_5m_drop_pct": round(float(returns.min()), 2),
    }


async def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    now_utc = datetime.now(timezone.utc)
    current_ms = int(now_utc.timestamp() * 1000)
    start_ms = current_ms - (DAYS_BACK * 24 * 60 * 60 * 1000)
    # Align start_ms to 5m interval boundary
    start_ms = (start_ms // INTERVAL_MS) * INTERVAL_MS

    chunks = get_time_chunks(start_ms, current_ms)
    chunks_per_symbol = len(chunks)
    total_requests = chunks_per_symbol * len(SYMBOLS)

    print(f"=== Binance 5m Kline Fetcher ===")
    print(f"Target: {DAYS_BACK} days ({len(SYMBOLS)} symbols: {', '.join(SYMBOLS)})")
    print(
        f"Time range: {datetime.fromtimestamp(start_ms/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} -> {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )
    print(f"Chunks per symbol: {chunks_per_symbol} (Total HTTP requests: {total_requests})")
    print(f"Concurrency Semaphore: {CONCURRENCY_LIMIT}")

    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    completed_counts = {sym: 0 for sym in SYMBOLS}
    t0 = time.time()

    def on_chunk_done(sym: str):
        completed_counts[sym] += 1
        total_done = sum(completed_counts.values())
        if total_done % 50 == 0 or total_done == total_requests:
            elapsed = time.time() - t0
            rate = total_done / elapsed if elapsed > 0 else 0
            pct = (total_done / total_requests) * 100
            print(
                f"[{elapsed:5.1f}s] Fetched {total_done}/{total_requests} chunks ({pct:5.1f}%) | {rate:4.1f} req/s"
            )

    connector = aiohttp.TCPConnector(limit=100, limit_per_host=30, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        symbol_tasks = [
            fetch_symbol_chunks(session, sym, chunks, sem, on_chunk_done)
            for sym in SYMBOLS
        ]
        all_symbols_data = await asyncio.gather(*symbol_tasks)

    elapsed_fetch = time.time() - t0
    print(f"\nAll chunks downloaded in {elapsed_fetch:.2f} seconds.")

    # Save to Parquet and calculate stats
    stats_dict = {}
    now_post_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    for sym, raw_rows in zip(SYMBOLS, all_symbols_data):
        df = parse_klines_to_df(raw_rows, max_close_time_ms=now_post_ms)
        parquet_path = DATA_DIR / f"{sym}_5m.parquet"
        df.to_parquet(parquet_path, engine="pyarrow", index=False)

        stats = calculate_streak_stats(df)
        stats_dict[sym] = stats

        print(f"\n[Symbol: {sym}]")
        print(f"  Saved to: {parquet_path}")
        print(f"  Rows: {stats['row_count']:,}")
        print(f"  Date range: {stats['start_date']} -> {stats['end_date']}")
        print(
            f"  Candles: Up={stats['up_candles']:,} ({stats['up_candles']/stats['row_count']*100:.1f}%), "
            f"Down={stats['down_candles']:,} ({stats['down_candles']/stats['row_count']*100:.1f}%), "
            f"Flat={stats['flat_candles']:,}"
        )
        print(
            f"  Up streak: max={stats['max_up_streak']} consecutive, avg={stats['avg_up_streak']:.2f}"
        )
        print(
            f"  Down streak: max={stats['max_down_streak']} consecutive, avg={stats['avg_down_streak']:.2f}"
        )
        print(
            f"  5m Return Extremes: Max Gain=+{stats['max_5m_gain_pct']}%, Max Drop={stats['max_5m_drop_pct']}%"
        )

    print("\nFetch and Parquet generation finished successfully!")


if __name__ == "__main__":
    asyncio.run(main())
