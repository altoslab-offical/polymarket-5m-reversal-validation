#!/usr/bin/env python3
"""
Polymarket 5m Chainlink TWAP Oracle vs Binance Spot Audit Script
================================================================
Fetches 3,000 five-minute klines (10.4 days) and 15,000+ one-minute klines for BTCUSDT, ETHUSDT, SOLUSDT.
Computes 60s TWAP at the start and end of every 5m bucket, audits divergence against Binance spot,
evaluates strategy triggers, queries Polymarket Gamma API on all triggered trades,
and analyzes alpha filter enhancement from twap_gap_pct and intra-candle bounce.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import os
import sys
import time
from typing import Any

import httpx
import numpy as np
import pandas as pd

# Add parent directory to sys.path so we can import engine modules
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from engine.indicators import enrich_indicators
from engine.master_strategy import MasterStrategySpec, evaluate_master_signal_at_index

CONFIG_PATH = os.path.join(BASE_DIR, "data", "polymarket_clob_config.json")
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "polymarket_oracle_twap_audit.json")

BINANCE_KLINES_URL = "https://data-api.binance.vision/api/v3/klines"
GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
SYMBOL_PREFIX = {"BTCUSDT": "btc", "ETHUSDT": "eth", "SOLUSDT": "sol"}

KLINE_COLS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "number_of_trades",
    "taker_buy_base", "taker_buy_quote", "ignore"
]


def load_config() -> dict[str, Any]:
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[WARN] Failed to load config: {e}")
    return {}


async def fetch_binance_klines_forward(
    client: httpx.AsyncClient,
    symbol: str,
    interval: str,
    start_time_ms: int,
    end_time_ms: int,
    step_ms: int,
) -> list[list[Any]]:
    """Fetch klines forward from start_time_ms to end_time_ms."""
    klines: list[list[Any]] = []
    curr = start_time_ms
    while curr < end_time_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": curr,
            "endTime": end_time_ms - 1,
            "limit": 1000,
        }
        for attempt in range(3):
            try:
                resp = await client.get(BINANCE_KLINES_URL, params=params)
                if resp.status_code == 200:
                    batch = resp.json()
                    if not batch:
                        return klines
                    klines.extend(batch)
                    curr = batch[-1][0] + step_ms
                    break
            except Exception as e:
                if attempt == 2:
                    print(f"[ERROR] Failed fetching {symbol} {interval} at {curr}: {e}")
                await asyncio.sleep(0.5)
        await asyncio.sleep(0.04)  # Prevent rate limiting
    return klines


async def query_polymarket_event(
    client_proxy: httpx.AsyncClient | None,
    client_direct: httpx.AsyncClient,
    slug: str,
) -> dict[str, Any]:
    """Query Polymarket Gamma API for an event slug, trying proxy first then direct."""
    # 1. Try proxy if available
    if client_proxy is not None:
        try:
            resp = await client_proxy.get(GAMMA_EVENTS_URL, params={"slug": slug})
            if resp.status_code == 200:
                data = resp.json()
                if data:
                    return parse_gamma_event(data[0], source="proxy")
        except Exception:
            pass

    # 2. Fallback to direct connection
    try:
        resp = await client_direct.get(GAMMA_EVENTS_URL, params={"slug": slug})
        if resp.status_code == 200:
            data = resp.json()
            if data:
                return parse_gamma_event(data[0], source="direct")
    except Exception as e:
        return {"found": False, "error": str(e), "slug": slug}

    return {"found": False, "slug": slug, "error": "not_found"}


def parse_gamma_event(event_dict: dict[str, Any], source: str) -> dict[str, Any]:
    markets = event_dict.get("markets", [])
    if not markets:
        return {"found": False, "source": source, "slug": event_dict.get("slug")}
    m = markets[0]
    em = event_dict.get("eventMetadata") or {}

    raw_prices = m.get("outcomePrices")
    prices = json.loads(raw_prices) if isinstance(raw_prices, str) else (raw_prices or ["0.5", "0.5"])
    up_price = float(prices[0]) if len(prices) >= 1 else 0.5
    down_price = float(prices[1]) if len(prices) >= 2 else 0.5

    ptb = em.get("priceToBeat")
    try:
        price_to_beat = float(ptb) if ptb is not None else None
    except (ValueError, TypeError):
        price_to_beat = None

    return {
        "found": True,
        "source": source,
        "slug": event_dict.get("slug"),
        "title": event_dict.get("title"),
        "question": m.get("question"),
        "priceToBeat": price_to_beat,
        "outcomePrices": prices,
        "up_price": up_price,
        "down_price": down_price,
        "closed": bool(m.get("closed", False)),
        "active": bool(m.get("active", False)),
        "umaResolutionStatuses": m.get("umaResolutionStatuses"),
    }


async def main() -> None:
    print("=" * 80)
    print("⚡ POLYMARKET 5M CLOB / CHAINLINK TWAP ORACLE AUDIT PIPELINE")
    print("=" * 80)

    config = load_config()
    proxy_url = config.get("proxy_url", "socks5://43.245.199.34:1080").strip()
    print(f"[CONFIG] Proxy URL: {proxy_url}")

    # Determine time ranges for 3,000 five-minute candles
    now_sec = int(time.time())
    last_closed_bucket = (now_sec // 300) * 300
    # Last 3,000 candles
    t0_sec = last_closed_bucket - 3000 * 300
    t_end_sec = last_closed_bucket

    start_utc = datetime.datetime.fromtimestamp(t0_sec, tz=datetime.timezone.utc).isoformat()
    end_utc = datetime.datetime.fromtimestamp(t_end_sec, tz=datetime.timezone.utc).isoformat()
    print(f"[TIMERANGE] 3,000 5m candles from {start_utc} to {end_utc} (approx 10.42 days)")

    # 1. Fetch Binance 5m and 1m klines
    print("\n--- [STEP 1] Fetching Binance 5m & 1m Klines ---")
    async with httpx.AsyncClient(timeout=15.0) as binance_client:
        raw_5m: dict[str, list[list[Any]]] = {}
        raw_1m: dict[str, list[list[Any]]] = {}

        for sym in SYMBOLS:
            t_s = time.time()
            print(f"[*] Fetching 3,000 5m candles for {sym}...")
            k5 = await fetch_binance_klines_forward(
                binance_client,
                symbol=sym,
                interval="5m",
                start_time_ms=t0_sec * 1000,
                end_time_ms=t_end_sec * 1000,
                step_ms=300000,
            )
            raw_5m[sym] = k5
            print(f"    -> Received {len(k5)} 5m candles in {time.time()-t_s:.2f}s")

            t_s = time.time()
            print(f"[*] Fetching 15,001 1m candles for {sym}...")
            # We need from t0_sec - 60s up to t_end_sec
            k1 = await fetch_binance_klines_forward(
                binance_client,
                symbol=sym,
                interval="1m",
                start_time_ms=(t0_sec - 60) * 1000,
                end_time_ms=t_end_sec * 1000,
                step_ms=60000,
            )
            raw_1m[sym] = k1
            print(f"    -> Received {len(k1)} 1m candles in {time.time()-t_s:.2f}s")

    # 2. Build 1m TWAP lookup maps
    print("\n--- [STEP 2] Computing 60s 1m TWAP Lookup Maps ---")
    twap_1m_map: dict[str, dict[int, float]] = {}  # symbol -> {open_time_sec: (o+h+l+c)/4}
    for sym in SYMBOLS:
        twap_1m_map[sym] = {}
        for r in raw_1m[sym]:
            sec = int(r[0]) // 1000
            o, h, l, c = float(r[1]), float(r[2]), float(r[3]), float(r[4])
            twap_1m_map[sym][sec] = (o + h + l + c) / 4.0
        print(f"[*] {sym}: Built 1m TWAP lookup for {len(twap_1m_map[sym])} timestamps")

    # 3. Enrich 5m DataFrames with indicators and TWAP metrics
    print("\n--- [STEP 3] Enriching 5m DataFrames with Indicators & TWAP Metrics ---")
    dfs: dict[str, pd.DataFrame] = {}
    enriched_candles: dict[str, list[dict[str, Any]]] = {}

    for sym in SYMBOLS:
        df = pd.DataFrame(raw_5m[sym], columns=KLINE_COLS)
        for col in ["open", "high", "low", "close", "volume", "taker_buy_base"]:
            df[col] = df[col].astype(float)
        df["open_time"] = df["open_time"].astype(int)
        df = enrich_indicators(df)
        dfs[sym] = df

        # Compute TWAP metrics for each 5m candle
        c_list: list[dict[str, Any]] = []
        n = len(df)
        sym_twap = twap_1m_map[sym]

        for i in range(n):
            t_sec = int(df["open_time"].iat[i]) // 1000
            s_open = float(df["open"].iat[i])
            s_high = float(df["high"].iat[i])
            s_low = float(df["low"].iat[i])
            s_close = float(df["close"].iat[i])

            # TWAP start is from 1m candle [t-60, t)
            twap_start = sym_twap.get(t_sec - 60)
            # TWAP end is from 1m candle [t+240, t+300)
            twap_end = sym_twap.get(t_sec + 240)

            # Spot direction: close >= open -> UP, else DOWN
            spot_up = s_close >= s_open
            spot_ret_pct = ((s_close - s_open) / s_open) * 100.0

            # TWAP direction
            if twap_start is not None and twap_end is not None:
                twap_up = twap_end >= twap_start
                twap_ret_pct = ((twap_end - twap_start) / twap_start) * 100.0
                diverged = (spot_up != twap_up)
                twap_gap_pct = ((s_open - twap_start) / twap_start) * 100.0
                # Intra-candle bounce vs TWAP start
                twap_bounce_up_pct = ((s_high - twap_start) / twap_start) * 100.0
                twap_bounce_down_pct = ((twap_start - s_low) / twap_start) * 100.0
            else:
                twap_up = None
                twap_ret_pct = None
                diverged = None
                twap_gap_pct = None
                twap_bounce_up_pct = None
                twap_bounce_down_pct = None

            spot_bounce_up_pct = ((s_high - s_open) / s_open) * 100.0
            spot_bounce_down_pct = ((s_open - s_low) / s_open) * 100.0

            c_list.append({
                "candle_idx": i,
                "open_time_sec": t_sec,
                "datetime_utc": datetime.datetime.fromtimestamp(t_sec, tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M"),
                "spot_open": s_open,
                "spot_high": s_high,
                "spot_low": s_low,
                "spot_close": s_close,
                "spot_up": spot_up,
                "spot_ret_pct": round(spot_ret_pct, 4),
                "spot_bounce_up_pct": round(spot_bounce_up_pct, 4),
                "spot_bounce_down_pct": round(spot_bounce_down_pct, 4),
                "twap_start": round(twap_start, 4) if twap_start else None,
                "twap_end": round(twap_end, 4) if twap_end else None,
                "twap_up": twap_up,
                "twap_ret_pct": round(twap_ret_pct, 4) if twap_ret_pct else None,
                "twap_gap_pct": round(twap_gap_pct, 4) if twap_gap_pct else None,
                "twap_bounce_up_pct": round(twap_bounce_up_pct, 4) if twap_bounce_up_pct else None,
                "twap_bounce_down_pct": round(twap_bounce_down_pct, 4) if twap_bounce_down_pct else None,
                "diverged": diverged,
            })
        enriched_candles[sym] = c_list

    # 4. Global Divergence Audit Across ALL 9,000 Candles
    print("\n--- [STEP 4] Auditing Divergence Across All 9,000 5m Candles ---")
    divergence_summary: dict[str, Any] = {}
    all_divergences: list[dict[str, Any]] = []
    all_gaps: list[float] = []

    for sym in SYMBOLS:
        valid_c = [c for c in enriched_candles[sym] if c["diverged"] is not None]
        total_c = len(valid_c)
        agree_c = sum(1 for c in valid_c if not c["diverged"])
        div_c = sum(1 for c in valid_c if c["diverged"])
        # Divergence Type 1: Spot UP (Green) but TWAP DOWN (Red)
        spot_up_twap_down = sum(1 for c in valid_c if c["spot_up"] and not c["twap_up"])
        # Divergence Type 2: Spot DOWN (Red) but TWAP UP (Green)
        spot_down_twap_up = sum(1 for c in valid_c if not c["spot_up"] and c["twap_up"])

        gaps = [c["twap_gap_pct"] for c in valid_c if c["twap_gap_pct"] is not None]
        all_gaps.extend(gaps)

        # Divergence returns magnitude
        div_returns = [abs(c["spot_ret_pct"]) for c in valid_c if c["diverged"]]
        avg_div_move = float(np.mean(div_returns)) if div_returns else 0.0

        divergence_summary[sym] = {
            "total_candles": total_c,
            "agreement_count": agree_c,
            "agreement_rate_pct": round((agree_c / total_c) * 100.0, 2),
            "divergence_count": div_c,
            "divergence_rate_pct": round((div_c / total_c) * 100.0, 2),
            "spot_green_twap_red_count": spot_up_twap_down,
            "spot_green_twap_red_pct": round((spot_up_twap_down / total_c) * 100.0, 2),
            "spot_red_twap_green_count": spot_down_twap_up,
            "spot_red_twap_green_pct": round((spot_down_twap_up / total_c) * 100.0, 2),
            "avg_abs_spot_move_on_divergence_pct": round(avg_div_move, 4),
            "twap_gap_mean_pct": round(float(np.mean(gaps)), 4),
            "twap_gap_std_pct": round(float(np.std(gaps)), 4),
            "twap_gap_p10_pct": round(float(np.percentile(gaps, 10)), 4),
            "twap_gap_p90_pct": round(float(np.percentile(gaps, 90)), 4),
        }
        print(f"[{sym}] Divergence Rate: {divergence_summary[sym]['divergence_rate_pct']}% "
              f"({div_c}/{total_c}) | Spot Green/TWAP Red: {spot_up_twap_down} | Spot Red/TWAP Green: {spot_down_twap_up}")

    # Combined across all 9,000 candles
    total_all = sum(d["total_candles"] for d in divergence_summary.values())
    div_all = sum(d["divergence_count"] for d in divergence_summary.values())
    agree_all = sum(d["agreement_count"] for d in divergence_summary.values())
    sg_tr_all = sum(d["spot_green_twap_red_count"] for d in divergence_summary.values())
    sr_tg_all = sum(d["spot_red_twap_green_count"] for d in divergence_summary.values())

    combined_summary = {
        "total_candles": total_all,
        "agreement_count": agree_all,
        "agreement_rate_pct": round((agree_all / total_all) * 100.0, 2),
        "divergence_count": div_all,
        "divergence_rate_pct": round((div_all / total_all) * 100.0, 2),
        "spot_green_twap_red_count": sg_tr_all,
        "spot_green_twap_red_pct": round((sg_tr_all / total_all) * 100.0, 2),
        "spot_red_twap_green_count": sr_tg_all,
        "spot_red_twap_green_pct": round((sr_tg_all / total_all) * 100.0, 2),
        "twap_gap_mean_pct": round(float(np.mean(all_gaps)), 4),
        "twap_gap_std_pct": round(float(np.std(all_gaps)), 4),
        "twap_gap_p10_pct": round(float(np.percentile(all_gaps, 10)), 4),
        "twap_gap_p90_pct": round(float(np.percentile(all_gaps, 90)), 4),
    }
    divergence_summary["ALL_COMBINED"] = combined_summary
    print(f"\n[GLOBAL TOTAL] Divergence Rate: {combined_summary['divergence_rate_pct']}% "
          f"({div_all}/{total_all}) | Agreement Rate: {combined_summary['agreement_rate_pct']}%")

    # 5. Evaluate Strategy Signals and Collect Triggered Sequences
    print("\n--- [STEP 5] Evaluating Master Strategy Signals (Last 10 Days) ---")
    spec = MasterStrategySpec()
    n_candles = len(dfs["BTCUSDT"])
    raw_signals: list[dict[str, Any]] = []

    # Non-overlapping live sequence simulation
    sequences: list[dict[str, Any]] = []
    i = 60
    limit = n_candles - 5

    while i < limit:
        cand = evaluate_master_signal_at_index(dfs, i, spec)
        if cand is None:
            i += 1
            continue

        raw_signals.append({
            "candle_idx": i,
            "open_time_sec": int(dfs["BTCUSDT"]["open_time"].iat[i]) // 1000,
            "datetime_utc": datetime.datetime.fromtimestamp(int(dfs["BTCUSDT"]["open_time"].iat[i]) // 1000, tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M"),
            "symbol": cand["symbol"],
            "direction": cand["direction"],
            "score": cand["score"],
            "rsi_6": cand["rsi_6"],
            "mean_rsi_6": cand["mean_rsi_6"],
            "streak": cand["streak"],
            "streak_ret_pct": cand["streak_ret_pct"],
            "vol_ratio": cand["vol_ratio"],
            "taker_buy_ratio": cand["taker_buy_ratio"],
        })

        # Process sequence
        sym = cand["symbol"]
        is_long = cand["direction"] == "LONG_BOUNCE"
        seq_id = len(sequences) + 1

        seq_info: dict[str, Any] = {
            "seq_id": seq_id,
            "trigger_idx": i,
            "trigger_time_sec": int(dfs[sym]["open_time"].iat[i]) // 1000,
            "trigger_datetime_utc": datetime.datetime.fromtimestamp(int(dfs[sym]["open_time"].iat[i]) // 1000, tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M"),
            "symbol": sym,
            "direction": cand["direction"],
            "polymarket_side": cand["polymarket_side"],
            "score": cand["score"],
            "rsi_6": cand["rsi_6"],
            "mean_rsi_6": cand["mean_rsi_6"],
            "streak": cand["streak"],
            "streak_ret_pct": cand["streak_ret_pct"],
            "vol_ratio": cand["vol_ratio"],
            "taker_buy_ratio": cand["taker_buy_ratio"],
            "steps": [],
            "won_2step": False,
            "won_at_step": None,
            "exit_type": None,
        }

        # Step 1 is i+1, Step 2 is i+2
        steps_taken = 0
        for step_num in [1, 2]:
            t_idx = i + step_num
            c_data = enriched_candles[sym][t_idx]
            t_sec = c_data["open_time_sec"]
            slug = f"{SYMBOL_PREFIX[sym]}-updown-5m-{t_sec}"

            # Step outcome flags
            spot_won = (c_data["spot_close"] > c_data["spot_open"]) if is_long else (c_data["spot_close"] < c_data["spot_open"])
            binance_twap_won = (c_data["twap_end"] >= c_data["twap_start"]) if (is_long and c_data["twap_end"] and c_data["twap_start"]) else (c_data["twap_end"] <= c_data["twap_start"] if (c_data["twap_end"] and c_data["twap_start"]) else False)

            spot_bounce = c_data["spot_bounce_up_pct"] if is_long else c_data["spot_bounce_down_pct"]
            twap_bounce = c_data["twap_bounce_up_pct"] if is_long else c_data["twap_bounce_down_pct"]
            bounce_hit = spot_bounce >= spec.target_bounce_pct  # >= 0.10%

            step_entry = {
                "step_num": step_num,
                "candle_idx": t_idx,
                "open_time_sec": t_sec,
                "datetime_utc": c_data["datetime_utc"],
                "slug": slug,
                "spot_open": c_data["spot_open"],
                "spot_high": c_data["spot_high"],
                "spot_low": c_data["spot_low"],
                "spot_close": c_data["spot_close"],
                "spot_won": spot_won,
                "spot_return_pct": c_data["spot_ret_pct"],
                "spot_bounce_pct": spot_bounce,
                "bounce_hit": bounce_hit,
                "twap_start": c_data["twap_start"],
                "twap_end": c_data["twap_end"],
                "twap_gap_pct": c_data["twap_gap_pct"],
                "twap_bounce_pct": twap_bounce,
                "binance_twap_won": binance_twap_won,
                "diverged": c_data["diverged"],
                # Polymarket oracle data to be populated next
                "polymarket": None,
            }
            seq_info["steps"].append(step_entry)
            steps_taken = step_num

            # If won on expiration or bounce, stop this sequence
            if spot_won or bounce_hit:
                seq_info["won_2step"] = True
                seq_info["won_at_step"] = step_num
                seq_info["exit_type"] = "100¢_EXPIRATION" if spot_won else "72¢_BOUNCE_PROTECT"
                break
        else:
            seq_info["won_2step"] = False
            seq_info["won_at_step"] = None
            seq_info["exit_type"] = "0¢_LOSS"

        sequences.append(seq_info)
        i += steps_taken

    print(f"[*] Found {len(raw_signals)} raw signal triggers across 10.4 days.")
    print(f"[*] Generated {len(sequences)} non-overlapping trade sequences.")

    # 6. Query Polymarket Gamma API for all trade steps
    print("\n--- [STEP 6] Querying Polymarket Gamma API for Real Oracle Resolutions ---")
    proxy_client = None
    if proxy_url:
        try:
            proxy_client = httpx.AsyncClient(proxy=proxy_url, timeout=4.0)
        except Exception as e:
            print(f"[WARN] Failed to initialize proxy client: {e}")

    async with httpx.AsyncClient(timeout=8.0) as direct_client:
        all_steps: list[dict[str, Any]] = []
        for s in sequences:
            all_steps.extend(s["steps"])

        print(f"[*] Querying Polymarket Gamma API for {len(all_steps)} trade market slugs...")
        for idx, step in enumerate(all_steps):
            slug = step["slug"]
            poly_res = await query_polymarket_event(proxy_client, direct_client, slug)
            step["polymarket"] = poly_res

            # Calculate polymarket winning status
            if poly_res.get("found"):
                up_p = poly_res.get("up_price", 0.5)
                down_p = poly_res.get("down_price", 0.5)
                # Check direction of parent sequence
                parent_seq = next(s for s in sequences if any(st is step for st in s["steps"]))
                is_long = parent_seq["direction"] == "LONG_BOUNCE"

                poly_won = (up_p >= 0.8) if is_long else (down_p >= 0.8)
                step["poly_oracle_won"] = poly_won
                ptb = poly_res.get("priceToBeat")
                if ptb and step["twap_start"]:
                    step["basis_diff_pct"] = round(((step["twap_start"] - ptb) / ptb) * 100.0, 4)
                else:
                    step["basis_diff_pct"] = None
            else:
                step["poly_oracle_won"] = None
                step["basis_diff_pct"] = None

            if (idx + 1) % 5 == 0 or (idx + 1) == len(all_steps):
                print(f"    [{idx+1}/{len(all_steps)}] Slug: {slug} -> found={poly_res.get('found')} "
                      f"prices={poly_res.get('outcomePrices')} ptb={poly_res.get('priceToBeat')}")
            await asyncio.sleep(0.05)

    if proxy_client:
        await proxy_client.aclose()

    # 7. Aggregate Statistics and Comparisons on Triggered Trades
    print("\n--- [STEP 7] Computing Strategy Win Rates & Oracle Comparison ---")
    total_seq = len(sequences)
    # Step 1 metrics
    s1_steps = [s["steps"][0] for s in sequences if len(s["steps"]) >= 1]
    s1_total = len(s1_steps)
    s1_spot_wins = sum(1 for st in s1_steps if st["spot_won"])
    s1_binance_twap_wins = sum(1 for st in s1_steps if st["binance_twap_won"])
    s1_poly_known = [st for st in s1_steps if st.get("poly_oracle_won") is not None]
    s1_poly_wins = sum(1 for st in s1_poly_known if st["poly_oracle_won"])
    s1_bounce_wins = sum(1 for st in s1_steps if st["bounce_hit"])
    s1_dual_stage_wins = sum(1 for st in s1_steps if st["spot_won"] or st["bounce_hit"])
    s1_poly_dual_stage_wins = sum(1 for st in s1_poly_known if st["poly_oracle_won"] or st["bounce_hit"])

    # Full 2-Step Martingale sequence metrics
    seq_spot_wins = sum(1 for s in sequences if any(st["spot_won"] for st in s["steps"]))
    seq_binance_twap_wins = sum(1 for s in sequences if any(st["binance_twap_won"] for st in s["steps"]))
    seq_poly_wins = sum(1 for s in sequences if any(st.get("poly_oracle_won") for st in s["steps"]))
    seq_dual_stage_wins = sum(1 for s in sequences if s["won_2step"])

    # Polymarket dual-stage: won at expiration on Polymarket or hit bounce
    seq_poly_dual_stage_wins = sum(
        1 for s in sequences
        if any(st.get("poly_oracle_won") or st["bounce_hit"] for st in s["steps"])
    )

    strategy_stats = {
        "total_sequences": total_seq,
        "step1": {
            "total_trades": s1_total,
            "spot_expiration_wins": s1_spot_wins,
            "spot_expiration_win_rate_pct": round((s1_spot_wins / s1_total) * 100.0, 2) if s1_total else 0.0,
            "binance_twap_wins": s1_binance_twap_wins,
            "binance_twap_win_rate_pct": round((s1_binance_twap_wins / s1_total) * 100.0, 2) if s1_total else 0.0,
            "polymarket_oracle_known": len(s1_poly_known),
            "polymarket_oracle_wins": s1_poly_wins,
            "polymarket_oracle_win_rate_pct": round((s1_poly_wins / len(s1_poly_known)) * 100.0, 2) if s1_poly_known else 0.0,
            "intra_candle_bounce_hits": s1_bounce_wins,
            "intra_candle_bounce_hit_rate_pct": round((s1_bounce_wins / s1_total) * 100.0, 2) if s1_total else 0.0,
            "spot_dual_stage_wins": s1_dual_stage_wins,
            "spot_dual_stage_win_rate_pct": round((s1_dual_stage_wins / s1_total) * 100.0, 2) if s1_total else 0.0,
            "polymarket_dual_stage_wins": s1_poly_dual_stage_wins,
            "polymarket_dual_stage_win_rate_pct": round((s1_poly_dual_stage_wins / len(s1_poly_known)) * 100.0, 2) if s1_poly_known else 0.0,
        },
        "martingale_2step": {
            "total_sequences": total_seq,
            "spot_expiration_wins": seq_spot_wins,
            "spot_expiration_win_rate_pct": round((seq_spot_wins / total_seq) * 100.0, 2) if total_seq else 0.0,
            "binance_twap_wins": seq_binance_twap_wins,
            "binance_twap_win_rate_pct": round((seq_binance_twap_wins / total_seq) * 100.0, 2) if total_seq else 0.0,
            "polymarket_oracle_wins": seq_poly_wins,
            "polymarket_oracle_win_rate_pct": round((seq_poly_wins / total_seq) * 100.0, 2) if total_seq else 0.0,
            "spot_dual_stage_wins": seq_dual_stage_wins,
            "spot_dual_stage_win_rate_pct": round((seq_dual_stage_wins / total_seq) * 100.0, 2) if total_seq else 0.0,
            "polymarket_dual_stage_wins": seq_poly_dual_stage_wins,
            "polymarket_dual_stage_win_rate_pct": round((seq_poly_dual_stage_wins / total_seq) * 100.0, 2) if total_seq else 0.0,
        }
    }

    print("\n--- RESULTS PREVIEW ---")
    print(f"Step 1 Spot Expiration Win Rate:        {strategy_stats['step1']['spot_expiration_win_rate_pct']}% ({s1_spot_wins}/{s1_total})")
    print(f"Step 1 Binance TWAP Win Rate:           {strategy_stats['step1']['binance_twap_win_rate_pct']}% ({s1_binance_twap_wins}/{s1_total})")
    print(f"Step 1 Polymarket Oracle Win Rate:      {strategy_stats['step1']['polymarket_oracle_win_rate_pct']}% ({s1_poly_wins}/{len(s1_poly_known)})")
    print(f"Step 1 Polymarket Dual-Stage Win Rate:  {strategy_stats['step1']['polymarket_dual_stage_win_rate_pct']}% ({s1_poly_dual_stage_wins}/{len(s1_poly_known)})")
    print(f"2-Step Polymarket Dual-Stage Win Rate:  {strategy_stats['martingale_2step']['polymarket_dual_stage_win_rate_pct']}% ({seq_poly_dual_stage_wins}/{total_seq})")

    # 8. TWAP Gap Predictive Power (Across ALL 9,000 candles)
    print("\n--- [STEP 8] Analyzing TWAP Gap Predictive Power Across ALL 9,000 Candles ---")
    valid_candles_all = [
        c for sym in SYMBOLS for c in enriched_candles[sym]
        if c.get("twap_up") is not None and c.get("twap_gap_pct") is not None
    ]
    all_gaps_arr = np.array([c["twap_gap_pct"] for c in valid_candles_all], dtype=np.float64)
    all_twap_up_arr = np.array([c["twap_up"] for c in valid_candles_all], dtype=bool)
    baseline_up_rate = float(np.mean(all_twap_up_arr) * 100.0) if len(all_twap_up_arr) else 50.0

    predictive_curve: list[dict[str, Any]] = []
    for th in [0.00, 0.01, 0.02, 0.03, 0.05, 0.08, 0.10]:
        mask_pos = all_gaps_arr >= th
        mask_neg = all_gaps_arr <= -th
        pos_count = int(np.sum(mask_pos))
        neg_count = int(np.sum(mask_neg))
        up_win = float(np.mean(all_twap_up_arr[mask_pos]) * 100.0) if pos_count else 0.0
        down_win = float(np.mean(~all_twap_up_arr[mask_neg]) * 100.0) if neg_count else 0.0
        predictive_curve.append({
            "threshold_pct": th,
            "pos_gap_candle_count": pos_count,
            "pos_gap_up_win_rate_pct": round(up_win, 2),
            "neg_gap_candle_count": neg_count,
            "neg_gap_down_win_rate_pct": round(down_win, 2),
        })
        print(f"Gap >= +{th:0.2f}% ({pos_count:4d} candles): TWAP UP win rate = {up_win:5.2f}% | "
              f"Gap <= -{th:0.2f}% ({neg_count:4d} candles): TWAP DOWN win rate = {down_win:5.2f}%")

    # Strategy filter tests
    filter_tests: list[dict[str, Any]] = []
    for threshold in [-0.02, -0.01, 0.0, 0.01]:
        filtered_s1 = []
        rejected_s1 = []
        for s in sequences:
            st = s["steps"][0]
            is_long = s["direction"] == "LONG_BOUNCE"
            gap = st["twap_gap_pct"]
            if gap is None:
                continue
            is_favorable = (gap >= threshold) if is_long else (gap <= -threshold)
            if is_favorable:
                filtered_s1.append(st)
            else:
                rejected_s1.append(st)

        f_total = len(filtered_s1)
        r_total = len(rejected_s1)

        f_poly_wins = sum(1 for st in filtered_s1 if st.get("poly_oracle_won"))
        f_dual_wins = sum(1 for st in filtered_s1 if st.get("poly_oracle_won") or st["bounce_hit"])
        r_poly_wins = sum(1 for st in rejected_s1 if st.get("poly_oracle_won"))
        r_dual_wins = sum(1 for st in rejected_s1 if st.get("poly_oracle_won") or st["bounce_hit"])

        filter_tests.append({
            "threshold_pct": threshold,
            "accepted_trades": f_total,
            "accepted_poly_oracle_win_rate_pct": round((f_poly_wins / f_total) * 100.0, 2) if f_total else 0.0,
            "accepted_poly_dual_stage_win_rate_pct": round((f_dual_wins / f_total) * 100.0, 2) if f_total else 0.0,
            "rejected_trades": r_total,
            "rejected_poly_oracle_win_rate_pct": round((r_poly_wins / r_total) * 100.0, 2) if r_total else 0.0,
            "rejected_poly_dual_stage_win_rate_pct": round((r_dual_wins / r_total) * 100.0, 2) if r_total else 0.0,
        })

    for ft in filter_tests:
        print(f"Threshold {ft['threshold_pct']:+.2f}% | Accepted: {ft['accepted_trades']} trades -> "
              f"Poly Oracle WR: {ft['accepted_poly_oracle_win_rate_pct']}% | "
              f"Dual-Stage WR: {ft['accepted_poly_dual_stage_win_rate_pct']}% "
              f"(Rejected {ft['rejected_trades']} trades with {ft['rejected_poly_dual_stage_win_rate_pct']}% WR)")

    # 9. Basis Difference Analysis (Binance 1m TWAP vs Chainlink PriceToBeat)
    basis_diffs = [st["basis_diff_pct"] for s in sequences for st in s["steps"] if st.get("basis_diff_pct") is not None]
    basis_stats = {
        "sample_count": len(basis_diffs),
        "mean_basis_pct": round(float(np.mean(basis_diffs)), 4) if basis_diffs else 0.0,
        "std_basis_pct": round(float(np.std(basis_diffs)), 4) if basis_diffs else 0.0,
        "max_abs_basis_pct": round(float(np.max(np.abs(basis_diffs))), 4) if basis_diffs else 0.0,
    }

    # 10. Prepare Full Output Payload
    output_payload: dict[str, Any] = {
        "metadata": {
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "time_range_start_utc": start_utc,
            "time_range_end_utc": end_utc,
            "period_days": round((t_end_sec - t0_sec) / 86400.0, 2),
            "symbols": SYMBOLS,
            "total_5m_candles_per_symbol": 3000,
            "total_5m_candles_analyzed": total_all,
            "total_1m_candles_fetched": sum(len(raw_1m[s]) for s in SYMBOLS),
            "proxy_url": proxy_url,
        },
        "general_divergence_audit": divergence_summary,
        "twap_gap_predictive_power_curve": predictive_curve,
        "chainlink_basis_audit": basis_stats,
        "strategy_performance": strategy_stats,
        "alpha_twap_gap_filter": filter_tests,
        "trade_sequences": sequences,
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output_payload, f, indent=2, ensure_ascii=False)

    print(f"\n[DONE] Successfully saved full audit results to:\n  {OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
