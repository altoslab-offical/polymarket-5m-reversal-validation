"""Polymarket Chainlink RTDS spot + official 60s TWAP feed client."""
from __future__ import annotations

import asyncio
from collections import deque
import json
import time
from typing import Any

import websockets

RTDS_WS_URL = "wss://ws-live-data.polymarket.com"

CHAINLINK_SYMBOL_MAP = {
    "btc/usd": "BTCUSDT",
    "eth/usd": "ETHUSDT",
    "sol/usd": "SOLUSDT",
}


class ChainlinkRTDSFeed:
    """
    Pure Polymarket Chainlink Data Streams RTDS Client (`wss://ws-live-data.polymarket.com`).

    Subscribes 24/7 to BOTH:
    - `crypto_prices_chainlink` (`btc/usd`, `eth/usd`, `sol/usd`): 1-second Chainlink spot price
    - `crypto_prices_twap_sixty` (`btc/usd`, `eth/usd`, `sol/usd`): official Polymarket 60-second Chainlink TWAP
      (bit-for-bit identical to `data.chain.link/streams/*-usd-twap-60s-streams` & PolyBolt `price.crypto.twap`)

    Maintains:
    - Live 1-second Chainlink spot and official 60s TWAP per symbol
    - Exact 5m bucket opening anchor (`bucket_ts * 1000` -> official Polymarket `priceToBeat` anchor)
    - Time-weighted step-integral 60s TWAP fallback from spot ticks if the TWAP topic lags
    """

    def __init__(self, proxy_url: str | None = None, max_ticks_per_symbol: int = 25000) -> None:
        self.proxy_url = (proxy_url or "").strip() or None
        self.max_ticks = max(1000, int(max_ticks_per_symbol))
        self.ticks: dict[str, deque[tuple[int, float]]] = {
            "BTCUSDT": deque(maxlen=self.max_ticks),
            "ETHUSDT": deque(maxlen=self.max_ticks),
            "SOLUSDT": deque(maxlen=self.max_ticks),
        }
        self.twap_ticks: dict[str, deque[tuple[int, float]]] = {
            "BTCUSDT": deque(maxlen=self.max_ticks),
            "ETHUSDT": deque(maxlen=self.max_ticks),
            "SOLUSDT": deque(maxlen=self.max_ticks),
        }
        # sym -> {bucket_ts_sec: (anchor_price, tick_ts_ms)}
        self.bucket_opens: dict[str, dict[int, tuple[float, int]]] = {
            "BTCUSDT": {},
            "ETHUSDT": {},
            "SOLUSDT": {},
        }
        self.twap_bucket_opens: dict[str, dict[int, tuple[float, int]]] = {
            "BTCUSDT": {},
            "ETHUSDT": {},
            "SOLUSDT": {},
        }
        self.connected: bool = False
        self.total_ticks: int = 0
        self.total_twap_ticks: int = 0
        self.last_tick_recv_ms: int = 0
        self.last_error: str | None = None
        self._running: bool = False
        self._task: asyncio.Task[None] | None = None

    @staticmethod
    def _record_bucket_open(
        table: dict[str, dict[int, tuple[float, int]]], sym: str, value: float, ts_ms: int
    ) -> None:
        sec = int(ts_ms // 1000)
        b_floor = (sec // 300) * 300
        b_next = b_floor + 300
        for cand_b in (b_floor, b_next):
            b_ms = cand_b * 1000
            dt_ms = ts_ms - b_ms
            if -1500 <= dt_ms <= 15000:
                existing = table[sym].get(cand_b)
                if existing is None or abs(dt_ms) < abs(existing[1] - b_ms):
                    table[sym][cand_b] = (float(value), int(ts_ms))
        if len(table[sym]) > 120:
            cutoff_b = b_floor - 8 * 3600
            for old_b in [k for k in table[sym] if k < cutoff_b]:
                table[sym].pop(old_b, None)

    def ingest_tick(self, raw_symbol: str, value: float, ts_ms: int) -> None:
        sym = CHAINLINK_SYMBOL_MAP.get(str(raw_symbol).strip().lower())
        if not sym or value <= 0.0 or ts_ms <= 0:
            return
        buf = self.ticks[sym]
        if buf and buf[-1][0] == ts_ms and buf[-1][1] == value:
            return
        buf.append((int(ts_ms), float(value)))
        self.total_ticks += 1
        self.last_tick_recv_ms = int(time.time() * 1000)
        self._record_bucket_open(self.bucket_opens, sym, value, ts_ms)

    def ingest_twap_tick(self, raw_symbol: str, value: float, ts_ms: int) -> None:
        sym = CHAINLINK_SYMBOL_MAP.get(str(raw_symbol).strip().lower())
        if not sym or value <= 0.0 or ts_ms <= 0:
            return
        buf = self.twap_ticks[sym]
        if buf:
            if ts_ms == buf[-1][0]:
                if buf[-1][1] == value:
                    return
            elif ts_ms < buf[-1][0]:
                # Avoid duplicates from snapshot replays when out of order
                if any(t == ts_ms for t, _ in list(buf)[-120:]):
                    return
                buf.append((int(ts_ms), float(value)))
                ordered = sorted(buf, key=lambda x: x[0])
                buf.clear()
                buf.extend(ordered)
                self._record_bucket_open(self.twap_bucket_opens, sym, value, ts_ms)
                return
        buf.append((int(ts_ms), float(value)))
        self.total_twap_ticks += 1
        self.last_tick_recv_ms = int(time.time() * 1000)
        self._record_bucket_open(self.twap_bucket_opens, sym, value, ts_ms)

    def get_latest_price(self, symbol: str, max_age_sec: float = 20.0) -> tuple[float | None, float]:
        buf = self.ticks.get(symbol.upper())
        if not buf:
            return None, 9999.0
        ts_ms, val = buf[-1]
        age_sec = max(0.0, (time.time() * 1000.0 - float(ts_ms)) / 1000.0)
        if age_sec > max_age_sec:
            return None, round(age_sec, 2)
        return float(val), round(age_sec, 2)

    def get_latest_twap(self, symbol: str, max_age_sec: float = 20.0) -> tuple[float | None, float]:
        buf = self.twap_ticks.get(symbol.upper())
        if not buf:
            return None, 9999.0
        ts_ms, val = buf[-1]
        age_sec = max(0.0, (time.time() * 1000.0 - float(ts_ms)) / 1000.0)
        if age_sec > max_age_sec:
            return None, round(age_sec, 2)
        return float(val), round(age_sec, 2)

    def get_settlement_twap_at(self, symbol: str, target_ts_sec: int, max_dist_ms: int = 4000) -> float | None:
        """Exact official Chainlink 60s TWAP tick at a 5m bucket boundary (`target_ts_sec * 1000`)."""
        sym = symbol.upper()
        anchor = self.twap_bucket_opens.get(sym, {}).get(int(target_ts_sec))
        target_ms = int(target_ts_sec) * 1000
        if anchor is not None and abs(anchor[1] - target_ms) <= max_dist_ms:
            return float(anchor[0])
        buf = self.twap_ticks.get(sym)
        if buf:
            best_val = None
            best_dist = max_dist_ms + 1
            for ts_ms, val in buf:
                dist = abs(ts_ms - target_ms)
                if dist < best_dist:
                    best_dist = dist
                    best_val = float(val)
            if best_val is not None:
                return best_val
        return None

    def get_bucket_open_price(self, symbol: str, bucket_ts_sec: int) -> float | None:
        sym = symbol.upper()
        twap_open = self.get_settlement_twap_at(sym, int(bucket_ts_sec))
        if twap_open is not None:
            return twap_open
        twap_calc, _ = self.get_60s_twap(sym, end_ts_ms=int(bucket_ts_sec) * 1000, min_ticks=20)
        if twap_calc is not None:
            return twap_calc
        anchor = self.bucket_opens.get(sym, {}).get(int(bucket_ts_sec))
        if anchor is not None:
            return float(anchor[0])
        buf = self.ticks.get(sym)
        if not buf:
            return None
        target_ms = int(bucket_ts_sec) * 1000
        best_val = None
        best_dist = 15001
        for ts_ms, val in buf:
            dist = abs(ts_ms - target_ms)
            if dist < best_dist:
                best_dist = dist
                best_val = float(val)
        return best_val

    def get_60s_twap(
        self,
        symbol: str,
        end_ts_ms: int | None = None,
        min_ticks: int = 12,
    ) -> tuple[float | None, int]:
        """Official 60s Chainlink TWAP at `end_ts_ms`, or time-weighted step-integral fallback."""
        sym = symbol.upper()
        end_ms = int(end_ts_ms if end_ts_ms is not None else time.time() * 1000)
        twap_buf = self.twap_ticks.get(sym)
        if twap_buf:
            best_val = None
            best_dist = 5001
            for ts_ms, val in twap_buf:
                dist = abs(ts_ms - end_ms)
                if dist < best_dist:
                    best_dist = dist
                    best_val = float(val)
            if best_val is not None:
                return best_val, 60

        buf = self.ticks.get(sym)
        if not buf:
            return None, 0
        start_ms = end_ms - 60_000
        pts = [(ts_ms, val) for ts_ms, val in buf if start_ms - 5000 <= ts_ms <= end_ms]
        in_win = [p for p in pts if p[0] >= start_ms]
        if len(in_win) < min_ticks:
            return None, len(in_win)
        # Time-weighted step-integral across [start_ms, end_ms]
        total_w = 0.0
        weighted_sum = 0.0
        for idx, (ts_ms, val) in enumerate(pts):
            t0 = max(start_ms, ts_ms)
            t1 = end_ms if idx + 1 >= len(pts) else min(end_ms, pts[idx + 1][0])
            dt = max(0, t1 - t0)
            if dt > 0:
                weighted_sum += float(val) * float(dt)
                total_w += float(dt)
        if total_w <= 0.0:
            return float(sum(v for _, v in in_win) / len(in_win)), len(in_win)
        return float(weighted_sum / total_w), len(in_win)

    def get_status(self) -> dict[str, Any]:
        prices: dict[str, Any] = {}
        for sym in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
            px, age = self.get_latest_price(sym, max_age_sec=60.0)
            tw, tw_age = self.get_latest_twap(sym, max_age_sec=60.0)
            prices[sym] = {
                "price_usd": round(px, 4) if px is not None else None,
                "twap60_usd": round(tw, 4) if tw is not None else None,
                "age_sec": age,
                "twap_age_sec": tw_age,
                "ticks_cached": len(self.ticks[sym]),
                "twap_ticks_cached": len(self.twap_ticks[sym]),
            }
        return {
            "source": "polymarket_chainlink_rtds_plus_twap60",
            "ws_url": RTDS_WS_URL,
            "connected": bool(self.connected),
            "total_ticks": int(self.total_ticks),
            "total_twap_ticks": int(self.total_twap_ticks),
            "last_tick_age_sec": round(max(0.0, (time.time() * 1000.0 - self.last_tick_recv_ms) / 1000.0), 1)
            if self.last_tick_recv_ms > 0
            else None,
            "last_error": self.last_error,
            "prices": prices,
        }

    def _ingest_rtds_message(self, data: dict[str, Any]) -> None:
        topic = str(data.get("topic") or "")
        payload = data.get("payload")
        if not isinstance(payload, dict):
            return
        raw_sym = payload.get("symbol")
        if not raw_sym:
            return
        is_twap = topic == "crypto_prices_twap_sixty"
        ingest = self.ingest_twap_tick if is_twap else self.ingest_tick
        arr = payload.get("data")
        if isinstance(arr, list):
            for pt in arr:
                if isinstance(pt, dict) and pt.get("value") is not None and pt.get("timestamp") is not None:
                    ingest(str(raw_sym), float(pt["value"]), int(pt["timestamp"]))
            return
        val = payload.get("value")
        ts = payload.get("timestamp")
        if val is not None and ts is not None:
            ingest(str(raw_sym), float(val), int(ts))

    async def _consume_ws(self, ws: Any, sub_msg: str) -> None:
        self.connected = True
        self.last_error = None
        await ws.send(sub_msg)
        while self._running:
            raw = await asyncio.wait_for(ws.recv(), timeout=20.0)
            if not raw or not raw.strip():
                continue
            data = json.loads(raw)
            if isinstance(data, dict):
                self._ingest_rtds_message(data)

    async def _run_loop(self) -> None:
        self._running = True
        sub_msg = json.dumps({
            "action": "subscribe",
            "subscriptions": [
                {"topic": "crypto_prices_chainlink", "type": "*"},
                {"topic": "crypto_prices_twap_sixty", "type": "*"},
            ],
        })
        while self._running:
            try:
                async with websockets.connect(
                    RTDS_WS_URL,
                    open_timeout=8.0,
                    ping_interval=15.0,
                    ping_timeout=10.0,
                    close_timeout=3.0,
                ) as ws:
                    await self._consume_ws(ws, sub_msg)
            except asyncio.CancelledError:
                self.connected = False
                raise
            except Exception as e:
                self.connected = False
                self.last_error = f"{type(e).__name__}: {e}"
                await asyncio.sleep(2.0)

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run_loop())

    def stop(self) -> None:
        self._running = False
        self.connected = False
        if self._task is not None and not self._task.done():
            self._task.cancel()
