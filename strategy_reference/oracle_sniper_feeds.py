"""Second-level market data hub for the Chainlink sniper (BTC / ETH / SOL).

* CEX lead price: Binance (aggTrade + bookTicker WebSocket) blended with Coinbase ticker, USDT-basis adjusted.
* Polymarket CLOB: executable (VWAP-filtered) best bid/ask of the Up/Down tokens via WebSocket with REST fallback.
* Window metadata (token ids, official `priceToBeat` strike) comes from `PolymarketAutoTrader.discover_live_5m_market`.
The Chainlink RTDS price itself is read from the shared `ChainlinkRTDSFeed` owned by the scanner.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import json
import time
from typing import Any, Optional

import aiohttp
import websockets

from engine.oracle_sniper import DEFAULT_SNIPER_CONFIG, OrderBookTop, RollingMicrostructureTracker, SniperConfig

SYMBOLS: tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
COINBASE_PRODUCT = {"BTCUSDT": "BTC-USD", "ETHUSDT": "ETH-USD", "SOLUSDT": "SOL-USD"}
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream?streams=" + "/".join(
    f"{s.lower()}@{kind}" for s in SYMBOLS for kind in ("aggTrade", "bookTicker")
)
BINANCE_REST_BOOK_URL = "https://data-api.binance.vision/api/v3/ticker/bookTicker"
COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
COINBASE_REST_TICKER = "https://api.exchange.coinbase.com/products/{product}/ticker"
POLY_CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CLOB_BOOK_URL = "https://clob.polymarket.com/book"
BOOK_TARGET_USDC = 25.0  # depth filter used when the source database was recorded


# --------------------------------------------------------------------------------------
# Order-book helpers (pure)
# --------------------------------------------------------------------------------------


def _levels(raw: list[dict[str, Any]], reverse: bool) -> list[tuple[float, float]]:
    return sorted(_levels_map(raw).items(), key=lambda t: t[0], reverse=reverse)


def _levels_map(raw: list[dict[str, Any]]) -> dict[float, float]:
    out: dict[float, float] = {}
    for x in raw:
        px, sz = float(x.get("price") or 0), float(x.get("size") or 0)
        if sz > 0 and 0.0 < px < 1.0:
            out[px] = sz
    return out


def _executable_from_sorted(
    bids: list[tuple[float, float]], asks: list[tuple[float, float]], target_usdc: float = BOOK_TARGET_USDC
) -> tuple[Optional[float], Optional[float]]:
    vwap_ask = sweep_vwap(asks, target_usdc / max(0.20, asks[0][0])) if asks else None
    vwap_bid = sweep_vwap(bids, target_usdc / max(0.20, min(0.85, bids[0][0]))) if bids else None
    if vwap_bid is not None and vwap_ask is not None and vwap_bid >= vwap_ask:
        return None, None
    return vwap_bid, vwap_ask


def sweep_vwap(
    levels: list[tuple[float, float]], target_shares: float, max_slip_from_top: float = 0.03
) -> Optional[float]:
    """Executable VWAP across best-first levels; rejects dust (<40% of target within 3c of the top)."""
    if not levels:
        return None
    top_p = levels[0][0]
    if top_p >= 0.98 or top_p <= 0.02:
        return top_p
    filled = 0.0
    cost = 0.0
    for price, size in levels:
        if abs(price - top_p) > max_slip_from_top:
            break
        take = min(size, target_shares - filled)
        filled += take
        cost += take * price
        if filled >= target_shares - 1e-6:
            break
    if filled < max(5.0, 0.40 * target_shares):
        return None
    return round(cost / filled, 4)


def extract_executable_bid_ask(
    book_payload: dict[str, Any], target_usdc: float = BOOK_TARGET_USDC
) -> tuple[Optional[float], Optional[float]]:
    """Depth-aware VWAP bid/ask so 0.5-share dust quotes never look executable."""
    bids = _levels(book_payload.get("bids") or [], reverse=True)
    asks = _levels(book_payload.get("asks") or [], reverse=False)
    return _executable_from_sorted(bids, asks, target_usdc=target_usdc)


def parse_book_ts_sec(payload: dict[str, Any]) -> float:
    """Polymarket book timestamp in seconds (accepts ms or s)."""
    raw = payload.get("timestamp")
    if raw is None:
        return 0.0
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return 0.0
    return v / 1000.0 if v > 1e11 else v


# --------------------------------------------------------------------------------------
# Per-symbol state
# --------------------------------------------------------------------------------------


@dataclass
class SymbolState:
    """Everything the sniper needs to know about one coin."""

    symbol: str
    tracker: RollingMicrostructureTracker
    binance_price: Optional[float] = None
    binance_ts: float = 0.0
    coinbase_price: Optional[float] = None
    coinbase_ts: float = 0.0
    ratio_ewma: float = 1.0
    ratio_ready: bool = False
    window_ts: int = 0
    slug: str = ""
    up_token: Optional[str] = None
    down_token: Optional[str] = None
    condition_id: str = ""
    price_to_beat: Optional[float] = None
    min_tick_size: float = 0.01
    neg_risk: bool = False
    book: OrderBookTop = field(default_factory=OrderBookTop)
    up_bids_raw: dict[float, float] = field(default_factory=dict)
    up_asks_raw: dict[float, float] = field(default_factory=dict)
    down_bids_raw: dict[float, float] = field(default_factory=dict)
    down_asks_raw: dict[float, float] = field(default_factory=dict)
    up_book_ts: float = 0.0
    down_book_ts: float = 0.0
    last_ws_book_local_ts: float = 0.0
    up_local_ts: float = 0.0
    down_local_ts: float = 0.0
    meta_checked_ts: float = 0.0
    prefetch_ts: float = 0.0

    def update_basis(self, now: float) -> None:
        """Track the Binance-USDT / Coinbase-USD ratio with a slow EWMA."""
        cb, bn = self.coinbase_price, self.binance_price
        if not cb or not bn or cb <= 0 or (now - self.coinbase_ts) >= 8.0 or (now - self.binance_ts) >= 8.0:
            return
        ratio = bn / cb
        if not 0.98 < ratio < 1.02:
            return
        if not self.ratio_ready:
            self.ratio_ewma, self.ratio_ready = ratio, True
        else:
            self.ratio_ewma = 0.02 * ratio + 0.98 * self.ratio_ewma

    def cex_lead_price(self, now: float) -> Optional[float]:
        """Coinbase / Binance blend in USD terms (55% / 45%), falling back to whichever side is fresh."""
        cb_fresh = self.coinbase_price is not None and (now - self.coinbase_ts) < 10.0
        bn_fresh = self.binance_price is not None and (now - self.binance_ts) < 10.0
        bn_usd = (float(self.binance_price) / self.ratio_ewma) if (bn_fresh and self.binance_price) else None
        if cb_fresh and bn_usd is not None:
            return 0.55 * float(self.coinbase_price or 0.0) + 0.45 * bn_usd
        if cb_fresh:
            return float(self.coinbase_price or 0.0)
        return bn_usd

    def reset_for_window(self, window_ts: int) -> None:
        """Clear per-window market state at the 5-minute boundary."""
        self.window_ts = window_ts
        self.slug = ""
        self.up_token = None
        self.down_token = None
        self.condition_id = ""
        self.price_to_beat = None
        self.book = OrderBookTop()
        self.up_bids_raw.clear()
        self.up_asks_raw.clear()
        self.down_bids_raw.clear()
        self.down_asks_raw.clear()
        self.up_book_ts = 0.0
        self.down_book_ts = 0.0
        self.last_ws_book_local_ts = 0.0
        self.up_local_ts = 0.0
        self.down_local_ts = 0.0
        self.meta_checked_ts = 0.0

    def apply_market(self, market: dict[str, Any]) -> None:
        """Absorb `discover_live_5m_market` output for this symbol's current window."""
        self.slug = str(market.get("slug", self.slug))
        self.up_token = str(market.get("up_token_id") or "") or self.up_token
        self.down_token = str(market.get("down_token_id") or "") or self.down_token
        self.condition_id = str(market.get("condition_id", self.condition_id))
        self.min_tick_size = float(market.get("min_tick_size") or 0.01)
        self.neg_risk = bool(market.get("neg_risk", False))
        ptb = market.get("price_to_beat")
        if ptb is not None:
            self.price_to_beat = float(ptb)


# --------------------------------------------------------------------------------------
# Hub
# --------------------------------------------------------------------------------------


class SniperMarketHub:
    """Owns the WebSocket / REST feeds and per-symbol state for the sniper."""

    def __init__(self, auto_trader: Any, cfg: SniperConfig = DEFAULT_SNIPER_CONFIG) -> None:
        self.auto_trader = auto_trader
        self.cfg = cfg
        self.states: dict[str, SymbolState] = {
            s: SymbolState(s, RollingMicrostructureTracker(cfg.vol_lookback_sec, cfg.cvd_window_sec)) for s in SYMBOLS
        }
        self.prefetched: dict[tuple[str, int], dict[str, Any]] = {}
        self.stats: dict[str, Any] = {"binance_ws": False, "coinbase_ws": False, "clob_ws": False}
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._meta_tasks: set[asyncio.Task[None]] = set()
        self._session: Optional[aiohttp.ClientSession] = None

    # ----- lifecycle -----
    def start(self) -> None:
        """Launch background feed tasks (idempotent)."""
        if self._tasks and not all(t.done() for t in self._tasks):
            return
        self._stop.clear()
        loops = (self._binance_ws_loop, self._coinbase_ws_loop, self._clob_ws_loop, self._rest_fallback_loop)
        self._tasks = [asyncio.create_task(fn()) for fn in loops]

    async def stop(self) -> None:
        """Cancel feed tasks and close the HTTP session."""
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        self._tasks = []
        if self._session is not None:
            await self._session.close()
            self._session = None

    def _http(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=3.0), headers={"Accept": "application/json"}, trust_env=False
            )
        return self._session

    # ----- window metadata -----
    def roll_window(self, now: float) -> int:
        """Advance every symbol to the current 5-minute bucket, adopting prefetched tokens."""
        window_ts = (int(now) // 300) * 300
        for sym, st in self.states.items():
            if st.window_ts == window_ts:
                continue
            st.reset_for_window(window_ts)
            pre = self.prefetched.pop((sym, window_ts), None)
            if pre:
                st.apply_market(pre)
        for key in [k for k in self.prefetched if k[1] < window_ts]:
            self.prefetched.pop(key, None)
        return window_ts

    async def refresh_windows(self, now: float) -> None:
        """Fetch missing tokens / official strike and prefetch the next bucket near rollover."""
        window_ts = self.roll_window(now)
        elapsed = now - window_ts
        jobs = []
        for sym, st in self.states.items():
            need_tokens = not st.up_token or not st.down_token
            need_strike = st.price_to_beat is None and elapsed >= 8.0
            # Gamma only publishes `priceToBeat` after a window resolves, so poll for it sparingly; the RTDS bucket
            # open (the strike used by the validated replay) is the normal source inside the sniping window.
            if (need_tokens or need_strike) and (now - st.meta_checked_ts) >= (3.0 if need_tokens else 30.0):
                st.meta_checked_ts = now
                jobs.append(self._fetch_current(sym, window_ts))
            if elapsed >= 270.0 and (sym, window_ts + 300) not in self.prefetched and (now - st.prefetch_ts) >= 5.0:
                st.prefetch_ts = now
                jobs.append(self._prefetch_next(sym, window_ts + 300))
        for job in jobs:  # never block the 1 Hz trading loop on Gamma latency
            task = asyncio.create_task(self._guarded(job))
            self._meta_tasks.add(task)
            task.add_done_callback(self._meta_tasks.discard)

    async def _guarded(self, job: Any) -> None:
        try:
            await job
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - metadata is retried on the next refresh
            self.stats["meta_error"] = f"{type(exc).__name__}: {exc}"

    async def _fetch_current(self, sym: str, window_ts: int) -> None:
        market = await asyncio.wait_for(
            self.auto_trader.discover_live_5m_market(sym, bucket_offset=0, target_bucket_ts=window_ts), timeout=6.0
        )
        st = self.states[sym]
        if market and st.window_ts == window_ts:
            st.apply_market(market)

    async def _prefetch_next(self, sym: str, next_ts: int) -> None:
        market = await asyncio.wait_for(
            self.auto_trader.discover_live_5m_market(sym, bucket_offset=1, target_bucket_ts=next_ts), timeout=6.0
        )
        if market and market.get("up_token_id") and market.get("down_token_id"):
            self.prefetched[(sym, next_ts)] = market

    # ----- CEX feeds -----
    def _on_binance_message(self, msg: dict[str, Any]) -> None:
        stream = str(msg.get("stream", ""))
        data = msg.get("data") or {}
        sym = stream.split("@")[0].upper()
        st = self.states.get(sym)
        if st is None:
            return
        now = time.time()
        if "aggTrade" in stream:
            price, qty = float(data["p"]), float(data["q"])
            st.binance_price, st.binance_ts = price, now
            st.update_basis(now)
            st.tracker.on_trade(now, price, qty, bool(data["m"]))
            st.tracker.on_price(now, st.cex_lead_price(now) or price)
        elif "bookTicker" in stream:
            bid, ask = float(data["b"]), float(data["a"])
            st.binance_price, st.binance_ts = 0.5 * (bid + ask), now
            st.update_basis(now)
            st.tracker.on_book_ticker(float(data["B"]), float(data["A"]))

    async def _binance_ws_loop(self) -> None:
        while not self._stop.is_set():
            try:
                async with websockets.connect(BINANCE_WS_URL, ping_interval=15, ping_timeout=10, open_timeout=8) as ws:
                    self.stats["binance_ws"] = True
                    while not self._stop.is_set():
                        self._on_binance_message(json.loads(await asyncio.wait_for(ws.recv(), timeout=8.0)))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - reconnect on any transport error
                self.stats["binance_ws"] = False
                self.stats["binance_error"] = f"{type(exc).__name__}: {exc}"
                await asyncio.sleep(2.5)

    def _on_coinbase_message(self, data: dict[str, Any]) -> None:
        if data.get("type") != "ticker" or "price" not in data:
            return
        product = str(data.get("product_id", ""))
        for sym, prod in COINBASE_PRODUCT.items():
            if prod == product:
                st = self.states[sym]
                st.coinbase_price, st.coinbase_ts = float(data["price"]), time.time()
                st.update_basis(st.coinbase_ts)

    async def _coinbase_ws_loop(self) -> None:
        sub = {"type": "subscribe", "product_ids": list(COINBASE_PRODUCT.values()), "channels": ["ticker"]}
        while not self._stop.is_set():
            try:
                async with websockets.connect(COINBASE_WS_URL, ping_interval=15, ping_timeout=10, open_timeout=8) as ws:
                    await ws.send(json.dumps(sub))
                    self.stats["coinbase_ws"] = True
                    while not self._stop.is_set():
                        self._on_coinbase_message(json.loads(await asyncio.wait_for(ws.recv(), timeout=8.0)))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - reconnect on any transport error
                self.stats["coinbase_ws"] = False
                self.stats["coinbase_error"] = f"{type(exc).__name__}: {exc}"
                await asyncio.sleep(2.5)

    # ----- CLOB book feeds -----
    def _token_map(self) -> dict[str, tuple[str, str]]:
        out: dict[str, tuple[str, str]] = {}
        for sym, st in self.states.items():
            if st.up_token and st.down_token:
                out[st.up_token] = (sym, "UP")
                out[st.down_token] = (sym, "DOWN")
        return out

    def _set_side_book(self, st: SymbolState, side: str, bid: Optional[float], ask: Optional[float], ev_ts: float, via_ws: bool) -> None:
        local = time.time()
        if side == "UP":
            st.book.up_bid, st.book.up_ask = bid, ask
            st.up_book_ts, st.up_local_ts = max(st.up_book_ts, ev_ts), local
        else:
            st.book.down_bid, st.book.down_ask = bid, ask
            st.down_book_ts, st.down_local_ts = max(st.down_book_ts, ev_ts), local
        if via_ws:
            st.last_ws_book_local_ts = local

    def apply_book(self, sym: str, side: str, payload: dict[str, Any], via_ws: bool) -> None:
        """Store executable bid/ask for one token, ignoring older snapshots and other windows' tokens."""
        st = self.states[sym]
        asset = str(payload.get("asset_id") or "")
        expected = st.up_token if side == "UP" else st.down_token
        if asset and expected and asset != expected:
            return  # event belongs to a previous / different window's token
        ev_ts = parse_book_ts_sec(payload)
        held_ts = st.up_book_ts if side == "UP" else st.down_book_ts
        if ev_ts > 0.0 and ev_ts < held_ts - 0.05:
            return
        bids_map = _levels_map(payload.get("bids") or [])
        asks_map = _levels_map(payload.get("asks") or [])
        if side == "UP":
            st.up_bids_raw, st.up_asks_raw = bids_map, asks_map
        else:
            st.down_bids_raw, st.down_asks_raw = bids_map, asks_map
        bids = sorted(bids_map.items(), key=lambda t: t[0], reverse=True)
        asks = sorted(asks_map.items(), key=lambda t: t[0])
        bid, ask = _executable_from_sorted(bids, asks)
        self._set_side_book(st, side, bid, ask, ev_ts, via_ws=via_ws)

    def apply_price_change(self, payload: dict[str, Any], tokens: dict[str, tuple[str, str]], via_ws: bool = True) -> None:
        """Apply Polymarket CLOB WS `price_change` deltas incrementally so top-of-book never freezes stale."""
        ev_ts = parse_book_ts_sec(payload)
        if ev_ts > 0.0 and (time.time() - ev_ts) > 2.5:
            return
        touched: set[tuple[str, str]] = set()
        for ch in payload.get("price_changes") or ():
            if not isinstance(ch, dict):
                continue
            asset = str(ch.get("asset_id") or "")
            mapped = tokens.get(asset)
            if not mapped:
                continue
            sym, side = mapped
            st = self.states[sym]
            expected = st.up_token if side == "UP" else st.down_token
            if expected and asset != expected:
                continue
            held_ts = st.up_book_ts if side == "UP" else st.down_book_ts
            if ev_ts > 0.0 and ev_ts < held_ts - 0.05:
                continue
            px, sz = float(ch.get("price") or 0), float(ch.get("size") or 0)
            if not 0.0 < px < 1.0:
                continue
            b_side = str(ch.get("side") or "").upper()
            bids_map = st.up_bids_raw if side == "UP" else st.down_bids_raw
            asks_map = st.up_asks_raw if side == "UP" else st.down_asks_raw
            target = bids_map if b_side == "BUY" else (asks_map if b_side == "SELL" else None)
            if target is None:
                continue
            if sz <= 0.0:
                target.pop(px, None)
            else:
                target[px] = sz
            touched.add((sym, side))
        for sym, side in touched:
            st = self.states[sym]
            bids_map = st.up_bids_raw if side == "UP" else st.down_bids_raw
            asks_map = st.up_asks_raw if side == "UP" else st.down_asks_raw
            if not bids_map and not asks_map:
                continue
            bids = sorted(bids_map.items(), key=lambda t: t[0], reverse=True)
            asks = sorted(asks_map.items(), key=lambda t: t[0])
            bid, ask = _executable_from_sorted(bids, asks)
            self._set_side_book(st, side, bid, ask, ev_ts, via_ws=via_ws)

    async def _clob_ws_loop(self) -> None:
        while not self._stop.is_set():
            tokens = self._token_map()
            if not tokens:
                await asyncio.sleep(0.5)
                continue
            subscribed_window = (int(time.time()) // 300) * 300
            try:
                async with websockets.connect(POLY_CLOB_WS_URL, ping_interval=15, ping_timeout=10, open_timeout=8) as ws:
                    await ws.send(json.dumps({"assets_ids": list(tokens), "type": "market"}))
                    self.stats["clob_ws"] = True
                    await self._clob_ws_pump(ws, tokens, subscribed_window)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - reconnect on any transport error
                self.stats["clob_ws"] = False
                self.stats["clob_reconnects"] = int(self.stats.get("clob_reconnects", 0)) + 1
                self.stats["clob_error"] = f"{type(exc).__name__}: {exc}"
                await asyncio.sleep(1.5)

    async def _clob_ws_pump(self, ws: Any, tokens: dict[str, tuple[str, str]], subscribed_window: int) -> None:
        last_msg = time.time()
        while not self._stop.is_set() and (int(time.time()) // 300) * 300 == subscribed_window:
            if time.time() - last_msg >= 12.0 or len(self._token_map()) != len(tokens):
                return
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            last_msg = time.time()
            if not raw or raw in ("PING", "PONG"):
                continue
            try:
                payload = json.loads(raw)
            except ValueError:
                continue
            for ev in payload if isinstance(payload, list) else [payload]:
                if not isinstance(ev, dict):
                    continue
                ev_type = ev.get("event_type")
                if ev_type == "book":
                    ev_ts = parse_book_ts_sec(ev)
                    mapped = tokens.get(str(ev.get("asset_id")))
                    if mapped and not (ev_ts > 0.0 and (time.time() - ev_ts) > 2.5):
                        self.apply_book(mapped[0], mapped[1], ev, via_ws=True)
                elif ev_type == "price_change":
                    self.apply_price_change(ev, tokens, via_ws=True)

    async def _fetch_book(self, sym: str, side: str, token: str) -> None:
        try:
            async with self._http().get(CLOB_BOOK_URL, params={"token_id": token}) as resp:
                if resp.status == 200:
                    payload = dict(await resp.json())
                    payload["asset_id"] = token  # guard against applying a book after the window rolled
                    self.apply_book(sym, side, payload, via_ws=False)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
            return

    async def _rest_fallback_loop(self) -> None:
        """Poll REST books for symbols whose WebSocket is silent (>=2.5s) or empty; refresh CEX if WS is down."""
        while not self._stop.is_set():
            try:
                now = time.time()
                jobs = []
                for sym, st in self.states.items():
                    silent = (now - st.last_ws_book_local_ts) >= 2.5
                    empty = st.book.up_ask is None and st.book.down_ask is None
                    if st.up_token and st.down_token and (silent or empty):
                        jobs.append(self._fetch_book(sym, "UP", st.up_token))
                        jobs.append(self._fetch_book(sym, "DOWN", st.down_token))
                if jobs:
                    await asyncio.gather(*jobs)
                await self._cex_rest_fallback(now)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - never let the fallback loop die
                self.stats["rest_error"] = f"{type(exc).__name__}: {exc}"
            await asyncio.sleep(1.2)

    async def _cex_rest_fallback(self, now: float) -> None:
        if all((now - st.binance_ts) < 2.5 for st in self.states.values()):
            return
        try:
            params = {"symbols": json.dumps(list(SYMBOLS), separators=(",", ":"))}
            async with self._http().get(BINANCE_REST_BOOK_URL, params=params) as resp:
                rows = await resp.json() if resp.status == 200 else []
            for row in rows:
                st = self.states.get(str(row.get("symbol")))
                if st is not None:
                    st.binance_price = 0.5 * (float(row["bidPrice"]) + float(row["askPrice"]))
                    st.binance_ts = time.time()
                    st.update_basis(st.binance_ts)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, KeyError):
            return

    def health(self) -> dict[str, Any]:
        """Compact feed-health snapshot for status APIs."""
        now = time.time()
        per_sym = {
            s: {
                "cex_age_sec": round(now - max(st.binance_ts, st.coinbase_ts), 1) if (st.binance_ts or st.coinbase_ts) else None,
                "has_tokens": bool(st.up_token and st.down_token),
                "has_strike": st.price_to_beat is not None,
                "book": {"up": [st.book.up_bid, st.book.up_ask], "down": [st.book.down_bid, st.book.down_ask]},
            }
            for s, st in self.states.items()
        }
        return {**self.stats, "symbols": per_sym}
