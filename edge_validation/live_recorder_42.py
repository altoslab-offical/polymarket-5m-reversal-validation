#!/usr/bin/env python3
"""
Section 4.2 Three-Coin Read-Only Live Recorder & Conservative (+1s / +2s) Fill Simulator.

Zero real orders, zero private-key signing. Records strictly public feeds for BTC, ETH, SOL:
  1. Polymarket CLOB Market WebSocket (`top-5` bids/asks + `last_trade_price`) + REST fallback
  2. Polymarket Chainlink RTDS (`crypto_prices_chainlink` spot + `crypto_prices_twap_sixty` 60s TWAP)
  3. Binance `aggTrade` (1s OHLCV + buy/sell taker flow)
  4. Official Gamma window settlement (`outcomePrices`, `eventMetadata.priceToBeat`, `finalPrice`)
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
from datetime import datetime, timezone
import json
import logging
import pathlib
import sqlite3
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx
import numpy as np
import pandas as pd
import websockets

ROOT = pathlib.Path(__file__).resolve().parent
REPO = ROOT.parent
sys.path.insert(0, str(REPO))

from engine.chainlink_oracle import ChainlinkRTDSFeed
from engine.oracle_sniper import (
    DEFAULT_SNIPER_CONFIG,
    TWAP_V2_FILTERED_CONFIG,
    TWAP_V2_HIGHBAND_SHADOW_CONFIG,
    TWAP_V2_PURE_CONFIG,
    FairValueEstimate,
    OpenPosition,
    OrderBookTop,
    RollingMicrostructureTracker,
    SniperConfig,
    compute_shares,
    effective_exit_proceeds,
    entry_limit_price,
    estimate_fair_value,
    estimate_twap_fair_value,
    evaluate_entry,
    evaluate_exit,
    pick_best_candidate,
    taker_fee_per_share,
)

LOG = logging.getLogger("recorder42")
SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
COIN_MAP = {"BTCUSDT": "btc", "ETHUSDT": "eth", "SOLUSDT": "sol"}
BINANCE_WS = "wss://stream.binance.com:9443/stream?streams=" + "/".join(f"{s.lower()}@aggTrade" for s in SYMBOLS)
CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_URL = "https://gamma-api.polymarket.com/events"
CLOB_BOOK_URL = "https://clob.polymarket.com/book"

DEFAULT_DB = REPO / "data/validation_42/live_3coin_42.db"
FROZEN_CFG_PATH = ROOT / "frozen_config.json"


def init_db(db_path: pathlib.Path) -> sqlite3.Connection:
    """Create SQLite schema for 4.2 windows, 1Hz snapshots, and Polymarket trades."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path, timeout=30.0)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS windows_42 (
            window_ts INTEGER NOT NULL, symbol TEXT NOT NULL, slug TEXT NOT NULL,
            condition_id TEXT, up_token_id TEXT, down_token_id TEXT,
            twap_strike REAL, spot_strike REAL, twap_close REAL, spot_close REAL,
            gamma_price_to_beat REAL, gamma_final_price REAL, resolved_outcome TEXT, fee_rate REAL,
            PRIMARY KEY (window_ts, symbol)
        );
        CREATE TABLE IF NOT EXISTS snapshots_1hz (
            ts_sec INTEGER NOT NULL, window_ts INTEGER NOT NULL, symbol TEXT NOT NULL, elapsed_sec INTEGER NOT NULL,
            cl_spot REAL, cl_twap60 REAL, bn_close REAL, bn_vol REAL, bn_buy_vol REAL, bn_sell_vol REAL, bn_trades INTEGER,
            up_bids_json TEXT, up_asks_json TEXT, down_bids_json TEXT, down_asks_json TEXT,
            up_bid1 REAL, up_bid1_sz REAL, up_ask1 REAL, up_ask1_sz REAL,
            down_bid1 REAL, down_bid1_sz REAL, down_ask1 REAL, down_ask1_sz REAL,
            PRIMARY KEY (ts_sec, symbol)
        );
        CREATE TABLE IF NOT EXISTS pm_trades_42 (
            id INTEGER PRIMARY KEY AUTOINCREMENT, recv_ts_ms INTEGER NOT NULL, window_ts INTEGER NOT NULL,
            symbol TEXT NOT NULL, side TEXT NOT NULL, token_id TEXT NOT NULL, price REAL NOT NULL, size REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_snap_win ON snapshots_1hz(window_ts, symbol, elapsed_sec);
        CREATE INDEX IF NOT EXISTS idx_trades_win ON pm_trades_42(window_ts, symbol);
        """
    )
    con.commit()
    return con


def _resolve_git_sha(repo_dir: pathlib.Path) -> Tuple[str, bool]:
    for cand in (repo_dir, repo_dir / "polymarket-5m-reversal-validation"):
        if not (cand / ".git").exists():
            continue
        try:
            sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=cand, text=True).strip()
            dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=cand, text=True).strip())
            return sha, dirty
        except subprocess.CalledProcessError as exc:
            LOG.warning("git rev-parse failed in %s: %s", cand, exc)
    return "UNCOMMITTED_LOCAL", True


NOV21_UTC_TS = 1795219200  # 2026-11-21T00:00:00Z primary evaluation boundary


def write_frozen_config(
    path: pathlib.Path = FROZEN_CFG_PATH, fee_rate_override: float | None = None, git_sha_override: str | None = None
) -> dict[str, Any]:
    """Write frozen Section 4.2 parameters, SHA, fee schedule, and pre-registered gates."""
    sha, dirty = _resolve_git_sha(REPO)
    if git_sha_override:
        sha, dirty = git_sha_override, False
    cfg_dict = asdict(DEFAULT_SNIPER_CONFIG)
    if fee_rate_override is not None:
        cfg_dict["fee_rate"] = float(fee_rate_override)
    payload = {
        "schema_version": "4.2-v2-bonferroni",
        "frozen_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "git_sha": sha,
        "engine_dirty": dirty,
        "primary_eval_start_utc": "2026-11-21T00:00:00Z",
        "primary_eval_start_ts": NOV21_UTC_TS,
        "pre_registered_models": ["v2_pure", "v2_filtered", "v1"],
        "shadow_models": ["v2_highband_shadow"],
        "bonferroni_alpha": round(0.05 / 3, 6),
        "bonferroni_ci_pct": 98.33,
        "effective_fees_post_nov20": {"BTCUSDT": 0.05, "ETHUSDT": 0.01, "SOLUSDT": 0.01},
        "sniper_config": cfg_dict,
        "sniper_config_v2_filtered": asdict(TWAP_V2_FILTERED_CONFIG),
        "sniper_config_v2_pure": asdict(TWAP_V2_PURE_CONFIG),
        "shadow_config_v2_highband": {**asdict(TWAP_V2_HIGHBAND_SHADOW_CONFIG), "_meta": {"bonferroni_included": False, "role": "data_collection_only_not_for_launch"}},
        "execution_rules": {
            "fill_delay_sec_primary": 1, "fill_delay_sec_stress": 2,
            "require_top_ask_px_le_limit": True, "require_top_ask_sz_ge_shares": True,
            "early_exit_delay_sec": 1, "require_top_bid_sz_ge_shares": True,
            "bankroll_gates": {
                "pilot_fixed_shares": 5, "pilot_trades": 100, "pilot_slippage_max_cents": 1.0,
                "max_risk_pct_ge_200u": 0.03, "low_bankroll_daily_loss_stop_usd": 10.0,
                "low_bankroll_consec_loss_stop": 3, "rolling_50_wr_buffer_pp": 2.0,
            },
        },
        "pre_registered_pass_gates": {
            "min_days": 7, "max_days": 14, "min_trades_total": 300,
            "pnl_per_share_ci95_lo_gt": 0.0, "bonferroni_ci98_3_lo_gt": 0.0,
            "multi_config_count": 3, "min_positive_coins": 2,
            "min_median_trades_per_day": 10, "min_positive_weeks_pct": 0.70,
            "max_drawdown_pct": 0.40, "min_r1_settlement_parity": 0.99,
        },
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


class CoinState42:
    """Mutable live feed state for one tracked coin inside a 5-minute window."""

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.window_ts = 0
        self.slug = ""
        self.condition_id = ""
        self.up_token = ""
        self.down_token = ""
        self.bn_last_px: Optional[float] = None
        self.bn_sec_bucket = 0
        self.bn_vol = self.bn_buy = self.bn_sell = 0.0
        self.bn_n = 0
        self.up_bids: List[Tuple[float, float]] = []
        self.up_asks: List[Tuple[float, float]] = []
        self.dn_bids: List[Tuple[float, float]] = []
        self.dn_asks: List[Tuple[float, float]] = []

    def reset_window(self, window_ts: int) -> None:
        """Reset market tokens and top-5 orderbooks on window rollover."""
        self.window_ts = window_ts
        self.slug = f"{COIN_MAP[self.symbol]}-updown-5m-{window_ts}"
        self.condition_id = self.up_token = self.down_token = ""
        self.up_bids.clear()
        self.up_asks.clear()
        self.dn_bids.clear()
        self.dn_asks.clear()


class LiveRecorder42:
    """Async multi-feed read-only recorder for BTC/ETH/SOL top-5 depth, Chainlink TWAP, and Binance 1s."""

    def __init__(self, db_path: pathlib.Path = DEFAULT_DB) -> None:
        self.db_path = db_path
        self.con = init_db(db_path)
        self.chainlink = ChainlinkRTDSFeed()
        self.states: Dict[str, CoinState42] = {s: CoinState42(s) for s in SYMBOLS}
        self.token_to_meta: Dict[str, Tuple[str, str]] = {}
        self.prefetched: Dict[Tuple[str, int], dict[str, Any]] = {}
        self._stop = asyncio.Event()
        self._clob_resub = asyncio.Event()
        self._trade_buf: List[Tuple[int, int, str, str, str, float, float]] = []

    async def _discover_market(self, client: httpx.AsyncClient, sym: str, window_ts: int) -> Optional[dict[str, Any]]:
        slug = f"{COIN_MAP[sym]}-updown-5m-{window_ts}"
        try:
            r = await client.get(GAMMA_URL, params={"slug": slug}, timeout=6.0)
            if r.status_code != 200 or not r.json():
                return None
            ev = r.json()[0]
            mkt = (ev.get("markets") or [{}])[0]
            toks = mkt.get("clobTokenIds")
            toks = json.loads(toks) if isinstance(toks, str) else toks
            if not toks or len(toks) < 2:
                return None
            em = ev.get("eventMetadata") or {}
            op = mkt.get("outcomePrices")
            op = json.loads(op) if isinstance(op, str) else op
            resolved = "UP" if op == ["1", "0"] else ("DOWN" if op == ["0", "1"] else None)
            return {
                "slug": slug, "condition_id": str(mkt.get("conditionId") or ""),
                "up_token": str(toks[0]), "down_token": str(toks[1]),
                "gamma_ptb": float(em["priceToBeat"]) if em.get("priceToBeat") is not None else None,
                "gamma_final": float(em["finalPrice"]) if em.get("finalPrice") is not None else None,
                "resolved": resolved, "closed": bool(ev.get("closed", False)),
            }
        except (httpx.HTTPError, ValueError, KeyError) as exc:
            LOG.debug("Gamma discover failed for %s: %s", slug, exc)
            return None

    def _on_binance_msg(self, raw: str) -> None:
        msg = json.loads(raw)
        data = msg.get("data") or {}
        sym = str(msg.get("stream", "")).split("@")[0].upper()
        st = self.states.get(sym)
        if st is None or "p" not in data or "q" not in data:
            return
        px, qty = float(data["p"]), float(data["q"])
        ts_s = int(data.get("T") or time.time() * 1000) // 1000
        if ts_s != st.bn_sec_bucket:
            st.bn_sec_bucket, st.bn_vol, st.bn_buy, st.bn_sell, st.bn_n = ts_s, 0.0, 0.0, 0.0, 0
        st.bn_last_px = px
        st.bn_vol += qty
        if data.get("m", False):
            st.bn_sell += qty
        else:
            st.bn_buy += qty
        st.bn_n += 1

    async def _binance_loop(self) -> None:
        while not self._stop.is_set():
            try:
                async with websockets.connect(BINANCE_WS, open_timeout=8.0, ping_interval=20.0) as ws:
                    while not self._stop.is_set():
                        self._on_binance_msg(await asyncio.wait_for(ws.recv(), timeout=15.0))
            except asyncio.CancelledError:
                raise
            except (websockets.WebSocketException, asyncio.TimeoutError, OSError, ValueError) as exc:
                LOG.warning("Binance WS reconnect: %s", exc)
                await asyncio.sleep(2.0)

    def _apply_book(self, token_id: str, bids_raw: Any, asks_raw: Any) -> None:
        meta = self.token_to_meta.get(token_id)
        if not meta:
            return
        sym, side = meta
        st = self.states[sym]
        if bids_raw is not None:
            lv = sorted([(float(x["price"]), float(x["size"])) for x in bids_raw if float(x.get("size", 0)) > 0], key=lambda t: t[0], reverse=True)[:5]
            if side == "UP":
                st.up_bids = lv
            else:
                st.dn_bids = lv
        if asks_raw is not None:
            lv = sorted([(float(x["price"]), float(x["size"])) for x in asks_raw if float(x.get("size", 0)) > 0], key=lambda t: t[0])[:5]
            if side == "UP":
                st.up_asks = lv
            else:
                st.dn_asks = lv

    def _apply_price_change(self, msg: dict[str, Any]) -> None:
        for ch in msg.get("price_changes") or []:
            tok = str(ch.get("asset_id") or "")
            meta = self.token_to_meta.get(tok)
            px, sz = float(ch.get("price") or 0), float(ch.get("size") or 0)
            if not meta or px <= 0:
                continue
            sym, side = meta
            st, b_side = self.states[sym], str(ch.get("side") or "").upper()
            if b_side == "BUY":
                lv = dict(st.up_bids if side == "UP" else st.dn_bids)
                if sz <= 0:
                    lv.pop(px, None)
                else:
                    lv[px] = sz
                res = sorted(lv.items(), key=lambda t: t[0], reverse=True)[:5]
                setattr(st, "up_bids" if side == "UP" else "dn_bids", res)
            elif b_side == "SELL":
                lv = dict(st.up_asks if side == "UP" else st.dn_asks)
                if sz <= 0:
                    lv.pop(px, None)
                else:
                    lv[px] = sz
                res = sorted(lv.items(), key=lambda t: t[0])[:5]
                setattr(st, "up_asks" if side == "UP" else "dn_asks", res)

    def _handle_clob_msg(self, raw: str) -> None:
        if not raw or raw in ("PING", "PONG"):
            return
        obj = json.loads(raw)
        for m in (obj if isinstance(obj, list) else [obj]):
            if not isinstance(m, dict):
                continue
            ev_type = str(m.get("event_type") or "")
            if ev_type == "book" and m.get("asset_id"):
                self._apply_book(str(m["asset_id"]), m.get("bids"), m.get("asks"))
            elif ev_type == "price_change":
                self._apply_price_change(m)
            elif ev_type == "last_trade_price" and m.get("asset_id"):
                meta = self.token_to_meta.get(str(m["asset_id"]))
                if meta:
                    ts_ms = int(m.get("timestamp") or time.time() * 1000)
                    self._trade_buf.append((ts_ms, self.states[meta[0]].window_ts, meta[0], meta[1], str(m["asset_id"]), float(m.get("price") or 0), float(m.get("size") or 0)))

    async def _clob_ws_loop(self) -> None:
        while not self._stop.is_set():
            assets = [tok for st in self.states.values() for tok in (st.up_token, st.down_token) if tok]
            if not assets:
                await asyncio.sleep(0.5)
                continue
            self._clob_resub.clear()
            try:
                async with websockets.connect(CLOB_WS, open_timeout=8.0, ping_interval=20.0) as ws:
                    await ws.send(json.dumps({"assets_ids": assets, "type": "market"}))
                    while not self._stop.is_set() and not self._clob_resub.is_set():
                        self._handle_clob_msg(await asyncio.wait_for(ws.recv(), timeout=15.0))
            except asyncio.CancelledError:
                raise
            except (websockets.WebSocketException, asyncio.TimeoutError, OSError, ValueError) as exc:
                LOG.warning("CLOB WS reconnect: %s", exc)
                await asyncio.sleep(1.5)

    async def _refresh_symbol_meta(self, client: httpx.AsyncClient, sym: str, cur_t: int, elapsed: float) -> bool:
        st, changed = self.states[sym], False
        if st.window_ts != cur_t:
            st.reset_window(cur_t)
            pre = self.prefetched.pop((sym, cur_t), None)
            if pre:
                st.condition_id, st.up_token, st.down_token = pre["condition_id"], pre["up_token"], pre["down_token"]
                self.token_to_meta[st.up_token], self.token_to_meta[st.down_token] = (sym, "UP"), (sym, "DOWN")
                changed = True
        if not st.up_token or not st.down_token:
            info = await self._discover_market(client, sym, cur_t)
            if info:
                st.condition_id, st.up_token, st.down_token = info["condition_id"], info["up_token"], info["down_token"]
                self.token_to_meta[st.up_token], self.token_to_meta[st.down_token] = (sym, "UP"), (sym, "DOWN")
                changed = True
        if elapsed >= 270.0 and (sym, cur_t + 300) not in self.prefetched:
            nxt = await self._discover_market(client, sym, cur_t + 300)
            if nxt:
                self.prefetched[(sym, cur_t + 300)] = nxt
        await self._fill_empty_rest_books(client, st)
        return changed

    async def _fill_empty_rest_books(self, client: httpx.AsyncClient, st: CoinState42) -> None:
        for tok, empty in ((st.up_token, not st.up_asks), (st.down_token, not st.dn_asks)):
            if not tok or not empty:
                continue
            try:
                rb = await client.get(CLOB_BOOK_URL, params={"token_id": tok}, timeout=3.0)
                if rb.status_code == 200:
                    bk = rb.json()
                    self._apply_book(tok, bk.get("bids"), bk.get("asks"))
            except (httpx.HTTPError, ValueError) as exc:
                LOG.debug("REST /book fallback failed %s: %s", tok[:10], exc)

    async def _settle_pending_windows(self, client: httpx.AsyncClient, cur_t: int) -> None:
        pending = self.con.execute(
            "SELECT window_ts, symbol FROM windows_42 WHERE window_ts < ? AND (resolved_outcome IS NULL OR gamma_final_price IS NULL) LIMIT 30",
            (cur_t - 120,),
        ).fetchall()
        for w_ts, sym in pending:
            info = await self._discover_market(client, sym, int(w_ts))
            if not info:
                continue
            tw_s = self.chainlink.get_settlement_twap_at(sym, int(w_ts))
            tw_c = self.chainlink.get_settlement_twap_at(sym, int(w_ts) + 300)
            self.con.execute(
                """
                UPDATE windows_42
                SET gamma_price_to_beat = COALESCE(?, gamma_price_to_beat),
                    gamma_final_price = COALESCE(?, gamma_final_price),
                    resolved_outcome = COALESCE(?, resolved_outcome),
                    twap_strike = COALESCE(twap_strike, ?),
                    twap_close = COALESCE(twap_close, ?)
                WHERE window_ts=? AND symbol=?
                """,
                (info["gamma_ptb"], info["gamma_final"], info["resolved"], tw_s, tw_c, w_ts, sym),
            )
        self.con.commit()

    async def _meta_and_settlement_loop(self, client: httpx.AsyncClient) -> None:
        last_settle_scan = 0.0
        while not self._stop.is_set():
            now = time.time()
            cur_t = (int(now) // 300) * 300
            changed = any([await self._refresh_symbol_meta(client, sym, cur_t, now - cur_t) for sym in SYMBOLS])
            if changed:
                keep = {tok for st in self.states.values() for tok in (st.up_token, st.down_token) if tok}
                self.token_to_meta = {k: v for k, v in self.token_to_meta.items() if k in keep}
                self._clob_resub.set()
            if now - last_settle_scan >= 20.0:
                last_settle_scan = now
                await self._settle_pending_windows(client, cur_t)
            await asyncio.sleep(2.0)

    def _record_second(self, ts_sec: int) -> None:
        cur_t, elapsed = (ts_sec // 300) * 300, ts_sec % 300
        for sym, st in self.states.items():
            cl_spot, _ = self.chainlink.get_latest_price(sym, max_age_sec=30.0)
            cl_twap, _ = self.chainlink.get_latest_twap(sym, max_age_sec=30.0)
            if cl_twap is None:
                cl_twap, _ = self.chainlink.get_60s_twap(sym, end_ts_ms=ts_sec * 1000, min_ticks=12)
            tw_s = self.chainlink.get_settlement_twap_at(sym, cur_t)
            sp_s = self.chainlink.bucket_opens[sym].get(cur_t, (None, 0))[0]
            slug_val = st.slug or f"{COIN_MAP[sym]}-updown-5m-{cur_t}"
            self.con.execute(
                """
                INSERT INTO windows_42 (window_ts, symbol, slug, condition_id, up_token_id, down_token_id, twap_strike, spot_strike)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(window_ts, symbol) DO UPDATE SET
                    slug = COALESCE(NULLIF(excluded.slug, ''), windows_42.slug),
                    condition_id = COALESCE(NULLIF(excluded.condition_id, ''), windows_42.condition_id),
                    up_token_id = COALESCE(NULLIF(excluded.up_token_id, ''), windows_42.up_token_id),
                    down_token_id = COALESCE(NULLIF(excluded.down_token_id, ''), windows_42.down_token_id),
                    twap_strike = COALESCE(windows_42.twap_strike, excluded.twap_strike),
                    spot_strike = COALESCE(windows_42.spot_strike, excluded.spot_strike)
                """,
                (cur_t, sym, slug_val, st.condition_id, st.up_token, st.down_token, tw_s, sp_s),
            )
            if elapsed >= 298 or elapsed <= 2:
                prev_t = cur_t if elapsed >= 298 else (cur_t - 300)
                tw_c = self.chainlink.get_settlement_twap_at(sym, prev_t + 300)
                if tw_c is not None:
                    self.con.execute("UPDATE windows_42 SET twap_close = COALESCE(twap_close, ?) WHERE window_ts=? AND symbol=?", (tw_c, prev_t, sym))
            u_b1 = st.up_bids[0] if st.up_bids else (None, None)
            u_a1 = st.up_asks[0] if st.up_asks else (None, None)
            d_b1 = st.dn_bids[0] if st.dn_bids else (None, None)
            d_a1 = st.dn_asks[0] if st.dn_asks else (None, None)
            same_sec = st.bn_sec_bucket == ts_sec
            self.con.execute(
                "INSERT OR REPLACE INTO snapshots_1hz VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    ts_sec, cur_t, sym, elapsed, cl_spot, cl_twap, st.bn_last_px,
                    st.bn_vol if same_sec else 0.0, st.bn_buy if same_sec else 0.0,
                    st.bn_sell if same_sec else 0.0, st.bn_n if same_sec else 0,
                    json.dumps(st.up_bids), json.dumps(st.up_asks), json.dumps(st.dn_bids), json.dumps(st.dn_asks),
                    u_b1[0], u_b1[1], u_a1[0], u_a1[1], d_b1[0], d_b1[1], d_a1[0], d_a1[1],
                ),
            )
        if self._trade_buf:
            self.con.executemany("INSERT INTO pm_trades_42(recv_ts_ms, window_ts, symbol, side, token_id, price, size) VALUES (?,?,?,?,?,?,?)", self._trade_buf)
            self._trade_buf.clear()
        if ts_sec % 5 == 0:
            self.con.commit()

    async def run_record(self, duration_sec: int = 0) -> None:
        """Start read-only WebSocket & settlement loops and record 1Hz snapshots."""
        self.chainlink.start()
        limits = httpx.Limits(max_connections=20, max_keepalive_connections=20)
        async with httpx.AsyncClient(limits=limits) as client:
            tasks = [asyncio.create_task(fn) for fn in (self._binance_loop(), self._clob_ws_loop(), self._meta_and_settlement_loop(client))]
            t_start = time.time()
            try:
                while not self._stop.is_set():
                    t0 = time.time()
                    if duration_sec > 0 and (t0 - t_start) >= duration_sec:
                        break
                    self._record_second(int(t0))
                    await asyncio.sleep(max(0.05, 1.0 - (time.time() - t0)))
            finally:
                self._stop.set()
                for t in tasks:
                    t.cancel()
                self.chainlink.stop()
                self.con.commit()


def _build_tick_views(
    tick: Tuple[int, int, int], sec_maps: Dict[str, Dict[int, Any]], w_meta: Dict[Tuple[int, str], Any],
    trackers: Dict[str, RollingMicrostructureTracker], cfg: SniperConfig, twap_acc: Optional[Dict[str, List[float]]] = None,
) -> Dict[str, Tuple[FairValueEstimate, OrderBookTop]]:
    T, t_sec, elapsed = tick
    views: Dict[str, Tuple[FairValueEstimate, OrderBookTop]] = {}
    for s in SYMBOLS:
        row = sec_maps[s].get(t_sec)
        wm = w_meta.get((int(T), s))
        strike = (wm.gamma_price_to_beat if (wm and pd.notna(wm.gamma_price_to_beat)) else (wm.twap_strike if wm else None))
        if row is None or row.cl_spot is None or row.bn_close is None or not strike:
            continue
        if twap_acc is not None and elapsed >= 240:
            twap_acc[s].append(float(row.cl_spot))
        tr = trackers[s]
        tr.on_price(float(t_sec), float(row.bn_close))
        if float(row.bn_buy_vol or 0.0) > 0:
            tr.on_trade(float(t_sec), float(row.bn_close), float(row.bn_buy_vol), False)
        if float(row.bn_sell_vol or 0.0) > 0:
            tr.on_trade(float(t_sec), float(row.bn_close), float(row.bn_sell_vol), True)
        vol = tr.get_realized_vol_1s(floor=cfg.min_1s_vol_floor)
        gap = tr.update_oracle_basis(float(row.cl_spot), float(row.bn_close))
        cvd, _, flow = tr.get_orderflow_metrics(float(t_sec))
        if cfg.use_twap_projection:
            acc = twap_acc[s] if twap_acc is not None else []
            a_mean = (sum(acc) / len(acc)) if acc else float(row.cl_spot)
            a_sec = float(elapsed - 240) if elapsed >= 240 else 0.0
            fv = estimate_twap_fair_value(float(strike), float(row.cl_spot), float(300 - elapsed), vol, (a_mean, a_sec), (flow, gap, cvd), cfg)
        else:
            fv = estimate_fair_value(float(strike), float(row.cl_spot), float(300 - elapsed), vol, flow, gap, cfg.student_t_df, cvd)
        if fv is not None:
            views[s] = (fv, OrderBookTop(row.up_bid1, row.up_ask1, row.down_bid1, row.down_ask1))
    return views


def _miss_reason(ask1: Optional[float], sz1: Optional[float], lim: float, shares: int) -> str:
    if shares <= 0:
        return "SHARES_ZERO"
    if ask1 is None or sz1 is None:
        return "NO_BOOK_T1"
    if ask1 > lim:
        return "ASK_ABOVE_LIMIT"
    return "SIZE_LT_SHARES"


def _try_enter_window(
    tick: Tuple[int, int, int], views: Dict[str, Tuple[FairValueEstimate, OrderBookTop]],
    market_ctx: Tuple[Dict[str, Dict[int, Any]], Dict[Tuple[int, str], Any]],
    cfg: SniperConfig, fee_map: Dict[str, float], eq: float,
) -> Tuple[Optional[dict[str, Any]], Optional[dict[str, Any]]]:
    T, t_sec, elapsed = tick
    sec_maps, w_meta = market_ctx
    cands = [
        cd for s, (fv, bk) in views.items()
        if (cd := evaluate_entry(s, SniperConfig(**{**asdict(cfg), "fee_rate": float(fee_map.get(s, cfg.fee_rate))}), float(elapsed), fv, bk)) is not None
    ]
    best = pick_best_candidate(cands)
    if best is None:
        return None, None
    sym, fee_r = best.symbol, float(fee_map.get(best.symbol, cfg.fee_rate))
    lim, shares = entry_limit_price(cfg, best.ask, best.p_true), compute_shares(cfg, eq, best.total_cost_per_share)
    r0, r1, r2 = sec_maps[sym].get(t_sec), sec_maps[sym].get(t_sec + 1), sec_maps[sym].get(t_sec + 2)
    bid0 = (r0.up_bid1 if best.side == "UP" else r0.down_bid1) if r0 else None
    sz0 = (r0.up_ask1_sz if best.side == "UP" else r0.down_ask1_sz) if r0 else None
    ask1 = (r1.up_ask1 if best.side == "UP" else r1.down_ask1) if r1 else None
    sz1 = (r1.up_ask1_sz if best.side == "UP" else r1.down_ask1_sz) if r1 else None
    ask2 = (r2.up_ask1 if best.side == "UP" else r2.down_ask1) if r2 else None
    sz2 = (r2.up_ask1_sz if best.side == "UP" else r2.down_ask1_sz) if r2 else None
    ok1 = bool(shares > 0 and ask1 is not None and sz1 is not None and ask1 <= lim and sz1 >= shares)
    ok2 = bool(shares > 0 and ask2 is not None and sz2 is not None and ask2 <= lim and sz2 >= shares)
    miss = "" if ok1 else _miss_reason(ask1, sz1, lim, shares)
    wm = w_meta.get((int(T), sym))
    out = wm.resolved_outcome if wm else None
    cost1 = (float(ask1) + taker_fee_per_share(float(ask1), fee_r)) if ok1 else np.nan
    cost2 = (float(ask2) + taker_fee_per_share(float(ask2), fee_r)) if ok2 else np.nan
    pnl2 = round((1.0 if out == best.side else 0.0) - cost2, 5) if (ok2 and out in ("UP", "DOWN")) else np.nan
    pos: dict[str, Any] = {
        "signal_ts": t_sec, "window_ts": int(T), "elapsed_sec": int(elapsed), "symbol": sym, "side": best.side,
        "regime": "post_nov21" if int(T) >= NOV21_UTC_TS else "pre_nov21",
        "fair_at_sig": round(float(best.p_true), 5), "ask_t0": round(float(best.ask), 5),
        "ask_sz_t0": sz0, "spread_t0": round(float(best.ask) - float(bid0), 5) if bid0 is not None else np.nan,
        "ask_t1": ask1, "ask_sz_t1": sz1, "ask_t2": ask2, "ask_sz_t2": sz2,
        "limit_px": lim, "shares": shares, "filled_1s": ok1, "filled_2s": ok2, "miss_reason": miss,
        "exit_reason": "HOLD_SETTLE" if ok1 else f"MISSED_{miss}", "hold_sec": None if ok1 else 0,
        "cl_twap_at_sig": float(r0.cl_twap60) if (r0 and pd.notna(r0.cl_twap60)) else np.nan,
        "bn_px_at_sig": float(r0.bn_close) if (r0 and pd.notna(r0.bn_close)) else np.nan,
        "resolved_outcome": out, "would_have_won": (out == best.side) if out in ("UP", "DOWN") else None,
        "entry_p_true": best.p_true, "entry_z": best.z, "fee_rate": fee_r,
        "cost_1s": cost1, "cost_2s": cost2, "exit_ts": None, "exit_bid_1s": None, "exit_sz_1s": None,
        "pnl_per_share": np.nan, "pnl_per_share_2s": pnl2, "pnl_usd": np.nan,
    }
    return (pos, None) if ok1 else (None, pos)


def _try_exit_pos(
    open_pos: dict[str, Any], tick: Tuple[int, int, int],
    views: Dict[str, Tuple[FairValueEstimate, OrderBookTop]], sec_maps: Dict[str, Dict[int, Any]], cfg: SniperConfig,
) -> Optional[dict[str, Any]]:
    T, t_sec, elapsed = tick
    sym = open_pos["symbol"]
    if sym not in views:
        return None
    fv, bk = views[sym]
    c_sym = SniperConfig(**{**asdict(cfg), "fee_rate": open_pos["fee_rate"]})
    op_obj = OpenPosition(sym, open_pos["side"], int(T), open_pos["shares"], float(open_pos["ask_t1"]), float(open_pos["cost_1s"]), float(open_pos["entry_p_true"]), float(open_pos["entry_z"]))
    ex = evaluate_exit(c_sym, op_obj, float(elapsed), fv, bk)
    if ex is None:
        return None
    r1 = sec_maps[sym].get(t_sec + 1)
    b1 = (r1.up_bid1 if open_pos["side"] == "UP" else r1.down_bid1) if r1 else None
    bsz1 = (r1.up_bid1_sz if open_pos["side"] == "UP" else r1.down_bid1_sz) if r1 else None
    if b1 is None or bsz1 is None or bsz1 < open_pos["shares"]:
        return None
    proc = effective_exit_proceeds(float(b1), open_pos["fee_rate"])
    pnl1 = round(proc - open_pos["cost_1s"], 5)
    pnl2 = round(proc - float(open_pos["cost_2s"]), 5) if (open_pos["filled_2s"] and pd.notna(open_pos["cost_2s"])) else open_pos["pnl_per_share_2s"]
    open_pos.update({
        "exit_reason": ex.reason, "exit_ts": t_sec + 1, "hold_sec": (t_sec + 1) - int(open_pos["signal_ts"]),
        "exit_bid_1s": b1, "exit_sz_1s": bsz1, "pnl_per_share": pnl1,
        "pnl_per_share_2s": pnl2, "pnl_usd": round(pnl1 * open_pos["shares"], 4),
    })
    return open_pos


def _cfg_from_dict(raw: dict[str, Any]) -> SniperConfig:
    return SniperConfig(**{k: (tuple(v) if k == "slippage_ladder" else v) for k, v in raw.items()})


def _sim_one_cfg(
    cfg: SniperConfig, fee_map: Dict[str, float], settled_ts: List[int],
    snap_by_wt_sym: Dict[Any, pd.DataFrame], w_meta: Dict[Tuple[int, str], Any],
) -> pd.DataFrame:
    trackers = {s: RollingMicrostructureTracker(cfg.vol_lookback_sec, cfg.cvd_window_sec) for s in SYMBOLS}
    trades_1s: List[dict[str, Any]] = []
    eq = 100.0
    for T in settled_ts:
        sec_maps = {s: ({int(r.ts_sec): r for r in g.itertuples(index=False)} if (g := snap_by_wt_sym.get((int(T), s))) is not None else {}) for s in SYMBOLS}
        twap_acc: Dict[str, List[float]] = {s: [] for s in SYMBOLS}
        open_pos, entered = None, False
        for t_sec in range(int(T), int(T) + 300):
            tick = (int(T), t_sec, t_sec - int(T))
            views = _build_tick_views(tick, sec_maps, w_meta, trackers, cfg, twap_acc)
            if open_pos is None and not entered:
                open_pos, unfilled = _try_enter_window(tick, views, (sec_maps, w_meta), cfg, fee_map, eq)
                entered = open_pos is not None or unfilled is not None
                if unfilled is not None:
                    trades_1s.append(unfilled)
            elif open_pos is not None:
                done = _try_exit_pos(open_pos, tick, views, sec_maps, cfg)
                if done is not None:
                    trades_1s.append(done)
                    open_pos = None
        if open_pos is not None:
            wm = w_meta.get((int(T), open_pos["symbol"]))
            out = wm.resolved_outcome if wm else None
            pnl1 = round((1.0 if out == open_pos["side"] else 0.0) - open_pos["cost_1s"], 5)
            open_pos.update({"resolved_outcome": out, "hold_sec": (int(T) + 300) - int(open_pos["signal_ts"]), "pnl_per_share": pnl1, "pnl_usd": round(pnl1 * open_pos["shares"], 4)})
            trades_1s.append(open_pos)
    return pd.DataFrame(trades_1s)


def _bootstrap_cis(vals: np.ndarray) -> Tuple[Optional[List[float]], Optional[List[float]]]:
    clean = vals[np.isfinite(vals)]
    if len(clean) < 2:
        return None, None
    rng = np.random.default_rng(20261121)
    idx = rng.integers(0, len(clean), size=(2000, len(clean)))
    means = clean[idx].mean(axis=1)
    c95 = [round(float(x), 5) for x in np.percentile(means, [2.5, 97.5])]
    c98 = [round(float(x), 5) for x in np.percentile(means, [0.8333, 99.1667])]
    return c95, c98


def _summarize_by_coin(df_t: pd.DataFrame) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for sym in SYMBOLS:
        sub = df_t[df_t["symbol"] == sym]
        if len(sub) == 0:
            continue
        f1 = sub[sub["filled_1s"] == True]
        m1 = sub[sub["filled_1s"] != True]
        c95, c98 = _bootstrap_cis(f1["pnl_per_share"].to_numpy(dtype=float)) if len(f1) else (None, None)
        mw = m1["would_have_won"].dropna()
        out[sym] = {
            "signals": int(len(sub)), "filled_1s": int(len(f1)), "missed_1s": int(len(m1)),
            "miss_rate": round(float(len(m1) / len(sub)), 4),
            "mean_spread_t0": round(float(sub["spread_t0"].dropna().mean()), 5) if sub["spread_t0"].notna().any() else None,
            "wr_1s": round(float((f1["pnl_per_share"] > 0).mean()), 4) if len(f1) else None,
            "missed_would_win_rate": round(float(mw.astype(float).mean()), 4) if len(mw) else None,
            "mean_pnl_1s": round(float(f1["pnl_per_share"].mean()), 5) if len(f1) else None,
            "ci95_1s": c95, "ci98_3_bonferroni_1s": c98,
        }
    return out


def _build_daily_cumulative(df_t: pd.DataFrame) -> List[Dict[str, Any]]:
    if len(df_t) == 0:
        return []
    work = df_t.copy()
    work["utc_day"] = pd.to_datetime(work["window_ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
    rows: List[Dict[str, Any]] = []
    for day in sorted(work["utc_day"].unique()):
        cum = work[work["utc_day"] <= day]
        f1 = cum[cum["filled_1s"] == True]
        c95, c98 = _bootstrap_cis(f1["pnl_per_share"].to_numpy(dtype=float)) if len(f1) else (None, None)
        rows.append({
            "utc_day": day, "cum_signals": int(len(cum)), "cum_filled_1s": int(len(f1)),
            "cum_miss_rate": round(float((cum["filled_1s"] != True).mean()), 4),
            "cum_mean_pnl_1s": round(float(f1["pnl_per_share"].mean()), 5) if len(f1) else None,
            "cum_ci95_1s": c95, "cum_ci98_3_bonferroni_1s": c98, "gate_ready": bool(len(f1) >= 300),
        })
    return rows


def _summarize_model_trades(df_t: pd.DataFrame) -> Dict[str, Any]:
    if len(df_t) == 0:
        return {"signals": 0, "filled_1s": 0, "missed_1s": 0, "miss_rate": None, "mean_pnl_1s": None, "filled_2s": 0, "gate_ready": False}
    f1 = df_t[df_t["filled_1s"] == True]
    f2 = df_t[df_t["filled_2s"] == True]
    m1 = df_t[df_t["filled_1s"] != True]
    c95, c98 = _bootstrap_cis(f1["pnl_per_share"].to_numpy(dtype=float)) if len(f1) else (None, None)
    pos_coins = int(sum(float(g["pnl_per_share"].mean()) > 0 for _, g in f1.groupby("symbol"))) if len(f1) else 0
    mw, fw = m1["would_have_won"].dropna(), f1["would_have_won"].dropna()
    return {
        "signals": int(len(df_t)), "filled_1s": int(len(f1)), "missed_1s": int(len(m1)),
        "miss_rate": round(float(len(m1) / len(df_t)), 4), "filled_2s": int(len(f2)),
        "positive_coins_1s": pos_coins, "wr_1s": round(float((f1["pnl_per_share"] > 0).mean()), 4) if len(f1) else None,
        "missed_would_win_rate": round(float(mw.astype(float).mean()), 4) if len(mw) else None,
        "filled_would_win_rate": round(float(fw.astype(float).mean()), 4) if len(fw) else None,
        "mean_spread_t0": round(float(df_t["spread_t0"].dropna().mean()), 5) if df_t["spread_t0"].notna().any() else None,
        "mean_pnl_1s": round(float(f1["pnl_per_share"].mean()), 5) if len(f1) else None,
        "mean_pnl_2s": round(float(f2["pnl_per_share_2s"].dropna().mean()), 5) if f2["pnl_per_share_2s"].notna().any() else None,
        "ci95_1s": c95, "ci98_3_bonferroni_1s": c98,
        "gate_ready": bool(len(f1) >= 300),
        "by_coin": _summarize_by_coin(df_t),
        "daily_cumulative": _build_daily_cumulative(df_t),
    }


def _append_shadow_section(lines: List[str], shadow: Dict[str, Any]) -> None:
    lines += [
        "",
        "## Shadow Model (`v2_highband_shadow`: [0.84, 0.95] — Excluded from 3-Config Bonferroni Gate & Launch Decision)",
        "",
        "| Split | Signals | Filled +1s | Missed | Miss Rate | Filled WR | Missed Would-Win | Mean Spread t0 | Mean PnL +1s | 95% CI (Ref Only) |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    sh = shadow.get("v2_highband_shadow") or {}
    for split_lbl in ("post_nov21", "pre_nov21"):
        st = sh.get(split_lbl) or {}
        lines.append(
            f"| `{split_lbl}` | {st.get('signals', 0)} | {st.get('filled_1s', 0)} | {st.get('missed_1s', 0)} | "
            f"{st.get('miss_rate')} | {st.get('wr_1s')} | {st.get('missed_would_win_rate')} | "
            f"{st.get('mean_spread_t0')} | {st.get('mean_pnl_1s')} | `{st.get('ci95_1s')}` |"
        )


def _write_daily_report_md(report: Dict[str, Any], out_dir: pathlib.Path) -> pathlib.Path:
    lines = [
        "# Section 4.2 Daily Monitoring Report (Read-Only, Pre-Registered 3 Configs, Bonferroni 98.3% CI)",
        "",
        f"- Generated UTC: `{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}`",
        f"- Primary evaluation window boundary: `>= 2026-11-21T00:00:00Z` (`window_ts >= {NOV21_UTC_TS}`)",
        f"- Windows recorded / settled: `{report['windows_recorded']}` / `{report['windows_settled']}` | R1 TWAP parity: `{report['r1_twap_parity']}`",
        "",
        "## Primary Gate (`post_nov21`) — Gate Pending Until `filled_1s >= 300`",
        "",
        "| Model | Signals | Filled +1s | Missed | Miss Rate | Filled WR | Missed Would-Win WR | Mean PnL +1s | 95% CI | 98.3% Bonferroni CI | Status |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|---|---|",
    ]
    for lbl, st in report["post_nov21"].items():
        status = "GATE_READY_CHECK_CI" if st.get("gate_ready") else f"PENDING ({st.get('filled_1s', 0)}/300)"
        lines.append(
            f"| `{lbl}` | {st.get('signals', 0)} | {st.get('filled_1s', 0)} | {st.get('missed_1s', 0)} | "
            f"{st.get('miss_rate')} | {st.get('wr_1s')} | {st.get('missed_would_win_rate')} | "
            f"{st.get('mean_pnl_1s')} | `{st.get('ci95_1s')}` | `{st.get('ci98_3_bonferroni_1s')}` | **{status}** |"
        )
    _append_shadow_section(lines, report.get("shadow") or {})
    for sec_name in ("post_nov21", "pre_nov21"):
        sec_title = "Primary Per-Coin (`post_nov21`)" if sec_name == "post_nov21" else "Pre-Nov21 Baseline (`window_ts < 1795219200`)"
        lines += ["", f"## {sec_title}", "", "| Model | Coin | Signals | Filled +1s | Miss Rate | Mean Spread t0 | Filled WR | Missed Would-Win | Mean PnL +1s | 98.3% Bonferroni CI |", "|---|---|---:|---:|---:|---:|---:|---:|---:|---|"]
        for lbl, st in report[sec_name].items():
            for sym, cs in (st.get("by_coin") or {}).items():
                lines.append(
                    f"| `{lbl}` | `{sym}` | {cs['signals']} | {cs['filled_1s']} | {cs['miss_rate']} | "
                    f"{cs['mean_spread_t0']} | {cs['wr_1s']} | {cs['missed_would_win_rate']} | "
                    f"{cs['mean_pnl_1s']} | `{cs['ci98_3_bonferroni_1s']}` |"
                )
    p = out_dir / "phase42_daily_report.md"
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return p


def analyze_recording(
    db_path: pathlib.Path = DEFAULT_DB, frozen_path: pathlib.Path = FROZEN_CFG_PATH,
    out_dir: pathlib.Path = ROOT, daily_report: bool = False,
) -> dict[str, Any]:
    """Conservative (+1s / +2s) 3-config Bonferroni simulation + excluded shadow tracker."""
    frozen = json.loads(frozen_path.read_text(encoding="utf-8"))
    fee_map = frozen.get("effective_fees_post_nov20", {"BTCUSDT": 0.05, "ETHUSDT": 0.01, "SOLUSDT": 0.01})
    con = sqlite3.connect(db_path)
    df_w = pd.read_sql_query("SELECT * FROM windows_42 ORDER BY window_ts, symbol", con)
    df_s = pd.read_sql_query("SELECT * FROM snapshots_1hz ORDER BY window_ts, symbol, elapsed_sec", con)
    con.close()
    settled = df_w[df_w["resolved_outcome"].isin(["UP", "DOWN"])]
    has_tw = settled[settled["twap_strike"].notna() & settled["twap_close"].notna()]
    tw_match = int(((has_tw["twap_close"] >= has_tw["twap_strike"]).map({True: "UP", False: "DOWN"}) == has_tw["resolved_outcome"]).sum())
    r1_parity = (tw_match / len(has_tw)) if len(has_tw) else np.nan
    snap_by_wt_sym = {k: g.sort_values("elapsed_sec") for k, g in df_s.groupby(["window_ts", "symbol"])}
    w_meta = {(int(r.window_ts), str(r.symbol)): r for r in df_w.itertuples(index=False)}
    settled_ts = sorted(settled["window_ts"].unique())
    models = {
        "v2_pure": _cfg_from_dict(frozen["sniper_config_v2_pure"]) if "sniper_config_v2_pure" in frozen else TWAP_V2_PURE_CONFIG,
        "v2_filtered": _cfg_from_dict(frozen["sniper_config_v2_filtered"]) if "sniper_config_v2_filtered" in frozen else TWAP_V2_FILTERED_CONFIG,
        "v1": _cfg_from_dict(frozen["sniper_config"]),
    }
    post_summaries: Dict[str, Any] = {}
    pre_summaries: Dict[str, Any] = {}
    combined: Dict[str, Any] = {}
    for lbl, cfg in models.items():
        df_t = _sim_one_cfg(cfg, fee_map, settled_ts, snap_by_wt_sym, w_meta)
        df_t.to_csv(out_dir / f"phase42_trades_1s_{lbl}.csv", index=False)
        pre_summaries[lbl] = _summarize_model_trades(df_t[df_t["window_ts"] < NOV21_UTC_TS] if len(df_t) else df_t)
        post_summaries[lbl] = _summarize_model_trades(df_t[df_t["window_ts"] >= NOV21_UTC_TS] if len(df_t) else df_t)
        combined[lbl] = _summarize_model_trades(df_t)
    raw_sh = {k: v for k, v in (frozen.get("shadow_config_v2_highband") or {}).items() if k != "_meta"}
    sh_cfg = _cfg_from_dict(raw_sh) if raw_sh else TWAP_V2_HIGHBAND_SHADOW_CONFIG
    df_sh = _sim_one_cfg(sh_cfg, fee_map, settled_ts, snap_by_wt_sym, w_meta)
    df_sh.to_csv(out_dir / "phase42_trades_1s_v2_highband_shadow.csv", index=False)
    shadow_out = {
        "v2_highband_shadow": {
            "bonferroni_included": False,
            "post_nov21": _summarize_model_trades(df_sh[df_sh["window_ts"] >= NOV21_UTC_TS] if len(df_sh) else df_sh),
            "pre_nov21": _summarize_model_trades(df_sh[df_sh["window_ts"] < NOV21_UTC_TS] if len(df_sh) else df_sh),
        }
    }
    v2_post = post_summaries["v2_filtered"]
    report: Dict[str, Any] = {
        "windows_recorded": int(len(df_w)), "windows_settled": int(len(settled)),
        "r1_twap_parity": None if np.isnan(r1_parity) else round(float(r1_parity), 4),
        "primary_eval_start_ts": NOV21_UTC_TS, "signals_total_post_nov21": v2_post["signals"],
        "filled_1s_post_nov21": v2_post["filled_1s"], "mean_pnl_per_share_1s_post_nov21": v2_post["mean_pnl_1s"],
        "post_nov21": post_summaries, "pre_nov21": pre_summaries, "all_combined": combined, "shadow": shadow_out,
    }
    if daily_report:
        report["daily_report_path"] = str(_write_daily_report_md(report, out_dir))
    return report


def main() -> None:
    """CLI entry point for freezing config, recording live feeds, or analyzing recorded DB."""
    ap = argparse.ArgumentParser(description="Section 4.2 3-Coin Read-Only Recorder & Simulator")
    ap.add_argument("--db", type=pathlib.Path, default=DEFAULT_DB)
    ap.add_argument("--duration", type=int, default=0, help="Seconds to record (0 = unlimited)")
    ap.add_argument("--freeze-config", action="store_true", help="Write frozen_config.json and exit")
    ap.add_argument("--analyze", action="store_true", help="Run +1s/+2s offline fill simulation")
    ap.add_argument("--daily-report", action="store_true", help="Write phase42_daily_report.md during --analyze")
    args = ap.parse_args()
    if args.freeze_config:
        cfg = write_frozen_config()
        print(json.dumps({"frozen_config_written": str(FROZEN_CFG_PATH), "git_sha": cfg["git_sha"]}, indent=2))
        return
    if args.analyze:
        print(json.dumps(analyze_recording(args.db, daily_report=args.daily_report), indent=2))
        return
    if not FROZEN_CFG_PATH.exists():
        write_frozen_config()
    asyncio.run(LiveRecorder42(args.db).run_record(duration_sec=args.duration))


if __name__ == "__main__":
    main()
