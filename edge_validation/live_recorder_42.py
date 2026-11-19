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
from dataclasses import asdict, dataclass, field
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
    SniperConfig,
    compute_shares,
    effective_exit_proceeds,
    entry_limit_price,
    taker_fee_per_share,
)
from engine.signal_core import (
    DEFAULT_MAX_AGES,
    MIN_VOL_SAMPLES,
    Bn1sMicroTracker,
    MarketInputs,
    SignalDecision,
    choose_best_candidate,
    compute_fv,
    compute_z_twap_proj_diagnostic,
    directional_book_age,
    evaluate as evaluate_signal,
    simulate_fak_buy,
    simulate_fak_sell,
)

LOG = logging.getLogger("recorder42")
SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
COIN_MAP = {"BTCUSDT": "btc", "ETHUSDT": "eth", "SOLUSDT": "sol"}
BINANCE_WS = "wss://stream.binance.com:9443/stream?streams=" + "/".join(
    f"{s.lower()}@{kind}" for s in SYMBOLS for kind in ("aggTrade", "bookTicker")
)
CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_URL = "https://gamma-api.polymarket.com/events"
CLOB_BOOK_URL = "https://clob.polymarket.com/book"

DEFAULT_DB = REPO / "data/validation_42/live_3coin_42.db"
DEFAULT_SHADOW_LOG = REPO / "data/validation_42/runner_shadow_events.jsonl"
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
        CREATE TABLE IF NOT EXISTS signal_enters_42 (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts_sec INTEGER NOT NULL, window_ts INTEGER NOT NULL,
            symbol TEXT NOT NULL, model_name TEXT NOT NULL, side TEXT NOT NULL,
            z REAL, p_model REAL, sigma REAL, ask_top REAL,
            cl_twap_age_sec REAL, cl_spot_age_sec REAL, bn_age_sec REAL, book_age_sec REAL,
            UNIQUE(ts_sec, symbol, model_name)
        );
        CREATE TABLE IF NOT EXISTS signal_skips_42 (
            ts_sec INTEGER NOT NULL, window_ts INTEGER NOT NULL, model_name TEXT NOT NULL,
            reason TEXT NOT NULL, count INTEGER NOT NULL,
            PRIMARY KEY (ts_sec, model_name, reason)
        );
        CREATE INDEX IF NOT EXISTS idx_snap_win ON snapshots_1hz(window_ts, symbol, elapsed_sec);
        CREATE INDEX IF NOT EXISTS idx_trades_win ON pm_trades_42(window_ts, symbol);
        CREATE INDEX IF NOT EXISTS idx_enters_win ON signal_enters_42(window_ts, model_name);
        """
    )
    existing_cols = {row[1] for row in con.execute("PRAGMA table_info(snapshots_1hz)").fetchall()}
    extra_cols = (
        "cl_twap_age_sec", "cl_spot_age_sec", "bn_age_sec", "up_book_age_sec", "down_book_age_sec",
        "cl_twap_age_ms", "cl_spot_age_ms", "bn_age_ms", "up_book_age_ms", "down_book_age_ms",
        "cl_twap_lag_ms", "cl_spot_lag_ms", "bn_lag_ms", "up_book_lag_ms", "down_book_lag_ms",
    )
    for col in extra_cols:
        if col not in existing_cols:
            con.execute(f"ALTER TABLE snapshots_1hz ADD COLUMN {col} REAL")
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
    path: pathlib.Path = FROZEN_CFG_PATH,
    fee_rate_override: float | None = None,
    git_sha_override: str | None = None,
    count_start_window_ts: int | None = None,
    reset_note: str | None = None,
) -> dict[str, Any]:
    """Write frozen Section 4.2 parameters, SHA, fee schedule, and pre-registered gates."""
    sha, dirty = _resolve_git_sha(REPO)
    if git_sha_override:
        sha, dirty = git_sha_override, False
    start_wt = int(count_start_window_ts) if count_start_window_ts is not None else ((int(time.time()) // 300) + 1) * 300
    cfg_dict = asdict(DEFAULT_SNIPER_CONFIG)
    if fee_rate_override is not None:
        cfg_dict["fee_rate"] = float(fee_rate_override)
    payload = {
        "schema_version": "4.2-v2-bonferroni",
        "frozen_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "git_sha": sha,
        "engine_dirty": dirty,
        "count_start_window_ts": start_wt,
        "reset_note": str(reset_note or ""),
        "primary_eval_start_utc": "2026-11-21T00:00:00Z",
        "primary_eval_start_ts": NOV21_UTC_TS,
        "position_policy": "single_slot",
        "fill_model": "fak_depth_aware_+1s",
        "vol_source": "binance_1s_close",
        "vol_warmup_sec": 1800,
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
        self.bn_last_ts: float = 0.0
        self.bn_recv_mono_ms: float = 0.0
        self.bn_recv_wall_ms: float = 0.0
        self.bn_source_ts_ms: int = 0
        self.bn_last_lag_ms: Optional[float] = None
        self.bn_vol = self.bn_buy = self.bn_sell = 0.0
        self.bn_n = 0
        self.up_bids_raw: Dict[float, float] = {}
        self.up_asks_raw: Dict[float, float] = {}
        self.dn_bids_raw: Dict[float, float] = {}
        self.dn_asks_raw: Dict[float, float] = {}
        self._up_bids: List[Tuple[float, float]] = []
        self._up_asks: List[Tuple[float, float]] = []
        self._dn_bids: List[Tuple[float, float]] = []
        self._dn_asks: List[Tuple[float, float]] = []
        self._up_dirty: bool = False
        self._dn_dirty: bool = False
        self.up_book_ts: float = 0.0
        self.dn_book_ts: float = 0.0
        self.up_ev_ts_ms: int = 0
        self.dn_ev_ts_ms: int = 0
        self.up_recv_mono_ms: float = 0.0
        self.up_recv_wall_ms: float = 0.0
        self.up_source_ts_ms: int = 0
        self.up_last_lag_ms: Optional[float] = None
        self.dn_recv_mono_ms: float = 0.0
        self.dn_recv_wall_ms: float = 0.0
        self.dn_source_ts_ms: int = 0
        self.dn_last_lag_ms: Optional[float] = None

    def reset_window(self, window_ts: int) -> None:
        """Reset market tokens and full-depth orderbooks on window rollover."""
        self.window_ts = window_ts
        self.slug = f"{COIN_MAP[self.symbol]}-updown-5m-{window_ts}"
        self.condition_id = self.up_token = self.down_token = ""
        self.up_bids_raw.clear()
        self.up_asks_raw.clear()
        self.dn_bids_raw.clear()
        self.dn_asks_raw.clear()
        self._up_bids.clear()
        self._up_asks.clear()
        self._dn_bids.clear()
        self._dn_asks.clear()
        self._up_dirty = self._dn_dirty = False
        self.up_book_ts = 0.0
        self.dn_book_ts = 0.0
        self.up_ev_ts_ms = self.dn_ev_ts_ms = 0
        self.up_recv_mono_ms = self.up_recv_wall_ms = 0.0
        self.dn_recv_mono_ms = self.dn_recv_wall_ms = 0.0
        self.up_source_ts_ms = self.dn_source_ts_ms = 0
        self.up_last_lag_ms = self.dn_last_lag_ms = None

    def flush_bn_bucket(self) -> Tuple[float, float, float, int]:
        """Atomically read and reset accumulated Binance trades since previous 1Hz snapshot."""
        vals = (self.bn_vol, self.bn_buy, self.bn_sell, self.bn_n)
        self.bn_vol = self.bn_buy = self.bn_sell = 0.0
        self.bn_n = 0
        return vals

    def mark_dirty(self, side: str) -> None:
        """Mark book side dirty so O(1) WS updates defer sorting until read."""
        if side == "UP":
            self._up_dirty = True
        else:
            self._dn_dirty = True

    def refresh_top_levels(self, side: str) -> None:
        """Project full-depth book maps down to sorted top-10 lists for 1Hz snapshot/eval."""
        if side == "UP":
            self._up_bids = sorted(self.up_bids_raw.items(), key=lambda t: t[0], reverse=True)[:10]
            self._up_asks = sorted(self.up_asks_raw.items(), key=lambda t: t[0])[:10]
            self._up_dirty = False
        else:
            self._dn_bids = sorted(self.dn_bids_raw.items(), key=lambda t: t[0], reverse=True)[:10]
            self._dn_asks = sorted(self.dn_asks_raw.items(), key=lambda t: t[0])[:10]
            self._dn_dirty = False

    @property
    def up_bids(self) -> List[Tuple[float, float]]:
        """Lazy top-10 UP bids sorted descending."""
        if self._up_dirty:
            self.refresh_top_levels("UP")
        return self._up_bids

    @property
    def up_asks(self) -> List[Tuple[float, float]]:
        """Lazy top-10 UP asks sorted ascending."""
        if self._up_dirty:
            self.refresh_top_levels("UP")
        return self._up_asks

    @property
    def dn_bids(self) -> List[Tuple[float, float]]:
        """Lazy top-10 DOWN bids sorted descending."""
        if self._dn_dirty:
            self.refresh_top_levels("DOWN")
        return self._dn_bids

    @property
    def dn_asks(self) -> List[Tuple[float, float]]:
        """Lazy top-10 DOWN asks sorted ascending."""
        if self._dn_dirty:
            self.refresh_top_levels("DOWN")
        return self._dn_asks


class LiveRecorder42:
    """Async multi-feed read-only recorder for BTC/ETH/SOL depth, Chainlink TWAP, and Binance 1s."""

    def __init__(self, db_path: pathlib.Path = DEFAULT_DB) -> None:
        self.db_path = db_path
        self.con = init_db(db_path)
        self.chainlink = ChainlinkRTDSFeed()
        self.states: Dict[str, CoinState42] = {s: CoinState42(s) for s in SYMBOLS}
        self.micro: Dict[str, Bn1sMicroTracker] = {s: Bn1sMicroTracker(1800, 20) for s in SYMBOLS}
        self.strikes: Dict[Tuple[int, str], Optional[float]] = {}
        self.token_to_meta: Dict[str, Tuple[str, str]] = {}
        self.prefetched: Dict[Tuple[str, int], dict[str, Any]] = {}
        self._stop = asyncio.Event()
        self._clob_resub = asyncio.Event()
        self._clob_stale_since: float = 0.0
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
        stream = str(msg.get("stream", ""))
        data = msg.get("data") or {}
        sym = stream.split("@")[0].upper()
        st = self.states.get(sym)
        if st is None:
            return
        now_m = time.monotonic_ns() / 1_000_000.0
        now_r = time.time()
        src_ms = int(data.get("E") or data.get("T") or 0)
        st.bn_recv_mono_ms = now_m
        st.bn_recv_wall_ms = now_r * 1000.0
        if src_ms > 0:
            st.bn_source_ts_ms = src_ms
            st.bn_last_lag_ms = round(now_r * 1000.0 - float(src_ms), 2)
        if "bookTicker" in stream and "b" in data and "a" in data:
            st.bn_last_ts = now_r
            if st.bn_last_px is None:
                st.bn_last_px = 0.5 * (float(data["b"]) + float(data["a"]))
            return
        if "p" not in data or "q" not in data:
            return
        px, qty = float(data["p"]), float(data["q"])
        st.bn_last_px = px
        st.bn_last_ts = now_r
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

    @staticmethod
    def _parse_src_ms(raw_ts: Any) -> int:
        if raw_ts is None:
            return 0
        try:
            v = float(raw_ts)
        except (TypeError, ValueError):
            return 0
        return int(round(v if v > 1e11 else v * 1000.0)) if v > 0.0 else 0

    @staticmethod
    def _parse_book_dict(raw_lvls: Any) -> Dict[float, float]:
        out: Dict[float, float] = {}
        for x in raw_lvls or ():
            px, sz = float(x.get("price") or 0), float(x.get("size") or 0)
            if sz > 0.0 and 0.0 < px < 1.0:
                out[px] = sz
        return out

    @staticmethod
    def _stamp_book_recv(st: CoinState42, side: str, ts_tuple: Tuple[float, float, int, bool]) -> None:
        now_m, now_r, src_ms, via_ws = ts_tuple
        if side == "UP":
            st.up_book_ts = now_r
            st.up_recv_mono_ms, st.up_recv_wall_ms = now_m, now_r * 1000.0
            if src_ms > 0:
                st.up_ev_ts_ms = max(st.up_ev_ts_ms, src_ms)
            if via_ws and src_ms > 0:
                st.up_source_ts_ms = src_ms
                st.up_last_lag_ms = round(now_r * 1000.0 - float(src_ms), 2)
        else:
            st.dn_book_ts = now_r
            st.dn_recv_mono_ms, st.dn_recv_wall_ms = now_m, now_r * 1000.0
            if src_ms > 0:
                st.dn_ev_ts_ms = max(st.dn_ev_ts_ms, src_ms)
            if via_ws and src_ms > 0:
                st.dn_source_ts_ms = src_ms
                st.dn_last_lag_ms = round(now_r * 1000.0 - float(src_ms), 2)

    def _note_clob_freshness(self, now_r: float, src_ms: int) -> bool:
        if src_ms > 0 and (now_r * 1000.0 - float(src_ms)) > 2500.0:
            self._clob_stale_since = self._clob_stale_since or now_r
            if (now_r - self._clob_stale_since) >= 1.5:
                self._clob_resub.set()
            return False
        self._clob_stale_since = 0.0
        return True

    def _apply_book(
        self, token_id: str, bids_raw: Any, asks_raw: Any, source_ts_ms: int = 0, via_ws: bool = True
    ) -> None:
        meta = self.token_to_meta.get(token_id)
        if not meta:
            return
        sym, side = meta
        st = self.states[sym]
        held_ev = st.up_ev_ts_ms if side == "UP" else st.dn_ev_ts_ms
        if source_ts_ms > 0 and source_ts_ms < held_ev - 50:
            return
        now_m, now_r = time.monotonic_ns() / 1_000_000.0, time.time()
        if bids_raw is not None:
            setattr(st, "up_bids_raw" if side == "UP" else "dn_bids_raw", self._parse_book_dict(bids_raw))
        if asks_raw is not None:
            setattr(st, "up_asks_raw" if side == "UP" else "dn_asks_raw", self._parse_book_dict(asks_raw))
        st.mark_dirty(side)
        self._stamp_book_recv(st, side, (now_m, now_r, source_ts_ms, via_ws))

    def _apply_price_change(self, msg: dict[str, Any]) -> None:
        now_m, now_r = time.monotonic_ns() / 1_000_000.0, time.time()
        top_src = self._parse_src_ms(msg.get("timestamp"))
        if not self._note_clob_freshness(now_r, top_src):
            return
        touched: Dict[Tuple[str, str], int] = {}
        for ch in msg.get("price_changes") or ():
            meta = self.token_to_meta.get(str(ch.get("asset_id") or ""))
            px, sz = float(ch.get("price") or 0), float(ch.get("size") or 0)
            if not meta or not 0.0 < px < 1.0:
                continue
            sym, side = meta
            st = self.states[sym]
            src_ms = top_src or self._parse_src_ms(ch.get("timestamp"))
            held_ev = st.up_ev_ts_ms if side == "UP" else st.dn_ev_ts_ms
            if src_ms > 0 and src_ms < held_ev - 50:
                continue
            b_side = str(ch.get("side") or "").upper()
            target = (st.up_bids_raw if side == "UP" else st.dn_bids_raw) if b_side == "BUY" else (
                (st.up_asks_raw if side == "UP" else st.dn_asks_raw) if b_side == "SELL" else None
            )
            if target is None:
                continue
            target.pop(px, None) if sz <= 0.0 else target.__setitem__(px, sz)
            touched[(sym, side)] = max(touched.get((sym, side), 0), src_ms)
        for (sym, side), src_ms in touched.items():
            st = self.states[sym]
            st.mark_dirty(side)
            self._stamp_book_recv(st, side, (now_m, now_r, src_ms, True))

    def _handle_clob_msg(self, raw: str) -> None:
        if not raw or raw in ("PING", "PONG"):
            return
        obj = json.loads(raw)
        for m in (obj if isinstance(obj, list) else [obj]):
            if not isinstance(m, dict):
                continue
            ev_type = str(m.get("event_type") or "")
            if ev_type == "book" and m.get("asset_id"):
                src_ms = self._parse_src_ms(m.get("timestamp"))
                if self._note_clob_freshness(time.time(), src_ms):
                    self._apply_book(str(m["asset_id"]), m.get("bids"), m.get("asks"), src_ms, via_ws=True)
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
            self._clob_stale_since = 0.0
            try:
                async with websockets.connect(CLOB_WS, open_timeout=8.0, ping_interval=15.0, ping_timeout=10.0, max_queue=64) as ws:
                    await ws.send(json.dumps({"assets_ids": assets, "type": "market"}))
                    while not self._stop.is_set() and not self._clob_resub.is_set():
                        self._handle_clob_msg(await asyncio.wait_for(ws.recv(), timeout=12.0))
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
        now_m = time.monotonic_ns() / 1_000_000.0
        for tok, empty, m_ts in (
            (st.up_token, not st.up_asks, st.up_recv_mono_ms),
            (st.down_token, not st.dn_asks, st.dn_recv_mono_ms),
        ):
            stale = m_ts <= 0.0 or (now_m - m_ts) >= 2500.0
            if not tok or not (empty or stale):
                continue
            try:
                rb = await client.get(CLOB_BOOK_URL, params={"token_id": tok}, timeout=3.0)
                if rb.status_code == 200:
                    bk = rb.json()
                    self._apply_book(tok, bk.get("bids"), bk.get("asks"), self._parse_src_ms(bk.get("timestamp")), via_ws=False)
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

    @staticmethod
    def _age_lag_ms(mono_ms: float, last_lag_ms: Optional[float], now_m: float) -> Tuple[Optional[float], Optional[float], float]:
        if mono_ms <= 0.0:
            return None, None, 9999.0
        a_ms = max(0.0, now_m - mono_ms)
        return round(a_ms, 2), last_lag_ms, round(a_ms / 1000.0, 3)

    def _roll_window_if_needed(self, cur_t: int) -> None:
        changed = False
        for sym, st in self.states.items():
            if st.window_ts == cur_t:
                continue
            st.reset_window(cur_t)
            pre = self.prefetched.pop((sym, cur_t), None)
            if pre:
                st.condition_id, st.up_token, st.down_token = pre["condition_id"], pre["up_token"], pre["down_token"]
                self.token_to_meta[st.up_token], self.token_to_meta[st.down_token] = (sym, "UP"), (sym, "DOWN")
            changed = True
        if changed:
            keep = {tok for st in self.states.values() for tok in (st.up_token, st.down_token) if tok}
            self.token_to_meta = {k: v for k, v in self.token_to_meta.items() if k in keep}
            self._clob_resub.set()

    def _record_second(self, ts_sec: int) -> None:
        cur_t, elapsed = (ts_sec // 300) * 300, ts_sec % 300
        self._roll_window_if_needed(cur_t)
        now_m = time.monotonic_ns() / 1_000_000.0
        rtds_ok = bool(getattr(self.chainlink, "connected", True))
        skips_sec: Dict[str, int] = {}
        for sym, st in self.states.items():
            cl_spot, sp_ms, sp_lag = self.chainlink.get_latest_price_metrics(sym, max_age_sec=9999.0)
            cl_twap, tw_ms, tw_lag = self.chainlink.get_latest_twap_metrics(sym, max_age_sec=9999.0)
            sp_age = round(sp_ms / 1000.0, 3) if (rtds_ok and sp_ms < 9_000_000.0) else 9999.0
            tw_age = round(tw_ms / 1000.0, 3) if (rtds_ok and tw_ms < 9_000_000.0) else 9999.0
            sp_ms_v = round(sp_ms, 2) if (rtds_ok and sp_ms < 9_000_000.0) else None
            tw_ms_v = round(tw_ms, 2) if (rtds_ok and tw_ms < 9_000_000.0) else None
            bn_ms_v, bn_lag, bn_age = self._age_lag_ms(st.bn_recv_mono_ms if st.bn_last_px else 0.0, st.bn_last_lag_ms, now_m)
            up_ms_v, up_lag, up_age = self._age_lag_ms(st.up_recv_mono_ms, st.up_last_lag_ms, now_m)
            dn_ms_v, dn_lag, dn_age = self._age_lag_ms(st.dn_recv_mono_ms, st.dn_last_lag_ms, now_m)
            tw_s = self.chainlink.get_settlement_twap_at(sym, cur_t)
            sp_s = self.chainlink.bucket_opens[sym].get(cur_t, (None, 0))[0]
            self._upsert_window_row(cur_t, elapsed, sym, st, tw_s, sp_s)
            bn_flush = st.flush_bn_bucket()
            ms_metrics = (tw_ms_v, sp_ms_v, bn_ms_v, up_ms_v, dn_ms_v, tw_lag, sp_lag, bn_lag, up_lag, dn_lag)
            self._insert_snapshot_row(ts_sec, cur_t, elapsed, sym, st, (cl_spot, cl_twap, tw_age, sp_age, bn_age, up_age, dn_age, ms_metrics, bn_flush))
            dec, ages = self._eval_live_symbol(ts_sec, cur_t, sym, st, (cl_spot, cl_twap, tw_age, sp_age, bn_age, up_age, dn_age, tw_s, bn_flush))
            if dec.action == "ENTER":
                self.con.execute(
                    "INSERT OR IGNORE INTO signal_enters_42 (ts_sec,window_ts,symbol,model_name,side,z,p_model,sigma,ask_top,cl_twap_age_sec,cl_spot_age_sec,bn_age_sec,book_age_sec) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (ts_sec, cur_t, sym, "v2_filtered", dec.side, dec.z, dec.p_model, dec.sigma, dec.ask_top, ages["cl_twap"], ages["cl_spot"], ages["bn"], ages["book"]),
                )
            elif 150 <= elapsed <= 255:
                skips_sec[dec.reason] = skips_sec.get(dec.reason, 0) + 1
        for reason, cnt in skips_sec.items():
            self.con.execute("INSERT OR REPLACE INTO signal_skips_42 VALUES (?,?,?,?,?)", (ts_sec, cur_t, "v2_filtered", reason, cnt))
        if self._trade_buf:
            self.con.executemany("INSERT INTO pm_trades_42(recv_ts_ms, window_ts, symbol, side, token_id, price, size) VALUES (?,?,?,?,?,?,?)", self._trade_buf)
            self._trade_buf.clear()
        if ts_sec % 5 == 0:
            self.con.commit()

    def _upsert_window_row(self, cur_t: int, elapsed: int, sym: str, st: CoinState42, tw_s: Optional[float], sp_s: Optional[float]) -> None:
        same_w = st.window_ts == cur_t
        slug_val = st.slug if (same_w and st.slug) else f"{COIN_MAP[sym]}-updown-5m-{cur_t}"
        cid_val = st.condition_id if same_w else ""
        up_tok = st.up_token if same_w else ""
        dn_tok = st.down_token if same_w else ""
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
            (cur_t, sym, slug_val, cid_val, up_tok, dn_tok, tw_s, sp_s),
        )
        if elapsed >= 298 or elapsed <= 2:
            prev_t = cur_t if elapsed >= 298 else (cur_t - 300)
            tw_c = self.chainlink.get_settlement_twap_at(sym, prev_t + 300)
            if tw_c is not None:
                self.con.execute("UPDATE windows_42 SET twap_close = COALESCE(twap_close, ?) WHERE window_ts=? AND symbol=?", (tw_c, prev_t, sym))

    def _insert_snapshot_row(self, ts_sec: int, cur_t: int, elapsed: int, sym: str, st: CoinState42, feed_vals: Tuple[Any, ...]) -> None:
        cl_spot, cl_twap, tw_age, sp_age, bn_age, up_age, dn_age, ms_metrics, bn_flush = feed_vals
        u_b1 = st.up_bids[0] if st.up_bids else (None, None)
        u_a1 = st.up_asks[0] if st.up_asks else (None, None)
        d_b1 = st.dn_bids[0] if st.dn_bids else (None, None)
        d_a1 = st.dn_asks[0] if st.dn_asks else (None, None)
        b_vol, b_buy, b_sell, b_n = bn_flush
        self.con.execute(
            """
            INSERT OR REPLACE INTO snapshots_1hz (
                ts_sec, window_ts, symbol, elapsed_sec, cl_spot, cl_twap60, bn_close,
                bn_vol, bn_buy_vol, bn_sell_vol, bn_trades,
                up_bids_json, up_asks_json, down_bids_json, down_asks_json,
                up_bid1, up_bid1_sz, up_ask1, up_ask1_sz, down_bid1, down_bid1_sz, down_ask1, down_ask1_sz,
                cl_twap_age_sec, cl_spot_age_sec, bn_age_sec, up_book_age_sec, down_book_age_sec,
                cl_twap_age_ms, cl_spot_age_ms, bn_age_ms, up_book_age_ms, down_book_age_ms,
                cl_twap_lag_ms, cl_spot_lag_ms, bn_lag_ms, up_book_lag_ms, down_book_lag_ms
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                ts_sec, cur_t, sym, elapsed, cl_spot, cl_twap, st.bn_last_px,
                b_vol, b_buy, b_sell, b_n,
                json.dumps(st.up_bids), json.dumps(st.up_asks), json.dumps(st.dn_bids), json.dumps(st.dn_asks),
                u_b1[0], u_b1[1], u_a1[0], u_a1[1], d_b1[0], d_b1[1], d_a1[0], d_a1[1],
                round(tw_age, 3), round(sp_age, 3), round(bn_age, 3), round(up_age, 3), round(dn_age, 3),
                *ms_metrics,
            ),
        )

    def _eval_live_symbol(self, ts_sec: int, cur_t: int, sym: str, st: CoinState42, vals: Tuple[Any, ...]) -> Tuple[SignalDecision, Dict[str, float]]:
        cl_spot, cl_twap, tw_age, sp_age, bn_age, up_age, dn_age, tw_s, bn_flush = vals
        _, b_buy, b_sell, b_n = bn_flush
        tr = self.micro[sym]
        px = float(st.bn_last_px) if st.bn_last_px else None
        has_tr = bool(b_n > 0 and px is not None)
        buy_u = (b_buy * px) if has_tr else 0.0
        sell_u = (b_sell * px) if has_tr else 0.0
        feed_alive = bool(px is not None and bn_age <= 5.0)
        tr.ingest_bn_1s(ts_sec, px if feed_alive else None, buy_u, sell_u, real_trade=has_tr, feed_alive=feed_alive)
        gap = tr.update_basis_1s(ts_sec, cl_spot, tr.prices[-1] if tr.prices else px)
        tr.record_cl_240(cur_t, ts_sec, cl_spot)
        cvd, flow = tr.get_orderflow_metrics(ts_sec)
        strike = self.strikes.get((cur_t, sym))
        if strike is None and tw_s is not None:
            strike = float(tw_s)
            self.strikes[(cur_t, sym)] = strike
        bk_age = directional_book_age(cl_spot, strike, up_age, dn_age)
        ages = {"cl_twap": round(tw_age, 3), "cl_spot": round(sp_age, 3), "bn": round(bn_age, 3), "book": round(bk_age, 3)}
        r240 = tr.get_realized_twap_240(cur_t, ts_sec, fallback_spot=cl_spot)
        inp = MarketInputs(sym, cur_t, float(ts_sec), strike, cl_twap, cl_spot, tr.prices[-1] if tr.prices else px, tuple(tr.prices), st.up_asks, st.dn_asks, st.up_bids, st.dn_bids, ages, flow, cvd, gap, r240)
        return evaluate_signal(inp, TWAP_V2_FILTERED_CONFIG), ages

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
                    await asyncio.sleep(max(0.05, float(int(t0)) + 1.05 - time.time()))
            finally:
                self._stop.set()
                for t in tasks:
                    t.cancel()
                self.chainlink.stop()
                self.con.commit()


def _parse_levels_json(raw: Any, reverse: bool = False) -> List[Tuple[float, float]]:
    if not raw or not isinstance(raw, str):
        return []
    try:
        pts = [(float(px), float(sz)) for px, sz in json.loads(raw) if float(sz) > 0.0 and 0.0 < float(px) < 1.0]
    except (ValueError, TypeError):
        return []
    return sorted(pts, key=lambda t: t[0], reverse=reverse)


@dataclass
class RecorderStreamState:
    """Sequential causal state shared across offline simulator & parity replay."""

    trackers: Dict[str, Bn1sMicroTracker] = field(default_factory=lambda: {s: Bn1sMicroTracker(1800, 20) for s in SYMBOLS})
    strikes: Dict[Tuple[int, str], Optional[float]] = field(default_factory=dict)
    last_ts: Dict[str, Dict[str, int]] = field(default_factory=lambda: {s: {} for s in SYMBOLS})
    prev_vals: Dict[str, Dict[str, Any]] = field(default_factory=lambda: {s: {} for s in SYMBOLS})


def _sec_from_ms_or_sec(row: Any, ms_col: str, sec_col: str) -> float:
    s_v = float(getattr(row, sec_col, 9999.0) or 0.0)
    if s_v >= 9000.0:
        return s_v
    m_v = getattr(row, ms_col, None)
    return round(float(m_v) / 1000.0, 3) if (m_v is not None and pd.notna(m_v)) else s_v


def _row_ages(row: Any, s: str, t_sec: int, state: RecorderStreamState, strike: Optional[float], cl_spot: Optional[float]) -> Dict[str, float]:
    tw_rec = getattr(row, "cl_twap_age_sec", None)
    if tw_rec is not None and pd.notna(tw_rec):
        up_a = _sec_from_ms_or_sec(row, "up_book_age_ms", "up_book_age_sec")
        dn_a = _sec_from_ms_or_sec(row, "down_book_age_ms", "down_book_age_sec")
        bk_a = directional_book_age(cl_spot, strike, up_a, dn_a)
        return {
            "cl_twap": _sec_from_ms_or_sec(row, "cl_twap_age_ms", "cl_twap_age_sec"),
            "cl_spot": _sec_from_ms_or_sec(row, "cl_spot_age_ms", "cl_spot_age_sec"),
            "bn": _sec_from_ms_or_sec(row, "bn_age_ms", "bn_age_sec"),
            "book": bk_a,
        }
    lt, pv = state.last_ts[s], state.prev_vals[s]
    win_ts = int(getattr(row, "window_ts", 0) or (t_sec // 300) * 300)
    if pv.get("last_win_ts") != win_ts:
        pv["last_win_ts"] = win_ts
        lt.pop("up_bk", None)
        lt.pop("dn_bk", None)
    sp_v = float(row.cl_spot) if pd.notna(row.cl_spot) else None
    tw_v = float(row.cl_twap60) if pd.notna(row.cl_twap60) else None
    bn_v = float(row.bn_close) if pd.notna(row.bn_close) else None
    if sp_v is not None:
        lt["sp"] = t_sec
    if tw_v is not None:
        lt["tw"] = t_sec
    if bn_v is not None and (int(getattr(row, "bn_trades", 0) or 0) > 0 or bn_v != pv.get("bn_age_px")):
        lt["bn"], pv["bn_age_px"] = t_sec, bn_v
    if pd.notna(row.up_ask1) or pd.notna(row.up_bid1):
        lt["up_bk"] = t_sec
    if pd.notna(row.down_ask1) or pd.notna(row.down_bid1):
        lt["dn_bk"] = t_sec
    sp_a = float(t_sec - lt["sp"]) if "sp" in lt else 9999.0
    tw_a = float(t_sec - lt["tw"]) if "tw" in lt else 9999.0
    bn_a = float(t_sec - lt["bn"]) if "bn" in lt else 9999.0
    up_a = float(t_sec - lt["up_bk"]) if "up_bk" in lt else 9999.0
    dn_a = float(t_sec - lt["dn_bk"]) if "dn_bk" in lt else 9999.0
    return {"cl_twap": tw_a, "cl_spot": sp_a, "bn": bn_a, "book": directional_book_age(sp_v, strike, up_a, dn_a)}


def assemble_recorder_inputs(
    tick: Tuple[int, int, int],
    sec_maps: Dict[str, Dict[int, Any]],
    w_meta: Dict[Tuple[int, str], Any],
    state: RecorderStreamState,
) -> Dict[str, MarketInputs]:
    """Assemble identical 1Hz `MarketInputs` from recorded rows with strict causal TWAP strikes."""
    T, t_sec, elapsed = tick
    out: Dict[str, MarketInputs] = {}
    for s in SYMBOLS:
        row = sec_maps[s].get(t_sec)
        if row is None:
            continue
        wm = w_meta.get((int(T), s))
        cl_spot = float(row.cl_spot) if pd.notna(row.cl_spot) else None
        cl_twap = float(row.cl_twap60) if pd.notna(row.cl_twap60) else None
        bn_px = float(row.bn_close) if pd.notna(row.bn_close) else None
        key = (int(T), s)
        has_tw_age = pd.notna(getattr(row, "cl_twap_age_sec", None))
        tw_a_s = _sec_from_ms_or_sec(row, "cl_twap_age_ms", "cl_twap_age_sec") if has_tw_age else 0.0
        tw_lag_col = getattr(row, "cl_twap_lag_ms", None)
        tw_l_s = (float(tw_lag_col) / 1000.0) if (tw_lag_col is not None and pd.notna(tw_lag_col)) else 0.0
        on_or_after = (float(t_sec) - tw_a_s - tw_l_s) >= float(T) if has_tw_age else True
        if key not in state.strikes and 0 <= elapsed <= 4 and cl_twap is not None and cl_twap > 0.0 and on_or_after:
            state.strikes[key] = cl_twap
        elif key not in state.strikes and elapsed > 8 and wm and pd.notna(wm.twap_strike):
            state.strikes[key] = float(wm.twap_strike)
        strike = state.strikes.get(key)
        ages = _row_ages(row, s, t_sec, state, strike, cl_spot)
        tr = state.trackers[s]
        prev_bn = state.prev_vals[s].get("bn_vol_px")
        bn_upd = bool(bn_px is not None and (int(getattr(row, "bn_trades", 0) or 0) > 0 or bn_px != prev_bn))
        if bn_px is not None:
            state.prev_vals[s]["bn_vol_px"] = bn_px
        has_tr = bool(int(getattr(row, "bn_trades", 0) or 0) > 0 and bn_px is not None)
        buy_u = (float(row.bn_buy_vol or 0.0) * bn_px) if (has_tr and bn_px) else 0.0
        sell_u = (float(row.bn_sell_vol or 0.0) * bn_px) if (has_tr and bn_px) else 0.0
        feed_alive = bool(bn_px is not None and ages["bn"] <= 5.0)
        tr.ingest_bn_1s(t_sec, bn_px if (bn_upd or feed_alive) else None, buy_u, sell_u, real_trade=bn_upd, feed_alive=feed_alive)
        bn_px_eff = tr.prices[-1] if tr.prices else (bn_px if (bn_upd or feed_alive) else None)
        gap = tr.update_basis_1s(t_sec, cl_spot, bn_px_eff)
        tr.record_cl_240(int(T), t_sec, cl_spot)
        cvd, flow = tr.get_orderflow_metrics(t_sec)
        r240 = tr.get_realized_twap_240(int(T), t_sec, fallback_spot=cl_spot)
        up_asks = _parse_levels_json(getattr(row, "up_asks_json", None), reverse=False)
        dn_asks = _parse_levels_json(getattr(row, "down_asks_json", None), reverse=False)
        up_bids = _parse_levels_json(getattr(row, "up_bids_json", None), reverse=True)
        dn_bids = _parse_levels_json(getattr(row, "down_bids_json", None), reverse=True)
        out[s] = MarketInputs(s, int(T), float(t_sec), strike, cl_twap, cl_spot, bn_px_eff, tuple(tr.prices), up_asks, dn_asks, up_bids, dn_bids, ages, flow, cvd, gap, r240)
    return out


def _try_enter_window(
    tick: Tuple[int, int, int],
    inps: Dict[str, MarketInputs],
    market_ctx: Tuple[Dict[str, Dict[int, Any]], Dict[Tuple[int, str], Any]],
    cfg: SniperConfig,
    fee_map: Dict[str, float],
    eq: float,
) -> Tuple[Optional[dict[str, Any]], Optional[dict[str, Any]], Dict[str, SignalDecision]]:
    T, t_sec, elapsed = tick
    sec_maps, w_meta = market_ctx
    decs: Dict[str, SignalDecision] = {}
    for s, inp in inps.items():
        c_sym = SniperConfig(**{**asdict(cfg), "fee_rate": float(fee_map.get(s, cfg.fee_rate))})
        decs[s] = evaluate_signal(inp, c_sym, max_ages=DEFAULT_MAX_AGES, enforce_warmup=True)
    best = choose_best_candidate(list(decs.values()))
    if best is None:
        return None, None, decs
    sym, fee_r = best.symbol, float(fee_map.get(best.symbol, cfg.fee_rate))
    inp0, dec0 = inps[sym], decs[sym]
    lim = entry_limit_price(SniperConfig(**{**asdict(cfg), "fee_rate": fee_r}), best.ask, best.p_true)
    shares = compute_shares(cfg, eq, best.total_cost_per_share)
    r0, r1, r2 = sec_maps[sym].get(t_sec), sec_maps[sym].get(t_sec + 1), sec_maps[sym].get(t_sec + 2)
    asks1 = _parse_levels_json(getattr(r1, "up_asks_json" if best.side == "UP" else "down_asks_json", None), False) if r1 else []
    asks2 = _parse_levels_json(getattr(r2, "up_asks_json" if best.side == "UP" else "down_asks_json", None), False) if r2 else []
    f1 = simulate_fak_buy(asks1, float(shares), lim) if shares > 0 else simulate_fak_buy([], 0.0, lim)
    f2 = simulate_fak_buy(asks2, float(shares), lim) if shares > 0 else simulate_fak_buy([], 0.0, lim)
    wm = w_meta.get((int(T), sym))
    out = wm.resolved_outcome if wm else None
    cost1 = (float(f1.avg_px) + taker_fee_per_share(float(f1.avg_px), fee_r)) if (f1.filled and f1.avg_px) else np.nan
    cost2 = (float(f2.avg_px) + taker_fee_per_share(float(f2.avg_px), fee_r)) if (f2.filled and f2.avg_px) else np.nan
    pnl2 = round((1.0 if out == best.side else 0.0) - cost2, 5) if (f2.filled and out in ("UP", "DOWN")) else np.nan
    ask1_top = (r1.up_ask1 if best.side == "UP" else r1.down_ask1) if r1 else None
    sz1_top = (r1.up_ask1_sz if best.side == "UP" else r1.down_ask1_sz) if r1 else None
    sz0 = (r0.up_ask1_sz if best.side == "UP" else r0.down_ask1_sz) if r0 else None
    bid0 = (r0.up_bid1 if best.side == "UP" else r0.down_bid1) if r0 else None
    hold_pnl = round((1.0 if out == best.side else 0.0) - cost1, 5) if (f1.filled and out in ("UP", "DOWN")) else np.nan
    pos: dict[str, Any] = {
        "signal_ts": t_sec, "window_ts": int(T), "elapsed_sec": int(elapsed), "symbol": sym, "side": best.side,
        "regime": "post_nov21" if int(T) >= NOV21_UTC_TS else "pre_nov21",
        "fair_at_sig": round(float(best.p_true), 5), "ask_t0": round(float(best.ask), 5),
        "ask_sz_t0": sz0, "spread_t0": round(float(best.ask) - float(bid0), 5) if bid0 is not None else np.nan,
        "ask_t1": ask1_top, "ask_sz_t1": sz1_top, "avg_fill_px_1s": f1.avg_px if f1.filled else np.nan,
        "fill_status_1s": f1.status, "partial_shares_1s": f1.filled_shares, "fill_status_2s": f2.status,
        "limit_px": lim, "shares": shares, "filled_1s": f1.filled, "filled_2s": f2.filled,
        "miss_reason": "" if f1.filled else f1.status,
        "exit_reason": "HOLD_SETTLE" if f1.filled else f"MISSED_{f1.status}", "hold_sec": None if f1.filled else 0,
        "cl_twap_at_sig": inp0.cl_twap if inp0.cl_twap is not None else np.nan,
        "bn_px_at_sig": inp0.bn_close_1s if inp0.bn_close_1s is not None else np.nan,
        "cl_twap_age_sec": inp0.ages["cl_twap"], "cl_spot_age_sec": inp0.ages["cl_spot"],
        "bn_age_sec": inp0.ages["bn"], "book_age_sec": inp0.ages["book"],
        "resolved_outcome": out, "would_have_won": (out == best.side) if out in ("UP", "DOWN") else None,
        "entry_p_true": best.p_true, "entry_z": best.z, "sigma_used": dec0.sigma, "fee_rate": fee_r,
        "cost_1s": cost1, "cost_2s": cost2, "exit_ts": None, "exit_bid_1s": None, "exit_sz_1s": None,
        "pnl_per_share": np.nan, "pnl_with_sl": np.nan, "pnl_hold_to_settle": hold_pnl,
        "pnl_per_share_2s": pnl2, "pnl_usd": np.nan, "sl_triggered": False, "sl_was_wrong": False, "z_twap_proj": np.nan,
    }
    return (pos, None, decs) if f1.filled else (None, pos, decs)


def _try_exit_pos(
    open_pos: dict[str, Any], tick: Tuple[int, int, int], dec: Optional[SignalDecision],
    inps: Dict[str, MarketInputs], sec_maps: Dict[str, Dict[int, Any]], cfg: SniperConfig,
) -> Optional[dict[str, Any]]:
    T, t_sec, elapsed = tick
    sym = open_pos["symbol"]
    if sym not in inps:
        return None
    inp = inps[sym]
    c_sym = SniperConfig(**{**asdict(cfg), "fee_rate": open_pos["fee_rate"]})
    fv = dec.fv if dec is not None else None
    sig = dec.sigma if dec is not None else None
    if fv is None and float(inp.ages.get("cl_spot", 9999.0)) <= DEFAULT_MAX_AGES["cl_spot"] and float(inp.ages.get("book", 9999.0)) <= DEFAULT_MAX_AGES["book"]:
        res = compute_fv(inp, c_sym)
        if res is not None:
            sig, fv = res
    if fv is None:
        return None
    z_tp = compute_z_twap_proj_diagnostic(inp.strike_twap or 0.0, inp.cl_spot or 0.0, float(300 - elapsed), sig or 5e-5, inp.realized_twap_240, open_pos["side"])
    if z_tp is not None:
        open_pos["z_twap_proj"] = round(z_tp, 4)
    from engine.oracle_sniper import evaluate_exit
    bids0 = inp.up_bids if open_pos["side"] == "UP" else inp.down_bids
    asks0 = inp.up_asks if open_pos["side"] == "UP" else inp.down_asks
    bk0 = OrderBookTop(up_bid=bids0[0][0] if (open_pos["side"] == "UP" and bids0) else None, up_ask=asks0[0][0] if (open_pos["side"] == "UP" and asks0) else None,
                       down_bid=bids0[0][0] if (open_pos["side"] == "DOWN" and bids0) else None, down_ask=asks0[0][0] if (open_pos["side"] == "DOWN" and asks0) else None)
    op_obj = OpenPosition(sym, open_pos["side"], int(T), open_pos["shares"], float(open_pos["avg_fill_px_1s"]), float(open_pos["cost_1s"]), float(open_pos["entry_p_true"]), float(open_pos["entry_z"]))
    ex = evaluate_exit(c_sym, op_obj, float(elapsed), fv, bk0)
    if ex is None:
        return None
    r1 = sec_maps[sym].get(t_sec + 1)
    bids1 = _parse_levels_json(getattr(r1, "up_bids_json" if open_pos["side"] == "UP" else "down_bids_json", None), True) if r1 else []
    min_bid = max(0.01, round(ex.bid - (0.05 if ex.reason == "EMERGENCY_STOP_LOSS" else 0.03), 2))
    sell = simulate_fak_sell(bids1, float(open_pos["shares"]), min_bid)
    if not sell.filled or sell.avg_px is None:
        return None
    proc = effective_exit_proceeds(float(sell.avg_px), open_pos["fee_rate"])
    pnl1 = round(proc - open_pos["cost_1s"], 5)
    pnl2 = round(proc - float(open_pos["cost_2s"]), 5) if (open_pos["filled_2s"] and pd.notna(open_pos["cost_2s"])) else open_pos["pnl_per_share_2s"]
    sl_trig = ex.reason == "EMERGENCY_STOP_LOSS"
    open_pos.update({
        "exit_reason": ex.reason, "exit_ts": t_sec + 1, "hold_sec": (t_sec + 1) - int(open_pos["signal_ts"]),
        "exit_bid_1s": sell.avg_px, "exit_sz_1s": sell.filled_shares, "pnl_per_share": pnl1, "pnl_with_sl": pnl1,
        "pnl_per_share_2s": pnl2, "pnl_usd": round(pnl1 * open_pos["shares"], 4),
        "sl_triggered": sl_trig, "sl_was_wrong": bool(sl_trig and open_pos["resolved_outcome"] == open_pos["side"]),
    })
    return open_pos


def _cfg_from_dict(raw: dict[str, Any]) -> SniperConfig:
    return SniperConfig(**{k: (tuple(v) if k == "slippage_ladder" else v) for k, v in raw.items()})


def _sim_window(
    T: int,
    cfg: SniperConfig,
    fee_map: Dict[str, float],
    market_ctx: Tuple[Dict[str, Dict[int, Any]], Dict[Tuple[int, str], Any]],
    diag_ctx: Tuple[RecorderStreamState, Dict[str, int], Dict[str, List[float]], List[dict[str, Any]], set[int]],
) -> None:
    sec_maps, w_meta = market_ctx
    stream_state, skip_counts, age_samples, trades_1s, warmup_secs = diag_ctx
    open_pos: Optional[dict[str, Any]] = None
    entered = False
    for t_sec in range(int(T), int(T) + 300):
        tick = (int(T), t_sec, t_sec - int(T))
        inps = assemble_recorder_inputs(tick, sec_maps, w_meta, stream_state)
        if any(len(inp.vol_hist) < MIN_VOL_SAMPLES for inp in inps.values()):
            warmup_secs.add(t_sec)
        if open_pos is None and not entered:
            open_pos, unfilled, decs = _try_enter_window(tick, inps, (sec_maps, w_meta), cfg, fee_map, 100.0)
            entered = open_pos is not None or unfilled is not None
            _collect_diag_tick(tick[2], inps, decs, skip_counts, age_samples)
            if unfilled is not None:
                trades_1s.append(unfilled)
            continue
        if open_pos is None or open_pos["symbol"] not in inps:
            continue
        sym_o = open_pos["symbol"]
        c_sym = SniperConfig(**{**asdict(cfg), "fee_rate": open_pos["fee_rate"]})
        dec_o = evaluate_signal(inps[sym_o], c_sym, max_ages=DEFAULT_MAX_AGES, enforce_warmup=False)
        done = _try_exit_pos(open_pos, tick, dec_o, inps, sec_maps, cfg)
        if done is not None:
            trades_1s.append(done)
            open_pos = None
    if open_pos is not None:
        hold_p = open_pos["pnl_hold_to_settle"]
        open_pos.update({"hold_sec": (int(T) + 300) - int(open_pos["signal_ts"]), "pnl_per_share": hold_p, "pnl_with_sl": hold_p, "pnl_usd": round(hold_p * open_pos["shares"], 4) if pd.notna(hold_p) else np.nan})
        trades_1s.append(open_pos)


def _sim_one_cfg(
    cfg: SniperConfig, fee_map: Dict[str, float], settled_ts: List[int],
    snap_by_wt_sym: Dict[Any, pd.DataFrame], w_meta: Dict[Tuple[int, str], Any],
) -> Tuple[pd.DataFrame, Dict[str, int], Dict[str, List[float]], set[int]]:
    stream_state = RecorderStreamState()
    trades_1s: List[dict[str, Any]] = []
    skip_counts: Dict[str, int] = {}
    age_samples: Dict[str, List[float]] = {"cl_twap": [], "cl_spot": [], "bn": [], "book": []}
    warmup_secs: set[int] = set()
    for T in settled_ts:
        sec_maps = {s: ({int(r.ts_sec): r for r in g.itertuples(index=False)} if (g := snap_by_wt_sym.get((int(T), s))) is not None else {}) for s in SYMBOLS}
        _sim_window(int(T), cfg, fee_map, (sec_maps, w_meta), (stream_state, skip_counts, age_samples, trades_1s, warmup_secs))
    return pd.DataFrame(trades_1s), skip_counts, age_samples, warmup_secs


def _collect_diag_tick(
    elapsed: int, inps: Dict[str, MarketInputs], decs: Dict[str, SignalDecision],
    skip_counts: Dict[str, int], age_samples: Dict[str, List[float]],
) -> None:
    if not 150 <= elapsed <= 255:
        return
    for s, inp in inps.items():
        for k in age_samples:
            v = float(inp.ages.get(k, 9999.0))
            if v < 9000.0:
                age_samples[k].append(v)
        d = decs.get(s)
        if d is not None and d.action == "SKIP":
            skip_counts[d.reason] = skip_counts.get(d.reason, 0) + 1


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
        return {"signals": 0, "filled_1s": 0, "missed_1s": 0, "miss_rate": None, "mean_pnl_1s": None, "mean_pnl_hold": None, "filled_2s": 0, "gate_ready": False}
    f1 = df_t[df_t["filled_1s"] == True]
    f2 = df_t[df_t["filled_2s"] == True]
    m1 = df_t[df_t["filled_1s"] != True]
    c95, c98 = _bootstrap_cis(f1["pnl_per_share"].to_numpy(dtype=float)) if len(f1) else (None, None)
    pos_coins = int(sum(float(g["pnl_per_share"].mean()) > 0 for _, g in f1.groupby("symbol"))) if len(f1) else 0
    mw, fw = m1["would_have_won"].dropna(), f1["would_have_won"].dropna()
    miss_cnt = int(len(m1))
    md_cnt = int((m1["miss_reason"] == "MISSED_DEPTH").sum()) if miss_cnt else 0
    mp_cnt = int((m1["miss_reason"] == "MISSED_PRICE").sum()) if miss_cnt else 0
    sl_trig = int((f1["sl_triggered"] == True).sum()) if ("sl_triggered" in f1 and len(f1)) else 0
    sl_wrong = int((f1["sl_was_wrong"] == True).sum()) if ("sl_was_wrong" in f1 and len(f1)) else 0
    return {
        "signals": int(len(df_t)), "filled_1s": int(len(f1)), "missed_1s": miss_cnt,
        "missed_depth": md_cnt, "missed_price": mp_cnt,
        "missed_depth_pct": round(md_cnt / miss_cnt * 100.0, 1) if miss_cnt else 0.0,
        "missed_price_pct": round(mp_cnt / miss_cnt * 100.0, 1) if miss_cnt else 0.0,
        "miss_rate": round(float(miss_cnt / len(df_t)), 4), "filled_2s": int(len(f2)),
        "positive_coins_1s": pos_coins, "wr_1s": round(float((f1["pnl_per_share"] > 0).mean()), 4) if len(f1) else None,
        "missed_would_win_rate": round(float(mw.astype(float).mean()), 4) if len(mw) else None,
        "filled_would_win_rate": round(float(fw.astype(float).mean()), 4) if len(fw) else None,
        "mean_spread_t0": round(float(df_t["spread_t0"].dropna().mean()), 5) if df_t["spread_t0"].notna().any() else None,
        "mean_pnl_1s": round(float(f1["pnl_per_share"].mean()), 5) if len(f1) else None,
        "mean_pnl_hold": round(float(f1["pnl_hold_to_settle"].dropna().mean()), 5) if ("pnl_hold_to_settle" in f1 and f1["pnl_hold_to_settle"].notna().any()) else None,
        "sl_triggered": sl_trig, "sl_was_wrong": sl_wrong,
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


def _pct_ms(vals: List[float], disc: int = 0) -> Dict[str, Any]:
    if not vals:
        return {"n": 0, "p50": None, "p90": None, "p99": None, "max": None, "disconnected": disc}
    arr = np.asarray(vals, dtype=float)
    p50, p90, p99 = np.percentile(arr, [50, 90, 99])
    return {
        "n": len(vals), "p50": round(float(p50), 2), "p90": round(float(p90), 2),
        "p99": round(float(p99), 2), "max": round(float(np.max(arr)), 2), "disconnected": disc,
    }


def _compute_ms_freshness(df_s: pd.DataFrame, count_start_ts: int, fallback_ages: Dict[str, List[float]]) -> Dict[str, Any]:
    win = df_s[(df_s["elapsed_sec"] >= 150) & (df_s["elapsed_sec"] <= 255)] if len(df_s) else df_s
    has_ms = win[win["cl_spot_age_ms"].notna() | win["bn_age_ms"].notna()] if ("cl_spot_age_ms" in win.columns and len(win)) else win.iloc[0:0]
    post = has_ms[has_ms["window_ts"] >= count_start_ts] if (len(has_ms) and count_start_ts > 0) else has_ms
    sub = post if len(post) else has_ms
    if len(sub) == 0:
        return {
            "scope": "fallback_sec_to_ms",
            "age_ms": {k: _pct_ms([round(v * 1000.0, 2) for v in vs]) for k, vs in fallback_ages.items()},
            "lag_ms": {k: _pct_ms([]) for k in ("cl_twap", "cl_spot", "bn", "book")},
        }
    ages_m: Dict[str, List[float]] = {"cl_twap": [], "cl_spot": [], "bn": [], "book": []}
    lags_m: Dict[str, List[float]] = {"cl_twap": [], "cl_spot": [], "bn": [], "book": []}
    disc: Dict[str, int] = {"cl_twap": 0, "cl_spot": 0, "bn": 0, "book": 0}
    for r in sub.itertuples(index=False):
        _collect_row_ms(r, ages_m, lags_m, disc)
    scope = "post_reset_ms" if len(post) else "all_ms"
    return {
        "scope": scope,
        "age_ms": {k: _pct_ms(ages_m[k], disc[k]) for k in ages_m},
        "lag_ms": {k: _pct_ms(lags_m[k]) for k in lags_m},
    }


def _collect_row_ms(r: Any, ages_m: Dict[str, List[float]], lags_m: Dict[str, List[float]], disc: Dict[str, int]) -> None:
    tw_s = float(getattr(r, "cl_twap_age_sec", 9999.0) or 0.0)
    sp_s = float(getattr(r, "cl_spot_age_sec", 9999.0) or 0.0)
    bn_s = float(getattr(r, "bn_age_sec", 9999.0) or 0.0)
    use_up = not (pd.notna(r.cl_spot) and pd.notna(getattr(r, "cl_twap60", np.nan)) and float(r.cl_spot) < float(r.cl_twap60))
    bk_age_col = "up_book_age_ms" if use_up else "down_book_age_ms"
    bk_lag_col = "up_book_lag_ms" if use_up else "down_book_lag_ms"
    bk_sec_col = "up_book_age_sec" if use_up else "down_book_age_sec"
    bk_s = float(getattr(r, bk_sec_col, 9999.0) or 0.0)
    pairs = (
        ("cl_twap", tw_s, getattr(r, "cl_twap_age_ms", None), getattr(r, "cl_twap_lag_ms", None)),
        ("cl_spot", sp_s, getattr(r, "cl_spot_age_ms", None), getattr(r, "cl_spot_lag_ms", None)),
        ("bn", bn_s, getattr(r, "bn_age_ms", None), getattr(r, "bn_lag_ms", None)),
        ("book", bk_s, getattr(r, bk_age_col, None), getattr(r, bk_lag_col, None)),
    )
    for k, s_val, a_ms, l_ms in pairs:
        if s_val >= 9000.0 or a_ms is None or pd.isna(a_ms):
            disc[k] += 1
        else:
            ages_m[k].append(float(a_ms))
        if l_ms is not None and pd.notna(l_ms):
            lags_m[k].append(float(l_ms))


def _compute_r1_diagnostics(df_w: pd.DataFrame, count_start_ts: int) -> Dict[str, Any]:
    settled = df_w[df_w["resolved_outcome"].isin(["UP", "DOWN"])]
    has_tw = settled[settled["twap_strike"].notna() & settled["twap_close"].notna()]
    mismatches: List[Dict[str, Any]] = []
    matched = 0
    for r in has_tw.itertuples(index=False):
        pred = "UP" if float(r.twap_close) >= float(r.twap_strike) else "DOWN"
        if pred == r.resolved_outcome:
            matched += 1
        else:
            mismatches.append({
                "window_ts": int(r.window_ts), "symbol": str(r.symbol), "slug": str(r.slug),
                "twap_strike": float(r.twap_strike), "twap_close": float(r.twap_close),
                "gamma_price_to_beat": float(r.gamma_price_to_beat) if pd.notna(r.gamma_price_to_beat) else None,
                "gamma_final_price": float(r.gamma_final_price) if pd.notna(r.gamma_final_price) else None,
                "resolved_outcome": str(r.resolved_outcome), "pred": pred,
            })
    post = has_tw[has_tw["window_ts"] >= count_start_ts] if (len(has_tw) and count_start_ts > 0) else has_tw.iloc[0:0]
    post_m = int(((post["twap_close"] >= post["twap_strike"]).map({True: "UP", False: "DOWN"}) == post["resolved_outcome"]).sum()) if len(post) else 0
    return {
        "parity_all": round(matched / len(has_tw), 4) if len(has_tw) else None,
        "matched_all": matched, "total_all": int(len(has_tw)),
        "parity_post_reset": round(post_m / len(post), 4) if len(post) else None,
        "matched_post_reset": post_m, "total_post_reset": int(len(post)),
        "mismatches": mismatches,
    }


def _count_warmup_episodes(secs: set[int]) -> Dict[str, Any]:
    ordered = sorted(secs)
    restarts = 0
    prev: Optional[int] = None
    for t in ordered:
        if prev is None or (t - prev) > 10:
            restarts += 1
        prev = t
    return {"seconds": len(ordered), "hours": round(len(ordered) / 3600.0, 2), "restarts": restarts}


def _compute_live_shadow_parity(con: sqlite3.Connection, shadow_path: pathlib.Path, count_start_ts: int) -> Dict[str, Any]:
    rows_e = con.execute("SELECT ts_sec, window_ts, symbol, side, z, ask_top FROM signal_enters_42 WHERE model_name='v2_filtered' AND window_ts >= ?", (count_start_ts,)).fetchall()
    rec_map: Dict[Tuple[int, str], Tuple[str, int, float, float]] = {}
    for ts_s, wt, sym, side, z_v, ask_v in rows_e:
        rec_map.setdefault((int(wt), str(sym)), (str(side), int(ts_s), float(z_v or 0.0), float(ask_v or 0.0)))
    run_warm: set[Tuple[int, str]] = set()
    run_map: Dict[Tuple[int, str], Tuple[str, int, float, float]] = {}
    if shadow_path.exists():
        for line in shadow_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            ev = json.loads(line)
            wt = int(ev.get("window_ts") or 0)
            sym = str(ev.get("symbol") or "")
            if wt < count_start_ts or not sym:
                continue
            if ev.get("type") == "WARM_WINDOW":
                run_warm.add((wt, sym))
            elif ev.get("type") == "ENTER":
                run_warm.add((wt, sym))
                run_map.setdefault((wt, sym), (str(ev["side"]), int(ev["ts_sec"]), float(ev.get("z") or 0.0), float(ev.get("ask_top") or 0.0)))
    cov_rows = con.execute("SELECT window_ts, symbol FROM snapshots_1hz WHERE window_ts >= ? AND elapsed_sec BETWEEN 150 AND 255 GROUP BY window_ts, symbol HAVING COUNT(*) >= 60", (count_start_ts,)).fetchall()
    overlap = sorted({(int(wt), str(sym)) for wt, sym in cov_rows} & (run_warm | set(rec_map)))
    return _aggregate_shadow_parity(overlap, rec_map, run_map)


def _aggregate_shadow_parity(
    overlap: List[Tuple[int, str]],
    rec_map: Dict[Tuple[int, str], Tuple[str, int, float, float]],
    run_map: Dict[Tuple[int, str], Tuple[str, int, float, float]],
) -> Dict[str, Any]:
    by_day: Dict[str, Dict[str, Any]] = {}
    mismatches: List[Dict[str, Any]] = []
    tot_u = tot_m = 0
    for wt, sym in overlap:
        day = datetime.fromtimestamp(wt, timezone.utc).strftime("%Y-%m-%d")
        d = by_day.setdefault(day, {"utc_day": day, "overlap_coin_windows": 0, "recorder_enters": 0, "runner_enters": 0, "union": 0, "matched": 0})
        d["overlap_coin_windows"] += 1
        ra, rb = rec_map.get((wt, sym)), run_map.get((wt, sym))
        if ra is not None:
            d["recorder_enters"] += 1
        if rb is not None:
            d["runner_enters"] += 1
        if ra is None and rb is None:
            continue
        d["union"] += 1
        tot_u += 1
        ok = bool(ra and rb and ra[0] == rb[0] and abs(ra[1] - rb[1]) <= 1 and abs(ra[2] - rb[2]) <= 0.02 and abs(ra[3] - rb[3]) <= 0.001)
        if ok:
            d["matched"] += 1
            tot_m += 1
        elif len(mismatches) < 20:
            mismatches.append({"window_ts": wt, "symbol": sym, "recorder": ra, "runner": rb})
    for d in by_day.values():
        d["parity_rate"] = round(d["matched"] / d["union"], 4) if d["union"] else None
    overall_rate = round(tot_m / tot_u, 4) if tot_u else None
    return {
        "by_day": [by_day[k] for k in sorted(by_day)],
        "overlap_coin_windows": len(overlap), "union_enters": tot_u, "matched": tot_m,
        "parity_rate": overall_rate, "gate_pass": (overall_rate >= 0.99) if overall_rate is not None else None,
        "mismatches": mismatches,
    }


def _append_diagnostics_sections(lines: List[str], report: Dict[str, Any]) -> None:
    fm = report.get("freshness_ms") or {}
    lines += ["", f"## Feed Freshness & Upstream Lag in Milliseconds (`v2_filtered` Window 150–255s, scope=`{fm.get('scope', 'ms')}`)", "", "| Feed | Metric | Samples (Disc) | P50 (ms) | P90 (ms) | P99 (ms) | Max (ms) |", "|---|---|---:|---:|---:|---:|---:|"]
    for metric_lbl, bucket in (("age (`mono_ns`)", fm.get("age_ms") or {}), ("lag (`recv_wall - src`)", fm.get("lag_ms") or {})):
        for k, st in bucket.items():
            lines.append(f"| `{k}` | {metric_lbl} | {st['n']} ({st.get('disconnected', 0)}) | {st['p50']} | {st['p90']} | {st['p99']} | {st['max']} |")
    w_all = (report.get("warmup_diag") or {}).get("all") or {}
    w_post = (report.get("warmup_diag") or {}).get("post_reset") or {}
    lines += [
        "", "## Warmup & Restart Uptime", "",
        f"- All recorded history: `{w_all.get('seconds', 0)}s` (**`{w_all.get('hours', 0.0)}h`**) warmup across `{w_all.get('restarts', 0)}` warmup/restart episodes.",
        f"- Post-reset (`>= {report.get('count_start_window_ts', 0)}`): `{w_post.get('seconds', 0)}s` (**`{w_post.get('hours', 0.0)}h`**) warmup across `{w_post.get('restarts', 0)}` episodes.",
        "", "## Signal Skip Breakdown (`v2_filtered` Window 150–255s)", "", "| Reason | Count |", "|---|---:|",
    ]
    for reason, cnt in sorted((report.get("skip_counts") or {}).items(), key=lambda kv: -kv[1]):
        lines.append(f"| `{reason}` | {cnt} |")
    _append_r1_and_shadow_tables(lines, report)


def _summarize_twap_age_buckets(df_t: pd.DataFrame) -> List[Dict[str, Any]]:
    """Group +1s filled trades into <0.5s, 0.5-1.5s, >1.5s Chainlink TWAP age buckets."""
    f1 = df_t[df_t["filled_1s"] == True] if len(df_t) else df_t
    ages = pd.to_numeric(f1["cl_twap_age_sec"], errors="coerce") if len(f1) else pd.Series(dtype=float)
    specs = (("< 0.5s", ages < 0.5), ("0.5–1.5s", (ages >= 0.5) & (ages <= 1.5)), ("> 1.5s", ages > 1.5))
    out: List[Dict[str, Any]] = []
    for label, mask in specs:
        sub = f1[mask] if len(f1) else f1
        n = int(len(sub))
        wr = round(float((sub["pnl_per_share"] > 0).mean()), 4) if n else None
        p_sl = round(float(sub["pnl_per_share"].mean()), 5) if n else None
        has_hold = n > 0 and "pnl_hold_to_settle" in sub and sub["pnl_hold_to_settle"].notna().any()
        p_hold = round(float(sub["pnl_hold_to_settle"].dropna().mean()), 5) if has_hold else None
        out.append({"bucket": label, "trades": n, "wr": wr, "mean_pnl_1s": p_sl, "mean_pnl_hold": p_hold})
    return out


def _carry_symbol_metrics(rows: pd.DataFrame) -> Tuple[int, int, int, int]:
    rows_s = rows.sort_values("ts_sec")
    tot = zero_c = gt5 = streak = max_s = 0
    for tr_raw, age_raw in zip(rows_s["bn_trades"], rows_s["bn_age_sec"]):
        tot += 1
        n_tr = int(tr_raw) if pd.notna(tr_raw) else 0
        age_v = float(age_raw) if pd.notna(age_raw) else 9999.0
        if n_tr == 0 and age_v <= 5.0:
            zero_c += 1
            streak += 1
            max_s = max(max_s, streak)
            if streak > 5:
                gt5 += 1
        else:
            streak = 0
    return tot, zero_c, gt5, max_s


def _compute_carry_forward_stats(df_s: pd.DataFrame, count_start_ts: int) -> Dict[str, Any]:
    """Measure Binance zero-trade carry-forward ratio (total and >5s extended) in vol_hist."""
    if len(df_s) == 0 or "bn_age_sec" not in df_s.columns:
        return {"scope": "none", "rows": []}
    has_bn = df_s[df_s["bn_age_sec"].notna()]
    post = has_bn[has_bn["window_ts"] >= count_start_ts] if (len(has_bn) and count_start_ts > 0) else has_bn
    sub = post if len(post) else has_bn
    scope = "post_reset" if len(post) else "all_ms"
    out_rows: List[Dict[str, Any]] = []
    sum_tot = sum_zero = sum_gt5 = all_max = 0
    for sym in SYMBOLS:
        g = sub[sub["symbol"] == sym]
        tot, zero_c, gt5, max_s = _carry_symbol_metrics(g) if len(g) else (0, 0, 0, 0)
        sum_tot, sum_zero, sum_gt5, all_max = sum_tot + tot, sum_zero + zero_c, sum_gt5 + gt5, max(all_max, max_s)
        z_pct = round(zero_c / tot * 100.0, 2) if tot else 0.0
        out_rows.append({"symbol": sym, "total_sec": tot, "zero_trade_sec": zero_c, "zero_trade_pct": z_pct, "gt5s_sec": gt5, "gt5s_pct": round(gt5 / tot * 100.0, 2) if tot else 0.0, "max_quiet_sec": max_s, "status": "ALERT (>5%)" if (tot > 0 and z_pct > 5.0) else "OK"})
    all_pct = round(sum_zero / sum_tot * 100.0, 2) if sum_tot else 0.0
    out_rows.append({"symbol": "ALL", "total_sec": sum_tot, "zero_trade_sec": sum_zero, "zero_trade_pct": all_pct, "gt5s_sec": sum_gt5, "gt5s_pct": round(sum_gt5 / sum_tot * 100.0, 2) if sum_tot else 0.0, "max_quiet_sec": all_max, "status": "ALERT (>5%)" if (sum_tot > 0 and all_pct > 5.0) else "OK"})
    return {"scope": scope, "rows": out_rows}


def _append_twap_age_and_carry_tables(lines: List[str], report: Dict[str, Any]) -> None:
    ta = report.get("twap_age_pnl") or {}
    lines += ["", "## Chainlink TWAP Age vs Filled PnL (`+1s`, `v2_filtered`)", "", "| Scope | `cl_twap_age_sec` | Filled Trades | Win Rate | Mean PnL +1s | Mean Hold PnL |", "|---|---|---:|---:|---:|---:|"]
    for sc_lbl in ("post_reset", "all"):
        for b in ta.get(sc_lbl) or []:
            lines.append(f"| `{sc_lbl}` | `{b['bucket']}` | {b['trades']} | {b['wr']} | {b['mean_pnl_1s']} | {b['mean_pnl_hold']} |")
    cf = report.get("carry_forward") or {}
    lines += ["", f"## Binance 1s Volatility Carry-Forward Ratio (`feed_alive` Monitor, scope=`{cf.get('scope', 'none')}`)", "", "_Note: `compute_sigma_from_vol_hist` takes `max(sample_var, ewma_var)`; multi-second cumulative variance stays unbiased under zero-return carry-forward while `sample_var` floor prevents quiet-regime EWMA under-estimation._", "", "| Symbol | Total Sec | Zero-Trade Carry (`bn_trades=0`) | Extended `>5s` Carry | Max Quiet Streak | Status (`<=5%`) |", "|---|---:|---:|---:|---:|---|"]
    for r in cf.get("rows") or []:
        lines.append(f"| `{r['symbol']}` | {r['total_sec']} | {r['zero_trade_sec']} (**{r['zero_trade_pct']}%**) | {r['gt5s_sec']} ({r['gt5s_pct']}%) | {r['max_quiet_sec']}s | **{r['status']}** |")


def _append_r1_and_shadow_tables(lines: List[str], report: Dict[str, Any]) -> None:
    lines += ["", "## Depth vs Price Miss Ratio (`+1s` FAK) & Dual-Track Stop-Loss vs Hold-to-Settle", "", "| Scope / Model | Missed Total | `MISSED_DEPTH` | `MISSED_PRICE` | `pnl_with_sl` (Mean/sh) | `pnl_hold_to_settle` (Mean/sh) | SL Triggered (`wrong`) |", "|---|---:|---:|---:|---:|---:|---:|"]
    for lbl, st in (report.get("all_combined") or {}).items():
        lines.append(
            f"| `all / {lbl}` | {st.get('missed_1s', 0)} | {st.get('missed_depth', 0)} ({st.get('missed_depth_pct', 0.0)}%) | "
            f"{st.get('missed_price', 0)} ({st.get('missed_price_pct', 0.0)}%) | {st.get('mean_pnl_1s')} | "
            f"{st.get('mean_pnl_hold')} | {st.get('sl_triggered', 0)} ({st.get('sl_was_wrong', 0)}) |"
        )
    _append_twap_age_and_carry_tables(lines, report)
    sh = report.get("live_shadow_parity") or {}
    lines += ["", f"## Live Shadow Parity (`PAPER_40U` Runner vs 4.2 Recorder, overlap={sh.get('overlap_coin_windows', 0)}, overall={sh.get('parity_rate')})", "", "| UTC Day | Overlap Coin-Win | Recorder ENTER | Runner ENTER | Union | Matched | Parity | Gate (`>=99%`) |", "|---|---:|---:|---:|---:|---:|---:|---|"]
    for d in sh.get("by_day") or []:
        st_lbl = "PASS" if (d["parity_rate"] is not None and d["parity_rate"] >= 0.99) else ("FAIL" if d["parity_rate"] is not None else "PENDING")
        lines.append(f"| `{d['utc_day']}` | {d['overlap_coin_windows']} | {d['recorder_enters']} | {d['runner_enters']} | {d['union']} | {d['matched']} | {d['parity_rate']} | **{st_lbl}** |")
    r1 = report.get("r1_diagnostics") or {}
    lines += ["", f"## R1 TWAP Parity Mismatches (All: {r1.get('matched_all', 0)}/{r1.get('total_all', 0)}={r1.get('parity_all')} | Post-reset: {r1.get('matched_post_reset', 0)}/{r1.get('total_post_reset', 0)}={r1.get('parity_post_reset')})", "", "| Window TS | Symbol | Slug | Program `twap_strike` | Program `twap_close` | Gamma `priceToBeat` | Gamma `finalPrice` | Gamma Outcome | Pred |", "|---:|---|---|---:|---:|---:|---:|---|---|"]
    for m in r1.get("mismatches") or []:
        lines.append(f"| {m['window_ts']} | `{m['symbol']}` | `{m['slug']}` | {m['twap_strike']:.6f} | {m['twap_close']:.6f} | {m['gamma_price_to_beat']} | {m['gamma_final_price']} | `{m['resolved_outcome']}` | `{m['pred']}` |")


def _read_parity_enter_summary(out_dir: pathlib.Path) -> str:
    for cand in (out_dir / "parity_replay_report.md", pathlib.Path(__file__).with_name("parity_replay_report.md")):
        if not cand.exists():
            continue
        for line in cand.read_text(encoding="utf-8").splitlines():
            if "Coin-window signal union (`ENTER`):" in line:
                return line.split(":", 1)[1].strip()
    return ""


def _write_daily_report_md(report: Dict[str, Any], out_dir: pathlib.Path) -> pathlib.Path:
    r1 = report.get("r1_diagnostics") or {}
    lines = [
        "# Section 4.2 Daily Monitoring Report (Read-Only, Pre-Registered 3 Configs, Bonferroni 98.3% CI)",
        "",
        f"- Generated UTC: `{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}`",
        f"- Re-freeze count start boundary: `window_ts >= {report.get('count_start_window_ts', 0)}`",
        f"- Primary evaluation window boundary: `>= 2026-11-21T00:00:00Z` (`window_ts >= {report['primary_eval_window_ts']}`)",
        f"- Windows recorded / settled: `{report['windows_recorded']}` / `{report['windows_settled']}` | R1 TWAP parity (all): `{r1.get('parity_all')}` (`{r1.get('matched_all')}/{r1.get('total_all')}`) | post-reset: `{r1.get('parity_post_reset')}` (`{r1.get('matched_post_reset')}/{r1.get('total_post_reset')}`)",
    ]
    if report.get("reset_note"):
        lines.append(f"- Reset note: `{report['reset_note']}`")
    if report.get("parity_enter_summary"):
        lines.append(f"- Cumulative ENTER parity (`parity_replay`): {report['parity_enter_summary']}")
    lines += [
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
    _append_diagnostics_sections(lines, report)
    for sec_name in ("post_nov21", "pre_nov21"):
        sec_title = "Primary Per-Coin (`post_nov21`)" if sec_name == "post_nov21" else "Post-Reset Pre-Nov21 Baseline"
        lines += ["", f"## {sec_title}", "", "| Model | Coin | Signals | Filled +1s | Missed | Miss Rate | Mean Spread t0 | Filled WR | Missed Would-Win | Mean PnL +1s | 98.3% Bonferroni CI |", "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
        for lbl, st in report[sec_name].items():
            for sym, cs in (st.get("by_coin") or {}).items():
                missed_c = cs.get("missed_1s", int(cs["signals"]) - int(cs["filled_1s"]))
                lines.append(
                    f"| `{lbl}` | `{sym}` | {cs['signals']} | {cs['filled_1s']} | {missed_c} | {cs['miss_rate']} | "
                    f"{cs['mean_spread_t0']} | {cs['wr_1s']} | {cs['missed_would_win_rate']} | "
                    f"{cs['mean_pnl_1s']} | `{cs['ci98_3_bonferroni_1s']}` |"
                )
    p = out_dir / "phase42_daily_report.md"
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return p


def _freshness_stats(age_samples: Dict[str, List[float]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, vals in age_samples.items():
        if not vals:
            out[k] = {"n": 0, "p50": None, "p90": None, "p99": None}
        else:
            arr = np.asarray(vals, dtype=float)
            p50, p90, p99 = np.percentile(arr, [50, 90, 99])
            out[k] = {"n": len(vals), "p50": round(float(p50), 3), "p90": round(float(p90), 3), "p99": round(float(p99), 3)}
    return out


def analyze_recording(
    db_path: pathlib.Path = DEFAULT_DB, frozen_path: pathlib.Path = FROZEN_CFG_PATH,
    out_dir: pathlib.Path = ROOT, daily_report: bool = False,
    shadow_log_path: pathlib.Path = DEFAULT_SHADOW_LOG,
) -> dict[str, Any]:
    """Conservative (+1s / +2s) depth-aware FAK simulation + diagnostics with re-freeze reset boundary."""
    frozen = json.loads(frozen_path.read_text(encoding="utf-8"))
    count_start_ts = int(frozen.get("count_start_window_ts", 0) or 0)
    primary_ts = max(count_start_ts, NOV21_UTC_TS)
    fee_map = frozen.get("effective_fees_post_nov20", {"BTCUSDT": 0.05, "ETHUSDT": 0.01, "SOLUSDT": 0.01})
    con = sqlite3.connect(db_path)
    df_w = pd.read_sql_query("SELECT * FROM windows_42 ORDER BY window_ts, symbol", con)
    df_s = pd.read_sql_query("SELECT * FROM snapshots_1hz ORDER BY window_ts, symbol, elapsed_sec", con)
    shadow_parity = _compute_live_shadow_parity(con, shadow_log_path, count_start_ts)
    con.close()
    settled = df_w[df_w["resolved_outcome"].isin(["UP", "DOWN"])]
    r1_diag = _compute_r1_diagnostics(df_w, count_start_ts)
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
    v2_skips: Dict[str, int] = {}
    v2_ages: Dict[str, List[float]] = {}
    v2_warm_secs: set[int] = set()
    v2_df_t = pd.DataFrame()
    v2_df_reset = pd.DataFrame()
    for lbl, cfg in models.items():
        df_t, skips_m, ages_m, warm_s = _sim_one_cfg(cfg, fee_map, settled_ts, snap_by_wt_sym, w_meta)
        df_t.to_csv(out_dir / f"phase42_trades_1s_{lbl}.csv", index=False)
        df_reset = df_t[df_t["window_ts"] >= count_start_ts] if (len(df_t) and count_start_ts > 0) else df_t
        if lbl == "v2_filtered":
            v2_skips, v2_ages, v2_warm_secs, v2_df_t, v2_df_reset = skips_m, ages_m, warm_s, df_t, df_reset
        pre_summaries[lbl] = _summarize_model_trades(df_reset[df_reset["window_ts"] < primary_ts] if len(df_reset) else df_reset)
        post_summaries[lbl] = _summarize_model_trades(df_reset[df_reset["window_ts"] >= primary_ts] if len(df_reset) else df_reset)
        combined[lbl] = _summarize_model_trades(df_t)
    raw_sh = {k: v for k, v in (frozen.get("shadow_config_v2_highband") or {}).items() if k != "_meta"}
    sh_cfg = _cfg_from_dict(raw_sh) if raw_sh else TWAP_V2_HIGHBAND_SHADOW_CONFIG
    df_sh, _, _, _ = _sim_one_cfg(sh_cfg, fee_map, settled_ts, snap_by_wt_sym, w_meta)
    df_sh.to_csv(out_dir / "phase42_trades_1s_v2_highband_shadow.csv", index=False)
    df_sh_reset = df_sh[df_sh["window_ts"] >= count_start_ts] if (len(df_sh) and count_start_ts > 0) else df_sh
    shadow_out = {
        "v2_highband_shadow": {
            "bonferroni_included": False,
            "post_nov21": _summarize_model_trades(df_sh_reset[df_sh_reset["window_ts"] >= primary_ts] if len(df_sh_reset) else df_sh_reset),
            "pre_nov21": _summarize_model_trades(df_sh_reset[df_sh_reset["window_ts"] < primary_ts] if len(df_sh_reset) else df_sh_reset),
        }
    }
    v2_post = post_summaries["v2_filtered"]
    report: Dict[str, Any] = {
        "windows_recorded": int(len(df_w)), "windows_settled": int(len(settled)),
        "r1_twap_parity": r1_diag["parity_all"], "r1_diagnostics": r1_diag,
        "count_start_window_ts": count_start_ts, "primary_eval_window_ts": primary_ts,
        "reset_note": str(frozen.get("reset_note") or ""),
        "parity_enter_summary": _read_parity_enter_summary(out_dir),
        "signals_total_post_nov21": v2_post["signals"],
        "filled_1s_post_nov21": v2_post["filled_1s"], "mean_pnl_per_share_1s_post_nov21": v2_post["mean_pnl_1s"],
        "post_nov21": post_summaries, "pre_nov21": pre_summaries, "all_combined": combined, "shadow": shadow_out,
        "skip_counts": v2_skips, "freshness_dist": _freshness_stats(v2_ages),
        "freshness_ms": _compute_ms_freshness(df_s, count_start_ts, v2_ages),
        "warmup_diag": {"all": _count_warmup_episodes(v2_warm_secs), "post_reset": _count_warmup_episodes({t for t in v2_warm_secs if t >= count_start_ts})},
        "live_shadow_parity": shadow_parity,
        "twap_age_pnl": {"post_reset": _summarize_twap_age_buckets(v2_df_reset), "all": _summarize_twap_age_buckets(v2_df_t)},
        "carry_forward": _compute_carry_forward_stats(df_s, count_start_ts),
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
    ap.add_argument("--git-sha", type=str, default="", help="Override git SHA stored in frozen_config.json")
    ap.add_argument("--count-start-ts", type=int, default=0, help="Override count_start_window_ts")
    ap.add_argument("--reset-note", type=str, default="", help="Reset reason stored in frozen_config.json")
    ap.add_argument("--analyze", action="store_true", help="Run +1s/+2s offline fill simulation")
    ap.add_argument("--daily-report", action="store_true", help="Write phase42_daily_report.md during --analyze")
    args = ap.parse_args()
    if args.freeze_config:
        cfg = write_frozen_config(git_sha_override=args.git_sha or None, count_start_window_ts=args.count_start_ts or None, reset_note=args.reset_note or None)
        print(json.dumps({"frozen_config_written": str(FROZEN_CFG_PATH), "git_sha": cfg["git_sha"], "count_start_window_ts": cfg["count_start_window_ts"], "reset_note": cfg["reset_note"]}, indent=2))
        return
    if args.analyze:
        print(json.dumps(analyze_recording(args.db, daily_report=args.daily_report), indent=2))
        return
    if not FROZEN_CFG_PATH.exists():
        write_frozen_config()
    asyncio.run(LiveRecorder42(args.db).run_record(duration_sec=args.duration))


if __name__ == "__main__":
    main()
