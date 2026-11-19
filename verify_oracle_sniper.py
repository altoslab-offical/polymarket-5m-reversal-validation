"""Verification suite for the Chainlink late-window sniper (engine/oracle_sniper*.py).

Run: .venv/bin/python verify_oracle_sniper.py [--fast]
  default : unit / integration checks (no network, no real orders, temp state files) + historical replay parity
  --fast  : skip the (~25 s) replay of the recorded 1,007-window BTC database, which otherwise runs by default and
            asserts exact parity with the source repo's validated results (75 trades / 68 wins / identical entries).
"""
from __future__ import annotations

import asyncio
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock

from engine.notifier import NotificationManager, NotifyConfig
from engine.oracle_sniper_notify import alert_message, entry_message, exit_message, status_message, strip_html_tags
from engine.oracle_sniper import (
    DEFAULT_SNIPER_CONFIG,
    Candidate,
    FairValueEstimate,
    OpenPosition,
    OrderBookTop,
    RollingMicrostructureTracker,
    SniperConfig,
    compute_shares,
    effective_exit_proceeds,
    entry_limit_price,
    estimate_fair_value,
    evaluate_entry,
    evaluate_exit,
    pick_best_candidate,
    taker_fee_per_share,
)
from engine.oracle_sniper_feeds import SniperMarketHub, SymbolState, extract_executable_bid_ask, sweep_vwap
from engine.oracle_sniper_ledger import SniperLedger
from engine.oracle_sniper_runner import OracleSniperRunner
import engine.polymarket_auto_trader as pat
from engine.polymarket_auto_trader import PolymarketAutoTrader, PolymarketClobConfig

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data" / "oracle_sniper"
LIVE_DB = DATA_DIR / "live_btc_5m.db"
REFERENCE_JSON = DATA_DIR / "reference_trades_live_btc_5m.json"
CFG = DEFAULT_SNIPER_CONFIG


# ----------------------------------------------------------------------------------------------
# Historical replay (exact parity with the source repository)
# ----------------------------------------------------------------------------------------------
def _effective_strike(rows: list[tuple], db_strike: Optional[float], strike_src: Optional[str]) -> Optional[float]:
    first_cl = next((r[2] for r in rows if r[2] is not None and r[1] < 5.0), None)
    return db_strike if (strike_src == "GAMMA_OFFICIAL" and db_strike) else (first_cl or db_strike)


def _tick_fv(tracker: RollingMicrostructureTracker, cfg: SniperConfig, strike: float, row: tuple) -> Optional[FairValueEstimate]:
    ts, elapsed, cl, cex, cvd, obi = row[0], row[1], row[2], row[3], row[4], row[5]
    tracker.on_price(ts, cex or cl)
    vol = tracker.get_realized_vol_1s(floor=cfg.min_1s_vol_floor)
    gap = tracker.update_oracle_basis(cl, cex)
    flow = 0.70 * (cvd or 0.0) + 0.30 * (obi or 0.0)
    return estimate_fair_value(
        strike, cl, max(0.0, 300.0 - elapsed), vol, orderflow_score=flow, dynamic_gap_bps=gap, student_t_df=cfg.student_t_df
    )


def _book_from_row(row: tuple) -> OrderBookTop:
    if row[1] < 1.5:
        return OrderBookTop()
    return OrderBookTop(up_bid=row[6], up_ask=row[7], down_bid=row[8], down_ask=row[9])


def _replay_window(
    cfg: SniperConfig, tracker: RollingMicrostructureTracker, rows: list[tuple], strike: float, w_ts: int, outcome: str
) -> Optional[dict]:
    pos: Optional[OpenPosition] = None
    trade: Optional[dict] = None
    for row in rows:
        if row[2] is None:
            continue
        fv = _tick_fv(tracker, cfg, strike, row)
        if fv is None:
            continue
        book = _book_from_row(row)
        if pos is not None and trade is not None and trade["exit_reason"] is None:
            dec = evaluate_exit(cfg, pos, row[1], fv, book)
            if dec is not None:
                trade.update(exit_reason=dec.reason, payout=dec.net_proceeds_per_share)
            continue
        cand = evaluate_entry("BTCUSDT", cfg, row[1], fv, book) if trade is None else None
        if cand is not None:
            pos = OpenPosition("BTCUSDT", cand.side, w_ts, 5.0, cand.ask, cand.total_cost_per_share, cand.p_true, cand.z)
            trade = {"window_ts": w_ts, "side": cand.side, "entry": cand.ask, "cost": cand.total_cost_per_share,
                     "exit_reason": None, "payout": None}
    if trade is not None and trade["exit_reason"] is None:
        won = outcome == trade["side"]
        trade.update(exit_reason=f"SETTLED_{outcome}", payout=1.0 if won else 0.0)
    return trade


def replay_database(db_path: Path, cfg: SniperConfig = CFG) -> list[dict]:
    """Replay recorded snapshots through the production entry/exit functions (one symbol, one shot/window)."""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    tracker = RollingMicrostructureTracker(vol_lookback_sec=cfg.vol_lookback_sec)
    trades: list[dict] = []
    windows = cur.execute(
        "SELECT window_ts, strike_price, strike_source, close_price, resolved_outcome FROM windows "
        "WHERE resolved_outcome IS NOT NULL ORDER BY window_ts"
    ).fetchall()
    for w_ts, db_strike, strike_src, _close, outcome in windows:
        rows = cur.execute(
            "SELECT ts, elapsed_sec, chainlink_price, cex_lead_price, cvd_imb, obi, up_bid, up_ask, down_bid, "
            "down_ask FROM snapshots WHERE window_ts=? ORDER BY ts ASC", (w_ts,),
        ).fetchall()
        strike = _effective_strike(rows, db_strike, strike_src) if rows else None
        trade = _replay_window(cfg, tracker, rows, strike, w_ts, outcome) if strike else None
        if trade is not None:
            trades.append(trade)
    conn.close()
    return trades


def check_replay_parity() -> int:
    """Exact parity against the source repo's replay (75 trades, 68 wins, identical entries and exits)."""
    trades = replay_database(LIVE_DB)
    ref = json.loads(REFERENCE_JSON.read_text(encoding="utf-8"))
    assert len(trades) == len(ref) == 75, (len(trades), len(ref))
    for mine, theirs in zip(trades, ref):
        assert (mine["window_ts"], mine["side"]) == (theirs["window_ts"], theirs["side"]), (mine, theirs)
        assert abs(mine["entry"] - theirs["entry"]) < 1e-9 and mine["exit_reason"] == theirs["reason"], (mine, theirs)
        assert abs(mine["payout"] - theirs["payout"]) < 1e-9, (mine, theirs)
    assert sum(1 for t in trades if t["payout"] > t["cost"]) == 68
    return 1


# ----------------------------------------------------------------------------------------------
# Pure-logic unit checks
# ----------------------------------------------------------------------------------------------
def _fv(z_frac: float = 0.0009, tau: float = 100.0, flow: float = 0.0, cvd: float = 0.0, gap: float = 0.0) -> FairValueEstimate:
    fv = estimate_fair_value(100.0, 100.0 * (1.0 + z_frac), tau, 5e-5, orderflow_score=flow, dynamic_gap_bps=gap, cvd_imbalance=cvd)
    assert fv is not None
    return fv


def _good_book(fv: FairValueEstimate, side: str = "UP") -> OrderBookTop:
    ask = min(0.82, max(0.70, round((fv.p_true_up if side == "UP" else fv.p_true_down) - 0.10, 2)))
    if side == "UP":
        return OrderBookTop(up_bid=ask - 0.02, up_ask=ask, down_bid=1 - ask - 0.02, down_ask=1 - ask + 0.02)
    return OrderBookTop(down_bid=ask - 0.02, down_ask=ask, up_bid=1 - ask - 0.02, up_ask=1 - ask + 0.02)


def check_fee_and_model() -> int:
    """Fee rounding, break-even, and monotonic fat-tail probabilities."""
    assert taker_fee_per_share(0.80) == 0.0112 and taker_fee_per_share(0.0) == 0.0 and taker_fee_per_share(1.0) == 0.0
    assert abs(effective_exit_proceeds(0.99) - 0.98931) < 1e-9
    fv_lo, fv_hi = _fv(0.0005), _fv(0.0010)
    assert 0.5 < fv_lo.p_true_up < fv_hi.p_true_up < 1.0
    assert abs(_fv(-0.0009).p_true_up - (1.0 - _fv(0.0009).p_true_up)) < 2e-3
    assert _fv(0.0009).raw_z_score > 1.3
    return 1


def check_entry_gates() -> int:
    """Each entry gate must block independently and the happy path must pass."""
    fv = _fv()
    book = _good_book(fv)
    ok = evaluate_entry("BTCUSDT", CFG, 200.0, fv, book)
    assert ok is not None and ok.side == "UP" and ok.net_ev >= CFG.min_net_edge
    assert evaluate_entry("BTCUSDT", CFG, 149.0, fv, book) is None and evaluate_entry("BTCUSDT", CFG, 256.0, fv, book) is None
    assert evaluate_entry("BTCUSDT", CFG, 200.0, _fv(0.0003), book) is None  # z too small
    assert evaluate_entry("BTCUSDT", CFG, 200.0, _fv(flow=-0.2), book) is None  # counter order-flow
    assert evaluate_entry("BTCUSDT", CFG, 200.0, _fv(cvd=-0.2), book) is None  # counter CVD
    assert evaluate_entry("BTCUSDT", CFG, 200.0, _fv(gap=-9.0), book) is None  # adverse oracle gap
    assert evaluate_entry("BTCUSDT", CFG, 200.0, fv, OrderBookTop(up_bid=0.80, up_ask=0.88)) is None  # ask too high
    assert evaluate_entry("BTCUSDT", CFG, 200.0, fv, OrderBookTop(up_bid=0.60, up_ask=0.70)) is None  # spread > 6c
    assert evaluate_entry("BTCUSDT", CFG, 200.0, fv, OrderBookTop(up_bid=0.60, up_ask=0.64)) is None  # ask too low
    assert evaluate_entry("BTCUSDT", CFG, 200.0, fv, OrderBookTop()) is None  # no quotes
    down_fv = _fv(-0.0009)
    down = evaluate_entry("ETHUSDT", CFG, 200.0, down_fv, _good_book(down_fv, "DOWN"))
    assert down is not None and down.side == "DOWN"
    return 1


def _cand(sym: str, ev: float) -> Candidate:
    return Candidate(sym, "UP", 0.8, 0.78, 0.9, 1.8, ev, 0.011, 0.811, 200.0)


def check_single_slot_ranking_and_sizing() -> int:
    """Highest net EV wins (BTC gets a 1c validated-coin bonus); chip control sizing and price caps."""
    assert pick_best_candidate([]) is None
    assert pick_best_candidate([_cand("ETHUSDT", 0.10), _cand("SOLUSDT", 0.12), _cand("BTCUSDT", 0.115)]).symbol == "BTCUSDT"
    assert pick_best_candidate([_cand("ETHUSDT", 0.10), _cand("SOLUSDT", 0.12), _cand("BTCUSDT", 0.05)]).symbol == "SOLUSDT"
    assert compute_shares(CFG, 43.10, 0.8132) == 5.0
    assert compute_shares(CFG, 3.9, 0.81) == 0.0
    assert 6.0 < compute_shares(CFG, 100.0, 0.81) < 6.5 and compute_shares(CFG, 400.0, 0.81) > 24.0
    assert entry_limit_price(CFG, 0.78, 0.95) == 0.82 and entry_limit_price(CFG, 0.81, 0.845) <= 0.82
    return 1


def check_exit_rules() -> int:
    """Early take-profit at >=0.99 (or 0.98 when EV-consistent) and the anti-whipsaw structural stop."""
    pos = OpenPosition("BTCUSDT", "UP", 0, 5.0, 0.79, 0.801, 0.9, 1.8)
    fv_ok = _fv(0.0009, tau=20.0)
    tp = evaluate_exit(CFG, pos, 280.0, fv_ok, OrderBookTop(up_bid=0.99, up_ask=1.0))
    assert tp is not None and tp.reason == "EARLY_TAKE_PROFIT"
    assert evaluate_exit(CFG, pos, 200.0, fv_ok, OrderBookTop(up_bid=0.90, up_ask=0.92)) is None
    fv_bad = _fv(-0.0004, tau=40.0, flow=-0.6)
    sl = evaluate_exit(CFG, pos, 260.0, fv_bad, OrderBookTop(up_bid=0.45, up_ask=0.50))
    assert sl is not None and sl.reason == "EMERGENCY_STOP_LOSS"
    assert evaluate_exit(CFG, pos, 260.0, fv_bad, OrderBookTop(up_bid=0.45, up_ask=0.60)) is None  # illiquid spread
    assert evaluate_exit(CFG, pos, 295.0, fv_bad, OrderBookTop(up_bid=0.45, up_ask=0.50)) is None  # too late to dump
    return 1


# ----------------------------------------------------------------------------------------------
# Ledger / guards / feeds helpers
# ----------------------------------------------------------------------------------------------
def _trade(sym: str, idx: int, pnl: float) -> dict[str, Any]:
    return {"id": f"{idx}-{sym}", "symbol": sym, "side": "UP", "window_ts": idx, "cost_usd": 4.0, "entry_ts": 1.0,
            "shares": 5.0, "shares_open": 5.0}


def check_ledger_guards(tmp: Path) -> int:
    """Daily loss breaker, rolling/per-symbol win-rate pauses, persistence and settling queue."""
    led = SniperLedger(tmp / "ledger_a.json")
    led.ensure_baseline(50.0)
    assert led.guard_reason() is None and abs(led.daily_loss_limit_u() - 8.0) < 1e-9
    led.close_trade(_trade("BTCUSDT", 1, -4.0), {"pnl_usd": -4.0})
    assert led.guard_reason() is None
    alerts = led.close_trade(_trade("BTCUSDT", 2, -4.0), {"pnl_usd": -4.0})
    assert led.guard_reason() == "DAILY_LOSS_LIMIT" and alerts
    led2 = SniperLedger(tmp / "ledger_a.json")
    assert led2.guard_reason() == "DAILY_LOSS_LIMIT" and len(led2.state["trades"]) == 2
    led3 = SniperLedger(tmp / "ledger_b.json")
    led3.ensure_baseline(500.0)
    for i in range(12):  # ETH wins only 8/12 = 67% < 75%
        led3.close_trade(_trade("ETHUSDT", i, 1.0 if i < 8 else -4.0), {"pnl_usd": 1.0 if i < 8 else -4.0})
    assert led3.symbol_paused("ETHUSDT") and not led3.symbol_paused("BTCUSDT") and led3.guard_reason() is None
    led4 = SniperLedger(tmp / "ledger_c.json")
    led4.ensure_baseline(500.0)
    for i in range(20):  # 13/20 = 65% < 72% global
        led4.close_trade(_trade("BTCUSDT", i, 1.0 if i < 13 else -4.0), {"pnl_usd": 1.0 if i < 13 else -4.0})
    assert led4.guard_reason() == "ROLLING_WIN_RATE_PAUSE"
    led4.mark_settling(_trade("SOLUSDT", 99, 0.0))
    assert [t["id"] for t in led4.state["settling"]] == ["99-SOLUSDT"]
    led4.set_enabled(False)
    assert SniperLedger(tmp / "ledger_c.json").guard_reason() == "DISABLED"
    return 1


def check_feed_helpers() -> int:
    """Depth-aware VWAP book parsing, CEX lead blend and window rollover prefetch adoption."""
    assert sweep_vwap([], 10.0) is None and sweep_vwap([(0.99, 1.0)], 30.0) == 0.99
    assert sweep_vwap([(0.80, 2.0), (0.81, 40.0)], 31.0) is not None
    assert sweep_vwap([(0.80, 1.0)], 31.0) is None  # dust only
    payload = {"bids": [{"price": "0.78", "size": "60"}], "asks": [{"price": "0.80", "size": "70"}, {"price": "0.81", "size": "50"}]}
    bid, ask = extract_executable_bid_ask(payload)
    assert bid == 0.78 and ask == 0.80
    assert extract_executable_bid_ask({"bids": [{"price": "0.80", "size": "60"}], "asks": [{"price": "0.79", "size": "60"}]}) == (None, None)
    st = SymbolState("BTCUSDT", RollingMicrostructureTracker())
    now = time.time()
    st.coinbase_price, st.coinbase_ts, st.binance_price, st.binance_ts = 100.0, now, 100.05, now
    st.update_basis(now)
    assert abs((st.cex_lead_price(now) or 0.0) - 100.0) < 0.05
    hub = SniperMarketHub(MagicMock())
    hub.prefetched[("BTCUSDT", 600)] = {"slug": "btc-updown-5m-600", "up_token_id": "u", "down_token_id": "d", "price_to_beat": 1.5}
    hub.roll_window(630.0)
    s = hub.states["BTCUSDT"]
    assert (s.window_ts, s.up_token, s.down_token, s.price_to_beat) == (600, "u", "d", 1.5)
    return 1


# ----------------------------------------------------------------------------------------------
# Runner integration (mocked exchange, chainlink, notifier)
# ----------------------------------------------------------------------------------------------
class FakeChainlink:
    """Minimal ChainlinkRTDSFeed stand-in with controllable prices."""

    def __init__(self) -> None:
        self.prices = {"BTCUSDT": 100.0, "ETHUSDT": 100.0, "SOLUSDT": 100.0}
        self.opens = {"BTCUSDT": 100.0, "ETHUSDT": 100.0, "SOLUSDT": 100.0}
        self.ticks: dict[str, list] = {s: [] for s in self.prices}

    def seed_boundary(self, window_ts: int, offset_ms: float = 0.0) -> None:
        """Provide a Chainlink tick near the 5m boundary (the RTDS bucket-open strike source)."""
        for sym in self.prices:
            self.ticks[sym] = [(window_ts * 1000.0 + offset_ms, 100.0)]

    def get_latest_price(self, sym: str, max_age_sec: float = 20.0) -> tuple[Optional[float], float]:
        return self.prices[sym], 0.4

    def get_bucket_open_price(self, sym: str, _bucket: int) -> Optional[float]:
        return self.opens[sym]


def _fake_trader(wallet_cash: float = 43.10) -> MagicMock:
    at = MagicMock()
    at.live_wallet_state = {"cash_pusd": wallet_cash, "portfolio_pusd": 0.0, "total_equity_pusd": wallet_cash,
                            "updated_at_ms": int(time.time() * 1000)}
    at.last_geoblock_status = {"can_trade_api": True}
    at.sync_live_wallet_balance = AsyncMock(return_value=at.live_wallet_state)
    at.auto_redeem_winning_positions = AsyncMock(return_value={"claimed_pusd": 5.0})
    at.sniper_buy = AsyncMock(return_value={"filled": True, "shares": 5.0, "usd": 3.95, "avg_price": 0.79, "order_id": "o1",
                                            "status": "FILLED", "error": None})
    at.sniper_sell = AsyncMock(return_value={"filled": True, "shares": 5.0, "usd": 4.95, "avg_price": 0.99, "order_id": "o2",
                                             "status": "FILLED", "error": None})
    at.query_live_market_resolution = AsyncMock(return_value={"resolved": True, "polymarket_won": True,
                                                              "final_price": 100.2, "price_to_beat": 100.0})
    at.discover_live_5m_market = AsyncMock(return_value=None)
    return at


def _make_runner(tmp: Path, tag: str, cash: float = 43.10) -> tuple[OracleSniperRunner, MagicMock, FakeChainlink, MagicMock, int]:
    at, cl = _fake_trader(cash), FakeChainlink()
    notifier = MagicMock()
    notifier.dispatch_signal = AsyncMock()
    runner = OracleSniperRunner(at, cl, notifier, ledger=SniperLedger(tmp / f"runner_{tag}.json"))
    window_ts = (int(time.time()) // 300) * 300 + 300
    at.live_wallet_state["updated_at_ms"] = int((window_ts + 200.0) * 1000)  # simulated clock sits inside the window
    for sym, st in runner.hub.states.items():
        st.reset_for_window(window_ts)
        st.up_token, st.down_token, st.slug, st.price_to_beat = f"{sym}-up", f"{sym}-dn", f"{sym[:3].lower()}-updown-5m-{window_ts}", 100.0
        st.meta_checked_ts = 1e12  # no metadata refresh needed
        st.prefetch_ts = 1e12
        for i in range(60):
            st.tracker.on_price(window_ts - 60 + i, 100.0)
    return runner, at, cl, notifier, window_ts


def _arm(runner: OracleSniperRunner, cl: FakeChainlink, sym: str, now: float, z_frac: float, ev_gap: float = 0.10) -> None:
    cl.prices[sym] = 100.0 * (1.0 + z_frac)
    fv = estimate_fair_value(100.0, cl.prices[sym], 300.0 - (now % 300), 5e-5)
    assert fv is not None
    ask = min(0.82, max(0.70, round(fv.p_true_up - ev_gap, 2))) if z_frac > 0 else min(0.82, max(0.70, round(fv.p_true_down - ev_gap, 2)))
    st = runner.hub.states[sym]
    st.book = OrderBookTop(ask - 0.02, ask, 1 - ask - 0.02, 1 - ask + 0.02) if z_frac > 0 else OrderBookTop(1 - ask - 0.02, 1 - ask + 0.02, ask - 0.02, ask)
    st.up_local_ts = st.down_local_ts = now


async def _scenario_single_slot(tmp: Path) -> int:
    runner, at, cl, notifier, w = _make_runner(tmp, "slot")
    now = w + 200.0
    _arm(runner, cl, "BTCUSDT", now, 0.0009, ev_gap=0.14)
    _arm(runner, cl, "ETHUSDT", now, 0.0011, ev_gap=0.10)
    _arm(runner, cl, "SOLUSDT", now, -0.0010, ev_gap=0.12)
    await runner._tick(now)
    await asyncio.sleep(0)
    assert at.sniper_buy.await_count == 1, "all three coins fired -> exactly one order"
    token = at.sniper_buy.await_args.args[0]
    assert token.endswith("-up") and runner._pos is not None and runner._pos.symbol in ("BTCUSDT", "ETHUSDT")
    for dt in (1.0, 2.0, 8.0):  # same window, still no second order
        _arm(runner, cl, "SOLUSDT", now + dt, -0.0010)
        await runner._tick(now + dt)
    assert at.sniper_buy.await_count == 1
    assert runner.ledger.state["open_position"]["symbol"] == runner._pos.symbol
    return 1


async def _scenario_take_profit(tmp: Path) -> int:
    runner, at, cl, notifier, w = _make_runner(tmp, "tp")
    now = w + 200.0
    _arm(runner, cl, "BTCUSDT", now, 0.0009)
    await runner._tick(now)
    assert runner._pos is not None
    later = w + 275.0
    cl.prices["BTCUSDT"] = 100.0 * 1.0020
    runner.hub.states["BTCUSDT"].book = OrderBookTop(0.99, 1.0, 0.0, 0.01)
    runner.hub.states["BTCUSDT"].up_local_ts = runner.hub.states["BTCUSDT"].down_local_ts = later
    await runner._tick(later)
    await asyncio.sleep(0)
    assert at.sniper_sell.await_count == 1 and runner._pos is None
    closed = runner.ledger.state["trades"][0]
    net_ps = effective_exit_proceeds(0.99)
    assert closed["exit_reason"] == "EARLY_TAKE_PROFIT" and closed["won"]
    assert abs(closed["pnl_usd"] - (5.0 * net_ps - (3.95 + 5.0 * taker_fee_per_share(0.79)))) < 1e-3, closed["pnl_usd"]
    types = [c.args[0]["event_type"] for c in notifier.dispatch_signal.await_args_list]
    assert types[:2] == ["SNIPER_ENTRY", "SNIPER_EXIT"], types
    assert all(c.args[0]["source"] == "oracle_sniper" for c in notifier.dispatch_signal.await_args_list)
    msg = notifier.dispatch_signal.await_args_list[1].args[0]["message"]
    assert "[TARGET HIT // 獲利止盈" in msg and "▸ 累計戰績：" in msg and "▸ 累計損益：" in msg and "權益" in msg
    return 1


async def _scenario_settlement(tmp: Path, won: bool) -> int:
    runner, at, cl, notifier, w = _make_runner(tmp, f"settle_{won}")
    at.query_live_market_resolution = AsyncMock(return_value={"resolved": True, "polymarket_won": won, "final_price": 100.2, "price_to_beat": 100.0})
    now = w + 200.0
    _arm(runner, cl, "BTCUSDT", now, 0.0009)
    await runner._tick(now)
    assert runner._pos is not None
    trade = dict(runner._trade or {})
    runner._begin_settlement()
    assert runner._pos is None and [t["id"] for t in runner.ledger.state["settling"]] == [trade["id"]]
    real_sleep = asyncio.sleep
    asyncio.sleep = lambda *_a, **_k: real_sleep(0)  # type: ignore[assignment]
    try:
        await runner._settle(trade)
        await real_sleep(0.05)
    finally:
        asyncio.sleep = real_sleep  # type: ignore[assignment]
    closed = runner.ledger.state["trades"][0]
    assert closed["won"] is won and runner.ledger.state["settling"] == []
    abs_cost = 3.95 + 5.0 * taker_fee_per_share(0.79)  # spent + fee on top, independent of the runner's formula
    assert abs(trade["cost_usd"] - abs_cost) < 1e-3, (trade["cost_usd"], abs_cost)
    expected = (5.0 - abs_cost) if won else -abs_cost
    assert abs(closed["pnl_usd"] - expected) < 1e-3, (closed["pnl_usd"], expected)
    assert (at.auto_redeem_winning_positions.await_count >= 1) is won
    return 1


async def _scenario_no_fill_and_guards(tmp: Path) -> int:
    runner, at, cl, notifier, w = _make_runner(tmp, "nofill")
    at.sniper_buy = AsyncMock(return_value={"filled": False, "shares": 0.0, "usd": 0.0, "avg_price": 0.0, "order_id": "",
                                            "status": "UNMATCHED", "error": "FAK matched no shares"})
    now = w + 200.0
    _arm(runner, cl, "BTCUSDT", now, 0.0009)
    await runner._tick(now)
    assert runner._pos is None and at.sniper_buy.await_count == 1
    await runner._tick(now + 1.0)  # inside the 5s spacing -> no retry
    assert at.sniper_buy.await_count == 1
    for k in (6.0, 12.0, 18.0, 24.0):
        _arm(runner, cl, "BTCUSDT", now + k, 0.0009)
        await runner._tick(now + k)
    assert at.sniper_buy.await_count == 3, "capped at 3 attempts per window"
    runner2, at2, cl2, _n2, w2 = _make_runner(tmp, "guards")
    now2 = w2 + 200.0
    _arm(runner2, cl2, "BTCUSDT", now2, 0.0009)
    runner2.set_enabled(False)
    await runner2._tick(now2)
    assert at2.sniper_buy.await_count == 0
    runner2.set_enabled(True)
    at2.live_wallet_state["updated_at_ms"] = int((now2 - 600) * 1000)  # stale wallet reading
    await runner2._tick(now2 + 1.0)
    assert at2.sniper_buy.await_count == 0
    at2.live_wallet_state["updated_at_ms"] = int((now2 + 1.0) * 1000)
    runner2.ledger.state["pause"]["symbol_until_ms"]["BTCUSDT"] = int((now2 + 600) * 1000)
    await runner2._tick(now2 + 2.0)
    assert at2.sniper_buy.await_count == 0, "symbol paused by edge guard"
    runner3, at3, cl3, _n3, w3 = _make_runner(tmp, "unknown")
    at3.sniper_buy = AsyncMock(return_value={"filled": False, "shares": 0.0, "usd": 0.0, "avg_price": 0.0, "order_id": "",
                                             "status": "UNKNOWN", "error": "closed"})
    now3 = w3 + 200.0
    for k in (0.0, 6.0, 12.0):
        _arm(runner3, cl3, "BTCUSDT", now3 + k, 0.0009)
        await runner3._tick(now3 + k)
    assert at3.sniper_buy.await_count == 1, "uncertain fill state => never buy twice in the same window"
    return 1


async def _scenario_insufficient_cash_and_recovery(tmp: Path) -> int:
    runner, at, cl, _n, w = _make_runner(tmp, "poor", cash=3.0)
    now = w + 200.0
    _arm(runner, cl, "BTCUSDT", now, 0.0009)
    await runner._tick(now)
    assert at.sniper_buy.await_count == 0 and runner.ledger.state["skips"].get("BELOW_MIN_LOT", 0) == 1
    ledger = SniperLedger(tmp / "runner_recover.json")
    ledger.open_trade({"id": "1-BTCUSDT", "symbol": "BTCUSDT", "side": "UP", "window_ts": w, "slug": "x", "token_id": "t",
                       "shares_open": 5.0, "entry_price": 0.79, "cost_per_share": 0.8, "p_true": 0.9, "z": 1.8,
                       "cost_usd": 4.0, "partial_proceeds_usd": 0.0})
    r2 = OracleSniperRunner(_fake_trader(), FakeChainlink(), MagicMock(), ledger=SniperLedger(tmp / "runner_recover.json"))
    r2._recover_open_position()
    assert r2._pos is not None and r2._pos.symbol == "BTCUSDT" and r2._pos.shares == 5.0
    return 1



async def _scenario_restart_mid_entry(tmp: Path) -> int:
    """A crash between order-send and ledger-write must be reconciled from the on-chain balance."""
    runner, at, cl, _n, w = _make_runner(tmp, "pending")
    intent = {"symbol": "BTCUSDT", "side": "UP", "token_id": "BTCUSDT-up", "window_ts": w, "slug": "s", "condition_id": "c",
              "strike": 100.0, "ts": w + 200.0, "target_shares": 5.0, "limit_px": 0.82, "est_cost_usd": 4.2, "n_cands": 1,
              "cand": {"symbol": "BTCUSDT", "side": "UP", "ask": 0.79, "bid": 0.77, "p_true": 0.93, "z": 1.8, "net_ev": 0.12,
                       "fee_per_share": 0.0116, "total_cost_per_share": 0.8016, "elapsed_sec": 200.0}}
    runner.ledger.set_pending_entry(intent)
    assert runner.ledger.guard_reason() is None and not runner._entry_allowed(w, w + 200.0), "pending intent blocks entries"
    at.sniper_token_balance = AsyncMock(return_value=5.0)
    real_sleep = asyncio.sleep
    asyncio.sleep = lambda *_a, **_k: real_sleep(0)  # type: ignore[assignment]
    try:
        await runner._reconcile_pending(intent)
    finally:
        asyncio.sleep = real_sleep  # type: ignore[assignment]
    assert runner._pos is not None and runner._pos.shares == 5.0 and runner.ledger.state["pending_entry"] is None
    assert runner.ledger.state["open_position"]["fill_status"] == "ADOPTED_FROM_BALANCE"
    r2, at2, _c2, _n2, w2 = _make_runner(tmp, "pending_zero")
    r2.ledger.set_pending_entry(intent)
    at2.sniper_token_balance = AsyncMock(return_value=0.0)
    asyncio.sleep = lambda *_a, **_k: real_sleep(0)  # type: ignore[assignment]
    try:
        await r2._reconcile_pending(intent)
    finally:
        asyncio.sleep = real_sleep  # type: ignore[assignment]
    assert r2._pos is None and r2.ledger.state["pending_entry"] is None
    r3, at3, _c3, _n3, _w3 = _make_runner(tmp, "pending_unknown")
    r3.ledger.set_pending_entry(intent)
    at3.sniper_token_balance = AsyncMock(side_effect=RuntimeError("api down"))
    asyncio.sleep = lambda *_a, **_k: real_sleep(0)  # type: ignore[assignment]
    try:
        await r3._reconcile_pending(intent)
    finally:
        asyncio.sleep = real_sleep  # type: ignore[assignment]
    assert r3.ledger.state["pending_entry"] is not None, "never clear an intent we could not verify"
    return 1


async def _scenario_same_window_after_restart(tmp: Path) -> int:
    runner, at, cl, _n, w = _make_runner(tmp, "reentry")
    runner.ledger.state["trades"].insert(0, {**_trade("BTCUSDT", w, 1.0), "window_ts": w, "status": "CLOSED", "won": True,
                                              "pnl_usd": 1.0, "exit_ts": w + 250.0})
    runner.ledger.save()
    r2 = OracleSniperRunner(at, cl, MagicMock(), ledger=SniperLedger(tmp / "runner_reentry.json"))
    r2._seed_traded_windows()
    assert w in r2._traded_windows
    for sym, st in runner.hub.states.items():
        r2.hub.states[sym] = st
    _arm(r2, cl, "BTCUSDT", w + 200.0, 0.0009)
    r2.notifier.dispatch_signal = AsyncMock()
    await r2._tick(w + 200.0)
    assert at.sniper_buy.await_count == 0, "no second entry in a window that already traded"
    return 1


async def _scenario_sell_balance_gone(tmp: Path) -> int:
    runner, at, cl, notifier, w = _make_runner(tmp, "gone")
    now = w + 200.0
    _arm(runner, cl, "BTCUSDT", now, 0.0009)
    await runner._tick(now)
    assert runner._pos is not None
    at.sniper_sell = AsyncMock(return_value={"filled": False, "shares": 0.0, "usd": 0.0, "avg_price": 0.0, "order_id": "",
                                             "status": "NO_BALANCE", "error": "verified conditional balance 0.0 shares"})
    later = w + 275.0
    cl.prices["BTCUSDT"] = 100.0 * 1.0020
    st = runner.hub.states["BTCUSDT"]
    st.book = OrderBookTop(0.99, 1.0, 0.0, 0.01)
    st.up_local_ts = st.down_local_ts = later
    await runner._tick(later)
    await asyncio.sleep(0)
    closed = runner.ledger.state["trades"][0]
    assert runner._pos is None and closed.get("reconciled_from_balance") is True and closed["exit_reason"] == "EARLY_TAKE_PROFIT"
    types = [c.args[0]["event_type"] for c in notifier.dispatch_signal.await_args_list]
    assert "SNIPER_ALERT" in types
    return 1


async def _scenario_data_hygiene(tmp: Path) -> int:
    """RTDS strike window, per-side book freshness, exposure breaker and corrupt-ledger recovery."""
    runner, at, cl, _n, w = _make_runner(tmp, "hygiene")
    for sym, st in runner.hub.states.items():
        st.price_to_beat = None
    cl.seed_boundary(w, offset_ms=800.0)
    assert runner._rtds_open("BTCUSDT", w) == 100.0
    cl.seed_boundary(w, offset_ms=12_000.0)  # first tick only arrives 12s late -> unusable strike
    runner._open_cache.clear()
    assert runner._rtds_open("BTCUSDT", w + 1000) is None
    st = runner.hub.states["BTCUSDT"]
    st.book = OrderBookTop(0.76, 0.78, 0.20, 0.22)
    now = w + 200.0
    st.up_local_ts, st.down_local_ts = now, now - 30.0
    book = runner._book_for(st, now, 200.0)
    assert book.up_ask == 0.78 and book.down_ask is None and book.down_bid is None, "stale side must be dropped"
    led = SniperLedger(tmp / "exposure.json")
    led.ensure_baseline(50.0)
    led.close_trade(_trade("BTCUSDT", 1, -4.0), {"pnl_usd": -4.0})
    led.mark_settling({**_trade("BTCUSDT", 2, 0.0), "cost_usd": 4.1})
    assert led.guard_reason() == "DAILY_LOSS_LIMIT_PENDING"
    bad = tmp / "corrupt.json"
    bad.write_text("{not json", encoding="utf-8")
    led_bad = SniperLedger(bad)
    assert led_bad.enabled is False and led_bad.state.get("load_alert") and list(tmp.glob("corrupt.json.corrupt-*"))
    partial = tmp / "partial.json"
    partial.write_text(json.dumps({"pause": {"reason": "x"}, "daily": {"date": "2000-01-01"}}), encoding="utf-8")
    assert SniperLedger(partial).state["pause"]["global_until_ms"] == 0
    return 1


def check_trader_hygiene(tmp: Path) -> int:
    """Outcome-label token mapping, live-trading gate and strict (raising) balance reads."""
    assert pat.resolve_up_down_indices(["Up", "Down"]) == (0, 1)
    assert pat.resolve_up_down_indices(["Down", "Up"]) == (1, 0), "token order must follow labels, not positions"
    assert pat.resolve_up_down_indices(["Yes", "No"]) is None and pat.resolve_up_down_indices(["Up"]) is None
    original = pat.CONFIG_PATH
    pat.CONFIG_PATH = tmp / "clob_hyg.json"
    try:
        at = PolymarketAutoTrader()
        at.config = PolymarketClobConfig(private_key="0x" + "1" * 64, sniper_live=False)
        at.last_geoblock_status = {"can_trade_api": True}

        async def _run() -> None:
            assert (await at.sniper_buy("t", 5.0, 0.8, 0.82))["status"] == "DISABLED"
            at.config.sniper_live = True
            at.last_geoblock_status = {}
            assert (await at.sniper_sell("t", 5.0, 0.9))["status"] == "DISABLED", "no geoblock status => refuse"
            client = MagicMock()
            client.get_balance_allowance = AsyncMock(return_value=SimpleNamespace(balance=4_996_000))
            assert await at._strict_conditional_shares(client, "t") == 4.99, "balances are floored, never rounded up"
            client.get_balance_allowance = AsyncMock(side_effect=RuntimeError("down"))
            try:
                await at._strict_conditional_shares(client, "t")
            except RuntimeError:
                return
            raise AssertionError("strict balance must raise when every query fails")

        asyncio.run(_run())
    finally:
        pat.CONFIG_PATH = original
    return 1


def check_runner_scenarios(tmp: Path) -> int:
    """Run every mocked end-to-end runner scenario."""
    n = 0
    for coro in (_scenario_single_slot(tmp), _scenario_take_profit(tmp), _scenario_settlement(tmp, True),
                 _scenario_settlement(tmp, False), _scenario_no_fill_and_guards(tmp),
                 _scenario_insufficient_cash_and_recovery(tmp), _scenario_restart_mid_entry(tmp),
                 _scenario_same_window_after_restart(tmp), _scenario_sell_balance_gone(tmp), _scenario_data_hygiene(tmp)):
        n += asyncio.run(coro)
    return 1 if n == 10 else 0


# ----------------------------------------------------------------------------------------------
# Auto-trader execution (mocked SDK) and configuration pinning
# ----------------------------------------------------------------------------------------------
def _accepted(taking: str, making: str, status: str = "matched") -> SimpleNamespace:
    return SimpleNamespace(ok=True, order_id="0xabc", status=status, taking_amount=Decimal(taking), making_amount=Decimal(making))


async def _scenario_trader_exec(tmp: Path) -> int:
    pat.CONFIG_PATH = tmp / "clob_cfg.json"
    at = PolymarketAutoTrader()
    at.config = PolymarketClobConfig(private_key="0x" + "1" * 64, legacy_simulation_only=True, dry_run=False, sniper_live=True)
    at.last_geoblock_status = {"can_trade_api": True}
    client = MagicMock()
    client.place_market_order = AsyncMock(return_value=_accepted("5", "3.95"))
    client.cancel_order = AsyncMock()
    at.ensure_warm_v2_client = AsyncMock(return_value=client)  # type: ignore[method-assign]
    at._invalidate_warm_client = AsyncMock()  # type: ignore[method-assign]
    buy = await at.sniper_buy("tok", 5.0, 0.79, 0.82)
    kwargs = client.place_market_order.await_args.kwargs
    assert buy["filled"] and buy["shares"] == 5.0 and abs(buy["avg_price"] - 0.79) < 1e-9
    assert kwargs["side"] == "BUY" and kwargs["order_type"] == "FAK" and kwargs["max_price"] == 0.82 and kwargs["amount"] == 4.1
    client.place_market_order = AsyncMock(return_value=SimpleNamespace(ok=False, code="fak_not_filled", message="no match"))
    rej = await at.sniper_buy("tok", 5.0, 0.79, 0.82)
    assert not rej["filled"] and rej["status"] == "REJECTED"
    client.place_market_order = AsyncMock(side_effect=pat.RequestRejectedError("no orders found to match", status=400))
    at._invalidate_warm_client.reset_mock()
    live_rej = await at.sniper_buy("tok", 5.0, 0.79, 0.82)  # what the live exchange does on an unmatched FAK
    assert live_rej["status"] == "REJECTED" and at._invalidate_warm_client.await_count == 0, "warm client must survive a rejection"
    client.place_market_order = AsyncMock(side_effect=RuntimeError("socket reset"))
    at._strict_conditional_shares = AsyncMock(return_value=5.0)  # type: ignore[method-assign]
    lost = await at.sniper_buy("tok", 5.0, 0.79, 0.82)
    assert lost["filled"] and lost["status"] == "RECOVERED_FROM_BALANCE" and client.place_market_order.await_count == 1
    at._strict_conditional_shares = AsyncMock(return_value=0.0)  # type: ignore[method-assign]
    nofill = await at.sniper_buy("tok", 5.0, 0.79, 0.82)
    assert not nofill["filled"] and nofill["status"] == "ERROR"
    at._strict_conditional_shares = AsyncMock(side_effect=RuntimeError("closed"))  # type: ignore[method-assign]
    unknown = await at.sniper_buy("tok", 5.0, 0.79, 0.82)
    assert unknown["status"] == "UNKNOWN" and not unknown["filled"]
    assert (await at.sniper_buy("tok", 1.0, 0.5, 0.82))["status"] == "TOO_SMALL"
    return 1


async def _scenario_trader_sell_and_pin(tmp: Path) -> int:
    pat.CONFIG_PATH = tmp / "clob_cfg2.json"
    at = PolymarketAutoTrader()
    at.config = PolymarketClobConfig(private_key="0x" + "1" * 64, legacy_simulation_only=True, dry_run=False, sniper_live=True)
    at.last_geoblock_status = {"can_trade_api": True}
    client = MagicMock()
    client.place_market_order = AsyncMock(return_value=_accepted("4.95", "5"))
    at.ensure_warm_v2_client = AsyncMock(return_value=client)  # type: ignore[method-assign]
    at._strict_conditional_shares = AsyncMock(return_value=5.0)  # type: ignore[method-assign]
    sell = await at.sniper_sell("tok", 5.0, 0.96)
    kw = client.place_market_order.await_args.kwargs
    assert sell["filled"] and sell["shares"] == 5.0 and abs(sell["avg_price"] - 0.99) < 1e-9
    assert kw["side"] == "SELL" and kw["shares"] == 5.0 and kw["min_price"] == 0.96
    at._strict_conditional_shares = AsyncMock(return_value=0.0)  # type: ignore[method-assign]
    assert (await at.sniper_sell("tok", 0.05, 0.9))["status"] == "NO_BALANCE"
    at._strict_conditional_shares = AsyncMock(side_effect=RuntimeError("api down"))  # type: ignore[method-assign]
    client.place_market_order.reset_mock()
    err = await at.sniper_sell("tok", 5.0, 0.9)
    assert err["status"] == "ERROR" and client.place_market_order.await_count == 0, "never sell by ledger qty when balance unknown"
    at.save_config({"dry_run": False, "enabled": True})
    assert at.config.dry_run is True, "legacy strategy must stay pinned to simulation"
    return 1


def check_trader_execution(tmp: Path) -> int:
    """Sniper FAK buy/sell parsing, lost-response recovery, and legacy simulation pinning."""
    original = pat.CONFIG_PATH
    try:
        return asyncio.run(_scenario_trader_exec(tmp)) + asyncio.run(_scenario_trader_sell_and_pin(tmp)) - 1
    finally:
        pat.CONFIG_PATH = original


def check_notifier_mute(tmp: Path) -> int:
    """Legacy simulation events are muted; sniper events reach Telegram."""
    import engine.notifier as nt

    nt.CONFIG_PATH, nt.SIGNAL_LOG_PATH = tmp / "nc.json", tmp / "sig.json"
    nm = NotificationManager()
    nm.config = NotifyConfig(macos_desktop=False, macos_sound=False, telegram_notify_orders=True)
    nm.send_telegram_text = AsyncMock(return_value=True)  # type: ignore[method-assign]

    async def _run() -> None:
        legacy = await nm.dispatch_signal({"symbol": "BTCUSDT", "event_type": "SIGNAL_ENTRY", "message": "legacy"})
        assert legacy["delivery"] == {"muted": "legacy_simulation"} and nm.send_telegram_text.await_count == 0
        live = await nm.dispatch_signal({"symbol": "BTCUSDT", "event_type": "SNIPER_ENTRY", "message": "live", "source": "oracle_sniper"})
        assert live["delivery"].get("telegram") is True and nm.send_telegram_text.await_count == 1

    asyncio.run(_run())

    async def _system() -> None:
        sysev = await nm.dispatch_signal({"symbol": "SYSTEM", "event_type": "SYSTEM_ALERT", "message": "vpn down"})
        assert sysev["delivery"].get("muted") is None, "non-legacy events must not be swallowed"

    asyncio.run(_system())
    return 1


def check_notification_templates(tmp: Path) -> int:
    """Verify tactical HTML entry/exit/alert cards, plain status cards, escaping, and channel stripping."""
    summary = {"equity_u": 62.42, "cash_u": 62.42, "vault_u": 0.0, "initial_u": 43.0, "net_u": 19.42, "pct": 45.16,
               "wins": 36, "losses": 7, "trades": 43, "win_rate_pct": 83.7, "daily_pnl_usd": 4.2, "streak_type": "W", "streak_len": 5}
    base = {"seq": 43, "symbol": "BTCUSDT", "side": "DOWN", "window_ts": 1795033800, "entry_elapsed": 253.0,
            "shares": 7.0, "entry_price": 0.820, "cost_per_share": 0.8303, "cost_usd": 5.81, "p_true": 0.973,
            "z": 2.30, "net_ev": 0.143, "candidates": 1, "order_id": "0xf5d36f0460d266ae999", "entry_ts": 100.0}
    live_in = entry_message({**base, "paper_mode": False}, "", summary=summary)
    assert "[SNIPE EXECUTED // 實盤進場 #43]" in live_in and "SHORT" in live_in and "04:30~04:35" in live_in
    assert "$0.820" in live_in and "$0.8303" in live_in and "0xf5d36f0460d266ae" in live_in and "5 連勝" in live_in
    paper_in = entry_message({**base, "paper_mode": True, "paper_initial_u": 40, "fill_status": "PAPER_FILLED_1S"}, "", summary=summary)
    assert "[PAPER SNIPE // 🧪 模擬盤 $40U 進場 #43]" in paper_in and "PAPER_FILLED_1S" in paper_in
    tp_out = exit_message({**base, "paper_mode": False, "exit_reason": "EARLY_TAKE_PROFIT", "exit_price": 0.9893, "pnl_usd": 1.11, "won": True, "exit_ts": 142.0}, "", summary=summary)
    assert "[TARGET HIT // 獲利止盈 #43] +1.11 pUSD" in tp_out and "WIN [提前止盈]" in tp_out and "$0.9893" in tp_out
    settle_loss = exit_message({**base, "paper_mode": True, "exit_reason": "SETTLED_LOSS", "pnl_usd": -5.81, "won": False, "exit_ts": 300.0, "strike": 62748.0, "final_price": 62760.5}, "", summary={**summary, "streak_type": "L", "streak_len": 2})
    assert "[PAPER LOSS // 模擬結算負 #43] -5.81 pUSD" in settle_loss and "2 連敗" in settle_loss and "$62,748.00" in settle_loss
    sl_out = exit_message({**base, "paper_mode": False, "exit_reason": "EMERGENCY_STOP_LOSS", "exit_price": 0.65, "pnl_usd": -1.2, "won": False, "exit_ts": 280.0}, "", summary=summary)
    assert "[STOP TRIGGERED // 結構止損 #43] -1.20 pUSD" in sl_out and "STOP [緊急止損]" in sl_out
    guard = alert_message(["<test> & gate hit"], "", paper_mode=True, summary=summary)
    assert "[PAPER GUARD // 模擬風控]" in guard and "&lt;test&gt; &amp; gate hit" in guard
    plain = status_message("PAPER $40U", "🟢 運行中", "空倉", ["BTC | Z +1.50 | P(UP) 91% | 漲 0.72 / 跌 0.28"], "▸ 累計戰績：0W - 0L")
    assert "[SNIPER STATUS // PAPER $40U · 🟢 運行中]" in plain and "<b>" not in plain and "<code>" not in plain

    import engine.notifier as nt
    nt.CONFIG_PATH, nt.SIGNAL_LOG_PATH = tmp / "ntc.json", tmp / "nts.json"
    nm = NotificationManager()
    nm.config = NotifyConfig(macos_desktop=True, macos_sound=False, telegram_notify_orders=True)
    nm.send_telegram_text = AsyncMock(return_value=True)  # type: ignore[method-assign]
    nm._send_macos_notification = MagicMock(return_value=True)  # type: ignore[method-assign]
    asyncio.run(nm.dispatch_signal({"symbol": "BTCUSDT", "event_type": "SNIPER_ENTRY", "message": live_in, "parse_mode": "HTML", "source": "oracle_sniper"}))
    assert nm.send_telegram_text.call_args.kwargs.get("parse_mode") == "HTML"
    mac_body = nm._send_macos_notification.call_args.kwargs["body"]
    assert "<b>" not in mac_body and "<code>" not in mac_body and "[SNIPE EXECUTED // 實盤進場 #43]" in mac_body and strip_html_tags(live_in) == mac_body
    return 1


def main() -> None:
    """Entry point."""
    passed = 0
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        for fn in (check_fee_and_model, check_entry_gates, check_single_slot_ranking_and_sizing, check_exit_rules,
                   check_feed_helpers):
            passed += fn()
        for tfn in (check_ledger_guards, check_runner_scenarios, check_trader_execution, check_trader_hygiene,
                    check_notifier_mute, check_notification_templates):
            passed += tfn(tmp)
    if "--fast" not in sys.argv and LIVE_DB.exists():
        passed += check_replay_parity()
    print(json.dumps({"status": "ORACLE_SNIPER_CHECKS_PASSED", "groups_passed": passed}))


if __name__ == "__main__":
    main()
