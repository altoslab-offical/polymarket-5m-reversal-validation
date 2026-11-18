#!/usr/bin/env python3
"""End-to-end verification of Claude Round-3 Section 3 production safety invariants."""
from __future__ import annotations

import asyncio
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import time
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock

from engine.oracle_sniper import (
    TWAP_V2_FILTERED_CONFIG,
    OrderBookTop,
    SniperConfig,
    check_frozen_config_parity,
)
from engine.oracle_sniper_ledger import PAPER_43U_RISK_POLICY, PRODUCTION_RISK_POLICY, SniperLedger, local_date_str
from engine.oracle_sniper_runner import PAPER_43U_EXEC_POLICY, PRODUCTION_EXEC_POLICY, OracleSniperRunner, SniperExecutionPolicy


class StrictFakeChainlink:
    """Mock Chainlink RTDS feed exposing both spot and 60s TWAP freshness plus connection flag."""

    def __init__(self) -> None:
        self.connected = True
        self.spot_age = 0.3
        self.twap_age = 0.5
        self.prices = {"BTCUSDT": 100.048, "ETHUSDT": 100.050, "SOLUSDT": 100.049}
        self.opens = {"BTCUSDT": 100.0, "ETHUSDT": 100.0, "SOLUSDT": 100.0}
        self.ticks = {s: [(0.0, 100.0)] for s in self.prices}

    def get_latest_price(self, sym: str, max_age_sec: float = 20.0) -> tuple[Optional[float], float]:
        if self.spot_age > max_age_sec:
            return None, self.spot_age
        return self.prices[sym], self.spot_age

    def get_latest_twap(self, sym: str, max_age_sec: float = 20.0) -> tuple[Optional[float], float]:
        if self.twap_age > max_age_sec:
            return None, self.twap_age
        return self.prices[sym], self.twap_age

    def get_bucket_open_price(self, sym: str, _bucket: int) -> Optional[float]:
        return self.opens[sym]


def _make_strict_runner(
    tmp: Path, tag: str, cfg: SniperConfig = TWAP_V2_FILTERED_CONFIG, cash: float = 100.0
) -> tuple[OracleSniperRunner, MagicMock, StrictFakeChainlink, int]:
    at = MagicMock()
    at.live_wallet_state = {"cash_pusd": cash, "portfolio_pusd": 0.0, "updated_at_ms": int((time.time() + 400) * 1000)}
    at.last_geoblock_status = {"can_trade_api": True}
    at.sync_live_wallet_balance = AsyncMock(return_value=at.live_wallet_state)
    at.sniper_buy = AsyncMock(return_value={"filled": True, "shares": 5.0, "usd": 3.70, "avg_price": 0.74, "order_id": "o", "status": "FILLED", "error": None})
    at.sniper_sell = AsyncMock(return_value={"filled": True, "shares": 5.0, "usd": 4.95, "avg_price": 0.99, "order_id": "s", "status": "FILLED", "error": None})
    notifier = MagicMock()
    notifier.dispatch_signal = AsyncMock()
    cl = StrictFakeChainlink()
    pol = replace(PRODUCTION_EXEC_POLICY, frozen_config_path=Path(__file__).resolve().parent / "edge_validation" / "frozen_config.json")
    led = SniperLedger(tmp / f"{tag}.json", policy=PRODUCTION_RISK_POLICY)
    runner = OracleSniperRunner(at, cl, notifier, cfg=cfg, ledger=led, policy=pol)
    w = (int(time.time()) // 300) * 300 + 300
    now = float(w + 200)
    at.live_wallet_state["updated_at_ms"] = int(now * 1000)
    for sym, st in runner.hub.states.items():
        st.reset_for_window(w)
        st.up_token, st.down_token, st.slug, st.price_to_beat = f"{sym}-up", f"{sym}-dn", f"{sym}-5m-{w}", 100.0
        st.meta_checked_ts = st.prefetch_ts = 1e12
        for i in range(600):
            st.tracker.on_price(now - 600 + i, 100.0 + 0.048 * (i + 1) / 600.0)
        st.binance_price, st.binance_ts = cl.prices[sym], now
        st.coinbase_price, st.coinbase_ts = cl.prices[sym], now
        st.book = OrderBookTop(0.76, 0.78, 0.20, 0.22)
        st.up_local_ts = st.down_local_ts = now
    return runner, at, cl, w


def check_frozen_parity_and_concurrency(tmp: Path) -> int:
    """Verify frozen_config parity gate and simultaneous 3-coin (1 per coin) entry support."""
    frozen_p = Path(__file__).resolve().parent / "edge_validation" / "frozen_config.json"
    ok, diffs = check_frozen_config_parity(TWAP_V2_FILTERED_CONFIG, frozen_p, "v2_filtered")
    assert ok and not diffs, diffs

    # Drifted config MUST block all entries
    drifted = replace(TWAP_V2_FILTERED_CONFIG, min_net_edge=0.01)
    r_bad, at_bad, _cl_bad, w_bad = _make_strict_runner(tmp, "drift", cfg=drifted)
    asyncio.run(r_bad._tick(float(w_bad + 200)))
    assert at_bad.sniper_buy.await_count == 0 and r_bad.ledger.state["skips"].get("FROZEN_CONFIG_DRIFT", 0) >= 1

    # Valid config with all 3 coins eligible enters up to 3 simultaneous positions (1 per coin)
    r, at, _cl, w = _make_strict_runner(tmp, "conc3", cash=100.0)
    asyncio.run(r._tick(float(w + 200)))
    asyncio.run(asyncio.sleep(0))
    assert at.sniper_buy.await_count == 3, f"expected 3 concurrent orders (1/coin), got {at.sniper_buy.await_count}"
    assert set(r._positions.keys()) == {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
    # Re-tick same window must NOT enter a 4th position
    asyncio.run(r._tick(float(w + 210)))
    assert at.sniper_buy.await_count == 3
    return 1


def check_freshness_hard_blocks(tmp: Path) -> int:
    """Verify RTDS disconnected, Chainlink TWAP > 3s, and Binance > 2s hard-block entries (while fresh allows)."""
    # Control baseline: fresh feeds MUST actually enter (anti-vacuous-pass check)
    r0, at0, _cl0, w0 = _make_strict_runner(tmp, "fresh_ok")
    asyncio.run(r0._tick(float(w0 + 200)))
    assert at0.sniper_buy.await_count == 3, f"fresh baseline must enter 3 coins, got {at0.sniper_buy.await_count}"
    for idx, mode in enumerate(("rtds_down", "twap_stale", "binance_stale")):
        r, at, cl, w = _make_strict_runner(tmp, f"fresh_{idx}")
        now = float(w + 200)
        if mode == "rtds_down":
            cl.connected = False
        elif mode == "twap_stale":
            cl.twap_age = 3.2
        elif mode == "binance_stale":
            for st in r.hub.states.values():
                st.binance_ts = now - 2.3
        asyncio.run(r._tick(now))
        assert at.sniper_buy.await_count == 0, f"{mode} must block order!"
    return 1


def check_section3_circuit_breakers(tmp: Path) -> int:
    """Verify 3 consecutive losses, $10U daily stop, rolling 50 BE+2pp halt, and daily report."""
    led = SniperLedger(tmp / "sec3.json", policy=PRODUCTION_RISK_POLICY)
    led.ensure_baseline(50.0)
    now_ts = time.time()
    # 1) 3 consecutive losses trigger CONSEC_LOSS_3_HALT
    for i in range(3):
        led.close_trade({"id": f"c{i}", "symbol": "BTCUSDT", "shares": 5.0, "cost_per_share": 0.76, "sim_ask": 0.75, "entry_price": 0.76}, {"pnl_usd": -2.0, "exit_ts": now_ts + i})
    assert led.guard_reason() == "CONSEC_LOSS_3_HALT", led.guard_reason()

    # 2) New UTC day resets consec halt; -$10U daily realized triggers DAILY_LOSS_LIMIT
    led2 = SniperLedger(tmp / "sec3_daily.json", policy=PRODUCTION_RISK_POLICY)
    led2.ensure_baseline(200.0)
    led2.close_trade({"id": "d1", "symbol": "ETHUSDT", "shares": 10.0, "cost_per_share": 0.80, "sim_ask": 0.80, "entry_price": 0.80}, {"pnl_usd": -5.0, "exit_ts": now_ts})
    led2.close_trade({"id": "d2", "symbol": "SOLUSDT", "shares": 10.0, "cost_per_share": 0.80, "sim_ask": 0.80, "entry_price": 0.80}, {"pnl_usd": -5.0, "exit_ts": now_ts + 1})
    assert led2.guard_reason() == "DAILY_LOSS_LIMIT", led2.guard_reason()

    # 3) Rolling 50 trades: cost_per_share=0.78 -> BE=78% -> BE+2pp=80%; 39/50 = 78% < 80% triggers permanent halt
    led3 = SniperLedger(tmp / "sec3_r50.json", policy=PRODUCTION_RISK_POLICY)
    led3.ensure_baseline(500.0)
    for i in range(50):
        # Alternate wins/losses so consec losses stay <= 1; 39 wins total (78% < 80% floor)
        won = i not in (1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41)
        led3.close_trade({"id": f"r{i}", "symbol": "BTCUSDT", "shares": 5.0, "cost_per_share": 0.78, "sim_ask": 0.77, "entry_price": 0.775}, {"pnl_usd": 1.0 if won else -3.8, "exit_ts": now_ts + i})
    assert not led3.enabled and led3.guard_reason() == "ROLLING_50_BE_HALT", (led3.enabled, led3.guard_reason())
    led3.record_attempt({"ts": now_ts, "filled": True, "latency_ms": 145.0})
    led3.record_attempt({"ts": now_ts + 1, "filled": False, "latency_ms": 180.0})
    rep = led3.generate_daily_report(ref_42_mean=0.03, date_str=local_date_str(now_ts, utc=True))
    assert rep["n_closed"] == 50 and rep["miss_rate"] == 0.5 and rep["mean_slippage_vs_sim_cents"] == 0.5 and rep["median_latency_ms"] == 162.5, rep
    return 1


def check_paper_43u_simulation(tmp: Path) -> int:
    """Verify $43U paper simulation never hits real CLOB, simulates +1s miss/fill, updates $43 cash, and emits TG alerts."""
    r, at, cl, w = _make_strict_runner(tmp, "paper43")
    r.ledger = SniperLedger(tmp / "paper43_state.json", policy=PAPER_43U_RISK_POLICY)
    r.policy = replace(PAPER_43U_EXEC_POLICY, frozen_config_path=Path(__file__).resolve().parent / "edge_validation" / "frozen_config.json", paper_fill_delay_sec=0.0)
    r.ledger.ensure_baseline(43.0)
    now = float(w + 200)
    asyncio.run(r._tick(now))
    asyncio.run(asyncio.sleep(0))
    assert at.sniper_buy.await_count == 0, "paper_simulation must NEVER call real sniper_buy"
    assert len(r._positions) == 3, f"expected 3 paper positions on $43U? wait check pending cap"
    # Verify paper debit decreased cash cleanly from 43.0
    assert 30.0 <= float(r.ledger.state["paper_cash_u"]) < 34.0, r.ledger.state["paper_cash_u"]
    msg_types = [c.args[0]["event_type"] for c in r.notifier.dispatch_signal.await_args_list]
    assert "SNIPER_ENTRY" in msg_types and "🧪 模擬盤 $43U" in r.notifier.dispatch_signal.await_args_list[0].args[0]["message"]
    # Verify +1s price spike causes realistic PAPER_MISSED_1S instead of free fill
    r2, at2, _cl2, w2 = _make_strict_runner(tmp, "paper_miss")
    r2.ledger = SniperLedger(tmp / "paper_miss_state.json", policy=PAPER_43U_RISK_POLICY)
    r2.policy = replace(PAPER_43U_EXEC_POLICY, frozen_config_path=Path(__file__).resolve().parent / "edge_validation" / "frozen_config.json", paper_fill_delay_sec=0.0)
    for st in r2.hub.states.values():
        st.book = OrderBookTop(0.76, 0.78, 0.20, 0.22)
    # Wait: fill only works if ask <= limit_px; test _paper_buy directly when ask moves to 0.96!
    cand_mv = r2._positions
    r2.hub.states["BTCUSDT"].book = OrderBookTop(0.94, 0.96, 0.02, 0.04)
    res_miss = asyncio.run(r2._paper_buy(MagicMock(symbol="BTCUSDT", side="UP"), 5.0, 0.82))
    assert not res_miss["filled"] and res_miss["status"] == "PAPER_MISSED_1S", res_miss
    return 1


def check_telegram_channel_broadcast(tmp: Path) -> int:
    """Verify multi-channel TG broadcast, channel_post auto-bind, and ReplyKeyboardMarkup stripping on channels."""
    from unittest.mock import patch
    import engine.notifier as nmod
    from engine.notifier import NotificationManager
    from engine.scanner import LiveScanner

    orig_cfg = nmod.CONFIG_PATH
    nmod.CONFIG_PATH = tmp / "notify.json"
    try:
        nm = NotificationManager()
        nm.save_config({"telegram_enabled": True, "telegram_bot_token": "dummy", "telegram_chat_id": "111111", "telegram_extra_chat_ids": []})
        sent_payloads: list[dict[str, Any]] = []

        async def fake_post(_url: str, payload: dict[str, Any], proxy: Any = None, timeout_s: int = 6) -> int:
            sent_payloads.append(payload)
            return 200

        with patch.object(NotificationManager, "_post_json", side_effect=fake_post):
            sc = LiveScanner.__new__(LiveScanner)
            sc.notifier = nm
            sc.sniper = None
            sc._tg_update_offset = 0
            asyncio.run(sc._handle_tg_update({
                "update_id": 1,
                "channel_post": {"chat": {"id": -1001234567890, "type": "channel", "title": "MySignalChan"}, "text": "hi"},
            }))
            assert "-1001234567890" in nm.config.telegram_extra_chat_ids
            sent_payloads.clear()
            ok = asyncio.run(nm.send_telegram_text("SNIPER_TEST", reply_markup={"keyboard": [[{"text": "/sniper"}]]}))
            assert ok and len(sent_payloads) == 2
            by_id = {str(p["chat_id"]): p for p in sent_payloads}
            assert "reply_markup" in by_id["111111"]
            assert "reply_markup" not in by_id["-1001234567890"], "channel MUST NOT get reply_markup"
            asyncio.run(sc._handle_tg_update({
                "update_id": 2,
                "my_chat_member": {"chat": {"id": -1001234567890}, "new_chat_member": {"status": "left"}},
            }))
            assert "-1001234567890" not in nm.config.telegram_extra_chat_ids
    finally:
        nmod.CONFIG_PATH = orig_cfg
    return 1


def main() -> None:
    """Run all Section 3 production safety checks."""
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        groups = (
            check_frozen_parity_and_concurrency(tmp)
            + check_freshness_hard_blocks(tmp)
            + check_section3_circuit_breakers(tmp)
            + check_paper_43u_simulation(tmp)
            + check_telegram_channel_broadcast(tmp)
        )
    print(json.dumps({"status": "SECTION3_PRODUCTION_GUARDS_PASSED", "groups_passed": groups}))


if __name__ == "__main__":
    main()
