"""Adversarial parity and resilience verification suite for V11.0 live execution."""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
import json
import tempfile
import time
from pathlib import Path
from typing import Iterator
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd

import engine.master_strategy as ms
import engine.notifier as nt
import engine.polymarket_auto_trader as pat
import engine.review_manager as rm
import engine.scanner as sc_mod
from engine.master_strategy import MASTER_REPORT_PATH, LivePaperTrader, MasterStrategySpec
from engine.notifier import NotificationManager, NotifyConfig
from engine.polymarket_auto_trader import PolymarketAutoTrader, PolymarketClobConfig
from engine.scanner import LiveScanner
from engine.v8_strategy import V8_CONFIG
from engine.watchdog import SystemWatchdog


@contextmanager
def _isolated_paths(tmp_dir: Path, tag: str) -> Iterator[None]:
    """Redirect persistent file paths into an isolated temp directory during tests."""
    with (
        patch.object(ms, "PAPER_ACCOUNT_PATH", tmp_dir / f"paper_{tag}.json"),
        patch.object(pat, "CONFIG_PATH", tmp_dir / f"clob_{tag}.json"),
        patch.object(nt, "CONFIG_PATH", tmp_dir / f"notify_{tag}.json"),
        patch.object(nt, "SIGNAL_LOG_PATH", tmp_dir / f"sig_{tag}.json"),
        patch.object(rm, "PAPER_ACCOUNT_PATH", tmp_dir / f"paper_{tag}.json"),
        patch.object(rm, "REVIEWS_PATH", tmp_dir / f"rev_{tag}.json"),
        patch.object(rm, "FLASH_REVIEW_LOG_PATH", tmp_dir / f"flash_{tag}.json"),
    ):
        yield


def _build_scanner(tmp_dir: Path, dry_run: bool = True) -> tuple[LivePaperTrader, LiveScanner]:
    """Construct an isolated LivePaperTrader + LiveScanner pair with silent mocked Telegram."""
    pt = LivePaperTrader(MasterStrategySpec(initial_capital_u=40.62))
    at = PolymarketAutoTrader()
    at.config = PolymarketClobConfig(enabled=True, dry_run=dry_run, private_key="0x" + "1" * 64)
    at.live_wallet_state = {"cash_pusd": 40.62, "total_equity_pusd": 40.62, "updated_at_ms": int(time.time() * 1000)}
    at.last_geoblock_status = {"can_trade_api": True, "country": "HK"}
    nm = NotificationManager()
    nm.config = NotifyConfig(telegram_notify_orders=False, macos_desktop=False, macos_sound=False)
    nm.send_telegram_text = AsyncMock()
    nm.dispatch_signal = AsyncMock()
    return pt, LiveScanner(nm, pt, at)


def _make_dfs(base_bucket_ts: int, n: int = 80) -> dict[str, pd.DataFrame]:
    """Create minimal 3-coin 5m indicator DataFrames aligned to `base_bucket_ts`."""
    start_ms = (base_bucket_ts - 70 * 300) * 1000
    dfs: dict[str, pd.DataFrame] = {}
    for sym in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        dfs[sym] = pd.DataFrame({
            "open_time": [start_ms + i * 300_000 for i in range(n)],
            "open": [100.0] * n,
            "high": [100.5] * n,
            "low": [99.5] * n,
            "close": [100.0] * n,
            "volume": [1000.0] * n,
            "is_green": [False] * n,
            "body_pct": [0.0] * n,
            "rsi_2": [50.0] * n,
            "rsi_6": [50.0] * n,
            "rsi_14": [50.0] * n,
            "streak_down": [0] * n,
            "streak_up": [0] * n,
            "streak_ret_pct": [0.0] * n,
            "bb_pct_b": [0.5] * n,
            "vol_ratio": [1.0] * n,
            "lower_wick_ratio": [0.1] * n,
            "upper_wick_ratio": [0.1] * n,
            "taker_buy_ratio": [0.5] * n,
            "atr_pct": [0.20] * n,
            "ret_12bars_pct": [0.0] * n,
            "ret_48bars_pct": [0.0] * n,
            "close_pos_in_bar": [0.5] * n,
            "body_accel": [1.0] * n,
            "cvd_3": [0.0] * n,
        })
    return dfs


def _inject_long_term_signal(dfs: dict[str, pd.DataFrame], sym: str, idx: int) -> None:
    """Inject a valid V11.0 E_RSI_TERM LONG_BOUNCE signal at `idx` on `sym`."""
    for s in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        dfs[s].loc[idx, ["rsi_2", "rsi_6", "rsi_14", "streak_down"]] = [4.0, 19.0, 31.0, 3]
    dfs[sym].loc[
        idx,
        [
            "rsi_2", "rsi_6", "rsi_14", "streak_down", "streak_ret_pct", "bb_pct_b",
            "vol_ratio", "lower_wick_ratio", "taker_buy_ratio", "body_accel", "close_pos_in_bar", "cvd_3",
        ],
    ] = [3.2, 17.5, 31.0, 3, -0.45, 0.08, 1.4, 0.25, 0.39, 1.25, 0.20, -0.05]


def test_config_and_report_parity(tmp_dir: Path) -> int:
    """Verify Hold-to-100c config parity, 2-year report benchmarks, and 100% ReviewManager parity."""
    assert V8_CONFIG.bounce_protect_enabled is False
    assert V8_CONFIG.surge_take_profit_enabled is False
    rep = json.loads(MASTER_REPORT_PATH.read_text(encoding="utf-8"))
    ov = rep["overall"]
    assert ov["profitable_months"] == "24/24"
    assert ov["hit_200u_months"] == "24/24"
    with _isolated_paths(tmp_dir, "cfg"):
        pt, sc = _build_scanner(tmp_dir, dry_run=False)
        mgr = rm.ReviewManager(notifier=sc.notifier, auto_trader=sc.auto_trader, paper_trader=pt, scanner=sc)
        par = mgr.verify_strategy_parity()
        assert par["all_parity_checks_passed"] is True, f"Parity warnings: {par['drift_warnings']}"
        assert par["parity_score_pct"] == 100.0
    return 1


async def _test_multistep_cross_coin_replay(tmp_dir: Path) -> int:
    """Replay Step-1 loss on ETHUSDT followed by cross-coin Step-2 win on BTCUSDT at 100c."""
    base_b = (int(time.time()) // 300) * 300
    with _isolated_paths(tmp_dir, "replay"):
        pt, sc = _build_scanner(tmp_dir, dry_run=True)
        sc.chainlink_feed.get_latest_price = lambda sym, max_age_sec=20.0: (2500.0 if sym == "ETHUSDT" else 90000.0, 0.5)  # type: ignore[method-assign]
        sc.chainlink_feed.get_bucket_open_price = lambda sym, _b: 2500.0 if sym == "ETHUSDT" else 90000.0  # type: ignore[method-assign]
        sc.chainlink_feed.get_60s_twap = lambda sym, end_ts_ms=0, min_ticks=5: (2500.0 if sym == "ETHUSDT" else 90000.0, 20)  # type: ignore[method-assign]
        sc.auto_trader.discover_live_5m_market = AsyncMock(  # type: ignore[method-assign]
            side_effect=lambda sym, bucket_offset=0, target_bucket_ts=None, fast_cached_only=False: {
                "symbol": sym,
                "slug": f"{sym.lower()[:3]}-updown-5m-{target_bucket_ts or base_b}",
                "bucket_ts": target_bucket_ts or base_b,
                "condition_id": f"0x{sym.lower()}",
                "up_token_id": f"tok-up-{sym}",
                "down_token_id": f"tok-dn-{sym}",
                "up_price_cents": 50.0,
                "down_price_cents": 50.0,
                "price_to_beat": 2500.0 if sym == "ETHUSDT" else 90000.0,
            }
        )
        sc.auto_trader.get_live_token_best_bid_ask = AsyncMock(return_value=(0.49, 0.50))  # type: ignore[method-assign]
        dfs = _make_dfs(base_b)
        _inject_long_term_signal(dfs, "ETHUSDT", 70)
        await sc._handle_synchronized_close(dfs, 70, target_bucket_ts=base_b)
        assert sc.active_sequences["ETHUSDT"] is not None
        assert sc.active_sequences["ETHUSDT"]["step"] == 1

        sc.auto_trader.query_live_market_resolution = AsyncMock(  # type: ignore[method-assign]
            return_value={"resolved": True, "polymarket_won": False, "price_to_beat": 2500.0, "final_price": 2498.0}
        )
        await sc._handle_synchronized_close(dfs, 71, target_bucket_ts=base_b + 300)
        ps2 = pt.state.get("pending_step2")
        assert isinstance(ps2, dict) and ps2["step"] == 2 and ps2["stake_usd"] == 3.5 and ps2["shares"] == 7.0
        assert sc.risk_controller.can_open_step1("BTCUSDT") is False
        assert sc.risk_controller.can_open_rescue("BTCUSDT") is True
        assert sc.risk_controller.can_open_rescue("SOLUSDT") is True

        _inject_long_term_signal(dfs, "BTCUSDT", 72)
        await sc._handle_synchronized_close(dfs, 72, target_bucket_ts=base_b + 600)
        assert pt.state.get("pending_step2") is None
        assert sc.active_sequences["BTCUSDT"] is not None and sc.active_sequences["BTCUSDT"]["step"] == 2

        sc.auto_trader.query_live_market_resolution = AsyncMock(  # type: ignore[method-assign]
            return_value={"resolved": True, "polymarket_won": True, "price_to_beat": 90000.0, "final_price": 90050.0}
        )
        await sc._handle_synchronized_close(dfs, 73, target_bucket_ts=base_b + 900)
        assert pt.state["step2_wins"] == 1
        assert pt.state["total_equity_u"] == 41.62
        assert pt.state["pending_step2"] is None
        assert pt.state["trade_log"][0]["pnl_u"] == 1.0
        assert all(ln.status == "IDLE" for ln in sc.risk_controller.lanes.values())
        dispatched = [c.args[0] for c in sc.notifier.dispatch_signal.call_args_list]
        assert [ev["event_type"] for ev in dispatched] == [
            "SIGNAL_ENTRY",
            "STEP_LOSS_RESCUE",
            "MARTINGALE_ADD",
            "SEQUENCE_WIN",
        ]
        assert all("• 總覽：權益" in ev["message"] and "• 戰績：" in ev["message"] for ev in dispatched)
    return 1


async def _test_deferred_oracle_settlement(tmp_dir: Path) -> int:
    """Verify deferred settlement detaches stale sequences and blocks new entries until resolved."""
    base_b = (int(time.time()) // 300) * 300
    orig_sleep = asyncio.sleep

    async def _fast_sleep(_sec: float) -> None:
        await orig_sleep(0)

    with (
        _isolated_paths(tmp_dir, "defer"),
        patch.object(sc_mod.asyncio, "sleep", new=_fast_sleep),
    ):
        pt, sc = _build_scanner(tmp_dir, dry_run=True)
        slug_d = f"eth-updown-5m-{base_b}"
        seq_obj = {
            "symbol": "ETHUSDT", "direction": "LONG_BOUNCE", "polymarket_side": "BUY UP",
            "step": 1, "stake_usd": 2.5, "shares": 5.0, "s1_shares_base": 5.0, "prior_cost_u": 0.0,
            "prior_shares_lost": 0.0, "planned_stakes": [2.5, 3.5, 7.0, 14.0], "s1_already_deducted": False,
            "slug": slug_d, "bucket_ts": base_b, "strike_twap_60s": 2500.0, "entry_price": 2500.0,
            "entry_rsi_6": 17.5, "initial_streak": 3, "entry_share_cents": 50.0, "entry_verified_matched": True,
        }
        sc.active_sequences["ETHUSDT"] = seq_obj
        sc.auto_trader.active_clob_positions["ETHUSDT"] = {
            "symbol": "ETHUSDT", "slug": slug_d, "entry_matched": True, "entry_filled_shares": 5.0, "is_dry_run": True,
        }
        sc.chainlink_feed.get_60s_twap = lambda _s, end_ts_ms=0, min_ticks=12: (None, 0)  # type: ignore[method-assign]
        sc.auto_trader.query_live_market_resolution = AsyncMock(  # type: ignore[method-assign]
            side_effect=[
                {"resolved": False},
                {"resolved": True, "polymarket_won": True, "price_to_beat": 2500.0, "final_price": 2505.0},
            ]
        )
        sc.auto_trader.execute_5m_entry = AsyncMock()  # type: ignore[method-assign]
        dfs = _make_dfs(base_b)
        _inject_long_term_signal(dfs, "BTCUSDT", 70)
        await sc._handle_synchronized_close(dfs, 70)
        assert isinstance(sc._pending_oracle_settlements, dict) and slug_d in sc._pending_oracle_settlements
        assert sc.active_sequences["ETHUSDT"] is None and pt.state["active_position"] is None
        await sc._handle_synchronized_close(dfs, 71, target_bucket_ts=base_b + 300)
        assert sc.auto_trader.execute_5m_entry.await_count == 0
        await sc._pending_oracle_settlements[slug_d]
        assert slug_d not in sc._pending_oracle_settlements
        assert pt.state["step1_wins"] == 1 and pt.state["total_equity_u"] == 43.12
    return 1


async def _test_late_surge_and_rescue_cash_guards(tmp_dir: Path) -> int:
    """Verify execute_late_surge_sell fill/floor guards and SKIPPED_INSUFFICIENT_RESCUE_CASH protection."""
    with _isolated_paths(tmp_dir, "surge"):
        _, sc = _build_scanner(tmp_dir, dry_run=False)
        sc.auto_trader.check_and_sync_position_fill = AsyncMock(return_value={"entry_filled_shares": 0.0, "entry_matched": False})  # type: ignore[method-assign]
        sc.auto_trader.active_clob_positions["ETHUSDT"] = {"symbol": "ETHUSDT", "token_id": "tok-1", "entry_price": 0.50}
        res_unfilled = await sc.auto_trader.execute_late_surge_sell("ETHUSDT", prior_cost_u=0.0)
        assert res_unfilled is not None and res_unfilled["status"] == "SKIPPED_ENTRY_UNFILLED"

        sc.auto_trader.check_and_sync_position_fill = AsyncMock(return_value={"entry_filled_shares": 8.0, "entry_matched": True})  # type: ignore[method-assign]
        sc.auto_trader.get_live_token_best_bid_ask = AsyncMock(return_value=(0.94, 0.96))  # type: ignore[method-assign]
        sc.auto_trader.active_clob_positions["ETHUSDT"] = {
            "symbol": "ETHUSDT", "token_id": "tok-1", "entry_price": 0.60, "effective_cost_u": 4.80,
        }
        res_floor = await sc.auto_trader.execute_late_surge_sell("ETHUSDT", min_bid=0.93, limit_sell_price=0.95, prior_cost_u=2.90)
        assert res_floor is not None and res_floor["status"] == "SKIPPED_BELOW_MARTINGALE_PROFIT_FLOOR"

        sc.auto_trader.live_wallet_state = {"cash_pusd": 3.10, "total_equity_pusd": 3.10}
        sc.auto_trader.auto_redeem_winning_positions = AsyncMock(return_value={"claimed_pusd": 0.0})  # type: ignore[method-assign]
        sc.auto_trader.sync_live_wallet_balance = AsyncMock(return_value={"cash_pusd": 3.10, "total_equity_pusd": 3.10})  # type: ignore[method-assign]
        mkt = {"symbol": "ETHUSDT", "slug": "eth-updown-5m-cash", "condition_id": "0xc", "up_token_id": "t1", "down_token_id": "t2"}
        rec_cash = await sc.auto_trader.execute_5m_entry("ETHUSDT", "LONG_BOUNCE", 3.50, step=2, prior_cost_u=2.90, s1_shares_base=5.0, market=mkt)
        assert rec_cash["status"] == "SKIPPED_INSUFFICIENT_RESCUE_CASH" and rec_cash["shares"] == 0.0
    return 1


async def _test_atomic_pending_step2_on_error(tmp_dir: Path) -> int:
    """Verify pending_step2 remains intact if execute_5m_entry raises mid-attempt."""
    base_b = (int(time.time()) // 300) * 300
    with _isolated_paths(tmp_dir, "atomic"):
        pt, sc = _build_scanner(tmp_dir, dry_run=False)
        ps2 = {
            "step": 2, "symbol": "ETHUSDT", "stake_usd": 3.50, "shares": 7.0,
            "s1_shares_base": 5.0, "prior_cost_u": 2.50, "prior_shares_lost": 5.0,
            "planned_stakes": [2.50, 3.50, 7.00, 14.00], "s1_already_deducted": False,
        }
        pt.state["pending_step2"] = dict(ps2)
        sc.risk_controller.lanes["ETHUSDT"].status = "PENDING_RESCUE"
        sc.chainlink_feed.get_latest_price = lambda _s, max_age_sec=20.0: (2500.0, 0.5)  # type: ignore[method-assign]
        sc.chainlink_feed.get_bucket_open_price = lambda _s, _b: 2500.0  # type: ignore[method-assign]
        sc.auto_trader.discover_live_5m_market = AsyncMock(  # type: ignore[method-assign]
            return_value={"symbol": "ETHUSDT", "slug": f"eth-updown-5m-{base_b}", "bucket_ts": base_b, "up_token_id": "t1", "down_token_id": "t2", "price_to_beat": 2500.0}
        )
        sc.auto_trader.execute_5m_entry = AsyncMock(side_effect=RuntimeError("network timeout"))  # type: ignore[method-assign]
        dfs = _make_dfs(base_b)
        _inject_long_term_signal(dfs, "ETHUSDT", 70)
        await sc._handle_synchronized_close(dfs, 70, target_bucket_ts=base_b)
        assert pt.state["pending_step2"] is not None
        assert pt.state["pending_step2"]["step"] == 2 and pt.state["pending_step2"]["stake_usd"] == 3.50
    return 1


async def _test_startup_recovery_and_watchdog(tmp_dir: Path) -> int:
    """Verify LiveScanner startup CLOB hydration, Forward-Start bucket guard, and Watchdog RTDS heal."""
    now_b = (int(time.time()) // 300) * 300
    with _isolated_paths(tmp_dir, "recover"):
        pt_seed = LivePaperTrader(MasterStrategySpec(initial_capital_u=40.62))
        pt_seed.state["active_position"] = {
            "symbol": "ETHUSDT", "direction": "LONG_BOUNCE", "polymarket_side": "BUY UP",
            "step": 1, "stake_usd": 2.55, "shares": 5.0, "s1_shares_base": 5.0, "prior_cost_u": 0.0,
            "slug": f"eth-updown-5m-{now_b + 300}", "token_id": "tok-rec", "condition_id": "0xrec",
            "bucket_ts": now_b + 300, "started_ts": int(time.time() * 1000), "entry_price": 2500.0,
            "entry_rsi_6": 17.5, "initial_streak": 3, "entry_share_cents": 51.0,
            "entry_order_id": "ord-rec", "entry_verified_matched": True, "entry_status": "LIVE_MATCHED",
        }
        pt_seed.save()

        _, sc = _build_scanner(tmp_dir, dry_run=False)
        assert sc.active_sequences["ETHUSDT"] is not None
        assert sc.auto_trader.active_clob_positions["ETHUSDT"]["token_id"] == "tok-rec"
        assert sc.auto_trader.active_clob_positions["ETHUSDT"]["entry_order_id"] == "ord-rec"
        assert sc.risk_controller.lanes["ETHUSDT"].status == "ACTIVE_STEP1"

        dfs = _make_dfs(now_b)
        dfs["ETHUSDT"].loc[70, "open_time"] = now_b * 1000
        await sc._handle_synchronized_close(dfs, 70)
        assert sc.active_sequences["ETHUSDT"] is not None, "Forward-start bucket must NOT settle on earlier bar"

        sc.running = True
        sc._last_scan_time = time.time()
        sc._task = asyncio.create_task(asyncio.sleep(10.0))
        sc.chainlink_feed.last_tick_recv_ms = int((time.time() - 60.0) * 1000)
        sc.chainlink_feed.stop = MagicMock()  # type: ignore[method-assign]
        sc.chainlink_feed.start = MagicMock()  # type: ignore[method-assign]
        wd = SystemWatchdog(scanner=sc)
        await wd._run_health_check()
        assert sc.chainlink_feed.stop.call_count == 1 and sc.chainlink_feed.start.call_count == 1
        sc._task.cancel()
        sc.running = False
    return 1


async def _test_direct_vpn_routing_and_geoblock_failover(tmp_dir: Path) -> int:
    """Verify Surfshark direct VPN priority (`DIRECT`) and automatic emergency failover when direct IP is IT-blocked."""
    with _isolated_paths(tmp_dir, "surfshark"):
        at = PolymarketAutoTrader()
        at.config = PolymarketClobConfig(enabled=True, dry_run=False, private_key="0x" + "1" * 64, proxy_url="")
        assert at._is_direct_mode() is True

        at.direct_geoblock_info = {"can_trade_api": True, "blocked": False, "country": "HK"}
        assert at._get_failover_proxy_chain()[0] == "DIRECT"

        at.direct_geoblock_info = {"can_trade_api": False, "blocked": True, "country": "IT"}
        chain_blocked = at._get_failover_proxy_chain()
        assert chain_blocked[0] != "DIRECT" and "DIRECT" in chain_blocked

        async def _fake_probe(proxy: str | None) -> dict[str, object]:
            if proxy is None:
                return {"blocked": True, "can_trade_api": False, "api_only_unrestricted": False, "ip": "192.228.119.22", "country": "IT", "region": "25", "proxy_used": "Surfshark / System VPN Direct"}
            return {"blocked": False, "can_trade_api": True, "api_only_unrestricted": False, "ip": "43.99.60.244", "country": "HK", "region": "", "proxy_used": str(proxy)}

        at._probe_geoblock_with_proxy = AsyncMock(side_effect=_fake_probe)  # type: ignore[method-assign]
        geo = await at.check_geoblock_status()
        assert at.config.proxy_url == "", "Direct mode proxy_url must never be overwritten by fallback proxy"
        assert geo["direct_blocked"] is True and geo["country"] == "IT" and geo["can_trade_api"] is True
    return 1


def main() -> None:
    """Execute all V11.0 live-parity, resilience, and recovery checks."""
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        passed = 0
        passed += test_config_and_report_parity(tmp)
        passed += asyncio.run(_test_multistep_cross_coin_replay(tmp))
        passed += asyncio.run(_test_deferred_oracle_settlement(tmp))
        passed += asyncio.run(_test_late_surge_and_rescue_cash_guards(tmp))
        passed += asyncio.run(_test_atomic_pending_step2_on_error(tmp))
        passed += asyncio.run(_test_startup_recovery_and_watchdog(tmp))
        passed += asyncio.run(_test_direct_vpn_routing_and_geoblock_failover(tmp))
    print(json.dumps({"status": "ALL_V11_LIVE_PARITY_CHECKS_PASSED", "groups_passed": passed, "version": V8_CONFIG.version}))


if __name__ == "__main__":
    main()
