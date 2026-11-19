"""End-to-end verification suite for V11.0 Multi-Period RSI & 4-Step Guaranteed-Profit Martingale."""
from __future__ import annotations

import asyncio
from decimal import Decimal
import json
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pandas as pd
from polymarket import AcceptedOrder

import engine.master_strategy as ms
import engine.notifier as nt
import engine.polymarket_auto_trader as pat
import engine.review_manager as rm
from engine.master_strategy import LivePaperTrader, MasterStrategySpec, compute_step_stakes
from engine.notifier import NotificationManager, NotifyConfig
from engine.polymarket_auto_trader import PolymarketAutoTrader, PolymarketClobConfig
from engine.scanner import LiveScanner
from engine.watchdog import SystemWatchdog
from engine.v8_strategy import (
    V8_CONFIG,
    compute_v8_tier_and_stakes,
    evaluate_v8_signal_at_index,
)


def _make_base_dfs(n: int = 80) -> dict[str, pd.DataFrame]:
    base_ts = 1794600000000
    dfs: dict[str, pd.DataFrame] = {}
    for sym in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        df = pd.DataFrame({
            "open_time": [base_ts + i * 300_000 for i in range(n)],
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
        dfs[sym] = df
    return dfs


def test_rsi_term_and_wyckoff_gates() -> int:
    """Verify Multi-Period RSI Term-Structure (E_RSI_TERM) triggers and sluggish RSI(2) rejections."""
    idx = 60

    dfs_long = _make_base_dfs()
    for sym in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        dfs_long[sym].loc[idx, ["rsi_2", "rsi_6", "rsi_14", "streak_down"]] = [4.0, 19.0, 31.0, 3]
    dfs_long["ETHUSDT"].loc[
        idx,
        ["rsi_2", "rsi_6", "rsi_14", "streak_down", "streak_ret_pct", "bb_pct_b", "vol_ratio", "lower_wick_ratio", "taker_buy_ratio", "body_accel", "close_pos_in_bar", "cvd_3"],
    ] = [3.2, 17.5, 31.0, 3, -0.45, 0.08, 1.4, 0.25, 0.39, 1.25, 0.20, -0.05]
    cand_long = evaluate_v8_signal_at_index(dfs_long, idx, V8_CONFIG)
    assert cand_long is not None, "Expected Long E_RSI_TERM candidate"
    assert cand_long["direction"] == "LONG_BOUNCE"
    assert "E_RSI_TERM" in cand_long["trigger_branch"], f"Expected E_RSI_TERM in {cand_long['trigger_branch']}"
    assert cand_long["is_tier_s"] is True

    dfs_short = _make_base_dfs()
    for sym in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        dfs_short[sym].loc[idx, ["rsi_2", "rsi_6", "rsi_14", "streak_up"]] = [96.0, 82.0, 68.0, 3]
    dfs_short["SOLUSDT"].loc[
        idx,
        ["rsi_2", "rsi_6", "rsi_14", "streak_up", "streak_ret_pct", "bb_pct_b", "vol_ratio", "upper_wick_ratio", "taker_buy_ratio", "body_accel", "close_pos_in_bar", "cvd_3"],
    ] = [97.8, 84.0, 68.0, 3, 0.50, 0.94, 1.5, 0.22, 0.62, 1.30, 0.85, 0.06]
    cand_short = evaluate_v8_signal_at_index(dfs_short, idx, V8_CONFIG)
    assert cand_short is not None, "Expected Short E_RSI_TERM candidate"
    assert cand_short["direction"] == "SHORT_PULLBACK"
    assert "E_RSI_TERM" in cand_short["trigger_branch"], f"Expected E_RSI_TERM in {cand_short['trigger_branch']}"

    dfs_slug = _make_base_dfs()
    for sym in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        dfs_slug[sym].loc[idx, ["rsi_2", "rsi_6", "rsi_14", "streak_down"]] = [22.0, 21.0, 28.0, 3]
    dfs_slug["BTCUSDT"].loc[
        idx,
        ["rsi_2", "rsi_6", "rsi_14", "streak_down", "bb_pct_b", "body_accel", "close_pos_in_bar", "taker_buy_ratio", "lower_wick_ratio"],
    ] = [20.0, 21.0, 26.0, 3, 0.35, 0.80, 0.25, 0.42, 0.15]
    assert evaluate_v8_signal_at_index(dfs_slug, idx, V8_CONFIG) is None, "Sluggish RSI(2)>14.5 single branch must reject"

    dfs_wyck = _make_base_dfs()
    for sym in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        dfs_wyck[sym].loc[idx, ["rsi_6", "streak_down"]] = [20.0, 1]
    dfs_wyck["ETHUSDT"].loc[
        idx,
        ["rsi_2", "rsi_6", "rsi_14", "streak_down", "bb_pct_b", "body_accel", "taker_buy_ratio", "ret_12bars_pct", "cvd_3"],
    ] = [8.0, 19.0, 35.0, 1, 0.06, 1.45, 0.40, -0.55, 0.01]
    cand_wyck = evaluate_v8_signal_at_index(dfs_wyck, idx, V8_CONFIG)
    assert cand_wyck is not None and "D_WYCKOFF_2LEG" in cand_wyck["trigger_branch"]

    dfs_wyck["ETHUSDT"].loc[idx, "rsi_2"] = 15.0
    assert evaluate_v8_signal_at_index(dfs_wyck, idx, V8_CONFIG) is None, "Wyckoff RSI(2)>10 must reject"
    return 4


def test_guaranteed_profit_4step_martingale_ladder() -> int:
    """Verify V12.0 capital gates (25/65/128U), Option A <$55U 5-share lock, trailing vault, and positive step profits."""
    assert V8_CONFIG.step2_min_active_u == 25.0
    assert V8_CONFIG.phase1_flat_equity_ceiling == 55.0
    assert V8_CONFIG.phase1_tier_s_min_active_u == 55.0
    assert V8_CONFIG.micro_equity_tier_s_only_u == 0.0

    t_micro_std = compute_v8_tier_and_stakes(20.0, is_tier_s=False)
    t_micro_s = compute_v8_tier_and_stakes(20.0, is_tier_s=True)
    assert t_micro_std["micro_tier_s_only_blocked"] is False
    assert t_micro_s["micro_tier_s_only_blocked"] is False
    assert t_micro_std["effective_max_steps"] == 1
    assert t_micro_s["effective_max_steps"] == 1

    v_trail, _, _ = ms.compute_vault_locked_u(108.0, 0.0, active_peak_u=205.0)
    assert v_trail == 75.0, f"Expected $75 trailing vault lock, got {v_trail}"

    # Option A: < $55U (e.g. $27.67U, $33.67U, $40.62U, $54.90U) must strictly lock to 5 shares even on TIER_S+
    for eq_small in (27.67, 33.67, 40.62, 54.90):
        t_sm = compute_v8_tier_and_stakes(eq_small, is_tier_s=True, is_tier_s_plus=True)
        assert t_sm["shares"] == [5.0, 7.0, 14.0, 28.0], f"Option A violated at {eq_small}: {t_sm['shares']}"
        assert t_sm["stakes"] == [2.5, 3.5, 7.0, 14.0], f"Option A stakes violated at {eq_small}: {t_sm['stakes']}"
        assert t_sm["effective_max_steps"] == 2

    t40 = compute_v8_tier_and_stakes(40.62, is_tier_s=False)
    assert t40["shares"] == [5.0, 7.0, 14.0, 28.0], f"Unexpected shares at 40.62: {t40['shares']}"
    assert t40["stakes"] == [2.5, 3.5, 7.0, 14.0], f"Unexpected stakes at 40.62: {t40['stakes']}"
    assert t40["effective_max_steps"] == 2

    t80 = compute_v8_tier_and_stakes(80.0, is_tier_s=False)
    assert t80["effective_max_steps"] == 3, f"Expected 3 steps at $80, got {t80['effective_max_steps']}"
    t180 = compute_v8_tier_and_stakes(180.0, is_tier_s=False)
    assert t180["effective_max_steps"] == 4, f"Expected 4 steps at $180, got {t180['effective_max_steps']}"

    for eq, t_info in [(40.62, t40), (80.0, t80), (180.0, t180)]:
        shs = t_info["shares"]
        stks = t_info["stakes"]
        for k in range(1, 5):
            cum_sh_prev = sum(shs[: k - 1])
            cum_cost = sum(stks[:k])
            net_win_pnl = shs[k - 1] * 1.0 - cum_cost
            assert shs[k - 1] > cum_sh_prev, f"eq={eq} step={k}: sh {shs[k-1]} <= cum_prev {cum_sh_prev}"
            assert net_win_pnl > 0.0, f"eq={eq} step={k}: net_win_pnl {net_win_pnl} <= 0"

    assert compute_step_stakes(40.62, MasterStrategySpec()) == [2.5, 3.5, 7.0, 14.0]
    return 4


def _build_isolated_scanner(tmp_dir: Path) -> tuple[LivePaperTrader, LiveScanner]:
    pt = LivePaperTrader(MasterStrategySpec(initial_capital_u=40.62))
    at = PolymarketAutoTrader()
    at.config = PolymarketClobConfig(enabled=True, dry_run=True)
    nm = NotificationManager()
    nm.config = NotifyConfig(telegram_notify_orders=False, macos_desktop=False, macos_sound=False)
    nm.send_telegram_text = AsyncMock()
    nm.dispatch_signal = AsyncMock()
    return pt, LiveScanner(nm, pt, at)


async def _check_scanner_40u_step2_and_bust(tmp_dir: Path) -> int:
    with (
        patch.object(ms, "PAPER_ACCOUNT_PATH", tmp_dir / "paper40.json"),
        patch.object(pat, "CONFIG_PATH", tmp_dir / "clob40.json"),
        patch.object(nt, "CONFIG_PATH", tmp_dir / "notify40.json"),
        patch.object(nt, "SIGNAL_LOG_PATH", tmp_dir / "sig40.json"),
    ):
        pt, sc = _build_isolated_scanner(tmp_dir)
        seq1 = {
            "symbol": "ETHUSDT",
            "direction": "LONG_BOUNCE",
            "entry_price": 2500.0,
            "entry_rsi_6": 17.5,
            "initial_streak": 3,
            "step": 1,
            "stake_usd": 2.50,
            "shares": 5.0,
            "s1_shares_base": 5.0,
            "prior_cost_u": 0.0,
            "prior_shares_lost": 0.0,
            "planned_stakes": [2.50, 3.50, 7.00, 14.00],
            "s1_already_deducted": False,
            "trigger_branch": "E_RSI_TERM",
            "tier": "TIER_S",
            "cvd_3": -0.05,
        }
        ctx1 = {
            "symbol": "ETHUSDT",
            "direction": "LONG_BOUNCE",
            "cur_step": 1,
            "max_steps": 4,
            "bucket_ts": 1794640000,
            "strike_twap": 2500.0,
            "ending_twap": 2498.0,
            "provisional_oracle": False,
            "from_oracle_waiter": True,
        }
        res1 = await sc._handle_martingale_loss_progression(seq1, ctx1)
        assert res1["advanced"] is True
        ps2 = pt.state["pending_step2"]
        assert ps2["step"] == 2 and ps2["stake_usd"] == 3.50 and ps2["shares"] == 7.0
        assert ps2["prior_cost_u"] == 2.50 and ps2["prior_shares_lost"] == 5.0
        assert sc.risk_controller.lanes["ETHUSDT"].status == "PENDING_RESCUE"

        seq2 = dict(seq1, step=2, stake_usd=ps2["stake_usd"], shares=ps2["shares"], prior_cost_u=ps2["prior_cost_u"], prior_shares_lost=ps2["prior_shares_lost"])
        _, sh2, _, _, tot_cost2 = sc._compute_seq_costs(seq2, 2)
        assert tot_cost2 == 6.00 and round(sh2 * 1.0 - tot_cost2, 4) == 1.00

        ctx2 = dict(ctx1, cur_step=2, bucket_ts=1794640300, ending_twap=2497.0, from_oracle_waiter=False)
        res2 = await sc._handle_martingale_loss_progression(seq2, ctx2)
        assert res2["advanced"] is False
        assert pt.state["pending_step2"] is None
        assert res2["record"]["pnl_u"] == -6.00
        assert pt.state["total_equity_u"] == 34.62
    return 3


async def _check_scanner_180u_4step_ladder(tmp_dir: Path) -> int:
    with (
        patch.object(ms, "PAPER_ACCOUNT_PATH", tmp_dir / "paper180.json"),
        patch.object(pat, "CONFIG_PATH", tmp_dir / "clob180.json"),
        patch.object(nt, "CONFIG_PATH", tmp_dir / "notify180.json"),
        patch.object(nt, "SIGNAL_LOG_PATH", tmp_dir / "sig180.json"),
    ):
        pt, sc = _build_isolated_scanner(tmp_dir)
        pt.reset_account(initial_u=180.0, target_u=200000.0)
        plan1 = sc._plan_next_martingale_step(
            {"stake_usd": 9.0, "shares": 18.0, "s1_shares_base": 18.0, "prior_cost_u": 0.0, "prior_shares_lost": 0.0, "planned_stakes": [9.0, 11.5, 23.0, 46.0]},
            cur_step=1, max_steps=4, post_loss_avail_u=171.0,
        )
        assert plan1["can_advance"] and plan1["next_step"] == 2 and plan1["next_shares"] == 23.0
        plan2 = sc._plan_next_martingale_step(
            {"stake_usd": 11.5, "shares": 23.0, "s1_shares_base": 18.0, "prior_cost_u": 9.0, "prior_shares_lost": 18.0, "planned_stakes": [9.0, 11.5, 23.0, 46.0]},
            cur_step=2, max_steps=4, post_loss_avail_u=159.5,
        )
        assert plan2["can_advance"] and plan2["next_step"] == 3 and plan2["next_shares"] == 46.0
        plan3 = sc._plan_next_martingale_step(
            {"stake_usd": 23.0, "shares": 46.0, "s1_shares_base": 18.0, "prior_cost_u": 20.5, "prior_shares_lost": 41.0, "planned_stakes": [9.0, 11.5, 23.0, 46.0]},
            cur_step=3, max_steps=4, post_loss_avail_u=136.5,
        )
        assert plan3["can_advance"] and plan3["next_step"] == 4 and plan3["next_shares"] == 92.0
    return 1


def test_review_manager_v11_parity(tmp_dir: Path) -> int:
    """Verify V11.0 5-dimension strategy parity audit passes at 100.0% with live real-money state."""
    with (
        patch.object(ms, "PAPER_ACCOUNT_PATH", tmp_dir / "paper_rm.json"),
        patch.object(pat, "CONFIG_PATH", tmp_dir / "clob_rm.json"),
        patch.object(rm, "PAPER_ACCOUNT_PATH", tmp_dir / "paper_rm.json"),
        patch.object(rm, "REVIEWS_PATH", tmp_dir / "reviews_rm.json"),
        patch.object(rm, "FLASH_REVIEW_LOG_PATH", tmp_dir / "flash_rm.json"),
        patch.object(nt, "CONFIG_PATH", tmp_dir / "notify_rm.json"),
    ):
        pt = LivePaperTrader(MasterStrategySpec(initial_capital_u=40.62))
        at = PolymarketAutoTrader()
        at.config = PolymarketClobConfig(enabled=True, dry_run=False)
        at.live_wallet_state = {"cash_pusd": 40.62, "total_equity_pusd": 40.62}
        at.last_geoblock_status = {"can_trade_api": True, "country": "HK"}
        nm = NotificationManager()
        nm.config = NotifyConfig(telegram_notify_orders=True, macos_desktop=False, macos_sound=False)
        nm.send_telegram_text = AsyncMock()
        mgr = rm.ReviewManager(auto_trader=at, paper_trader=pt, notifier=nm)

        pt.record_completed_sequence(
            symbol="BTCUSDT",
            direction="LONG_BOUNCE",
            entry_price=90000.0,
            resolve_price=90050.0,
            rsi_6=18.2,
            streak=3,
            won_at_step=2,
            exit_cents=100.0,
            exit_type="100¢_EXPIRATION",
            actual_pnl_u=1.00,
            trigger_branch="E_RSI_TERM+B_BACC",
            tier="TIER_S",
            cvd_3=-0.03,
            oracle_source="POLYMARKET_GAMMA_OFFICIAL",
            actual_stake_u=3.50,
            actual_entry_cents=50.0,
        )
        rep = mgr.generate_review(force=True)
        par = rep["strategy_parity"]
        assert par["strategy_version"] == "V11.0_ULTIMATE_RSI_MARTINGALE"
        assert par["all_parity_checks_passed"] is True, f"Parity failed: {par['drift_warnings']}"
        assert par["parity_score_pct"] == 100.0
        assert rep["metrics"]["step2_wins"] == 1
        assert par["branch_counts"]["E_RSI_TERM"] == 1
        assert rep["confidence"]["is_live_real_money"] is True
        mgr.state["next_review_ts_ms"] = 0
        periodic_rep = asyncio.run(mgr.check_and_run_periodic_review())
        assert periodic_rep is not None
        assert nm.send_telegram_text.call_count == 0
    return 1


def _attach_tilted_mock_client(sc: LiveScanner, ask_px: str, bid_px: str) -> tuple[SimpleNamespace, list[dict[str, object]]]:
    """Attach a mock CLOB client returning a skewed orderbook and recording order calls."""
    calls: list[dict[str, object]] = []
    tilted_book = SimpleNamespace(
        asks=[SimpleNamespace(price=ask_px, size="150.0")],
        bids=[SimpleNamespace(price=bid_px, size="120.0")],
    )

    async def _fake_place(**kwargs: object) -> AcceptedOrder:
        calls.append(dict(kwargs))
        sh_dec, px_dec, is_po = Decimal(str(kwargs["size"])), Decimal(str(kwargs["price"])), bool(kwargs.get("post_only", False))
        return AcceptedOrder(
            order_id="ord-tilt-52",
            status="live" if is_po else "matched",
            making_amount=Decimal("0") if is_po else (sh_dec * px_dec),
            taking_amount=Decimal("0") if is_po else sh_dec,
            trade_ids=() if is_po else ("tr-1",),
            transactions_hashes=() if is_po else ("0x01",),
        )

    fake_client = SimpleNamespace(
        get_order_book=AsyncMock(return_value=tilted_book),
        place_limit_order=AsyncMock(side_effect=_fake_place),
        cancel_order=AsyncMock(),
        get_order=AsyncMock(return_value=SimpleNamespace(size_matched="0", status="live")),
        get_balance_allowance=AsyncMock(return_value=SimpleNamespace(balance="0")),
    )
    sc.auto_trader._warm_client = fake_client  # type: ignore[assignment]
    sc.auto_trader.ensure_warm_v2_client = AsyncMock(return_value=fake_client)  # type: ignore[method-assign]
    sc.auto_trader.sync_live_wallet_balance = AsyncMock(return_value={"cash_pusd": 40.62, "total_equity_pusd": 40.62})  # type: ignore[method-assign]
    return tilted_book, calls


async def _check_tilted_book_adaptive_fill(tmp_dir: Path) -> int:
    """Verify tilted-book Stage-2 Marketable Limit execution (52c & 57c) and Step-2 rescue recovery."""
    with (
        patch.object(ms, "PAPER_ACCOUNT_PATH", tmp_dir / "paper_tilt.json"),
        patch.object(pat, "CONFIG_PATH", tmp_dir / "clob_tilt.json"),
        patch.object(nt, "CONFIG_PATH", tmp_dir / "notify_tilt.json"),
        patch.object(nt, "SIGNAL_LOG_PATH", tmp_dir / "sig_tilt.json"),
    ):
        _, sc = _build_isolated_scanner(tmp_dir)
        sc.auto_trader.config = PolymarketClobConfig(enabled=True, dry_run=False, private_key="0x" + "1" * 64)
        sc.auto_trader.live_wallet_state = {"cash_pusd": 40.62, "total_equity_pusd": 40.62}
        tilted_book, calls = _attach_tilted_mock_client(sc, "0.52", "0.51")

        mkt = {
            "symbol": "ETHUSDT", "slug": "eth-updown-5m-1794641400", "bucket_ts": 1794641400,
            "condition_id": "0xcond", "up_token_id": "tok-up", "down_token_id": "tok-dn", "price_to_beat": 2500.0,
        }
        seq_tpl = {
            "symbol": "ETHUSDT", "direction": "LONG_BOUNCE", "step": 1, "stake_usd": 2.60, "shares": 5.0,
            "s1_shares_base": 5.0, "prior_cost_u": 0.0, "prior_shares_lost": 0.0,
            "planned_stakes": [2.50, 3.50, 7.00, 14.00], "s1_already_deducted": False,
        }
        rec1 = await sc.auto_trader.execute_5m_entry("ETHUSDT", "LONG_BOUNCE", 2.50, step=1, market=mkt)
        assert rec1["status"] == "LIVE_MATCHED" and rec1["price"] == 0.52 and rec1["shares"] == 5.0
        assert len(calls) == 1 and calls[0]["post_only"] is False and calls[0]["price"] == "0.52"
        plan2 = sc._plan_next_martingale_step(seq_tpl, cur_step=1, max_steps=4, post_loss_avail_u=38.02)
        assert plan2["can_advance"] is True and plan2["next_step"] == 2
        assert plan2["next_shares"] * 1.0 - (2.60 + plan2["next_stake"]) > 0.0

        rec2 = await sc.auto_trader.execute_5m_entry("ETHUSDT", "LONG_BOUNCE", plan2["next_stake"], step=2, prior_cost_u=2.60, s1_shares_base=5.0, market=mkt)
        assert rec2["status"] == "LIVE_MATCHED" and rec2["price"] == 0.52
        assert rec2["shares"] * 1.0 - (2.60 + rec2["effective_cost_u"]) > 0.0

        tilted_book.asks = [SimpleNamespace(price="0.57", size="150.0")]
        tilted_book.bids = [SimpleNamespace(price="0.56", size="120.0")]
        rec57 = await sc.auto_trader.execute_5m_entry("SOLUSDT", "LONG_BOUNCE", 2.50, step=1, market=mkt)
        assert rec57["status"] == "LIVE_MATCHED" and rec57["price"] == 0.57 and rec57["shares"] == 5.0
        plan57 = sc._plan_next_martingale_step(dict(seq_tpl, symbol="SOLUSDT", stake_usd=rec57["effective_cost_u"]), cur_step=1, max_steps=4, post_loss_avail_u=37.77)
        rec57_s2 = await sc.auto_trader.execute_5m_entry("SOLUSDT", "LONG_BOUNCE", plan57["next_stake"], step=2, prior_cost_u=rec57["effective_cost_u"], s1_shares_base=5.0, market=mkt)
        assert rec57_s2["status"] == "LIVE_MATCHED" and rec57_s2["price"] == 0.57
        assert rec57_s2["shares"] * 1.0 - (rec57["effective_cost_u"] + rec57_s2["effective_cost_u"]) > 0.0
        await asyncio.sleep(0)
    return 3


async def _check_lane_deadlock_and_redeem_guard(tmp_dir: Path) -> int:
    """Verify unfilled-cancel lane unlock and pre-redemption live wallet sync protection."""
    with (
        patch.object(ms, "PAPER_ACCOUNT_PATH", tmp_dir / "paper_guard.json"),
        patch.object(pat, "CONFIG_PATH", tmp_dir / "clob_guard.json"),
        patch.object(nt, "CONFIG_PATH", tmp_dir / "notify_guard.json"),
        patch.object(nt, "SIGNAL_LOG_PATH", tmp_dir / "sig_guard.json"),
    ):
        pt, sc = _build_isolated_scanner(tmp_dir)
        sc.risk_controller.lanes["ETHUSDT"].status = "ACTIVE_STEP1"
        sc._restore_lanes_after_unfilled_entry("ETHUSDT", {"symbol": "ETHUSDT", "step": 1, "direction": "LONG_BOUNCE"})
        assert sc.risk_controller.lanes["ETHUSDT"].status == "IDLE"
        assert sc.risk_controller.can_open_step1("BTCUSDT") is True

        sc.risk_controller.lanes["ETHUSDT"].status = "ACTIVE_RESCUE"
        sc._restore_lanes_after_unfilled_entry(
            "ETHUSDT",
            {"symbol": "ETHUSDT", "step": 2, "prior_cost_u": 2.55, "direction": "LONG_BOUNCE"},
        )
        assert sc.risk_controller.lanes["ETHUSDT"].status == "PENDING_RESCUE"
        assert sc.risk_controller.can_open_rescue("ETHUSDT") is True
        assert sc.risk_controller.can_open_step1("BTCUSDT") is False

        pt.state["total_equity_u"] = 43.12
        pt.state["active_equity_u"] = 43.12
        pt.state["pending_redeem_until_ms"] = int(time.time() * 1000) + 300_000
        pt.sync_with_live_wallet(38.07, 38.07, is_dry_run=False)
        assert pt.state["total_equity_u"] == 43.12
        assert pt.state["live_cash_pusd"] == 38.07

        pt.sync_with_live_wallet(43.12, 43.12, is_dry_run=False)
        assert pt.state["total_equity_u"] == 43.12
        assert pt.state["pending_redeem_until_ms"] == 0

        # Verify Skewed CLOB Book (>58c ask) rests LIVE_LIVE Post-Only Maker @ 56c instead of overpaying
        sc.auto_trader.config = PolymarketClobConfig(enabled=True, dry_run=False, private_key="0x" + "1" * 64)
        sc.auto_trader.live_wallet_state = {"cash_pusd": 40.62, "total_equity_pusd": 40.62}
        tilted_60, calls_60 = _attach_tilted_mock_client(sc, "0.60", "0.55")
        tilted_60.asks = [SimpleNamespace(price="0.60", size="150.0")]
        mkt60 = {
            "symbol": "ETHUSDT", "slug": "eth-updown-5m-1794665100", "bucket_ts": 1794665100,
            "condition_id": "0xcond60", "up_token_id": "tok-up-60", "down_token_id": "tok-dn-60", "price_to_beat": 1870.0,
        }
        rec60 = await sc.auto_trader.execute_5m_entry("ETHUSDT", "LONG_BOUNCE", 3.00, step=1, market=mkt60, target_bucket_ts=1794665100)
        assert rec60["status"] == "LIVE_LIVE", f"Expected LIVE_LIVE resting Maker on 60c ask, got {rec60}"
        assert rec60["price"] == 0.56 and len(calls_60) == 1 and calls_60[0]["post_only"] is True
        await asyncio.sleep(0)
    return 3


async def _check_watchdog_recovery_and_climax_fallback(tmp_dir: Path) -> int:
    """Verify Watchdog restart_loop recovery, pre-boundary non-fill retry, and stabilized climax fallback."""
    dfs_c = _make_base_dfs(80)
    eth_c = dfs_c["ETHUSDT"]
    eth_c.loc[70, ["streak_down", "rsi_2", "rsi_6", "lower_wick_ratio", "bb_pct_b", "ret_12bars_pct", "ret_48bars_pct", "taker_buy_ratio", "body_accel"]] = [
        4, 0.6, 10.86, 0.40, 0.08, -1.36, -3.97, 0.47, 0.41,
    ]
    sig_eth = evaluate_v8_signal_at_index(dfs_c, 70, V8_CONFIG)
    assert sig_eth is not None and sig_eth["symbol"] == "ETHUSDT" and sig_eth["direction"] == "LONG_BOUNCE"

    btc_s = _make_base_dfs(80)
    btc_s["BTCUSDT"].loc[70, ["streak_up", "rsi_2", "rsi_6", "upper_wick_ratio", "bb_pct_b", "ret_12bars_pct", "ret_48bars_pct", "taker_buy_ratio", "body_accel", "cvd_3"]] = [
        6, 99.3, 82.21, 0.26, 1.25, 0.91, -0.99, 0.59, 1.49, 0.22,
    ]
    sig_btc_s = evaluate_v8_signal_at_index(btc_s, 70, V8_CONFIG)
    assert sig_btc_s is not None and sig_btc_s["symbol"] == "BTCUSDT" and sig_btc_s["direction"] == "SHORT_PULLBACK"

    with (
        patch.object(ms, "PAPER_ACCOUNT_PATH", tmp_dir / "paper_wd.json"),
        patch.object(pat, "CONFIG_PATH", tmp_dir / "clob_wd.json"),
        patch.object(nt, "CONFIG_PATH", tmp_dir / "notify_wd.json"),
        patch.object(nt, "SIGNAL_LOG_PATH", tmp_dir / "sig_wd.json"),
    ):
        _, sc = _build_isolated_scanner(tmp_dir)
        sc.running = True
        hung_task = asyncio.create_task(asyncio.sleep(60.0))
        sc._task = hung_task
        sc._last_scan_time = time.time() - 90.0
        sc.scan_once = AsyncMock()  # type: ignore[method-assign]
        sc.run_loop = AsyncMock()  # type: ignore[method-assign]
        wd = SystemWatchdog(scanner=sc)
        await wd._run_health_check()
        await asyncio.sleep(0)
        assert hung_task.cancelled() and sc._task is not None and sc._task is not hung_task
        if sc._task and not sc._task.done():
            sc._task.cancel()
        sc.running = False

        # Pre-boundary non-fill must NOT set _last_processed_closed_ts to forming_open_ts_scan
        _, sc2 = _build_isolated_scanner(tmp_dir)
        now_ms = int(time.time() * 1000)
        base_bucket = ((now_ms // 300_000) - 200) * 300_000
        rows_df = _make_base_dfs(200)
        for s in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
            d = rows_df[s]
            d["open_time"] = [base_bucket + j * 300_000 for j in range(200)]
            d["close_time"] = [base_bucket + j * 300_000 + 299_999 for j in range(200)]
        rows_df["ETHUSDT"].loc[199, ["streak_down", "rsi_2", "rsi_6", "lower_wick_ratio", "bb_pct_b", "ret_12bars_pct", "ret_48bars_pct", "taker_buy_ratio", "body_accel"]] = [
            4, 0.6, 10.86, 0.40, 0.08, -1.36, -3.97, 0.47, 0.41,
        ]
        prev_ts = int(rows_df["BTCUSDT"]["open_time"].iloc[-2])
        forming_ts = int(rows_df["BTCUSDT"]["open_time"].iloc[-1])
        sc2._last_processed_closed_ts = prev_ts
        sc2.fetch_recent_klines = AsyncMock(side_effect=lambda _sess, sym, limit=200: rows_df[sym])  # type: ignore[method-assign]
        sc2._handle_synchronized_close = AsyncMock()  # type: ignore[method-assign]
        fake_now = (int(rows_df["BTCUSDT"]["close_time"].iloc[-1]) - 1500) / 1000.0
        with patch("engine.scanner.time.time", return_value=fake_now):
            await sc2.scan_once()
        assert sc2._handle_synchronized_close.await_count == 1
        assert sc2._last_processed_closed_ts == prev_ts and sc2._last_processed_closed_ts < forming_ts
    return 3


def test_high_atr_climax_signal_capture() -> int:
    """Verify High-ATR Climax Reversal signals trigger on SU=5 climax and filter decelerated bacc<1.05."""
    dfs_hi = _make_base_dfs(80)
    for sym in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        dfs_hi[sym].loc[70, ["rsi_6", "streak_up", "atr_pct", "ret_12bars_pct", "ret_48bars_pct"]] = [86.0, 4, 0.90, 2.2, 3.5]
    dfs_hi["ETHUSDT"].loc[
        70,
        ["streak_up", "rsi_6", "rsi_2", "bb_pct_b", "upper_wick_ratio", "lower_wick_ratio", "body_accel", "taker_buy_ratio", "atr_pct", "ret_12bars_pct", "ret_48bars_pct"],
    ] = [5, 87.9, 99.56, 1.21, 0.22, 0.12, 1.14, 0.531, 1.01, 2.5, 4.0]
    sig_s5 = evaluate_v8_signal_at_index(dfs_hi, 70, V8_CONFIG)
    assert sig_s5 is not None and sig_s5["symbol"] == "ETHUSDT" and sig_s5["direction"] == "SHORT_PULLBACK"
    dfs_hi["ETHUSDT"].loc[70, "body_accel"] = 0.87
    assert evaluate_v8_signal_at_index(dfs_hi, 70, V8_CONFIG) is None, "Decelerated su=5 bacc<1.05 must reject"
    return 2


async def _check_skewed_maker_preserve(tmp_dir: Path) -> int:
    """Verify Stage-1 Maker stays LIVE_LIVE on skewed tick-up and fill sync updates shares and cost."""
    with (
        patch.object(ms, "PAPER_ACCOUNT_PATH", tmp_dir / "paper_skew.json"),
        patch.object(pat, "CONFIG_PATH", tmp_dir / "clob_skew.json"),
        patch.object(nt, "CONFIG_PATH", tmp_dir / "notify_skew.json"),
        patch.object(nt, "SIGNAL_LOG_PATH", tmp_dir / "sig_skew.json"),
    ):
        _, sc_a = _build_isolated_scanner(tmp_dir)
        sc_a.auto_trader.config = PolymarketClobConfig(enabled=True, dry_run=False, private_key="0x" + "1" * 64, maker_wait_sec=0.1)
        sc_a.auto_trader.live_wallet_state = {"cash_pusd": 40.62, "total_equity_pusd": 40.62}
        book_56 = SimpleNamespace(asks=[SimpleNamespace(price="0.56", size="100.0")], bids=[SimpleNamespace(price="0.54", size="10.0")])
        book_60 = SimpleNamespace(asks=[SimpleNamespace(price="0.60", size="100.0")], bids=[SimpleNamespace(price="0.56", size="10.0")])
        calls_a: list[dict[str, object]] = []

        async def _place_a(**kwargs: object) -> AcceptedOrder:
            calls_a.append(dict(kwargs))
            return AcceptedOrder(order_id="ord-live-55", status="live", making_amount=Decimal("0"), taking_amount=Decimal("0"), trade_ids=(), transactions_hashes=())

        client_a = SimpleNamespace(
            get_order_book=AsyncMock(side_effect=[book_56, book_60]),
            place_limit_order=AsyncMock(side_effect=_place_a),
            cancel_order=AsyncMock(),
            get_order=AsyncMock(return_value=SimpleNamespace(size_matched="0", status="live")),
            get_balance_allowance=AsyncMock(return_value=SimpleNamespace(balance="0")),
        )
        sc_a.auto_trader._warm_client = client_a  # type: ignore[assignment]
        sc_a.auto_trader.ensure_warm_v2_client = AsyncMock(return_value=client_a)  # type: ignore[method-assign]
        sc_a.auto_trader.sync_live_wallet_balance = AsyncMock(return_value={"cash_pusd": 40.62, "total_equity_pusd": 40.62})  # type: ignore[method-assign]
        mkt_a = {"symbol": "ETHUSDT", "slug": "eth-updown-5m-skew", "bucket_ts": 1794665400, "condition_id": "0xskew", "up_token_id": "tok-up-skew", "down_token_id": "tok-dn-skew"}
        rec_a = await sc_a.auto_trader.execute_5m_entry("ETHUSDT", "LONG_BOUNCE", 2.75, step=1, market=mkt_a, target_bucket_ts=1794665400)
        assert rec_a["status"] == "LIVE_LIVE" and rec_a["price"] == 0.55, f"Expected LIVE_LIVE @ 0.55, got {rec_a}"
        assert client_a.cancel_order.await_count == 0, "Skewed ask > max_taker_px must NOT cancel resting Stage-1 Maker"
        assert len(calls_a) == 1 and calls_a[0]["post_only"] is True
        client_a.get_order = AsyncMock(return_value=SimpleNamespace(size_matched="5.0", status="matched"))
        sync_res = await sc_a.auto_trader.check_and_sync_position_fill("ETHUSDT")
        assert sync_res["entry_matched"] is True and sync_res["entry_filled_shares"] == 5.0
        pos_a = sc_a.auto_trader.active_clob_positions["ETHUSDT"]
        assert pos_a["entry_status"] == "LIVE_MATCHED" and pos_a["shares"] == 5.0 and pos_a["effective_cost_u"] == 2.75
        await asyncio.sleep(0)
    return 3


async def _check_oracle_open_anchor_no_twap_false_block(tmp_dir: Path) -> int:
    """Verify pre-entry ref_oracle_p prioritizes cl_open over lagging 60s cl_twap_open when official_ptb is None."""
    with (
        patch.object(ms, "PAPER_ACCOUNT_PATH", tmp_dir / "paper_or.json"),
        patch.object(pat, "CONFIG_PATH", tmp_dir / "clob_or.json"),
        patch.object(nt, "CONFIG_PATH", tmp_dir / "notify_or.json"),
        patch.object(nt, "SIGNAL_LOG_PATH", tmp_dir / "sig_or.json"),
    ):
        _, sc_b = _build_isolated_scanner(tmp_dir)
        sc_b.auto_trader.config = PolymarketClobConfig(enabled=True, dry_run=False, private_key="0x" + "1" * 64)
        sc_b.auto_trader.live_wallet_state = {"cash_pusd": 40.62, "total_equity_pusd": 40.62, "updated_at_ms": int(time.time() * 1000)}
        now_b = (int(time.time()) // 300) * 300
        sc_b.auto_trader.discover_live_5m_market = AsyncMock(  # type: ignore[method-assign]
            return_value={"symbol": "ETHUSDT", "slug": f"eth-updown-5m-{now_b}", "bucket_ts": now_b, "condition_id": "0xcl", "up_token_id": "tok-up-cl", "down_token_id": "tok-dn-cl", "up_price_cents": 50.0, "down_price_cents": 50.0}
        )
        sc_b.auto_trader.execute_5m_entry = AsyncMock(  # type: ignore[method-assign]
            return_value={"status": "LIVE_MATCHED", "shares": 5.0, "effective_cost_u": 2.55, "price": 0.51, "price_cents": 51.0, "slug": f"eth-updown-5m-{now_b}"}
        )
        sc_b.chainlink_feed.get_latest_price = lambda _sym, max_age_sec=20.0: (2493.75, 0.5)  # type: ignore[method-assign]
        sc_b.chainlink_feed.get_bucket_open_price = lambda _sym, _bt: 2493.75  # type: ignore[method-assign]
        sc_b.chainlink_feed.get_60s_twap = lambda _sym, end_ts_ms=0, min_ticks=5: (2500.0, 20)  # type: ignore[method-assign]
        dfs_b = _make_base_dfs(80)
        for s in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
            dfs_b[s].loc[70, ["rsi_2", "rsi_6", "rsi_14", "streak_down"]] = [4.0, 19.0, 31.0, 3]
        dfs_b["ETHUSDT"].loc[70, ["rsi_2", "rsi_6", "rsi_14", "streak_down", "streak_ret_pct", "bb_pct_b", "vol_ratio", "lower_wick_ratio", "taker_buy_ratio", "body_accel", "close_pos_in_bar", "cvd_3"]] = [
            3.2, 17.5, 31.0, 3, -0.45, 0.08, 1.4, 0.25, 0.39, 1.25, 0.20, -0.05,
        ]
        await sc_b._handle_synchronized_close(dfs_b, 70, target_bucket_ts=now_b)
        assert sc_b.auto_trader.execute_5m_entry.await_count == 1, "Lagging cl_twap_open must NOT false-block when cl_open aligns"
        await asyncio.sleep(0)
    return 1


def main() -> None:
    """Run all V11.0 E2E signal, Martingale sizing, scanner rescue, and parity verification checks."""
    total = 0
    total += test_rsi_term_and_wyckoff_gates()
    total += test_guaranteed_profit_4step_martingale_ladder()
    total += test_high_atr_climax_signal_capture()
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        total += asyncio.run(_check_scanner_40u_step2_and_bust(tmp))
        total += asyncio.run(_check_scanner_180u_4step_ladder(tmp))
        total += asyncio.run(_check_tilted_book_adaptive_fill(tmp))
        total += asyncio.run(_check_lane_deadlock_and_redeem_guard(tmp))
        total += asyncio.run(_check_watchdog_recovery_and_climax_fallback(tmp))
        total += asyncio.run(_check_skewed_maker_preserve(tmp))
        total += asyncio.run(_check_oracle_open_anchor_no_twap_false_block(tmp))
        total += test_review_manager_v11_parity(tmp)
    print(json.dumps({"status": "ALL_V11_E2E_CHECKS_PASSED", "assertions_groups_passed": total, "version": V8_CONFIG.version}))


if __name__ == "__main__":
    main()
