#!/usr/bin/env python3
"""End-to-end verification of pure signal_core, parity >=99%, warmup, depth-FAK, freshness, SL dual-track, and single_slot."""
from __future__ import annotations

import asyncio
from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import tempfile
import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pandas as pd

from edge_validation.live_recorder_42 import LiveRecorder42, _aggregate_shadow_parity, _sim_one_cfg, analyze_recording, init_db
from edge_validation.parity_replay import run_parity_replay
from engine.chainlink_oracle import ChainlinkRTDSFeed
from engine.oracle_sniper import TWAP_V2_FILTERED_CONFIG, OrderBookTop, estimate_twap_fair_value, taker_fee_per_share
from engine.oracle_sniper_feeds import SniperMarketHub
from engine.oracle_sniper_ledger import PAPER_40U_RISK_POLICY, PRODUCTION_RISK_POLICY, SniperLedger
from engine.oracle_sniper_runner import PAPER_40U_EXEC_POLICY, PRODUCTION_EXEC_POLICY, OracleSniperRunner
from engine.signal_core import (
    Bn1sMicroTracker,
    MarketInputs,
    evaluate as evaluate_signal,
    simulate_fak_buy,
    simulate_fak_sell,
)
from verify_section3_guards import StrictFakeChainlink, _make_strict_runner


def _warm_runner(tmp: Path, tag: str, cb_scale: float = 1.0) -> tuple[OracleSniperRunner, int]:
    r, _at, cl, w = _make_strict_runner(tmp, f"core_{tag}")
    now = float(w + 200)
    for sym, st in r.hub.states.items():
        st.coinbase_price = cl.prices[sym] * cb_scale
        st.coinbase_ts = now
    return r, w


def check_1_determinism_and_coinbase_isolation(tmp: Path) -> int:
    """Verify signal_core determinism and zero Coinbase contamination in Runner sigma/z/gap."""
    hist = tuple(100.0 + 0.048 * ((i + 1) / 1800.0) for i in range(1800))
    ages = {"cl_twap": 0.4, "cl_spot": 0.4, "bn": 0.2, "book": 0.2}
    inp = MarketInputs(
        "BTCUSDT", 1795020000, 1795020200.0, 100.0, 100.048, 100.048, 100.048,
        hist, [(0.74, 100.0)], [(0.26, 100.0)], [(0.72, 100.0)], [(0.24, 100.0)], ages,
    )
    d1 = evaluate_signal(inp, TWAP_V2_FILTERED_CONFIG)
    d2 = evaluate_signal(inp, TWAP_V2_FILTERED_CONFIG)
    assert d1 == d2 and d1.action == "ENTER" and d1.candidate is not None

    r_a, w_a = _warm_runner(tmp, "cb1", cb_scale=1.0)
    r_b, w_b = _warm_runner(tmp, "cb2", cb_scale=1.15)
    _, decs_a = r_a.evaluate_second(float(w_a + 200), w_a)
    _, decs_b = r_b.evaluate_second(float(w_b + 200), w_b)
    da, db = decs_a["BTCUSDT"], decs_b["BTCUSDT"]
    assert da.sigma == db.sigma and da.z == db.z and da.p_model == db.p_model
    assert da.candidate == db.candidate
    assert r_a.hub.states["BTCUSDT"].micro._last_basis_gap_bps == r_b.hub.states["BTCUSDT"].micro._last_basis_gap_bps
    return 1


def check_2_historical_parity_replay(_tmp: Path) -> int:
    """Run Path A vs Path B full per-second + ENTER parity replay and assert >=99.5% / >=99%."""
    res = run_parity_replay()
    assert res["per_second_total"] > 0 and res["per_second_parity_rate"] >= 0.995, res
    assert len(res.get("by_decision_group") or {}) >= 2, res
    assert res["coin_window_union_signals"] > 0, res
    assert res["coin_window_parity_rate"] >= 0.99 and res["single_slot_parity_rate"] >= 0.99, res
    assert res["enter_parity_gate_pass"] is True and "enter_sample_gate" in res, res
    return 1


def _cold_runner(tmp: Path, tag: str, cache_path: Path | None) -> tuple[OracleSniperRunner, int]:
    at = MagicMock()
    at.live_wallet_state = {"cash_pusd": 40.0, "portfolio_pusd": 0.0, "updated_at_ms": int((time.time() + 500) * 1000)}
    at.sniper_buy = AsyncMock(return_value={"filled": False})
    cl = StrictFakeChainlink()
    cl.spot_age = cl.twap_age = 0.2
    pol = replace(
        PAPER_40U_EXEC_POLICY,
        frozen_config_path=Path(__file__).resolve().parent / "edge_validation" / "frozen_config.json",
        vol_cache_path=cache_path,
        live_shadow_log_path=None,
    )
    led = SniperLedger(tmp / f"{tag}.json", policy=PAPER_40U_RISK_POLICY)
    runner = OracleSniperRunner(at, cl, MagicMock(), cfg=TWAP_V2_FILTERED_CONFIG, ledger=led, policy=pol)
    w = (int(time.time()) // 300) * 300 + 300
    for sym, st in runner.hub.states.items():
        st.reset_for_window(w)
        st.up_token, st.down_token, st.price_to_beat = f"{sym}-up", f"{sym}-dn", 100.0
        st.meta_checked_ts = st.prefetch_ts = 1e12
        st.book = OrderBookTop(0.72, 0.74, 0.24, 0.26)
        st.up_local_ts = st.down_local_ts = float(w + 200)
    return runner, w


def check_3_warmup_gap_and_vol_cache(tmp: Path) -> int:
    """Verify 1800s cold warmup lock, <=5s gap-fill vs 6s reset, and 60s disk cache reload."""
    runner, w = _cold_runner(tmp, "warm", cache_path=None)
    now = float(w + 200)
    t0 = int(now) - 1799
    st = runner.hub.states["BTCUSDT"]
    for i in range(1799):
        st.ingest_binance_trade(float(t0 + i), 100.0 + 0.048 * ((i + 1) / 1800.0), 0.0, False)
    _, decs = runner.evaluate_second(float(t0 + 1798), w)
    assert decs["BTCUSDT"].action == "SKIP" and decs["BTCUSDT"].reason == "WARMUP"
    st.ingest_binance_trade(now, 100.048, 0.0, False)
    assert len(st.micro.prices) == 1800
    _, decs2 = runner.evaluate_second(now, w)
    assert decs2["BTCUSDT"].action == "ENTER", decs2["BTCUSDT"]

    tr = Bn1sMicroTracker(vol_lookback_sec=1800)
    for i in range(1795):
        tr.ingest_bn_1s(1000 + i, 100.0, 0.0, 0.0, real_trade=True)
    tr.ingest_bn_1s(2799, 100.0, 0.0, 0.0, real_trade=False)
    assert len(tr.prices) == 1800, f"<=5s gap should fill, got {len(tr.prices)}"
    tr.ingest_bn_1s(2800, 100.0, 0.0, 0.0, real_trade=False)
    assert len(tr.prices) == 0, f"6s stale gap without real trade must clear, got {len(tr.prices)}"
    tr.ingest_bn_1s(2801, 100.0, 0.0, 0.0, real_trade=True)
    assert len(tr.prices) == 1, f"real trade after >5s reset starts at 1, got {len(tr.prices)}"

    cache_p = tmp / "vol_cache.json"
    runner_s, _ = _cold_runner(tmp, "save", cache_path=cache_p)
    for i in range(1800):
        runner_s.hub.states["BTCUSDT"].micro.ingest_bn_1s(5000 + i, 100.0 + 1e-4 * (i % 5), 0.0, 0.0, real_trade=True)
    runner_s._last_vol_save_ts = 0.0
    runner_s._maybe_save_vol_cache(6799.0)
    assert cache_p.exists()

    raw = json.loads(cache_p.read_text(encoding="utf-8"))
    tr_ok = Bn1sMicroTracker(1800)
    assert tr_ok.load_persisted_dict(raw["BTCUSDT"], 6804) and len(tr_ok.prices) == 1804
    tr_old = Bn1sMicroTracker(1800)
    assert (not tr_old.load_persisted_dict(raw["BTCUSDT"], 6805)) and len(tr_old.prices) == 0
    return 1


def check_4_depth_aware_fak_buy_and_sell(_tmp: Path) -> int:
    """Verify multi-level FAK sweep, weighted avg_px, MISSED_DEPTH, MISSED_PRICE, and taker fee."""
    b_ok = simulate_fak_buy([(0.72, 2.0), (0.76, 3.0)], shares=5.0, limit_px=0.77)
    assert b_ok.filled and b_ok.status == "FILLED" and b_ok.filled_shares == 5.0
    assert b_ok.avg_px is not None and abs(b_ok.avg_px - 0.744) < 1e-6

    b_depth = simulate_fak_buy([(0.72, 2.0), (0.76, 1.0)], shares=5.0, limit_px=0.77)
    assert (not b_depth.filled) and b_depth.status == "MISSED_DEPTH" and b_depth.filled_shares == 3.0

    b_px = simulate_fak_buy([(0.85, 10.0)], shares=5.0, limit_px=0.77)
    assert (not b_px.filled) and b_px.status == "MISSED_PRICE" and b_px.filled_shares == 0.0

    s_ok = simulate_fak_sell([(0.98, 2.0), (0.96, 3.0)], shares=5.0, min_bid=0.95)
    assert s_ok.filled and s_ok.status == "FILLED" and s_ok.avg_px is not None and abs(s_ok.avg_px - 0.968) < 1e-6
    s_px = simulate_fak_sell([(0.90, 10.0)], shares=5.0, min_bid=0.95)
    assert (not s_px.filled) and s_px.status == "MISSED_PRICE"
    assert abs(taker_fee_per_share(b_ok.avg_px, 0.05) - round(0.05 * 0.744 * 0.256, 5)) < 1e-9
    return 1


def check_5_freshness_codes_and_db_schema(tmp: Path) -> int:
    """Verify individual STALE_* skip codes and SQLite schema for ages + enter/skip logs."""
    hist = tuple(100.0 + 1e-4 * (i % 5) for i in range(1800))
    base_ages = {"cl_twap": 0.5, "cl_spot": 0.5, "bn": 0.5, "book": 0.5}
    for key, val, expected in (
        ("cl_twap", 3.1, "STALE_CL_TWAP"),
        ("cl_spot", 3.1, "STALE_CL_SPOT"),
        ("bn", 2.1, "STALE_BN"),
        ("book", 3.6, "STALE_BOOK"),
    ):
        ages = {**base_ages, key: val}
        inp = MarketInputs("BTCUSDT", 1000, 1200.0, 100.0, 100.05, 100.05, 100.05, hist, [(0.74, 10.0)], [], [(0.72, 10.0)], [], ages)
        dec = evaluate_signal(inp, TWAP_V2_FILTERED_CONFIG)
        assert dec.action == "SKIP" and dec.reason == expected, (key, dec)

    con = init_db(tmp / "schema42.db")
    cols = {r[1] for r in con.execute("PRAGMA table_info(snapshots_1hz)").fetchall()}
    expected_cols = {
        "cl_twap_age_sec", "cl_spot_age_sec", "bn_age_sec", "up_book_age_sec", "down_book_age_sec",
        "cl_twap_age_ms", "cl_spot_age_ms", "bn_age_ms", "up_book_age_ms", "down_book_age_ms",
        "cl_twap_lag_ms", "cl_spot_lag_ms", "bn_lag_ms", "up_book_lag_ms", "down_book_lag_ms",
    }
    assert expected_cols.issubset(cols), expected_cols - cols
    con.execute(
        "INSERT INTO signal_enters_42 (ts_sec, window_ts, symbol, model_name, side, z, p_model, sigma, ask_top, cl_twap_age_sec, cl_spot_age_sec, bn_age_sec, book_age_sec) VALUES (1,0,'BTCUSDT','v2_filtered','UP',1.8,0.92,5e-5,0.74,0.3,0.3,0.1,0.2)"
    )
    con.execute("INSERT INTO signal_skips_42 (ts_sec, window_ts, model_name, reason, count) VALUES (1,0,'v2_filtered','STALE_BN',1)")
    con.commit()
    assert con.execute("SELECT COUNT(*) FROM signal_enters_42").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM signal_skips_42").fetchone()[0] == 1
    con.close()
    return 1


def _build_synthetic_rows(win_ts: int, resolved: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    asks = json.dumps([[0.74, 50.0]])
    bids = json.dumps([[0.72, 50.0]])
    for sec in range(300):
        px = 100.0 + 1e-4 * (sec % 4)
        rows.append({
            "ts_sec": win_ts + sec, "window_ts": win_ts, "symbol": "BTCUSDT", "elapsed_sec": sec,
            "cl_spot": px, "cl_twap60": px, "bn_close": px, "bn_vol": 0.0, "bn_buy_vol": 0.0, "bn_sell_vol": 0.0, "bn_trades": 1,
            "up_bids_json": bids, "up_asks_json": asks, "down_bids_json": json.dumps([[0.24, 50.0]]), "down_asks_json": json.dumps([[0.26, 50.0]]),
            "up_bid1": 0.72, "up_bid1_sz": 50.0, "up_ask1": 0.74, "up_ask1_sz": 50.0,
            "down_bid1": 0.24, "down_bid1_sz": 50.0, "down_ask1": 0.26, "down_ask1_sz": 50.0,
            "cl_twap_age_sec": 0.2, "cl_spot_age_sec": 0.2, "bn_age_sec": 0.2, "up_book_age_sec": 0.2, "down_book_age_sec": 0.2,
        })
    meta = SimpleNamespace(window_ts=win_ts, symbol="BTCUSDT", twap_strike=100.0, resolved_outcome=resolved)
    return rows, meta


def check_6_dual_track_stop_loss_and_frozen_param(_tmp: Path) -> int:
    """Verify EMERGENCY_STOP_LOSS dual-track audit metrics without altering frozen stop_loss_z_score."""
    assert TWAP_V2_FILTERED_CONFIG.stop_loss_z_score == -0.35
    base = 1795020000
    all_rows: list[dict[str, Any]] = []
    w_meta: dict[tuple[int, str], Any] = {}
    settled: list[int] = []
    for k in range(7):
        wt = base + k * 300
        settled.append(wt)
        rows, meta = _build_synthetic_rows(wt, "UP")
        if k == 6:
            for r in rows[180:200]:
                r.update({"cl_spot": 100.06, "cl_twap60": 100.04, "up_asks_json": json.dumps([[0.74, 50.0]]), "up_ask1": 0.74, "up_bids_json": json.dumps([[0.72, 50.0]]), "up_bid1": 0.72})
            for r in rows[258:265]:
                r.update({"cl_spot": 99.90, "cl_twap60": 99.95, "bn_sell_vol": 25.0, "bn_trades": 5, "up_bids_json": json.dumps([[0.18, 50.0]]), "up_bid1": 0.18, "up_asks_json": json.dumps([[0.20, 50.0]]), "up_ask1": 0.20})
        all_rows.extend(rows)
        w_meta[(wt, "BTCUSDT")] = meta
    df_s = pd.DataFrame(all_rows)
    snap_by_wt_sym = {(int(k[0]), str(k[1])): g for k, g in df_s.groupby(["window_ts", "symbol"])}
    df_t, _, _, _ = _sim_one_cfg(TWAP_V2_FILTERED_CONFIG, {"BTCUSDT": 0.05}, settled, snap_by_wt_sym, w_meta)
    assert len(df_t) == 1, df_t
    tr = df_t.iloc[0]
    assert bool(tr["sl_triggered"]) and bool(tr["sl_was_wrong"])
    assert float(tr["pnl_with_sl"]) < 0.0 and float(tr["pnl_hold_to_settle"]) > 0.0
    assert pd.notna(tr["z_twap_proj"])
    return 1


def check_7_single_slot_aligned(tmp: Path) -> int:
    """Verify PAPER_40U and PRODUCTION policies both enforce single_slot (max 1 position)."""
    assert PAPER_40U_EXEC_POLICY.max_concurrent_positions == 1
    assert PRODUCTION_EXEC_POLICY.max_concurrent_positions == 1
    for label, pol_base, risk_pol in (
        ("40u", PAPER_40U_EXEC_POLICY, PAPER_40U_RISK_POLICY),
        ("prod", PRODUCTION_EXEC_POLICY, PRODUCTION_RISK_POLICY),
    ):
        r, at, _cl, w = _make_strict_runner(tmp, f"slot_{label}", cash=100.0)
        r.policy = replace(
            pol_base, frozen_config_path=r.policy.frozen_config_path, vol_cache_path=None,
            paper_fill_delay_sec=0.0, clock=lambda w=w: float(w + 200), live_shadow_log_path=None,
        )
        r.ledger = SniperLedger(tmp / f"led_{label}.json", policy=risk_pol)
        asyncio.run(r._tick(float(w + 200)))
        assert at.sniper_buy.await_count == 1 or len(r._positions) == 1
        assert len(r._positions) == 1 and set(r._positions) == {"BTCUSDT"}
    return 1


def _make_mono_runner(tmp: Path, tag: str, shadow_p: Path | None = None) -> tuple[OracleSniperRunner, ChainlinkRTDSFeed, SniperMarketHub, list[float], list[float], int]:
    wall_ref, mono_ref = [1795020200.0], [50_000.0]
    w = 1795020000
    at = MagicMock()
    at.last_geoblock_status = {"can_trade_api": True}
    cl = ChainlinkRTDSFeed(clock=lambda: wall_ref[0], mono_clock_ms=lambda: mono_ref[0])
    cl.connected = True
    hub = SniperMarketHub(at, TWAP_V2_FILTERED_CONFIG, clock=lambda: wall_ref[0], mono_clock_ms=lambda: mono_ref[0])
    pol = replace(
        PAPER_40U_EXEC_POLICY,
        frozen_config_path=Path(__file__).resolve().parent / "edge_validation" / "frozen_config.json",
        vol_cache_path=None, clock=lambda: wall_ref[0], mono_clock_ms=lambda: mono_ref[0], live_shadow_log_path=shadow_p,
    )
    runner = OracleSniperRunner(at, cl, MagicMock(), cfg=TWAP_V2_FILTERED_CONFIG, ledger=SniperLedger(tmp / f"{tag}.json", policy=PAPER_40U_RISK_POLICY), hub=hub, policy=pol)
    st = hub.states["BTCUSDT"]
    st.reset_for_window(w)
    st.up_token, st.down_token, st.price_to_beat = "BTC-up", "BTC-dn", 100.0
    st.meta_checked_ts = st.prefetch_ts = 1e12
    for i in range(1800):
        ts_i = int(wall_ref[0]) - 1800 + i
        st.micro.ingest_bn_1s(ts_i, 100.0 + 0.048 * ((i + 1) / 1800.0), 0.0, 0.0, real_trade=True)
    return runner, cl, hub, wall_ref, mono_ref, w


def _pulse_all_feeds(cl: ChainlinkRTDSFeed, hub: SniperMarketHub, wall_ref: list[float], mono_ref: list[float], pause: str = "") -> None:
    src_ms = int((wall_ref[0] - 0.25) * 1000.0)
    if pause != "cl_spot":
        cl.ingest_tick("btc/usd", 100.048, src_ms, recv_mono_ms=mono_ref[0], recv_wall_ms=wall_ref[0] * 1000.0)
    if pause != "cl_twap":
        cl.ingest_twap_tick("btc/usd", 100.048, src_ms, recv_mono_ms=mono_ref[0], recv_wall_ms=wall_ref[0] * 1000.0)
    if pause != "bn":
        hub._on_binance_message({"stream": "btcusdt@aggTrade", "data": {"p": "100.048", "q": "0.1", "m": False, "E": src_ms}})
    if pause != "book":
        hub.apply_book("BTCUSDT", "UP", {"asset_id": "BTC-up", "timestamp": src_ms, "bids": [{"price": "0.72", "size": "100"}], "asks": [{"price": "0.74", "size": "100"}]}, via_ws=True)


def check_8_monotonic_ms_age_lag_and_4s_pause(tmp: Path) -> int:
    """Pause each feed 4s under monotonic_ns tracking -> exact STALE_*; verify lag_ms & wall-jump invariance."""
    expected_map = {"cl_twap": "STALE_CL_TWAP", "cl_spot": "STALE_CL_SPOT", "bn": "STALE_BN", "book": "STALE_BOOK"}
    for idx, paused_feed in enumerate(expected_map):
        runner, cl, hub, wall_r, mono_r, w = _make_mono_runner(tmp, f"pause_{idx}")
        _pulse_all_feeds(cl, hub, wall_r, mono_r, pause="")
        _, d0 = runner.evaluate_second(wall_r[0], w)
        assert d0["BTCUSDT"].action == "ENTER", d0["BTCUSDT"]
        # Advance 4.0s and refresh every OTHER feed while pausing only `paused_feed`
        wall_r[0] += 4.0
        mono_r[0] += 4000.0
        _pulse_all_feeds(cl, hub, wall_r, mono_r, pause=paused_feed)
        _, d1 = runner.evaluate_second(wall_r[0], w)
        assert d1["BTCUSDT"].action == "SKIP" and d1["BTCUSDT"].reason == expected_map[paused_feed], (paused_feed, d1["BTCUSDT"])

    # Wall clock NTP +4s jump must NOT break monotonic freshness while preserving exact upstream lag_ms
    runner, cl, hub, wall_r, mono_r, w = _make_mono_runner(tmp, "wall_jump")
    _pulse_all_feeds(cl, hub, wall_r, mono_r, pause="")
    wall_r[0] += 4.0
    mono_r[0] += 300.0
    _, d_j = runner.evaluate_second(wall_r[0], w)
    assert d_j["BTCUSDT"].action == "ENTER", d_j["BTCUSDT"]
    assert abs(runner.diag["BTCUSDT"]["ages_ms"]["cl_spot"] - 300.0) < 1e-3
    assert abs(runner.diag["BTCUSDT"]["lags_ms"]["cl_spot_ms"] - 250.0) < 1e-3
    return 1


def check_9_live_shadow_parity_and_reports(tmp: Path) -> int:
    """Verify Runner JSONL shadow emission, daily shadow parity calculation, R1 mismatch & WARMUP report."""
    sh_p = tmp / "shadow.jsonl"
    runner, cl, hub, wall_r, mono_r, w = _make_mono_runner(tmp, "shadow", shadow_p=sh_p)
    _pulse_all_feeds(cl, hub, wall_r, mono_r, pause="")
    _, decs_sh = runner.evaluate_second(wall_r[0], w)
    z_sh = float(decs_sh["BTCUSDT"].z or 0.0)
    assert sh_p.exists() and '"type": "ENTER"' in sh_p.read_text(encoding="utf-8")

    # Verify aggregate parity matcher on matching vs mismatched shadow days
    good = _aggregate_shadow_parity([(w, "BTCUSDT")], {(w, "BTCUSDT"): ("UP", int(wall_r[0]), 2.0, 0.74)}, {(w, "BTCUSDT"): ("UP", int(wall_r[0]) + 1, 2.01, 0.74)})
    bad = _aggregate_shadow_parity([(w, "BTCUSDT")], {(w, "BTCUSDT"): ("UP", int(wall_r[0]), 2.0, 0.74)}, {(w, "BTCUSDT"): ("DOWN", int(wall_r[0]), 2.0, 0.74)})
    assert good["parity_rate"] == 1.0 and good["gate_pass"] is True
    assert bad["parity_rate"] == 0.0 and bad["gate_pass"] is False

    # End-to-end analyze_recording check on synthetic DB + frozen.json + shadow JSONL
    db_p = tmp / "shadow42.db"
    con = init_db(db_p)
    con.execute("INSERT INTO windows_42 (window_ts,symbol,slug,twap_strike,twap_close,resolved_outcome) VALUES (?,?,?,?,?,?)", (w, "BTCUSDT", f"btc-updown-5m-{w}", 100.0, 100.05, "DOWN"))
    con.execute("INSERT INTO signal_enters_42 (ts_sec,window_ts,symbol,model_name,side,z,p_model,sigma,ask_top) VALUES (?,?,?,?,?,?,?,?,?)", (int(wall_r[0]), w, "BTCUSDT", "v2_filtered", "UP", z_sh, 0.95, 5e-5, 0.74))
    for sec in range(150, 220):
        con.execute("INSERT INTO snapshots_1hz (ts_sec,window_ts,symbol,elapsed_sec,cl_spot,cl_twap60,bn_close,bn_trades,cl_twap_age_sec,cl_spot_age_sec,bn_age_sec,up_book_age_sec,down_book_age_sec,cl_twap_age_ms,cl_spot_age_ms,bn_age_ms,up_book_age_ms,down_book_age_ms,cl_spot_lag_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (w + sec, w, "BTCUSDT", sec, 100.04, 100.04, 100.04, 1, 0.4, 0.4, 0.2, 0.1, 0.1, 400.0, 400.0, 200.0, 100.0, 100.0, 250.0))
    con.commit()
    con.close()
    fr_p = tmp / "frozen.json"
    fr_data = json.loads((Path(__file__).resolve().parent / "edge_validation" / "frozen_config.json").read_text(encoding="utf-8"))
    fr_data["count_start_window_ts"] = w
    fr_p.write_text(json.dumps(fr_data), encoding="utf-8")
    rep = analyze_recording(db_path=db_p, frozen_path=fr_p, out_dir=tmp, daily_report=True, shadow_log_path=sh_p)
    assert rep["live_shadow_parity"]["matched"] == 1 and rep["live_shadow_parity"]["parity_rate"] == 1.0
    assert len(rep["r1_diagnostics"]["mismatches"]) == 1
    assert abs(rep["freshness_ms"]["age_ms"]["cl_spot"]["p50"] - 400.0) < 1e-3
    md = (tmp / "phase42_daily_report.md").read_text(encoding="utf-8")
    assert "P99 (ms)" in md and "Live Shadow Parity" in md and "R1 TWAP Parity Mismatches" in md
    return 1


def check_10_subsecond_twap_normalization(tmp: Path) -> int:
    """Verify sub-second remaining_sec normalizes TWAP weights (no phantom -59 sigma) in pure + Runner eval."""
    for elapsed in (240.0, 240.2, 252.73, 255.99):
        rem = 300.0 - elapsed
        fv = estimate_twap_fair_value(100.0, 100.05, rem, 5e-5, realized_240=(100.05, float(int(elapsed) - 240)), cfg=TWAP_V2_FILTERED_CONFIG)
        assert fv is not None and fv.raw_z_score > 0.0 and fv.p_true_up > 0.5, (elapsed, fv)
    runner, cl, hub, wall_r, mono_r, w = _make_mono_runner(tmp, "subsec")
    st = hub.states["BTCUSDT"]
    for sec in range(w + 200, w + 252):
        st.micro.ingest_bn_1s(sec, 100.048, 0.0, 0.0, real_trade=True)
        if sec >= w + 240:
            st.micro.record_cl_240(w, sec, 100.048)
    wall_r[0] = float(w) + 252.73
    mono_r[0] += 52_730.0
    _pulse_all_feeds(cl, hub, wall_r, mono_r, pause="")
    src_ms = int((wall_r[0] - 0.25) * 1000.0)
    hub.apply_book("BTCUSDT", "UP", {"asset_id": "BTC-up", "timestamp": src_ms, "bids": [{"price": "0.76", "size": "100"}], "asks": [{"price": "0.78", "size": "100"}]}, via_ws=True)
    _, decs = runner.evaluate_second(wall_r[0], w)
    d = decs["BTCUSDT"]
    assert d.action == "ENTER" and d.side == "UP" and float(d.z or 0.0) > 0.0, d
    return 1


def check_11_quiet_market_bookticker_vol_survival(tmp: Path) -> int:
    """Verify @bookTicker preserves 1800s vol history across quiet >5s periods while true disconnect resets."""
    tr = Bn1sMicroTracker(1800, 20)
    t0 = 1795020000
    for i in range(1800):
        tr.ingest_bn_1s(t0 - 1800 + i, 100.0, 10.0, 10.0, real_trade=True)
    for dt in range(15):
        tr.ingest_bn_1s(t0 + dt, 100.0, 0.0, 0.0, real_trade=False, feed_alive=True)
    cvd_q, _ = tr.get_orderflow_metrics(t0 + 14)
    assert len(tr.prices) == 1805 and cvd_q == 0.0 and tr._trades_1s[-1][0] == t0 - 1
    for dt in range(15, 22):
        tr.ingest_bn_1s(t0 + dt, None, 0.0, 0.0, real_trade=False, feed_alive=False)
    assert len(tr.prices) == 0

    _runner, _at, cl, w = _make_strict_runner(tmp, "quiet_hub")
    st = _runner.hub.states["SOLUSDT"]
    for dt in range(15):
        st.sync_micro_at(float(w + 200 + dt), cl.prices["SOLUSDT"], w, bn_age_sec=0.2)
    assert len(st.micro.prices) >= 1800
    for dt in range(15, 22):
        st.sync_micro_at(float(w + 200 + dt), cl.prices["SOLUSDT"], w, bn_age_sec=6.0)
    assert len(st.micro.prices) == 0
    return 1


def check_12_recorder_full_depth_clob_and_monotonic_lag(tmp: Path) -> int:
    """Verify full-depth CLOB parsing (>10 levels), price_change sweep, ev_ts guard & REST lag immunity."""
    rec = LiveRecorder42(tmp / "rec_clob.db")
    try:
        st = rec.states["BTCUSDT"]
        st.up_token = "UP-TOK"
        rec.token_to_meta["UP-TOK"] = ("BTCUSDT", "UP")
        now_ms = int(time.time() * 1000)
        asks = [{"price": f"{0.70 + 0.01 * i:.2f}", "size": "10"} for i in range(15)]
        bids = [{"price": "0.68", "size": "20"}]
        rec._handle_clob_msg(json.dumps({"event_type": "book", "asset_id": "UP-TOK", "timestamp": now_ms - 500, "bids": bids, "asks": asks}))
        assert len(st.up_asks_raw) == 15 and len(st.up_asks) == 10 and abs(st.up_asks[0][0] - 0.70) < 1e-9
        sweeps = [{"asset_id": "UP-TOK", "side": "SELL", "price": f"{0.70 + 0.01 * i:.2f}", "size": "0"} for i in range(11)]
        rec._handle_clob_msg(json.dumps({"event_type": "price_change", "timestamp": now_ms - 100, "price_changes": sweeps}))
        assert len(st.up_asks_raw) == 4 and len(st.up_asks) == 4 and abs(st.up_asks[0][0] - 0.81) < 1e-9
        rec._handle_clob_msg(json.dumps({"event_type": "book", "asset_id": "UP-TOK", "timestamp": now_ms - 1000, "bids": bids, "asks": [{"price": "0.50", "size": "1"}]}))
        assert abs(st.up_asks[0][0] - 0.81) < 1e-9 and st.up_source_ts_ms == now_ms - 100
        rec._apply_book("UP-TOK", bids, [{"price": "0.82", "size": "5"}], source_ts_ms=now_ms - 80, via_ws=False)
        assert st.up_source_ts_ms == now_ms - 100
    finally:
        rec.con.close()
    return 1


def check_13_recorder_1hz_binance_bucket_flush(tmp: Path) -> int:
    """Verify 1Hz flush preserves 100% of sub-second Binance trade vol/count and @bookTicker vol survival."""
    db_p = tmp / "rec_bn.db"
    rec = LiveRecorder42(db_p)
    try:
        t_sec = int(time.time())
        for px, qty, is_sell in (("100.0", "1.5", False), ("100.1", "2.5", True), ("100.2", "3.0", False)):
            rec._on_binance_msg(json.dumps({"stream": "btcusdt@aggTrade", "data": {"p": px, "q": qty, "m": is_sell, "E": t_sec * 1000}}))
        rec._record_second(t_sec)
        rec._on_binance_msg(json.dumps({"stream": "btcusdt@bookTicker", "data": {"b": "100.2", "a": "100.2", "E": (t_sec + 1) * 1000}}))
        rec._record_second(t_sec + 1)
        rec.con.commit()
        con = sqlite3.connect(db_p)
        r0 = con.execute("SELECT bn_vol, bn_buy_vol, bn_sell_vol, bn_trades FROM snapshots_1hz WHERE ts_sec=? AND symbol='BTCUSDT'", (t_sec,)).fetchone()
        r1 = con.execute("SELECT bn_close, bn_trades FROM snapshots_1hz WHERE ts_sec=? AND symbol='BTCUSDT'", (t_sec + 1,)).fetchone()
        con.close()
        assert r0 is not None and abs(r0[0] - 7.0) < 1e-9 and abs(r0[1] - 4.5) < 1e-9 and abs(r0[2] - 2.5) < 1e-9 and r0[3] == 3, r0
        assert rec.states["BTCUSDT"].bn_n == 0 and r1 == (100.2, 0), r1
        assert rec.micro["BTCUSDT"].last_real_bn_sec == t_sec + 1 and abs(rec.micro["BTCUSDT"].prices[-1] - 100.2) < 1e-9
    finally:
        rec.con.close()
    return 1


def main() -> None:
    """Execute all 13 verification checks against isolated temporary state."""
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        tests = (
            check_1_determinism_and_coinbase_isolation,
            check_2_historical_parity_replay,
            check_3_warmup_gap_and_vol_cache,
            check_4_depth_aware_fak_buy_and_sell,
            check_5_freshness_codes_and_db_schema,
            check_6_dual_track_stop_loss_and_frozen_param,
            check_7_single_slot_aligned,
            check_8_monotonic_ms_age_lag_and_4s_pause,
            check_9_live_shadow_parity_and_reports,
            check_10_subsecond_twap_normalization,
            check_11_quiet_market_bookticker_vol_survival,
            check_12_recorder_full_depth_clob_and_monotonic_lag,
            check_13_recorder_1hz_binance_bucket_flush,
        )
        passed = sum(fn(tmp) for fn in tests)
    print(json.dumps({"status": "SIGNAL_CORE_AND_PARITY_PASSED", "groups_passed": passed}))


if __name__ == "__main__":
    main()
