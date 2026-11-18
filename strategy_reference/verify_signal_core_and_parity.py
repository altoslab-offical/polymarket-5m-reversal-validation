#!/usr/bin/env python3
"""End-to-end verification of pure signal_core, parity >=99%, warmup, depth-FAK, freshness, SL dual-track, and single_slot."""
from __future__ import annotations

import asyncio
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pandas as pd

from edge_validation.live_recorder_42 import _sim_one_cfg, init_db
from edge_validation.parity_replay import run_parity_replay
from engine.oracle_sniper import TWAP_V2_FILTERED_CONFIG, OrderBookTop, taker_fee_per_share
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
    """Run Path A vs Path B 1Hz parity replay over recorded DB and assert >= 99% match."""
    res = run_parity_replay()
    assert res["coin_window_union_signals"] > 0, res
    assert res["coin_window_parity_rate"] >= 0.99, res
    assert res["single_slot_parity_rate"] >= 0.99, res
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
    assert {"cl_twap_age_sec", "cl_spot_age_sec", "bn_age_sec", "up_book_age_sec", "down_book_age_sec"}.issubset(cols)
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
    df_t, _, _ = _sim_one_cfg(TWAP_V2_FILTERED_CONFIG, {"BTCUSDT": 0.05}, settled, snap_by_wt_sym, w_meta)
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
            paper_fill_delay_sec=0.0, clock=lambda w=w: float(w + 200),
        )
        r.ledger = SniperLedger(tmp / f"led_{label}.json", policy=risk_pol)
        asyncio.run(r._tick(float(w + 200)))
        assert at.sniper_buy.await_count == 1 or len(r._positions) == 1
        assert len(r._positions) == 1 and set(r._positions) == {"BTCUSDT"}
    return 1


def main() -> None:
    """Execute all 7 verification checks against isolated temporary state."""
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
        )
        passed = sum(fn(tmp) for fn in tests)
    print(json.dumps({"status": "SIGNAL_CORE_AND_PARITY_PASSED", "groups_passed": passed}))


if __name__ == "__main__":
    main()
