#!/usr/bin/env python3
"""Parity replay harness: compares Path A (Recorder) vs Path B (Runner + simulated WS) on live_3coin_42.db."""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import json
import pathlib
import sqlite3
import sys
import tempfile
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent
REPO = ROOT.parent
sys.path.insert(0, str(REPO))

from edge_validation.live_recorder_42 import DEFAULT_DB, SYMBOLS, RecorderStreamState, _parse_levels_json, assemble_recorder_inputs
from engine.chainlink_oracle import ChainlinkRTDSFeed
from engine.oracle_sniper import TWAP_V2_FILTERED_CONFIG, SniperConfig
from engine.oracle_sniper_feeds import SniperMarketHub
from engine.oracle_sniper_ledger import PAPER_40U_RISK_POLICY, SniperLedger
from engine.oracle_sniper_runner import PAPER_40U_EXEC_POLICY, PRODUCTION_FEE_RATES, OracleSniperRunner
from engine.signal_core import DEFAULT_MAX_AGES, SignalDecision, choose_best_candidate, evaluate as evaluate_signal

REPORT_PATH = ROOT / "parity_replay_report.md"


@dataclass(frozen=True)
class EnterPoint:
    """First ENTER signal recorded for one `(window_ts, symbol)` or window slot."""

    side: str
    ts: int
    z: float
    p_model: float
    sigma: float
    ask_top: float


def same_signal(a: Optional[EnterPoint], b: Optional[EnterPoint]) -> bool:
    """Acceptance predicate from spec: same side, |dt| <= 1s, |dz| <= 0.02."""
    if a is None and b is None:
        return True
    if (a is None) != (b is None):
        return False
    assert a is not None and b is not None
    return a.side == b.side and abs(a.ts - b.ts) <= 1 and abs(a.z - b.z) <= 0.02


def _make_replay_runner(tmp_dir: pathlib.Path, clock_ref: List[float]) -> Tuple[ChainlinkRTDSFeed, SniperMarketHub, OracleSniperRunner]:
    clock_fn = lambda: clock_ref[0]  # noqa: E731
    at = MagicMock()
    at.last_geoblock_status = {"can_trade_api": True}
    cl = ChainlinkRTDSFeed(clock=clock_fn, max_ticks_per_symbol=4000)
    cl.connected = True
    hub = SniperMarketHub(at, TWAP_V2_FILTERED_CONFIG, clock=clock_fn)
    led = SniperLedger(tmp_dir / "replay_ledger.json", policy=PAPER_40U_RISK_POLICY)
    pol = replace(PAPER_40U_EXEC_POLICY, vol_cache_path=None, clock=clock_fn)
    runner = OracleSniperRunner(at, cl, MagicMock(), cfg=TWAP_V2_FILTERED_CONFIG, ledger=led, hub=hub, policy=pol)
    return cl, hub, runner


def _feed_ws_row(
    row: Any, sym: str, w: int, t_sec: int, wm: Any, refs: Tuple[ChainlinkRTDSFeed, SniperMarketHub, Dict[str, Optional[float]]]
) -> None:
    cl, hub, prev_bn = refs
    st = hub.states[sym]
    st.up_token, st.down_token = f"{sym}-up-{w}", f"{sym}-dn-{w}"
    st.slug = f"{sym[:3].lower()}-updown-5m-{w}"
    st.price_to_beat = None
    st.meta_checked_ts = st.prefetch_ts = 1e12
    elapsed = t_sec - w
    if elapsed > 8 and cl.get_settlement_twap_at(sym, w) is None and wm is not None and pd.notna(wm.twap_strike):
        cl.twap_bucket_opens[sym][w] = (float(wm.twap_strike), w * 1000)
    sp_age_col = getattr(row, "cl_spot_age_sec", None)
    tw_age_col = getattr(row, "cl_twap_age_sec", None)
    has_ages = sp_age_col is not None and pd.notna(sp_age_col) and tw_age_col is not None and pd.notna(tw_age_col)
    raw_s = {"BTCUSDT": "btc/usd", "ETHUSDT": "eth/usd", "SOLUSDT": "sol/usd"}[sym]
    if pd.notna(row.cl_spot):
        sp_ms = int((float(t_sec) - float(sp_age_col)) * 1000.0) if has_ages else t_sec * 1000
        cl.ingest_tick(raw_s, float(row.cl_spot), sp_ms)
    if pd.notna(row.cl_twap60):
        tw_ms = int((float(t_sec) - float(tw_age_col)) * 1000.0) if has_ages else t_sec * 1000
        cl.ingest_twap_tick(raw_s, float(row.cl_twap60), tw_ms)
    _feed_bn_and_book(row, sym, t_sec, has_ages, hub, prev_bn)


def _feed_bn_and_book(
    row: Any, sym: str, t_sec: int, has_ages: bool, hub: SniperMarketHub, prev_bn: Dict[str, Optional[float]]
) -> None:
    st = hub.states[sym]
    n_tr = int(getattr(row, "bn_trades", 0) or 0)
    bn_v = float(row.bn_close) if pd.notna(row.bn_close) else None
    bn_upd = bool(bn_v is not None and (n_tr > 0 or bn_v != prev_bn.get(sym)))
    if bn_v is not None:
        prev_bn[sym] = bn_v
    if bn_upd and bn_v is not None:
        px = str(bn_v)
        b_vol = float(row.bn_buy_vol or 0.0) if n_tr > 0 else 0.0
        s_vol = float(row.bn_sell_vol or 0.0) if n_tr > 0 else 0.0
        if b_vol > 0.0:
            hub._on_binance_message({"stream": f"{sym.lower()}@aggTrade", "data": {"p": px, "q": str(b_vol), "m": False}})
        if s_vol > 0.0:
            hub._on_binance_message({"stream": f"{sym.lower()}@aggTrade", "data": {"p": px, "q": str(s_vol), "m": True}})
        if b_vol <= 0.0 and s_vol <= 0.0:
            hub._on_binance_message({"stream": f"{sym.lower()}@aggTrade", "data": {"p": px, "q": "0", "m": False}})
    if has_ages and pd.notna(row.bn_age_sec) and st.binance_price is not None:
        st.binance_ts = float(t_sec) - float(row.bn_age_sec)
    up_b = _parse_levels_json(getattr(row, "up_bids_json", None), True)
    up_a = _parse_levels_json(getattr(row, "up_asks_json", None), False)
    dn_b = _parse_levels_json(getattr(row, "down_bids_json", None), True)
    dn_a = _parse_levels_json(getattr(row, "down_asks_json", None), False)
    if up_b or up_a:
        hub.apply_book(sym, "UP", {"asset_id": st.up_token, "timestamp": t_sec * 1000, "bids": [{"price": str(p), "size": str(q)} for p, q in up_b], "asks": [{"price": str(p), "size": str(q)} for p, q in up_a]}, via_ws=True)
        if has_ages and pd.notna(getattr(row, "up_book_age_sec", None)):
            st.up_local_ts = float(t_sec) - float(row.up_book_age_sec)
    if dn_b or dn_a:
        hub.apply_book(sym, "DOWN", {"asset_id": st.down_token, "timestamp": t_sec * 1000, "bids": [{"price": str(p), "size": str(q)} for p, q in dn_b], "asks": [{"price": str(p), "size": str(q)} for p, q in dn_a]}, via_ws=True)
        if has_ages and pd.notna(getattr(row, "down_book_age_sec", None)):
            st.down_local_ts = float(t_sec) - float(row.down_book_age_sec)


def _to_point(dec: Optional[SignalDecision], ts: int) -> Optional[EnterPoint]:
    if dec is None or dec.action != "ENTER" or dec.side is None:
        return None
    return EnterPoint(dec.side, ts, round(float(dec.z or 0.0), 6), round(float(dec.p_model or 0.0), 6), float(dec.sigma or 0.0), float(dec.ask_top or 0.0))


def _load_db(db_path: pathlib.Path) -> Tuple[Dict[Tuple[int, str], Any], Dict[int, Dict[str, Any]], List[int]]:
    con = sqlite3.connect(db_path)
    df_w = pd.read_sql_query("SELECT * FROM windows_42 ORDER BY window_ts, symbol", con)
    df_s = pd.read_sql_query("SELECT * FROM snapshots_1hz ORDER BY ts_sec, symbol", con)
    con.close()
    w_meta = {(int(r.window_ts), str(r.symbol)): r for r in df_w.itertuples(index=False)}
    by_sec: Dict[int, Dict[str, Any]] = {}
    for row in df_s.itertuples(index=False):
        by_sec.setdefault(int(row.ts_sec), {})[str(row.symbol)] = row
    ts_list = sorted(by_sec.keys())
    return w_meta, by_sec, ts_list


def _compare_entries(a_map: Dict[Any, EnterPoint], b_map: Dict[Any, EnterPoint]) -> Tuple[int, int, float, List[dict[str, Any]]]:
    union_keys = sorted(set(a_map.keys()) | set(b_map.keys()))
    matched = 0
    mismatches: List[dict[str, Any]] = []
    for k in union_keys:
        pa, pb = a_map.get(k), b_map.get(k)
        if same_signal(pa, pb):
            matched += 1
        else:
            mismatches.append({"key": str(k), "recorder": asdict(pa) if pa else None, "runner": asdict(pb) if pb else None})
    rate = (matched / len(union_keys)) if union_keys else 1.0
    return matched, len(union_keys), rate, mismatches


def run_parity_replay(db_path: pathlib.Path = DEFAULT_DB, report_path: pathlib.Path = REPORT_PATH) -> dict[str, Any]:
    """Run Path A (Recorder) vs Path B (Runner) chronologically and write `parity_replay_report.md`."""
    w_meta, by_sec, ts_list = _load_db(db_path)
    rec_state = RecorderStreamState()
    cfgs = {s: SniperConfig(**{**asdict(TWAP_V2_FILTERED_CONFIG), "fee_rate": PRODUCTION_FEE_RATES[s]}) for s in SYMBOLS}
    a_coin: Dict[Tuple[int, str], EnterPoint] = {}
    b_coin: Dict[Tuple[int, str], EnterPoint] = {}
    a_win: Dict[int, EnterPoint] = {}
    b_win: Dict[int, EnterPoint] = {}
    clock_ref = [float(ts_list[0]) if ts_list else 0.0]
    prev_bn: Dict[str, Optional[float]] = {}
    with tempfile.TemporaryDirectory() as td:
        cl, hub, runner = _make_replay_runner(pathlib.Path(td), clock_ref)
        for t_sec in ts_list:
            w = (t_sec // 300) * 300
            sec_rows = by_sec[t_sec]
            sec_maps = {s: {t_sec: sec_rows[s]} for s in sec_rows}
            inps = assemble_recorder_inputs((w, t_sec, t_sec - w), sec_maps, w_meta, rec_state)
            decs_a = {s: evaluate_signal(inps[s], cfgs[s], DEFAULT_MAX_AGES, enforce_warmup=True) for s in inps}
            clock_ref[0] = float(t_sec)
            hub.roll_window(float(t_sec))
            for s, row in sec_rows.items():
                _feed_ws_row(row, s, w, t_sec, w_meta.get((w, s)), (cl, hub, prev_bn))
            _views, decs_b = runner.evaluate_second(float(t_sec), w)
            _record_step(w, t_sec, decs_a, decs_b, (a_coin, b_coin, a_win, b_win))
    m_coin, n_coin, rate_coin, mm_coin = _compare_entries(a_coin, b_coin)
    m_win, n_win, rate_win, mm_win = _compare_entries(a_win, b_win)
    summary = {
        "rows_replayed": sum(len(by_sec[t]) for t in ts_list), "seconds_replayed": len(ts_list),
        "coin_window_union_signals": n_coin, "coin_window_matched": m_coin, "coin_window_parity_rate": round(rate_coin, 6),
        "single_slot_union_windows": n_win, "single_slot_matched": m_win, "single_slot_parity_rate": round(rate_win, 6),
        "coin_window_mismatches": mm_coin, "single_slot_mismatches": mm_win,
    }
    _write_report(summary, report_path)
    return summary


def _record_step(
    w: int, t_sec: int, decs_a: Dict[str, SignalDecision], decs_b: Dict[str, SignalDecision],
    maps: Tuple[Dict[Tuple[int, str], EnterPoint], Dict[Tuple[int, str], EnterPoint], Dict[int, EnterPoint], Dict[int, EnterPoint]],
) -> None:
    a_coin, b_coin, a_win, b_win = maps
    for s in SYMBOLS:
        if (w, s) not in a_coin:
            pt = _to_point(decs_a.get(s), t_sec)
            if pt is not None:
                a_coin[(w, s)] = pt
        if (w, s) not in b_coin:
            pt = _to_point(decs_b.get(s), t_sec)
            if pt is not None:
                b_coin[(w, s)] = pt
    if w not in a_win:
        best_a = choose_best_candidate(list(decs_a.values()))
        if best_a is not None:
            a_win[w] = EnterPoint(best_a.side, t_sec, round(best_a.z, 6), round(best_a.p_true, 6), float(decs_a[best_a.symbol].sigma or 0.0), float(best_a.ask))
    if w not in b_win:
        best_b = choose_best_candidate(list(decs_b.values()))
        if best_b is not None:
            b_win[w] = EnterPoint(best_b.side, t_sec, round(best_b.z, 6), round(best_b.p_true, 6), float(decs_b[best_b.symbol].sigma or 0.0), float(best_b.ask))


def _write_report(summary: dict[str, Any], path: pathlib.Path) -> None:
    lines = [
        "# Path A (Recorder) vs Path B (Runner) Parity Replay Report",
        "",
        f"- Database rows replayed: `{summary['rows_replayed']}` across `{summary['seconds_replayed']}` 1Hz ticks",
        f"- Coin-window signal union (`ENTER`): `{summary['coin_window_union_signals']}`, matched: `{summary['coin_window_matched']}`, parity: **`{summary['coin_window_parity_rate'] * 100:.2f}%`**",
        f"- Single-slot window union (`ENTER`): `{summary['single_slot_union_windows']}`, matched: `{summary['single_slot_matched']}`, parity: **`{summary['single_slot_parity_rate'] * 100:.2f}%`**",
        f"- Gate (`>= 99.0%`): **{'PASS' if min(summary['coin_window_parity_rate'], summary['single_slot_parity_rate']) >= 0.99 else 'FAIL'}**",
        "",
        "## Discrepancies",
        "",
    ]
    all_mm = [("coin_window", m) for m in summary["coin_window_mismatches"]] + [("single_slot", m) for m in summary["single_slot_mismatches"]]
    if not all_mm:
        lines.append("Zero discrepancies between Path A (Recorder) and Path B (Runner) across the entire historical recording.")
    else:
        for scope, m in all_mm:
            lines.append(f"- `{scope}` `{m['key']}`: recorder=`{m['recorder']}` vs runner=`{m['runner']}`")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    out = run_parity_replay()
    print(json.dumps(out, indent=2))
    assert out["coin_window_parity_rate"] >= 0.99 and out["single_slot_parity_rate"] >= 0.99, out
