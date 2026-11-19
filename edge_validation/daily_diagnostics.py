"""Q2 two-metric live shadow parity & Q3 read-only diagnostic tables for Section 4.2."""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

SYMBOLS: Tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
PARITY_V2_GATE: float = 0.95
MIN_PARITY_RUNNERS: int = 20
PARITY_TIME_TOL_SEC: int = 2


def _pct_pair(vals: List[float], digits: int = 4) -> Tuple[Optional[float], Optional[float]]:
    if not vals:
        return None, None
    arr = np.asarray(vals, dtype=float)
    return round(float(np.percentile(arr, 50)), digits), round(float(np.percentile(arr, 90)), digits)


def _classify_mismatch(
    ra: Optional[Tuple[str, int, float, float]],
    rb: Optional[Tuple[str, int, float, float]],
    ctx: Dict[str, Any],
) -> str:
    lag = ctx.get("rec_book_lag_ms")
    age = ctx.get("rec_book_age_sec")
    skip_r = str(ctx.get("skip_reason") or "")
    if (lag is not None and float(lag) > 500.0) or (age is not None and float(age) > 2.5) or "STALE_BOOK" in skip_r:
        return "CLOB_STALL"
    if (ra is not None and rb is not None and ra[0] == rb[0]) or (
        lag is not None and float(lag) <= 500.0 and age is not None and float(age) <= 2.5
    ):
        return "PHASE_1HZ"
    return "OTHER"


def _day_bucket(by_day: Dict[str, Dict[str, Any]], day: str) -> Dict[str, Any]:
    return by_day.setdefault(
        day,
        {
            "utc_day": day, "overlap_coin_windows": 0, "recorder_enters": 0, "runner_enters": 0,
            "union": 0, "matched": 0, "_dasks": [], "_dzs": [],
        },
    )


def _finalize_day_parity(d: Dict[str, Any]) -> Dict[str, Any]:
    d_asks = d.pop("_dasks")
    d_zs = d.pop("_dzs")
    rate = round(d["matched"] / d["union"], 4) if d["union"] else None
    da50, da90 = _pct_pair(d_asks, 4)
    dz50, dz90 = _pct_pair(d_zs, 4)
    st = "PENDING (<20)" if d["runner_enters"] < MIN_PARITY_RUNNERS else (
        "PASS" if (rate is not None and rate >= PARITY_V2_GATE) else "FAIL"
    )
    return {
        **d, "parity_rate": rate, "gate_status": st,
        "delta_ask_p50": da50, "delta_ask_p90": da90, "delta_z_p50": dz50, "delta_z_p90": dz90,
    }


def aggregate_shadow_parity_v2(
    overlap: List[Tuple[int, str]],
    rec_map: Dict[Tuple[int, str], Tuple[str, int, float, float]],
    run_map: Dict[Tuple[int, str], Tuple[str, int, float, float]],
    ctx_map: Optional[Dict[Tuple[int, str], Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Evaluate ±2s same-direction shadow parity (>=95% at >=20 Runner ENTERs) plus |Δask|/|Δz|."""
    by_day: Dict[str, Dict[str, Any]] = {}
    mismatches: List[Dict[str, Any]] = []
    counts: Dict[str, int] = {"CLOB_STALL": 0, "PHASE_1HZ": 0, "OTHER": 0}
    all_asks: List[float] = []
    all_zs: List[float] = []
    tot_u = tot_m = tot_run = 0
    ctx_lu = ctx_map or {}
    for wt, sym in overlap:
        day = datetime.fromtimestamp(wt, timezone.utc).strftime("%Y-%m-%d")
        d = _day_bucket(by_day, day)
        d["overlap_coin_windows"] += 1
        ra, rb = rec_map.get((wt, sym)), run_map.get((wt, sym))
        d["recorder_enters"] += int(ra is not None)
        if rb is not None:
            d["runner_enters"] += 1
            tot_run += 1
        if ra is None and rb is None:
            continue
        d["union"] += 1
        tot_u += 1
        matched = bool(ra and rb and ra[0] == rb[0] and abs(ra[1] - rb[1]) <= PARITY_TIME_TOL_SEC)
        if matched and ra is not None and rb is not None:
            d["matched"] += 1
            tot_m += 1
            da, dz = abs(float(ra[3]) - float(rb[3])), abs(float(ra[2]) - float(rb[2]))
            d["_dasks"].append(da)
            d["_dzs"].append(dz)
            all_asks.append(da)
            all_zs.append(dz)
            continue
        ctx = ctx_lu.get((wt, sym)) or {}
        cause = _classify_mismatch(ra, rb, ctx)
        counts[cause] += 1
        if len(mismatches) < 25:
            mismatches.append({
                "window_ts": wt, "symbol": sym, "cause": cause,
                "recorder": ra, "runner": rb,
                "skip_reason": ctx.get("skip_reason"), "rec_book_lag_ms": ctx.get("rec_book_lag_ms"),
            })
    days_out = [_finalize_day_parity(by_day[k]) for k in sorted(by_day)]
    overall_rate = round(tot_m / tot_u, 4) if tot_u else None
    da50, da90 = _pct_pair(all_asks, 4)
    dz50, dz90 = _pct_pair(all_zs, 4)
    gate_ready = tot_run >= MIN_PARITY_RUNNERS
    gate_pass = (overall_rate >= PARITY_V2_GATE) if overall_rate is not None else None
    gate_status = "PENDING (<20)" if not gate_ready else ("PASS" if gate_pass else "FAIL")
    return {
        "by_day": days_out, "overlap_coin_windows": len(overlap),
        "runner_enters": tot_run, "union_enters": tot_u, "matched": tot_m,
        "parity_rate": overall_rate, "gate_threshold": PARITY_V2_GATE,
        "gate_ready": gate_ready, "gate_pass": gate_pass, "gate_status": gate_status,
        "delta_ask_p50": da50, "delta_ask_p90": da90, "delta_z_p50": dz50, "delta_z_p90": dz90,
        "mismatch_counts": counts, "mismatches": mismatches,
    }


def _load_mismatch_ctx(
    con: sqlite3.Connection,
    overlap: List[Tuple[int, str]],
    rec_map: Dict[Tuple[int, str], Tuple[str, int, float, float]],
    run_map: Dict[Tuple[int, str], Tuple[str, int, float, float]],
) -> Dict[Tuple[int, str], Dict[str, Any]]:
    ctx_map: Dict[Tuple[int, str], Dict[str, Any]] = {}
    for wt, sym in overlap:
        ra, rb = rec_map.get((wt, sym)), run_map.get((wt, sym))
        if (ra is None and rb is None) or (ra and rb and ra[0] == rb[0] and abs(ra[1] - rb[1]) <= PARITY_TIME_TOL_SEC):
            continue
        probe_ts = int(rb[1]) if rb is not None else int(ra[1])
        side = str(rb[0]) if rb is not None else str(ra[0])
        a_col = "up_book_age_sec" if side == "UP" else "down_book_age_sec"
        l_col = "up_book_lag_ms" if side == "UP" else "down_book_lag_ms"
        srow = con.execute(
            f"SELECT {a_col}, {l_col} FROM snapshots_1hz WHERE window_ts=? AND symbol=? AND ts_sec=?",
            (wt, sym, probe_ts),
        ).fetchone()
        krow = con.execute(
            "SELECT reason FROM signal_skips_42 WHERE window_ts=? AND ts_sec=? AND model_name='v2_filtered' ORDER BY count DESC LIMIT 1",
            (wt, probe_ts),
        ).fetchone()
        ctx_map[(wt, sym)] = {
            "rec_book_age_sec": srow[0] if srow else None,
            "rec_book_lag_ms": srow[1] if srow else None,
            "skip_reason": krow[0] if krow else None,
        }
    return ctx_map


def compute_live_shadow_parity_v2(
    con: sqlite3.Connection, shadow_path: Path, count_start_ts: int
) -> Dict[str, Any]:
    """Compare live `PAPER_40U` JSONL ENTERs against Recorder `v2_filtered` DB ENTERs."""
    rows_e = con.execute(
        "SELECT ts_sec, window_ts, symbol, side, z, ask_top FROM signal_enters_42 WHERE model_name='v2_filtered' AND window_ts >= ?",
        (count_start_ts,),
    ).fetchall()
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
            wt, sym = int(ev.get("window_ts") or 0), str(ev.get("symbol") or "")
            if wt < count_start_ts or not sym:
                continue
            if ev.get("type") in ("WARM_WINDOW", "ENTER"):
                run_warm.add((wt, sym))
            if ev.get("type") == "ENTER":
                run_map.setdefault((wt, sym), (str(ev["side"]), int(ev["ts_sec"]), float(ev.get("z") or 0.0), float(ev.get("ask_top") or 0.0)))
    cov_rows = con.execute(
        "SELECT window_ts, symbol FROM snapshots_1hz WHERE window_ts >= ? AND elapsed_sec BETWEEN 150 AND 255 GROUP BY window_ts, symbol HAVING COUNT(*) >= 60",
        (count_start_ts,),
    ).fetchall()
    overlap = sorted({(int(wt), str(sym)) for wt, sym in cov_rows} & (run_warm | set(rec_map)))
    ctx_map = _load_mismatch_ctx(con, overlap, rec_map, run_map)
    return aggregate_shadow_parity_v2(overlap, rec_map, run_map, ctx_map)


def _snap_lookup(df_s: pd.DataFrame) -> Dict[Tuple[str, int], Tuple[Optional[float], Optional[float], Optional[float]]]:
    lu: Dict[Tuple[str, int], Tuple[Optional[float], Optional[float], Optional[float]]] = {}
    if len(df_s) == 0:
        return lu
    for sym, ts_s, bn_c, ua, da in zip(df_s["symbol"], df_s["ts_sec"], df_s["bn_close"], df_s["up_ask1"], df_s["down_ask1"]):
        lu[(str(sym), int(ts_s))] = (
            float(bn_c) if pd.notna(bn_c) else None,
            float(ua) if pd.notna(ua) else None,
            float(da) if pd.notna(da) else None,
        )
    return lu


def _trade_slice_summary(sub: pd.DataFrame, signals_n: Optional[int] = None) -> Dict[str, Any]:
    n = int(len(sub))
    wr = round(float((sub["pnl_per_share"] > 0).mean()), 4) if n else None
    p1 = round(float(sub["pnl_per_share"].mean()), 5) if n else None
    ph = round(float(sub["pnl_hold_to_settle"].dropna().mean()), 5) if (n and sub["pnl_hold_to_settle"].notna().any()) else None
    c1 = float(sub["cost_1s"].mean()) if (n and "cost_1s" in sub and sub["cost_1s"].notna().any()) else None
    ev_pct = round(float(p1 / c1 * 100.0), 2) if (p1 is not None and c1 and c1 > 0.0) else None
    out: Dict[str, Any] = {"trades": n, "wr": wr, "mean_pnl_1s": p1, "mean_pnl_hold": ph, "ev_on_stake_pct": ev_pct}
    if signals_n is not None:
        out["signals"] = signals_n
    return out


def _annotate_filled_trades(f1: pd.DataFrame, lu: Dict[Tuple[str, int], Tuple[Optional[float], Optional[float], Optional[float]]]) -> pd.DataFrame:
    if len(f1) == 0:
        return f1
    out = f1.copy()
    fk_flags: List[str] = []
    bn_flags: List[str] = []
    for sym, side, ts_raw in zip(out["symbol"], out["side"], out["signal_ts"]):
        s, ts_i = str(sym), int(ts_raw)
        ask_idx = 1 if str(side) == "UP" else 2
        prev_asks = [lu[(s, t)][ask_idx] for t in range(ts_i - 5, ts_i) if (s, t) in lu and lu[(s, t)][ask_idx] is not None]
        fk_flags.append("From >0.825" if (prev_asks and max(float(x) for x in prev_asks if x is not None) > 0.825) else "Other")
        bn_0 = lu.get((s, ts_i), (None,))[0]
        bn_3 = lu.get((s, ts_i - 3), (None,))[0]
        if bn_0 and bn_3 and bn_3 > 0:
            s_ret = (1.0 if str(side) == "UP" else -1.0) * (bn_0 - bn_3) / bn_3
            bn_flags.append("Adverse (<0)" if s_ret < -1e-9 else "Aligned/Flat (>=0)")
        else:
            bn_flags.append("Unknown")
    out["prev5s_bucket"] = fk_flags
    out["bn3s_bucket"] = bn_flags
    return out


def compute_q3_diagnostics(df_t: pd.DataFrame, df_s: pd.DataFrame, count_start_ts: int) -> Dict[str, Any]:
    """Compute Q3 3 read-only diagnostic breakdowns on `v2_filtered` (`prev5s >0.825`, 3s BN, per-coin)."""
    lu = _snap_lookup(df_s)
    df_all = df_t.copy()
    f1_all = _annotate_filled_trades(df_all[df_all["filled_1s"] == True] if len(df_all) else df_all, lu)
    df_post = df_all[df_all["window_ts"] >= count_start_ts] if (len(df_all) and count_start_ts > 0) else df_all
    f1_post = f1_all[f1_all["window_ts"] >= count_start_ts] if (len(f1_all) and count_start_ts > 0) else f1_all
    out: Dict[str, Any] = {}
    for scope_lbl, d_all, d_f1 in (("post_reset", df_post, f1_post), ("all", df_all, f1_all)):
        fk = {lbl: _trade_slice_summary(d_f1[d_f1["prev5s_bucket"] == lbl] if len(d_f1) else d_f1) for lbl in ("From >0.825", "Other")}
        bn3 = {lbl: _trade_slice_summary(d_f1[d_f1["bn3s_bucket"] == lbl] if len(d_f1) else d_f1) for lbl in ("Aligned/Flat (>=0)", "Adverse (<0)")}
        coins = {
            sym: _trade_slice_summary(
                d_f1[d_f1["symbol"] == sym] if len(d_f1) else d_f1,
                signals_n=int((d_all["symbol"] == sym).sum()) if len(d_all) else 0,
            )
            for sym in SYMBOLS
        }
        out[scope_lbl] = {"prev5s_ask": fk, "bn3s_dir": bn3, "by_coin": coins}
    return out


def append_shadow_parity_md(lines: List[str], sh: Dict[str, Any]) -> None:
    """Append Q2 two-metric live shadow parity & mismatch attribution tables to report lines."""
    lines += [
        "",
        f"## Live Shadow Parity (`PAPER_40U` Runner vs 4.2 Recorder, `±2s` same-dir, overlap={sh.get('overlap_coin_windows', 0)}, runner_enters={sh.get('runner_enters', 0)}, parity={sh.get('parity_rate')}, status=**{sh.get('gate_status', 'PENDING')}**)",
        "",
        f"- Gate: `>= 95.0%` once Runner `ENTER >= 20` (pre-live check). Auxiliary diffs over matched: `|Δask|` P50/P90=`{sh.get('delta_ask_p50')}`/`{sh.get('delta_ask_p90')}`, `|Δz|` P50/P90=`{sh.get('delta_z_p50')}`/`{sh.get('delta_z_p90')}`.",
        f"- Mismatch attribution counts: `{sh.get('mismatch_counts')}`",
        "",
        "| UTC Day | Overlap Coin-Win | Rec ENTER | Run ENTER | Union | Matched (`±2s`) | Parity | `|Δask|` P50/P90 | `|Δz|` P50/P90 | Gate (`>=95% @ >=20`) |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for d in sh.get("by_day") or []:
        da_s = f"{d.get('delta_ask_p50')}/{d.get('delta_ask_p90')}"
        dz_s = f"{d.get('delta_z_p50')}/{d.get('delta_z_p90')}"
        lines.append(
            f"| `{d['utc_day']}` | {d['overlap_coin_windows']} | {d['recorder_enters']} | {d['runner_enters']} | "
            f"{d['union']} | {d['matched']} | {d['parity_rate']} | {da_s} | {dz_s} | **{d['gate_status']}** |"
        )
    if sh.get("mismatches"):
        lines += ["", "| Window TS | Symbol | Cause | Skip Reason | Rec Lag (ms) | Recorder | Runner |", "|---:|---|---|---|---:|---|---|"]
        for m in sh["mismatches"]:
            lines.append(f"| {m['window_ts']} | `{m['symbol']}` | **`{m['cause']}`** | `{m.get('skip_reason')}` | {m.get('rec_book_lag_ms')} | `{m.get('recorder')}` | `{m.get('runner')}` |")


def append_q3_diagnostics_md(lines: List[str], q3: Dict[str, Any]) -> None:
    """Append Q3 3 read-only diagnostic breakdown tables (excluded from 3-config Bonferroni gate)."""
    lines += ["", "## Q3 Read-Only Diagnostic Breakdowns (`v2_filtered`, +1s Filled, Excluded from Bonferroni Gate)", "", "_Two-tier launch rule: Tier 1 `n>=300` & `98.3% CI_lo > 0`; Tier 2 100-trade 5-share pilot `EV/stake >= +8%` & standalone `BTC PnL > 0`._", "", "### 3.1 Entry Trajectory (`prev_5s_max_ask > 0.825` vs Other)", "", "| Scope | Bucket | Trades | Win Rate | Mean PnL +1s | Mean Hold PnL |", "|---|---|---:|---:|---:|---:|"]
    for sc in ("post_reset", "all"):
        for lbl, st in (q3.get(sc, {}).get("prev5s_ask") or {}).items():
            lines.append(f"| `{sc}` | `{lbl}` | {st['trades']} | {st['wr']} | {st['mean_pnl_1s']} | {st['mean_pnl_hold']} |")
    lines += ["", "### 3.2 Pre-Entry 3s Binance Direction", "", "| Scope | Binance 3s | Trades | Win Rate | Mean PnL +1s | Mean Hold PnL |", "|---|---|---:|---:|---:|---:|"]
    for sc in ("post_reset", "all"):
        for lbl, st in (q3.get(sc, {}).get("bn3s_dir") or {}).items():
            lines.append(f"| `{sc}` | `{lbl}` | {st['trades']} | {st['wr']} | {st['mean_pnl_1s']} | {st['mean_pnl_hold']} |")
    lines += ["", "### 3.3 Per-Coin Diagnostic (`v2_filtered`, Pre-11/21 Baseline + Scaling Check)", "", "| Scope | Coin | Signals | Filled +1s | Win Rate | Mean PnL +1s | Mean Hold PnL | EV / Stake % |", "|---|---|---:|---:|---:|---:|---:|---:|"]
    for sc in ("post_reset", "all"):
        for sym, st in (q3.get(sc, {}).get("by_coin") or {}).items():
            lines.append(f"| `{sc}` | `{sym}` | {st.get('signals', 0)} | {st['trades']} | {st['wr']} | {st['mean_pnl_1s']} | {st['mean_pnl_hold']} | {st['ev_on_stake_pct']} |")
