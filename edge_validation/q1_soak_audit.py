#!/usr/bin/env python3
"""Lightweight read-only Q1 soak auditor for live Section 4.2 5m Recorder + Runner."""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Optional

REPO = pathlib.Path(__file__).resolve().parent.parent
DEFAULT_DB = REPO / "data/validation_42/live_3coin_42.db"
DEFAULT_RECORDER_LOG = REPO / "data/validation_42/recorder_42.log"
DEFAULT_SHADOW_LOG = REPO / "data/validation_42/runner_shadow_events.jsonl"
DEFAULT_STATUS_URL = "http://127.0.0.1:8088/api/sniper/status"
SYMBOLS: tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
DEFAULT_Q1_START_TS = 1795078200
FORENSIC_LOSS_WINDOW_TS = 1795080900

EXPECTED_FROZEN_SHA256: dict[str, str] = {
    "app.py": "6a681639075e5a7f",
    "engine/oracle_sniper.py": "fa926ed270b58070",
    "engine/signal_core.py": "19aa314df48db6bb",
    "engine/oracle_sniper_feeds.py": "14e60024d83a993d",
    "engine/oracle_sniper_runner.py": "89e14fa740495862",
    "engine/oracle_sniper_ledger.py": "9bbf603be7fafc77",
    "engine/scanner.py": "160374bb1e8136df",
    "engine/master_strategy.py": "9e997bbc7bb32dc7",
    "edge_validation/live_recorder_42.py": "7dc6d6d9dda1ade4",
    "edge_validation/frozen_config.json": "5c066dc175cd9041",
    "edge_validation/sqlite_writer.py": "96730e8cc9ca3e30",
    "edge_validation/daily_diagnostics.py": "367df65ec1d0a04e",
}


CORE_5M_FILES: frozenset[str] = frozenset(k for k in EXPECTED_FROZEN_SHA256 if k not in ("app.py", "engine/scanner.py"))


def check_frozen_hashes(
    repo_dir: pathlib.Path = REPO,
    expected_hashes: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """Verify SHA256 prefixes for frozen core files without modifying them."""
    ref = EXPECTED_FROZEN_SHA256 if expected_hashes is None else expected_hashes
    mismatches: list[dict[str, str]] = []
    actual: dict[str, str] = {}
    for rel, expected in ref.items():
        fp = repo_dir / rel
        got = hashlib.sha256(fp.read_bytes()).hexdigest()[:16] if fp.exists() else "MISSING"
        actual[rel] = got
        if got != expected:
            mismatches.append({"file": rel, "expected": expected, "actual": got})
    core_mismatches = [m for m in mismatches if m["file"] in CORE_5M_FILES]
    core_total = sum(1 for k in ref if k in CORE_5M_FILES)
    return {
        "matched": len(ref) - len(mismatches), "total": len(ref), "ok": len(mismatches) == 0,
        "core_5m_matched": core_total - len(core_mismatches), "core_5m_total": core_total,
        "core_5m_ok": len(core_mismatches) == 0, "mismatches": mismatches, "actual": actual,
    }


def count_recorder_1013(log_path: pathlib.Path) -> dict[str, Any]:
    """Count `code=1013` events and WS reconnect warnings in `recorder_42.log`."""
    if not log_path.exists():
        return {"exists": False, "code_1013": 0, "ws_lines": 0, "lines": []}
    lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    hits_1013 = [ln for ln in lines if "code=1013" in ln or "1013" in ln.replace("1013_total", "")]
    ws_lines = [ln for ln in lines if "CLOB WS" in ln]
    return {"exists": True, "code_1013": len(hits_1013), "ws_lines": len(ws_lines), "lines": hits_1013[-20:]}


def fetch_runner_status(url: str = DEFAULT_STATUS_URL, timeout_sec: float = 3.0) -> Optional[dict[str, Any]]:
    """Fetch live Runner `/api/sniper/status` JSON, returning `None` if unreachable."""
    try:
        with urllib.request.urlopen(url, timeout=timeout_sec) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, ValueError, OSError):
        return None


def snapshot_baseline(
    out_path: pathlib.Path,
    recorder_log: pathlib.Path = DEFAULT_RECORDER_LOG,
    status_url: str = DEFAULT_STATUS_URL,
    repo_dir: pathlib.Path = REPO,
    now_ts: Optional[int] = None,
) -> dict[str, Any]:
    """Capture clean idle-soak start baseline JSON from running Recorder + Runner."""
    t_now = int(time.time() if now_ts is None else now_ts)
    rec_1013 = count_recorder_1013(recorder_log)
    st = fetch_runner_status(status_url) or {}
    feeds = st.get("feeds") or {}
    skips = (st.get("stats") or {}).get("skips") or {}
    payload = {
        "baseline_ts": t_now,
        "baseline_window_ts": (t_now // 300) * 300,
        "recorder_1013": int(rec_1013["code_1013"]),
        "runner_available": bool(st),
        "runner_running": bool(st.get("running", False)),
        "runner_1013": int(feeds.get("clob_1013_count") or 0),
        "runner_reconnects": int(feeds.get("clob_reconnects") or 0),
        "runner_skips": {str(k): int(v) for k, v in skips.items()},
        "frozen_check": check_frozen_hashes(repo_dir),
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def audit_runner_shadow_enters(shadow_log: pathlib.Path, since_ts: int) -> dict[str, Any]:
    """Audit Runner shadow `ENTER` events (`ts_sec >= since_ts`) for stale lag/age."""
    if not shadow_log.exists():
        return {"total": 0, "stale_count": 0, "max_book_age_ms": None, "max_book_lag_ms": None, "stale_rows": [], "recent_rows": []}
    rows: list[dict[str, Any]] = []
    stale_rows: list[dict[str, Any]] = []
    for raw in shadow_log.read_text(encoding="utf-8", errors="replace").splitlines():
        if not raw.strip():
            continue
        ev = json.loads(raw)
        if ev.get("type") != "ENTER" or int(ev.get("ts_sec") or 0) < since_ts:
            continue
        ages = ev.get("ages_ms") or {}
        lags = ev.get("lags_ms") or {}
        b_age = float(ages["book"]) if ages.get("book") is not None else None
        b_lag = float(lags["book_ms"]) if lags.get("book_ms") is not None else None
        is_stale = bool((b_lag is not None and b_lag > 1000.0) or (b_age is not None and b_age > 1500.0))
        rec = {
            "ts_sec": int(ev["ts_sec"]), "window_ts": int(ev["window_ts"]),
            "symbol": str(ev["symbol"]), "side": str(ev["side"]),
            "ask_top": ev.get("ask_top"), "book_age_ms": b_age, "book_lag_ms": b_lag,
            "cl_twap_age_ms": ages.get("cl_twap"), "stale": is_stale,
        }
        rows.append(rec)
        if is_stale:
            stale_rows.append(rec)
    age_vals = [r["book_age_ms"] for r in rows if r["book_age_ms"] is not None]
    lag_vals = [r["book_lag_ms"] for r in rows if r["book_lag_ms"] is not None]
    return {
        "total": len(rows), "stale_count": len(stale_rows),
        "max_book_age_ms": round(max(age_vals), 2) if age_vals else None,
        "max_book_lag_ms": round(max(lag_vals), 2) if lag_vals else None,
        "stale_rows": stale_rows, "recent_rows": rows[-10:],
    }


def _classify_recorder_enter(row: tuple[Any, ...]) -> dict[str, Any]:
    ts_sec, win_ts, sym, model_name, side, ask_top, b_age_s, up_age, dn_age, up_lag, dn_lag = row
    side_age = up_age if side == "UP" else dn_age
    side_lag = up_lag if side == "UP" else dn_lag
    eff_age = float(side_age) if side_age is not None else (float(b_age_s) * 1000.0 if b_age_s is not None else None)
    eff_lag = float(side_lag) if side_lag is not None else None
    is_stale = bool((eff_lag is not None and eff_lag > 1000.0) or (eff_age is not None and eff_age > 1500.0))
    return {
        "ts_sec": int(ts_sec), "window_ts": int(win_ts), "symbol": str(sym), "model": str(model_name),
        "side": str(side), "ask_top": ask_top, "book_age_ms": eff_age, "book_lag_ms": eff_lag, "stale": is_stale,
    }


def audit_recorder_enters(con: sqlite3.Connection, since_ts: int) -> dict[str, Any]:
    """Audit Recorder `signal_enters_42` joined with `snapshots_1hz` (raw vs single-slot first entry)."""
    raw_rows = con.execute(
        """
        SELECT e.ts_sec, e.window_ts, e.symbol, e.model_name, e.side, e.ask_top, e.book_age_sec,
               s.up_book_age_ms, s.down_book_age_ms, s.up_book_lag_ms, s.down_book_lag_ms
        FROM signal_enters_42 e
        LEFT JOIN snapshots_1hz s ON s.ts_sec = e.ts_sec AND s.symbol = e.symbol
        WHERE e.ts_sec >= ? ORDER BY e.window_ts, e.ts_sec, e.symbol
        """,
        (since_ts,),
    ).fetchall()
    parsed = [_classify_recorder_enter(r) for r in raw_rows]
    seen_wins: set[int] = set()
    first_entries: list[dict[str, Any]] = []
    for rec in parsed:
        if rec["window_ts"] not in seen_wins:
            seen_wins.add(rec["window_ts"])
            first_entries.append(rec)
    raw_stale = [r for r in parsed if r["stale"]]
    first_stale = [r for r in first_entries if r["stale"]]
    return {
        "raw_total": len(parsed), "raw_stale_count": len(raw_stale), "raw_stale_rows": raw_stale,
        "first_entry_total": len(first_entries), "first_entry_stale_count": len(first_stale), "first_entry_stale_rows": first_stale,
        "all_rows": parsed[-15:],
    }


def _percentile(sorted_vals: list[float], q: float) -> Optional[float]:
    if not sorted_vals:
        return None
    idx = min(len(sorted_vals) - 1, max(0, int(len(sorted_vals) * q)))
    return round(float(sorted_vals[idx]), 2)


def _query_symbol_lag(con: sqlite3.Connection, since_ts: int, sym: str) -> dict[str, Any]:
    """Compute lag, age, and entry-window health summary for a single symbol."""
    agg = con.execute(
        """
        SELECT COUNT(*),
               SUM(CASE WHEN up_book_lag_ms > 2000 OR down_book_lag_ms > 2000 THEN 1 ELSE 0 END),
               SUM(CASE WHEN up_book_lag_ms > 1000 OR down_book_lag_ms > 1000 THEN 1 ELSE 0 END),
               SUM(CASE WHEN up_book_age_ms > 1500 OR down_book_age_ms > 1500 THEN 1 ELSE 0 END),
               SUM(CASE WHEN elapsed_sec BETWEEN 150 AND 255 THEN 1 ELSE 0 END),
               SUM(CASE WHEN elapsed_sec BETWEEN 150 AND 255 AND (up_book_age_ms IS NULL OR up_book_age_ms > 1500 OR down_book_age_ms IS NULL OR down_book_age_ms > 1500) THEN 1 ELSE 0 END),
               SUM(CASE WHEN elapsed_sec BETWEEN 150 AND 255 AND (up_ask1 IS NULL OR down_ask1 IS NULL) THEN 1 ELSE 0 END),
               MAX(MAX(COALESCE(up_book_lag_ms, 0.0), COALESCE(down_book_lag_ms, 0.0))),
               MIN(ts_sec), MAX(ts_sec)
        FROM snapshots_1hz WHERE ts_sec >= ? AND symbol = ?
        """,
        (since_ts, sym),
    ).fetchone()
    lags = sorted(
        float(r[0]) for r in con.execute(
            "SELECT MAX(COALESCE(up_book_lag_ms, -1.0), COALESCE(down_book_lag_ms, -1.0)) "
            "FROM snapshots_1hz WHERE ts_sec >= ? AND symbol = ?",
            (since_ts, sym),
        ).fetchall() if float(r[0]) >= 0.0
    )
    n, entry_s = int(agg[0] or 0), int(agg[4] or 0)
    span_hr = round((int(agg[9]) - int(agg[8])) / 3600.0, 2) if (agg[8] is not None and agg[9] is not None) else 0.0
    return {
        "rows": n, "span_hours": span_hr,
        "gt_2000ms_sec": int(agg[1] or 0), "gt_2000ms_pct": round((int(agg[1] or 0) / max(n, 1)) * 100.0, 3),
        "gt_1000ms_sec": int(agg[2] or 0), "age_gt_1500ms_sec": int(agg[3] or 0),
        "entry_window_secs": entry_s, "entry_age_gt1500_sec": int(agg[5] or 0),
        "entry_age_gt1500_pct": round((int(agg[5] or 0) / max(entry_s, 1)) * 100.0, 3),
        "entry_no_ask_sec": int(agg[6] or 0),
        "p50_lag_ms": _percentile(lags, 0.50), "p95_lag_ms": _percentile(lags, 0.95),
        "p99_lag_ms": _percentile(lags, 0.99),
        "max_lag_ms": round(float(agg[7]), 2) if agg[7] is not None else None,
    }


def audit_per_coin_lag(con: sqlite3.Connection, since_ts: int) -> dict[str, Any]:
    """Compute per-coin `>2000ms`, `>1000ms`, `age>1500ms`, entry-window health & loop gaps."""
    symbols_out = {sym: _query_symbol_lag(con, since_ts, sym) for sym in SYMBOLS}
    ts_vals = [int(r[0]) for r in con.execute("SELECT DISTINCT ts_sec FROM snapshots_1hz WHERE ts_sec >= ? ORDER BY ts_sec", (since_ts,)).fetchall()]
    gaps = [(ts_vals[i - 1], ts_vals[i], ts_vals[i] - ts_vals[i - 1]) for i in range(1, len(ts_vals)) if ts_vals[i] - ts_vals[i - 1] > 1]
    return {"symbols": symbols_out, "unique_seconds": len(ts_vals), "loop_gaps_gt_1s": len(gaps), "top_gaps": gaps[:5]}


def inspect_forensic_trade(con: sqlite3.Connection, shadow_log: pathlib.Path) -> dict[str, Any]:
    """Extract Runner & Recorder exact `book_age` / `book_lag` for `1795080900 BTCUSDT UP`."""
    runner_rows: list[dict[str, Any]] = []
    if shadow_log.exists():
        for raw in shadow_log.read_text(encoding="utf-8", errors="replace").splitlines():
            if not raw.strip():
                continue
            ev = json.loads(raw)
            if ev.get("type") == "ENTER" and int(ev.get("window_ts") or 0) == FORENSIC_LOSS_WINDOW_TS and ev.get("symbol") == "BTCUSDT":
                runner_rows.append({
                    "ts_sec": int(ev["ts_sec"]), "elapsed": int(ev["ts_sec"]) - FORENSIC_LOSS_WINDOW_TS,
                    "ask_top": ev.get("ask_top"), "ages_ms": ev.get("ages_ms"), "lags_ms": ev.get("lags_ms"),
                })
    rec_rows = [
        {"ts_sec": int(r[0]), "elapsed": int(r[1]), "up_book_age_ms": r[2], "up_book_lag_ms": r[3], "up_ask1": r[4]}
        for r in con.execute(
            "SELECT ts_sec, elapsed_sec, up_book_age_ms, up_book_lag_ms, up_ask1 "
            "FROM snapshots_1hz WHERE window_ts = ? AND symbol = 'BTCUSDT' AND elapsed_sec BETWEEN 250 AND 252 ORDER BY ts_sec",
            (FORENSIC_LOSS_WINDOW_TS,),
        ).fetchall()
    ]
    return {"window_ts": FORENSIC_LOSS_WINDOW_TS, "runner_enter_events": runner_rows, "recorder_snapshots_250_252": rec_rows}


def _compute_runner_delta(st: Optional[dict[str, Any]], baseline: Optional[dict[str, Any]], rec_1013: dict[str, Any]) -> dict[str, Any]:
    """Compute Runner & Recorder 1013/reconnect/skip deltas vs baseline."""
    feeds = (st or {}).get("feeds") or {}
    skips_now = {str(k): int(v) for k, v in ((st or {}).get("stats") or {}).get("skips", {}).items()}
    base_skips = (baseline or {}).get("runner_skips") or {}
    rec_total = int(rec_1013["code_1013"])
    rec_base = int((baseline or {}).get("recorder_1013") or 0)
    run_1013_now = int(feeds.get("clob_1013_count") or 0)
    run_1013_base = int((baseline or {}).get("runner_1013") or 0)
    reconn_now = int(feeds.get("clob_reconnects") or 0)
    reconn_base = int((baseline or {}).get("runner_reconnects") or 0)
    skip_delta = {k: skips_now.get(k, 0) - int(base_skips.get(k, 0)) for k in set(skips_now) | set(base_skips)}
    stale_skips = int(skip_delta.get("STALE_BOOK", 0))
    total_evals = sum(max(0, v) for k, v in skip_delta.items() if k != "WARMUP")
    return {
        "runner_available": st is not None,
        "recorder_1013_total": rec_total,
        "recorder_1013_delta": rec_total - rec_base if baseline else rec_total,
        "runner_1013_total": run_1013_now,
        "runner_1013_delta": (run_1013_now - run_1013_base) if (st and baseline) else (run_1013_now if st else None),
        "runner_reconnects_total": reconn_now if st else None,
        "runner_reconnects_delta": (reconn_now - reconn_base) if (st and baseline) else (reconn_now if st else None),
        "runner_stale_book_skips": stale_skips if st else None,
        "runner_no_book_skips": int(skip_delta.get("NO_BOOK", 0)) if st else None,
        "runner_stale_book_pct": round((stale_skips / max(total_evals, 1)) * 100.0, 4) if st else None,
        "runner_evals_non_warmup": total_evals if st else None,
    }


def _evaluate_gates(rec_enters: dict[str, Any], run_enters: dict[str, Any], per_coin: dict[str, Any], delta: dict[str, Any]) -> dict[str, Any]:
    """Evaluate all 4 Claude Section 1 Q1 acceptance gates explicitly without hiding edge cases."""
    rec_1013 = delta["recorder_1013_delta"]
    run_1013 = delta["runner_1013_delta"]
    sol_gt2 = per_coin["symbols"]["SOLUSDT"]["gt_2000ms_sec"]
    eth_gt2 = per_coin["symbols"]["ETHUSDT"]["gt_2000ms_sec"]
    gate1 = (run_enters["stale_count"] == 0) and (rec_enters["raw_stale_count"] == 0)
    gate2 = (rec_1013 == 0) and (run_1013 in (0, None))
    gate3 = (per_coin["symbols"]["ETHUSDT"]["gt_2000ms_pct"] <= 0.5) and (per_coin["symbols"]["SOLUSDT"]["gt_2000ms_pct"] <= 0.1)
    return {
        "gate1_zero_stale_decisions": gate1,
        "gate1_first_entry_only_zero_stale": (run_enters["stale_count"] == 0) and (rec_enters["first_entry_stale_count"] == 0),
        "gate2_zero_1013": gate2,
        "gate3_eth_sol_near_zero_gt2s": gate3,
        "eth_gt2000ms_sec": eth_gt2,
        "sol_gt2000ms_sec": sol_gt2,
    }


def run_q1_audit(
    db_path: pathlib.Path = DEFAULT_DB,
    recorder_log: pathlib.Path = DEFAULT_RECORDER_LOG,
    shadow_log: pathlib.Path = DEFAULT_SHADOW_LOG,
    since_ts: int = DEFAULT_Q1_START_TS,
    baseline_path: Optional[pathlib.Path] = None,
    status_url: Optional[str] = DEFAULT_STATUS_URL,
    repo_dir: pathlib.Path = REPO,
) -> dict[str, Any]:
    """Run read-only Q1 soak audit across Recorder DB, logs, and Runner API."""
    baseline = json.loads(baseline_path.read_text(encoding="utf-8")) if baseline_path and baseline_path.exists() else None
    eff_since = int(baseline["baseline_ts"]) if baseline and "baseline_ts" in baseline else int(since_ts)
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    con.execute("PRAGMA busy_timeout=5000")
    try:
        run_enters = audit_runner_shadow_enters(shadow_log, eff_since)
        rec_enters = audit_recorder_enters(con, eff_since)
        per_coin = audit_per_coin_lag(con, eff_since)
        forensics = inspect_forensic_trade(con, shadow_log)
    finally:
        con.close()
    rec_1013 = count_recorder_1013(recorder_log)
    st = fetch_runner_status(status_url) if status_url else None
    delta = _compute_runner_delta(st, baseline, rec_1013)
    gates = _evaluate_gates(rec_enters, run_enters, per_coin, delta)
    return {
        "since_ts": eff_since,
        "has_baseline": baseline is not None,
        "runner_enters": run_enters,
        "recorder_enters": rec_enters,
        "per_coin_lag": per_coin,
        "reconnect_and_1013": delta,
        "gates": gates,
        "forensics_1795080900": forensics,
        "frozen_check": check_frozen_hashes(repo_dir),
    }


def render_q1_markdown(res: dict[str, Any]) -> str:
    """Render clean Markdown summary matching Claude's 4 Section 1 metrics + forensics."""
    g, re_r, rc_r, dlt = res["gates"], res["runner_enters"], res["recorder_enters"], res["reconnect_and_1013"]
    lines = [
        f"# Q1 潔淨運行報告 (`since_ts={res['since_ts']}`)",
        "",
        "## 1. 四項修正版驗收指標（`CLAUDE_RESPONSE_10` 第 1 節）",
        "",
        "| 指標 | Runner | Recorder (Raw ENTER / Single-Slot First) | 門檻 | 判定 |",
        "|---|---:|---:|---|---|",
        f"| 1. 舊資料決策 (`lag>1000ms` 或 `age>1500ms`) | `{re_r['stale_count']}/{re_r['total']}` | `{rc_r['raw_stale_count']}/{rc_r['raw_total']}` / `{rc_r['first_entry_stale_count']}/{rc_r['first_entry_total']}` | `0` | `{'PASS' if g['gate1_zero_stale_decisions'] else ('FIRST_ONLY' if g['gate1_first_entry_only_zero_stale'] else 'FAIL')}` |",
        f"| 2. `code=1013` 斷線次數 | `{dlt['runner_1013_delta']}` | `{dlt['recorder_1013_delta']}` | `0` | `{'PASS' if g['gate2_zero_1013'] else 'FAIL'}` |",
        f"| 4. Runner 重連 / `STALE_BOOK` 佔比 | `{dlt['runner_reconnects_delta']}` 次 / `{dlt['runner_stale_book_pct']}%` | 1 Hz 缺秒 `{res['per_coin_lag']['loop_gaps_gt_1s']}` | 觀察報告 | `INFO` |",
        "",
        "## 2. 分幣別 WebSocket 延遲與進場時段 `[150,255]s` 健康度",
        "",
        "| Coin | Rows | Span (h) | `>2000ms` (s / %) | `>1000ms` (s) | `age>1500ms` (s) | Entry `[150,255]s` `age>1500ms` | P50 / P95 / P99 / Max (ms) |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for sym in SYMBOLS:
        s = res["per_coin_lag"]["symbols"][sym]
        lines.append(
            f"| `{sym}` | {s['rows']} | {s['span_hours']} | {s['gt_2000ms_sec']} ({s['gt_2000ms_pct']}%) | {s['gt_1000ms_sec']} | {s['age_gt_1500ms_sec']} | "
            f"{s['entry_age_gt1500_sec']}/{s['entry_window_secs']} ({s['entry_age_gt1500_pct']}%) | {s['p50_lag_ms']} / {s['p95_lag_ms']} / {s['p99_lag_ms']} / {s['max_lag_ms']} |"
        )
    f = res["forensics_1795080900"]
    lines += [
        "",
        f"## 3. Runner `-4.04U` (`{f['window_ts']} BTCUSDT UP`) 進場當秒鑑識",
        "",
    ]
    for ev in f["runner_enter_events"]:
        ages, lags = ev["ages_ms"] or {}, ev["lags_ms"] or {}
        lines.append(f"- Runner `ENTER` (`ts={ev['ts_sec']}`, `t={ev['elapsed']}s`, `ask={ev['ask_top']}`): `book_age={ages.get('book')} ms`, `book_lag={lags.get('book_ms')} ms`, `cl_twap_age={ages.get('cl_twap')} ms`。")
    for snap in f["recorder_snapshots_250_252"]:
        lines.append(f"- Recorder 1 Hz (`ts={snap['ts_sec']}`, `t={snap['elapsed']}s`, `up_ask1={snap['up_ask1']}`): `up_book_age={snap['up_book_age_ms']} ms`, `up_book_lag={snap['up_book_lag_ms']} ms`。")
    lines.append(
        f"- 凍結檔案指紋檢查：`{res['frozen_check']['matched']}/{res['frozen_check']['total']}` 符合（`ok={res['frozen_check']['ok']}`，"
        f"5m 核心 `{res['frozen_check']['core_5m_matched']}/{res['frozen_check']['core_5m_total']}` `core_5m_ok={res['frozen_check']['core_5m_ok']}`）。"
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    """CLI entrypoint for baseline snapshotting and Q1 soak audit reporting."""
    ap = argparse.ArgumentParser(description="Read-only Q1 Clean Soak Auditor")
    ap.add_argument("--db", type=pathlib.Path, default=DEFAULT_DB)
    ap.add_argument("--recorder-log", type=pathlib.Path, default=DEFAULT_RECORDER_LOG)
    ap.add_argument("--shadow-log", type=pathlib.Path, default=DEFAULT_SHADOW_LOG)
    ap.add_argument("--status-url", type=str, default=DEFAULT_STATUS_URL)
    ap.add_argument("--since-ts", type=int, default=DEFAULT_Q1_START_TS)
    ap.add_argument("--baseline", type=pathlib.Path, default=None)
    ap.add_argument("--snapshot-baseline", type=pathlib.Path, default=None)
    ap.add_argument("--out-md", type=pathlib.Path, default=None)
    args = ap.parse_args()
    if args.snapshot_baseline is not None:
        print(json.dumps(snapshot_baseline(args.snapshot_baseline, args.recorder_log, args.status_url), indent=2))
        return
    res = run_q1_audit(args.db, args.recorder_log, args.shadow_log, args.since_ts, args.baseline, args.status_url or None)
    if args.out_md is not None:
        args.out_md.write_text(render_q1_markdown(res), encoding="utf-8")
    print(json.dumps({k: v for k, v in res.items() if k != "frozen_check"} | {"frozen_ok": res["frozen_check"]["ok"], "frozen_matched": res["frozen_check"]["matched"]}, indent=2))


if __name__ == "__main__":
    main()
