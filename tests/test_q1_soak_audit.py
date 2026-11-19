#!/usr/bin/env python3
"""Unit tests for `edge_validation/q1_soak_audit.py` using isolated temp fixtures."""
from __future__ import annotations

import hashlib
import json
import pathlib
import sqlite3
import sys

REPO = pathlib.Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from edge_validation import q1_soak_audit as qa
from edge_validation.live_recorder_42 import init_db


def _seed_temp_env(tmp_path: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path, pathlib.Path, pathlib.Path]:
    """Create isolated DB, recorder log, shadow JSONL, and baseline JSON fixtures."""
    db_path = tmp_path / "live_3coin_42.db"
    con = init_db(db_path)
    base_ts = 1795090200
    rows = [
        (1795081151, 1795080900, "BTCUSDT", 251, 0.79, 0.07, 454.23, 0.80, 10.0, 20.0),
        (base_ts + 200, base_ts, "BTCUSDT", 200, 0.80, 4.0, 400.0, 0.80, 4.0, 400.0),
        (base_ts + 201, base_ts, "BTCUSDT", 201, 0.81, 60.0, 1300.0, 0.81, 5.0, 2100.0),
        (base_ts + 202, base_ts, "ETHUSDT", 202, 0.75, 10.0, 90.0, 0.75, 10.0, 90.0),
        (base_ts + 203, base_ts, "SOLUSDT", 203, 0.76, 8.0, 80.0, 0.76, 8.0, 2200.0),
    ]
    for ts, wt, sym, el, u_ask, u_age, u_lag, d_ask, d_age, d_lag in rows:
        con.execute(
            """
            INSERT INTO snapshots_1hz (
                ts_sec, window_ts, symbol, elapsed_sec, up_ask1, down_ask1,
                up_book_age_ms, up_book_lag_ms, down_book_age_ms, down_book_lag_ms
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (ts, wt, sym, el, u_ask, d_ask, u_age, u_lag, d_age, d_lag),
        )
    con.execute(
        "INSERT INTO signal_enters_42 (ts_sec,window_ts,symbol,model_name,side,z,p_model,sigma,ask_top,book_age_sec) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (base_ts + 200, base_ts, "BTCUSDT", "v2_filtered", "UP", 1.4, 0.92, 4e-5, 0.80, 0.004),
    )
    con.execute(
        "INSERT INTO signal_enters_42 (ts_sec,window_ts,symbol,model_name,side,z,p_model,sigma,ask_top,book_age_sec) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (base_ts + 201, base_ts, "BTCUSDT", "v2_filtered", "UP", 1.45, 0.93, 4e-5, 0.81, 0.06),
    )
    con.commit()
    con.close()

    rec_log = tmp_path / "recorder_42.log"
    rec_log.write_text(
        "CLOB WS BTCUSDT closed (code=1013, 1013_total=1)\n"
        "CLOB WS BTCUSDT closed (code=1013, 1013_total=2)\n",
        encoding="utf-8",
    )
    sh_log = tmp_path / "runner_shadow_events.jsonl"
    events = [
        {"type": "ENTER", "ts_sec": 1795081150, "window_ts": 1795080900, "symbol": "BTCUSDT", "side": "UP", "ask_top": 0.78, "ages_ms": {"book": 1.0, "cl_twap": 311.0}, "lags_ms": {"book_ms": 18.55}},
        {"type": "ENTER", "ts_sec": base_ts + 200, "window_ts": base_ts, "symbol": "ETHUSDT", "side": "UP", "ask_top": 0.75, "ages_ms": {"book": 12.0}, "lags_ms": {"book_ms": 95.0}},
        {"type": "ENTER", "ts_sec": base_ts + 205, "window_ts": base_ts, "symbol": "SOLUSDT", "side": "DOWN", "ask_top": 0.77, "ages_ms": {"book": 1600.0}, "lags_ms": {"book_ms": 50.0}},
    ]
    sh_log.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")

    base_json = tmp_path / "baseline.json"
    base_json.write_text(
        json.dumps({
            "baseline_ts": base_ts,
            "recorder_1013": 2,
            "runner_1013": 0,
            "runner_reconnects": 100,
            "runner_skips": {"STALE_BOOK": 3, "NO_BOOK": 1000, "Z_LOW": 4000, "WARMUP": 1800},
        }),
        encoding="utf-8",
    )
    return db_path, rec_log, sh_log, base_json


def test_q1_soak_audit_metrics_and_deltas(tmp_path: pathlib.Path, monkeypatch: object) -> None:
    """Verify stale ENTER detection, baseline deltas, per-coin >2000ms counts, and forensics."""
    db_path, rec_log, sh_log, base_json = _seed_temp_env(tmp_path)
    fake_status = {
        "running": True,
        "feeds": {"clob_1013_count": 0, "clob_reconnects": 112},
        "stats": {"skips": {"STALE_BOOK": 4, "NO_BOOK": 1005, "Z_LOW": 4994, "WARMUP": 1800}},
    }
    monkeypatch.setattr(qa, "fetch_runner_status", lambda *_a, **_k: fake_status)

    res = qa.run_q1_audit(db_path=db_path, recorder_log=rec_log, shadow_log=sh_log, baseline_path=base_json)
    assert res["runner_enters"]["total"] == 2
    assert res["runner_enters"]["stale_count"] == 1
    assert res["recorder_enters"]["raw_total"] == 2 and res["recorder_enters"]["raw_stale_count"] == 1
    assert res["recorder_enters"]["first_entry_total"] == 1 and res["recorder_enters"]["first_entry_stale_count"] == 0
    assert res["reconnect_and_1013"]["recorder_1013_delta"] == 0
    assert res["reconnect_and_1013"]["runner_1013_delta"] == 0
    assert res["reconnect_and_1013"]["runner_reconnects_delta"] == 12
    assert res["reconnect_and_1013"]["runner_stale_book_skips"] == 1
    assert abs(res["reconnect_and_1013"]["runner_stale_book_pct"] - 0.1) < 1e-6
    assert res["per_coin_lag"]["symbols"]["BTCUSDT"]["gt_2000ms_sec"] == 1
    assert res["per_coin_lag"]["symbols"]["ETHUSDT"]["gt_2000ms_sec"] == 0
    assert res["per_coin_lag"]["symbols"]["SOLUSDT"]["gt_2000ms_sec"] == 1
    assert res["forensics_1795080900"]["runner_enter_events"][0]["lags_ms"]["book_ms"] == 18.55
    assert res["forensics_1795080900"]["recorder_snapshots_250_252"][0]["up_book_lag_ms"] == 454.23
    assert res["gates"]["gate2_zero_1013"] is True and res["gates"]["gate1_zero_stale_decisions"] is False
    md = qa.render_q1_markdown(res)
    assert "1795080900" in md and "gate" not in md.lower() or "PASS" in md or "FAIL" in md


def test_q1_soak_clean_pass_gates(tmp_path: pathlib.Path, monkeypatch: object) -> None:
    """Verify PASS gates when clean idle soak has 0 stale ENTERs, 0 1013, and 0 ETH/SOL >2s."""
    db_path, rec_log, sh_log, base_json = _seed_temp_env(tmp_path)
    sh_log.write_text(
        json.dumps({"type": "ENTER", "ts_sec": 1795090400, "window_ts": 1795090200, "symbol": "ETHUSDT", "side": "UP", "ask_top": 0.75, "ages_ms": {"book": 5.0}, "lags_ms": {"book_ms": 80.0}}) + "\n",
        encoding="utf-8",
    )
    con = sqlite3.connect(db_path)
    con.execute("DELETE FROM signal_enters_42 WHERE ts_sec = 1795090401")
    con.execute("DELETE FROM snapshots_1hz WHERE symbol = 'SOLUSDT'")
    con.commit()
    con.close()
    monkeypatch.setattr(qa, "fetch_runner_status", lambda *_a, **_k: {"feeds": {"clob_1013_count": 0, "clob_reconnects": 100}, "stats": {"skips": {"STALE_BOOK": 3}}})
    res = qa.run_q1_audit(db_path=db_path, recorder_log=rec_log, shadow_log=sh_log, baseline_path=base_json)
    assert res["gates"]["gate1_zero_stale_decisions"] is True
    assert res["gates"]["gate2_zero_1013"] is True
    assert res["gates"]["gate3_eth_sol_near_zero_gt2s"] is True
    assert res["frozen_check"]["core_5m_ok"] is True


def test_count_recorder_1013_ignores_total_counter_and_detects_drift(tmp_path: pathlib.Path) -> None:
    """Verify `1013_total=N` in generic WS reconnect lines is not double-counted as a 1013 close."""
    log_p = tmp_path / "recorder.log"
    log_p.write_text(
        "\n".join([
            "CLOB WS BTCUSDT closed (code=1013, 1013_total=6)",
            "Binance WS reconnect: no close frame received or sent",
            "CLOB WS ETHUSDT reconnect (1013_total=6): ",
            "CLOB WS SOLUSDT reconnect (1013_total=6): ",
        ]) + "\n",
        encoding="utf-8",
    )
    out = qa.count_recorder_1013(log_p)
    assert out["code_1013"] == 1
    assert out["ws_lines"] == 3

    f1 = tmp_path / "engine" / "signal_core.py"
    f1.parent.mkdir(parents=True, exist_ok=True)
    f1.write_text("x = 1\n", encoding="utf-8")
    h1 = hashlib.sha256(f1.read_bytes()).hexdigest()[:16]
    ok_chk = qa.check_frozen_hashes(repo_dir=tmp_path, expected_hashes={"engine/signal_core.py": h1})
    assert ok_chk["ok"] is True and ok_chk["matched"] == 1
    f1.write_text("x = 2\n", encoding="utf-8")
    bad_chk = qa.check_frozen_hashes(repo_dir=tmp_path, expected_hashes={"engine/signal_core.py": h1})
    assert bad_chk["ok"] is False and bad_chk["mismatches"][0]["file"] == "engine/signal_core.py"
