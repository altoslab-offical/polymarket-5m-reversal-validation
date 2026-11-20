#!/usr/bin/env python3
"""Daily Section 4.2 + Tier-2 Exploration Grid wrapper (zero edits to frozen core)."""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import time
from typing import Any, Optional

REPO = pathlib.Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from edge_validation.exploration_grid import run_exploration_grid
from edge_validation.live_recorder_42 import DEFAULT_DB, NOV21_UTC_TS, analyze_recording
from edge_validation.q1_soak_audit import DEFAULT_RECORDER_LOG, DEFAULT_SHADOW_LOG, DEFAULT_STATUS_URL, run_q1_audit

PRIMARY_MODELS: tuple[str, ...] = ("v2_pure", "v2_filtered", "v1")
DAY10_EARLY_STOP_HI_LT = 0.02


def apply_low_priority() -> Optional[int]:
    """Set process CPU priority to `nice +19` so live recorder/runner daemons never contend."""
    try:
        return int(os.nice(19))
    except OSError:
        return None


def evaluate_day10_early_stop(
    base_res: dict[str, Any],
    now_ts: Optional[float] = None,
    primary_start_ts: int = NOV21_UTC_TS,
) -> dict[str, Any]:
    """Evaluate fail-only Day-10 early-stop rule (`all 3 primary 98.33% CI_hi < +2c/sh`)."""
    now = float(time.time() if now_ts is None else now_ts)
    elapsed_days = max(0.0, (now - float(primary_start_ts)) / 86400.0)
    post = base_res.get("post_nov21") or {}
    models_out: dict[str, dict[str, Any]] = {}
    highs: list[float] = []
    for name in PRIMARY_MODELS:
        summary = post.get(name) or {}
        ci98 = summary.get("ci98_3_bonferroni_1s")
        ci_lo = float(ci98[0]) if (isinstance(ci98, (list, tuple)) and len(ci98) == 2 and ci98[0] is not None) else None
        ci_hi = float(ci98[1]) if (isinstance(ci98, (list, tuple)) and len(ci98) == 2 and ci98[1] is not None) else None
        if ci_hi is not None:
            highs.append(ci_hi)
        models_out[name] = {
            "filled_1s": int(summary.get("filled_1s") or 0),
            "mean_pnl_1s": summary.get("mean_pnl_1s"),
            "ci98_3_lo": ci_lo,
            "ci98_3_hi": ci_hi,
            "hi_lt_2c": bool(ci_hi is not None and ci_hi < DAY10_EARLY_STOP_HI_LT),
        }
    if elapsed_days < 10.0:
        status = "PRE_DAY10"
    elif len(highs) == len(PRIMARY_MODELS) and all(hi < DAY10_EARLY_STOP_HI_LT for hi in highs):
        status = "EARLY_STOP_INSUFFICIENT_EDGE"
    else:
        status = "CONTINUE_TO_300"
    return {
        "rule": "fail_only_never_early_launch",
        "elapsed_days": round(elapsed_days, 2),
        "threshold_ci_hi": DAY10_EARLY_STOP_HI_LT,
        "status": status,
        "models": models_out,
    }


def _append_early_stop_block(target: pathlib.Path, early_stop: dict[str, Any]) -> None:
    """Insert or replace fail-only Day-10 early-stop monitor block inside daily report."""
    s_tag, e_tag = "<!-- DAY10_EARLY_STOP_START -->", "<!-- DAY10_EARLY_STOP_END -->"
    lines = [
        s_tag, "",
        f"## Day-10 Fail-Only Early-Stop Monitor (`elapsed_days={early_stop['elapsed_days']}`｜Status=**`{early_stop['status']}`**)",
        "",
        "> 規則（`CLAUDE_RESPONSE_10`）：僅用於第 10 天 fail-only 提早停損（3 組 98.33% CI 上界皆 `< +2c/sh`），**絕不提早放行**。",
        "",
        "| Primary Model | Filled +1s | Mean PnL/sh | 98.33% CI Lo | 98.33% CI Hi | `CI_hi < +2c` |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for name in PRIMARY_MODELS:
        m = early_stop["models"][name]
        lines.append(f"| `{name}` | {m['filled_1s']} | {m['mean_pnl_1s']} | {m['ci98_3_lo']} | {m['ci98_3_hi']} | `{m['hi_lt_2c']}` |")
    lines += ["", e_tag]
    block = "\n".join(lines)
    txt = target.read_text(encoding="utf-8") if target.exists() else ""
    if s_tag in txt and e_tag in txt:
        pre, rest = txt.split(s_tag, 1)
        _mid, post = rest.split(e_tag, 1)
        target.write_text(pre.rstrip() + "\n\n" + block + "\n" + post.lstrip(), encoding="utf-8")
    else:
        sep = "\n\n" if txt and not txt.endswith("\n\n") else ""
        target.write_text(txt + sep + block + "\n", encoding="utf-8")


def build_standing_redline_block(q1_res: dict[str, Any]) -> str:
    """Format Phase 4.2 permanent standing red-lines header for daily report top."""
    re_r = q1_res["runner_enters"]
    rc_r = q1_res["recorder_enters"]
    fc = q1_res["frozen_check"]
    dlt = q1_res["reconnect_and_1013"]
    syms = q1_res["per_coin_lag"]["symbols"]
    stale_total = int(re_r["stale_count"]) + int(rc_r["raw_stale_count"])
    stale_verdict = "PASS (0)" if stale_total == 0 else "INVALID — PAUSE RUNNER & INVESTIGATE (Recorder 續錄)"
    btc_entry_pct = float(syms["BTCUSDT"]["entry_age_gt1500_pct"])
    btc_alert = " ⚠️ **ALERT > 10%**" if btc_entry_pct > 10.0 else ""
    lines = [
        "<!-- PHASE42_REDLINE_START -->",
        "> **Phase 4.2 常設紅線與紀律看板（每日開頭強制檢查）**",
        ">",
        f"> 1. **舊資料決策 (`lag>1000ms` 或 `age>1500ms`)**：Runner `{re_r['stale_count']}/{re_r['total']}` / Recorder Raw `{rc_r['raw_stale_count']}/{rc_r['raw_total']}`（單槽首筆 `{rc_r['first_entry_stale_count']}/{rc_r['first_entry_total']}`） → **`{stale_verdict}`**",
        f"> 2. **凍結指紋檢查**：`{fc['matched']}/{fc['total']}`（`ok={fc['ok']}`，必須為 `12/12 True`）",
        f"> 3. **觀察指標（只觀察，不設停機門檻）**：`1013` Runner `{dlt['runner_1013_total']}` / Recorder `{dlt['recorder_1013_total']}`｜重連 `{dlt['runner_reconnects_total']}`｜`>2s`: `BTC {syms['BTCUSDT']['gt_2000ms_pct']}%`, `ETH {syms['ETHUSDT']['gt_2000ms_pct']}%`, `SOL {syms['SOLUSDT']['gt_2000ms_pct']}%`｜**BTC 進場 `[150,255]s` `age>1.5s`**: `{btc_entry_pct}%`{btc_alert}",
        "> 4. **執行與判定鎖定**：禁併發重運算（離線一律 `nice -n 19` 固定時段）｜3 組預註冊 (`v2_pure` / `v2_filtered` / `v1`)｜Bonferroni `98.33%`｜起算 `window_ts >= 1795219200`｜`n >= 300` 上線 / Day-10 fail-only 停損｜`sniper_live = False`",
        "<!-- PHASE42_REDLINE_END -->",
    ]
    return "\n".join(lines)


def _prepend_redline_block(target: pathlib.Path, block: str) -> None:
    """Insert or replace Phase 4.2 standing red-lines banner at top of daily report."""
    s_tag, e_tag = "<!-- PHASE42_REDLINE_START -->", "<!-- PHASE42_REDLINE_END -->"
    txt = target.read_text(encoding="utf-8") if target.exists() else ""
    if s_tag in txt and e_tag in txt:
        pre, rest = txt.split(s_tag, 1)
        _mid, post = rest.split(e_tag, 1)
        new_txt = (pre.strip() + "\n\n" if pre.strip() else "") + block + "\n\n" + post.lstrip()
    else:
        new_txt = block + ("\n\n" + txt.lstrip() if txt.strip() else "\n")
    target.write_text(new_txt, encoding="utf-8")


def run_daily_bundle(db_path: pathlib.Path, sanity: bool = False) -> dict[str, Any]:
    """Generate frozen Section 4.2 daily report and prepend/append governance checks."""
    base_res = analyze_recording(db_path, daily_report=True)
    report_p = pathlib.Path(str(base_res["daily_report_path"]))
    grid_res = run_exploration_grid(db_path, start_ts=NOV21_UTC_TS, sanity=sanity, append_to=report_p)
    early_stop = evaluate_day10_early_stop(base_res)
    _append_early_stop_block(report_p, early_stop)
    q1_res = run_q1_audit(
        db_path=db_path, recorder_log=DEFAULT_RECORDER_LOG, shadow_log=DEFAULT_SHADOW_LOG,
        since_ts=NOV21_UTC_TS, status_url=DEFAULT_STATUS_URL, repo_dir=REPO,
    )
    _prepend_redline_block(report_p, build_standing_redline_block(q1_res))
    return {
        "daily_report_path": str(report_p),
        "base_signals_post_nov21": base_res.get("signals_total_post_nov21"),
        "all_parity": grid_res["all_parity"],
        "e4_diff_count": grid_res["e4_diff_count"],
        "day10_early_stop": early_stop,
        "redline_frozen_ok": q1_res["frozen_check"]["ok"],
    }


def main() -> None:
    """CLI entrypoint for combined daily report generation (defaults to `nice -n 19`)."""
    ap = argparse.ArgumentParser(description="Generate Tier-1 Daily Report + Tier-2 Exploration Table")
    ap.add_argument("--db", type=pathlib.Path, default=DEFAULT_DB)
    ap.add_argument("--sanity", action="store_true", help="Include pre-11/21 windows in exploration table")
    ap.add_argument("--no-nice", action="store_true", help="Skip os.nice(19) CPU yield")
    args = ap.parse_args()
    if not args.no_nice:
        apply_low_priority()
    print(json.dumps(run_daily_bundle(args.db, sanity=args.sanity), indent=2))


if __name__ == "__main__":
    main()
