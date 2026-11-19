#!/usr/bin/env python3
"""Daily Section 4.2 + Tier-2 Exploration Grid wrapper (zero edits to frozen core)."""
from __future__ import annotations

import argparse
import json
import pathlib
import sys

REPO = pathlib.Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from edge_validation.exploration_grid import run_exploration_grid
from edge_validation.live_recorder_42 import DEFAULT_DB, NOV21_UTC_TS, analyze_recording


def run_daily_bundle(db_path: pathlib.Path, sanity: bool = False) -> dict[str, object]:
    """Generate frozen Section 4.2 daily report and append Tier-2 exploration table."""
    base_res = analyze_recording(db_path, daily_report=True)
    report_p = pathlib.Path(str(base_res["daily_report_path"]))
    grid_res = run_exploration_grid(db_path, start_ts=NOV21_UTC_TS, sanity=sanity, append_to=report_p)
    return {
        "daily_report_path": str(report_p),
        "base_signals_post_nov21": base_res.get("signals_total_post_nov21"),
        "all_parity": grid_res["all_parity"],
        "e4_diff_count": grid_res["e4_diff_count"],
    }


def main() -> None:
    """CLI entrypoint for combined daily report generation."""
    ap = argparse.ArgumentParser(description="Generate Tier-1 Daily Report + Tier-2 Exploration Table")
    ap.add_argument("--db", type=pathlib.Path, default=DEFAULT_DB)
    ap.add_argument("--sanity", action="store_true", help="Include pre-11/21 windows in exploration table")
    args = ap.parse_args()
    print(json.dumps(run_daily_bundle(args.db, sanity=args.sanity), indent=2))


if __name__ == "__main__":
    main()
