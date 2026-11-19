from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

import pandas as pd
from engine.indicators import enrich_indicators
from engine.backtester import (
    StrategyConfig,
    compute_raw_streak_distribution,
    run_backtest,
)

DATA_DIR = ROOT_DIR / "data"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


def optimize_for_symbol(symbol: str) -> dict:
    parquet_path = DATA_DIR / f"{symbol}_5m.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(f"Missing {parquet_path}")

    print(f"[{symbol}] Loading 2-year 5m dataset and enriching indicators...")
    raw_df = pd.read_parquet(parquet_path)
    df = enrich_indicators(raw_df)

    streak_dist = compute_raw_streak_distribution(df)

    # Grid search candidates
    candidates_single_candle = []
    candidates_single_bounce = []
    candidates_martingale_3 = []
    candidates_martingale_4_zero_bust = []

    rsi_configs = [
        (2, 5.0, 95.0),
        (2, 2.0, 98.0),
        (6, 20.0, 80.0),
        (6, 15.0, 85.0),
        (6, 12.0, 88.0),
        (14, 28.0, 72.0),
        (14, 24.0, 76.0),
    ]
    streaks = [3, 4, 5, 6, 7]
    ret_filters = [0.0, 0.3, 0.6]
    bb_filters = [False, True]
    vol_filters = [0.0, 1.3]
    wick_filters = [0.0, 0.2]

    for rsi_p, os_val, ob_val in rsi_configs:
        for st in streaks:
            for ret_f in ret_filters:
                for bb_f in bb_filters:
                    for vol_f in vol_filters:
                        for wick_f in wick_filters:
                            # 1) Pure Candle Color Reversal + 3-Step Martingale
                            cfg3 = StrategyConfig(
                                symbol=symbol,
                                rsi_period=rsi_p,
                                oversold_rsi=os_val,
                                overbought_rsi=ob_val,
                                min_streak=st,
                                min_streak_ret_pct=ret_f,
                                use_bb_filter=bb_f,
                                min_vol_ratio=vol_f,
                                min_wick_ratio=wick_f,
                                direction="both",
                                win_mode="candle_color",
                                max_martingale_steps=3,
                                martingale_multiplier=2.1,
                                payout_ratio=0.95,
                                base_stake=10.0,
                            )
                            res3 = run_backtest(df, cfg3)
                            s3 = res3["summary"]
                            if s3["total_sequences"] >= 20:
                                candidates_single_candle.append({
                                    "config": res3["config"],
                                    "total_sequences": s3["total_sequences"],
                                    "first_shot_win_rate_pct": s3["first_shot_win_rate_pct"],
                                    "martingale_2_win_rate_pct": res3["martingale_curve_1_to_6"][1]["cumulative_win_rate_pct"],
                                    "martingale_3_win_rate_pct": s3["sequence_win_rate_pct"],
                                    "martingale_4_win_rate_pct": res3["martingale_curve_1_to_6"][3]["cumulative_win_rate_pct"],
                                    "net_profit_3step_usd": s3["net_profit_usd"],
                                    "max_drawdown_3step_usd": s3["max_drawdown_usd"],
                                    "max_steps_needed_in_2y": s3["max_steps_needed_in_2y"],
                                    "busts_at_3step": s3["bust_sequences"],
                                    "busts_at_4step": res3["martingale_curve_1_to_6"][3]["bust_count_in_2y"],
                                })

                            # If >= 60 signals and high 3-step win rate & profitable
                            if s3["total_sequences"] >= 80 and s3["sequence_win_rate_pct"] >= 88.0:
                                candidates_martingale_3.append({
                                    "config": res3["config"],
                                    "summary": s3,
                                    "martingale_curve": res3["martingale_curve_1_to_6"],
                                })

                            # Check 4-step zero-bust or ultra-high win rate (>= 95.5%)
                            w4 = res3["martingale_curve_1_to_6"][3]["cumulative_win_rate_pct"]
                            if s3["total_sequences"] >= 25 and (s3["max_steps_needed_in_2y"] <= 4 or w4 >= 95.5):
                                cfg4 = StrategyConfig(**{**res3["config"], "max_martingale_steps": 4})
                                res4 = run_backtest(df, cfg4)
                                if res4["summary"]["net_profit_usd"] > 0:
                                    candidates_martingale_4_zero_bust.append({
                                        "config": res4["config"],
                                        "summary": res4["summary"],
                                        "martingale_curve": res4["martingale_curve_1_to_6"],
                                    })

    # Additional sniper zero-bust search (extreme streak 7..10 + volume climax + wick)
    for st in [6, 7, 8, 9]:
        for ret_f in [0.4, 0.8, 1.2]:
            for vol_f in [1.3, 1.8]:
                for wick_f in [0.15, 0.25]:
                    for wm in ["candle_color", "target_bounce"]:
                        cfg_sniper = StrategyConfig(
                            symbol=symbol,
                            rsi_period=6,
                            oversold_rsi=14.0,
                            overbought_rsi=86.0,
                            min_streak=st,
                            min_streak_ret_pct=ret_f,
                            use_bb_filter=True,
                            min_vol_ratio=vol_f,
                            min_wick_ratio=wick_f,
                            direction="both",
                            win_mode=wm,
                            target_bounce_pct=0.12,
                            max_martingale_steps=4,
                            martingale_multiplier=2.1,
                            payout_ratio=0.95,
                            base_stake=10.0,
                        )
                        res_sn = run_backtest(df, cfg_sniper)
                        if res_sn["summary"]["total_sequences"] >= 15 and res_sn["summary"]["sequence_win_rate_pct"] >= 96.0:
                            candidates_martingale_4_zero_bust.append({
                                "config": res_sn["config"],
                                "summary": res_sn["summary"],
                                "martingale_curve": res_sn["martingale_curve_1_to_6"],
                            })

    # Also test Single-Shot Target Bounce mode (rebound >= 0.12% or green candle on next 1..2 candles)
    for rsi_p, os_val, ob_val in [(2, 3.0, 97.0), (6, 15.0, 85.0), (6, 12.0, 88.0)]:
        for st in [4, 5, 6]:
            for wick_f in [0.0, 0.25]:
                cfg_tb = StrategyConfig(
                    symbol=symbol,
                    rsi_period=rsi_p,
                    oversold_rsi=os_val,
                    overbought_rsi=ob_val,
                    min_streak=st,
                    min_streak_ret_pct=0.3,
                    use_bb_filter=True,
                    min_vol_ratio=1.2,
                    min_wick_ratio=wick_f,
                    direction="both",
                    win_mode="target_bounce",
                    target_bounce_pct=0.12,
                    max_martingale_steps=2,
                    martingale_multiplier=2.1,
                    payout_ratio=0.95,
                    base_stake=10.0,
                )
                res_tb = run_backtest(df, cfg_tb)
                if res_tb["summary"]["total_sequences"] >= 25:
                    candidates_single_bounce.append({
                        "config": res_tb["config"],
                        "summary": res_tb["summary"],
                        "martingale_curve": res_tb["martingale_curve_1_to_6"],
                    })

    # Sort & pick best presets
    candidates_single_candle.sort(key=lambda x: (x["first_shot_win_rate_pct"], x["total_sequences"]), reverse=True)
    candidates_martingale_3.sort(key=lambda x: (x["summary"]["net_profit_usd"], x["summary"]["sequence_win_rate_pct"]), reverse=True)
    candidates_martingale_4_zero_bust.sort(
        key=lambda x: (
            x["summary"]["bust_sequences"] == 0,
            x["summary"]["sequence_win_rate_pct"],
            x["summary"]["net_profit_usd"],
        ),
        reverse=True,
    )
    candidates_single_bounce.sort(key=lambda x: (x["summary"]["first_shot_win_rate_pct"], x["summary"]["sequence_win_rate_pct"]), reverse=True)

    return {
        "symbol": symbol,
        "total_candles": len(df),
        "start_time": str(df["timestamp"].iloc[0]) if "timestamp" in df.columns else int(df["open_time"].iloc[0]),
        "end_time": str(df["timestamp"].iloc[-1]) if "timestamp" in df.columns else int(df["open_time"].iloc[-1]),
        "raw_streak_distribution": streak_dist,
        "top_single_candle_reversal": candidates_single_candle[:5],
        "top_martingale_3step_balanced": candidates_martingale_3[:5],
        "top_zero_bust_4step_ironclad": candidates_martingale_4_zero_bust[:5],
        "top_target_bounce": candidates_single_bounce[:5],
    }


def main() -> None:
    report = {}
    for sym in SYMBOLS:
        report[sym] = optimize_for_symbol(sym)
        s_best3 = report[sym]["top_martingale_3step_balanced"][0]["summary"] if report[sym]["top_martingale_3step_balanced"] else None
        s_iron = report[sym]["top_zero_bust_4step_ironclad"][0]["summary"] if report[sym]["top_zero_bust_4step_ironclad"] else None
        print(f"=== {sym} Summary ===")
        if s_best3:
            print(
                f"  [3-Step High Profit] Signals: {s_best3['total_sequences']} | "
                f"1st-Shot WR: {s_best3['first_shot_win_rate_pct']}% | "
                f"3-Step WR: {s_best3['sequence_win_rate_pct']}% | "
                f"Net Profit: ${s_best3['net_profit_usd']} | Max Steps Needed in 2Y: {s_best3['max_steps_needed_in_2y']}"
            )
        if s_iron:
            print(
                f"  [4-Step 0-Bust Ironclad] Signals: {s_iron['total_sequences']} | "
                f"4-Step WR: {s_iron['sequence_win_rate_pct']}% (Busts: {s_iron['bust_sequences']}) | "
                f"Net Profit: ${s_iron['net_profit_usd']} | Max Steps Needed in 2Y: {s_iron['max_steps_needed_in_2y']}"
            )

    out_path = DATA_DIR / "optimal_presets.json"
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nSaved complete optimization results to {out_path}")


if __name__ == "__main__":
    main()
