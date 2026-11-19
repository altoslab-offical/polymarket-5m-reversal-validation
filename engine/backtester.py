from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any
import numpy as np
import pandas as pd


@dataclass
class StrategyConfig:
    symbol: str = "BTCUSDT"
    rsi_period: int = 6              # 2, 6, or 14
    oversold_rsi: float = 20.0       # Trigger long bounce when RSI <= oversold_rsi
    overbought_rsi: float = 80.0     # Trigger short pullback when RSI >= overbought_rsi
    min_streak: int = 4              # Consecutive red/green candles required
    min_streak_ret_pct: float = 0.0  # Minimum cumulative % drop/rise during streak (e.g. 0.4 means >=0.4% move)
    use_bb_filter: bool = False      # Require price outside Bollinger Band (%B < 0.05 or %B > 0.95)
    min_vol_ratio: float = 0.0       # Require volume >= min_vol_ratio * 20MA (e.g. 1.2 for climax volume)
    min_wick_ratio: float = 0.0      # Require rejection wick ratio (e.g. 0.15)
    direction: str = "both"          # "long_only" (oversold bounce), "short_only" (overbought drop), or "both"
    win_mode: str = "candle_color"   # "candle_color" (next candle reverses color) or "target_bounce" (rebounds >= target_pct)
    target_bounce_pct: float = 0.15  # Used when win_mode == "target_bounce": % bounce from open/close
    max_martingale_steps: int = 3    # 1 = no Martingale (single shot), 2..6 = Martingale steps
    martingale_multiplier: float = 2.1  # Stake multiplier per step (e.g. 2.1x covers 95% payout / Polymarket fees)
    payout_ratio: float = 0.95       # Net profit on 1 unit stake win (0.95 = 95% net payout, 1.0 = even money)
    base_stake: float = 10.0         # Base bet in USD (e.g. $10 on Step 1)


def get_rsi_array(df: pd.DataFrame, rsi_period: int) -> np.ndarray:
    col = f"rsi_{rsi_period}"
    if col in df.columns:
        return df[col].to_numpy(dtype=np.float64)
    from engine.indicators import compute_rsi
    return compute_rsi(df["close"].to_numpy(dtype=np.float64), rsi_period)


def compute_raw_streak_distribution(df: pd.DataFrame) -> dict[str, Any]:
    """
    Analyze the unconditional distribution of consecutive red and green candles
    across the 2-year dataset, including empirical conditional bounce probability at each streak length.
    """
    streak_down = df["streak_down"].to_numpy(dtype=np.int32)
    streak_up = df["streak_up"].to_numpy(dtype=np.int32)
    is_green = df["is_green"].to_numpy(dtype=bool)
    is_red = df["is_red"].to_numpy(dtype=bool)

    n = len(df)
    down_stats = []
    up_stats = []

    for k in range(1, 15):
        # Candles where streak_down == k (exclude last 6 candles for horizon lookahead)
        idx_d = np.where(streak_down[: n - 6] == k)[0]
        count_d = len(idx_d)
        if count_d > 0:
            win_1 = np.mean(is_green[idx_d + 1]) * 100.0
            win_2 = np.mean(is_green[idx_d + 1] | is_green[idx_d + 2]) * 100.0
            win_3 = np.mean(is_green[idx_d + 1] | is_green[idx_d + 2] | is_green[idx_d + 3]) * 100.0
            win_4 = np.mean(
                is_green[idx_d + 1] | is_green[idx_d + 2] | is_green[idx_d + 3] | is_green[idx_d + 4]
            ) * 100.0
            down_stats.append({
                "streak": k,
                "occurrences": int(count_d),
                "next_1_bounce_pct": round(float(win_1), 2),
                "martingale_2_win_pct": round(float(win_2), 2),
                "martingale_3_win_pct": round(float(win_3), 2),
                "martingale_4_win_pct": round(float(win_4), 2),
            })

        idx_u = np.where(streak_up[: n - 6] == k)[0]
        count_u = len(idx_u)
        if count_u > 0:
            win_1_u = np.mean(is_red[idx_u + 1]) * 100.0
            win_2_u = np.mean(is_red[idx_u + 1] | is_red[idx_u + 2]) * 100.0
            win_3_u = np.mean(is_red[idx_u + 1] | is_red[idx_u + 2] | is_red[idx_u + 3]) * 100.0
            win_4_u = np.mean(
                is_red[idx_u + 1] | is_red[idx_u + 2] | is_red[idx_u + 3] | is_red[idx_u + 4]
            ) * 100.0
            up_stats.append({
                "streak": k,
                "occurrences": int(count_u),
                "next_1_pullback_pct": round(float(win_1_u), 2),
                "martingale_2_win_pct": round(float(win_2_u), 2),
                "martingale_3_win_pct": round(float(win_3_u), 2),
                "martingale_4_win_pct": round(float(win_4_u), 2),
            })

    return {
        "max_consecutive_down": int(np.max(streak_down)) if n > 0 else 0,
        "max_consecutive_up": int(np.max(streak_up)) if n > 0 else 0,
        "down_streak_table": down_stats,
        "up_streak_table": up_stats,
    }


def run_backtest(df: pd.DataFrame, cfg: StrategyConfig) -> dict[str, Any]:
    """
    Execute a strict, non-overlapping sequential backtest on 5m candles
    using the specified RSI + Consecutive Streak + Martingale configuration.
    """
    n = len(df)
    open_arr = df["open"].to_numpy(dtype=np.float64)
    high_arr = df["high"].to_numpy(dtype=np.float64)
    low_arr = df["low"].to_numpy(dtype=np.float64)
    close_arr = df["close"].to_numpy(dtype=np.float64)
    ts_arr = df["open_time"].to_numpy(dtype=np.int64)

    rsi_arr = get_rsi_array(df, cfg.rsi_period)
    streak_down = df["streak_down"].to_numpy(dtype=np.int32)
    streak_up = df["streak_up"].to_numpy(dtype=np.int32)
    streak_ret_pct = df["streak_ret_pct"].to_numpy(dtype=np.float64)
    bb_pct_b = df["bb_pct_b"].to_numpy(dtype=np.float64)
    vol_ratio = df["vol_ratio"].to_numpy(dtype=np.float64)
    lower_wick = df["lower_wick_ratio"].to_numpy(dtype=np.float64)
    upper_wick = df["upper_wick_ratio"].to_numpy(dtype=np.float64)

    max_steps = max(1, int(cfg.max_martingale_steps))
    mult = float(cfg.martingale_multiplier)
    payout = float(cfg.payout_ratio)
    base_stake = float(cfg.base_stake)

    # Precompute stake table and cumulative cost at each step
    step_stakes = [base_stake * (mult ** s) for s in range(max_steps)]
    total_table_bankroll = sum(step_stakes)

    # Signal mask
    long_cond = (
        (streak_down >= cfg.min_streak)
        & (rsi_arr <= cfg.oversold_rsi)
        & (np.abs(streak_ret_pct) >= cfg.min_streak_ret_pct)
    )
    short_cond = (
        (streak_up >= cfg.min_streak)
        & (rsi_arr >= cfg.overbought_rsi)
        & (np.abs(streak_ret_pct) >= cfg.min_streak_ret_pct)
    )

    if cfg.use_bb_filter:
        long_cond &= bb_pct_b <= 0.05
        short_cond &= bb_pct_b >= 0.95

    if cfg.min_vol_ratio > 0:
        long_cond &= vol_ratio >= cfg.min_vol_ratio
        short_cond &= vol_ratio >= cfg.min_vol_ratio

    if cfg.min_wick_ratio > 0:
        long_cond &= lower_wick >= cfg.min_wick_ratio
        short_cond &= upper_wick >= cfg.min_wick_ratio

    if cfg.direction == "long_only":
        short_cond[:] = False
    elif cfg.direction == "short_only":
        long_cond[:] = False

    # Sequential non-overlapping state machine
    i = 25  # Warmup for RSI & BB
    limit = n - 12

    step_wins = np.zeros(max_steps + 1, dtype=np.int32)  # 1..max_steps
    busts = 0
    total_sequences = 0
    long_sequences = 0
    short_sequences = 0

    # Track how many steps it ACTUALLY took to reverse even beyond max_steps
    actual_steps_needed_list: list[int] = []
    worst_streak_event: dict[str, Any] | None = None
    max_steps_needed_observed = 0

    equity = 0.0
    peak_equity = 0.0
    max_drawdown = 0.0
    gross_profit = 0.0
    gross_loss = 0.0

    equity_curve: list[dict[str, Any]] = []
    recent_trades: list[dict[str, Any]] = []

    target_ratio = cfg.target_bounce_pct / 100.0

    while i < limit:
        is_long_sig = bool(long_cond[i])
        is_short_sig = bool(short_cond[i])

        if not (is_long_sig or is_short_sig):
            i += 1
            continue

        sig_dir = "LONG_BOUNCE" if is_long_sig else "SHORT_PULLBACK"
        total_sequences += 1
        if is_long_sig:
            long_sequences += 1
        else:
            short_sequences += 1

        # Determine how many steps until first actual reversal (up to 12 candles lookahead)
        actual_step_found = 12
        for look_s in range(1, 13):
            c_idx = i + look_s
            if c_idx >= n:
                break
            if cfg.win_mode == "candle_color":
                won_candle = (
                    (close_arr[c_idx] > open_arr[c_idx])
                    if is_long_sig
                    else (close_arr[c_idx] < open_arr[c_idx])
                )
            else:
                # Target bounce mode: did price bounce >= target_bounce_pct from candle open (or reach green close)
                if is_long_sig:
                    won_candle = (
                        (high_arr[c_idx] - open_arr[c_idx]) / max(open_arr[c_idx], 1e-12) >= target_ratio
                    ) or (close_arr[c_idx] > open_arr[c_idx])
                else:
                    won_candle = (
                        (open_arr[c_idx] - low_arr[c_idx]) / max(open_arr[c_idx], 1e-12) >= target_ratio
                    ) or (close_arr[c_idx] < open_arr[c_idx])

            if won_candle:
                actual_step_found = look_s
                break

        actual_steps_needed_list.append(actual_step_found)
        if actual_step_found > max_steps_needed_observed:
            max_steps_needed_observed = actual_step_found
            worst_streak_event = {
                "timestamp_ms": int(ts_arr[i]),
                "direction": sig_dir,
                "entry_price": float(close_arr[i]),
                "rsi": round(float(rsi_arr[i]), 2),
                "initial_streak": int(streak_down[i] if is_long_sig else streak_up[i]),
                "steps_to_reverse": int(actual_step_found),
                "total_streak_length": int((streak_down[i] if is_long_sig else streak_up[i]) + actual_step_found - 1),
            }

        # Evaluate Martingale sequence within max_steps
        won_step = 0
        seq_pnl = 0.0
        for s in range(1, max_steps + 1):
            stake_s = step_stakes[s - 1]
            if s == actual_step_found:
                # Won on step s!
                seq_pnl += stake_s * payout
                won_step = s
                break
            else:
                # Lost step s
                seq_pnl -= stake_s

        if won_step > 0:
            step_wins[won_step] += 1
            gross_profit += seq_pnl
            advance_candles = won_step
        else:
            busts += 1
            gross_loss += abs(seq_pnl)
            advance_candles = max_steps

        equity += seq_pnl
        if equity > peak_equity:
            peak_equity = equity
        dd = peak_equity - equity
        if dd > max_drawdown:
            max_drawdown = dd

        # Downsample equity curve (~250 points max for fast JSON rendering)
        if total_sequences <= 300 or total_sequences % max(1, (limit // 1500)) == 0:
            equity_curve.append({
                "seq": total_sequences,
                "ts": int(ts_arr[i]),
                "equity": round(equity, 2),
                "dd": round(dd, 2),
            })

        # Keep recent trades (last 50)
        trade_record = {
            "seq": total_sequences,
            "signal_ts": int(ts_arr[i]),
            "resolve_ts": int(ts_arr[min(i + advance_candles, n - 1)]),
            "direction": sig_dir,
            "signal_price": round(float(close_arr[i]), 4),
            "resolve_price": round(float(close_arr[min(i + advance_candles, n - 1)]), 4),
            "rsi": round(float(rsi_arr[i]), 2),
            "streak": int(streak_down[i] if is_long_sig else streak_up[i]),
            "streak_ret_pct": round(float(streak_ret_pct[i]), 3),
            "won": won_step > 0,
            "won_at_step": int(won_step) if won_step > 0 else None,
            "actual_steps_to_reverse": int(actual_step_found),
            "pnl_usd": round(seq_pnl, 2),
        }
        recent_trades.append(trade_record)
        if len(recent_trades) > 60:
            recent_trades.pop(0)

        # Advance index past the resolved sequence (strictly non-overlapping!)
        i += advance_candles

    won_sequences = int(np.sum(step_wins[1:]))
    win_rate = (won_sequences / total_sequences * 100.0) if total_sequences > 0 else 0.0
    step_1_wr = (step_wins[1] / total_sequences * 100.0) if total_sequences > 0 else 0.0

    # Cumulative win rate curve by Martingale step (1..6) using actual_steps_needed_list
    steps_arr = np.array(actual_steps_needed_list, dtype=np.int32)
    martingale_curve = []
    for k in range(1, 7):
        if len(steps_arr) > 0:
            cum_wr = float(np.mean(steps_arr <= k) * 100.0)
            exact_cnt = int(np.sum(steps_arr == k))
        else:
            cum_wr = 0.0
            exact_cnt = 0
        req_cap = sum(base_stake * (mult ** m) for m in range(k))
        martingale_curve.append({
            "max_steps": k,
            "exact_wins_at_step": exact_cnt,
            "cumulative_win_rate_pct": round(cum_wr, 2),
            "bust_count_in_2y": int(np.sum(steps_arr > k)) if len(steps_arr) > 0 else 0,
            "required_table_bankroll_usd": round(req_cap, 2),
        })

    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)

    return {
        "config": asdict(cfg),
        "summary": {
            "symbol": cfg.symbol,
            "total_candles": n,
            "total_sequences": total_sequences,
            "long_sequences": long_sequences,
            "short_sequences": short_sequences,
            "won_sequences": won_sequences,
            "bust_sequences": busts,
            "sequence_win_rate_pct": round(win_rate, 2),
            "first_shot_win_rate_pct": round(step_1_wr, 2),
            "net_profit_usd": round(equity, 2),
            "max_drawdown_usd": round(max_drawdown, 2),
            "required_table_bankroll_usd": round(total_table_bankroll, 2),
            "return_on_table_bankroll_pct": round(
                (equity / total_table_bankroll * 100.0) if total_table_bankroll > 0 else 0.0, 2
            ),
            "profit_factor": round(profit_factor, 2),
            "max_steps_needed_in_2y": int(max_steps_needed_observed),
            "worst_streak_event": worst_streak_event,
        },
        "step_breakdown": [
            {
                "step": int(s),
                "stake_usd": round(step_stakes[s - 1], 2),
                "wins": int(step_wins[s]),
                "share_of_total_pct": round(
                    (step_wins[s] / total_sequences * 100.0) if total_sequences > 0 else 0.0, 2
                ),
                "cumulative_win_rate_pct": round(
                    (np.sum(step_wins[1 : s + 1]) / total_sequences * 100.0) if total_sequences > 0 else 0.0, 2
                ),
            }
            for s in range(1, max_steps + 1)
        ],
        "martingale_curve_1_to_6": martingale_curve,
        "equity_curve": equity_curve,
        "recent_trades": list(reversed(recent_trades)),
    }
