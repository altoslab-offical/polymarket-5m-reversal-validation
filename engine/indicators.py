from __future__ import annotations

import numpy as np
import pandas as pd


def compute_rsi(close: np.ndarray, period: int = 14) -> np.ndarray:
    """Fast Wilder's smoothed RSI calculation using NumPy."""
    n = len(close)
    rsi = np.full(n, 50.0, dtype=np.float64)
    if n <= period:
        return rsi

    delta = np.diff(close, prepend=close[0])
    gains = np.where(delta > 0, delta, 0.0)
    losses = np.where(delta < 0, -delta, 0.0)

    avg_gain = np.mean(gains[1 : period + 1])
    avg_loss = np.mean(losses[1 : period + 1])

    alpha = 1.0 / period
    one_minus_alpha = 1.0 - alpha

    for i in range(period, n):
        if i > period:
            avg_gain = avg_gain * one_minus_alpha + gains[i] * alpha
            avg_loss = avg_loss * one_minus_alpha + losses[i] * alpha

        if avg_loss == 0.0:
            rsi[i] = 100.0 if avg_gain > 0 else 50.0
        else:
            rs = avg_gain / avg_loss
            rsi[i] = 100.0 - (100.0 / (1.0 + rs))

    return rsi


def compute_streaks(open_arr: np.ndarray, close_arr: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Compute consecutive bearish (close < open) and bullish (close > open) candle streaks,
    plus cumulative return (%) from the start of the active streak.
    """
    n = len(close_arr)
    streak_down = np.zeros(n, dtype=np.int32)
    streak_up = np.zeros(n, dtype=np.int32)
    streak_ret_pct = np.zeros(n, dtype=np.float64)

    cur_down = 0
    cur_up = 0
    streak_start_open = open_arr[0] if n > 0 else 1.0

    for i in range(n):
        o = open_arr[i]
        c = close_arr[i]
        if c < o:  # Bearish (Red)
            if cur_down == 0:
                streak_start_open = o
            cur_down += 1
            cur_up = 0
            streak_down[i] = cur_down
            streak_ret_pct[i] = ((c - streak_start_open) / streak_start_open) * 100.0 if streak_start_open > 0 else 0.0
        elif c > o:  # Bullish (Green)
            if cur_up == 0:
                streak_start_open = o
            cur_up += 1
            cur_down = 0
            streak_up[i] = cur_up
            streak_ret_pct[i] = ((c - streak_start_open) / streak_start_open) * 100.0 if streak_start_open > 0 else 0.0
        else:  # Doji (flat) resets strict streak
            cur_down = 0
            cur_up = 0
            streak_start_open = o
            streak_ret_pct[i] = 0.0

    return streak_down, streak_up, streak_ret_pct


def enrich_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add RSI(2, 6, 14), streaks, Bollinger Band Z-score/%B, volume climax, and wick metrics."""
    out = df.copy()
    open_arr = out["open"].to_numpy(dtype=np.float64)
    high_arr = out["high"].to_numpy(dtype=np.float64)
    low_arr = out["low"].to_numpy(dtype=np.float64)
    close_arr = out["close"].to_numpy(dtype=np.float64)
    vol_arr = out["volume"].to_numpy(dtype=np.float64)

    # Candle direction
    out["is_green"] = close_arr > open_arr
    out["is_red"] = close_arr < open_arr
    out["body_pct"] = (close_arr - open_arr) / np.maximum(open_arr, 1e-12) * 100.0
    candle_range = np.maximum(high_arr - low_arr, 1e-12)
    out["range_pct"] = candle_range / np.maximum(open_arr, 1e-12) * 100.0
    out["lower_wick_ratio"] = (np.minimum(open_arr, close_arr) - low_arr) / candle_range
    out["upper_wick_ratio"] = (high_arr - np.maximum(open_arr, close_arr)) / candle_range

    # Multi-period Wilder's RSI
    out["rsi_2"] = compute_rsi(close_arr, 2)
    out["rsi_6"] = compute_rsi(close_arr, 6)
    out["rsi_14"] = compute_rsi(close_arr, 14)

    # Consecutive candle streaks
    streak_down, streak_up, streak_ret_pct = compute_streaks(open_arr, close_arr)
    out["streak_down"] = streak_down
    out["streak_up"] = streak_up
    out["streak_ret_pct"] = streak_ret_pct

    # Bollinger Bands (20, 2) & 30m EMA20 Macro Slope
    s_close = pd.Series(close_arr)
    sma20 = s_close.rolling(20, min_periods=20).mean().to_numpy(dtype=np.float64)
    std20 = s_close.rolling(20, min_periods=20).std(ddof=0).to_numpy(dtype=np.float64)
    safe_std = np.where(std20 > 1e-12, std20, np.nan)
    bb_zscore = np.nan_to_num((close_arr - sma20) / safe_std, nan=0.0)
    out["bb_zscore"] = bb_zscore
    # %B: 0.0 = at lower band (sma - 2*std), 1.0 = at upper band (sma + 2*std)
    out["bb_pct_b"] = (bb_zscore + 2.0) / 4.0

    # 30-Minute EMA20 Macro Trend Slope (%) to filter out unilateral trend-tsunami traps
    s_ema20 = s_close.ewm(span=20, adjust=False).mean()
    out["ema_20"] = s_ema20.to_numpy(dtype=np.float64)
    out["ema_slope_6"] = ((s_ema20 / s_ema20.shift(6) - 1.0) * 100.0).fillna(0.0).to_numpy(dtype=np.float64)

    # Volume climax ratio vs 20-period average
    s_vol = pd.Series(vol_arr)
    vol_ma20 = s_vol.rolling(20, min_periods=5).mean().to_numpy(dtype=np.float64)
    out["vol_ratio"] = np.where(vol_ma20 > 0, vol_arr / vol_ma20, 1.0)

    # Order-Flow Taker Buy Ratio (0.0 to 1.0; < 0.42 indicates heavy panic market selling climax)
    if "taker_buy_base" in out.columns:
        tbb_arr = out["taker_buy_base"].to_numpy(dtype=np.float64)
        ratio = np.divide(tbb_arr, vol_arr, out=np.full_like(tbb_arr, 0.5), where=vol_arr > 0)
        out["taker_buy_ratio"] = np.clip(ratio, 0.0, 1.0)
    else:
        out["taker_buy_ratio"] = np.clip((close_arr - low_arr) / candle_range, 0.0, 1.0)

    # V10.0 Wyckoff & Chip Microstructure Features (ATR, Body Accel, Close Pos, 3-Bar CVD)
    o_arr = out["open"].to_numpy(dtype=np.float64)
    h_arr = out["high"].to_numpy(dtype=np.float64)
    l_arr = out["low"].to_numpy(dtype=np.float64)
    c_arr = out["close"].to_numpy(dtype=np.float64)
    prev_c = np.roll(c_arr, 1)
    prev_c[0] = o_arr[0]
    tr = np.maximum(h_arr - l_arr, np.maximum(np.abs(h_arr - prev_c), np.abs(l_arr - prev_c)))
    atr_14 = pd.Series(tr).rolling(14, min_periods=1).mean().to_numpy(dtype=np.float64)
    atr_pct = (atr_14 / np.maximum(c_arr, 1e-12)) * 100.0
    body_pct = ((c_arr - o_arr) / np.maximum(o_arr, 1e-12)) * 100.0
    bar_rng = np.maximum(h_arr - l_arr, 1e-12)

    out["atr_pct"] = atr_pct
    out["body_pct"] = body_pct
    out["close_pos_in_bar"] = np.clip((c_arr - l_arr) / bar_rng, 0.0, 1.0)
    out["body_accel"] = np.abs(body_pct) / np.maximum(atr_pct, 1e-6)
    out["ret_12bars_pct"] = (out["close"].pct_change(12) * 100.0).fillna(0.0)
    out["ret_48bars_pct"] = (out["close"].pct_change(48) * 100.0).fillna(0.0)

    # 3-Bar Cumulative Volume Delta Ratio (CVD_3 in [-1.0, +1.0]) for Iceberg Absorption Detection
    tbr_np = out["taker_buy_ratio"].to_numpy(dtype=np.float64)
    net_taker_delta = vol_arr * (2.0 * tbr_np - 1.0)
    vol_sum_3 = pd.Series(vol_arr).rolling(3, min_periods=1).sum().to_numpy(dtype=np.float64)
    delta_sum_3 = pd.Series(net_taker_delta).rolling(3, min_periods=1).sum().to_numpy(dtype=np.float64)
    out["cvd_3"] = np.where(vol_sum_3 > 1e-9, delta_sum_3 / vol_sum_3, 0.0)

    return out
