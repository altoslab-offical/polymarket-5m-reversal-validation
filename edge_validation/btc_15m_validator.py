#!/usr/bin/env python3
"""Standalone read-only Polymarket 15m BTC validator & dual-engine ($40U -> $10,000U) analysis.

Probes live Polymarket 5m vs 15m orderbook depths, evaluates 24h (96 consecutive settled
windows) of official `btc-updown-15m` outcomes + CLOB `prices-history` + Binance 1m klines,
verifies 30-day offline 15m trend-persistence, and runs depth-capped dual-engine Monte Carlo.
Zero modifications or interference with live 5m Phase 4.2 processes.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import json
import math
import pathlib
import sys
import time
from typing import Any, Dict, List, Optional, Tuple
import urllib.error
import urllib.request

import numpy as np
import pandas as pd

REPO = pathlib.Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from engine.master_strategy import compute_vault_locked_u
from engine.oracle_sniper import TWAP_V2_FILTERED_CONFIG, analyze_trade_ev, estimate_twap_fair_value, taker_fee_per_share

DATA_DIR = REPO / "data"
OUT_CSV = REPO / "edge_validation" / "btc_15m_settled_audit.csv"
OUT_MD = REPO / "edge_validation" / "BTC_15M_ENGINE_RESEARCH.md"
BTC_NOV20_FEE = 0.05


def _http_get_json(url: str, timeout: float = 8.0) -> Any:
    """HTTP GET JSON helper with explicit error payload (never silently swallowed)."""
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
        return {"_error": f"{type(exc).__name__}: {exc}"}


def _book_depth_metrics(asks: List[tuple[float, float]], bids: List[tuple[float, float]]) -> dict[str, Any]:
    """Compute top-of-book spread and cumulative +1c/+2c/+3c ask depth in USD."""
    top_ask = asks[0][0] if asks else None
    top_bid = bids[0][0] if bids else None
    spread = round(top_ask - top_bid, 4) if (top_ask is not None and top_bid is not None) else None
    d1 = sum(p * q for p, q in asks if top_ask is not None and p <= top_ask + 0.0101)
    d2 = sum(p * q for p, q in asks if top_ask is not None and p <= top_ask + 0.0201)
    d3 = sum(p * q for p, q in asks if top_ask is not None and p <= top_ask + 0.0301)
    bid_d2 = sum(p * q for p, q in bids if top_bid is not None and p >= top_bid - 0.0201)
    return {
        "top_bid": top_bid, "top_ask": top_ask, "spread": spread,
        "ask_d1_usd": round(d1, 2), "ask_d2_usd": round(d2, 2),
        "ask_d3_usd": round(d3, 2), "bid_d2_usd": round(bid_d2, 2),
    }


def probe_live_5m_vs_15m() -> List[dict[str, Any]]:
    """Compare live active 15m BTC CLOB book depth against 5m BTC/ETH/SOL."""
    now = int(time.time())
    targets = [
        ("15m", "BTC", (now // 900) * 900),
        ("5m", "BTC", (now // 300) * 300),
        ("5m", "ETH", (now // 300) * 300),
        ("5m", "SOL", (now // 300) * 300),
    ]
    rows: List[dict[str, Any]] = []
    for tf, coin, bucket in targets:
        slug = f"{coin.lower()}-updown-{tf}-{bucket}"
        m = _http_get_json(f"https://gamma-api.polymarket.com/markets/slug/{slug}")
        if not isinstance(m, dict) or "_error" in m or not m.get("clobTokenIds"):
            rows.append({"tf": tf, "coin": coin, "slug": slug, "error": m.get("_error", "missing") if isinstance(m, dict) else "invalid"})
            continue
        tids = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
        up_tid = str(tids[0])
        bk = _http_get_json(f"https://clob.polymarket.com/book?token_id={up_tid}")
        fr = _http_get_json(f"https://clob.polymarket.com/fee-rate?token_id={up_tid}")
        asks = sorted(((float(a["price"]), float(a["size"])) for a in (bk.get("asks") or [])), key=lambda x: x[0]) if isinstance(bk, dict) else []
        bids = sorted(((float(b["price"]), float(b["size"])) for b in (bk.get("bids") or [])), key=lambda x: x[0], reverse=True) if isinstance(bk, dict) else []
        dm = _book_depth_metrics(asks, bids)
        desc = str(m.get("description") or "").replace("\n", " ")
        rows.append({
            "tf": tf, "coin": coin, "slug": slug,
            "volume_usd": round(float(m.get("volumeNum") or 0.0), 2),
            "liquidity_usd": round(float(m.get("liquidityNum") or 0.0), 2),
            "base_fee": fr.get("base_fee") if isinstance(fr, dict) else None,
            "fee_type": m.get("feeType"),
            "twap_60s": "twap-60s" in str(m.get("resolutionSource") or "").lower() or "TWAP" in desc,
            **dm, "error": None,
        })
    return rows


def _fetch_one_settled_15m(window_ts: int) -> dict[str, Any]:
    """Fetch one settled `btc-updown-15m` market metadata and UP `prices-history`."""
    slug = f"btc-updown-15m-{window_ts}"
    m = _http_get_json(f"https://gamma-api.polymarket.com/markets/slug/{slug}")
    if not isinstance(m, dict) or "_error" in m or not m.get("clobTokenIds"):
        return {"window_ts": window_ts, "slug": slug, "ok": False, "error": m.get("_error", "not_found") if isinstance(m, dict) else "bad_json", "history": []}
    outcomes = json.loads(m["outcomes"]) if isinstance(m.get("outcomes"), str) else (m.get("outcomes") or [])
    prices = json.loads(m["outcomePrices"]) if isinstance(m.get("outcomePrices"), str) else (m.get("outcomePrices") or [])
    tids = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
    winner: Optional[str] = None
    if m.get("closed") and len(outcomes) == len(prices):
        for o_lbl, p_val in zip(outcomes, prices):
            if float(p_val) >= 0.99:
                winner = str(o_lbl).upper()
    up_tid = str(tids[0])
    ph = _http_get_json(f"https://clob.polymarket.com/prices-history?market={up_tid}&startTs={window_ts}&endTs={window_ts + 900}&fidelity=1")
    hist = ph.get("history", []) if isinstance(ph, dict) else []
    return {
        "window_ts": window_ts, "slug": slug, "ok": winner in ("UP", "DOWN"),
        "resolved_outcome": winner, "volume_usd": round(float(m.get("volumeNum") or 0.0), 2),
        "liquidity_usd": round(float(m.get("liquidityNum") or 0.0), 2),
        "up_token_id": up_tid, "history": hist, "error": None,
    }


def _fetch_binance_1m(start_ts: int, end_ts: int) -> dict[int, Tuple[float, float]]:
    """Fetch Binance 1m klines `(open, close)` keyed by open_ts_sec across exact range."""
    out: dict[int, Tuple[float, float]] = {}
    cur_ms = start_ts * 1000
    end_ms = end_ts * 1000
    while cur_ms <= end_ms:
        url = f"https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&startTime={cur_ms}&endTime={end_ms}&limit=1000"
        data = _http_get_json(url, timeout=10.0)
        if not isinstance(data, list) or not data:
            break
        for row in data:
            ts_s = int(row[0] // 1000)
            out[ts_s] = (float(row[1]), float(row[4]))
        last_ms = int(data[-1][0])
        if last_ms <= cur_ms:
            break
        cur_ms = last_ms + 60_000
    return out


def _causal_sigma_1s_from_1m(bn_1m: dict[int, Tuple[float, float]], t_sec: int, lookback_mins: int = 30) -> float:
    """Estimate causal 1s volatility from trailing `lookback_mins` closed 1m returns strictly <= t_sec."""
    rets: List[float] = []
    for k in range(lookback_mins, 0, -1):
        bar = bn_1m.get(t_sec - k * 60)
        if bar is not None and bar[0] > 0.0 and bar[1] > 0.0:
            rets.append(math.log(bar[1] / bar[0]))
    if len(rets) < 8:
        return TWAP_V2_FILTERED_CONFIG.min_1s_vol_floor
    sig_1m = float(np.std(np.asarray(rets, dtype=float), ddof=1))
    return max(TWAP_V2_FILTERED_CONFIG.min_1s_vol_floor, sig_1m / math.sqrt(60.0))


def _latest_hist_up_mid(hist: List[dict[str, Any]], t_eval: int) -> Optional[float]:
    """Return latest CLOB UP midpoint recorded strictly at or before `t_eval` within 90s."""
    cands = [(int(h["t"]), float(h["p"])) for h in hist if (t_eval - 90) <= int(h["t"]) <= t_eval]
    if not cands:
        return None
    cands.sort(key=lambda x: x[0])
    return cands[-1][1]


def evaluate_15m_window_signal(
    mkt: dict[str, Any],
    bn_1m: dict[int, Tuple[float, float]],
    eval_minutes: Tuple[int, ...],
    max_ask: float = 0.82,
    spread_pad: float = 0.01,
) -> Optional[dict[str, Any]]:
    """Evaluate first passing late-window entry for one settled 15m window (zero look-ahead)."""
    wt = int(mkt["window_ts"])
    out = mkt.get("resolved_outcome")
    s0_bar = bn_1m.get(wt)
    if out not in ("UP", "DOWN") or s0_bar is None or s0_bar[0] <= 0.0:
        return None
    strike = s0_bar[0]
    cfg = TWAP_V2_FILTERED_CONFIG
    for m_idx in eval_minutes:
        t_eval = wt + m_idx * 60
        bar_now = bn_1m.get(t_eval - 60)
        if bar_now is None or bar_now[1] <= 0.0:
            continue
        spot = bar_now[1]
        rem = float(900 - m_idx * 60)
        sigma_1s = _causal_sigma_1s_from_1m(bn_1m, t_eval - 60, 30)
        fv = estimate_twap_fair_value(strike, spot, rem, sigma_1s, (None, 0.0), (0.0, None, 0.0), cfg)
        if fv is None or abs(fv.raw_z_score) < cfg.min_z_score:
            continue
        side = "UP" if fv.raw_z_score > 0.0 else "DOWN"
        p_dir = fv.p_true_up if side == "UP" else fv.p_true_down
        if p_dir < cfg.min_true_prob:
            continue
        up_mid = _latest_hist_up_mid(mkt["history"], t_eval)
        if up_mid is None:
            continue
        side_mid = up_mid if side == "UP" else (1.0 - up_mid)
        ask_proxy = round(min(0.99, side_mid + spread_pad), 2)
        if ask_proxy < cfg.min_entry_price or ask_proxy > max_ask or (p_dir - ask_proxy) > cfg.max_model_market_divergence:
            continue
        ev = analyze_trade_ev(p_dir, ask_proxy, BTC_NOV20_FEE)
        if ev.net_ev_per_share < cfg.min_net_edge:
            continue
        won = side == out
        pnl_sh = round((1.0 if won else 0.0) - ev.total_cost_per_share, 5)
        return {
            "window_ts": wt, "slug": mkt["slug"], "volume_usd": mkt["volume_usd"],
            "resolved_outcome": out, "eval_elapsed_sec": m_idx * 60, "side": side,
            "z": round(abs(fv.raw_z_score), 3), "p_true": round(p_dir, 4),
            "ask_proxy": ask_proxy, "fee_sh": round(ev.fee_per_share, 5),
            "won": won, "pnl_per_share": pnl_sh,
        }
    return None


def summarize_15m_signals(rows: List[dict[str, Any]], total_settled: int) -> dict[str, Any]:
    """Summarize 15m replay rows into count, win rate, mean PnL/share, and SE."""
    n = len(rows)
    if n == 0:
        return {"settled_windows": total_settled, "signals": 0, "trades_per_day": 0.0, "wr_pct": None, "mean_ask": None, "mean_pnl_sh": None, "se_pnl_sh": None}
    pnls = np.array([float(r["pnl_per_share"]) for r in rows], dtype=float)
    asks = np.array([float(r["ask_proxy"]) for r in rows], dtype=float)
    wins = sum(1 for r in rows if r["won"])
    days = max(1 / 96.0, total_settled / 96.0)
    se = float(np.std(pnls, ddof=1) / math.sqrt(n)) if n >= 2 else None
    return {
        "settled_windows": total_settled, "signals": n,
        "trades_per_day": round(n / days, 2),
        "wr_pct": round(wins / n * 100.0, 2),
        "mean_ask": round(float(np.mean(asks)), 4),
        "mean_pnl_sh": round(float(np.mean(pnls)), 5),
        "se_pnl_sh": round(se, 5) if se is not None else None,
    }


def check_30d_15m_structural_persistence(parquet_path: pathlib.Path, days: int = 30) -> dict[str, Any]:
    """Offline 30d check on `BTCUSDT_5m.parquet`: at 15m T+600s (remaining 5m) |Z|>=1.30 close consistency."""
    df = pd.read_parquet(parquet_path).sort_values("open_time").reset_index(drop=True)
    need_bars = days * 288 + 60
    sub = df.iloc[-min(len(df), need_bars):].reset_index(drop=True)
    rets_5m = np.log(sub["close"].to_numpy(float) / np.maximum(1e-9, sub["open"].to_numpy(float)))
    opens = sub["open"].to_numpy(float)
    closes = sub["close"].to_numpy(float)
    ts_ms = sub["open_time"].to_numpy(np.int64)
    total_w = wins = 0
    for i in range(40, len(sub) - 2):
        ts_sec = int(ts_ms[i] // 1000)
        if ts_sec % 900 != 0:
            continue
        if (ts_ms[i + 2] - ts_ms[i]) != 600_000:
            continue
        sig_5m = float(np.std(rets_5m[i - 36 : i], ddof=1))
        sig_1s = max(TWAP_V2_FILTERED_CONFIG.min_1s_vol_floor, sig_5m / math.sqrt(300.0))
        strike = opens[i]
        px_10m = closes[i + 1]
        final_c = closes[i + 2]
        fv = estimate_twap_fair_value(strike, px_10m, 300.0, sig_1s, (None, 0.0), (0.0, None, 0.0), TWAP_V2_FILTERED_CONFIG)
        if fv is None or abs(fv.raw_z_score) < TWAP_V2_FILTERED_CONFIG.min_z_score:
            continue
        total_w += 1
        pred_up = fv.raw_z_score > 0.0
        if pred_up == (final_c >= strike):
            wins += 1
    wr = round(wins / total_w * 100.0, 2) if total_w else None
    return {"days": days, "signals": total_w, "wins": wins, "wr_pct": wr}


def run_dual_engine_depth_mc(
    label: str,
    win_rate: float,
    trades_per_day: float,
    ask_cost: float = 0.78,
    n_paths: int = 10000,
    seed: int = 42,
) -> dict[str, Any]:
    """Depth-capped Monte Carlo ($40 -> $10,000, 5% active risk, max 30% of +2c book depth)."""
    rng = np.random.default_rng(seed)
    # Mix: 35% 5m BTC ($1050 depth), 20% 5m ETH ($135), 15% 5m SOL ($102), 30% 15m BTC ($2000)
    coin_caps = np.array([315.0, 40.5, 30.6, 600.0])  # 30% of +2c book depth
    coin_probs = np.array([0.35, 0.20, 0.15, 0.30])
    win_sh = 1.0 - ask_cost
    loss_sh = -ask_cost
    ev_sh = round(win_rate * win_sh + (1.0 - win_rate) * loss_sh, 4)
    n_steps_180 = max(1, int(round(trades_per_day * 180.0)))

    hit_120 = hit_180 = 0
    days_list: List[float] = []
    max_dds: List[float] = []
    for _ in range(n_paths):
        eq = 40.0
        vault = 0.0
        peak_act = 40.0
        peak_tot = 40.0
        mdd = 0.0
        hit_day: Optional[float] = None
        c_idxs = rng.choice(4, size=n_steps_180, p=coin_probs)
        rolls = rng.random(n_steps_180)
        for step_i in range(n_steps_180):
            act = max(0.0, eq - vault)
            min_usd = 5.0 * ask_cost
            if act < min_usd + 0.05:
                break
            cap_usd = float(coin_caps[c_idxs[step_i]])
            stake = min(cap_usd, max(min_usd, act * 0.05))
            shares = math.floor((stake / ask_cost) * 100.0) / 100.0
            pnl = shares * (win_sh if rolls[step_i] < win_rate else loss_sh)
            eq = max(vault, eq + pnl)
            new_v, _, _ = compute_vault_locked_u(eq, vault, active_peak_u=peak_act)
            if new_v > vault:
                vault = new_v
                peak_act = max(0.0, eq - vault)
            else:
                peak_act = max(peak_act, eq - vault)
            peak_tot = max(peak_tot, eq)
            mdd = max(mdd, (peak_tot - eq) / peak_tot if peak_tot > 0 else 0.0)
            if eq >= 10000.0 and hit_day is None:
                hit_day = (step_i + 1) / trades_per_day
                break
        max_dds.append(mdd * 100.0)
        if hit_day is not None:
            hit_180 += 1
            days_list.append(hit_day)
            if hit_day <= 120.0:
                hit_120 += 1
    return {
        "scenario": label, "win_rate_pct": round(win_rate * 100.0, 1), "trades_per_day": trades_per_day,
        "ev_sh": ev_sh, "ev_stake_pct": round(ev_sh / ask_cost * 100.0, 2),
        "p_10k_120d_pct": round(hit_120 / n_paths * 100.0, 2),
        "p_10k_180d_pct": round(hit_180 / n_paths * 100.0, 2),
        "p50_days": round(float(np.median(days_list)), 1) if days_list else None,
        "p95_mdd_pct": round(float(np.percentile(max_dds, 95)), 2),
    }


def _write_research_report(
    live_rows: List[dict[str, Any]],
    settled_ok: List[dict[str, Any]],
    s_narrow: dict[str, Any],
    s_wide: dict[str, Any],
    s_band88: dict[str, Any],
    struct_30d: dict[str, Any],
    mc_rows: List[dict[str, Any]],
) -> None:
    vols = [r["volume_usd"] for r in settled_ok]
    med_vol = round(float(np.median(vols)), 2) if vols else 0.0
    p25_vol = round(float(np.percentile(vols, 25)), 2) if vols else 0.0
    lines = [
        "# Polymarket `15m BTC` 雙引擎驗證與 `\\$40U → \\$10,000U` 容量分析",
        "",
        "## 1. 即時盤口對比（`15m BTC` vs `5m BTC/ETH/SOL`）",
        "",
        f"- 過去 24 小時（{len(settled_ok)} 個已結算 `btc-updown-15m` 視窗）真實成交量：**中位數 `\\${med_vol:,.0f}` / 窗**（P25 `\\${p25_vol:,.0f}`），結算來源與 5m 相同為 **`Chainlink btc-usd-twap-60s`**。",
        "",
        "| TF | Coin | Volume (Cur) | Liquidity | Bid/Ask (Spread) | `+1¢` Depth | `+2¢` Depth | `+3¢` Depth | `base_fee` | `60s TWAP` |",
        "|---|---|---:|---:|---|---:|---:|---:|---:|---|",
    ]
    for r in live_rows:
        if r.get("error"):
            lines.append(f"| `{r['tf']}` | `{r['coin']}` | ERR `{r['error']}` | - | - | - | - | - | - | - |")
            continue
        lines.append(
            f"| `{r['tf']}` | `{r['coin']}` | `${r['volume_usd']:,.0f}` | `${r['liquidity_usd']:,.0f}` | "
            f"`{r['top_bid']}/{r['top_ask']}` (`{r['spread']}`) | `${r['ask_d1_usd']:,.0f}` | **`${r['ask_d2_usd']:,.0f}`** | "
            f"`${r['ask_d3_usd']:,.0f}` | `{r['base_fee']}` | `{r['twap_60s']}` |"
        )
    lines += [
        "",
        "## 2. 過去 24 小時 `btc-updown-15m` 官方結算 × 真實 CLOB `prices-history` 重播",
        "",
        "> ⚠️ **誠實口徑聲明**：此處使用免費 CLOB `prices-history`（每分鐘 1 點 mid + 保守 `+1¢` ask proxy）配對 Binance 1m 波動率與 **官方 Gamma `resolved_outcome`**；僅驗證結構與方向成立與否，**不可直接取代 1 Hz `+1s` FAK 深度回測**（上實盤前須另開獨立 15m 1Hz 錄製驗證）。",
        "",
        "| Slice | Windows | Signals | /Day | Official WR% | Mean Ask Proxy | Net PnL/sh (5% BTC fee) | SE |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
        f"| 嚴格尾盤 `elapsed∈[720,840]` (`[0.68,0.82]`) | {s_narrow['settled_windows']} | {s_narrow['signals']} | {s_narrow['trades_per_day']} | **{s_narrow['wr_pct']}%** | `{s_narrow['mean_ask']}` | **{s_narrow['mean_pnl_sh']}** | {s_narrow['se_pnl_sh']} |",
        f"| 較寬尾盤 `elapsed∈[660,840]` (`[0.68,0.82]`) | {s_wide['settled_windows']} | {s_wide['signals']} | {s_wide['trades_per_day']} | **{s_wide['wr_pct']}%** | `{s_wide['mean_ask']}` | **{s_wide['mean_pnl_sh']}** | {s_wide['se_pnl_sh']} |",
        f"| 放寬高價帶 `elapsed∈[660,840]` (`[0.68,0.88]`) | {s_band88['settled_windows']} | {s_band88['signals']} | {s_band88['trades_per_day']} | **{s_band88['wr_pct']}%** | `{s_band88['mean_ask']}` | **{s_band88['mean_pnl_sh']}** | {s_band88['se_pnl_sh']} |",
        "",
        "## 3. 30 天離線結構驗證（`data/BTCUSDT_5m.parquet`，完全零前視）",
        "",
        f"- 檢驗 15m 視窗行至第 10 分鐘（剩最後 5 分鐘）時 `|Z| >= 1.30` 的順勢結算勝率：觸發 **`{struct_30d['signals']}` 次**，收盤順向勝 **`{struct_30d['wins']}` 次** = **`{struct_30d['wr_pct']}%`**。",
        "",
        "## 4. 雙引擎（`5m` + `15m BTC`）考慮盤口深度上限的 `\\$40U → \\$10,000U` Monte Carlo",
        "",
        "> 資金規則：單筆取活躍資金 `5%`（最小 5 股），且嚴格限制**單筆金額不得超過該市場 `+2¢` 深度的 `30%`**（`5m BTC=$315`、`15m BTC=$600`、`5m ETH=$40.5`、`5m SOL=$30.6`），並套用保險箱鎖利。",
        "",
        "| Scenario | WR% | Trades/d | Net EV/sh (Cost `0.78`) | EV/Stake% | 120d 達 `$10k` | 180d 達 `$10k` | P50 達標天數 | P95 MaxDD% |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for m in mc_rows:
        lines.append(
            f"| {m['scenario']} | {m['win_rate_pct']}% | {m['trades_per_day']} | `{m['ev_sh']:+.4f}` | {m['ev_stake_pct']}% | "
            f"**{m['p_10k_120d_pct']}%** | **{m['p_10k_180d_pct']}%** | {m['p50_days']} 天 | {m['p95_mdd_pct']}% |"
        )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_btc_15m_validation(n_windows: int = 96) -> dict[str, Any]:
    """Run live probe, 24h official-outcome replay, 30d structural check, and dual-engine MC."""
    live_rows = probe_live_5m_vs_15m()
    cur_15 = (int(time.time()) // 900) * 900
    w_list = [cur_15 - (k + 2) * 900 for k in range(n_windows)]
    with ThreadPoolExecutor(max_workers=6) as pool:
        fetched = list(pool.map(_fetch_one_settled_15m, sorted(w_list)))
    settled_ok = [m for m in fetched if m.get("ok")]
    bn_1m = _fetch_binance_1m(min(w_list) - 3600, max(w_list) + 900)

    r_narrow = [r for m in settled_ok if (r := evaluate_15m_window_signal(m, bn_1m, (12, 13, 14), 0.82)) is not None]
    r_wide = [r for m in settled_ok if (r := evaluate_15m_window_signal(m, bn_1m, (11, 12, 13, 14), 0.82)) is not None]
    r_88 = [r for m in settled_ok if (r := evaluate_15m_window_signal(m, bn_1m, (11, 12, 13, 14), 0.88)) is not None]
    pd.DataFrame(r_wide).to_csv(OUT_CSV, index=False)

    s_narrow = summarize_15m_signals(r_narrow, len(settled_ok))
    s_wide = summarize_15m_signals(r_wide, len(settled_ok))
    s_88 = summarize_15m_signals(r_88, len(settled_ok))
    struct_30d = check_30d_15m_structural_persistence(DATA_DIR / "BTCUSDT_5m.parquet", days=30)

    mc_rows = [
        run_dual_engine_depth_mc("Fail-Case (WR 80%, 20/d)", 0.80, 20.0),
        run_dual_engine_depth_mc("Conservative 5m Only (WR 82%, 15/d)", 0.82, 15.0),
        run_dual_engine_depth_mc("Conservative Dual (WR 82%, 22/d)", 0.82, 22.0),
        run_dual_engine_depth_mc("Target E4 Dual (WR 85%, 22/d)", 0.85, 22.0),
        run_dual_engine_depth_mc("Strong E4 Dual (WR 87%, 25/d)", 0.87, 25.0),
    ]
    _write_research_report(live_rows, settled_ok, s_narrow, s_wide, s_88, struct_30d, mc_rows)
    return {
        "settled_ok": len(settled_ok), "s_narrow": s_narrow, "s_wide": s_wide,
        "s_88": s_88, "struct_30d": struct_30d, "mc_rows": mc_rows, "report_path": str(OUT_MD),
    }


if __name__ == "__main__":
    print(json.dumps(run_btc_15m_validation(), indent=2))
