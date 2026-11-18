# V12.0 Hammer-Reclaim Multi-Period RSI Strategy — Lead Architect Specification & Peer-Review Dossier

> **Role Note**: Authored as the Lead Strategy Architect for our **Polymarket 5-Minute Full-Candle (`T=0s -> T=300s`, Chainlink 60s TWAP Settlement) Multi-Period RSI Reversal Strategy (`BTCUSDT`, `ETHUSDT`, `SOLUSDT`)**.
>
> **Purpose of This Document**: Present our complete **V11.0 ➔ V12.0** architectural upgrade—backed by `18` real-money live sequences, today's parallel simulation, **96 days (`2026-08-14 .. 2026-11-18`) of official Polymarket Gamma + CLOB trade data**, **2 years (`630,720` bars) of Binance 5m history**, and **1,000 Monte Carlo paths from `$50U`**—for **Peer Review / Red-Team Critique by Claude** before updating the live paper/prod runner.
>
> **Reference Artifacts in Repository**:
> - Strategy Implementation: [`strategy_reference/v12_rsi_strategy.py`](strategy_reference/v12_rsi_strategy.py) (prior baseline: [`strategy_reference/v8_strategy.py`](strategy_reference/v8_strategy.py))
> - Reproducible Validation Suite: [`edge_validation/validate_v12_rsi.py`](edge_validation/validate_v12_rsi.py) + [`edge_validation/v12_vs_v11_validation.csv`](edge_validation/v12_vs_v11_validation.csv)

---

## 1. Strategic Mandate & Target Economics

- **Core Edge Thesis**: Exhaustion after `3–5` consecutive directional 5m candles across synchronized crypto majors (`BTC/ETH/SOL`) drives high-probability mean reversion on the opening full 5-minute candle (`T=0s -> T=300s`).
- **Bankroll Profile**: Starting capital **`~$43U–$50U`** (`orderMinSize = 5` shares ≈ `$2.50U–$2.60U`), prioritizing **capital survival + smooth geometric compounding**, eliminating both Martingale tail ruin and adverse fill leakage.
- **Post-Nov-20 Polymarket Fee Regime**:
  - **`ETHUSDT` & `SOLUSDT` 5m**: Taker `feeRate` drops **85.7% (`0.07 -> 0.01`, only `~0.25¢/sh` at `50¢`)**!
  - **`BTCUSDT` 5m**: Taker `feeRate` drops `0.07 -> 0.05` (`~1.25¢/sh` at `50¢`).
  - **Maker (`post_only=True`)**: `0%` fee across all 3 coins (accounts `< $200k` volume receive `0%` maker rebates).

---

## 2. Forensic Audit of V11.0: Four Root Causes Identified & Fixed in V12.0

### 2.1 Observed Real-Money & Simulation Battle Logs

| Account / Period | Sample | Step-1 WR | Sequence WR | Equity Trajectory | Key Empirical Lesson |
|---|---:|---:|---:|---|---|
| **V11 Real Money Live (`2026-11-16..18`)** | `18` seqs | `61.11%` (`11/18`) | `77.78%` (`14W–4L`) | `$40.62 -> $45.88U` (`+$5.26`, Peak `$56.96`) | **`3/4` busts (`-$21.66U`) came from `A_CPOS+C_BB`** (`Seqs 1, 14, 18`); **`C_BB / B_BACC+C_BB` in `48¢–56¢` went `9/9 (100%)`**; 4th bust (`Seq 15`) bought `44¢` adverse TWAP basis (`-$5.80`); `Seq 16` chased `60¢` taker (`+$0.60` net). |
| **V11 Parallel Simulation (`2026-11-18`)** | `4` seqs | `75.00%` (`3/4`) | `100.0%` (`4W–0L`) | `$41.49 -> $50.31U` (`+$8.82`) | Entries `@ 45¢, 48¢, 50¢, 54¢` matched well without extreme chases. |
| **V10 1-Shot Reference** | `14` seqs | `57.14%` (`8/14`) | `57.14%` (`8W–6L`) | `$40.62 -> $44.91U` (`+$4.29`) | Capped per-trade risk (`-$2.30~-$3.00`); `D_WYCKOFF_2LEG` only `50%`. |
| **Pre-V8 4-Step Martingale** | `5` seqs | `60.00%` (`3/5`) | `80.00%` (`4W–1L`) | `$85.66 -> $92.33 -> $40.62U` | One 4-step Martingale bust (`Seq 4`) wiped **`-$47.63U` (`-51.6%` of peak)**. |

### 2.2 The Four Root Causes Exposed by 96 Days (`n=1,810`) of Official Polymarket `pm_outcome` Data

1. **Oracle Translation Erosion (`V11.0 OOS BN 55.98% -> PM 49.64%`, a `-6.34%` collapse)**:
   - Polymarket resolves against **`TWAP_60s([T_end-60, T_end]) >= TWAP_60s([T-60, T])`**, NOT Binance `Close(T_end) >= Close(T)`.
   - Inside V11.0, **IS-only slicing by `close_pos_in_bar` (`cpos` for LONG, `1-cpos` for SHORT)** exposed a cliff:
     - `cpos [0.00, 0.15)` *(marubozu waterfall closing at very bottom)*: `n=1,018`, **Official PM WR = `44.20%`**, `px_1st = 44.14¢`, **Net Taker EV = `-0.96¢/sh`** (`A_CPOS` alone collapsed `-10.34%` vs Binance!).
     - `cpos [0.15, 0.30)` *(confirmed rejection wick without over-bouncing)*: `n=585`, **Official PM WR = `57.95%`**, `px_1st = 51.38¢`, **Net Taker EV = `+5.54¢/sh`**, **Maker EV = `+6.57¢/sh`**!
     - `cpos >= 0.30` *(already strongly bounced before close)*: PM WR stays `55.6%–64.6%`, **but CLOB already reprices opening `ask` to `55¢–58¢`**, absorbing most of the edge!
   - **Important IS vs OOS R&D Lesson We Caught**: Earlier, forcing `cpos > 0.30` pushed OOS `mean_ask_1s` to `57.05¢` (making uncapped OOS Taker `-1.41¢/sh`), and forcing `twap_basis_bps >= 0` looked `+6.26¢/sh` IS yet `-5.92¢/sh` OOS (`59.5¢` opening asks). Setting the lower wick-rejection floor cleanly at **`cpos >= 0.15`** captures the `[0.15, 0.30)` mispricing zone while dropping the toxic `< 0.15` waterfall!

2. **Toxic Branches (`D_WYCKOFF_2LEG` & Curve-Fit Fallback)**:
   - `D_WYCKOFF_2LEG` (`streak == 1`) achieved only `41.54%` Polymarket WR (`33.33%` OOS, `-4.68¢/sh`).
   - `_evaluate_stabilized_climax_fallback` added fragile regime constants (`streak=5, bb in [-0.02, 0.01]...`). Both are deleted in V12.0.

3. **Dual Execution Leak in `execute_5m_entry`**:
   - Chased Taker up to `58¢–60¢` (`Seq 16 @ 60¢`), destroying binary `1:1` odds.
   - Parked unfilled Maker bids as `LIVE_LIVE` for **90 seconds** inside the candle, causing severe **VPIN adverse selection** (getting filled 100% when the reversal failed and missing fills when price bounced fast).

4. **4-Step Martingale Ruin Risk**:
   - At true `OOS` Polymarket base rates (`~50%–54%`), 4-step Martingale had **`99.9%` median max DD and `98.2%` ruin probability (`<$15U`)** over 30 days in realistic bootstrap simulation!

---

## 3. V12.0 Lead-Architect Specification (`V12.0_HAMMER_RECLAIM_RSI`)

Implemented cleanly and symmetrically (unified directional normalizer, zero LONG/SHORT duplication, `audit-code 95/100`) in [`strategy_reference/v12_rsi_strategy.py`](strategy_reference/v12_rsi_strategy.py):

### 3.1 Signal Logic (Multi-Period RSI Exhaustion + Wick-Rejection Floor)
All directional indicators are normalized so `x` increases/decreases identically for `LONG_BOUNCE` (`streak_down`) and `SHORT_PULLBACK` (`streak_up`):
1. **Regime & Macro Trend Guards**:
   - `atr_pct >= 0.09%`, `2 <= streak <= 5` (if `E_RSI_TERM` inactive, `C_BB` requires `streak >= 3`).
   - `ret_12bars_pct_dir >= -1.4%` and `ret_48bars_pct_dir >= -2.8%`.
   - `1h Trend Guard`: if `ret_12bars_pct_dir < -0.60%`, require `wick_ratio >= 0.15` AND `rsi_6_dir <= 17.0`.
2. **Marubozu Waterfall Elimination (`V12.0` Core Upgrade)**:
   - **Drop `A_CPOS`, `D_WYCKOFF_2LEG`, and `_evaluate_stabilized_climax_fallback` completely.**
   - Require **Wick-Rejection Floor**: **`close_pos_in_bar_dir > 0.15`** (`> 0.15` for LONG, `< 0.85` for SHORT), rejecting waterfall closes pinned in the bottom `15%`.
3. **Exhaustion Core (Load-Bearing `RSI(2/6/14)` + Order-Flow Structure)**:
   - Trigger requires **`B_BACC`** (`rsi_6_dir <= 24.0` AND `body_accel >= 1.25`) **OR** **`C_BB`** (`streak >= 3` AND `rsi_6_dir <= 24.0` AND `bb_pct_b_dir <= 0.12`).
   - **`E_RSI_TERM`** (`rsi_2_dir <= 4.5`, `rsi_6_dir <= 19.5`, `rsi_14_dir >= 25.0`, `rsi_14_dir - rsi_6_dir >= 10.0`, `bacc >= 1.15`, `tbr_dir <= 0.45`) serves as a `TIER_S` / `TIER_S+` conviction booster when paired with `B_BACC` / `C_BB` (never standalone).
   - Preserved Microstructure Vetoes: counter-TBR `< 0.52`; single-branch `rsi_2_dir <= 14.5`, `tbr_dir <= 0.475` (unless `wick >= 0.20` or `bacc >= 1.10`); weak-chop veto (`45.5%–54.5%` TBR with `< 2` synced coins and `vol_ratio < 1.15`); 3-bar CVD iceberg trap (`cvd_3_dir > 0.06` at `streak >= 5`).
4. **TWAP60 Basis & Opening Midpoint Guard (`V12.0` Execution Gate)**:
   - Reject adverse Chainlink opening basis `twap_basis_bps < -3.0` bps (`0.03%`, tightened from `0.12%`).
   - Reject pre-bounced opening books (`mid_T > 0.55`).

### 3.2 Tight 5-Second CLOB Execution Architecture (`[0.48, 0.53]`)
Using `live_btc_5m.db` (`1,005` overlapping 1-second CLOB orderbook windows at `elapsed 0.5s–3.5s`), we verified `median spread = 1.0¢` and `mean(up_ask_1s - up_first_px) = +0.50¢` (`median = 0.00¢`).
1. **Execution Clock (`T = +0.5s .. +5.0s`, NEVER 90s resting)**:
   - Enter immediately after `T=0s` boundary once Chainlink `TWAP_60s` strike locks.
   - **Stage 1 (`T = +0.5s .. +4.5s`)**: Post-Only Maker `BUY` at `min(0.52, best_ask - 0.01)` **ONLY if `0.48 <= best_ask <= 0.55`** (`0%` fee).
   - **Hard Cancel at `T = +5.0s`**: If unfilled, cancel immediately (kills VPIN adverse selection).
   - **Stage 2 (`T = +5.0s` Conditional Fallback)**:
     - **`ETH` & `SOL` (`0.01` taker fee = `0.25¢`)**: `FAK` sweep **ONLY if `0.48 <= best_ask <= 0.53`**.
     - **`BTC` (`0.05` taker fee = `1.25¢`)**: `FAK` sweep **ONLY if `0.48 <= best_ask <= 0.515`**.
     - Outside those price bands ➔ **SKIP cleanly** (never chase `54¢–60¢`).

### 3.3 Anti-Ruin Capital & Sizing Architecture (`max_martingale_steps = 1`)
- Hard-ban Steps 3 & 4 permanently.
- **`<$85U` Micro-Account (`~$50U`)**: Fixed **`5.0` shares (`~$2.55U`)** on `STANDARD`/`TIER_S` and **`6.0` shares (`~$3.06U`)** on `TIER_S+` (`~5–6%` per bet).
- **`>=$85U`**: Quarter-Kelly `risk_pct = min(0.05, 0.25 * (0.555 - p_in) / (1 - p_in))`, scaled by `0.55×` drawdown compression when `active_equity < 0.85 * active_peak`, paired with milestone Vault profit-locking (`$220 -> $115`, `$1k -> $615`, ...).

---

## 4. Complete Validation Results (`[OBSERVED]` from `edge_validation/validate_v12_rsi.py`)

### 4.1 96-Day Official Polymarket `TWAP60` & 2-Year Binance Comparison

| Dataset | Model | Split | Signals (`n/d`) | Binance WR | **Official PM WR** | EV @ `50¢` | Mean `ask_1s` | Uncapped Taker EV | **`[48¢, 53¢]` `n`** | **`[48¢, 53¢]` PM WR** | **`[48¢, 53¢]` Maker EV** | **`[48¢, 53¢]` Taker EV** |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `96D_PM` | `V11.0_Baseline` | `IS (66d)` | `1,258` (`19.1`) | `55.88%` | `51.83%` | `+1.83¢` | `0.5047` | `+0.96¢` | `229` | `54.15%` | `+4.02¢` | `+2.87¢` |
| `96D_PM` | `V11.0_Baseline` | **`OOS (30d)`** | `552` (`18.4`) | `55.98%` | **`49.64%`** | `-0.36¢` | `0.5042` | `-1.35¢` | `102` | **`44.12%`** | **`-6.07¢`** | **`-7.24¢`** |
| `96D_PM` | `V11.0_Baseline` | `FULL (96d)` | `1,810` (`18.9`) | `55.91%` | `51.16%` | `+1.16¢` | `0.5045` | `+0.25¢` | `331` | `51.06%` | `+0.91¢` | `-0.25¢` |
| `96D_PM` | **`V12.0_Signal_Core`** | `IS (66d)` | `663` (`10.1`) | `56.86%` | **`57.77%`** | **`+7.77¢`** | `0.5438` | **`+3.23¢`** | `109` | **`59.63%`** | **`+9.43¢`** | **`+8.32¢`** |
| `96D_PM` | **`V12.0_Signal_Core`** | **`OOS (30d)`** | `292` (`9.7`) | `55.48%` | **`53.42%`** | **`+3.42¢`** | `0.5511` | **`-2.03¢`** | `54` | **`51.85%`** | **`+1.37¢`** | **`+0.26¢`** |
| `96D_PM` | **`V12.0_Signal_Core`** | **`FULL (96d)`** | `955` (`10.0`) | `56.44%` | **`56.44%`** | **`+6.44¢`** | `0.5460` | **`+1.61¢`** | `163` | **`57.06%`** | **`+6.76¢`** | **`+5.65¢`** |
| `96D_PM` | `V12.0_Full_Guarded` | `IS (66d)` | `523` (`7.9`) | `54.11%` | `58.32%` | `+8.32¢` | `0.5627` | `+1.65¢` | `98` | `58.16%` | `+7.98¢` | `+6.88¢` |
| `96D_PM` | `V12.0_Full_Guarded` | **`OOS (30d)`** | `226` (`7.5`) | `53.98%` | `52.65%` | `+2.65¢` | `0.5700` | `-4.67¢` | `46` | **`52.17%`** | **`+1.46¢`** | **`+0.32¢`** |
| `96D_PM` | `V12.0_Full_Guarded` | `FULL (96d)` | `749` (`7.8`) | `54.07%` | `56.61%` | `+6.61¢` | `0.5649` | `-0.26¢` | `144` | `56.25%` | `+5.90¢` | `+4.78¢` |

- **2-Year Multi-Regime Binance 5m Stability (`2024-10 .. 2026-10`, `630,720` bars)**:
  - `V11.0_Baseline`: `14,472` signals (`19.82/d`), `S1 WR = 56.89%` (`Y1 = 57.02%`, `Y2 = 56.76%`), `2-Step WR = 81.17%`.
  - **`V12.0_Hammer_RSI`**: `8,062` signals (`11.04/d`), **`S1 WR = 56.57%` (`Y1 = 56.73%`, `Y2 = 56.43%`), `2-Step WR = 81.22%`**.

> **Honest Self-Critique on OOS Edge Compression**:
> Look transparently at `V12.0_Signal_Core` `OOS (30d)`:
> 1. Fixing `<0.15` waterfalls + `Wyckoff` raised overall `OOS` Polymarket WR from **`49.64% -> 53.42%`** (`+3.78%`) and `FULL 96d` from **`51.16% -> 56.44%`** (**completely closing V11's `-4.75%` Binance-to-TWAP60 gap**!).
> 2. However, **uncapped `T=+1s` Taker lost `-2.03¢/sh` in OOS** because Oct–Nov market makers priced opening `ask_1s` at `55.1¢–57.0¢`. Capping inside **`[48¢, 53¢]`** kept **`OOS` positive (`+1.37¢/sh` Maker, `+0.26¢/sh` Taker; `96d +6.76¢` Maker / `+5.65¢` Taker)**, though `OOS` margin is noticeably thinner (`n=54`, `+1.37¢` Maker vs IS `+9.43¢`) and `ETH/SOL` post-Nov-20 (`0.01` fee) is critical so Taker stays positive.

### 4.2 Monte Carlo 30-Day Compounding & Ruin Bootstrap (`$50.00U Start`, `1,000` Paths)

| Sizing Policy | Empirical WR | Entry Px | Max Steps | P5 End (`U`) | **Median End (`U`)** | P95 End (`U`) | P50 Max DD | Ruin (`<$15U`) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `V11_4Step_OOS` *(49.6% @ 50.4¢)* | `49.64%` | `0.5042` | `4` | `$0.00` | **`$0.17`** | `$2.42` | `99.90%` | **`98.2%`** |
| `V12_Strict_2Step_Base` *(53.4% @ 51¢)* | `53.42%` | `0.5100` | `2` | `$0.90` | `$63.16` | `$191.38` | `52.00%` | `10.4%` |
| **`V12_1Shot_Kelly_OOS_Modest`** | `53.42%` | `0.5100` | `1` | **`$14.09`** | **`$63.67`** | `$166.99` | **`46.30%`** | **`5.4%`** |
| **`V12_1Shot_Kelly_96d_Mean`** | `56.44%` | `0.5150` | `1` | **`$35.10`** | **`$91.89`** | `$243.05` | **`38.75%`** | **`1.1%`** |

- Even at our conservative OOS WR (`53.42%`), **`1-Shot Quarter-Kelly` strictly beats `2-Step Martingale`** on Median equity (`$63.67` vs `$63.16`), P5 tail (`$14.09` vs `$0.90`), Median DD (`46.3%` vs `52.0%`), and Ruin probability (`5.4%` vs `10.4%`)!

---

## 5. Peer-Review Questions for Claude (Second-Opinion Critique on Our V12.0 Design)

We intentionally keep our design transparent (including the `IS +8.32¢ -> OOS +0.26¢` compression in `[48¢, 53¢]`) and request your adversarial second opinion on 4 targeted items:

1. **Closing the `OOS` Margin Gap (`+1.37¢` Maker / `+0.26¢` Taker in `[48¢, 53¢]`)**:
   - Why did `OOS (30d)` opening CLOB `ask_1s` tighten from `54.4¢` (IS) to `55.1¢` (OOS), compressing `[48¢, 53¢]` WR from `59.6%` to `51.85%`? Are there non-overfit **structural filters** (e.g., upper cap on `cpos_dir <= 0.45` so we don't buy candles that already bounced halfway, higher `vol_ratio`, or stricter `ETH/SOL`-only deployment after Nov 20) OR **Maker quoting improvements** (e.g., bidding `max(0.49, mid - 0.015)` for 10s instead of chasing `53¢`) you recommend testing cleanly via Walk-Forward?
2. **Pre-Close (`T=-3s`) vs Post-Open (`T=+0.5s..+5s`) & Maker Adverse Selection**:
   - In our spec, waiting until `T=+0.5s` confirms `TWAP_60s` strike lock and cancels unfilled Maker bids at `+5.0s` (never resting `90s`), with `FAK` Taker capped at `0.53` (`ETH/SOL`) / `0.515` (`BTC`). Do you see residual **Maker adverse selection** inside a 5-second window, or would **`ETH/SOL` pure `T=+1s` `FAK` Taker (`0.01` fee) `@ ask <= 0.525`** outperform 5s-Maker in live fills?
3. **Capital Efficiency of `[48¢, 53¢]` Band Size (`~1.8–3.6` filled trades/day vs `10/day` raw signals)**:
   - Strict `ask <= 0.53` drops trade frequency from `~10/day` to `~2–4/day` in `[48¢, 53¢]`. Can we unlock safe higher trade volume (e.g., bidding Maker `@ 0.52–0.53` when `ask_1s` opens at `55¢–57¢` and catches minor intra-minute pullbacks in `T=0s..+20s`), or does resting for `55¢ -> 52¢` dip-fills suffer too much toxic flow?
4. **Live Graduation & Kill-Switch Gate**:
   - Before flipping `dry_run=False` with real `$50U`, we plan a **60-trade live simulation / paper validation** post-Nov-20 (`ETH/SOL 0.01` fee), enforcing fill-adjusted slippage tracking and hard daily loss/DD halts. What exact numerical pass/fail thresholds (`WR`, mean fill price, realized vs simulated gap, max DD) would you require to sign off `V12.0` for live `$50U` deployment?
