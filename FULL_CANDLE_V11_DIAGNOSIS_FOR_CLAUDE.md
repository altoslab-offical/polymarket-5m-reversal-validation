# Full-Candle Strategy Diagnosis, Battle Records & 91-Day Official Polymarket Audit (For Claude V12 Review)

> **Executive Summary & Strategic Decision (`2026-11-18`)**:
> 1. We have **paused** the late-window Chainlink TWAP Sniper (`$43U` paper: `2W–3L, -$7.68U`, largely due to an unintended `EMERGENCY_STOP_LOSS` bug, cold-start volatility floor inflation, `416` `CHAINLINK_TWAP_STALE` skips, and structural `1 : 4.5` negative skew at `0.79–0.82` entry prices).
> 2. Over the exact same period, our full 5-minute candle reversal method (**`V11.0_ULTIMATE_RSI_MARTINGALE`**, `T=0s -> T=300s`, `~50¢` entry, `~1 : 1` binary payoff) went **`4W–0L (+$8.82U, $41.49U -> $50.31U)`** in simulation, and previously made **`+$5.26U (+12.95%, peak +40.2% / $56.96U)`** over `18` real-money live sequences (`14W–4L, 77.78%` seq WR, `61.11%` Step-1 WR).
> 3. The trader/owner strongly prefers **betting the full 5m candle (`T=0s -> T=300s`) around `~50¢`** (`1 : 1` payoff symmetry rather than `80¢` `1 : 4.5` negative skew). We have switched our live simulation & Telegram feeds back to Full-Candle V11 (`dry_run=True`, zero real money) and audited **every historical battle log + 91 days (`2026-08-14..11-13`) of official Polymarket `pm_outcome` & CLOB `pm_trades_agg` prices** to identify **exact flaws for optimizing `V12 Full-Candle`**.

---

## Section 1 — Forensic Post-Mortem of Paused Late-Window TWAP V2 (`2W–3L, -$7.68U`)

Between `2026-11-18T08:16Z` and `2026-11-18T13:51Z`, two processes ran concurrently on the same live WebSocket feeds:
- **Read-Only Reference Monitor (`edge_validation/live_recorder_42.py`)**: across `204` recorded windows (`197` settled, `R1 TWAP parity = 0.9948`), `v2_filtered` achieved **`16 filled / 22 signals` (`93.75%` WR, `+0.14299/sh` 1s fill EV)** and `v2_pure` achieved **`22 filled / 29 signals` (`90.91%` WR, `+0.15601/sh`)**.
- **Paper Execution Runner (`OracleSniperRunner`, `data/oracle_sniper_paper_43u.json`)**: went **`2W–3L` (`$43.00U -> $35.3218U`, `-$7.6782U`)** with **`416` `CHAINLINK_TWAP_STALE` skips** and `1` `EXIT_NO_FILL`.

### Exact 5-Trade Paper Ledger vs Official `windows_42` Settlement

| Window (`symbol`) | Side / Entry Px / Elapsed | `strike` Recorded | Official `windows_42` Strike -> Close (`outcome`) | Paper Exit Reason & Px | Paper PnL | Root Cause |
|---|---|---:|---|---|---:|---|
| `1794998100-ETHUSDT` | `DOWN @ 0.8200` (`199.8s`) | `1798.2084` | `1798.2084 -> 1798.3265` (**`UP`**) | `SETTLED_UP @ 0.00` | **`-$4.1074U`** | Cold-start `min_1s_vol_floor=0.000035` inflated `Z=1.49` immediately after restart (`1800s` history empty) |
| `1795000200-BTCUSDT` | `DOWN @ 0.7900` (`160.5s`) | `null` | `64097.51 -> 64105.86` (**`UP`**) | `EMERGENCY_STOP_LOSS @ 0.3526` | **`-$2.2855U`** | Missing `strike` + cold-start vol + emergency SL exit |
| `1795003200-BTCUSDT` | `DOWN @ 0.8100` (`163.2s`) | `64157.2142` | `64157.21 -> 64099.30` (**`DOWN`**) | `SETTLED_DOWN @ 1.00` | `+$0.9116U` | Normal win (`+$0.91` on `$4.09` risk) |
| `1795005900-ETHUSDT` | `UP @ 0.7937` (`239.7s`) | `1789.9600` | `1790.0685 -> 1790.4117` (**`UP`**) | **`EMERGENCY_STOP_LOSS @ 0.1747`** | **`-$3.1104U`** | **CRITICAL BUG**: `TWAP_V2_FILTERED_CONFIG` disabled `early_tp_enabled=False` but inherited `enable_emergency_exit=True`; panic-sold a winning trade (`$4.13U` swing from `+$1.02`!) |
| `1795007700-SOLUSDT` | `DOWN @ 0.8158` (`213.1s`) | `74.3304` | `74.3304 -> 74.2593` (**`DOWN`**) | `SETTLED_DOWN @ 1.00` | `+$0.9135U` | Normal win (`+$0.91` on `$4.09` risk) |

**Strategic Takeaway**: Beyond those fixable runtime bugs, buying `@ 0.79–0.82` exposes the account to **`1 : 4.5` negative payoff skew** (`+$0.91U` profit vs `-$4.11U` max loss). For a `$43U` compounding account, the full-candle `~50¢` entry structure (`1 : 1` payoff) is far more intuitive and resilient once we eliminate its known alpha & execution leaks.

---

## Section 2 — Complete Battle Records of Full-Candle V11 (`T=0s -> T=300s`)

Full strategy source code is included in [`strategy_reference/v8_strategy.py`](strategy_reference/v8_strategy.py) and [`strategy_reference/indicators.py`](strategy_reference/indicators.py).

### 2.1 Real-Money Live Track Record (`V11.0`, `2026-11-16 .. 2026-11-18`)
- **Equity Trajectory**: **`$40.62U -> $45.88U` (`+$5.26U`, `+12.95%`, Peak `$56.96U` / `+40.2%`)**
- **Sequence Summary**: `18` sequences — **`14` wins (`77.78%`)**, `4` 2-step busts (`11` Step-1 wins = **`61.11%` S1 WR**, `3` Step-2 wins).

| Seq | Symbol | Direction | Branch | Entry (¢) | Won Step | Stake S1/S2 (U) | Net PnL (U) | Equity After (U) | Diagnostic Note |
|---:|---|---|---|---:|---:|---|---:|---:|---|
| 1 | `BTCUSDT` | `LONG` | `A_CPOS+C_BB` | `52.0¢` | Bust | `5.20 / 4.00` | **`-8.84`** | `31.78` | `A_CPOS+C_BB` loss + high S1 stake |
| 2 | `BTCUSDT` | `LONG` | `A_CPOS+C_BB` | **`57.0¢`** | `1` | `2.85 / 3.50` | `+2.15` | `33.76` | Chased Taker @ `57¢` |
| 3 | `ETHUSDT` | `LONG` | `C_BB` | **`56.0¢`** | `1` | `2.80 / 3.50` | `+2.20` | `35.87` | Pure `C_BB` win; chased @ `56¢` |
| 4 | `BTCUSDT` | `SHORT` | `C_BB` | `48.0¢` | `1` | `2.40 / 3.50` | `+2.60` | `38.47` | Clean `C_BB` @ `48¢` |
| 5 | `BTCUSDT` | `LONG` | `C_BB` | `49.0¢` | `1` | `2.40 / 3.50` | `+2.60` | `41.07` | Clean `C_BB` @ `49¢` |
| 6 | `BTCUSDT` | `LONG` | `C_BB` | **`56.0¢`** | `1` | `2.80 / 3.50` | `+2.20` | `43.18` | Chased Taker @ `56¢` |
| 7 | `ETHUSDT` | `SHORT` | `C_BB` | **`56.0¢`** | `2` | `3.92 / 3.50` | `+1.03` | `44.21` | `C_BB` Step-2 recovery @ `56¢` |
| 8 | `ETHUSDT` | `SHORT` | `B_BACC+C_BB` | **`56.0¢`** | `1` | `2.80 / 3.50` | `+2.20` | `46.33` | `B_BACC+C_BB` @ `56¢` |
| 9 | `ETHUSDT` | `LONG` | `C_BB` | `50.0¢` | `2` | `3.36 / 3.50` | `+1.39` | `47.72` | `C_BB` Step-2 @ `50¢` |
| 10 | `SOLUSDT` | `SHORT` | `C_BB` | `55.0¢` | `1` | `2.75 / 3.50` | `+2.25` | `49.76` | `C_BB` win @ `55¢` |
| 11 | `SOLUSDT` | `LONG` | `A_CPOS` | `54.0¢` | `1` | `2.70 / 3.50` | `+2.30` | `52.06` | `A_CPOS` win |
| 12 | `SOLUSDT` | `LONG` | `A_CPOS+C_BB` | `53.0¢` | `1` | `2.65 / 3.50` | `+2.35` | `54.41` | `A_CPOS+C_BB` win |
| 13 | `ETHUSDT` | `LONG` | `C_BB` | `49.0¢` | `1` | `2.45 / 3.50` | `+2.55` | **`56.96`** | Peak equity `+$16.34U (+40.2%)` |
| 14 | `SOLUSDT` | `LONG` | `A_CPOS+C_BB` | `51.0¢` | Bust | `4.00 / 3.50` | **`-6.45`** | `50.42` | `A_CPOS+C_BB` 2-step bust |
| 15 | `ETHUSDT` | `LONG` | `C_BB` | **`44.0¢`** | Bust | `3.01 / 3.50` | **`-5.80`** | `44.57` | **`44¢` adverse opening basis** bust! |
| 16 | `ETHUSDT` | `LONG` | `C_BB` | **`60.0¢`** | `2` | `4.20 / 3.50` | **`+0.60`** | `50.00` | Chased **`60¢`** (`$4.20U` for `+$0.60U` net!) |
| 17 | `BTCUSDT` | `SHORT` | `A_CPOS+B_BACC+C_BB` | `45.0¢` | `1` | `2.25 / 3.50` | `+2.75` | `52.55` | Win @ `45¢` |
| 18 | `BTCUSDT` | `LONG` | `A_CPOS+C_BB` | **`56.0¢`** | Bust | `3.92 / 3.50` | **`-6.37`** | **`46.09`** | `A_CPOS+C_BB` chased `56¢` ➔ Bust |

#### Smoking-Gun Observations from Real Live Trading:
1. **Branch Split**: `3` of the `4` live busts (`Seq 1, 14, 18`, losing `-$21.66U`) came from **`A_CPOS+C_BB`**. Meanwhile, **`C_BB` / `B_BACC+C_BB` in `48¢–56¢` (`Seqs 3, 4, 5, 6, 7, 8, 9, 10, 13`) went `9-for-9` (`100%` sequence WR)**.
2. **Taker Chasing (`56¢–60¢`) Destroyed Payoff Ratio**: Config allowed `max_maker=0.56`, `max_taker=0.58`, `rescue_max_taker=0.60`. On `Seq 16` (`60¢`), we risked `$4.20U` at Step 2 for just **`+$0.60U` profit**, while 2-step busts lost `-$5.80U ~ -$8.84U`.
3. **Discount `44¢` Trap (`Seq 15`)**: Buying `UP` at `44¢` meant Polymarket's opening Chainlink `TWAP_60s` strike was already significantly above spot — turning a Binance reversal candidate into a `-$5.80U` bust!

### 2.2 Parallel V11 Simulation Today (`2026-11-18`, `data/paper_account.json`)
- **`4W–0L (100%)`**, **`$41.49U -> $50.31U` (`+$8.82U / +21.26%`)**:
  - `Seq 1`: `SOLUSDT LONG` (`A_CPOS+C_BB`) Step 1 @ `50.0¢` -> **`+$2.50U`**
  - `Seq 2`: `SOLUSDT LONG` (`A_CPOS+C_BB`) Step 1 @ `45.0¢` -> **`+$2.75U`**
  - `Seq 3`: `SOLUSDT SHORT` (`A_CPOS+C_BB`) Step 1 @ `48.0¢` -> **`+$2.60U`**
  - `Seq 4`: `BTCUSDT LONG` (`A_CPOS+B_BACC+C_BB`) Step 2 @ `54.0¢` -> **`+$0.97U`**

### 2.3 Historical Archive Comparisons
- **`V10` 1-Shot Sim (`archive_v10_sim_paper_account.json`)**: `14` seqs, `8W/6L` (`57.14%`), `$40.62U -> $44.91U` (`+$4.29U`, max single loss `-$3.00U`). `D_WYCKOFF_2LEG` went `1W/1L`; pure `C_BB` went `4W/2L`.
- **`Pre-V8` 4-Step Martingale (`archive_pre_v8_paper_account_20261113.json`)**: `$85.66U -> $92.33U -> $40.62U` (`4W/1L`). A single 4-step bust on `Seq 4` wiped **`-$47.63U` (`-51.6%` of peak)** in 20 minutes.
- **2-Year Binance Backtest (`v8_validation_report.json`)**: `14,486` seqs (`19.17/day`), Step-1 WR = **`56.87%`** (`Y1 56.98%, Y2 56.76%`), `24/24` profitable months, median final `$755.62U`, but **median max drawdown = `67.58%`** under 4-step Martingale!

---

## Section 3 — 91-Day Official Polymarket `pm_outcome` + Real CLOB Price Audit (`2026-08-14 .. 2026-11-13`)

We ran `edge_validation/audit_v11_full_candle_91d.py` matching every `V11.0` Step-1 signal (`n = 1,732`, `19.03/day`) directly against **official resolved Polymarket Gamma `pm_outcome` (Chainlink 60s TWAP)** and **real historical Polymarket CLOB trades (`pm_trades_agg.parquet`, `n = 1,714` with opening trades)**:

- **IS (First 61 Days)**: `n = 1,258`, Binance 5m WR = `55.88%`, Official PM TWAP60 WR = `51.83%` (`-4.05%` gap), mean `px_0_30 = 0.4937`, 0-fee EV = **`+0.0260/sh`**.
- **OOS (Final 30 Days)**: `n = 474`, Binance 5m WR = `56.33%`, Official PM TWAP60 WR = **`50.42%`** (`-5.91%` gap), mean `px_0_30 = 0.4905`, 0-fee EV = **`+0.0126/sh`** (`+0.0101/sh` at Nov-20 `0.01` taker fee).
- **By Coin (`91d`)**:
  - `BTC` (`n=606`): `56.93%` BN -> `51.16%` PM (`-5.78%` gap), mean `px_0_30 = 0.4956`, 0-fee EV = `+0.0161/sh`.
  - `ETH` (`n=589`): `55.35%` BN -> `51.27%` PM (`-4.07%` gap), mean `px_0_30 = 0.5096`, 0-fee EV = `+0.0058/sh`.
  - `SOL` (`n=537`): `55.68%` BN -> **`51.96%`** PM (`-3.72%` gap), mean `px_0_30 = 0.4715`, 0-fee EV = **`+0.0473/sh`** (`+0.0448/sh` at Nov-20 `0.01` taker fee!).
- **By Tier (`91d`)**: `STANDARD` (`50.89% PM, +0.0103`), **`TIER_S` (`53.18% PM, +0.0347`)**, `TIER_S+` (`51.40% PM, +0.0384`).

### 3.1 Branch IS vs OOS Table (`edge_validation/v11_full_candle_91d_branches.csv`)

| Branch | `n` | Binance WR | PM TWAP60 WR | `IS` PM WR (`n`) | `OOS` PM WR (`n`) | Mean `px_pre30` | Mean `px_0_30` | 0-Fee Maker EV (`px_0_30`) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **`C_BB`** | `511` | `56.56%` | **`55.77%`** | `56.04% (364)` | **`55.10% (147)`** | `0.5370` | `0.5333` | **`+0.0282/sh`** |
| `A_CPOS+C_BB` | `423` | `56.03%` | `50.12%` | `50.33% (304)` | `49.58% (119)` | `0.5003` | `0.4742` | `+0.0246/sh` |
| `A_CPOS+B_BACC+C_BB` | `196` | `57.65%` | `50.00%` | `49.65% (141)` | `50.91% (55)` | `0.5019` | `0.4729` | `+0.0271/sh` |
| **`A_CPOS` (standalone)** | `145` | `57.93%` | **`47.59%`** | `46.79% (109)` | `50.00% (36)` | `0.4996` | `0.4673` | `+0.0050/sh` |
| `A_CPOS+B_BACC+C_BB+E` | `100` | `56.00%` | `52.00%` | `49.32% (73)` | `59.26% (27)` | `0.4781` | `0.4439` | `+0.0765/sh` |
| `B_BACC+C_BB` | `95` | `54.74%` | `51.58%` | `54.67% (75)` | `40.00% (20)` | `0.5283` | `0.5456` | `-0.0298/sh` |
| `A_CPOS+B_BACC` | `71` | `53.52%` | `45.07%` | `46.94% (49)` | `40.91% (22)` | `0.4875` | `0.4121` | `+0.0386/sh` |
| **`D_WYCKOFF_2LEG`** | `65` | `49.23%` | **`41.54%`** | `43.40% (53)` | **`33.33% (12)`** | `0.5034` | `0.4687` | **`-0.0468/sh`** |
| **`B_BACC` (standalone)** | `42` | `64.29%` | **`57.14%`** | `56.67% (30)` | **`58.33% (12)`** | `0.5098` | `0.5131` | **`+0.0583/sh`** |
| `A_CPOS+B_BACC+E_RSI_TERM` | `31` | `48.39%` | `41.94%` | `56.52% (23)` | `0.00% (8)` | `0.4750` | `0.4391` | `-0.0198/sh` |

### 3.2 Discovery A: Why `A_CPOS` Collapses (`-10.34%` Gap) While `C_BB` Holds (`-0.78%` Gap)
On Binance 5m, `A_CPOS` (candle close pinned at the extreme 8% tip) wins `57.93%` vs `Close(t-1)`. On Polymarket, however, the strike is **`TWAP_60s([T-60, T])`**!
- When price flushes rapidly right into `T=0s` (`A_CPOS`), `Close(t-1)` sits at the wick bottom while `TWAP_60s(t-1..t)` remains significantly higher. A small bounce in bar `t` beats `Close(t-1)` on Binance, **yet closes below `TWAP_60s` strike on Polymarket (`57.93% -> 47.59%`, `-10.34%` erosion)**.
- Conversely, `C_BB` (`%B <= 0.12`, mean `px_pre30 = 0.5370`) often has a lower wick / stabilization before `T=0s`, losing only `-0.78%` (`56.56% -> 55.77%`, OOS `55.10%`).
- Pruning just `D_WYCKOFF_2LEG` and `A_CPOS`-without-`C_BB/B_BACC` (`CORE_CBB_BBACC_ALL`, `n=1,516`) lifts 0-fee Maker EV to `+0.0268/sh`; keeping `C_BB`-only (`n=511`) yields **`55.77%` PM WR (`+0.0282/sh`)** without ever triggering `D_WYCKOFF_2LEG` (`-0.0468/sh`)!

### 3.3 Discovery B: The Opening TWAP Basis Paradox (`px_pre30 <= 0.50` vs `px_pre30 > 0.52`)
Here is the most counter-intuitive empirical finding from `edge_validation/v11_full_candle_91d_summary.csv`:

| Price Slice (Causal `px_pre30` & Opening `px_0_30`) | `n` | Binance WR | PM TWAP60 WR | Mean Px | 0-Fee Maker EV | Nov-20 `0.01` Taker EV |
|---|---:|---:|---:|---:|---:|---:|
| **`px_pre30 <= 0.48`** *(causal T=-30..0s)* | `426` | **`57.51%`** | **`41.55%`** | `0.4234` | **`-0.0079`** | **`-0.0103`** |
| **`px_pre30 <= 0.50`** *(causal T=-30..0s)* | `568` | **`56.87%`** | **`43.49%`** | `0.4403` | **`-0.0054`** | **`-0.0079`** |
| **`px_pre30 <= 0.51`** *(causal T=-30..0s)* | `689` | `55.01%` | `42.67%` | `0.4518` | `-0.0251` | `-0.0276` |
| **`px_pre30 > 0.52`** *(causal T=-30..0s)* | `759` | **`55.99%`** | **`58.89%`** | `0.5668` | **`+0.0222`** | **`+0.0197`** |
| `px_0_30 <= 0.51` *(intra 0..30s)* | `923` | `52.11%` | `39.76%` | `0.3908` | `+0.0068` | `+0.0044` |
| `px_0_30 > 0.52` *(intra 0..30s)* | `748` | `60.83%` | `65.78%` | `0.6175` | `+0.0402` | `+0.0379` |

**Why does `px_pre30 <= 0.50` lose on Polymarket even when Binance WR is `56.87%`?!**
Because `T=0s` Polymarket odds are ALREADY reflecting `(Spot - TWAP_60s)` opening basis AND order-flow toxicity!
- When `UP` trades `@ 42¢–48¢` in the final 30s (`px_pre30 <= 0.48`), `Spot` is below `TWAP_60s`, so Polymarket assigns `~42%` fair value — and actual `PM_WR` turns out to be `41.55%` (`-0.0079/sh`), JUST LIKE `Seq 15` (`ETHUSDT LONG @ 44¢ -> -$5.80U` bust) in our real live account!
- Meanwhile, when `px_pre30 > 0.52`, `PM_WR` is **`58.89%`** at mean price `0.5668` (`+0.0222/sh`), which previously lost money at Polymarket's old `0.07` (`1.72¢`) taker fee (`+2.22¢ - 1.72¢ = +0.50¢`, wiped by slippage), **but after Nov 20's fee overhaul (`0.01` taker on ETH/SOL = `0.25¢`) retains `+0.0197/sh` (`+1.97¢/sh`) even via Taker!**

### 3.4 91-Day Chronological `$43U` Bankroll Simulation (Official `pm_outcome` + Real `px_0_30`, `0%` Maker)

| Policy | Step Cap | Seqs | Wins | Busts | Seq WR | End Equity (`$43U` Start) | Net PnL | Max DD |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `ALL_4STEP_UNFILTERED` | `4` | `1,221` | `1,167` | `54` | `95.58%` | `$581.70` | `+$538.70` | **`94.81%`** *(near-ruin!)* |
| `ALL_2STEP_UNFILTERED` | `2` | `1,359` | `1,042` | `317` | `76.67%` | `$236.47` | `+$193.47` | **`80.22%`** |
| `ALL_1SHOT_5SH_UNFILTERED` | `1` | `1,732` | `891` | `841` | `51.44%` | `$229.17` | `+$186.17` | `72.30%` |
| `ALL_1SHOT_5SH_PX<=0.51` | `1` | `288` | `106` | `182` | `36.81%` | `$1.12` | **`-$41.88`** | `98.43%` |
| `ALL_2STEP_PX<=0.51` | `2` | `238` | `164` | `74` | `68.91%` | `$2.10` | **`-$40.90`** | `97.47%` |
| **`C_BB_ONLY_1SHOT_ALLPX`** | `1` | `511` | `285` | `226` | **`55.77%`** | `$106.54` | `+$63.54` | `62.14%` |
| `C_BB_ONLY_1SHOT_PX<=0.52` | `1` | `228` | `99` | `129` | `43.42%` | `$61.88` | `+$18.88` | **`40.03%`** |
| **`C_BB_ONLY_2STEP_ALLPX`** | `2` | `457` | `367` | `90` | **`80.31%`** | `$153.69` | `+$110.69` | `65.75%` |

*(Caveat: above chronological sim used `px_0_30` as fill proxy with `0%` fee, which still mixes in opening-30s price discovery; strict causal `vwap_pre30` or 1s-CLOB simulation is required to validate `V12` without adverse-selection bias!)*

---

## Section 4 — Five Concrete `V12 Full-Candle` Optimization Questions for Claude

1. **Fixing the `-4.56%` TWAP60 Basis Trap & Branch Pruning (`V12` Signal Design)**:
   - Empirically, `D_WYCKOFF_2LEG` (`41.54%` PM WR) and `A_CPOS`-without-`C_BB` (`47.59%` PM WR) fail on Chainlink TWAP60 resolution, while `C_BB` (`55.77%`, OOS `55.10%`) and `B_BACC` (`57.14%`, OOS `58.33%`) hold.
   - Instead of `A_CPOS`, if we measure **direct real-time TWAP60 basis** at `T=0s`: `(Spot(T) - TWAP_60s([T-60, T])) / ATR_5m` (plus wick stabilization so we reject falling knives whose Chainlink strike sits above spot), can we restore Binance-level `56%+` WR on Polymarket without overfitting? How should we prune branches cleanly?

2. **Solving the Opening-Price Paradox (`[min_px, max_px]` & Nov-20 `0.01` Taker vs Maker)**:
   - Naively capping `<= 50¢–51¢` caught falling knives (`px_pre30 <= 0.51` had `-2.51¢/sh` EV), whereas `px_pre30 > 0.52` won `58.89%` (`+2.22¢/sh`), EXCEPT our live runner lost payoff symmetry whenever it chased **`56¢–60¢`** (`Seq 16: +$0.60U vs -$6U`).
   - With Nov-20 cutting `ETH/SOL` taker fee 7× (`0.07 -> 0.01`, only `0.25¢` at 50¢), should `V12` use a **sweet-spot band + EV gate** (e.g., reject `< 47¢` falling-knife basis AND reject `> 55¢` expensive chases, entering `[0.48, 0.55]` via `ETH/SOL` `0.01` Taker/Post-Only-with-Taker-fallback ONLY when `calibrated_p_hat - ask - fee >= min_edge`)? What EV / band structure do you recommend?

3. **Killing Martingale Ruin While Compounding `$43U–$50U`**:
   - 4-step Martingale had **`67.58%` median max DD** (`94.81%` max DD over 91d!). Even 2-step Martingale (`5 -> 7`) had `80.22%` DD uncapped when S2 chased `60¢` at fixed `7` shares (`Seq 16`).
   - Between (a) **Pure 1-Shot Fractional Kelly** (`5` shares min until `~$85U`, then `3–5%` equity per trade, zero Martingale), vs (b) **Price-Aware Strict 2-Step Cap** (Step 2 allowed ONLY for `C_BB`/`TIER_S` when `ask_2 <= 0.53`, dynamically sizing `sh_2 = ceil((cost_1 + min_profit) / (1 - ask_2 - fee))`, hard-banning Steps 3 & 4), which dominates on 30-day growth vs `P(DD > 35%)` for a `$50U` account?

4. **Optimal Entry Clock (`T=-3s` Pre-Boundary vs `T=+1s..+5s` Post-Open)**:
   - Firing at `T=-2s` before `T=0s` cannot guarantee exact `TWAP_60s([T-60, T])` lock, whereas waiting until `T=+1s..+3s` gives the exact Chainlink RTDS 60s opening strike (`r1_twap_parity = 0.9948`) and exact post-reset CLOB book. Since Nov-20 makes `ETH/SOL` taker nearly free (`0.25¢`), is `T=+1s..+3s` strictly better than `T=-2s` pre-boundary?

5. **Strict Causal Validation Gate for `V12` Before Real Money**:
   - Propose an exact pre-registered **IS (`61d`) -> OOS (`30d`) -> Live Paper (`post-Nov-20`, `n >= 100` signals)** test protocol using our existing `83,019`-window `gamma_markets` + `pm_trades_agg` + live `1s` CLOB Top-5 recorder (`live_recorder_42.py`) so we never deploy an overfit full-candle update.
