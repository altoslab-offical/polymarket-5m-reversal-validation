# Section 4.2 Daily Monitoring Report (Read-Only, Pre-Registered 3 Configs, Bonferroni 98.3% CI)

- Generated UTC: `2026-11-19T08:47:06Z`
- Re-freeze count start boundary: `window_ts >= 1795078200`
- Primary evaluation window boundary: `>= 2026-11-21T00:00:00Z` (`window_ts >= 1795219200`)
- Windows recorded / settled: `879` / `874` | R1 TWAP parity (all): `0.9976` (`827/829`) | post-reset: `None` (`0/0`)
- Reset note: `Q1 transport fix: 3 per-symbol aiohttp CLOB WS (compress=0) + background SQLite writer thread to eliminate 1013 slow consumer; Q2 2-metric live shadow parity & Q3 3 read-only daily diagnostic tables; zero strategy param changes`
- Cumulative ENTER parity (`parity_replay`): `41` / `41` (**`100.00%`** | sample gate `>= 100`: **PENDING (41/100)**)

## Primary Gate (`post_nov21`) — Gate Pending Until `filled_1s >= 300`

| Model | Signals | Filled +1s | Missed | Miss Rate | Filled WR | Missed Would-Win WR | Mean PnL +1s | 95% CI | 98.3% Bonferroni CI | Status |
|---|---:|---:|---:|---:|---:|---:|---:|---|---|---|
| `v2_pure` | 0 | 0 | 0 | None | None | None | None | `None` | `None` | **PENDING (0/300)** |
| `v2_filtered` | 0 | 0 | 0 | None | None | None | None | `None` | `None` | **PENDING (0/300)** |
| `v1` | 0 | 0 | 0 | None | None | None | None | `None` | `None` | **PENDING (0/300)** |

## Shadow Model (`v2_highband_shadow`: [0.84, 0.95] — Excluded from 3-Config Bonferroni Gate & Launch Decision)

| Split | Signals | Filled +1s | Missed | Miss Rate | Filled WR | Missed Would-Win | Mean Spread t0 | Mean PnL +1s | 95% CI (Ref Only) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `post_nov21` | 0 | 0 | 0 | None | None | None | None | None | `None` |
| `pre_nov21` | 0 | 0 | 0 | None | None | None | None | None | `None` |

## Feed Freshness & Upstream Lag in Milliseconds (`v2_filtered` Window 150–255s, scope=`all_ms`)

| Feed | Metric | Samples (Disc) | P50 (ms) | P90 (ms) | P99 (ms) | Max (ms) |
|---|---|---:|---:|---:|---:|---:|
| `cl_twap` | age (`mono_ns`) | 53823 (78) | 570.88 | 1043.16 | 2197.91 | 24594.09 |
| `cl_spot` | age (`mono_ns`) | 53823 (78) | 585.94 | 1055.84 | 2181.81 | 26139.79 |
| `bn` | age (`mono_ns`) | 53901 (0) | 49.99 | 328.72 | 814.81 | 2737.86 |
| `book` | age (`mono_ns`) | 53889 (12) | 9.06 | 154.74 | 3046.13 | 168612.63 |
| `cl_twap` | lag (`recv_wall - src`) | 53883 (0) | 1369.46 | 1788.14 | 2232.94 | 3104.49 |
| `cl_spot` | lag (`recv_wall - src`) | 53883 (0) | 1317.19 | 1755.93 | 2207.52 | 3118.02 |
| `bn` | lag (`recv_wall - src`) | 53888 (0) | 27.59 | 1491.35 | 4373.29 | 11992.4 |
| `book` | lag (`recv_wall - src`) | 53853 (0) | 119.04 | 2496.42 | 16017.94 | 30950.15 |

## Warmup & Restart Uptime

- All recorded history: `33417s` (**`9.28h`**) warmup across `8` warmup/restart episodes.
- Post-reset (`>= 1795078200`): `0s` (**`0.0h`**) warmup across `0` episodes.

## Signal Skip Breakdown (`v2_filtered` Window 150–255s)

| Reason | Count |
|---|---:|
| `Z_LOW` | 29874 |
| `WARMUP` | 22732 |
| `FLOW_BLOCKED` | 13618 |
| `ASK_OUT_OF_BAND` | 10195 |
| `NO_BOOK` | 8583 |
| `NO_STRIKE` | 1257 |
| `STALE_BN` | 596 |
| `STALE_CL_TWAP` | 579 |
| `STALE_BOOK` | 278 |
| `STALE_CL_SPOT` | 10 |
| `DIVERGENCE_HIGH` | 9 |
| `SPREAD_WIDE` | 1 |

## Depth vs Price Miss Ratio (`+1s` FAK) & Dual-Track Stop-Loss vs Hold-to-Settle

| Scope / Model | Missed Total | `MISSED_DEPTH` | `MISSED_PRICE` | `pnl_with_sl` (Mean/sh) | `pnl_hold_to_settle` (Mean/sh) | SL Triggered (`wrong`) |
|---|---:|---:|---:|---:|---:|---:|
| `all / v2_pure` | 19 | 2 (10.5%) | 17 (89.5%) | 0.14026 | 0.09862 | 6 (1) |
| `all / v2_filtered` | 11 | 1 (9.1%) | 10 (90.9%) | 0.19002 | 0.10491 | 3 (0) |
| `all / v1` | 3 | 1 (33.3%) | 2 (66.7%) | 0.16687 | 0.18759 | 0 (0) |

## Q3 Read-Only Diagnostic Breakdowns (`v2_filtered`, +1s Filled, Excluded from Bonferroni Gate)

_Two-tier launch rule: Tier 1 `n>=300` & `98.3% CI_lo > 0`; Tier 2 100-trade 5-share pilot `EV/stake >= +8%` & standalone `BTC PnL > 0`._

### 3.1 Entry Trajectory (`prev_5s_max_ask > 0.825` vs Other)

| Scope | Bucket | Trades | Win Rate | Mean PnL +1s | Mean Hold PnL |
|---|---|---:|---:|---:|---:|
| `post_reset` | `From >0.825` | 0 | None | None | None |
| `post_reset` | `Other` | 0 | None | None | None |
| `all` | `From >0.825` | 6 | 0.8333 | 0.07377 | 0.02744 |
| `all` | `Other` | 18 | 1.0 | 0.22877 | 0.13073 |

### 3.2 Pre-Entry 3s Binance Direction

| Scope | Binance 3s | Trades | Win Rate | Mean PnL +1s | Mean Hold PnL |
|---|---|---:|---:|---:|---:|
| `post_reset` | `Aligned/Flat (>=0)` | 0 | None | None | None |
| `post_reset` | `Adverse (<0)` | 0 | None | None | None |
| `all` | `Aligned/Flat (>=0)` | 18 | 1.0 | 0.23635 | 0.23635 |
| `all` | `Adverse (<0)` | 6 | 0.8333 | 0.05105 | -0.2894 |

### 3.3 Per-Coin Diagnostic (`v2_filtered`, Pre-11/21 Baseline + Scaling Check)

| Scope | Coin | Signals | Filled +1s | Win Rate | Mean PnL +1s | Mean Hold PnL | EV / Stake % |
|---|---|---:|---:|---:|---:|---:|---:|
| `post_reset` | `BTCUSDT` | 0 | 0 | None | None | None | None |
| `post_reset` | `ETHUSDT` | 0 | 0 | None | None | None | None |
| `post_reset` | `SOLUSDT` | 0 | 0 | None | None | None | None |
| `all` | `BTCUSDT` | 11 | 9 | 1.0 | 0.21948 | 0.11882 | 28.5 |
| `all` | `ETHUSDT` | 19 | 12 | 0.9167 | 0.16765 | 0.14448 | 21.71 |
| `all` | `SOLUSDT` | 5 | 3 | 1.0 | 0.19113 | -0.09514 | 25.09 |

## Chainlink TWAP Age vs Filled PnL (`+1s`, `v2_filtered`)

| Scope | `cl_twap_age_sec` | Filled Trades | Win Rate | Mean PnL +1s | Mean Hold PnL |
|---|---|---:|---:|---:|---:|
| `post_reset` | `< 0.5s` | 0 | None | None | None |
| `post_reset` | `0.5–1.5s` | 0 | None | None | None |
| `post_reset` | `> 1.5s` | 0 | None | None | None |
| `all` | `< 0.5s` | 22 | 0.9545 | 0.19 | 0.09715 |
| `all` | `0.5–1.5s` | 2 | 1.0 | 0.19023 | 0.19023 |
| `all` | `> 1.5s` | 0 | None | None | None |

## Binance 1s Kline Integrity & Zero-Trade Parity (scope=`post_reset`, align=`ts_sec-1`)

_Replaces the revoked `>5%` zero-trade carry-forward alert (natural zero-trade 1s seconds ~14–44% match historical 1s klines). Official gates: Close `<=2bps` parity `>= 99.0%` (`FAIL_CLOSE` below) and `|Rec Zero - Kline Zero| <= 5.0 pp` (`ALERT_ZERO` above)._

| UTC Day | Symbol | Source | Matched Sec | Close `<=2bps` (`>=99%`) | Exact % | P99 / Max (bps) | Rec Zero % | Kline Zero % | Delta (`±5pp`) | Max Quiet | Status |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `PENDING` | `ALL` | `PENDING` | 0 | **n/a** | n/a | n/a / n/a | n/a | n/a | n/a | 0s | **PENDING** |

## Live Shadow Parity (`PAPER_40U` Runner vs 4.2 Recorder, `±2s` same-dir, overlap=0, runner_enters=0, parity=None, status=**PENDING (<20)**)

- Gate: `>= 95.0%` once Runner `ENTER >= 20` (pre-live check). Auxiliary diffs over matched: `|Δask|` P50/P90=`None`/`None`, `|Δz|` P50/P90=`None`/`None`.
- Mismatch attribution counts: `{'CLOB_STALL': 0, 'PHASE_1HZ': 0, 'OTHER': 0}`

| UTC Day | Overlap Coin-Win | Rec ENTER | Run ENTER | Union | Matched (`±2s`) | Parity | `|Δask|` P50/P90 | `|Δz|` P50/P90 | Gate (`>=95% @ >=20`) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|

## R1 TWAP Parity Mismatches (All: 827/829=0.9976 | Post-reset: 0/0=None)

| Window TS | Symbol | Slug | Program `twap_strike` | Program `twap_close` | Gamma `priceToBeat` | Gamma `finalPrice` | Gamma Outcome | Pred |
|---:|---|---|---:|---:|---:|---:|---|---|
| 1795004400 | `SOLUSDT` | `sol-updown-5m-1795004100` | 74.589984 | 74.599446 | 74.59169666579577 | 74.58917447538633 | `DOWN` | `UP` |
| 1795014000 | `SOLUSDT` | `sol-updown-5m-1795013700` | 74.015778 | 74.017976 | 74.01577803877893 | 74.00799576573691 | `DOWN` | `UP` |

## Primary Per-Coin (`post_nov21`)

| Model | Coin | Signals | Filled +1s | Missed | Miss Rate | Mean Spread t0 | Filled WR | Missed Would-Win | Mean PnL +1s | 98.3% Bonferroni CI |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|

## Post-Reset Pre-Nov21 Baseline

| Model | Coin | Signals | Filled +1s | Missed | Miss Rate | Mean Spread t0 | Filled WR | Missed Would-Win | Mean PnL +1s | 98.3% Bonferroni CI |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
