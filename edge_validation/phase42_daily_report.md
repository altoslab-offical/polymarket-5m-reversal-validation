# Section 4.2 Daily Monitoring Report (Read-Only, Pre-Registered 3 Configs, Bonferroni 98.3% CI)

- Generated UTC: `2026-11-19T03:09:26Z`
- Re-freeze count start boundary: `window_ts >= 1795057800`
- Primary evaluation window boundary: `>= 2026-11-21T00:00:00Z` (`window_ts >= 1795219200`)
- Windows recorded / settled: `681` / `677` | R1 TWAP parity (all): `0.997` (`654/656`) | post-reset: `None` (`0/0`)
- Reset note: `Pre-Nov21 hardening: O(1) lazy CLOB sort + max_queue=64 + stale-WS auto-reconnect, per-message lag_ms (no @bookTicker/REST cross-stream subtraction), elapsed=0 rollover parity (STALE_BOOK 100%), parity_replay shadow-log isolation, daily TWAP-age PnL & carry-forward monitor, tactical TG cards; zero strategy param changes`
- Cumulative ENTER parity (`parity_replay`): `38` / `38` (**`100.00%`** | sample gate `>= 100`: **PENDING (38/100)**)

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
| `cl_twap` | age (`mono_ns`) | 32982 (45) | 534.51 | 1102.26 | 2828.62 | 24437.31 |
| `cl_spot` | age (`mono_ns`) | 32982 (45) | 536.7 | 1104.98 | 2726.24 | 24427.18 |
| `bn` | age (`mono_ns`) | 33027 (0) | 45.45 | 311.7 | 811.23 | 2737.86 |
| `book` | age (`mono_ns`) | 33020 (7) | 9.91 | 124.02 | 2140.54 | 5186.25 |
| `cl_twap` | lag (`recv_wall - src`) | 33015 (0) | 1343.63 | 1774.76 | 2234.05 | 2895.18 |
| `cl_spot` | lag (`recv_wall - src`) | 33015 (0) | 1298.19 | 1741.8 | 2210.19 | 2916.64 |
| `bn` | lag (`recv_wall - src`) | 33026 (0) | 381.38 | 2034.09 | 5020.84 | 11992.4 |
| `book` | lag (`recv_wall - src`) | 33010 (0) | 87.74 | 5329.49 | 18699.05 | 30950.15 |

## Warmup & Restart Uptime

- All recorded history: `30724s` (**`8.53h`**) warmup across `6` warmup/restart episodes.
- Post-reset (`>= 1795057800`): `0s` (**`0.0h`**) warmup across `0` episodes.

## Signal Skip Breakdown (`v2_filtered` Window 150–255s)

| Reason | Count |
|---|---:|
| `Z_LOW` | 22218 |
| `WARMUP` | 19870 |
| `FLOW_BLOCKED` | 9883 |
| `ASK_OUT_OF_BAND` | 7641 |
| `NO_BOOK` | 6396 |
| `STALE_BN` | 596 |
| `STALE_CL_TWAP` | 443 |
| `NO_STRIKE` | 318 |
| `STALE_BOOK` | 25 |
| `STALE_CL_SPOT` | 10 |
| `DIVERGENCE_HIGH` | 9 |
| `SPREAD_WIDE` | 1 |

## Depth vs Price Miss Ratio (`+1s` FAK) & Dual-Track Stop-Loss vs Hold-to-Settle

| Scope / Model | Missed Total | `MISSED_DEPTH` | `MISSED_PRICE` | `pnl_with_sl` (Mean/sh) | `pnl_hold_to_settle` (Mean/sh) | SL Triggered (`wrong`) |
|---|---:|---:|---:|---:|---:|---:|
| `all / v2_pure` | 14 | 1 (7.1%) | 13 (92.9%) | 0.1774 | 0.11932 | 4 (1) |
| `all / v2_filtered` | 9 | 1 (11.1%) | 8 (88.9%) | 0.21932 | 0.14259 | 2 (0) |
| `all / v1` | 3 | 1 (33.3%) | 2 (66.7%) | 0.16687 | 0.18759 | 0 (0) |

## Chainlink TWAP Age vs Filled PnL (`+1s`, `v2_filtered`)

| Scope | `cl_twap_age_sec` | Filled Trades | Win Rate | Mean PnL +1s | Mean Hold PnL |
|---|---|---:|---:|---:|---:|
| `post_reset` | `< 0.5s` | 0 | None | None | None |
| `post_reset` | `0.5–1.5s` | 0 | None | None | None |
| `post_reset` | `> 1.5s` | 0 | None | None | None |
| `all` | `< 0.5s` | 21 | 1.0 | 0.22209 | 0.13806 |
| `all` | `0.5–1.5s` | 2 | 1.0 | 0.19023 | 0.19023 |
| `all` | `> 1.5s` | 0 | None | None | None |

## Binance 1s Volatility Carry-Forward Ratio (`feed_alive` Monitor, scope=`all_ms`)

_Note: `compute_sigma_from_vol_hist` takes `max(sample_var, ewma_var)`; multi-second cumulative variance stays unbiased under zero-return carry-forward while `sample_var` floor prevents quiet-regime EWMA under-estimation._

| Symbol | Total Sec | Zero-Trade Carry (`bn_trades=0`) | Extended `>5s` Carry | Max Quiet Streak | Status (`<=5%`) |
|---|---:|---:|---:|---:|---|
| `BTCUSDT` | 36328 | 14866 (**40.92%**) | 2545 (7.01%) | 61s | **ALERT (>5%)** |
| `ETHUSDT` | 36328 | 19373 (**53.33%**) | 4170 (11.48%) | 89s | **ALERT (>5%)** |
| `SOLUSDT` | 36328 | 23755 (**65.39%**) | 6742 (18.56%) | 91s | **ALERT (>5%)** |
| `ALL` | 108984 | 57994 (**53.21%**) | 13457 (12.35%) | 91s | **ALERT (>5%)** |

## Live Shadow Parity (`PAPER_40U` Runner vs 4.2 Recorder, overlap=0, overall=None)

| UTC Day | Overlap Coin-Win | Recorder ENTER | Runner ENTER | Union | Matched | Parity | Gate (`>=99%`) |
|---|---:|---:|---:|---:|---:|---:|---|

## R1 TWAP Parity Mismatches (All: 654/656=0.997 | Post-reset: 0/0=None)

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
