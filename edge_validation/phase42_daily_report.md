# Section 4.2 Daily Monitoring Report (Read-Only, Pre-Registered 3 Configs, Bonferroni 98.3% CI)

- Generated UTC: `2026-11-18T18:21:54Z`
- Re-freeze count start boundary: `window_ts >= 1795021200`
- Primary evaluation window boundary: `>= 2026-11-21T00:00:00Z` (`window_ts >= 1795219200`)
- Windows recorded / settled: `366` / `360` | R1 TWAP parity (all): `0.9943` (`349/351`) | post-reset: `1.0` (`39/39`)

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
| `pre_nov21` | 4 | 1 | 3 | 0.75 | 1.0 | 1.0 | 0.0125 | 0.12887 | `None` |

## Feed Freshness & Upstream Lag in Milliseconds (`v2_filtered` Window 150–255s, scope=`fallback_sec_to_ms`)

| Feed | Metric | Samples (Disc) | P50 (ms) | P90 (ms) | P99 (ms) | Max (ms) |
|---|---|---:|---:|---:|---:|---:|
| `cl_twap` | age (`mono_ns`) | 37467 (0) | 0.0 | 1690.0 | 2710.0 | 25790.0 |
| `cl_spot` | age (`mono_ns`) | 37467 (0) | 0.0 | 1660.0 | 2690.0 | 25790.0 |
| `bn` | age (`mono_ns`) | 37494 (0) | 0.0 | 2000.0 | 6000.0 | 17500.0 |
| `book` | age (`mono_ns`) | 37494 (0) | 0.0 | 3.0 | 266.14 | 8530.0 |
| `cl_twap` | lag (`recv_wall - src`) | 0 (0) | None | None | None | None |
| `cl_spot` | lag (`recv_wall - src`) | 0 (0) | None | None | None | None |
| `bn` | lag (`recv_wall - src`) | 0 (0) | None | None | None | None |
| `book` | lag (`recv_wall - src`) | 0 (0) | None | None | None | None |

## Warmup & Restart Uptime

- All recorded history: `33231s` (**`9.23h`**) warmup across `5` warmup/restart episodes.
- Post-reset (`>= 1795021200`): `4497s` (**`1.25h`**) warmup across `1` episodes.

## Signal Skip Breakdown (`v2_filtered` Window 150–255s)

| Reason | Count |
|---|---:|
| `WARMUP` | 30782 |
| `Z_LOW` | 3630 |
| `FLOW_BLOCKED` | 1261 |
| `ASK_OUT_OF_BAND` | 995 |
| `NO_BOOK` | 671 |
| `STALE_CL_TWAP` | 77 |
| `STALE_BN` | 62 |
| `STALE_CL_SPOT` | 6 |
| `STALE_BOOK` | 5 |

## Depth vs Price Miss Ratio (`+1s` FAK) & Dual-Track Stop-Loss vs Hold-to-Settle

| Scope / Model | Missed Total | `MISSED_DEPTH` | `MISSED_PRICE` | `pnl_with_sl` (Mean/sh) | `pnl_hold_to_settle` (Mean/sh) | SL Triggered (`wrong`) |
|---|---:|---:|---:|---:|---:|---:|
| `all / v2_pure` | 1 | 0 (0.0%) | 1 (100.0%) | 0.21769 | 0.07455 | 1 (0) |
| `all / v2_filtered` | 1 | 0 (0.0%) | 1 (100.0%) | 0.17439 | -0.04031 | 1 (0) |
| `all / v1` | 0 | 0 (0.0%) | 0 (0.0%) | 0.18072 | 0.2017 | 0 (0) |

## Live Shadow Parity (`PAPER_40U` Runner vs 4.2 Recorder, overlap=11, overall=None)

| UTC Day | Overlap Coin-Win | Recorder ENTER | Runner ENTER | Union | Matched | Parity | Gate (`>=99%`) |
|---|---:|---:|---:|---:|---:|---:|---|
| `2026-11-18` | 11 | 0 | 0 | 0 | 0 | None | **PENDING** |

## R1 TWAP Parity Mismatches (All: 349/351=0.9943 | Post-reset: 39/39=1.0)

| Window TS | Symbol | Slug | Program `twap_strike` | Program `twap_close` | Gamma `priceToBeat` | Gamma `finalPrice` | Gamma Outcome | Pred |
|---:|---|---|---:|---:|---:|---:|---|---|
| 1795004400 | `SOLUSDT` | `sol-updown-5m-1795004100` | 74.589984 | 74.599446 | 74.59169666579577 | 74.58917447538633 | `DOWN` | `UP` |
| 1795014000 | `SOLUSDT` | `sol-updown-5m-1795013700` | 74.015778 | 74.017976 | 74.01577803877893 | 74.00799576573691 | `DOWN` | `UP` |

## Primary Per-Coin (`post_nov21`)

| Model | Coin | Signals | Filled +1s | Miss Rate | Mean Spread t0 | Filled WR | Missed Would-Win | Mean PnL +1s | 98.3% Bonferroni CI |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|

## Post-Reset Pre-Nov21 Baseline

| Model | Coin | Signals | Filled +1s | Miss Rate | Mean Spread t0 | Filled WR | Missed Would-Win | Mean PnL +1s | 98.3% Bonferroni CI |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
