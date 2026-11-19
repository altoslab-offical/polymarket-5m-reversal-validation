# Section 4.2 Daily Monitoring Report (Read-Only, Pre-Registered 3 Configs, Bonferroni 98.3% CI)

- Generated UTC: `2026-11-19T02:32:59Z`
- Re-freeze count start boundary: `window_ts >= 1795055700`
- Primary evaluation window boundary: `>= 2026-11-21T00:00:00Z` (`window_ts >= 1795219200`)
- Windows recorded / settled: `657` / `652` | R1 TWAP parity (all): `0.9969` (`635/637`) | post-reset: `None` (`0/0`)
- Reset note: `Fix 3 overnight live bugs: sub-second TWAP denom normalization, 1Hz Binance bucket flush + @bookTicker vol survival, full-depth CLOB memory + monotonic ev_ts lag guard; zero strategy param changes`
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
| `cl_twap` | age (`mono_ns`) | 30936 (39) | 525.58 | 1112.84 | 3305.12 | 24437.31 |
| `cl_spot` | age (`mono_ns`) | 30936 (39) | 525.83 | 1113.99 | 3341.5 | 24427.18 |
| `bn` | age (`mono_ns`) | 30975 (0) | 45.63 | 314.22 | 813.01 | 2737.86 |
| `book` | age (`mono_ns`) | 30972 (3) | 9.67 | 112.37 | 1650.79 | 5186.25 |
| `cl_twap` | lag (`recv_wall - src`) | 30969 (0) | 1337.49 | 1764.47 | 2216.96 | 2895.18 |
| `cl_spot` | lag (`recv_wall - src`) | 30969 (0) | 1292.84 | 1733.23 | 2197.68 | 2916.64 |
| `bn` | lag (`recv_wall - src`) | 30974 (0) | 378.31 | 2034.76 | 5065.05 | 11992.4 |
| `book` | lag (`recv_wall - src`) | 30972 (0) | 87.12 | 5345.41 | 18351.55 | 30950.15 |

## Warmup & Restart Uptime

- All recorded history: `28924s` (**`8.03h`**) warmup across `5` warmup/restart episodes.
- Post-reset (`>= 1795055700`): `0s` (**`0.0h`**) warmup across `0` episodes.

## Signal Skip Breakdown (`v2_filtered` Window 150–255s)

| Reason | Count |
|---|---:|
| `Z_LOW` | 22152 |
| `WARMUP` | 17962 |
| `FLOW_BLOCKED` | 9791 |
| `ASK_OUT_OF_BAND` | 7553 |
| `NO_BOOK` | 6315 |
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

## Live Shadow Parity (`PAPER_40U` Runner vs 4.2 Recorder, overlap=0, overall=None)

| UTC Day | Overlap Coin-Win | Recorder ENTER | Runner ENTER | Union | Matched | Parity | Gate (`>=99%`) |
|---|---:|---:|---:|---:|---:|---:|---|

## R1 TWAP Parity Mismatches (All: 635/637=0.9969 | Post-reset: 0/0=None)

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
