# Section 4.2 Daily Monitoring Report (Read-Only, Pre-Registered 3 Configs, Bonferroni 98.3% CI)

- Generated UTC: `2026-11-18T16:55:34Z`
- Re-freeze count start boundary: `window_ts >= 1795021200`
- Primary evaluation window boundary: `>= 2026-11-21T00:00:00Z` (`window_ts >= 1795219200`)
- Windows recorded / settled: `315` / `308` | R1 TWAP parity: `0.9934`

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

## Feed Freshness Distribution (`v2_filtered` Window 150–255s)

| Feed | Samples | P50 (s) | P90 (s) | P99 (s) |
|---|---:|---:|---:|---:|
| `cl_twap` | 32094 | 0.0 | 0.0 | 0.0 |
| `cl_spot` | 32094 | 0.0 | 0.0 | 0.0 |
| `bn` | 32094 | 0.0 | 2.0 | 6.0 |
| `book` | 32094 | 0.0 | 0.0 | 0.0 |

## Signal Skip Breakdown (`v2_filtered` Window 150–255s)

| Reason | Count |
|---|---:|
| `WARMUP` | 26803 |
| `Z_LOW` | 3024 |
| `FLOW_BLOCKED` | 910 |
| `ASK_OUT_OF_BAND` | 811 |
| `NO_BOOK` | 487 |
| `STALE_BN` | 54 |

## Depth vs Price Miss Ratio (`+1s` FAK) & Dual-Track Stop-Loss vs Hold-to-Settle

| Scope / Model | Missed Total | `MISSED_DEPTH` | `MISSED_PRICE` | `pnl_with_sl` (Mean/sh) | `pnl_hold_to_settle` (Mean/sh) | SL Triggered (`wrong`) |
|---|---:|---:|---:|---:|---:|---:|
| `all / v2_pure` | 1 | 0 (0.0%) | 1 (100.0%) | 0.21769 | 0.07455 | 1 (0) |
| `all / v2_filtered` | 1 | 0 (0.0%) | 1 (100.0%) | 0.17439 | -0.04031 | 1 (0) |
| `all / v1` | 0 | 0 (0.0%) | 0 (0.0%) | 0.18072 | 0.2017 | 0 (0) |

## Primary Per-Coin (`post_nov21`)

| Model | Coin | Signals | Filled +1s | Miss Rate | Mean Spread t0 | Filled WR | Missed Would-Win | Mean PnL +1s | 98.3% Bonferroni CI |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|

## Post-Reset Pre-Nov21 Baseline

| Model | Coin | Signals | Filled +1s | Miss Rate | Mean Spread t0 | Filled WR | Missed Would-Win | Mean PnL +1s | 98.3% Bonferroni CI |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
