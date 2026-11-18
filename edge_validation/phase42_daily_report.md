# Section 4.2 Daily Monitoring Report (Read-Only, Pre-Registered 3 Configs, Bonferroni 98.3% CI)

- Generated UTC: `2026-11-18T09:48:22Z`
- Primary evaluation window boundary: `>= 2026-11-21T00:00:00Z` (`window_ts >= 1795219200`)
- Windows recorded / settled: `57` / `51` | R1 TWAP parity: `1.0`

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
| `pre_nov21` | 14 | 12 | 2 | 0.1429 | 0.9167 | 0.5 | 0.01071 | 0.07139 | `[-0.02335, 0.1305]` |

## Primary Per-Coin (`post_nov21`)

| Model | Coin | Signals | Filled +1s | Miss Rate | Mean Spread t0 | Filled WR | Missed Would-Win | Mean PnL +1s | 98.3% Bonferroni CI |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|

## Pre-Nov21 Baseline (`window_ts < 1795219200`)

| Model | Coin | Signals | Filled +1s | Miss Rate | Mean Spread t0 | Filled WR | Missed Would-Win | Mean PnL +1s | 98.3% Bonferroni CI |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| `v2_pure` | `BTCUSDT` | 5 | 5 | 0.0 | 0.01 | 1.0 | None | 0.14125 | `[0.08331, 0.18619]` |
| `v2_pure` | `ETHUSDT` | 2 | 1 | 0.5 | 0.02 | 1.0 | 1.0 | 0.17852 | `None` |
| `v2_pure` | `SOLUSDT` | 3 | 3 | 0.0 | 0.01667 | 0.6667 | None | -0.11172 | `[-0.8016, 0.27798]` |
| `v2_filtered` | `BTCUSDT` | 4 | 4 | 0.0 | 0.01 | 1.0 | None | 0.15291 | `[0.07053, 0.25293]` |
| `v2_filtered` | `ETHUSDT` | 1 | 1 | 0.0 | 0.02 | 1.0 | None | 0.17852 | `None` |
| `v2_filtered` | `SOLUSDT` | 4 | 3 | 0.25 | 0.02 | 0.6667 | 1.0 | -0.11172 | `[-0.8016, 0.27798]` |
