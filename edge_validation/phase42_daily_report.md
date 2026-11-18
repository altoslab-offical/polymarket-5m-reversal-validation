# Section 4.2 Daily Monitoring Report (Read-Only, Pre-Registered 3 Configs, Bonferroni 98.3% CI)

- Generated UTC: `2026-11-18T13:51:28Z`
- Primary evaluation window boundary: `>= 2026-11-21T00:00:00Z` (`window_ts >= 1795219200`)
- Windows recorded / settled: `204` / `197` | R1 TWAP parity: `0.9948`

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
| `pre_nov21` | 54 | 48 | 6 | 0.1111 | 0.8958 | 0.8333 | 0.01141 | 0.04759 | `[-0.01436, 0.09493]` |

## Primary Per-Coin (`post_nov21`)

| Model | Coin | Signals | Filled +1s | Miss Rate | Mean Spread t0 | Filled WR | Missed Would-Win | Mean PnL +1s | 98.3% Bonferroni CI |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|

## Pre-Nov21 Baseline (`window_ts < 1795219200`)

| Model | Coin | Signals | Filled +1s | Miss Rate | Mean Spread t0 | Filled WR | Missed Would-Win | Mean PnL +1s | 98.3% Bonferroni CI |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| `v2_pure` | `BTCUSDT` | 11 | 11 | 0.0 | 0.01273 | 0.9091 | None | 0.17558 | `[0.10526, 0.2404]` |
| `v2_pure` | `ETHUSDT` | 8 | 5 | 0.375 | 0.01625 | 1.0 | 0.6667 | 0.23422 | `[0.18448, 0.28931]` |
| `v2_pure` | `SOLUSDT` | 10 | 6 | 0.4 | 0.013 | 0.8333 | 1.0 | 0.05495 | `[-0.44506, 0.25809]` |
| `v2_filtered` | `BTCUSDT` | 8 | 7 | 0.125 | 0.01125 | 1.0 | 1.0 | 0.19755 | `[0.12282, 0.27469]` |
| `v2_filtered` | `ETHUSDT` | 4 | 3 | 0.25 | 0.0125 | 1.0 | 0.0 | 0.19177 | `[0.17852, 0.20834]` |
| `v2_filtered` | `SOLUSDT` | 10 | 6 | 0.4 | 0.015 | 0.8333 | 1.0 | 0.05495 | `[-0.44506, 0.25809]` |
| `v1` | `BTCUSDT` | 2 | 1 | 0.5 | 0.015 | 1.0 | 1.0 | 0.2099 | `None` |
| `v1` | `SOLUSDT` | 1 | 0 | 1.0 | 0.01 | None | 1.0 | None | `None` |
