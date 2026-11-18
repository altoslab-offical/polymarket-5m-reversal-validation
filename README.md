# Polymarket 5m Reversal Signal — Phase A Edge Validation

> Full empirical verification of the Polymarket 5-minute crypto Up/Down mean-reversion signal over the pure **60-second Chainlink TWAP** resolution regime (`2026-08-14T00:00:00Z` to `2026-11-18T02:00:00Z`, `27,673` windows × 3 coins = `83,019` rows).

## Verdict: **NO-GO** (`目前資料不支持訊號有優勢`)

Using fixed 1-share taker sizing (`vwap_pre30` fallback `vwap_0_30` → `first_px`) with live-verified fee schedule (`fee = C × 0.07 × p × (1 - p)`):

| Pre-Registered Rule | Threshold | Observed (`OOS`: `2026-10-17 01:20Z` – `2026-11-18 02:00Z`) | Result |
|---|---|---|---|
| Rule 1 | OOS T1 `n >= 300` | `n = 514` | **PASS** |
| Rule 2 | OOS T1 mean PnL 95% Bootstrap CI lower > `0` | Mean `-0.0429` (`[-0.0847, -0.0011]`), WR `47.28%` | **FAIL** |
| Rule 3 | `>= 2/3` coins with T2 mean PnL > `0` | OOS `0/3` (`BTC -0.0859`, `ETH -0.0574`, `SOL -0.0090`), Full `0/3` | **FAIL** |
| Rule 4 | No OOS month mean PnL `< -0.03` | `2026-10` OOS = `-0.0700` (`2026-11` = `-0.0216`) | **FAIL** |

Because Phase A failed (`NO-GO`), **Phase B (`live_windows.csv`) was not started**.

---

## Repository Contents

### Reports & API Notes
- [`summary.md`](summary.md): Full statistical report (**T1–T8**, Wilson 95% CI, 10,000-iter Bootstrap 95% CI, IS/OOS splits, ablation, calibration, loss-streak audit, data quality, pre-registered decision).
- [`api_notes.md`](api_notes.md): Verified Polymarket Gamma / Data API v2 / CLOB / Binance Vision requests, truncated real responses, breaking schema differences (`Data API v1` retired 410 → `/v2/trades?condition=`; `Gamma` requires `closed=any`; `60s TWAP` effective strictly `2026-08-14T00:00:00Z` after 7 days of `30s TWAP`), and full Chainlink rules text.

### Raw Row-Level Dataset (`83,019` rows × `71` cols)
Because raw `windows.csv` is `66 MB` (>50 MB GitHub blob limit), it is provided in all three clean formats:
- **Per-coin CSVs** (exact split of `windows.csv`, ~22–23 MB each):
  - [`windows_BTC.csv`](windows_BTC.csv) (`27,673` rows)
  - [`windows_ETH.csv`](windows_ETH.csv) (`27,673` rows)
  - [`windows_SOL.csv`](windows_SOL.csv) (`27,673` rows)
- **Combined Parquet**: [`windows.parquet`](windows.parquet) (`34.5 MB`, 77-digit token IDs stored as exact UTF-8 strings)
- **Combined Unsplit CSV (`66 MB`)**: Attached in **GitHub Release `v1.0.0`** (`windows.csv` and `windows.csv.zip`)

### Reproducible Scripts (`edge_validation/`)
1. [`edge_validation/fetch_binance.py`](edge_validation/fetch_binance.py): Downloads & caches Binance Spot `1m` + `1s` klines and Futures UM 5m `metrics` OI (`2026-08-07` warmup → `2026-11-18T02:05Z`), normalizes us/ms timestamps & sorts unsorted OI CSVs, computes all Section 2.3/2.4 features with zero look-ahead.
2. [`edge_validation/fetch_polymarket.py`](edge_validation/fetch_polymarket.py): Batch-fetches Gamma metadata, CLOB `batch-prices-history`, and Data API `/v2/trades` fills with SQLite checkpointing.
3. [`edge_validation/build_windows_and_summary.py`](edge_validation/build_windows_and_summary.py): Runs the Section 2.5 shared 3-coin state machine, computes fixed-1-share PnL (`pnl_main`, `pnl_assumed054`, `pnl_vwap_0_60`), and generates `windows.csv` + `summary.md`.
4. [`edge_validation/verify_outputs.py`](edge_validation/verify_outputs.py): Unit tests (Wilson CI, Bootstrap CI, state machine cooldown/bust, fee formula) + post-run output invariants.

## Reproducing Locally

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install httpx numpy pandas pyarrow

python edge_validation/fetch_binance.py
python edge_validation/fetch_polymarket.py --stage all
python edge_validation/build_windows_and_summary.py
python edge_validation/verify_outputs.py
```

Or rebuild `summary.md` directly from the provided `windows_{BTC,ETH,SOL}.csv` / `windows.parquet` without re-downloading APIs.
