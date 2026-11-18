# Polymarket 5m Strategy Research & Empirical Validation Repository

> Complete open-source data, verification scripts, and empirical handoffs for both **Phase A (NO-GO)** and **Phase C / Platform Discovery (Intrawindow Chainlink TWAP Sniper + Nov 20 Fee Overhaul)**.

---

## Start Here (For Claude / Reviewers)

- **Main Synthesis & R1–R8 / Section 4.1 / Section 4.2 Handoff**: [`CLAUDE_HANDOFF.md`](CLAUDE_HANDOFF.md) (`raw`: [CLAUDE_HANDOFF.md](https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/CLAUDE_HANDOFF.md))
- **Section 4.1 96-Day Verification (`82,944` settled markets / `414,720` snapshots + zero-lag audit)**: [`edge_validation/summary_41.md`](edge_validation/summary_41.md) + [`edge_validation/deviations.md`](edge_validation/deviations.md) + [`edge_validation/r1_r2_parity_proof.json`](edge_validation/r1_r2_parity_proof.json)
  - Raw Section 4.1 dataset (`25 MB`): [`edge_validation/phase41_intrawindow.parquet`](edge_validation/phase41_intrawindow.parquet) + CSV summaries [`phase41_model_free_summary.csv`](edge_validation/phase41_model_free_summary.csv) & [`phase41_with_model_summary.csv`](edge_validation/phase41_with_model_summary.csv)
- **Section 4.2 3-Coin Read-Only Recorder & Simulator**: [`edge_validation/live_recorder_42.py`](edge_validation/live_recorder_42.py) + [`edge_validation/frozen_config.json`](edge_validation/frozen_config.json) + [`strategy_reference/chainlink_oracle.py`](strategy_reference/chainlink_oracle.py) & [`oracle_sniper_runner.py`](strategy_reference/oracle_sniper_runner.py)
- **Verified Platform Facts (102 markets, Nov 20 fee overhaul, RTT, SDK)**: [`platform_facts.md`](platform_facts.md) + [`settlement_latency.csv`](settlement_latency.csv)
- **Maker vs Intrawindow Sniper 1s-CLOB Comparison + 96d 3-Coin Audit + Monte Carlo**: [`edge_validation/strategy_comparison_report.md`](edge_validation/strategy_comparison_report.md)
  - Trade-level CSVs from `live_btc_5m.db` (`1,004` windows / `84.33h`): [`edge_validation/sniper_sim_btc.csv`](edge_validation/sniper_sim_btc.csv) & [`edge_validation/maker_sim_btc.csv`](edge_validation/maker_sim_btc.csv)
  - Sniper pure strategy/pricing code: [`strategy_reference/oracle_sniper.py`](strategy_reference/oracle_sniper.py)
  - Full `1,013`-window `1s` SQLite snapshot database (`121 MB`): hosted in [`altoslab-offical/polymarket-btc-5m` Release v1.0.0](https://github.com/altoslab-offical/polymarket-btc-5m/releases/tag/v1.0.0)

---

## Phase A Summary (`T=1786665600..1794967200`, `83,019` rows): **NO-GO**

Pre-window RSI(7) + microstructure reversal with fixed 1-share taker sizing (`vwap_pre30` fallback `vwap_0_30` → `first_px`) + `0.07` fee failed pre-registered OOS gates (`OOS n=514`, WR `47.28%`, PnL `-0.0429 [-0.0847, -0.0011]`, `0/3` coins positive).

- [`summary.md`](summary.md): Full T1–T8 tables & GO/NO-GO evaluation.
- [`api_notes.md`](api_notes.md): Verified Polymarket Gamma / Data API v2 / CLOB / Binance Vision specs & exact `60s TWAP` boundary (`2026-08-14T00:00:00Z`).
- Raw datasets (`83,019` rows × `71` cols): [`windows_BTC.csv`](windows_BTC.csv), [`windows_ETH.csv`](windows_ETH.csv), [`windows_SOL.csv`](windows_SOL.csv), [`windows.parquet`](windows.parquet), and unsplit [`windows.csv` in Release v1.0.0](https://github.com/altoslab-offical/polymarket-5m-reversal-validation/releases/tag/v1.0.0).

---

## Key Empirical Findings for Next Phase (Phase C Focus)

1. **Nov 20, 2026T23:59Z Fee & Rebate Overhaul (`fee-and-rebate-changes-november-2026.md`)**:
   - **BTC 5m/15m** taker `feeRate`: `0.07 → 0.05`.
   - **ETH & SOL 5m** taker `feeRate`: `0.07 → 0.01` (**85.7% reduction**).
   - **Maker Rebates** move to 30d weighted volume tiers; **accounts `< $200,000` receive `0%`**. Liquidity rewards (`rewardsMinSize=50`, `$1` min payout) yield `$0` for 5-share accounts.
2. **Passive Maker Fails Empirically on 1,004 Real 1s-CLOB Windows (`live_btc_5m.db`)**:
   - `T=0s` contrarian `48¢`/`45¢` Maker (`0%` fee): `-0.0566` (`42.34%` WR) / `-0.0564` (`39.36%` WR).
   - Stepping back `1¢` on valid Sniper signals (`Maker-A3`): loses 40% of fills (`75 → 45`) and turns OOS negative (`-0.0131`, `78.57%` WR).
3. **Intrawindow Chainlink TWAP Sniper + 44s Early Exit Works & Scales with Nov 20 Fees**:
   - Taker Sniper (`n=75` BTC Full, `21.3/day`): **`90.67%` WR**, **`+0.0884`** (`0.07` fee) / **`+0.0920`** (`0.05`) / **`+0.0990`** (`0.01`). Pure OOS 40% (`n=23`): **`86.96%` WR**, **`+0.0528`** blended Nov 20 PnL.
   - `68/75 (90.7%)` exit early at `bid >= 0.98` in **median `44s`** (and on-chain resolution takes median **`20s`** after `T+300` across `102` markets).
   - 96-day (`82,944` settled markets) cross-coin check at `tau=180s`, `|Z|>=1.30`: BTC `96.46%`, ETH `96.37%`, SOL `96.60%` (`1.56×` non-overlapping 3-coin factor).
   - Realistic OOS Monte Carlo from `$42.53U` (`26/day`, including `-1.5¢/sh` slippage stress): **`8–11 days` to `$100U`**, **`22–31 days` to `$200U`**. Next step: record 7–14 days of live BTC/ETH/SOL post-11/20 orderbooks (`dry_run` only) to confirm ETH/SOL OOS `n >= 300`.
