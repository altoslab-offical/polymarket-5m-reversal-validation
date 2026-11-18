# Polymarket 5m Strategy Research & Empirical Validation Repository

> Complete open-source data, verification scripts, and empirical handoffs for both **Phase A (NO-GO)** and **Phase C / Platform Discovery (Intrawindow Chainlink TWAP Sniper + Nov 20 Fee Overhaul)**.

---

## Start Here (For Claude / Reviewers)

- **Round-3 Main Synthesis & Full Audit Responses (`CLAUDE_HANDOFF.md`)**: [`CLAUDE_HANDOFF.md`](CLAUDE_HANDOFF.md) (`raw`: [CLAUDE_HANDOFF.md](https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/CLAUDE_HANDOFF.md))
- **Section 4.1 Corrected `L1 + L2 + L3` 96-Day Report**: [`edge_validation/summary_41.md`](edge_validation/summary_41.md) + [`edge_validation/deviations.md`](edge_validation/deviations.md) + [`edge_validation/phase41_l2_post5_summary.csv`](edge_validation/phase41_l2_post5_summary.csv) + [`edge_validation/r1_r2_parity_proof.json`](edge_validation/r1_r2_parity_proof.json)
  - Datasets & CSVs: [`edge_validation/phase41_intrawindow.parquet`](edge_validation/phase41_intrawindow.parquet), [`phase41_model_free_summary.csv`](edge_validation/phase41_model_free_summary.csv), [`phase41_with_model_summary.csv`](edge_validation/phase41_with_model_summary.csv), [`phase41_l2_post5_summary.csv`](edge_validation/phase41_l2_post5_summary.csv)
- **Section 4.2 3-Coin Read-Only Recorder (`100/100` audit) & Frozen Bonferroni Config**: [`edge_validation/live_recorder_42.py`](edge_validation/live_recorder_42.py) + [`edge_validation/frozen_config.json`](edge_validation/frozen_config.json) + [`edge_validation/phase42_daily_report.md`](edge_validation/phase42_daily_report.md)
- **TWAP Sniper V2 Reference Note (`live_btc_5m.db` in-sample reference; old `L4` compounding retracted)**: [`edge_validation/strategy_v2_report.md`](edge_validation/strategy_v2_report.md) + [`edge_validation/v2_backtest_trades_clob1s.csv`](edge_validation/v2_backtest_trades_clob1s.csv) + [`edge_validation/backtest_twap_sniper_v2.py`](edge_validation/backtest_twap_sniper_v2.py)
- **Verified Platform Facts & Chainlink Oracle**: [`platform_facts.md`](platform_facts.md) + [`settlement_latency.csv`](settlement_latency.csv) + [`strategy_reference/chainlink_oracle.py`](strategy_reference/chainlink_oracle.py) + [`oracle_sniper.py`](strategy_reference/oracle_sniper.py) + [`oracle_sniper_runner.py`](strategy_reference/oracle_sniper_runner.py)

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
3. **Intrawindow Chainlink TWAP Sniper — Unbiased `L1+L2+L3` & Live 4.2 Status**:
   - After fixing `L1` (`idx_T + tau - 1`), `L2` (`[tau+1, tau+5]` post-trigger fill), and `L3` (`EV >= 3.5c` conditional half-spread), strictly causal 96-day 3-coin OOS (`n=1,897`) converges to **`+0.02988/sh` (`95% CI [+0.02199, +0.03857]`)**, matching independent `1s-CLOB` (`+0.037 ± 0.042` @ `+1s`, `+0.027` @ `+2s`), while Model-Free `[tau+1, tau+5]` remains negative (`-0.003` to `-0.039`).
   - Old `L4` portfolio compounding simulation (`$43 -> $192`) has been retracted. Live trading stays halted (`dry_run`); **`>= 2026-11-21T00:00:00Z`** live 1Hz Top-5 recording (`live_recorder_42.py`, 3 frozen configs, **Bonferroni `98.3% CI > 0` at `n >= 300`**) is the sole adjudication gate.
