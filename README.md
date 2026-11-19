# Polymarket 5m Strategy Research & Empirical Validation Repository

> Complete runnable **5m Late-Window Chainlink TWAP Sniper** codebase + open-source datasets, Phase 4.2 live validation tooling, and empirical handoffs (**100% 5m-only focus**; Phase A pre-window RSI `NO-GO`, 15m BTC track cancelled).

---

## Complete 5m Strategy Package — Architecture & Quick Start

### 1. Directory & Module Map

| Path | Role |
|---|---|
| [`app.py`](app.py) | FastAPI control room (`http://127.0.0.1:8088`), REST APIs (`/api/sniper/status`, `/api/status`, `/api/sniper/trades`, `/api/sniper/enable`), Telegram command integration |
| [`engine/signal_core.py`](engine/signal_core.py) | Single source of truth for 5m TWAP Sniper entry/exit gating, Binance 1s sigma + order-flow microstructure, depth-aware FAK fill simulation (`+1s`/`+2s`), and freshness guards |
| [`engine/oracle_sniper.py`](engine/oracle_sniper.py) | Pure 5m TWAP fair-value estimator, fee curve (`post_nov20`), EV sizing (`risk_fraction`, vault lock, 5-share floor), `TWAP_V2_FILTERED_CONFIG` / `TWAP_V2_PURE_CONFIG` / `DEFAULT_SNIPER_CONFIG` |
| [`engine/oracle_sniper_runner.py`](engine/oracle_sniper_runner.py) | 1 Hz single-slot execution & paper-trading loop, write-ahead intent guard, crash recovery, Gamma settlement, shadow telemetry (`runner_shadow_events.jsonl`) |
| [`engine/oracle_sniper_feeds.py`](engine/oracle_sniper_feeds.py) | Isolated per-symbol Polymarket CLOB WebSocket loops, Binance `aggTrade`/`bookTicker`, Coinbase fallback, REST `/book` fallback |
| [`engine/oracle_sniper_ledger.py`](engine/oracle_sniper_ledger.py) | Trade ledger, daily loss cap (`16%` / `>=$4.5`), consecutive/rolling win-rate pause gates, vault accounting |
| [`engine/chainlink_oracle.py`](engine/chainlink_oracle.py) | Polymarket Chainlink RTDS spot + 60s TWAP WebSocket client |
| [`engine/polymarket_auto_trader.py`](engine/polymarket_auto_trader.py) | Polymarket CLOB L2 execution (`sniper_buy` / `sniper_sell`), deterministic 5m slug discovery, `sniper_live = False` safety gate |
| [`engine/notifier.py`](engine/notifier.py) | Multi-channel alert dispatcher (Telegram via `TELEGRAM_BOT_TOKEN` & `TELEGRAM_CHAT_ID` env vars, Discord, macOS desktop) |
| [`static/index.html`](static/index.html) | Web cockpit dashboard served at `http://127.0.0.1:8088` |
| [`edge_validation/`](edge_validation/) | Phase 4.2 read-only 3-coin 1 Hz recorder ([`live_recorder_42.py`](edge_validation/live_recorder_42.py)), [`frozen_config.json`](edge_validation/frozen_config.json), 24-variant [`exploration_grid.py`](edge_validation/exploration_grid.py), [`run_daily_bundle.py`](edge_validation/run_daily_bundle.py), [`q1_soak_audit.py`](edge_validation/q1_soak_audit.py) |
| [`tests/`](tests/) & `verify_*.py` | Unit, parity, shadow, and end-to-end verification suites |

### 2. Quick Start (Paper / Read-Only Mode by Default)

> **Safety Gate**: `sniper_live` defaults to `False` (`paper_mode = True`). Do **NOT** flip to real-money orders until Phase 4.2 pre-registered gates (`n >= 300`, `98.33%` Bonferroni CI lower bound `> 0`) pass.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# Optional Telegram alerts (sanitized from source; set via environment or data/notify_config.json)
export TELEGRAM_BOT_TOKEN="your_bot_token"
export TELEGRAM_CHAT_ID="your_chat_id"

# Start FastAPI control room + 1 Hz paper sniper
bash run.sh

# Start Section 4.2 read-only 3-coin 1 Hz recorder (BTC/ETH/SOL)
.venv/bin/python -u edge_validation/live_recorder_42.py --db data/validation_42/live_3coin_42.db

# Run Q1 soak health audit & daily Tier-1 + Tier-2 (nice -n 19) bundle
.venv/bin/python edge_validation/q1_soak_audit.py --since-ts 1795078200
.venv/bin/python edge_validation/run_daily_bundle.py --db data/validation_42/live_3coin_42.db

# Run tests & parity verifiers
.venv/bin/pytest -q tests/
.venv/bin/python verify_oracle_sniper.py --fast
```

### 3. Latest Active Research & Monitoring Reports

- **Current 5m Monitoring & `CLAUDE_RESPONSE_10` Handoff**: [`edge_validation/CLAUDE_5M_MONITORING_HANDOFF.md`](edge_validation/CLAUDE_5M_MONITORING_HANDOFF.md)
- **24-Variant Offline Exploration Grid Report**: [`edge_validation/exploration_grid_report.md`](edge_validation/exploration_grid_report.md)
- **Q1 3.46h Live Telemetry & `-4.04U` Forensics**: [`edge_validation/Q1_SOAK_INTERIM_3H.md`](edge_validation/Q1_SOAK_INTERIM_3H.md)

---

## Start Here (Validation History & Audits)

- **🚀 LEAD ARCHITECT SPEC (`V12.0` Full-Candle Multi-Period RSI Strategy): [`V12_RSI_STRATEGY_SPEC.md`](V12_RSI_STRATEGY_SPEC.md)** (`raw`: [V12_RSI_STRATEGY_SPEC.md](https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/V12_RSI_STRATEGY_SPEC.md))
  - Strategy implementation: [`strategy_reference/v12_rsi_strategy.py`](strategy_reference/v12_rsi_strategy.py)
  - 96-Day Official Polymarket + 2-Year + Monte Carlo validation: [`edge_validation/validate_v12_rsi.py`](edge_validation/validate_v12_rsi.py) + [`edge_validation/v12_vs_v11_validation.csv`](edge_validation/v12_vs_v11_validation.csv)
- **Full-Candle V11 Battle Records & 91-Day Polymarket Audit (`FULL_CANDLE_V11_DIAGNOSIS_FOR_CLAUDE.md`)**: [`FULL_CANDLE_V11_DIAGNOSIS_FOR_CLAUDE.md`](FULL_CANDLE_V11_DIAGNOSIS_FOR_CLAUDE.md) (`raw`: [FULL_CANDLE_V11_DIAGNOSIS_FOR_CLAUDE.md](https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/FULL_CANDLE_V11_DIAGNOSIS_FOR_CLAUDE.md))
  - Strategy source: [`strategy_reference/v8_strategy.py`](strategy_reference/v8_strategy.py) + [`strategy_reference/indicators.py`](strategy_reference/indicators.py)
  - 91-day official `pm_outcome` audit script & tables: [`edge_validation/audit_v11_full_candle_91d.py`](edge_validation/audit_v11_full_candle_91d.py) + [`edge_validation/v11_full_candle_91d_summary.csv`](edge_validation/v11_full_candle_91d_summary.csv) + [`edge_validation/v11_full_candle_91d_branches.csv`](edge_validation/v11_full_candle_91d_branches.csv)
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
