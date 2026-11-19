# 探索網格報告（24 組策略變體離線重播 — Tier 2 Exploratory Only）

> **Scope**: `PRE_NOV21_SANITY_ALL_SETTLED (code correctness check only)` | 總已結算視窗 `312`（過濾後 `312` 窗 / `1.09` 天） | Start TS `0`
> ⚠️ **紀律聲明**：本表僅供探索期排序與候選發掘（**不構成判定、不作為直接實盤依據**）。24 組無優勢下，第 1 名純靠運氣的期望上限約 **`1.94 × SE`**（已逐列標註對照）。

## 1. 三組預註冊主設定 100% 一致性與核心零改動證明 (`exploration_grid` vs `live_recorder_42._sim_one_cfg`)

- 凍結檔案指紋（SHA256 前 12 碼）：`signal_core.py=19aa314df48d` | `oracle_sniper.py=fa926ed270b5` | `frozen_config.json=5c066dc175cd`（完全未改）。

| Primary Model | `check_frozen_config_parity` | Row Parity | Matched / Total | Mismatch |
|---|---|---:|---:|---|
| `v2_filtered` | **`True`** | **`100.0%`** | `37/37` | `None` |
| `v2_pure` | **`True`** | **`100.0%`** | `55/55` | `None` |
| `v1` | **`True`** | **`100.0%`** | `6/6` | `None` |

## 2. 24 組變體總表（依每股平均 PnL 排序）

| Rank | Code | Group | Description | Signals | Filled +1s | Trades/d | WR% | Mean PnL/sh | SE | Null Max (1.94×SE) | EV/Stake% | MaxDD% | Paired n | ΔvsBASE | Paired t |
|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | **D2** | D. Z Gate | z >= 1.6 | 13 | 11 | 10.12 | 100.0 | 0.23588 | 0.01308 | 0.02537 | 30.87 | 0.00% | 11 | -0.00199 | -0.213 |
| 2 | **E4** | E. BN Lead | min(|z_CL|, |z_BN_minus_60s_basis|) | 23 | 19 | 17.48 | 100.0 | 0.23011 | 0.01296 | 0.02514 | 30.08 | 0.00% | 19 | 0.00105 | 1.455 |
| 3 | **G3** | G. Subset/Combo | E(3s adverse BN) + F2(TWAP-proj SL) | 23 | 19 | 17.48 | 100.0 | 0.22906 | 0.01285 | 0.02493 | 29.9 | 0.00% | 19 | 0.0 | None |
| 4 | **E1** | E. BN Lead | skip on 2s adverse Binance move | 23 | 18 | 16.56 | 100.0 | 0.22689 | 0.01339 | 0.02598 | 29.55 | 0.00% | 18 | 0.0 | None |
| 5 | **E3** | E. BN Lead | skip on 3s adverse Binance > 0.5 sigma | 30 | 22 | 20.24 | 100.0 | 0.22072 | 0.01405 | 0.02725 | 28.72 | 0.00% | 22 | 0.0 | None |
| 6 | **G4** | G. Subset/Combo | E(3s adverse BN) + BTCUSDT only | 9 | 9 | 8.28 | 100.0 | 0.21948 | 0.02311 | 0.04484 | 28.5 | 0.00% | 9 | 0.0 | None |
| 7 | **G1** | G. Subset/Combo | BTCUSDT only | 14 | 10 | 9.20 | 100.0 | 0.21673 | 0.02085 | 0.04046 | 28.01 | 0.00% | 9 | 0.0 | None |
| 8 | **E2** | E. BN Lead | skip on 5s adverse Binance move | 24 | 19 | 17.48 | 100.0 | 0.21587 | 0.01544 | 0.02994 | 27.97 | 0.00% | 19 | 0.0 | None |
| 9 | **C1** | C. Window | elapsed in [200, 285]s | 34 | 22 | 20.24 | 95.45 | 0.2006 | 0.03556 | 0.06898 | 26.54 | 3.75% | 19 | -0.00054 | -0.111 |
| 10 | **BASE** | BASE | Baseline v2_filtered (frozen control) | 37 | 24 | 22.08 | 95.83 | 0.19002 | 0.03202 | 0.06212 | 24.68 | 3.71% | 24 | 0.0 | 0.0 |
| 11 | **B1** | B. EV Floor | net EV >= 2.0c | 37 | 24 | 22.08 | 95.83 | 0.19002 | 0.03202 | 0.06212 | 24.68 | 3.71% | 24 | 0.0 | None |
| 12 | **B2** | B. EV Floor | net EV >= 5.0c | 37 | 24 | 22.08 | 95.83 | 0.19002 | 0.03202 | 0.06212 | 24.68 | 3.71% | 24 | 0.0 | None |
| 13 | **B3** | B. EV Floor | net EV >= 7.0c | 37 | 24 | 22.08 | 95.83 | 0.19002 | 0.03202 | 0.06212 | 24.68 | 3.71% | 24 | 0.0 | None |
| 14 | **F2** | F. Exit | SL uses TWAP-proj z < -0.35 (rem<=60s) | 37 | 24 | 22.08 | 95.83 | 0.19002 | 0.03202 | 0.06212 | 24.68 | 3.71% | 24 | 0.0 | None |
| 15 | **G2** | G. Subset/Combo | exclude SOLUSDT (BTC+ETH) | 33 | 22 | 20.24 | 95.45 | 0.1898 | 0.03395 | 0.06586 | 24.55 | 3.82% | 22 | -0.00181 | -1.0 |
| 16 | **A1** | A. Price Band | ask in [0.60, 0.82] | 37 | 23 | 21.16 | 95.65 | 0.18771 | 0.0334 | 0.06479 | 24.36 | 3.78% | 23 | 0.00043 | 1.0 |
| 17 | **H2** | H. Regime | skip wide spread (ask-bid > 2c) | 33 | 20 | 18.40 | 95.0 | 0.18678 | 0.03724 | 0.07225 | 24.18 | 3.96% | 20 | -0.0005 | -1.0 |
| 18 | **H1** | H. Regime | skip top 10% 24h causal sigma | 36 | 22 | 20.24 | 95.45 | 0.18293 | 0.03459 | 0.0671 | 23.65 | 3.87% | 22 | 0.0 | None |
| 19 | **F3** | F. Exit | early TP at bid >= 0.97 | 37 | 24 | 22.08 | 95.83 | 0.17694 | 0.03149 | 0.06109 | 22.98 | 3.80% | 24 | -0.01308 | -6.283 |
| 20 | **C2** | C. Window | elapsed in [240, 290]s (final 60s TWAP) | 18 | 12 | 11.04 | 91.67 | 0.16774 | 0.06112 | 0.11857 | 22.06 | 4.43% | 9 | -0.0067 | -0.709 |
| 21 | **D1** | D. Z Gate | z >= 1.0 | 91 | 61 | 56.13 | 86.89 | 0.11289 | 0.03435 | 0.06664 | 14.49 | 8.16% | 22 | 3e-05 | 0.005 |
| 22 | **F1** | F. Exit | hold to settlement (disable SL) | 37 | 24 | 22.08 | 87.5 | 0.10491 | 0.0714 | 0.13852 | 13.62 | 9.36% | 24 | -0.08511 | -1.654 |
| 23 | **A2** | A. Price Band | ask in [0.68, 0.88] | 94 | 75 | 69.01 | 89.33 | 0.09248 | 0.02561 | 0.04968 | 11.04 | 10.37% | 24 | -0.02611 | -2.992 |
| 24 | **A3** | A. Price Band | ask in [0.75, 0.90] | 102 | 87 | 80.05 | 85.06 | 0.03467 | 0.02859 | 0.05546 | 4.03 | 15.09% | 19 | -0.04376 | -3.852 |
| 25 | **A4** | A. Price Band | ask in [0.84, 0.95] (highband) | 111 | 96 | 88.33 | 86.46 | 0.01402 | 0.02624 | 0.0509 | 1.57 | 21.61% | 8 | -0.08757 | -5.929 |

## 3. `E4` vs `BASE` 逐窗差異鑑識表

- 共有 **17 個視窗** 出現 `E4` 與 `BASE` 行為差異（過濾進場、延後觸發秒數或不同成交價）。

| Window TS | Coin | Side | Outcome | BASE El | BASE Status | BASE PnL/sh | E4 El | E4 Status | E4 PnL/sh | ΔPnL (E4-BASE) |
|---:|---|---|---|---:|---|---:|---:|---|---:|---:|
| 1794994800 | `ETHUSDT` | UP | UP | 174 | `FILLED` | 0.1984 | 186 | `FILLED` | 0.20834 | 0.00994 |
| 1795004400 | `ETHUSDT` | UP | DOWN | 223 | `MISSED_PRICE` | None | None | `SKIPPED` | None | None |
| 1795005900 | `BTCUSDT` | UP | UP | 232 | `MISSED_PRICE` | None | None | `SKIPPED` | None | None |
| 1795010100 | `ETHUSDT` | DOWN | DOWN | 164 | `MISSED_PRICE` | None | 171 | `MISSED_PRICE` | None | None |
| 1795012800 | `ETHUSDT` | DOWN | DOWN | 216 | `FILLED` | 0.26803 | None | `SKIPPED` | None | -0.26803 |
| 1795014000 | `SOLUSDT` | UP | DOWN | 248 | `FILLED` | 0.04726 | None | `SKIPPED` | None | -0.04726 |
| 1795015800 | `SOLUSDT` | UP | UP | 236 | `FILLED` | 0.2979 | 233 | `FILLED` | 0.30786 | 0.00996 |
| 1795032000 | `BTCUSDT` | DOWN | DOWN | 229 | `MISSED_PRICE` | None | None | `SKIPPED` | None | None |
| 1795032300 | `ETHUSDT` | UP | DOWN | 197 | `MISSED_PRICE` | None | None | `SKIPPED` | None | None |
| 1795033800 | `ETHUSDT` | DOWN | DOWN | 236 | `MISSED_PRICE` | None | None | `SKIPPED` | None | None |
| 1795035300 | `ETHUSDT` | DOWN | DOWN | 241 | `FILLED` | 0.18846 | None | `SKIPPED` | None | -0.18846 |
| 1795053600 | `ETHUSDT` | DOWN | DOWN | 182 | `FILLED` | 0.18846 | None | `SKIPPED` | None | -0.18846 |
| 1795068600 | `ETHUSDT` | DOWN | UP | 242 | `MISSED_PRICE` | None | None | `SKIPPED` | None | None |
| 1795071900 | `ETHUSDT` | UP | DOWN | 217 | `MISSED_PRICE` | None | None | `SKIPPED` | None | None |
| 1795075500 | `ETHUSDT` | UP | DOWN | 223 | `FILLED` | -0.48384 | None | `SKIPPED` | None | 0.48384 |
| 1795080900 | `BTCUSDT` | UP | DOWN | 251 | `MISSED_PRICE` | None | None | `SKIPPED` | None | None |
| 1795082400 | `ETHUSDT` | UP | UP | 204 | `MISSED_PRICE` | None | None | `SKIPPED` | None | None |

## 4. 需與規劃者（Claude）確認的兩項規格細節

1. **主判定層設定數量（3 組 vs 4 組）**：`EXPLORATION_GRID_PLAN.md` 第 0 節寫主判定層維持 4 組（包含 `v2_bnguard`），但目前 `edge_validation/frozen_config.json` 與 `CLAUDE_RESPONSE_8.md` Q3 凍結且 Bonferroni 計算的分母都是 **3 組**（`v2_pure`、`v2_filtered`、`v1`）。請確認主判定層維持 3 組不變，並將 `v2_bnguard` 類想法留在 `E1–E4` 探索層即可。
2. **`F2`（TWAP 投影停損）與現行 `v2_filtered` 的重疊度**：在現行 `v2_filtered`（`use_twap_projection=True`）下，`estimate_twap_fair_value` 於最後 60 秒（`elapsed >= 240`）原本就會切換為 `mean_twap` 投影公式算 `fv.raw_z_score`，因此 `F2` 在多數時刻會和 `BASE` 行為非常接近（僅在 240s 初期 tick 累積不足 60s 時分母略異；且仍保留 `dir_flow < -0.25` 閘門）。若要真正解放 TWAP 停損差異，下一輪可測試放寬 `dir_flow`。
