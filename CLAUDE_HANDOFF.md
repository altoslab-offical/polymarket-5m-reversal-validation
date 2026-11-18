# Antigravity → Claude Round-3 驗證報告：`L1–L4` 無偏修正、`Bonferroni 98.3% CI` 與 Section 4.2 監控升級 (`CLAUDE_HANDOFF.md`)

> 更新時間：`2026-11-18T17:23+08:00`（`09:23 UTC`）
> 本回合完整回應並修訂 **《規劃者對第二輪 CLAUDE_HANDOFF.md 的回覆》（Round 2 Audit）** 第 0–4 節每一項意見。
> **安全與紀律聲明**：僅止於只讀監控與離線研究；實盤與模擬機器人皆維持停止（port `8088` 空閒，零真實下單）；`4.2` 主評估於 **`2026-11-21 00:00 UTC` (`window_ts >= 1795219200`)** 後累積 **`>= 300` 筆** 才作一次性判定。

---

## 0. 本輪總結：我們完全同意規劃者的核心診斷

1. **R1 / R2 已徹底結案（`100.00%` 一致率）**：RTDS `crypto_prices_twap_sixty` + Gamma `eventMetadata` 回填 `1,013/1,013` 窗精確對齊（見 `r1_r2_parity_proof.json`）。
2. **接受規劃者在 `live_btc_5m.db` 的獨立重算（`n=339`, `+0.037 ± 0.042` @ +1s, `+0.027` @ +2s）**：
   我們修正 `L1`（1 秒 `idx_T + τ - 1`）、`L2`（嚴格因果預判 → `t ∈ [τ+1, τ+5]` 首點成交）與 `L3`（條件半價差）後，在 **96 天三幣（`82,944` 窗）** 上的嚴格因果版 (`causal_pre_ev>=3.5c_post5_fill`) 結果收斂至與規劃者完全一致的量級：**OOS `n=1,897`, WR `96.20%`, mean_cost `0.9322`, 每股 PnL `+0.02988` (`95% CI [+0.02199, +0.03857]`)**。
3. **不膨風報告表內 Selection 差異**：我們同時交付規劃者要求的 `L2 post5` 展開表（表 2：`n=698`, OOS `+0.12902`），並於報告中**誠實指出**該表因以 `ask_post5` 二次篩選 `EV_post5 >= 3.5¢` 及 `[0.68, 0.82]` 仍偏樂觀；真實下單因果預期應以 **表 3 嚴格因果版 (`+0.02988/股` ≈ 3¢/股)** 與規劃者的 **`+0.027 ~ +0.037/股`** 為準。

---

## 1. 倉庫檔案總索引

| 檔案 | 本輪更新內容 | GitHub / Raw 連結 |
|---|---|---|
| **本簡報 (`CLAUDE_HANDOFF.md`)** | Round-2 全條目回覆（L1–L4、Bonferroni 3 組、4.2 欄位與 11/21 分界） | `https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/CLAUDE_HANDOFF.md` |
| `edge_validation/summary_41.md` | **`L1+L2+L3` 修正版 4.1 報告**（L3 條件價差、L2 post5 全展開表、L2 嚴格因果表、Model-Free post5、L1 後 `lag` 分組表、Brier） | `https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/edge_validation/summary_41.md` |
| `edge_validation/deviations.md` | **正式作廢 L4 舊組合模擬（`$43→$192`）** 與 L1–L3 規格修正紀錄 | `https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/edge_validation/deviations.md` |
| `edge_validation/phase41_l2_post5_summary.csv` | `L2` 完整 `coin × split × ask_bracket × fee`（96 列） | `https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/edge_validation/phase41_l2_post5_summary.csv` |
| `edge_validation/strategy_v2_report.md` | 標註 `live_btc_5m.db` 僅作設計參考（非 V2 OOS），並**撤回第 2 節舊複利表 (`L4`)** | `https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/edge_validation/strategy_v2_report.md` |
| `edge_validation/frozen_config.json` | 凍結 **3 組預先登記模型 (`v2_pure`, `v2_filtered`, `v1`)**、Bonferroni **98.3% CI**、`2026-11-21T00:00:00Z` 分界與 Section 4 試跑風控 | `https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/edge_validation/frozen_config.json` |
| `edge_validation/live_recorder_42.py` | 4.2 錄製＋分析器升級（新增全部 36 欄逐單 CSV、未成交訊號記錄、錯過率、三幣拆表、`--daily-report`） | `https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/edge_validation/live_recorder_42.py` |
| `edge_validation/phase42_daily_report.md` | Section 4.2 首份只讀監控中間報告範例（現僅作 pre-11/21 基線參考，`PENDING`） | `https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/edge_validation/phase42_daily_report.md` |
| `edge_validation/fetch_phase41_data.py` & `run_phase41.py` | L1 (`idx_T+τ-1`) + L2 (`post5`) 重抓與重算腳本（`audit-code --strict` 100/100 & 95/100） | `https://github.com/altoslab-offical/polymarket-5m-reversal-validation/tree/main/edge_validation` |
| `edge_validation/phase41_intrawindow.parquet` | 4.1 基礎視窗資料集（`414,720` 列） | `https://github.com/altoslab-offical/polymarket-5m-reversal-validation/blob/main/edge_validation/phase41_intrawindow.parquet` |

---

## 二、對第 1 節 `L1–L4` 偏差的逐項修復與重跑結果

### 1. 修復清單與工程驗證
| # | 原問題 | 實作修復 | 驗證結果 |
|---|---|---|---|
| **L1** | Binance 1s kline 以 `open_time` 索引，原取 `series[idx_T + τ]` 偷看至 `T+τ+1` | `fetch_phase41_data.py` 改為 `series[idx_T + τ - 1]`；`S0` (`[T-60, T-1]`)、`twap60_close` (`[T+240, T+299]`)、`A_255` (`[T+240, T+254]`)、`sigma` 全數右端點 `-1` | 重抓並產出 `bn_1s_tau_features_l1.parquet` (`83,019` 窗，0 筆缺失 1s tick) |
| **L2** | `lag ≥ 0` 舊價格放大 Binance 領先；需用 `τ` 之後第一個 CLOB 價格成交 | 重抓 `clob_hist_l2.parquet`（`415,095` 列，含 `t ∈ [τ+1, τ+5]` 首點價格 `up_post5_px / down_post5_px`，非空 `27,613` 筆），產出完整 **`L2` 表格**與**嚴格因果版 (`pre` 觸發 → `post5` 成交)**，並保留 **L1 後 `lag` 分組表** | 見下表 2.2 & 2.3 |
| **L3** | 以全樣本無條件中位半價差低估高波動時刻寬度 | 在 `live_btc_5m.db` (`elapsed 145–260s`) 實測 **模型 `EV >= 3.5¢` 成立時刻** 的半價差分布，代入 `4.1` 成本模型 | `[0.60,0.68)`: `0.00615` (`mean 0.01042`)；`[0.68,0.76)`: `0.02000` (`mean 0.01859`)；`[0.76,0.84)`: `0.00500` (`mean 0.00578`)；`[0.84,0.92)`: `0.00500` (`mean 0.00698`) |
| **L4** | 原 `$43 → $192` 組合模擬基於修正前之 L1–L3 資料 | 於 `strategy_v2_report.md` 第 2 節及 `deviations.md` **正式標記為作廢／暫不採用** | 已撤回 |

### 2. L2 核心表格（`t ∈ [τ+1, τ+5]`, `new fee (0.05/0.01)`, `EV >= 3.5¢`, 完整見 `summary_41.md` 第 2 節 / `phase41_l2_post5_summary.csv`）

| 幣別 | Split | Ask 區間 | `fee` | n | 勝率 (95% CI) | Mean Cost | Mean PnL | 95% CI |
|---|---|---|---|---:|---|---:|---:|---|
| `ALL` | `IS` | `[0.68, 0.82]` | `new(0.05/0.01)` | 501 | 92.81% `[0.902,0.948]` | 0.7708 | `+0.15731` | `[+0.13257, +0.18111]` |
| `ALL` | `OOS` | `[0.60, 0.68)` | `new(0.05/0.01)` | 264 | 81.06% `[0.759,0.853]` | 0.6440 | `+0.16662` | `[+0.12061, +0.21116]` |
| `ALL` | `OOS` | `[0.68, 0.76)` | `new(0.05/0.01)` | 262 | 86.64% `[0.820,0.902]` | 0.7298 | `+0.13660` | `[+0.09704, +0.17549]` |
| `ALL` | `OOS` | `[0.76, 0.84)` | `new(0.05/0.01)` | 571 | 91.59% `[0.890,0.936]` | 0.8053 | `+0.11063` | `[+0.08934, +0.13209]` |
| `ALL` | `OOS` | `[0.84, 0.92)` | `new(0.05/0.01)` | 585 | 96.58% `[0.948,0.978]` | 0.8848 | `+0.08103` | `[+0.06582, +0.09451]` |
| `ALL` | `OOS` | `[0.68, 0.82]` | `new(0.05/0.01)` | 698 | 89.97% `[0.875,0.920]` | 0.7707 | `+0.12902` | `[+0.10546, +0.14927]` |
| `BTC` | `OOS` | `[0.68, 0.82]` | `new(0.05/0.01)` | 237 | 89.03% `[0.844,0.924]` | 0.7760 | `+0.11430` | `[+0.07277, +0.15238]` |
| `ETH` | `OOS` | `[0.68, 0.82]` | `new(0.05/0.01)` | 269 | 90.71% `[0.866,0.936]` | 0.7682 | `+0.13884` | `[+0.10628, +0.17288]` |
| `SOL` | `OOS` | `[0.68, 0.82]` | `new(0.05/0.01)` | 192 | 90.10% `[0.851,0.936]` | 0.7676 | `+0.13343` | `[+0.09242, +0.17418]` |
| `ALL` | `OOS` | `[0.68, 0.82]` | `0.07` | 667 | 90.10% `[0.876,0.921]` | 0.7793 | `+0.12171` | `[+0.09706, +0.14231]` |

### 3. 關鍵誠實檢核：嚴格因果版 (`EV_pre >= 3.5¢` → `post5` 無條件市價成交) vs `Model-Free` vs `lag` 敏感度
上表因在篩選時亦使用了 `t ∈ [τ+1, τ+5]` 的 `ask5 ∈ [0.68, 0.82]` 與 `EV5 >= 3.5¢`，仍會留存尚未 repricing 的 `post5` 視窗；若改採 **嚴格因果規則（僅以 `<= τ` 資訊觸發買入訊號，隨後在 `[τ+1, τ+5]` 以實際價格成交）**，則出現大幅收斂（見 `summary_41.md` 第 3–5 節）：

| 規則 / 子集 (`L1+L2+L3` 修正後) | 幣別 | Split | Fee | n | WR | Mean Cost | Mean PnL | 95% CI |
|---|---|---|---|---:|---:|---:|---:|---|
| **嚴格因果** `pre_ev>=3.5c → post5_fill` | `ALL` | `IS` | `new(0.05/0.01)` | 1,810 | 97.57% | 0.9344 | **`+0.04125`** | `[+0.03342, +0.04871]` |
| **嚴格因果** `pre_ev>=3.5c → post5_fill` | `ALL` | `OOS` | `0.07` | 1,880 | 96.28% | 0.9356 | **`+0.02718`** | `[+0.01867, +0.03535]` |
| **嚴格因果** `pre_ev>=3.5c → post5_fill` | `ALL` | `OOS` | `new(0.05/0.01)` | **1,897** | **96.20%** | **0.9322** | **`+0.02988`** | **`[+0.02199, +0.03857]`** |
| ├─ 分幣別 | `BTC` / `ETH` / `SOL` | `OOS` | `new(0.05/0.01)` | 650 / 645 / 602 | 96.2% / 96.1% / 96.4% | ~0.932 | `+0.0289` / `+0.0322` / `+0.0286` | 各自 CI 下界均 `> +0.013` |
| **Model-Free** `OOS_post5` (`[0.68,0.76)`) | `ALL` | `OOS` | `new(0.05/0.01)` | 787 | 73.19% | 0.7464 | **`-0.01452`** | `[-0.04371, +0.01660]` |
| **Model-Free** `OOS_post5` (`[0.76,0.84)`) | `ALL` | `OOS` | `new(0.05/0.01)` | 1,035 | 79.23% | 0.8111 | **`-0.01883`** | `[-0.04450, +0.00505]` |
| `L1` 後保留 `lag <= 1s` (`ask[0.68,0.82]`) | `ALL` | `OOS` | `new(0.05/0.01)` | 235 | 87.23% | 0.7705 | `+0.10186` | `[+0.05775, +0.14018]` |
| `L1` 後保留 `(1,5]s` (`ask[0.68,0.82]`) | `ALL` | `OOS` | `new(0.05/0.01)` | 373 | 84.18% | 0.7671 | `+0.07471` | `[+0.03587, +0.10812]` |

- **科學共識**：修正 `L1+L2+L3` 後，Model-Free 在 `[τ+1, τ+5]` 同樣全面為負（`-0.003` 至 `-0.039`），而嚴格因果 TWAP 模型在三幣 OOS 的真實淨優勢收斂於 **`~ +3.0¢/股`（`[+2.2¢, +3.9¢]`）**，與規劃者在 `live_btc_5m.db` 實測的 **`+2.7¢ ~ +3.7¢`** 高度吻合。由於真實 1Hz 掛單可加上 `limit_px` 上限保護（但也會面臨成交率「錯過」的逆向選擇），**最終只能交給 Section 4.2 三幣實盤盤口錄製來裁決**。

---

## 三、對第 2 節 TWAP Sniper V2 與多重檢定修正的回覆

1. **承認樣本性質**：`live_btc_5m.db`（原 `n=47`, `CI [+0.019, +0.171]`）是在設計 V2 期間檢視過的資料，正式歸類為**樣本內參考（In-Sample / Reference Only）**，不當作 V2 之獨立 OOS 證據。
2. **嚴格凍結 ≤ 3 組設定＋ Bonferroni `98.3% CI`**：
   `edge_validation/frozen_config.json` (`schema_version: "4.2-v2-bonferroni"`) 僅事先登記以下 **3 組固定設定**（錄製與驗證期間鎖定不改）：
   - `v2_pure`（純 TWAP 機率模型，無流過濾、不提早止盈）
   - `v2_filtered`（TWAP 模型 ＋ Order-Flow / Basis Gate）
   - `v1`（原 `DEFAULT_SNIPER_CONFIG` 基線）
   - 統計門檻以 **`α = 0.05 / 3 = 0.0167`** 採 **Bootstrap `98.3% CI`**（`[0.8333, 99.1667]` 分位數），要求主驗證組 **98.3% CI 下界 `> 0.0`** 才算統計顯著。

---

## 四、對第 3 節 Section 4.2（5 項強化要求）與目前實機快照

`edge_validation/live_recorder_42.py` 已完成升級並通過 `audit-code --strict` (`100/100`) 與邊界測試：

1. **時間分界（`2026-11-21 00:00 UTC` = `window_ts >= 1795219200`）**：
   - `--analyze` 明確將資料拆分為 **`post_nov21`（主要裁決組，`11/21 00:00 UTC` 後新費率時期）** 與 **`pre_nov21`（前哨基準線，僅供參考）**。
2. **逐單 CSV 新增全欄位（共 36 欄，含成交與未成交）**：
   `phase42_trades_1s_<model>.csv` 完整記錄 `signal_ts, window_ts, elapsed_sec, symbol, side, regime, fair_at_sig, ask_t0, ask_sz_t0, spread_t0, ask_t1, ask_sz_t1, ask_t2, ask_sz_t2, limit_px, shares, filled_1s, filled_2s, miss_reason, exit_reason, hold_sec, cl_twap_at_sig, bn_px_at_sig, resolved_outcome, would_have_won, pnl_per_share, pnl_per_share_2s, pnl_usd`。
3. **未成交訊號 (`missed_1s`) 逆向選擇追蹤**：
   計算 `miss_rate`（`ASK_ABOVE_LIMIT` / `SIZE_LT_SHARES` / `NO_BOOK_T1`）並直接比較 **`missed_would_win_rate` vs `filled_would_win_rate`**，防止因只記成交單而遺漏「好單特別難成交」偏差。
4. **每日中間報告 (`--daily-report` → `phase42_daily_report.md`)**：
   輸出逐日累積 `n`、`miss_rate`、`mean_pnl_1s`、`95% CI`、`98.3% Bonferroni CI`，在 **`post_nov21 filled_1s < 300`** 之前永遠標記 **`PENDING (filled/300)`**，絕不因中途數字漂亮提前宣告通過。
5. **三幣分開統計與做市商縮窄價差偵測**：
   各模型皆獨立輸出 `BTCUSDT` / `ETHUSDT` / `SOLUSDT` 之 `signals`、`filled_1s`、`miss_rate`、`mean_spread_t0`、`wr_1s`、`missed_would_win_rate`、`mean_pnl_1s` 與 `98.3% Bonferroni CI`，直接觀測 `11/20` 降費後做市商是否將 ETH/SOL 價差壓縮並吃掉原本的定價差。

### 目前背景純只讀錄製實況（`2026-11-18T09:22:55Z` 快照，僅供架構與 `pre-11/21` 基線核驗）
- 錄製窗口：`42` 窗 / 已結算 `37` 窗 ｜ **R1 TWAP 一致率：`100.0%` (`1.0`)**
- `post_nov21`（`>= 1795219200`）：`0 / 300`（**`PENDING`**，將於 11/21 00:00 UTC 開始累積）
- `pre_nov21` 初期煙霧測試（非判斷樣本）：`v2_filtered` 已捕捉 `7` 次訊號（`filled_1s=6`, `missed_1s=1` 在 SOLUSDT 且 `missed_would_win_rate=1.0`, `mean_spread_t0`: BTC `0.010` / ETH `0.020` / SOL `0.020`），證明全鏈路（Top-5 深度、未成交追蹤、逆向選擇指標）正常運作。

---

## 五、對第 4 節實盤試跑與風控規則的承諾

一旦 `post_nov21` 累積 **`>= 300` 筆** `+1s` 保守模擬單且滿足 **Bonferroni `98.3% CI 下界 > 0`（並滿足正幣數 ≥ 2、回撤 ≤ 40%、R1 parity ≥ 99%）**，實盤試跑將嚴格遵守已寫入 `frozen_config.json` 之紀律：
1. **試跑規格**：固定最低 **`5 股/單`** 實盤連跑 **`100 單`**，核驗真實成交均價與紙上模擬差距 **`<= 1¢`**。
2. **硬停機保護**：連輸 **`3` 單**或當日虧損 **`>= $10U`** 當日停機；最近 **`50` 單滾動勝率** 低於「損益兩平勝率 `+ 2 個百分點`」立即停機檢討。
