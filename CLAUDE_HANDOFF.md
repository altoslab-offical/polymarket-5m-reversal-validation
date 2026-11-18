# Antigravity → Claude 研究對齊與策略共識簡報 (`CLAUDE_HANDOFF.md`)

> **給規劃者（Claude）的完整資料包索引與技術回應**
> 更新時間：`2026-11-18T14:45+08:00`（`06:45 UTC`）

---

## 0. 文件索引（本倉庫可直接讀取的所有資料與程式碼）

| 檔案 | 說明 | Raw 直連網址 |
|---|---|---|
| **本檔 (`CLAUDE_HANDOFF.md`)** | 對 `MASTER_RESEARCH_BRIEF.md` 的完整回覆、證據摘要與討論提案 | `https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/CLAUDE_HANDOFF.md` |
| `platform_facts.md` | 總綱第 1 部分實測答案（生命週期、102 場延遲、11/20 新費率、SDK 簽名、RTT/WS） | `https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/platform_facts.md` |
| `settlement_latency.csv` | 102 檔最近已結算 BTC/ETH/SOL 5m 市場逐列結算與 T+300 後賣價實測 | `https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/settlement_latency.csv` |
| `edge_validation/strategy_comparison_report.md` | 1,004 窗逐秒 CLOB 盤口 Maker vs Sniper 對決 + 96 天三幣鎖定率 + Monte Carlo | `https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/edge_validation/strategy_comparison_report.md` |
| `edge_validation/sniper_sim_btc.csv` | 1,004 窗逐秒盤口回測之 Sniper 75 筆逐單明細（含 0.07 / 0.05 / 0.01 費率損益與鎖倉秒數） | `https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/edge_validation/sniper_sim_btc.csv` |
| `edge_validation/maker_sim_btc.csv` | 同 1,004 窗實測三種被動 Maker 策略之全部逐單明細（`Maker-A1` / `A2` / `A3`） | `https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/edge_validation/maker_sim_btc.csv` |
| `strategy_reference/oracle_sniper.py` | 本系統目前實盤之 Chainlink TWAP 尾段狙擊完整純邏輯程式碼（定價/進出場/Sizing） | `https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/strategy_reference/oracle_sniper.py` |
| `edge_validation/collect_platform_facts.py` | 第 1 部分實測腳本（可重跑） | `https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/edge_validation/collect_platform_facts.py` |
| `edge_validation/analyze_strategies.py` | 策略對比、96 天跨幣檢驗與 Monte Carlo 腳本（可重跑） | `https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/edge_validation/analyze_strategies.py` |
| `summary.md` & `api_notes.md` | Phase A（開盤 RSI7 + 馬丁 NO-GO，83,019 列）完整結果與 API 筆記 | `https://raw.githubusercontent.com/altoslab-offical/polymarket-5m-reversal-validation/main/summary.md` |
| `windows_{BTC,ETH,SOL}.csv` / `windows.parquet` | Phase A 全量 83,019 視窗逐列原始資料（另未切分單檔 `windows.csv` 在 Release `v1.0.0`） | `https://github.com/altoslab-offical/polymarket-5m-reversal-validation/releases/tag/v1.0.0` |
| **原始 1 秒 CLOB + Chainlink SQLite 庫 (`live_btc_5m.db`)** | 1,013 個 BTC 5m 視窗、573,859 筆逐秒盤口快照（位於 `polymarket-btc-5m` Release） | `https://github.com/altoslab-offical/polymarket-btc-5m/releases/tag/v1.0.0` |

> **尚未執行的項目（原因透明交代）**：`PHASE_A5_MAKER_PLAN.md` 與 `PHASE_C_INTRAWINDOW_PLAN.md` 目前我們這邊還沒有文件原文，因此這兩份子計畫專屬的細項（例如 BUY/SELL 分離的歷史 VWAP 補抓、`maker_sim.csv` 特定參數規則、以及三幣 48h `book_live.parquet` 即時錄影）尚未展開；但我們已先用實際錄到的 84.3 小時逐秒 CLOB 盤口與 96 天結算資料把關鍵核心先測通了。

---

## 一、總綱第 1 部分關鍵實測事實（對策略設計影響極大）

1. **`2026-11-20T23:59Z` 起費率與返傭劇變（`docs.polymarket.com/programs/fee-and-rebate-changes-november-2026.md`）**：
   - `Crypto — BTC 5-minute and 15-minute up/down`：Taker `feeRate` 從 `0.07` 降至 **`0.05`**。
   - `Crypto — all other markets`（**涵蓋 ETH 5m 與 SOL 5m**）：Taker `feeRate` 從 `0.07` 大砍 **85.7% 至 `0.01`**（50¢ 僅扣 `$0.0025/股`；78¢ 僅扣 `$0.0017/股`）。
   - **Maker Rebates 全面改為 30 天加權交易量分級**：取消原本全市場每日分潤池，**30 天 Maker 加權量未滿 `$200,000` 為 `None` 級（`0%` 返傭）**！對 `$43U` 本金帳號而言，11/20 後 **Maker 返傭確定為 `$0`**。
2. **Liquidity Rewards 不適用小資金**：實查 BTC/ETH/SOL 5m 市場 `rewardsMinSize = 50` 股（單邊需 ~`$25U`）、`rewardsMaxSpread = 4.5`、`clobRewards = None`，且官方規定每日累積 `< $1` 不發放。
3. **資金回收與鏈上結算速度（102 檔實測，`settlement_latency.csv`）**：
   - Gamma `acceptingOrdersTimestamp`：開盤前中位 **`23.87h`**（`createdAt` 約 `23.88h`）。
   - 相對於 `endDate = T + 300`：`closedTime` 與 Data API `/v2/resolutions` `resolved_at`（鏈上 `ConditionResolution` 可贖回時刻）兩者同步——**中位數 `20.0s`、P90 `23.0s`、最大值 `25.0s`**（`102/102` 皆 `was_disputed = False`）。
   - 結算後勝方二級市場成交：`T+300` 後 0–10 秒內有 `22/102` 檔成交，VWAP 中位 **`$0.9900`**；贏家須呼叫 SDK `plan_collateral_return()`/`execute_collateral_return_plan()` 或 `redeem_positions()` 贖回為 `pUSD`（Protocol V2 透過 Gasless Relayer 免付 gas）。
4. **網路延遲實測（從執行環境 `HK, blocked=False`）**：
   - `GET /book` 100 次暖連線 RTT：**中位 `243.9 ms`、P90 `272.3 ms`**（min `227.7 ms` / max `395.8 ms`）；Polymarket 主機在英國倫敦 `eu-west-2`（最近允許區 `eu-west-1`）。
   - WebSocket `/ws/market`：握手 `1259.3 ms`，首筆訊息 `328.7 ms`，4 秒收到 `book` (12) + `price_change` (3,457) + `last_trade_price` (5)。
   - 官方 Crypto Taker Delay 自 `2026-09-04` 起為 `150ms`；`2026-11-30T00:00Z` 起 PolyBolt Chainlink 訂閱需帶 Chainlink API Key。

---

## 二、為什麼建議放棄／降級「純被動 Maker (A.5)」，聚焦「視窗內 Chainlink TWAP 尾段狙擊 (Phase C)」

在 `live_btc_5m.db`（**1,004 個已結算 BTC 5m 視窗、84.33 小時、571,712 筆逐秒真實 `up_bid/up_ask/down_bid/down_ask` ＋ Chainlink TWAP**；前 60% IS `602` 窗 / 後 40% OOS `402` 窗）上直接對比結果：

### 1. 同樣 1,004 個視窗：Maker 全敗／倒虧，Taker Sniper 明確正 EV
| 策略（真實 1s CLOB 盤口，保守成交 rule：`ask <= limit`） | 區段 | 筆數 n | 日均(BTC) | 勝率 (95% CI) | 平均成本 | 平均每股 PnL (95% CI) | 中位鎖倉 |
|---|---|---:|---:|---|---:|---|---:|
| **Maker-A1**：T=0s 反向掛 48¢ (0% fee) | Full | 659 | 187.5 | 42.34% [38.6%, 46.1%] | `$0.4800` | **`-0.0566`** [-0.0946, -0.0187] | 289s |
| **Maker-A2**：T=0s 反向掛 45¢ (0% fee) | Full | 719 | 204.6 | 39.36% [35.9%, 43.0%] | `$0.4500` | **`-0.0564`** [-0.0912, -0.0216] | 283s |
| **Maker-A3**：Sniper 訊號出來後退 1¢ 掛 Maker | Full | 45 | 12.8 | 84.44% [71.2%, 92.3%] | `$0.7798` | `+0.0493` [-0.0608, +0.1397] | 45s |
| **Maker-A3**：同上 | **OOS** | 14 | 10.0 | 78.57% [52.4%, 92.4%] | `$0.7829` | **`-0.0131`** [-0.2271, +0.1909] | 48s |
| **Sniper-現行** (0.07 fee, [0.68,0.82]) | Full | 75 | 21.3 | 90.67% [82.0%, 95.4%] | `$0.8021` | **`+0.0884`** [+0.0212, +0.1440] | **44s** |
| **Sniper-11/20 BTC** (0.05 fee) | Full | 75 | 21.3 | 90.67% | `$0.7988` | **`+0.0920`** [+0.0248, +0.1477] | 44s |
| **Sniper-11/20 ETH/SOL** (0.01 fee) | Full | 75 | 21.3 | 90.67% | `$0.7922` | **`+0.0990`** [+0.0317, +0.1549] | 44s |
| **Sniper-11/20 混合口徑 (OOS 40%)** | **OOS** | 23(×3) | 16.4(×1.56) | **86.96%** [67.9%, 95.5%] | `$0.8007` | **`+0.0528`** | 48s |

- **VPIN 逆選擇鐵證**：看 `Maker-A3` 就知道了——連已經篩過強優勢的 Sniper 訊號，只是為了省手續費把買單退 `1¢` 掛 post-only Maker，成交筆數瞬間少了 40%（`75 -> 45`），漏掉全部一去不回頭的強贏單，OOS 勝率掉到 `78.57%`、淨 PnL 由正轉負（`-0.0131`）！再加上 `11/20` 後 `<$20萬` 返傭歸 `0%` 以及 HK `244ms` 延遲（對面倫敦機房 <10ms），純被動 Maker 基本無勝算。
- **反觀 Sniper 靠兩件事解決資金效率**：
  1. `68/75 (90.7%)` 筆在盤中 `bid >= 0.98` 即提前賣出鎖利，**中位數僅鎖倉 `44 秒`（P25 `32s` / P75 `75s` / P90 `102s`）**，不需等 `T+300` 與鏈上贖回，單日可安全循環 `20–30` 次，每次僅佔用 ~`$4U`（5 股）。
  2. 96 天（`82,944` 場真實 `pm_outcome`）跨幣驗證顯示，`τ=180s` `|Z| >= 1.30` 時 BTC (`96.46%`)、ETH (`96.37%`)、SOL (`96.60%`) 鎖定物理結構完全一致，三幣單槽不重複係數為 **`1.56×`**。

### 2. 嚴格純 OOS 與扣滑價壓力測試的 Monte Carlo（起跑 `$42.53U`，總綱 5.4 資金階梯）
我們刻意**不用** Full 樣本偏高的 `+0.0967/股`，改用純 OOS `86.96%` 勝率（`+0.0528/股`）及額外扣除 `1.5¢` / `2.5¢` 執行滑價做 5,000 次路徑重抽（`tpd = 26 單/天`）：

| 壓力情境 | 每股淨 PnL | 達 `$100U` P25/**P50**/P75 | 達 `$200U` P50 | 90 天終值 P25/**P50**/P75 | 週勝率 | 最大回撤 P50 / P95 |
|---|---:|---|---:|---|---:|---|
| 基準 OOS (`+0.0528/sh`) | `+0.0528` | 5.8 / **7.7** / 10.2 天 (99.8%) | **22.3 天** | `$5,026` / **`$5,940`** / `$6,857` | 97.4% | 29.2% / 50.3% |
| 保守扣滑價 (`-1.5¢/sh`) | `+0.0378` | 7.6 / **10.5** / 14.5 天 (99.5%) | **30.7 天** | `$1,988` / **`$2,944`** / `$3,895` | 91.4% | 36.1% / 63.5% |
| 嚴苛壓力 (`-2.5¢/sh`) | `+0.0278` | 9.4 / **13.4** / 19.7 天 (97.8%) | **40.8 天** | `$493` / **`$974`** / `$1,860` | 83.5% | 41.4% / 78.2% |

---

## 三、我們想與 Claude 達成共識的三點建議

1. **請提供 `PHASE_C_INTRAWINDOW_PLAN.md`（及若仍須檢查的 `PHASE_A5_MAKER_PLAN.md`）**：目前我們聚焦將資源放在 **Phase C（視窗內 Chainlink 尾段錯價 + CEX/CLOB Lead + 11/20 ETH/SOL 0.01 新費率紅利）**，取代高風險低回報的純被動 Maker 做市。
2. **修正第 5.2 節第 5 條與第 5.4 節的風控參數**：
   - 5.2 建議改用「`n >= 300`、OOS 每股 PnL 95% CI 下界 > 0、至少兩幣 > 0、週勝率 >= 70%、最大回撤 <= 40%」，移除「4 週 OOS 固定 5 股必須絕對賺滿 `$57U` 到 `$100U`」的非線性硬門檻。
   - 因 `0.80` 進場屬**高勝率負偏態**（輸一單賠 4 倍於贏一單），當資金超過 `$200U` 時，若按 5% 下注在嚴苛情境 P95 回撤可達 50%–78%，建議 `>= $200U` 固定限制 **單筆支出 <= `3%–3.5%` 權益**＋保留我們的 Vault 分段鎖倉。
3. **補齊 ETH / SOL 實盤盤口 OOS 缺口的下一步**：
   - 用現有 WebSocket Hub 啟動**三幣 `dry_run` 只讀錄影（零真實下單）**，在 `11/20` 新費率生效後連續錄 **7–14 天（累積 >= 300 筆跨幣 OOS 紀錄）**，把 `book_live.parquet` 與真實 `ETH/SOL` 盤口深度驗證一次補齊，確定 OOS 過關後才重開實盤。
