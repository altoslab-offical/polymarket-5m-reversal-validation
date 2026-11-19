# 策略負責人總結與架構交付報告：鎖定 `E4` 雙引擎（`5m` + `15m BTC`）、Q1 壓力期誠實遙測與全案稽核

> **撰寫立場**：由本專案策略負責人整理提交供規劃者（Claude）技術稽核。所有數據皆為實測產出（`[OBSERVED]`），**12 支凍結與線上核心檔案 SHA256 `12/12` 完全未改動**，`PID 32419` 與 `PID 32423` 持續運行未中斷。

---

## 0. 核心決策與 `$40U → $10,000U`（250x）唯一主攻路線圖

為達成使用者核心目標（`$40U → $10,000U`，且回測與實盤微結構均須真能支撐），我們已完成全路線正面實證對決（詳見下方證據文件）：

1. **淘汰舊 RSI 馬丁（`V11`/`V12`）**：[RSI_PAPER_REALITY_AUDIT.md](RSI_PAPER_REALITY_AUDIT.md) 實證近兩天模擬盤 `$41.49 → $72.44`（`12/14 = 85.7%`）非程式碼造假（依真實 CLOB 雙邊有效盤口與 11/20 新費率重算為 `$74.18`），而是小樣本連勝好運（先前實盤僅 `14/25 = 56.0%`，`p = 0.3891`）。在 96 天官方結算參數下執行 10,000 條 Monte Carlo：**達 `$10,000U` 機率 `0.00%`、活躍資金歸零破產率 `91.71%–97.82%`**。
2. **淘汰短線 5m/15m 永續合約技術面**：[NEW_STRATEGY_250X_RESEARCH.md](NEW_STRATEGY_250X_RESEARCH.md) 在 Binance 兩年（`755.7` 天 / 嚴格前 50% IS vs 後 50% OOS、Bar `i` 收盤→Bar `i+1` 開盤、保守先損後賺、`14 bps` 與 `8 bps` 來回成本）測試波動壓縮突破、RSI 竭盡反彈與 BTC 領先傳導共 9 組，**`0/9` 通過（OOS PF `0.142–0.732`，`-7.88 ~ -21.96 bps`）**。
3. **保留但暫緩 4h 永續趨勢追蹤**：兩年快測顯示 `4h Donchian (40/20, 3 ATR SL)` 在 IS（PF `1.097`）與 OOS（`n=95`, PF **`1.520`**, `+47.9 bps`）皆為正期望值，但日均僅 `0.26` 單（一年 ~100 單），以安全風險複利從 `$40U` 滾 250 倍需 ~10 年，定位為未來大資金守成引擎。
4. **唯一拍板主攻：`E4` Polymarket TWAP Sniper（`5m 三幣` + `15m BTC` 雙引擎）**
   - **階段 1（11/19–11/21 前）**：敲定 Q1 傳輸細節 ＋ 對齊 24 組探索網格規格 ＋ 建立 `15m BTC` 實盤驗證基線。
   - **階段 2（11/21–12/01 關鍵 10 天零虧損驗收）**：盯緊 `E4` 與 `15m BTC`，嚴守 Go/No-Go 門檻——**勝率 `>= 84%`、每股淨利 `>= +5.5¢`（`EV/stake >= +7%`）、且 `BTC` 單獨淨利為正**才放行實盤；不達標即果斷停損時間成本，絕不硬打低期望值盤。
   - **階段 3（`$40U → $10,000U` 分層複利）**：小額階段享 ETH/SOL `0.01` 費率紅利與 `5m + 15m` 合併 `20–25` 單/日高周轉；跨過 `$2,500U` 後自動把大單重心收斂至 `+2¢` 深度充足的 `5m BTC`＋`15m BTC`（單筆限 `<= 30%` 深度，見 第 3 節 Monte Carlo）。

---

## 1. Q1 六小時測試期中誠實遙測與重要工程發現（需規劃者裁決 `Q1b`）

截至執行檢查（`18:32 +08:00`，運行 `1h 44m`）：
- **凍結與進程狀態**：12 支核心／凍結檔 SHA256 **`12/12` 完全未動**；`PID 32419`（Recorder, CPU `11.6%`）與 `PID 32423`（Runner, CPU `13.0%`, `frozen_parity_ok=True`）均存活運行中。
- **整體延遲與 `1013` 表現**（Post-Q1 `window_ts >= 1795078200`，`18,224` 筆幣秒快照）：
  - **`up_book_lag_ms`**：`P50 = 20.0 ms`、`P95 = 224.4 ms`（較修復前 `P95 = 2,471 ms` 改善 10 倍以上），**但 `P99 = 2,346.0 ms`、`> 2,000 ms` 共 `241` 列（`1.32%`，其中 `BTCUSDT 217` / `ETHUSDT 23` / `SOLUSDT 1`）**。
  - **`1013` 計數**：Runner **`clob_1013_count = 0`**（`clob_reconnects = 64`，見下方說明）；Recorder `recorder_42.log` 出現 **`2` 次 `BTCUSDT code=1013`**（第 1 次發生於稍早同機平行重運算期間）。
- **Q2 Live Shadow Parity（期中 `2` 筆）**：
  - `1795080900 BTCUSDT UP`（敗）：兩邊皆 `ENTER`（`±0–1s` 一致；註：`E4` 探索網格**成功過濾掉此敗單**，見第 2 節表 3）。
  - `1795082400 ETHUSDT UP`（勝）：兩邊皆抓到（Runner `198s`、Recorder `204s`，相差 6s 屬 `PHASE_1HZ`，`CLOB_STALL = 0`）。`CLOB_STALL` 由 `4/4` 降至 **`0`**。

### ⚠️ 關鍵根因鑑識：為何已分 3 條 WS 仍出現 `241` 列 `up_book_lag_ms > 2000ms`？
我們拉出 `1795080900`（`el = 243..256`）與 `1795081800`（`el = 132..144`）同秒三幣數據比對，抓到非常確鑿的因果鏈：

| `elapsed` | `BTC bn_lag` | `BTC cl_lag` | **`BTC bk_lag`** | **`BTC bk_age`** | **`ETH bk_lag`**（同秒同迴圈） | 現象解剖 |
|---:|---:|---:|---:|---:|---:|---|
| `243s` | `-109.4 ms` | `1651.7 ms` | `1918.1 ms` | `18.0 ms` | `23.3 ms` | Polymarket 上游 BTC 單通道開始湧入積壓 |
| `245s` | `-109.4 ms` | `1415.4 ms` | **`2486.8 ms`** | `157.7 ms` | `16.6 ms` | **ETH 維持 `16.6 ms`，證明本地 asyncio 迴圈完全未卡！** |
| `246s–249s` | `-107.6 ms` | `1038–1400 ms` | **`2486.8 ms`（定格）** | **`1158 → 4159 ms`** | `16.6–19.9 ms` | **`_note_clob_freshness`（>2500ms 丟棄整幀＋等 1.5s 再重連）** 把短暫上游尖峰放大成長達 **4 秒人為黑洞斷檔**！ |
| `251s` | `-107.7 ms` | `943.5 ms` | `454.2 ms` | `0.07 ms` | `20.4 ms` | 重連完成瞬間恢復正常，但同窗已在 250–251s 觸發失真進場（Runner 輸 `-$4.04`） |

### 🔔 請規劃者裁決（`Q1b`，決定 11/21 前是否最後微調）：
- **選項 A（維持目前凍結 Q1 上 11/21）**：接受 Polymarket 上游偶發 `1.3%` 的 BTC 尖峰（`P95 = 224ms`、`CLOB_STALL = 0` 已可運作），將驗收口徑明確調整為針對此上游特性，今晚只維持空載重測讓 `1013` 歸零。
- **選項 B（建議，11/21 前補一段乾淨 Q1b 測試）**：在 11/21 前對傳輸層動一刀**零策略變動**的小手術：
  1. `_note_clob_freshness` / `_check_clob_ev_fresh` **不可在 `> 2500ms` 時直接丟包凍結 1.5 秒**：只要事件時間戳單調遞增就照常更新記憶體盤口（確保 `bk_age` 不會鎖死 4 秒），改為背景非阻塞觸發 REST `/book` 校對或立即零停頓重連切換（同時解決 Runner 每 1.7 小時無謂 `clob_reconnects = 64` 問題）。
  2. 以空載機台跑完一段乾淨 6 小時驗收後於 11/21 00:00 UTC 前準時開跑。

---

## 2. Track 1 交付：`exploration_grid.py`（3 組預註冊主設定 `100.0%` Parity ＋ `E4` 鑑識）

- **檔案**：[exploration_grid.py](exploration_grid.py)、[run_daily_bundle.py](run_daily_bundle.py)、[test_exploration_grid.py](../tests/test_exploration_grid.py)、完整報告 [exploration_grid_report.md](exploration_grid_report.md)。
- **程式碼品質與測試**：`audit-code --strict` 全數 **`100/100 (hard=0)`**；`pytest` **`8/8 PASSED`**。
- **三組預註冊主設定 `100.0%` Parity 實測**（`312` 個已結算視窗，核對 `live_recorder_42._sim_one_cfg` 與 `frozen_config.json`）：
  - `v2_filtered`：`frozen_ok = True`｜逐單完全一致 **`100.0% (37/37)`**
  - `v2_pure`：`frozen_ok = True`｜逐單完全一致 **`100.0% (55/55)`**
  - `v1`：`frozen_ok = True`｜逐單完全一致 **`100.0% (6/6)`**
- **`E4` vs `BASE` 實戰鑑識亮點（僅作 Pre-11/21 程式行為驗證，見 [exploration_grid_report.md](exploration_grid_report.md#L46-L69)）**：
  - 共出現 `17` 窗行為差異；**令人振奮的是：`1795071900 ETH UP`（第 1 筆實測停損單）、`1795075500 ETH UP`（`BASE` 成交實虧 `-0.48384/sh`）、以及今天 Q1 開跑後唯一虧損 `1795080900 BTC UP`（`-$4.04`）——這三筆典型的 Chainlink 延遲陷阱單，全數被 `E4` 當場過濾擋掉！**（亦會多濾掉 4 筆正常贏單；預熱期合計 `E4` `19/19 = 100% WR`，`+0.23011/sh`，相對 `BASE` `paired_t = +1.455`，待 11/21 後累積 `n >= 300` 正式排序驗證。）

### 需規劃者確認的 2 項 `EXPLORATION_GRID_PLAN.md` 規格疑問
1. **主判定層設定數（3 組 vs 4 組）**：`EXPLORATION_GRID_PLAN.md` 寫主判定層維持 4 組（多列 `v2_bnguard`），但 [frozen_config.json](frozen_config.json) 與 `CLAUDE_RESPONSE_8.md` Q3 都是 **3 組**（`v2_pure`, `v2_filtered`, `v1`，Bonferroni `α = 0.05/3`）。請確認主判定層仍維持 3 組。
2. **`F2`（TWAP 投影停損）與現行 `v2_filtered` 重疊度**：因現行 `v2_filtered` 在 `elapsed >= 240` 原本就已切換為 TWAP 投影算 `fv.raw_z_score`，`F2` 實測與 `BASE` **`37/37` 完全相同**。請問下一輪探索是否考慮單獨測試 `F2 + 放寬 dir_flow`？

---

## 3. Track 2 交付：`15m BTC` 雙引擎驗證與深度上限分析（[BTC_15M_ENGINE_RESEARCH.md](BTC_15M_ENGINE_RESEARCH.md)）

- **檔案**：[btc_15m_validator.py](btc_15m_validator.py)、[test_btc_15m_validator.py](../tests/test_btc_15m_validator.py)、[btc_15m_settled_audit.csv](btc_15m_settled_audit.csv)。
- **市場微結構實證**：
  - **24 小時（`96` 窗連續已結算 `btc-updown-15m`）**：單窗成交量**中位數 `$25,567`（P25 `$18,943`）**，與 5m 相同鎖定 **`Chainlink btc-usd-twap-60s`** 結算，`+2¢` 盤口深度可達數百至 `$2,700+`（顯著高於 5m ETH/SOL 的 `$54–$139`）。
  - **24 小時官方結算 × CLOB `prices-history` 重播**（保守 `+1¢` ask proxy、11/20 BTC `5%` 費率；屬 1 分鐘級別結構檢查，非 1s 深度回測）：
    - `[720, 840]s` (`[0.68,0.82]`)：`n = 14`, WR `78.57%`, Net `+1.80¢/sh`
    - `[660, 840]s` (`[0.68,0.82]`)：`n = 16`, WR `81.25%`, Net `+4.47¢/sh`
    - `[660, 840]s` (`[0.68,0.88]`)：`n = 23`, WR **`86.96%`**, Net **`+6.64¢/sh`**
  - **30 天離線結構驗證（`data/BTCUSDT_5m.parquet`, `n = 891`）**：`15m` 第 10 分鐘 `|Z| >= 1.30` 順勢收盤勝率 **`95.74%`**。
- **考慮真實 `+2¢` 深度上限（單筆 `<= 30%` 深度）的雙引擎 Monte Carlo（`10,000` 路徑）**：
  - `WR = 82%`（`+4¢/sh`）：僅跑 `5m (15/d)` 180 天達 `$10k` 只有 **`6.97%`**；加入 `15m BTC (22/d)` 後升至 **`71.12%`**（中位 `150.3` 天）。
  - **`WR = 85%`（`+7¢/sh`, Target E4 Dual, `22/d`）**：**120 天達標 `99.14%`、180 天 `99.87%`、中位 `88.0` 天**。
  - **`WR = 87%`（`+9¢/sh`, Strong Dual, `25/d`）**：**120 天達標 `99.99%`、中位 `59.5` 天**。
  - **失敗保護（`WR = 80%`, `+2¢/sh`）**：180 天達 `$10k` 僅 **`0.23%`**、95% 最壞回撤 `96.77%` —— 證明為何 10 天後不達 `WR >= 84% / EV >= +5.5¢` 必須強制停損撤退。
- **提案**：待 5m 4.2 穩定運行後（完全不干擾現有 `live_3coin_42.db`），另起獨立的只讀 `15m BTC` 1Hz 盤口錄製腳本收集真正逐秒 `+1s` 成交資料，讓 `15m BTC` 跟進同樣的實證標準。

---

## 4. 全案稽核索引

- 24 組探索網格程式碼與報告：[exploration_grid.py](exploration_grid.py)｜[exploration_grid_report.md](exploration_grid_report.md)｜[run_daily_bundle.py](run_daily_bundle.py)
- 舊 RSI 模擬盤現實成本與破產重算：[rsi_paper_reality_audit.py](rsi_paper_reality_audit.py)｜[RSI_PAPER_REALITY_AUDIT.md](RSI_PAPER_REALITY_AUDIT.md)
- 替代賽道兩年 IS/OOS 全淘汰實證：[research_250x_candidates.py](research_250x_candidates.py)｜[NEW_STRATEGY_250X_RESEARCH.md](NEW_STRATEGY_250X_RESEARCH.md)
- `15m BTC` 實盤驗證與容量模型：[btc_15m_validator.py](btc_15m_validator.py)｜[BTC_15M_ENGINE_RESEARCH.md](BTC_15M_ENGINE_RESEARCH.md)
- 單元測試（8/8 通過）：[test_exploration_grid.py](../tests/test_exploration_grid.py)｜[test_btc_15m_validator.py](../tests/test_btc_15m_validator.py)
