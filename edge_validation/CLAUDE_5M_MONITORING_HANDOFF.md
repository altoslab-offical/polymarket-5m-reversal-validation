# 5m 專屬監控與 `CLAUDE_RESPONSE_10` 合規交付報告

> **狀態**：**100% 專注 5m 路線**（15m BTC 雙引擎／錄製已依使用者決定正式取消）；`12/12` 凍結核心零改動；潔淨 6 小時 Q1 閒置運行監控已於 `T0 = 1795090647`（`2026-11-19 20:17:27 +08:00` / `12:17:27 UTC`）啟動，自動結算稽核排程於 `2026-11-20 02:22 +08:00`（`18:22 UTC`），仍早於 `2026-11-21 00:00 UTC` 約 30 小時。

---

## 0. 摘要對照（`CLAUDE_RESPONSE_10` 第 7 節與全體裁決）

| 項目 | 本版執行結果 | 證據與連結 |
|---|---|---|
| 路線範圍 | **100% 5m 專屬（取消 15m BTC）** | 不提交 15m 預註冊設定、不啟動 15m 錄製行程（Section 7 #3 略過） |
| Section 7 #2 (`BASE min net EV` + `F2` + `E4`) | **已完成**（`+8.54c` 已成交 / `+7.37c` 全訊號） | [exploration_grid_report.md](exploration_grid_report.md)、[exploration_grid.py](exploration_grid.py) |
| Section 7 #1b (Runner `-4.04U` 進場當秒鑑識) | **已完成**（`book_age = 1.0 ms`, `book_lag = 18.55 ms`） | [Q1_SOAK_INTERIM_3H.md](Q1_SOAK_INTERIM_3H.md) 第 3 節 |
| Section 7 #1a (Q1 潔淨 6h 驗收) | **期中 3.46h 已稽核 + 潔淨 6h 監控已啟動** | [Q1_SOAK_INTERIM_3H.md](Q1_SOAK_INTERIM_3H.md)、[q1_soak_audit.py](q1_soak_audit.py) |
| 主判定層與上線／提早停損門檻 | **維持 3 組**（`v2_pure`/`v2_filtered`/`v1`，Bonferroni `98.33%`） | [run_daily_bundle.py](run_daily_bundle.py)（含 Day-10 fail-only 與 `nice -n 19`） |
| 凍結核心保護 | **12/12 SHA256 一致、`sniper_live = False`** | [q1_soak_audit.py](q1_soak_audit.py) 內建 12 檔指紋比對 |

---

## 1. Section 7 #2：`BASE min(net_ev)` 實證、`F2` 標註與 `E4` 修正

- **`BASE min(net_ev)` 實證（證明 `B1/B2/B3` 非參數漏接）**：
  - 全數觸發訊號（`n = 37`）：最小 `net_ev` = **`+7.37c`**（`0.073725`，`1795005900 BTCUSDT UP`，`ask = 0.82`，`p = 0.9011`，`z = 1.304`，`fee = 0.0074`，`MISSED_PRICE`）。
  - 已成交 `+1s`（`n = 24`）：最小 `net_ev` = **`+8.54c`**（`0.085420`，`1795053600 ETHUSDT DOWN`，`ask = 0.82`，`p = 0.9069`，`z = 1.351`，`fee = 0.0015`）。
  - 原因：`z >= 1.30`（`p >= ~0.9008`）、`ask <= 0.82` 與 BTC 最高手續費 `0.74c` 已數學保證 `net_ev > +7.0c`，因此 `B1` (`>=2c`)、`B2` (`>=5c`)、`B3` (`>=7c`) 與 `BASE` 完全相同；已於 `exploration_grid.py` 新增 `compute_base_min_net_ev()` 自動輸出。
- **`F2` 重複標記**：`build_grid_variants()` 與 [exploration_grid_report.md](exploration_grid_report.md) 已加註 `與 BASE 相同`，保留於 24 組網格不移除。
- **`E4` 客觀鑑識**：`17` 個差異窗中僅 `7` 窗涉及實際 `+1s` 成交變化（`E4` 跳過 `1` 輸單 `+0.48384` 與 `4` 贏單 `-0.26803`、`-0.18846`、`-0.18846`、`-0.04726`，另有 `2` 筆微調 `+0.00994`、`+0.00996`，配對差距僅 `+0.00105/sh`）；`1795071900` 與 `1795080900` 在 Recorder `BASE` 本為 `MISSED_PRICE` 未成交。

---

## 2. Section 7 #1b：Runner `-4.04U` (`1795080900 BTCUSDT UP`) 進場當秒鑑識

取自 `runner_shadow_events.jsonl` 與 `live_3coin_42.db`（確認重連後為最新快照，屬策略正常輸單）：

- **Runner `ENTER`**（`ts = 1795081150`, `t = 250s`, `ask = 0.78`）：**`book_age = 1.0 ms`**、**`book_lag = 18.55 ms`**、`cl_twap_age = 311.0 ms`、`cl_spot_age = 215.0 ms`、`bn_age = 33.0 ms`。
- **Runner 續發微秒**（`ts = 1795081151`, `t = 251s`）：`book_age = 16.0 ms`, `book_lag = 26.67 ms`；（`ts = 1795081154`, `t = 254s`）：`book_age = 0.0 ms`, `book_lag = 18.51 ms`。
- **Recorder 1 Hz 對照**：`t = 250s` (`up_age = 205.41 ms`, `up_lag = 2,486.77 ms`, `up_ask1 = 0.83`)；`t = 251s` (`up_age = 0.07 ms`, `up_lag = 454.23 ms`, `up_ask1 = 0.79`)；`t = 252s` (`up_age = 0.88 ms`, `up_lag = 19.36 ms`, `up_ask1 = 0.87`)。

---

## 3. Section 7 #1a：Q1 監控狀態（3.46h 期中實況 + 6h 潔淨基線 `T0`）

### 3.1 前 3.46 小時（`window_ts >= 1795078200`）四項修正指標實況
見 [Q1_SOAK_INTERIM_3H.md](Q1_SOAK_INTERIM_3H.md)（`12,446s`、`0` 個 1Hz 迴圈缺秒）：

| 指標 | Runner | Recorder (Raw ENTER / 單槽首筆 First) | 觀察 |
|---|---:|---:|---|
| 1. 舊資料決策 (`lag>1000ms` 或 `age>1500ms`) | **`0/8`** (`max_age=22ms, max_lag=245.17ms`) | `1/4` / **`0/3`** | 見下方 3.2(a) 說明 |
| 2. `code=1013` 斷線 | **`0`** | `3` | 第 3 次發生於 `18:55:37 +08:00` 閒置期，見 3.2(b) |
| 3a. `BTCUSDT > 2000ms`（只觀察） | — | `524s` (`4.21%`)｜`P50=68.7, P95=2043.4, P99=2495.3 ms` | 上游偶發突波 |
| 3b. `ETHUSDT / SOLUSDT > 2000ms` | — | `ETH: 44s (0.354%)`｜`SOL: 1s (0.008%)` | 證實本機事件迴圈健康 |
| 4. Runner 重連與進場時段損失 | **`128` 次重連** / `STALE_BOOK = 3/11,274 (0.0266%)` | Recorder `[150,255]s` `age>1500ms`: `BTC 1.058%, ETH 0.23%, SOL 0.115%` | 僅極低佔比缺口 |

### 3.2 提前向規劃者揭露的兩項現場觀察（供審視）
1. **(a) Stateless `signal_enters_42` 第二秒續發列 `1795088639`**：
   - Recorder 於 `1795088400 BTCUSDT DOWN` 第 `238s` 首發進場時 `book_age = 4.18 ms`, `book_lag = 825.16 ms`（完全新鮮）；但因 `signal_enters_42` 為 stateless 紀錄器，第 `239s` 續發列出現 `book_age = 57.45 ms`, **`book_lag = 1,336.88 ms > 1,000 ms`**（同一視窗 Runner 已於 `237s` 持倉鎖單，未重進場）。
   - 因 Q1b 選 A 維持 `2,500 ms` 丟棄線不改傳輸層，BTC 進場時段內約 `4.93%`（`209/4240s`）落在 `1,000–2,500 ms` 上游 lag 區間；因此 6h 報告將同時誠實列出 **Single-Slot 首筆** 與 **Raw Stateless 全數** 兩組數字，請確認 Gate 1 以單槽實質決策為準或須如何判定。
2. **(b) `18:55:37 +08:00` 的第 3 次 `BTCUSDT code=1013`**：
   - 當時無重運算，DB 顯示 `ts=1795085737` 前後 BTC `lag = 66.74 ms`、1Hz 迴圈零缺秒（純屬 Polymarket 上游端送出 `1013 Try Again Later`，同一時間 Runner `clob_1013_count = 0`）；`18:55` 至 `20:17` 亦未再發生。

### 3.3 潔淨 6 小時監控視窗已啟動
- 不重啟常駐程式（`PID 32419` Recorder 與 `PID 32423` Uvicorn 持續運行中），已於 **`T0 = 1795090647`**（`2026-11-19 20:17:27 +08:00`）以 `q1_soak_audit.py --snapshot-baseline` 鎖定純閒置基線。
- 期間主機保持 100% 閒置（不跑回測／網格），預計於 **`2026-11-20 02:22 +08:00`** 自動產出 6 小時乾淨報告 `edge_validation/Q1_CLEAN_SOAK_REPORT.md` 後回報。

---

## 4. 治理強化與凍結檔案驗證（最後一次編輯後實測）

- **Day-10 Fail-Only 與 CPU 隔離**：[run_daily_bundle.py](run_daily_bundle.py) 預設 `os.nice(19)`，並加入 `evaluate_day10_early_stop()`（第 10 天後若 3 組 `98.33% CI` 上界皆 `< +2c/sh` 才標記 `EARLY_STOP_INSUFFICIENT_EDGE`，否則持續跑到 `n >= 300`）。
- **自動化測試**：`.venv/bin/pytest -q tests/test_exploration_grid.py tests/test_q1_soak_audit.py` → **`10 passed`**（[test_exploration_grid.py](../tests/test_exploration_grid.py)、[test_q1_soak_audit.py](../tests/test_q1_soak_audit.py)）。
- **嚴格代碼品質稽核**：`/Users/asdc163/KnowledgeBase/kb train audit-code --strict` → 5 支 Python 檔全數 **`100/100 (hard=0)`**。
- **12 檔凍結核心 SHA256**：**12/12 完全一致**，`sniper_live = False`、`paper_mode = True`、`frozen_parity_ok = True`。
