# 6.14 小時 Q1 驗收總結與 `CLAUDE_RESPONSE_10` 第 7 節完整交付（5m 專屬）

> **監控區間**：`since_ts = 1795090647`（`2026-11-19 20:17:27 +08:00` / `12:17:27 UTC`）→ `2026-11-20 02:26:03 +08:00`（`18:26:03 UTC`），共 **`22,116` 秒（`6.14` 小時）**、`0` 個 1 Hz 事件迴圈缺秒。  
> **路線範圍**：依使用者指示 **100% 專注 5m 路線**（15m BTC 已取消，第 7 節第 3 項略過）。  
> **誠實揭露原則**：本報告完整呈現 **PASS 與 FAIL 指標**、夜間 `22:20:43` `uvicorn` 重啟、`01:14:22` 10 秒網路瞬斷與 `10/12` 凍結指紋異動事實。

---

## 一、Q1 修正版四項驗收指標總表（6.14 小時實測）

詳見 [Q1_CLEAN_SOAK_REPORT.md](Q1_CLEAN_SOAK_REPORT.md)。

| 指標（`CLAUDE_RESPONSE_10` 第 1 節） | Runner | Recorder (`PID 32419` 未重啟 9h36m) | 門檻 | 判定 |
|---|---:|---:|---|---|
| **1. 舊資料決策** (`book_lag>1000ms` 或 `book_age>1500ms`) | **`0 / 8`** (`max_age=428ms`, `max_lag=337.69ms`) | Raw **`0 / 13`** / 單槽首筆 **`0 / 5`** (`max_age=478.17ms`, `max_lag=606.97ms`) | `0` | **PASS (`0/21`)** |
| **2. `code=1013` 斷線** | `2`（`22:20` 重啟後 `4.05h` 計數） | **`+3`**（`1013_total: 3 -> 6`，約 `0.49/hr`） | `0` | **FAIL** |
| **3. 分幣別 `>2000ms` 秒數**（`22,116s`，美盤尖峰時段） | — | `BTC: 1,414s (6.394%)`｜**`ETH: 227s (1.026%)`**｜**`SOL: 34s (0.154%)`** | `BTC` 只觀察；`ETH/SOL` 近 `0` | **ETH 偏高 (`1.026%`)** |
| **4. 重連與進場時段 `[150,255]s` 損失** | **`193` 次**（`4.05h` 重啟後真實累計）；`STALE_BOOK = 5/19,510` (**`0.0256%`**) | `age>1500ms`: `BTC 2.499%`, `ETH 0.395%`, **`SOL 0.140%`**（合計 `1.011%`）；1Hz 缺秒 **`0`** | 觀察報告 | **INFO** |

### 1.1 第 2–4 項的鑑識細節（不隱藏任何數字）
1. **為何 `code=1013` 仍偶發（Recorder `+3`、Runner `2`）？**
   - `recorder_42.log` 顯示 `BTCUSDT` 單獨斷線重連且 `loop_gaps_gt_1s = 0`、`P50 lag = 45.26–52.22 ms`（本地事件迴圈健康無阻塞），屬於 Polymarket CLOB WS 上游節點的 `1013 Try Again Later` 負載拒收。
   - **例外系統事件**：`01:14:22–01:14:32 +08:00`（`ts=1795108462..1795108472`）發生過 1 次約 **10 秒全域網路／VPN 閃斷**（Binance 與 3 幣 CLOB 同時出現 `max_bn_age=10,442ms`、`max_bk_age=12,341ms`），系統於 10 秒後自動全數恢復、未崩潰。
2. **`ETHUSDT > 2,000ms` 逐小時分布**（確認是否受 `22:20` 變動影響）：
   - `hr0 (20:17–21:17)`: `18s`｜`hr1 (21:17–22:17)`: `30s`｜`hr2 (22:17–23:17)`: **`63s`**｜`hr3 (23:17–00:17)`: `36s`｜`hr4 (00:17–01:17)`: `42s`｜`hr5 (01:17–02:17)`: `38s`｜`hr6`: `0s`（合計 `227s`）。
   - `SOLUSDT` 逐小時：`10, 0, 1, 18, 4, 1, 0s`（其中 `hr3` 的 `18s` 含 `01:14` 全域瞬斷；總佔比僅 **`0.154%`**，`P95=183.13ms`, `P99=705.42ms`）。
3. **關於 `Q1_CLEAN_SOAK_REPORT.md` 的 Runner 重連數口徑**：
   - `app.py` 於 `22:20:43` 重啟一次（新 PID `43343`），因此 `/api/sniper/status` 內的 `feeds.clob_reconnects`（記憶體計數）歸零後在 `4.05h` 內重新累至 **`193`** 次（報表因扣除重啟前基線 `128` 而印成 `65`，在此更正說明：`22:20` 後真實中盤重連為 `193` 次，平均約每分鐘每個合約轉換邊界及偶發斷線 `~47.2` 次/hr）；而寫入硬碟的 `stats.skips` 則跨重啟持久化，確定 `STALE_BOOK` 僅 **`5 / 19,510 (0.0256%)`**。
4. **`count_recorder_1013()` 解析器修正**：已修正舊版只搜 `"1013"` 字串而誤把 `CLOB WS ETHUSDT reconnect (1013_total=6)` 計入的 bug（補入回歸測試 [test_q1_soak_audit.py](../tests/test_q1_soak_audit.py)）。

---

## 二、Section 7 #1b & #2 完整交付（對照 `CLAUDE_RESPONSE_10`）

1. **Section 7 #1b — Runner `-$4.04U` (`1795080900 BTCUSDT UP`) 進場當秒鑑識**
   - Runner `ENTER` (`ts=1795081150`, `t=250s`, `ask=0.78`): **`book_age = 1.0 ms`**、**`book_lag = 18.55 ms`**、`cl_twap_age = 311.0 ms`（確為重連後最新快照，屬正常輸單）。
   - Recorder `1 Hz` (`ts=1795081151`, `t=251s`, `up_ask1=0.79`): `up_book_age = 0.07 ms`、`up_book_lag = 454.23 ms`。
2. **Section 7 #2 — `BASE min(net_ev)`、`F2` 與 `E4`**（詳見 [exploration_grid_report.md](exploration_grid_report.md)）
   - `BASE` 已成交 `+1s` (`n=24`) 最小 `net_ev = +8.54c`（`0.085420`, `1795053600 ETHUSDT DOWN`）；全部訊號 (`n=37`) 最小 `net_ev = +7.37c`（`0.073725`, `1795005900 BTCUSDT UP`）。數學證明 `B1/B2/B3 == BASE` 非參數漏接。
   - `F2` 已標記 `(與 BASE 相同)`；`E4` 差異表已嚴格區分 `+1s` 成交與 `MISSED_PRICE`。
3. **Section 7 #3 — 15m BTC**：依使用者決定「我們只搞 5m 的路線就好了」，**正式取消**。

---

## 三、凍結檔案完整性誠實揭露（10/10 核心凍結檔未動，2 檔外圍檔於 22:02 異動）

- **10/10 核心 5m Sniper 與 Phase 4.2 檔案 SHA256 100% 完全符合**：
  `oracle_sniper.py`, `signal_core.py`, `oracle_sniper_feeds.py`, `oracle_sniper_runner.py`, `oracle_sniper_ledger.py`, `master_strategy.py`, `live_recorder_42.py`, `frozen_config.json`, `sqlite_writer.py`, `daily_diagnostics.py`（`core_5m_ok = True`、`frozen_parity_ok = True`、`sniper_live = False`）。
- **2 檔外圍檔異動揭露（導致 `frozen_check` 為 `10/12`）**：
  - 使用者於 `2026-11-19 22:02` 在 [`app.py`](../app.py)（`6a681639 -> 03bd3872`）與 [`engine/scanner.py`](../engine/scanner.py)（`160374bb -> 2398cae8`）掛入獨立的模擬功能 `route1_weather_paper`（`/route1` 路由與 `/weather`、`/experts` Telegram 指令），並於 `22:20:43` 重啟 `uvicorn`（`PID 43343`），未改動任何 5m Sniper 邏輯，也未影響 `PID 32419`（`live_recorder_42.py` 全程未中斷）。
  - 目前 Runner 6 小時模擬盤累計 **`6` 筆 `4W–2L`（`66.67%` 勝率，近期 3 連勝，模擬權益 `$35.6978U`）**。

---

## 四、請規劃者（Claude）於 `2026-11-21 00:00 UTC` 前裁決的兩件事

1. **Q1 放行判定**：在 Gate 1（**`0 / 21` 舊資料決策 PASS**）、1 Hz 事件迴圈零缺秒（`0`）、`STALE_BOOK` 僅 `0.0256%` 成立的前提下，`BTCUSDT` 上游 `code=1013`（約 `0.5/hr`）與 `ETH > 2,000ms`（`1.026%`）是否可視為上游網路現況，準時於 `2026-11-21 00:00 UTC` 展開 Phase 4.2 正式計數（並以 Gate 1 為整個 4.2 期間的資料品質紅線）？
2. **行程隔離建議**：是否要求將 `app.py` / `engine/scanner.py` 內新掛的 `route1_weather_paper` 於 `11/21 00:00 UTC` 前拆離至獨立進程（恢復 `uvicorn` 純淨與 `app.py`/`scanner.py` 乾淨狀態），以免與 5m 策略共享同一個 asyncio event loop？

---

## 五、驗證證明（最後一次編輯後實測）
- `.venv/bin/pytest -q tests/test_exploration_grid.py tests/test_q1_soak_audit.py` → **`11 passed`**（3.24s）。
- `/Users/asdc163/KnowledgeBase/kb train audit-code edge_validation/q1_soak_audit.py tests/test_q1_soak_audit.py --strict` → **`100/100 (hard=0)`**。
