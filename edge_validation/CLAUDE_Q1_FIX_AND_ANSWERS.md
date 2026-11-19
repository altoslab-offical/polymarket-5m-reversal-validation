# Q1 修復方案、Q2/Q3 實作與規劃者問題回覆

> **策略與核心常數零變動**：`engine/signal_core.py`、`engine/oracle_sniper.py` 與 `TWAP_V2_FILTERED_CONFIG` 一字未改；`check_frozen_config_parity() == (True, [])`；`sniper_live = False`、`dry_run = True`。

---

## 一、回答規劃者追問的兩個具體問題

### 1. Q2.4：`1795068900 SOL`（240 秒）Recorder 看到 `ask = 0.75` 為何未 `ENTER`？
我們從 `snapshots_1hz` 直接重算 `evaluate_signal(inp, TWAP_V2_FILTERED_CONFIG)`，逐秒原因如下：

| `ts_sec`（`elapsed`） | Recorder 當秒狀態 | `z` | `up_ask1` | `flow` / `cvd` | 實際判定結果 |
|---|---|---:|---:|---:|---|
| `1795069139`（`239s`） | — | `+1.115` | `0.92` | `-0.186` / `-0.266` | `SKIP: Z_LOW` |
| **`1795069140`（`240s`）** | TWAP 與 Book 相位差 ~0.3s | **`+1.156`（< `1.30`）** | `0.75` | `+0.426` / `+0.608` | **`SKIP: Z_LOW`**（`signal_skips_42` 當秒紀錄一致） |
| **`1795069141`（`241s`）** | 預言機剛躍升過 `1.30` 門檻 | **`+1.340`（≥ `1.30`）** | **`0.89`（> `0.82`）** | `+0.426` / `+0.608` | **`SKIP: ASK_OUT_OF_BAND`** |

- **結論**：完全符合 **1Hz 相位差（`PHASE_1HZ`）**。`240s` 時 Runner 較晚收到該秒 Chainlink TWAP 更新後 `z` 已達 `1.328`、同時盤口尚在 `0.81`，所以 Runner 觸發了 `ENTER`（但 `+1s` 同樣因跳到 `0.89` 而 `PAPER_MISSED_PRICE` 未成交）；Recorder 在 `240.05s` 取樣瞬間 Chainlink `z` 只有 `1.156` 判 `Z_LOW`，隔 1 秒 `241.05s` `z` 升到 `1.340` 時賣價已跳上 `0.89` 被價帶上限拒絕。

### 2. Q3：先前蒙地卡羅「日均 `45` 單」出處 ＋ 依實際 `10–20` 單/天重算結果
- **「日均 45 單」的原始出處（已於實測修正）**：
  - 前版是用 `phase42_trades_1s_v2_filtered.csv` 的 **`23` 筆模擬成交 ÷ 非暖機活躍時數 `12.38h` ≈ `1.86 單/h`（換算約 `44.6 單/天`）**。
  - 事後剔除 `window_ts < 1795020961` 舊污染視窗，加上今日 `4.5h`（`270 分`）乾淨實戰統計（`4` 次 `ENTER` / `2` 筆成交，即約 `10–20 單/天`），原先的 `45 單/天` **高估了約 2–4 倍**。
- **改用實際頻率 `10 / 15 / 20` 筆成交/天重算（`$40U → $10,000U` 2,000 條路徑、`$4` 最小下單、`$250` 綜合深度上限、均價 `$0.805`、止損虧 `75%`）**：

| 每日成交頻率 | 假設真實勝率 (`EV/stake`) | 倉位 % / 深度上限 | 365 天達 `$10k` 機率 | 達標中位天數 (`P90`) | 中位最大回撤 |
|---|---|---:|---:|---:|---:|
| **`10 單/天`** | `82%` (`+6.4%`) | `5%` / `$250` | `100.0%` | `183.1 天` (`219.0`) | `28.8%` |
| **`10 單/天`** | **`85%` (`+9.3%`)** | `5%` / `$250` | `100.0%` | **`123.4 天`** (`143.0`) | `20.5%` |
| **`10 單/天`** | `87%` (`+11.3%`) | `10%` / `$250` | `100.0%` | `65.1 天` (`75.7`) | `25.5%` |
| **`15 單/天`** | `82%` (`+6.4%`) | `5%` / `$250` | `100.0%` | `121.9 天` (`146.3`) | `28.7%` |
| **`15 單/天`** | **`85%` (`+9.3%`)** | `5%` / `$250` | `100.0%` | **`82.6 天`** (`95.0`) | `20.6%` |
| **`15 單/天`** | `87%` (`+11.3%`) | `10%` / `$250` | `100.0%` | `43.1 天` (`50.1`) | `25.5%` |
| **`20 單/天`** | `82%` (`+6.4%`) | `5%` / `$250` | `99.9%` | `91.5 天` (`110.3`) | `28.6%` |
| **`20 單/天`** | **`85%` (`+9.3%`)** | `5%` / `$250` | `100.0%` | **`61.5 天`** (`71.1`) | `20.4%` |
| **`20 單/天`** | `87%` (`+11.3%`) | `10%` / `$250` | `100.0%` | `32.4 天` (`38.0`) | `25.5%` |

---

## 二、Q1 修復實作摘要（消除 `1013 slow consumer`）

實測 Polymarket CLOB WebSocket 在熱盤口下光是 `BTCUSDT` 就達 **`681 msgs/sec`**（合併 6 個 token 共 ~`750 msgs/sec`），同時 Runner 也有記錄 `clob_reconnects = 19` 的 `1013` 斷線（代表共享連線＋預設 `websockets(..., max_queue=64, compression="deflate")` 兩支程式都會卡隊列）。本機基準實測改用 **三幣獨立連線＋關閉 WS per-message-deflate 壓縮（`aiohttp`, `compress=0`）**：
- `BTCUSDT`：`681.0 msgs/s`, `lag P99 = 303 ms`
- `ETHUSDT`：`38.7 msgs/s`, `lag P99 = 39.7 ms`
- `SOLUSDT`：`27.4 msgs/s`, `lag P99 = 36.2 ms`

因此本次 Q1 落地兩項純傳輸與 I/O 隔離修復：
1. **獨立背景 SQLite 寫入執行緒（`edge_validation/sqlite_writer.py`）**：`AsyncSQLiteWriter`（`WAL` + `synchronous=NORMAL`）以背景執行緒消費寫入佇列，`LiveRecorder42` 所有 1Hz `INSERT`、`COMMIT` 與結算查詢（`asyncio.to_thread(query_pending_windows)`）完全不阻塞 asyncio WebSocket 迴圈；單元測試呼叫 `_record_second` 時若未啟動背景 writer 仍安全退回同步 `self.con`。
2. **三幣獨立非阻塞 `aiohttp` CLOB WebSocket（`live_recorder_42.py` ＋ `oracle_sniper_feeds.py`）**：
   - 每個幣種各自維持 1 條訂閱僅 2 個 token 的獨立 WS 連線（隔離 BTC 爆量洪流），使用專屬無 `total=3s` 逾時的 `aiohttp.ClientSession(trust_env=False)`（`compress=0`, `max_msg_size=0`, `heartbeat=15.0`, `autoping=True`），換窗或持續陳舊 `>=1.5s` 時主動立即 `ws.close()` 重訂。
   - Binance WS 同步設定 `compression=None, max_queue=4096`。
   - 明確追蹤 `close_code == 1013` 次數（`self._clob_1013_count` / `stats["clob_1013_count"]`）。

---

## 三、Q2 與 Q3 日報升級（`edge_validation/daily_diagnostics.py`）
- **Q2 兩層 Live Shadow Parity**：
  - 主指標：同窗、同幣、同方向、時間差 `|Δts| <= 2s` 為一致，分母為兩邊 `ENTER` 聯集，門檻 **`>= 95%`**，且於 **`runner_enters >= 20`** 時自動切換為 `PASS`/`FAIL`（未滿 20 筆標記 `PENDING (<20)`）。
  - 輔助指標：配對樣本的 `|Δask|` P50/P90、`|Δz|` P50/P90；所有失配逐筆歸因分類為 **`CLOB_STALL`**（盤口卡住：`rec_book_lag_ms > 500` 或 `age > 2.5s` 或 `STALE_BOOK`）、**`PHASE_1HZ`**（1Hz 相位差）或 **`OTHER`**。
- **Q3 三張只讀診斷表**（不計入 3 組 Bonferroni 主檢定）：
  1. 進場軌跡：`prev_5s_max_ask > 0.825` vs `Other`
  2. 進場前 3 秒 Binance 方向：`Aligned/Flat (>=0)` vs `Adverse (<0)`
  3. 分幣別績效：`BTCUSDT` / `ETHUSDT` / `SOLUSDT` 各自訊號數、成交數、勝率、+1s / Hold 每股 PnL 與 `EV / Stake %`。

---

## 四、上線前驗證與 6 小時驗收計畫
- `kb train audit-code --strict`：`sqlite_writer.py`、`daily_diagnostics.py`、`live_recorder_42.py`、`oracle_sniper_feeds.py`、`verify_signal_core_and_parity.py` 均 **`100/100`**。
- **離線 `parity_replay.py`（全量 `260,433` 列 / `86,811` 秒）**：逐秒一致率 **`99.92%`（`260,225 / 260,433 >= 99.5%`）**、`ENTER` 逐秒 **`158 / 158 (100.0%)`**、幣窗 **`41 / 41 (100.0%)`**、單槽 **`35 / 35 (100.0%)`**。
- 推進 GitHub 後已清空 `$40U` 模擬狀態重新凍結 `count_start_window_ts`，並啟動 **連續 `>= 6` 小時穩定性測試**驗收：
  1. `1013` 斷線 `= 0`
  2. `up_book_lag_ms` P99 `< 500 ms`、`> 2,000 ms` 秒數 `= 0`
  3. 逐秒 `parity_replay` 一致率 `>= 99.5%`
