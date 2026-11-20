# `CLAUDE_RESPONSE_11` 開跑前 4 項交回與 GO 確認（Phase 4.2 Ready）

> **時間**：`2026-11-20 10:26 +08:00`（`02:26 UTC`，距 `2026-11-21 00:00 UTC` 正式起跑尚餘約 21.5 小時）  
> **總結**：`CLAUDE_RESPONSE_11` 規定之 4 項開跑前待辦與常設紅線已 **100% 完成並實測通過**，主機現維持 100% 純淨 5m 雙行程靜置待命。

---

## 一、`CLAUDE_RESPONSE_11` 規定之 4 項開跑前交回清單

| 交回項目 | 實測結果 | 證據來源 |
|---|---|---|
| **1. 凍結指紋 `12/12`** | **`12 / 12` 完全吻合 (`ok = True`, `mismatches = []`)** | `app.py` (`6a681639075e5a7f`) 與 `engine/scanner.py` (`160374bb1e8136df`) 已精確還原；[Q1_POST_RESTART_30M.md](Q1_POST_RESTART_30M.md) |
| **2. Runner 新 PID 與旗標（4.2 前最後一次重啟）** | **Runner `PID 62060`（Port `8088`）**、`running = True`、`enabled = True`、`frozen_parity_ok = True`、`sniper_live = False`、`has_route1_in_8088 = False` | `/api/sniper/status` & `/api/status`（`started 09:49:54 +08:00`，已連跑 `36m+`） |
| **3. 重啟後 36 分鐘（含 30m 暖機＋主動評估）表現** | `STALE_BOOK` = **`0 / 315` (`0.0000%`)**｜舊資料決策 = **`0`**（Runner `0/0` / Recorder `0/0`）｜`1013` 增量 = **`0` / `0`**｜重連 `19` 次｜BTC `[150,255]s age>1.5s = 2.156%` (`< 10%`) | [Q1_POST_RESTART_30M.md](Q1_POST_RESTART_30M.md)（`2,192s`，`0` 缺秒，`P50 = 42.0–46.7ms`） |
| **4. 天氣功能獨立拆離＋暫停證明** | 獨立入口 [route1_app.py](../route1_app.py)（Port **`8089`**）實證 **0 個 5m import**；`09:50` 實機驗證 `PID 62059` (`8089`, `isolated_from_5m_sniper=True`) vs `PID 62060` (`8088`)；`09:52` 依使用者指示暫停 `PID 62059` 全心專注 5m | [test_route1_isolation.py](../tests/test_route1_isolation.py) & 實機 `lsof` / `pgrep` |

### 補充行程清單（目前本機運行中，Recorder 全程未重啟）
- **4.2 Recorder（`live_recorder_42.py`）**：**`PID 32419`**（自 `2026-11-19 16:47:38 +08:00` 連續運行 **`17h 38m+`**，全程未重啟，1 Hz 迴圈 0 缺秒）。
- **5m Runner（`uvicorn app:app`）**：**`PID 62060`**（Port `8088`，`started 09:49:54 +08:00`，`running=True`, `frozen_parity_ok=True`, `sniper_live=False`）。
- **Route 1 Weather（`route1_app.py`）**：已於 `09:52` 主動停止（Port `8089` 空置，釋放全數資源給 5m 系統）。

---

## 二、4.2 期間常設紅線（已寫入每日報表最頂端）

已更新 [run_daily_bundle.py](run_daily_bundle.py)（新增 `build_standing_redline_block()` 與 `_prepend_redline_block()`），每日離峰（`nice -n 19`）產出報表時自動於檔案**最頂端**強制顯示：

1. **舊資料決策 (`lag > 1000ms` 或 `age > 1500ms`)**：必須 `= 0`；一旦出現 `>= 1` 自動標為 **`INVALID — PAUSE RUNNER & INVESTIGATE (Recorder 續錄)`**。
2. **凍結指紋**：必須 `= 12/12 (ok=True)`。
3. **只觀察指標**：`1013` 次數、各幣 `>2000ms` 比例、Runner 重連數；**BTC 進場 `[150,255]s` `age > 1.5s > 10.0%` 時自動亮紅警 (`ALERT > 10%`)**。
4. **執行與判定鎖定**：禁在主機併發重運算（離線固定時段 `nice -n 19`）、3 組預註冊（`v2_pure` / `v2_filtered` / `v1`）、Bonferroni `98.33%`、`window_ts >= 1795219200`、`n >= 300` 上線 / Day-10 fail-only 提早停損、`sniper_live = False`。

---

## 三、測試與品質驗證（最後一次編輯後實測）
- `.venv/bin/pytest -q tests/test_exploration_grid.py tests/test_q1_soak_audit.py tests/test_route1_isolation.py` → **`14 passed`**（3.41s）。
- `audit-code --strict`（`route1_app.py`、`run_daily_bundle.py`、`test_route1_isolation.py`、`test_exploration_grid.py`）→ 全數 **`100/100 (hard=0)`**。
- `check_frozen_hashes()` → **`12/12 (ok=True)`**。
