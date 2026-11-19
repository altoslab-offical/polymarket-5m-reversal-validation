# 給規劃者（Claude）的 11/21 前審查回覆與實證加固報告

> 對應 `b847d3e` / `7b8cf00` 審查意見（第 1–6 節）之實測根因、程式碼修正、全量回歸驗證與明細檔案。  
> **Zero strategy parameter changes**（`check_frozen_config_parity == (True, [])`）。

---

## 第 1 節：`STALE_BOOK` 一致率 81.4% 原因查明 → 已修至 **100.00% (`221/221`)**

- **完整明細 CSV**：[`edge_validation/stale_book_41_details.csv`](./stale_book_41_details.csv)（共 42 列：`41` 列 Recorder=`STALE_BOOK` + `1` 列早期無 ms 欄位 Row `1795009500` Runner=`STALE_BOOK`）。
- **時間分佈（100% 在換窗後 0–5 秒，150–255s 進場窗為 0 筆）**：
  - `elapsed = 0s`: `1` 筆（legacy row `1795009500`，Recorder 未清換窗前 `up_bk`/`dn_bk`）
  - `elapsed = 1s`: `9` 筆、`2s`: `14` 筆、`3s`: `11` 筆、`4s`: `6` 筆、`5s`: `1` 筆（Recorder=`STALE_BOOK`, Runner=`NO_STRIKE`: `13` 筆 / `OUTSIDE_ENTRY_WINDOW`: `28` 筆）
- **雙重根因（並非 age 公式差異，而是兩邊換窗同步＋空盤口重播缺口）**：
  1. **線上 Recorder 換窗比 Runner 晚 1–5 秒**：`SniperMarketHub.roll_window()` 在 `elapsed = 0` 同步清盤口與重設 age；舊版 `LiveRecorder42` 的 `reset_window()` 放在非同步 `_meta_and_settlement_loop`（2 秒輪詢），導致 `elapsed = 0` 偶爾錄入上一窗殘餘時間戳，直到 `elapsed = 1..5s` 才變空盤口與 `age = 9999.0`。
  2. **離線重播 `parity_replay.py` 空盤口沒清空 Runner**：`_apply_side_books()` 遇到 `up_b == [] and up_a == []` 時直接略過，使重播期間的 `Runner` 仍持有 `elapsed = 0` 的舊 `up_recv_mono_ms`（`run_book_age = 1.00–3.48s` vs `rec_book_age = 9999.0`）；同時 legacy fallback `_row_ages()` 也沒在 `window_ts` 切換時清除 `up_bk` / `dn_bk`。
- **修正**：
  - 在 `LiveRecorder42._record_second()` 入口新增 `_roll_window_if_needed(cur_t)`，與 `Runner` 同步在 `elapsed = 0` 清空盤口、adopt prefetched tokens 並觸發 CLOB 重訂。
  - 在 `live_recorder_42.py::_row_ages` 於 `window_ts` 切換時清除 `up_bk` / `dn_bk`。
  - 在 `parity_replay.py::_apply_side_books` 於 DB `book_age >= 9000` 且盤口為空時同步清空 Runner 該側狀態與 `recv_mono_ms`。
  - **修復後實測（[`parity_replay_report.md`](./parity_replay_report.md)）**：`SKIP:STALE_BOOK` **`221 / 221`（100.0000%）**；整體逐秒全決策一致率 **`99.8984%`（`201,527 / 201,732`）**；`ENTER` **`155 / 155`（100.00%）**、單幣種 **`38 / 38`**、單槽 **`32 / 32`**。

---

## 第 2 節：本機時鐘、Polymarket CLOB 26s 卡頓與 Binance `bn_lag_ms` 實測查證

### 2.1 本機時鐘量測（`timed` 活躍運行中，無 root 權限執行 `sudo sntp -sS`）
- `system/com.apple.timed`（macOS launchd `PID 138`）保持運行中；兩次量測結果：
  - 初期：`sntp time.apple.com` = `+0.116289 +/- 0.079556` 秒、`sntp pool.ntp.org` = `+0.113205 +/- 0.078218` 秒。
  - 稍後收斂：`sntp time.apple.com` = **`+0.070450 +/- 0.068493`** 秒、`sntp pool.ntp.org` = **`+0.071113 +/- 0.043629`** 秒。
  - Binance `GET /api/v3/time` 5 次採樣：`RTT = 194.0–249.8 ms`，`local_mid - server = -162.3 ~ -187.4 ms`（非同步權限下 `sudo -n` 需要本機密碼）。建議使用者若要壓至 `< 50 ms`，可在終端執行一次 `sudo sntp -sS time.apple.com`。

### 2.2 Polymarket CLOB `src` 語意實測與 **26 秒卡頓重大 Bug 修復**
- **獨立 6-token CLOB WS 基準測試（8 秒、`4,095` 筆、`511.9 msg/s`）**：
  - `book` 與 `price_change` 的 `timestamp` 皆為**真實伺服器事件送出時間**（非最後盤口變動時間）：
    - `book` (`n=62`): `P50 = 47.5 ms`, `P90 = 164.2 ms`, `Max = 217.5 ms`
    - `price_change` (`n=4,005`): `P50 = 40.6 ms`, `P90 = 52.3 ms`, `Max = 250.4 ms`
    - 整體：`P50 = 40.6 ms`, `P90 = 52.6 ms`, `P99 = 204.6 ms`, `Max = 250.4 ms`（`REST /book`: `14.8 ms`）。
- **為何線上 `live_recorder_42.py` 在 `b847d3e` 後卻出現 `P90 = 6.45s`、`Max = 26.28s`**？
  1. `live_recorder_42.py` 在每秒 ~512 次 `price_change` 上每次執行整張盤口 `sorted(items())`，配合 `max_queue=4096` 導致 event loop 消費落後，WS queue 堆積逾 `2,500 ms`。
  2. 一旦超過 `2,500 ms`，`_apply_price_change` 與 `_handle_clob_msg` 只「靜默丟包」卻沒觸發重連（實測 `ts=1795055895..1795055917` 連卡 23 秒），期間全靠 `REST /book` 每 2.5–3s 補洞，而 `via_ws=False` 更新了 `recv_wall_ms` 卻保留凍結的舊 `up_source_ts_ms`，同時造成 **3–5 秒盤口凍結**＋**26.3 秒假延遲**！
- **修復**：
  - **`LiveRecorder42`**：`CoinState42` 改為 `O(1)` 字典更新＋dirty-flag 懶排序（只在 1Hz 快照/評估讀取時排序一次）；`SniperMarketHub` 同步將 `apply_book` / `apply_price_change` 改為 `max()` / `min()`。
  - **反背壓＋斷滯強制重連**：雙方均改 `max_queue=64`，一旦連續 `>= 1.5s` 收到落後 `> 2.5s` 的影格即主動強制重連 CLOB WS；另 `init_db` 明確開啟 `PRAGMA journal_mode=WAL` 與 `synchronous=NORMAL`。
  - **禁止跨來源時間相減**：`up_last_lag_ms` / `dn_last_lag_ms` 僅在 WS (`via_ws=True`) 收到有效 `src_ms` 當下計算儲存，REST fallback 絕不再污染 WS `lag_ms`。

### 2.3 Binance `bn_lag_ms` P50 = 378ms / P99 = 5,065ms 查證與修復
- Live Binance WS payload 實測：`@aggTrade` 帶事件時間 `E`，但 **`@bookTicker` keys 僅為 `['A','B','a','b','s','u']`（沒有 `E` 或 `T`）**。
- 舊程式（Recorder 與 Runner）在收到 `@bookTicker` 時更新了 `bn_recv_wall_ms`，卻延用上一筆 `@aggTrade` 的 `bn_source_ts_ms`，導致靜默秒（`bn_trades = 0`）把「距上一筆成交過了幾秒」計入 `bn_lag_ms`（歷史實測：`bn_trades > 0` `P50 = 59.0 ms`, `Max = 972.2 ms` vs `bn_trades == 0` `P50 = 918.3 ms`, `P99 = 5,918.8 ms`）。
- **修復**：雙方皆改為只在收到帶有 `src_ms > 0`（`@aggTrade`）訊息當下計算並鎖存 `bn_last_lag_ms`，不再被 `@bookTicker` 相減污染。

---

## 第 3 節：Chainlink `cl_twap_age` 分組績效表

- 已於 `live_recorder_42.py` 日報（[`phase42_daily_report.md`](./phase42_daily_report.md)）新增 `## Chainlink TWAP Age vs Filled PnL (+1s, v2_filtered)` 表，固定輸出 **`< 0.5s`**、**`0.5–1.5s`**、**`> 1.5s`** 三組的 `Filled Trades`、`Win Rate`、`Mean PnL +1s`、`Mean Hold PnL`（`post_reset` 與 `all` 雙視角）。

---

## 第 4 節：即時影子比對（`Live Shadow Parity`）＋ 修復重播腳本污染漏洞

- 說明：`10:33AM` 重置並清空 `runner_shadow_events.jsonl` 時，`RUNNER` 處於 `0/1800s` 暖機期（`10:43AM` 已達 `1485/1800s`），因此當時日報 overlap 尚為 `0`。
- **額外抓到並修掉的重大測試污染 Bug**：`parity_replay.py::_make_replay_runner` 原繼承了 `PAPER_40U_EXEC_POLICY.live_shadow_log_path`（未傳 `None`），每次離線重播都會意外將歷史 `ENTER` / `WARM_WINDOW` 追加寫進線上的 `data/validation_42/runner_shadow_events.jsonl`！已於本次明確設為 `live_shadow_log_path=None`，並在 `check_16` 加入鎖定斷言。

---

## 第 5 節：`feed_alive` 沿用前值秒數比例監控表

- 已於日報新增 `## Binance 1s Volatility Carry-Forward Ratio (feed_alive Monitor)` 逐幣與總計監控表，同時列出：
  - **Zero-Trade Carry (`bn_trades = 0 & bn_age <= 5s`)**：即全部零成交補值秒數（Post-055700 626 秒實測為 BTC `14.1%` / ETH `32.9%` / SOL `43.9%`，超過 `5%` 門檻會標註 **`ALERT (>5%)`**）。
  - **Extended `>5s` Carry**：僅因 `feed_alive` 新增保活的連續 `>5s` 靜默秒（實測 BTC `0.00%` / ETH `0.16%` / SOL `0.48%`，max streak `6s`）。
- **數學依據附註**：`compute_sigma_from_vol_hist` 採 `max(sample_var, ewma_var)`，零成交補值後於下筆成交補回累積多秒報酬時，`sample_var` 對真實 1s 擴散變異數保持無偏，且可防止 EWMA 在靜默段低估 σ。

---

## 第 6 節：本次合併之全量驗證摘要（未改任何策略參數）

- **`kb train audit-code --strict`（13 支檔案全過，`hard=0`）**：`oracle_sniper.py` `95/100`，其餘 12 檔（含 `signal_core.py`、`oracle_sniper_feeds.py`、`oracle_sniper_ledger.py`、`oracle_sniper_notify.py`、`oracle_sniper_runner.py`、`notifier.py`、`live_recorder_42.py`、`parity_replay.py` 等）皆 **`100/100`**。
- **全部 5 套測試套件 100% 通過**：
  - `verify_section3_guards.py` (`groups_passed: 6`)
  - `verify_v8_e2e.py` (`assertions_groups_passed: 28`)
  - `verify_v11_live_parity.py` (`groups_passed: 7`)
  - `verify_oracle_sniper.py` (`groups_passed: 12`，含 1,007 視窗歷史重播與新版戰術 Telegram 通知卡)
  - `verify_signal_core_and_parity.py` (`groups_passed: 16`，含新增 `check_14`–`check_16` 與完整歷史資料庫重播)
- 本次同步啟用新版戰術終端機 Telegram 通知卡（區分實盤與 `🧪 模擬盤 $40U`），`sniper_live` 全程維持 `False`，`count_start_window_ts` 重鎖為部署後的下一個 5 分鐘邊界。
