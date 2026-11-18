# TWAP Sniper 4 單 (`1W–3L`) 逐筆鑑識報告

- **生成時間**: `2026-11-18 23:40 CST` (`2026-11-18T15:40:00Z`)
- **交易停機與真錢保護狀態**:
  - `OracleSniperRunner.enabled = False`（已停機）
  - `PolymarketClobConfig.sniper_live = False`、`dry_run = True`、`paper_simulation = True`（已禁止真錢下單）
  - Section 4.2 只讀錄製器 (`edge_validation/live_recorder_42.py`) 持續運行中
  - **重要事實釐清（資金屬性與實際筆數）**:
    1. 該批 `1W–3L` 共 4 單出自 `$43U` 模擬帳本 (`data/oracle_sniper_paper_43u.json`, `is_real_money=False`, `paper_mode=True`)，並於 `21:18:33 CST` 另有第 5 筆模擬單 (`1795007700-SOLUSDT-DOWN`, `+$0.9135U`, 累計 `2W–3L`, 餘額 `$35.3218U`)；全期間最多僅 **2 連敗**（`#1`, `#2`），未達真實 3 連敗門檻。
    2. 實盤錢包 (`0x6457dFDEb4AB5E1d885f52faaA61dD8b154F441b`) 經 Polymarket Data API `v2/trades` 與 `data/oracle_sniper_state.json` 驗證，僅於 `2026-11-18 09:48:34 CST` 執行過 **1 筆真實鏈上 TWAP 單**（`1794966300-SOLUSDT-DOWN`, `5.3247` 股 `@ 0.77`, `EARLY_TAKE_PROFIT @ 0.9794`, 實賺 `+$1.0369U`）。
- **交付檔案**:
  - 逐筆總表 CSV: [`edge_validation/live_4trades_forensics.csv`](live_4trades_forensics.csv)
  - 3 筆虧損單逐秒 TWAP 路徑 CSV:
    - [`edge_validation/twap_path_1794998100_ETHUSDT.csv`](twap_path_1794998100_ETHUSDT.csv) (`101` rows)
    - [`edge_validation/twap_path_1795000200_BTCUSDT.csv`](twap_path_1795000200_BTCUSDT.csv) (`139` rows)
    - [`edge_validation/twap_path_1795005900_ETHUSDT.csv`](twap_path_1795005900_ETHUSDT.csv) (`61` rows)

---

## 逐題鑑識回答 (Q1–Q8)

### Q1. `strike_used` 與 `gamma_priceToBeat` 差多少？有沒有任何一單用了 spot 或 `BOOK_CALIBRATED`，而不是官方 TWAP？

| # | Window / Coin | `strike_used` (帳本) | `gamma_priceToBeat` | 差值 (`strike_used - ptb`) | 來源判定 |
|---|---|---|---|---|---|
| 1 | `1794998100` ETH | `1798.2084432700199` | `1798.2084432700199` | `0.0000` (結算時被 Gamma 覆寫；當下 RTDS TWAP `T=1798.1993` 差 `-0.0091`) | 進場未存原始 strike；`_settle` 以 Gamma 覆寫 (`POLY_TWAP_60S_OVERWRITTEN_BY_GAMMA_AT_SETTLE`) |
| 2 | `1795000200` BTC | `None` (`null`) | `64097.50686319119` | 帳本缺失 (`null`) | 觸發於 `19:12:40 CST`（早於 `19:14:38` 進場寫入 `strike` 之修補程式），又因提前 `EMERGENCY_STOP_LOSS` 未經過 `_settle`，致帳本遺留 `null`；執行時由 `_rtds_open` 取得 60s TWAP (`snapshots_1hz` 邊界 `T=0..1s` 為 `64097.8749` / `64097.5069`) |
| 3 | `1795003200` BTC | `64157.21416425121` | `64157.21416425121` | `0.0000` (結算覆寫) | `_settle` 以 Gamma `priceToBeat` 覆寫 (`POLY_TWAP_60S_OVERWRITTEN_BY_GAMMA_AT_SETTLE`) |
| 4 | `1795005900` ETH | **`1789.9600431820045`** | **`1790.0684710677037`** | **`-0.1084278857` (`-0.6057 bps`)** | **Confirmed Bug**: 數值精準等於 `ts_sec = 1795005899`（**`T - 1s`** 上一窗口第 `299` 秒）之 `cl_twap60`！ |

- **是否使用 spot / `BOOK_CALIBRATED`**：**沒有**（`OracleSniperRunner` 程式碼中完全不存在 `BOOK_CALIBRATED`，且 RTDS 均收到 `twap_ticks` 故採用 60s TWAP）。
- **Q1 根因 (`engine/oracle_sniper_runner.py:_rtds_open` + `engine/chainlink_oracle.py:_record_bucket_open`)**：`_rtds_open` 允許 `-1000.0 <= d <= 4000.0` ms 並在 `T + 0.1s` 首次找到任何刻度（即 `T - 1s`）後，**立即於第 239 行永久鎖定 `self._open_cache[key] = best`**，導致 `1s` 後到達的真正 `T >= window_ts` 刻度 (`1790.0684710677037`) 完全無法覆蓋更新。第 4 單 ETH UP 的 `z` 因此被人工膨脹（`1789.9600` 使報酬 `+5.77 bps, z=1.3037`；若改用真正 `1790.0685`，報酬僅 `+5.16 bps, z=1.167 < 1.30`），**根本不應進場**。

---

### Q2. 訊號當下 `cl_twap_age_sec` 與 `binance_age_sec`？新鮮度上限（3s / 2s）是否實作並生效？

- **逐筆實測值紀錄情形**: 紙上帳本與 `snapshots_1hz` 均未持久化每筆訊號當下的原始 `cl_twap_age_sec` 與 `binance_age_sec`（無法回溯精確毫秒數；CSV 標記為 `UNLOGGED(<=3.0s_GATE_PASSED)` / `UNLOGGED(<=2.0s_GATE_PASSED)`）。
- **程式碼檢查結果**：
  1. **已實作並強制攔截的部分**：`OracleSniperRunner._entry_feed_block_reason()`（`engine/oracle_sniper_runner.py:272-285`）在 `PAPER_43U_EXEC_POLICY` 啟用了：
     - `require_rtds_connected = True`
     - `require_twap_fresh = True`, `max_chainlink_age_sec = 3.0`（且 `oracle_sniper_paper_43u.json` 的 `skips` 計數確實出現 **`CHAINLINK_TWAP_STALE: 416`**，證實 3.0 秒 TWAP 阻斷已真實生效）
     - `max_binance_age_sec = 2.0`（透過 `now - st.binance_ts > 2.0` 檢查）
  2. **抓出的 3 個資料新鮮度／過期缺口（Bugs）**：
     - **缺口 A (`cl_spot` 年齡放寬至 `4.5s`)**：`_build_views()`（第 289 行）原寫 `cl_lim = max(CHAINLINK_MAX_AGE_SEC, self.policy.max_chainlink_age_sec)`，使輸入模型算 `Z` 的 `cl_spot` 最多可容忍 **4.5 秒**舊資料（超過 3.0 秒上限）。
     - **缺口 B (`_clob_ws_pump` 丟棄 `price_change` 增量事件 — 最致命實盤級 Bug)**：`engine/oracle_sniper_feeds.py:414` 僅處理 `event_type == "book"`，**完全忽略 Polymarket CLOB WS 的 `event_type == "price_change"` 增量流**；同時第 411 行又在收到 `price_change` 時重設 `last_msg = time.time()` 導致 WS 不會觸發 12 秒斷線重連，且僅每 `1.2s` 在 `silent >= 2.5s` 後抓 REST。結果 `OracleSniperRunner` 看到的盤口滯後 **6¢–10¢**（見 Q4/Q8）。
     - **缺口 C (`_paper_buy` 過期盤口漏寫 `return`)**：`engine/oracle_sniper_runner.py:446-449` 判斷了 `(time.time() - side_ts) > BOOK_MAX_AGE_SEC`，但內層僅判斷 `if live_ask is None: return ...`，非 `None` 的過期盤口仍直接成交。

---

### Q3. `token_id_bought` 與 `side` 是否一致？有無 Up/Down 對調或方向寫反？

**100% 一致，零對調、零正負號錯誤**：
1. `1794998100` ETH `DOWN` -> `74393893...0766363` = Gamma / `windows_42.down_token_id` (**`Down`**)
2. `1795000200` BTC `DOWN` -> `18730424...92378` = Gamma / `windows_42.down_token_id` (**`Down`**)
3. `1795003200` BTC `DOWN` -> `38927184...612548` = Gamma / `windows_42.down_token_id` (**`Down`**)
4. `1795005900` ETH `UP` -> `44523901...026487` = Gamma / `windows_42.up_token_id` (**`Up`**)

---

### Q4. 訊號到成交延遲、`fill_px − ask_at_signal` 與吃檔深度檢視

模擬設定 `paper_fill_delay_sec = 1.0`；實測延遲 `1000.2 ms – 1002.2 ms`（`~1.00s`）。對照 `OracleSniperRunner` 內部 `sim_ask` 與同時刻 `live_3coin_42.db` 真實 1Hz 盤口：

| # | Window / Coin / Side | 延遲 (ms) | Runner `sim_ask` | 真實 DB `ask1` (`sz`) @ T0 / T+1 | `fill_px` | `fill_px - sim_ask` | `fill_px - live_db_ask` | 異常說明 |
|---|---|---|---|---|---|---|---|---|
| 1 | `1794998100` ETH `DOWN` | `1000.9` | `0.8200` | **`0.88` (`4.96`)** / `0.88` (`4.96`) | `0.8200` | `0.0000` | **`-0.0600`** | 真實盤口為 **`0.88 > 0.82`** 且僅 `4.96 < 5` 股，Runner 因漏吃 `price_change` 滯留假價 `0.82` |
| 2 | `1795000200` BTC `DOWN` | `1000.2` | `0.7910` | **`0.85` (`3078.22`)** / `0.85` (`23.14`) | `0.7900` | `-0.0010` | **`-0.0600`** | `159s` 盤口已跳升至 `0.85 > 0.82`，Runner 仍見 `0.791` |
| 3 | `1795003200` BTC `DOWN` | `1002.2` | `0.8100` | **`0.85` (`85.0`)** / `0.85` (`85.0`) | `0.8100` | `0.0000` | **`-0.0400`** | Runner 見滯後盤口 `0.81 <= 0.82` 提早於 `163s` 進場（真實盤口當時 `0.85`） |
| 4 | `1795005900` ETH `UP` | `1002.1` | `0.8151` | **`0.81` (`20.0`)** / `0.81` (`35.0`) | `0.7937` | `-0.0214` | `-0.0163` | 唯一盤口貼近之單（`sim_ask=0.8151`, `db_ask1=0.81`, `pm_trades` 同時段有 `0.78–0.80` 成交） |

- 結論：模擬器僅讀頂檔／VWAP (`target_usdc=25`) 模擬 `0` 額外掃檔（並未上吃到第 2 檔實單），反而是 **CLOB WS 漏接 `price_change`** 導致 Runner 在 #1/#2/#3 全以比真實盤口低 **4¢–6¢** 的過期價通過 `max_entry_price = 0.82` 門檻。

---

### Q5. 實盤參數與 `frozen_config.json` 逐欄 diff

1. **`SniperConfig` 本體 (`TWAP_V2_FILTERED_CONFIG` vs `frozen_config.json["sniper_config_v2_filtered"]`)**：
   呼叫 `check_frozen_config_parity(TWAP_V2_FILTERED_CONFIG, "edge_validation/frozen_config.json", "v2_filtered")` 回傳 **`(True, [])`**，所有策略超參數 **100% 一致**：
   - `min_z_score=1.30`, `min_true_prob=0.88`, `min_entry_price=0.68`, `max_entry_price=0.82`, `max_entry_spread=0.06`, `min_net_ev_per_share=0.035`, `entry_start_elapsed_sec=150.0`, `entry_end_elapsed_sec=255.0`, `student_t_df=6.0`, `vol_lookback_sec=1800`, `min_1s_vol_floor=0.000035`, `max_model_market_divergence=0.30`, `use_twap_projection=True`, `twap_gaussian_weight=1.0`, `twap_student_t_df=6.0`, `use_flow_gates=False`, `early_tp_enabled=False`, `enable_emergency_exit=True`, `stop_loss_z_score=-0.35`, `risk_fraction=0.05`, `min_shares=5.0`, `slippage_ladder=(0.01, 0.02, 0.03, 0.04)`。
2. **執行層 / 風險層政策與 `frozen_config.json` 差異**：
   - **`per_symbol_fees`**: `PAPER_43U_EXEC_POLICY` 採用 `{"BTCUSDT": 0.05, "ETHUSDT": 0.01, "SOLUSDT": 0.01}`（對應 `frozen_config["effective_fees_post_nov20"]`），而非 `sniper_config_v2_filtered.fee_rate = 0.07`（Nov 20 前舊費率；註：`live_recorder_42.py` 亦已同步採用 `0.05/0.01/0.01`）。
   - **`max_concurrent_positions`**: 實盤/模擬 Runner 為 **`3`**（一幣最多一單可並發），而 4.2 回放評估預設為單槽邏輯。
   - **`pilot_fixed_shares`**: `PAPER_43U_EXEC_POLICY` 為 **`None`**（於 `$43U` 資金規模下 `5% * 43 / cost < 5` 自動進位/下限為 `5.0` 股，實測 4 單均為 `5.0` 股，等價於 `bankroll_gates.pilot_fixed_shares=5`）。
   - **`vol_1s` 輸入來源差異（關鍵實作漂移）**：`Runner._build_views()` 餵給 `RollingMicrostructureTracker` 的價格是 **`0.55 * Coinbase_USD + 0.45 * Binance_USDT_adj`** 加上 sub-second `aggTrade` 更新，而 `live_recorder_42.py` 餵給 `tracker` 的是 **1Hz `bn_close`（純 Binance）**，加上 `19:15:19 CST` 伺服器重啟清空 1800s 波動率歷史，導致 Runner `implied_sigma`（#2 `5.25e-5`, #3 `4.13e-5`）明顯低於錄製器（`7.57e-5`, `5.03e-5`）進而過度放大 `Z`。

---

### Q6. 輸的 3 單從訊號到結算的 Chainlink TWAP 路徑分析（現價反轉 vs 一開始判斷就錯？）

| # | Window / Coin / Side | 訊號當下狀態 (`el`, `cl_spot`, `cl_twap60` vs `gamma_ptb`) | 路徑演變與終點 | 歸因結論 |
|---|---|---|---|---|
| 1 | `1794998100` ETH `DOWN` | `el=199.8s`, `spot=1796.70 (-8.39bp)`, `twap=1797.34 (-4.84bp)` vs `ptb=1798.2084` | `el=220s` 仍有利 (`-6.53bp` spot)；`el=240s` 起 ETH 強彈至 `1797.98 -> 1798.40 (280s) -> 1799.20 (299s, +5.54bp)`，最終 60s TWAP 反超收 `1798.3265` (`UP`) | **盤尾真實反轉（加上盤口濾網失效）**：訊號當下方向與 Z (`1.52`) 屬實，但市場盤口當時已升至 **`0.88 > 0.82`**（`v2_filtered` 本不該進場），進場後最後 60 秒鏈上現價拉升 `+13.9 bps` 逆轉結果。 |
| 2 | `1795000200` BTC `DOWN` | `el=160.5s`, `spot=64052.57 (-7.01bp)`, `twap=64085.28 (-1.91bp)` vs `ptb=64097.5069` | 進場僅 **20 秒後**（`180s`）現價即反抽穿破 strike 至 `64109.88 (+1.93bp)`，`185.6s` 觸發 `EMERGENCY_STOP_LOSS`；`220s–299s` 多維持 `+1.5~+4.9bp`，最終結算 `64105.8619` (`UP`) | **一開始模型過早低估波動率（假極端 Z）＋現價隨後反轉**：錄製器滿 1800s `bn` 波動率算得 `z = -0.93 (< 1.30)`、且真實盤口 `down_ask1 = 0.85 (> 0.82)`；Runner 因低 `sigma (5.25e-5)` 與過期盤口 `0.791` 誤進場，20 秒後隨即被拉反轉。 |
| 4 | `1795005900` ETH `UP` | `el=239.7s`, `spot=1790.99 (+5.16bp vs true ptb 1790.0685; +5.77bp vs bug strike 1789.9600)`, `twap=1789.92 (-0.82bp)` | **先短暫回測後走高，官方確實結算 `UP` (`1790.4117`)**：`el=260s` spot 短暫回撤至 `1789.59 (-2.69bp)`（此時 `twap=1790.1078` 仍 `+0.22bp` 在水面上！），但已足以觸發 `stop_loss_z=-0.35` 以 `0.1747` 殺低賣出；`el=280s` 立即強彈回 `1791.48 (+7.87bp)` 並穩穩收在 `1790.4117` (`UP`) | **純屬程式 Bug 與結構停損雙重誤殺**：(1) 沒 `_rtds_open` `T-1s` bug 就根本不會觸發進場（真 `z=1.167`）；(2) 進場後方向**完全正確**，最終官方結算 `UP`，卻在 `260s` 被 spot 短線插針觸發 `EMERGENCY_STOP_LOSS` 割在最低點 (`0.1747`)。 |

---

### Q7. 是否觸發「緊急停損 (`stop_loss_z`)」賣出？與持有到結算比較？

| # | 交易 | 實際出場方式 | 實際損益 | 若持有到結算 | 停損造成差異 |
|---|---|---|---|---|---|
| 1 | `1794998100` ETH `DOWN` | 持有到結算 (`SETTLED_UP`) | `-$4.1074` | `-$4.1074` | `0.0000` (未觸發) |
| 2 | `1795000200` BTC `DOWN` | `EMERGENCY_STOP_LOSS @ 0.3526` (`185.6s`) | `-$2.2855` | `-$3.9915` (官方結算 `UP`, 歸零) | **`+$1.7060U` (少賠)** |
| 3 | `1795003200` BTC `DOWN` | 持有到結算 (`SETTLED_DOWN`) | `+$0.9116` | `+$0.9116` | `0.0000` (贏單) |
| 4 | `1795005900` ETH `UP` | `EMERGENCY_STOP_LOSS @ 0.1747` (`260.8s`) | **`-$3.1104`** | **`+$1.0233` (官方結算 `UP`, 領 $5.00)** | **`-$4.1337U` (誤殺反虧)** |

- **總計影響**：在 2 筆觸發緊急停損的單中，1 筆少虧 `$1.71U`，1 筆誤殺贏單多虧 `$4.13U`，合計 **`-$2.4277U`**。

---

### Q8. 4.2 只讀錄製器 (`live_recorder_42.py`) 同時段訊號與結果對照

比對 `edge_validation/phase42_trades_1s_v2_filtered.csv`（以及 `v2_pure` / `v2_highband_shadow`）：

| # | 視窗 / 幣種 | 實盤/模擬 Runner | 4.2 錄製器 (`v2_filtered`) | 4.2 錄製器 (`v2_pure` / `shadow`) | 差異原因 |
|---|---|---|---|---|---|
| 1 | `1794998100` ETH | `DOWN @ 199.8s, ask=0.82` -> **輸 (-$4.11)** | **無訊號** | `shadow`: `DOWN @ 188s, ask=0.88` -> SL `-$2.00` | 真實盤口為 **`0.88 > 0.82`**（`v2_filtered` 正確擋掉）；Runner 因 `price_change` 漏收看到假 `0.82` |
| 2 | `1795000200` BTC | `DOWN @ 160.5s, ask=0.791` -> **SL (-$2.29)** | **無訊號** | `v2_pure`: **反向 `UP @ 243s, ask=0.81->0.78`** -> SL `-$0.17` | 錄製器 `vol_1s` 高出 44% (`z=-0.93` 未過)，且真實 `down_ask1=0.85 > 0.82` |
| 3 | `1795003200` BTC | `DOWN @ 163.2s, ask=0.81` -> **贏 (+$0.91)** | **`DOWN @ 215s, ask=0.72->0.70`** -> **贏 (+$1.98)** | `v2_pure`: `DOWN @ 212s, 0.72->0.70` -> 贏 `+$1.98` | 方向結果一致；錄製器因正確等到盤口回落至 `0.72` 才進，賺更多 (`+28.95¢/sh`) |
| 4 | `1795005900` ETH | `ETH UP @ 239.7s, ask=0.815` -> **SL (-$3.11)** | **ETH 無訊號**（改觸發 `BTC UP @ 232s`, `0.82->0.83 MISSED`） | `v2_pure`: `SOL DOWN @ 225s` (`MISSED`) | Runner 同時因 `T-1s` strike bug 膨脹 Z 才觸發 ETH，且錯過錄製器偵測之 BTC UP |

---

## 本次已修復之程式 Bug（零更動任何策略參數）

1. **`engine/oracle_sniper_feeds.py` (`SniperMarketHub`)**：新增 `price_change` WS 增量盤口維護（依完整 `up_bids_raw`/`up_asks_raw`/`down_bids_raw`/`down_asks_raw` 即時更新與重算 `sweep_vwap` 頂檔價，杜絕 4¢–10¢ 盤口凍結滯後）。
2. **`engine/chainlink_oracle.py` & `engine/oracle_sniper_runner.py` (`_rtds_open`)**：嚴格優先採用 `ts_ms >= window_ts * 1000` 之 TWAP 刻度，絕不於 `window_ts + 8s` 內提前永久鎖死 `T - 1s` 刻度（根治 `#4 ETH` `-0.61 bps` strike 漂移）。
3. **`engine/oracle_sniper_runner.py` (`_build_views`, `_paper_buy`)**：將 `cl_spot` 最大容許年齡由 `max(4.5, 3.0)` 收緊回 `min(4.5, policy.max_chainlink_age_sec)`（嚴守 `3.0s`），並補齊 `_paper_buy` 過期盤口阻斷回傳 `PAPER_STALE_BOOK`。
4. **`edge_validation/live_recorder_42.py`**：修復 `windows_42.slug` 在 `ON CONFLICT DO UPDATE` 誤保留上一窗口 slug 之中繼資料 bug（不影響 Token ID / 價格 / 結算結果）。

- **Section 4.2 樣本重新計數基準時間點**：`1795016381`（次一完整 5m 視窗 **`window_ts >= 1795016400` / `2026-11-18T15:40:00Z`**），已保留只讀背景錄製器運行且未動任何策略參數與 `frozen_config.json`。
