# Deviations from Specification (`deviations.md`)

## 1. `CLOB batch-prices-history` (`fidelity=1`) 約每 60 秒一個快照點
- **規格期望**：取每個視窗 `τ ∈ {150, 180, 210, 240, 255}` 的最新快照價。
- **實測差異**：`clob.polymarket.com/batch-prices-history` 即使設定 `fidelity=1`（最低參數），已結算 5m 市場回傳的歷史軌跡實際解析度約為 **每 60 秒 1 點**。
- **影響與處理**：
| tau | pct_in_window | median_lag_sec | p90_lag_sec | pct_lag_gt_60s |
| --- | --- | --- | --- | --- |
| 150 | 99.25 | 16.0 | 21.0 | 4.9 |
| 180 | 99.46 | 45.0 | 50.0 | 1.88 |
| 210 | 99.53 | 16.0 | 20.0 | 4.01 |
| 240 | 99.69 | 45.0 | 49.0 | 1.23 |
| 255 | 99.78 | 46.0 | 62.0 | 14.93 |
- 相鄰的 `(150, 180)` 與 `(240, 255)` 有一定比例共享同一個 ~60s 快照點，因此歷史回測中已排除 `t < T` 的視窗外點（僅保留 `0 <= τ - t <= τ`），**並額外提供第 9 節 `lag <= 5s` / `lag <= 2s` 的零時差同步稽核**（排除 45s 滯後造成的前視膨脹，確認純同步狀態下 OOS With-Model `[0.68,0.82]` 在 BTC/ETH/SOL 仍全為顯著正 EV，而 Model-Free 為負）。真實 1 秒盤口與 `+1s/+2s` 延遲驗證仍必須依賴 Section 4.2 的即時只讀錄製。

## 2. Section 4.1 模型標準差 `σ` 的量綱對齊
- **規格原文**：`Φ((spot - S0) / (σ * sqrt(r - 40)))`。
- **實測調整**：若 `σ` 為無因次 1 秒報酬率（約 `5e-5`）而分子 `spot - S0` 為美元價格，兩者量綱不一致（會放大 ~65,000 倍）。實作中採用量綱一致的 `σ_usd = S0 * σ_log_1s`（過去 30 分鐘 1 秒對數報酬標準差 × `S0`），等同 `(spot - S0) / S0 / (σ_log_1s * sqrt(r - 40))`。

## 3. R1 / R2 Chainlink 官方報價源發現（無須等待 11/30 付費）
- **實測發現**：
  1. `polymarket.com/api/crypto/crypto-price` 回傳的是 Chainlink **SPOT**（僅供參考，與結算僅 ~73–91% 一致，因為結算規則為 60s TWAP）。
  2. 免費公開無需 Key 的 **RTDS `crypto_prices_twap_sixty`** (`wss://ws-live-data.polymarket.com`) 與需要免費 CLOB Key 的 **PolyBolt `price.crypto.twap`** (`wss://ws-live-v2.polymarket.com/ws`) 兩者數值逐秒完全一致（實測 12 個重複時間戳最大差異 `4.4e-11 USD`）。
  3. `live_btc_5m.db` 原本的 `resolved_outcome` 欄位本身有 5 筆極貼邊界 (`$0.06–$1.03`) 視窗與官方 Gamma `outcomePrices` 相反、且 6 筆為 NULL；以 Gamma `eventMetadata` 回填後，`finalPrice >= priceToBeat` 與官方 `outcomePrices` 一致率為 **1013/1013 (100.00%)**。
  4. 若 11/30 起 Chainlink API Key 限制同時影響 RTDS TWAP（目前僅文件公告於 PolyBolt），`live_recorder_42.py` 與 `ChainlinkRTDSFeed` 已內建基於 `crypto_prices_chainlink` 原始報價的時間加權積分（time-weighted step-integral）60s TWAP 備援，並持續對照 Binance/Coinbase。

## 4. `Nov 20` 前後費率切換
- 本報告同步呈現 `fee = 0.07`（歷史 96 天實際適用費率）與 `fee = 0.05 (BTC) / 0.01 (ETH, SOL)`（11/20 後新費率敏感度）；注意 11/20 前歷史盤口做市商報價反映的是 `0.07` 費率結構下的 equilibrium spread，新費率下的真實盤口深度須在 Section 4.2（11/21 00:00 UTC 後）實測。
