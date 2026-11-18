# Deviations from Specification (`deviations.md`)

## 1. 本輪已完全修正的三項偏差（L1 / L2 / L3）
- **L1（1 秒索引修正）**：Binance 1s klines 以 `open_time` 為索引，`open_time = s` 代表 `[s, s+1)`，收盤於 `s+1`。初版取 `idx_T + τ` 多偷看未來 1 秒，本版已改為 `idx_T + τ - 1`（`S0 = [T-60, T-1]`、`twap60 = [T+240, T+299]`、`A_255 = [T+240, T+254]`）。
- **L2（`[τ+1, τ+5]` 成交修正）**：`prices-history (fidelity=1)` 約每 60s 抽樣一次，前照 `lag > 0` 在 `τ` 前數十秒定格會造成 Binance 領先膨脹。本版實取 `t ∈ [τ+1, τ+5]` 的第一個價格點作為成交價（模型嚴格只用 `<= τ`），並完整保存前照 `lag` 分組表供比對。
- **L3（條件價差修正）**：已改用 `live_btc_5m.db` 在模型 `EV >= 3.5¢` 時刻實測的半價差分布；唯 ETH / SOL 在 `11/20` 降費後的真實 `ask_size` 仍待 Section 4.2 現場錄製驗證。

## 2. L4 組合模擬暫緩說明
- 舊版 `$43 -> $192` 組合模擬因基於修正前之 L1–L3 資料，依 Claude 回覆已標記為作廢/暫不採用；策略過關評估以 Section 4.2 即時 1Hz Top-5 錄製為準。

## 3. R1 / R2 官方 Chainlink 60s TWAP 串接
- 使用免費無 Key 的 RTDS `crypto_prices_twap_sixty`（與 PolyBolt `price.crypto.twap` 在重複時間戳最大差異 `4.4e-11 USD`），以 Gamma `eventMetadata` 回填 `live_btc_5m.db` 全數 `1,013/1,013` 窗達 **100.00%** 一致率。
