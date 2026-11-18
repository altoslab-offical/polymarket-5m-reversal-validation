# Polymarket 5m & Binance API 實測查證筆記 (`api_notes.md`)

> 查證日期：2026-11-18T02:40Z（10:40+08:00）
> 本文件所有端點、狀態碼與回應範例均為實機發送 HTTP GET/POST 請求與查閱 Polymarket 官方文件/Changelog 後之實測紀錄。

---

## 一、端點實測對照與規劃差異說明

### 1. Gamma API（依 slug 取市場與 token id）
- **原規劃寫法**：`GET https://gamma-api.polymarket.com/markets?slug=...` 或 `/events?slug=...`
- **實測差異（重要）**：
  - `GET /markets?slug=<slug>` **預設 `closed=false`**，因此直接查詢**已結算的歷史 5m 市場會回傳空陣列 `[]`**！必須加上 `closed=any`（或直接用單一市場路由 `/markets/slug/<slug>`，或 `/events?slug=<slug>`）。
  - `/markets?closed=any&limit=100&slug=s1&slug=s2...` 支援單次以重複 `slug=` 查詢最多 **100 個市場**（不可用逗號分隔 `slug=s1,s2`，否則回傳 `[]`），非常適合歷史批次擷取。
- **實測請求 1（單一市場，已結算）**：
  - `GET https://gamma-api.polymarket.com/markets/slug/btc-updown-5m-1794967200` → **HTTP 200 OK**
  - 回應節錄：
    ```json
    {
      "slug": "btc-updown-5m-1794967200",
      "question": "Bitcoin Up or Down - November 17, 9:00PM-9:05PM ET",
      "conditionId": "0x7fb7e7ffaa45b0562a99a24559ae9348ea0db531cdf5b27ec79c1425a05cb6d2",
      "startDate": "2026-11-17T02:09:16.839071Z",
      "endDate": "2026-11-18T02:05:00Z",
      "eventStartTime": "2026-11-18T02:00:00Z",
      "createdAt": "2026-11-17T02:07:34.162397Z",
      "acceptingOrdersTimestamp": "2026-11-17T02:07:55Z",
      "clobTokenIds": "[\"56725909994555424637316153540501018111386565511559081742090943842055313918466\", \"112965083731210098106560275438316964718281083553613936751397736873110323023706\"]",
      "outcomes": "[\"Up\", \"Down\"]",
      "outcomePrices": "[\"0\", \"1\"]",
      "closed": true,
      "orderPriceMinTickSize": 0.001,
      "orderMinSize": 5,
      "makerBaseFee": 1000,
      "takerBaseFee": 1000,
      "feesEnabled": true,
      "feeType": "crypto_fees_v2",
      "feeSchedule": {
        "exponent": 1,
        "rate": 0.07,
        "takerOnly": true,
        "rebateRate": 0.2
      }
    }
    ```
- **實測請求 2（批次 100 市場）**：
  - `GET https://gamma-api.polymarket.com/markets?closed=any&limit=100&slug=btc-updown-5m-1786060800&slug=btc-updown-5m-1786061100...` → **HTTP 200 OK**（100 筆耗時約 1.27 秒）。
  - 注意：`clobTokenIds`、`outcomes`、`outcomePrices` 在 Gamma 回應中為 **JSON 字串**，解析後索引 `0` 固定對應 `"Up"`、索引 `1` 固定對應 `"Down"`（須同步驗證 `outcomes` 陣列確保一致）。

---

### 2. Data API（歷史逐筆成交）
- **原規劃寫法**：`GET https://data-api.polymarket.com/trades?market=<conditionId>`
- **實測差異（重大破壞性變更）**：
  - **v1 已於 2026-11-02 退役**：呼叫 `/trades` 或 `/v1/trades` 皆回傳 **HTTP 410 Gone**：
    ```json
    {
      "error": "Data API v1 has been retired",
      "migration": "https://data-api.polymarket.com/v2/",
      "docs": "https://docs.polymarket.com/api-reference/data-api/overview"
    }
    ```
  - **v2 參數名稱改變**：呼叫 `/v2/trades?market=...` 回傳 **HTTP 400 Bad Request**：
    ```json
    {
      "error": "'market' is not a query param on this API: filter by condition id with 'condition' (aliases 'condition_id', 'conditionId')",
      "code": "invalid_request",
      "retryable": false
    }
    ```
  - **正確端點**：`GET https://data-api.polymarket.com/v2/trades?condition=<conditionId>&limit=1000`
  - **關鍵分頁與時間過濾限制（自 `/v2/openapi.json` 確認並實測）**：
    - `condition` 支援逗號分隔最多 20 個 condition ID；`limit` 上限為 `1000`（預設 `taker_only=true`，每筆撮合以 taker 視角只計一次）。
    - **重要陷阱**：OpenAPI 明確寫明 `start` / `end` 時間參數 *「honored on the `user` shape only... The `condition`/`event_id` shapes serve a fixed three-year window... ignoring both bounds」*；且預設固定按時間由新到舊（`DESC`）排序，需透過 `pagination.next_cursor` 往回翻頁直到最舊交易 `timestamp < T - 30` 或 `pagination.has_more == false`。
    - 官方速率上限（`docs.polymarket.com/api-reference/rate-limits.md`）：`/v2/trades` 為 `300 req / 10s`（全 `/v2` 合計 `800 req / 10s`）。
- **實測請求**：
  - `GET https://data-api.polymarket.com/v2/trades?condition=0x8b0eda62a3ebb120a61873b16d3590e96daaa7ddd007267cfd6b1f6bad7c27d9&limit=5` → **HTTP 200 OK**
  - 回應節錄：
    ```json
    {
      "data": [
        {
          "proxy_wallet": "0x07a2b472c3d343ccda7eabb1bc542414f9bbe4ad",
          "side": "SELL",
          "token_id": "100853188340479982720754865773961784888964276211149404742146682256358987948548",
          "condition_id": "0x8b0eda62a3ebb120a61873b16d3590e96daaa7ddd007267cfd6b1f6bad7c27d9",
          "size": 2.08,
          "price": 0.999,
          "timestamp": 1786061205,
          "slug": "btc-updown-5m-1786060800",
          "outcome": "Up",
          "outcome_index": 0
        }
      ],
      "pagination": {
        "limit": 5,
        "offset": 0,
        "has_more": true,
        "next_cursor": "eyJkYXRhIjp7InR5cGUiOiJ0cmFkZXMi..."
      }
    }
    ```

---

### 3. CLOB API（歷史價格序列與即時盤口）
- **歷史價格序列（單筆 / 批次）**：
  - 單市場 GET：`GET https://clob.polymarket.com/prices-history?market=<token_id>&startTs=<sec>&endTs=<sec>&fidelity=1` → **HTTP 200 OK**（8 月至今的 5m 市場實測皆完整有資料，約每分鐘 1 點）。
  - 官方批次 POST：`POST https://clob.polymarket.com/batch-prices-history`（上限每次 **20 個 `token_id`**，速率上限 `/prices-history` 為 `1,000 req / 10s`）。
  - 補充查證：`data-api.polymarket.com/v2/prices-history?token_id=...&as_of=...` 雖然也是新端點，但早期 8 月份市場的遠期 tier 會退化到 `resolution_seconds: 10800`（3 小時粗粒度），反而是 `clob.polymarket.com/prices-history` / `batch-prices-history` 搭配 `fidelity=1` 仍保留 ~60 秒細粒度，故優先採用 CLOB `/batch-prices-history`。
  - **關鍵實測發現**：同一個市場的 `Up` token 與 `Down` token 在 `prices-history` 中的快照時間戳各自獨立、未必完全對齊（例如 `t=1786147150 p_up=0.505` 與 `t=1786147151 p_down=0.52`，20 個抽樣市場中有 12 個不完全互補），因此必須**分別查詢 Up 與 Down 兩個 token**，不可用 `1 - p` 推算。
- **實測請求（批次）**：
  - `POST https://clob.polymarket.com/batch-prices-history`
  - Payload：`{"markets": ["100853...", "474307..."], "start_ts": 1786060200, "end_ts": 1786060900, "fidelity": 1}` → **HTTP 200 OK**
  - 回應節錄：
    ```json
    {
      "history": {
        "100853188340479982720754865773961784888964276211149404742146682256358987948548": [
          {"t": 1786060224, "p": 0.505},
          {"t": 1786060750, "p": 0.505},
          {"t": 1786060812, "p": 0.405}
        ]
      }
    }
    ```
- **即時盤口（Phase B 用）**：
  - 進行中市場 `GET https://clob.polymarket.com/book?token_id=63115568571922116585301054266161936903196380600467243167826772469619261384027` → **HTTP 200 OK**：
    ```json
    {
      "market": "0x7cc317a2df87a819247a95d1e2de84cf7d2e9365aec2c109771060d97dbdc9b2",
      "asset_id": "63115568571922116585301054266161936903196380600467243167826772469619261384027",
      "timestamp": "1794969458702",
      "bids": [{"price": "0.01", "size": "6624.35"}, {"price": "0.02", "size": "2114.5"}],
      "asks": [{"price": "0.99", "size": "7572.67"}, {"price": "0.98", "size": "2240.15"}],
      "min_order_size": "5",
      "tick_size": "0.01",
      "neg_risk": false,
      "last_trade_price": "0.470"
    }
    ```
  - 已結算市場呼叫 `/book` 回傳 **HTTP 404 Not Found**。

---

### 4. 手續費端點與公式
- **CLOB fee-rate 端點**：
  - `GET https://clob.polymarket.com/fee-rate?token_id=56725909994555424637316153540501018111386565511559081742090943842055313918466` → **HTTP 200 OK**：`{"base_fee":1000}`（`1000 bps` 顯示碼，實際費率曲線見 Gamma `feeSchedule` 與官方 `fees.md`）。
- **Gamma 市場物件（動態讀取）**：
  - 實測 BTC / ETH / SOL 5m 市場自 2026-08-06 至 2026-11-18 皆為：
    `"feeType": "crypto_fees_v2"`, `"feeSchedule": {"exponent": 1, "rate": 0.07, "takerOnly": true, "rebateRate": 0.2}`, `"orderMinSize": 5`, `"orderPriceMinTickSize": 0.001`。
- **官方手續費公式（`https://docs.polymarket.com/trading/fees.md`）**：
  - `fee = C × feeRate × p × (1 - p)`（其中 `C` 為股數、`p` 為成交價；目前 Crypto `feeRate = 0.07`、`takerOnly = true`、Maker 費率 `0`）。
  - *註：官方公告將於 2026-11-20T23:59Z 調整 DeFi fee/rebate（超出本次歷史回測區間）*。

---

### 5. Binance 現貨 K 線（1m / 1s）與 U 本位合約 OI（metrics）
- **Binance Vision Spot K 線**：
  - 實測 2026-08、2026-09、2026-10 月檔與 2026-11-01 ~ 2026-11-16 日檔均存在（`HTTP 200`）：
    - `https://data.binance.vision/data/spot/monthly/klines/{SYM}/1m/{SYM}-1m-YYYY-MM.zip`
    - `https://data.binance.vision/data/spot/monthly/klines/{SYM}/1s/{SYM}-1s-YYYY-MM.zip`（單月檔約 70 MB / 幣）
    - 日檔 `.../daily/klines/{SYM}/{1m|1s}/{SYM}-{1m|1s}-YYYY-MM-DD.zip`（目前最新至 `2026-11-16`，`2026-11-17` 尚未歸檔回傳 404）。
  - **時間戳單位差異（重要陷阱）**：Binance Vision 現貨 K 線自 2025 年起採用 **16 位數微秒時間戳**（例如 `1794700800000000`），除以 `1_000_000` 為 UTC 秒；而 `2026-11-17 ~ 18` 由 `GET https://api.binance.com/api/v3/klines?symbol={SYM}&interval={1m|1s}` 補齊，其時間戳為 **13 位數毫秒**（除以 `1_000`）。兩者欄位結構相同，無 header。
- **Binance Futures UM `metrics`（OI）**：
  - **沒有** `monthly/metrics/`（實測回傳 `HTTP 404`），只有日檔 `https://data.binance.vision/data/futures/um/daily/metrics/{SYM}/{SYM}-metrics-YYYY-MM-DD.zip`（實測自 2026-08-06 至 2026-11-16 每日皆有，每日 288 列、**5 分鐘粒度**）。
  - **關鍵格式與時間對齊陷阱（實測確認）**：
    1. CSV **有標頭**且內部列的 `create_time` **未排序**（實測前三列為 `00:25:00`, `00:10:00`），讀入後必須顯式按時間排序並去重。
    2. `metrics` 的 `create_time` 與 REST `https://fapi.binance.com/futures/data/openInterestHist?symbol={SYM}&period=5m&limit=500` 的 `timestamp`（含 `sum_open_interest == sumOpenInterest` 精確相等）採用同一 5m bar **起始時間（`open_time`）慣例**：在 `02:41:22 UTC`（`1794969682`）實測 `/futures/data/openInterestHist`，最新一筆為 `1794969300`（`02:35:00 UTC`，即剛收盤的 `[02:35, 02:40)`），而 `1794969600`（`02:40:00 UTC`）尚未出現。因此嚴格為無未來函數對齊，Polymarket 視窗 `T` 使用的當根 `[T-300, T)` OI 為 `create_time == T - 300` 那一列。

---

## 二、第 1 節五個必答事實與規則原文

### 1. slug 時間戳的意義
- `{btc|eth|sol}-updown-5m-<ts>` 的 `<ts>` 代表該 5 分鐘視窗的**開始時間 `T`**（UTC epoch 秒）。
- 實測對照 `btc-updown-5m-1794967200`（`1794967200` = `2026-11-18T02:00:00Z`）：
  - `eventStartTime`: `2026-11-18T02:00:00Z`（`= T`）
  - `endDate`: `2026-11-18T02:05:00Z`（`= T + 300`）

### 2. 市場規則原文（Gamma `description` 完整全文）
以 BTC `60s TWAP` 市場（如 `btc-updown-5m-1786665600` 與 `btc-updown-5m-1794967200`）為例，`description` 原文如下（ETH/SOL 僅將資產名稱與 Chainlink URL 替換為 `Ethereum`/`ETH/USD`/`eth-usd-twap-60s-streams` 與 `Solana`/`SOL/USD`/`sol-usd-twap-60s-streams`）：

```text
This market will resolve to "Up" if the time-weighted average price (TWAP) of Bitcoin, generated by Chainlink, of the time range specified in the title is greater than or equal to the price at the beginning of that range. Otherwise, it will resolve to "Down".
The resolution source for this market is information from Chainlink, specifically the BTC/USD TWAP data stream available at https://data.chain.link/streams/btc-usd-twap-60s-streams.
Please note that this market is about the price according to the TWAP Chainlink data stream for the asset pair BTC/USD, not according to any other sources or spot markets.
```

- **TWAP 窗口長度**：**60 秒**（`twap-60s-streams`；依官方 2026-08-07 與 2026-08-14 changelog 及規則文字，開盤基準價與收盤結算價皆來自同一 TWAP feed）。
- **平手判定**：規則寫明 *"greater than or equal to"*，因此終值 TWAP `>=` 起始價格時，**平手判給 `"Up"`**。
- **價格來源**：Chainlink Data Streams：
  - BTC：`https://data.chain.link/streams/btc-usd-twap-60s-streams`
  - ETH：`https://data.chain.link/streams/eth-usd-twap-60s-streams`
  - SOL：`https://data.chain.link/streams/sol-usd-twap-60s-streams`

### 3. TWAP 切換日期（官方 Changelog ＋ 逐市場二元搜尋實證）
Polymarket 5 分鐘加密市場共經歷兩次結算機制切換（BTC / ETH / SOL 三幣完全同步）：
1. **`T <= 1786060500`（`~ 2026-08-06T23:55:00Z`）**：單點價格 Snapshot 結算（`https://data.chain.link/streams/{btc|eth|sol}-usd`）。
2. **`1786060800 <= T <= 1786665300`（`2026-08-07T00:00:00Z` 至 `2026-08-13T23:55:00Z`，共 7 天 / 2,016 視窗）**：**30 秒 Chainlink TWAP**（`https://data.chain.link/streams/{btc|eth|sol}-usd-twap-30s-streams`）。
   - 官方 `predictions.md` Changelog (`Aug 7, 2026`)：*"Chainlink TWAP resolution for crypto up/down markets — Averaging windows: 5-minute markets use a 30-second lookback; 15-minute and 4-hour markets use a 60-second lookback."*
3. **`T >= 1786665600`（`2026-08-14T00:00:00Z` 起至今）**：**60 秒 Chainlink TWAP**（`https://data.chain.link/streams/{btc|eth|sol}-usd-twap-60s-streams`）。
   - 官方 `predictions.md` Changelog (`Aug 14, 2026`)：*"5-minute crypto markets moved to a 60-second Chainlink TWAP — All 5-minute crypto markets now resolve using a 60-second Chainlink TWAP, replacing the 30-second window introduced on August 7. The change took effect at 00:00 UTC."*
   - 本次驗證嚴格以 **`T = 1786665600`（`2026-08-14T00:00:00Z`）** 作為純 60 秒 TWAP 區間起點，不混入 `2026-08-07 ~ 08-13` 的 30 秒 TWAP 市場。
   - *另備註兩次基礎架構延遲調整（來自官方 Changelog）*：`2026-08-17 11:00 UTC` crypto taker delay 由 250ms 降至 50ms；`2026-09-04 14:00 UTC` 調升為 150ms。

### 4. 市場上架時間
- 從 Gamma API 欄位觀察（`createdAt`、`acceptingOrdersTimestamp` vs `eventStartTime`）：
  - `btc-updown-5m-1786665600`（`eventStartTime = 2026-08-14T00:00:00Z`）：預先於 **`2026-08-13T00:08Z`~`00:10Z`（約開盤前 ~23 小時 50 分鐘）** 即建立並開放掛單；實測歷史 `prices-history` 與成交資料亦顯示在 `T - 10m` ~ `T - 30s` 期間已有真實成交。
  - `btc/eth/sol-updown-5m-1794967200`（`eventStartTime = 2026-11-18T02:00:00Z`）：`createdAt` 與 `acceptingOrdersTimestamp` 為 **`2026-11-17T02:07:32Z ~ 02:07:55Z`（視窗開始前約 23 小時 52 分鐘）**。
- 結論：市場在視窗開始前約 **24 小時** 即開放下單，因此在訊號觸發時（`T - 5s` 或 `[T - 30, T)`）下一個 5m 市場已掛盤可交易。

### 5. 5m 加密市場實際費率與最低下單量
- **Taker 費率**：`feeSchedule.rate = 0.07`（即 `taker_fee_rate = 0.07`，手續費公式 `fee = C × 0.07 × p × (1 - p)`，在 `p = 0.50` 時等於每股 `$0.0175`，即 1.75¢ / 3.50% of $0.50；或看官方對照表：`feeType: "crypto_fees_v2"`）。
- **Maker 費率**：`0.0`（`feeSchedule.takerOnly = true`，官方明確寫 *"Makers are never charged fees"*，另有 Maker Rebate Pool 返傭，不計入固定 Taker 驗證成本）。
- **最低下單量與跳動單位**：`orderMinSize = 5`（最低 5 股）、`orderPriceMinTickSize = 0.001`（`0.1¢`；即時盤口通常為 `0.01` 至近端 `0.001`）。
