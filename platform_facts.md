# Polymarket 平台實測事實報告 (`platform_facts.md`)

- 查證時間：`2026-11-18T06:36:24Z`
- 執行環境位置：`HK`（`blocked=False`）

## 1.1 市場生命週期與資金回收（實測 102 檔最近已結算 BTC/ETH/SOL 5m 市場，見 `settlement_latency.csv`）

1. **上架與開放時間**：Gamma `acceptingOrdersTimestamp` 實測在 `T` 前中位數 **23.87 小時**（約 24 小時）即開放下單。
2. **結算與鏈上可贖回時間差**（相對於 `endDate = T + 300`，共 `102` 檔，`was_disputed = False` 佔 100%）：
   - `closedTime`（Gamma 關閉/`outcomePrices` 定案）：中位數 **`20.0s`**、P90 **`23.0s`**、最大 **`25.0s`**。
   - `/v2/resolutions` `resolved_at`（鏈上 `ConditionResolution` 結算可贖回）：中位數 **`20.0s`**、P90 **`23.0s`**、最大 **`25.0s`**（與 `closedTime` 完全同步）。
3. **贏家入帳機制**：贏家 token **不自動變現金**，需呼叫 `plan_collateral_return()`/`execute_collateral_return_plan()` 或 `redeem_positions()` 贖回為 `pUSD`；在 Protocol V2 透過 Polymarket Builder Gasless Relayer 執行（使用者 **0 gas**）。
4. **結算前/後勝方二級市場成交（T+300 後 0–10 秒）**：有成交場數 `22/102`、VWAP 中位數 **`$0.9900`**（平均 `$0.9904`）、累計股數 `1,475.5`。
4. **結算前/後勝方二級市場成交（T+300 後 10–30 秒）**：有成交場數 `14/102`、VWAP 中位數 **`$0.9945`**（平均 `$0.9945`）、累計股數 `419.7`。
4. **結算前/後勝方二級市場成交（T+300 後 30–60 秒）**：有成交場數 `7/102`、VWAP 中位數 **`$0.9900`**（平均 `$0.9926`）、累計股數 `107.8`。
5. **Merge / Split**：官方統一 SDK `AsyncSecureClient` 原生支援 `split_position` 與 `merge_positions`（`Up + Down -> 1 pUSD`），透過 Gasless Relayer 免用戶 gas；鏈上等待時間與 Relayer 單筆確認相當。

## 1.2 結算規則驗證

- **TWAP 起訖點**：官方 `docs.polymarket.com/concepts/resolution.md` 明文指出起始 `priceToBeat` 與最終 `final price` 皆取自同一 **60 秒回溯 Chainlink TWAP stream**（`twap-60s-streams`），平手（`final >= priceToBeat`）判 `Up`。
- **實測 `live_btc_5m.db` 連續 `25` 檔驗證**：`close_price >= strike_price` 與實際結算 `resolved_outcome` 吻合 **`23/25`**；連續視窗中「下一檔 `strike_price` == 上一檔 `close_price`」達 **`22/24`**。
- **作廢次數**：Phase A 全量 `83,019` 視窗中 75 檔未建市場，其餘 **`82,944 / 82,944`（100%）正常結算，0 檔作廢或 50-50 退款**。

## 1.3 手續費、返傭與流動性獎勵（含 `2026-11-20` 重大新制）

- **現行規則（至 `2026-11-20T23:59Z`）**：BTC/ETH/SOL 5m 全為 `feeRate = 0.07`、`takerOnly = true`、`rebateRate = 0.2`，手續費公式 `fee = shares × feeRate × p × (1 - p)`（Maker 免手續費）。
- **🔥 `2026-11-20T23:59Z` 起生效之官方新制（`fee-and-rebate-changes-november-2026.md`）**：
  1. **BTC 5m / 15m**：Taker `feeRate` 由 `0.07` 降至 **`0.05`**。
  2. **ETH / SOL 5m（`Crypto — all other markets`）**：Taker `feeRate` 由 `0.07` 大砍 85.7% 至 **`0.01`**（50¢ 時僅 `0.25¢/股`）。
  3. **Maker Rebates 全面改分級制**：取消每日全市場 pool，改依過去 30 天加權成交量（`shares × p × (1 - p)`）分級；**未滿 `$200,000` 為 `None` 級（`0%` 返傭）**，`$200k–$1M` 才給 3%。對 `$43U` 帳號返傭實質為 **`0`**。
- **Liquidity Rewards**：Gamma 實測 5m 市場為 `rewardsMinSize = 50` 股、`rewardsMaxSpread = 4.5`、`clobRewards = None`，且官方規定每日低於 `$1` 不發放，對小資金（每次 5 股）**無資格亦無實質收益**。

## 1.4 下單機制、SDK 簽名與網路延遲實測

- **CLOB `/book` 往返延遲（`n=100` 暖連線，來自 `HK`）**：中位數 **`243.9 ms`**、P90 **`272.3 ms`**（min `227.7 ms` / max `395.8 ms`）。官方伺服器主區為 `eu-west-2`（倫敦）。
- **WebSocket `wss://ws-subscriptions-clob.polymarket.com/ws/market`**：握手 `1259.3 ms`、首筆訊息 `328.7 ms`；4 秒實測事件類型 `{'book': 12, 'price_change': 3457, 'last_trade_price': 5}`；`book`（全量快照）欄位 `['market', 'asset_id', 'timestamp', 'hash', 'bids', 'asks', 'tick_size', 'event_type', 'last_trade_price']`；`price_change`（增量異動）欄位 `['market', 'price_changes', 'timestamp', 'event_type']`。
- **Taker Delay**：官方 Changelog 記載自 `2026-09-04 14:00Z` 起 Crypto 為 **`150ms`**（針對吃單；Maker 撤單不受 taker delay 限制）。`2026-11-30 00:00Z` 起 PolyBolt Chainlink 訂閱需附獨立 Chainlink API Key。
- **官方 `polymarket==0.13.0` (`AsyncSecureClient`) 函式簽名**：
```python
AsyncSecureClient.create_limit_order(self, *, asset_id: str | None = None, token_id: str | None = None, price: decimal.Decimal | int | float | str, size: decimal.Decimal | int | float | str, side: Literal['BUY', 'SELL'], post_only: bool = False, expiration: int | None = None, builder_code: str | None = None) -> polymarket.models.clob.orders.SignedOrder
AsyncSecureClient.create_market_order(self, *, asset_id: str | None = None, token_id: str | None = None, side: Literal['BUY', 'SELL'], amount: decimal.Decimal | int | float | str | None = None, shares: decimal.Decimal | int | float | str | None = None, max_spend: decimal.Decimal | int | float | str | None = None, max_price: decimal.Decimal | int | float | str | None = None, min_price: decimal.Decimal | int | float | str | None = None, order_type: Literal['FAK', 'FOK'] = 'FAK', builder_code: str | None = None) -> polymarket.models.clob.orders.SignedOrder
AsyncSecureClient.place_limit_order(self, *, asset_id: str | None = None, token_id: str | None = None, price: decimal.Decimal | int | float | str, size: decimal.Decimal | int | float | str, side: Literal['BUY', 'SELL'], post_only: bool = False, expiration: int | None = None, builder_code: str | None = None) -> polymarket.models.clob.order_response.AcceptedOrder | polymarket.models.clob.order_response.RejectedOrder
AsyncSecureClient.place_market_order(self, *, asset_id: str | None = None, token_id: str | None = None, side: Literal['BUY', 'SELL'], amount: decimal.Decimal | int | float | str | None = None, shares: decimal.Decimal | int | float | str | None = None, max_spend: decimal.Decimal | int | float | str | None = None, max_price: decimal.Decimal | int | float | str | None = None, min_price: decimal.Decimal | int | float | str | None = None, order_type: Literal['FAK', 'FOK'] = 'FAK', builder_code: str | None = None) -> polymarket.models.clob.order_response.AcceptedOrder | polymarket.models.clob.order_response.RejectedOrder
AsyncSecureClient.post_order(self, signed_order: polymarket.models.clob.orders.SignedOrder) -> polymarket.models.clob.order_response.AcceptedOrder | polymarket.models.clob.order_response.RejectedOrder
AsyncSecureClient.post_orders(self, signed_orders: collections.abc.Sequence[polymarket.models.clob.orders.SignedOrder]) -> tuple[polymarket.models.clob.order_response.AcceptedOrder | polymarket.models.clob.order_response.RejectedOrder, ...]
AsyncSecureClient.cancel_order(self, *, order_id: str) -> polymarket.models.clob.cancel.CancelOrdersResponse
AsyncSecureClient.cancel_orders(self, *, order_ids: collections.abc.Sequence[str]) -> polymarket.models.clob.cancel.CancelOrdersResponse
AsyncSecureClient.cancel_all(self) -> polymarket.models.clob.cancel.CancelOrdersResponse
AsyncSecureClient.cancel_market_orders(self, *, market: str | None = None, asset_id: str | None = None, token_id: str | None = None) -> polymarket.models.clob.cancel.CancelOrdersResponse
AsyncSecureClient.split_position(self, *, condition_id: str | None = None, legs: collections.abc.Sequence[str] | None = None, amount: int, metadata: str | None = None) -> polymarket.transactions.GaslessTransactionHandle | polymarket.transactions.EoaTransactionHandle
AsyncSecureClient.merge_positions(self, *, condition_id: str | None = None, legs: collections.abc.Sequence[str] | None = None, amount: Union[int, Literal['max']], metadata: str | None = None) -> polymarket.transactions.GaslessTransactionHandle | polymarket.transactions.EoaTransactionHandle
AsyncSecureClient.redeem_positions(self, *, condition_id: str | None = None, market_id: str | None = None, position_id: str | None = None, metadata: str | None = None) -> polymarket.transactions.GaslessTransactionHandle | polymarket.transactions.EoaTransactionHandle
AsyncSecureClient.plan_collateral_return(self) -> polymarket.models.collateral_return.CollateralReturnPlanResponse
AsyncSecureClient.execute_collateral_return_plan(self, *, plan: polymarket.models.collateral_return.CollateralReturnPlanResponse) -> polymarket.transactions.GaslessTransactionHandle
```

## 1.5–1.6 帳戶、地區與待錄項目

- **抵押品與 Gas**：Polymarket Protocol V2 全面採用 Polygon **`pUSD`**，CLOB 下單與 Gasless Relayer 贖回免使用者付 gas；受限地區依官方 `geoblock.md`（TW 為 Frontend+API 雙鎖 Close-only；HK 雙開放；JP/KR/NL/IE 僅鎖網頁）。
- **尚待補錄（未附子規格時之已知缺口）**：1) 三幣 **48 小時**即時盤口錄製 `book_live.parquet`（目前已有 BTC 84.4 小時 `live_btc_5m.db`）；2) 總綱指定依 `PHASE_A5_MAKER_PLAN.md` 與 `PHASE_C_INTRAWINDOW_PLAN.md` 執行的分買/賣方 VWAP 與因子測試（尚缺該兩份子文件原文）。