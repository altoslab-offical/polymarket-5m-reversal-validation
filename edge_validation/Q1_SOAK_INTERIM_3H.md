# Q1 潔淨運行報告 (`since_ts=1795078200`)

## 1. 四項修正版驗收指標（`CLAUDE_RESPONSE_10` 第 1 節）

| 指標 | Runner | Recorder (Raw ENTER / Single-Slot First) | 門檻 | 判定 |
|---|---:|---:|---|---|
| 1. 舊資料決策 (`lag>1000ms` 或 `age>1500ms`) | `0/8` | `1/4` / `0/3` | `0` | `FIRST_ONLY` |
| 2. `code=1013` 斷線次數 | `0` | `3` | `0` | `FAIL` |
| 4. Runner 重連 / `STALE_BOOK` 佔比 | `128` 次 / `0.0266%` | 1 Hz 缺秒 `0` | 觀察報告 | `INFO` |

## 2. 分幣別 WebSocket 延遲與進場時段 `[150,255]s` 健康度

| Coin | Rows | Span (h) | `>2000ms` (s / %) | `>1000ms` (s) | `age>1500ms` (s) | Entry `[150,255]s` `age>1500ms` | P50 / P95 / P99 / Max (ms) |
|---|---:|---:|---:|---:|---:|---:|---|
| `BTCUSDT` | 12446 | 3.46 | 524 (4.21%) | 893 | 244 | 46/4346 (1.058%) | 68.7 / 2043.38 / 2495.26 / 2499.61 |
| `ETHUSDT` | 12446 | 3.46 | 44 (0.354%) | 85 | 28 | 10/4346 (0.23%) | 66.62 / 236.05 / 886.32 / 2493.2 |
| `SOLUSDT` | 12446 | 3.46 | 1 (0.008%) | 6 | 11 | 5/4346 (0.115%) | 63.61 / 152.3 / 297.24 / 2005.6 |

## 3. Runner `-4.04U` (`1795080900 BTCUSDT UP`) 進場當秒鑑識

- Runner `ENTER` (`ts=1795081150`, `t=250s`, `ask=0.78`): `book_age=1.0 ms`, `book_lag=18.55 ms`, `cl_twap_age=311.0 ms`。
- Runner `ENTER` (`ts=1795081151`, `t=251s`, `ask=0.81`): `book_age=16.0 ms`, `book_lag=26.67 ms`, `cl_twap_age=869.0 ms`。
- Runner `ENTER` (`ts=1795081154`, `t=254s`, `ask=0.82`): `book_age=0.0 ms`, `book_lag=18.51 ms`, `cl_twap_age=903.0 ms`。
- Recorder 1 Hz (`ts=1795081150`, `t=250s`, `up_ask1=0.83`): `up_book_age=205.41 ms`, `up_book_lag=2486.77 ms`。
- Recorder 1 Hz (`ts=1795081151`, `t=251s`, `up_ask1=0.79`): `up_book_age=0.07 ms`, `up_book_lag=454.23 ms`。
- Recorder 1 Hz (`ts=1795081152`, `t=252s`, `up_ask1=0.87`): `up_book_age=0.88 ms`, `up_book_lag=19.36 ms`。
- 凍結檔案指紋檢查：`12/12` 符合（`ok=True`）。
