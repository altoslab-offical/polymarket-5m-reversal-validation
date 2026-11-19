# Q1 潔淨運行報告 (`since_ts=1795090647`)

## 1. 四項修正版驗收指標（`CLAUDE_RESPONSE_10` 第 1 節）

| 指標 | Runner | Recorder (Raw ENTER / Single-Slot First) | 門檻 | 判定 |
|---|---:|---:|---|---|
| 1. 舊資料決策 (`lag>1000ms` 或 `age>1500ms`) | `0/8` | `0/13` / `0/5` | `0` | `PASS` |
| 2. `code=1013` 斷線次數 | `2` | `3` | `0` | `FAIL` |
| 4. Runner 重連 / `STALE_BOOK` 佔比 | `65` 次 / `0.0256%` | 1 Hz 缺秒 `0` | 觀察報告 | `INFO` |

## 2. 分幣別 WebSocket 延遲與進場時段 `[150,255]s` 健康度

| Coin | Rows | Span (h) | `>2000ms` (s / %) | `>1000ms` (s) | `age>1500ms` (s) | Entry `[150,255]s` `age>1500ms` | P50 / P95 / P99 / Max (ms) |
|---|---:|---:|---:|---:|---:|---:|---|
| `BTCUSDT` | 22116 | 6.14 | 1414 (6.394%) | 2307 | 732 | 196/7844 (2.499%) | 52.22 / 2455.3 / 2496.5 / 2499.97 |
| `ETHUSDT` | 22116 | 6.14 | 227 (1.026%) | 528 | 123 | 31/7844 (0.395%) | 49.62 / 461.16 / 2181.02 / 2499.99 |
| `SOLUSDT` | 22116 | 6.14 | 34 (0.154%) | 124 | 55 | 11/7844 (0.14%) | 45.26 / 183.13 / 705.42 / 2479.85 |

## 3. Runner `-4.04U` (`1795080900 BTCUSDT UP`) 進場當秒鑑識

- Runner `ENTER` (`ts=1795081150`, `t=250s`, `ask=0.78`): `book_age=1.0 ms`, `book_lag=18.55 ms`, `cl_twap_age=311.0 ms`。
- Runner `ENTER` (`ts=1795081151`, `t=251s`, `ask=0.81`): `book_age=16.0 ms`, `book_lag=26.67 ms`, `cl_twap_age=869.0 ms`。
- Runner `ENTER` (`ts=1795081154`, `t=254s`, `ask=0.82`): `book_age=0.0 ms`, `book_lag=18.51 ms`, `cl_twap_age=903.0 ms`。
- Recorder 1 Hz (`ts=1795081150`, `t=250s`, `up_ask1=0.83`): `up_book_age=205.41 ms`, `up_book_lag=2486.77 ms`。
- Recorder 1 Hz (`ts=1795081151`, `t=251s`, `up_ask1=0.79`): `up_book_age=0.07 ms`, `up_book_lag=454.23 ms`。
- Recorder 1 Hz (`ts=1795081152`, `t=252s`, `up_ask1=0.87`): `up_book_age=0.88 ms`, `up_book_lag=19.36 ms`。
- 凍結檔案指紋檢查：`10/12` 符合（`ok=False`，5m 核心 `10/10` `core_5m_ok=True`）。
