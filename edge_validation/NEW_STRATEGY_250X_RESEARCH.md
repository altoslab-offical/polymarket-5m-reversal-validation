# Track 2：`$40U → $10,000U`（250x）替代賽道探索與兩年 Walk-Forward 篩選

> **數據區間**：`data/{BTC,ETH,SOL}USDT_5m.parquet`（`217,650` 根/幣 ≈ `755.7` 天） | IS 前半（`377.9` 天） vs OOS 後半（`377.9` 天）
> **執行紀律**：Bar `i` 收盤產生訊號 → Bar `i+1` 開盤進場（零前視）；單根同時觸及 SL/TP 視為 **先損後賺**；主口徑來回成本 **`14 bps`**（10 bps taker + 4 bps 滑價），另附 `8 bps` 敏感度。
> **Pass Gate**：`OOS n >= 200`、`OOS PF >= 1.25`、`OOS Sharpe >= 1.2`、`OOS MaxDD < 30%`、IS 與 OOS 均正期望值。

## 1. Part A：線性合約三大策略家族 2 年嚴格 IS/OOS 篩選結果

| Strategy | Gate | IS n | IS PF | IS Sharpe | OOS n | OOS /d | OOS WR% | OOS PF | OOS Exp(R) | OOS bps | OOS Sharpe | OOS MaxDD% | OOS @8bps PF/Sharpe |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `C1_15m_Squeeze_Breakout_Macro` | **REJECTED (OOS FAIL)** | 743 | 0.775 | -2.67 | 760 | 2.01 | 33.03% | 0.619 | -0.3268 | -16.56 | -4.79 | 92.63% | 0.745 / -2.96 |
| `C1_15m_Squeeze_Breakout_AllDir` | **REJECTED (OOS FAIL)** | 1259 | 0.8 | -3.07 | 1319 | 3.49 | 31.99% | 0.587 | -0.3583 | -16.85 | -6.59 | 99.25% | 0.71 / -4.39 |
| `C1_1h_Squeeze_Breakout_Macro` | **REJECTED (OOS FAIL)** | 164 | 1.02 | 0.1 | 166 | 0.44 | 31.93% | 0.732 | -0.2063 | -21.96 | -1.48 | 34.68% | 0.799 / -1.06 |
| `C2_5m_Climax_MR_All` | **REJECTED (OOS FAIL)** | 2511 | 0.446 | -13.92 | 2695 | 7.13 | 35.55% | 0.379 | -0.549 | -15.4 | -15.06 | 100.0% | 0.571 / -9.93 |
| `C2_15m_Climax_MR_All` | **REJECTED (OOS FAIL)** | 792 | 0.627 | -4.9 | 861 | 2.28 | 39.61% | 0.582 | -0.2947 | -13.44 | -5.93 | 93.09% | 0.728 / -3.67 |
| `C2_15m_Climax_MR_MacroPullback` | **REJECTED (OOS FAIL)** | 239 | 0.62 | -2.84 | 267 | 0.71 | 40.45% | 0.573 | -0.2803 | -15.09 | -3.13 | 55.69% | 0.75 / -1.7 |
| `C3_5m_BTC_Lead_Follow` | **REJECTED (OOS FAIL)** | 442 | 0.375 | -7.06 | 410 | 1.09 | 26.59% | 0.207 | -0.8523 | -12.8 | -6.36 | 97.13% | 0.416 / -5.4 |
| `C3_15m_BTC_Lead_Follow` | **REJECTED (OOS FAIL)** | 96 | 0.342 | -3.54 | 65 | 0.17 | 40.0% | 0.632 | -0.2038 | -7.88 | -1.4 | 14.47% | 0.908 / -0.29 |
| `C3_5m_BTC_Lead_Fade` | **REJECTED (OOS FAIL)** | 444 | 0.41 | -6.55 | 412 | 1.09 | 25.0% | 0.142 | -1.0019 | -14.54 | -6.14 | 98.49% | 0.294 / -5.57 |

## 2. `$40U → $10,000U` 180 天分數凱利 Monte Carlo（`10,000` 路徑，上限 10x 槓桿，最小名目 `$5U`）

| Strategy | Status | Kelly 倍數 (Risk%/單) | 180天達 `$10k` 機率 | 達標中位天數 (P50) | 回撤 >=50% 機率 |
|---|---|---|---:|---:|---:|
| `C3_15m_BTC_Lead_Follow` | `REJECTED - Reference Only` | `0.25x` (`0.5%`) | **0.0%** | None 天 | 0.0% |
| `C3_15m_BTC_Lead_Follow` | `REJECTED - Reference Only` | `0.5x` (`0.5%`) | **0.0%** | None 天 | 0.0% |

## 3. Part B：Polymarket 非 5m 加密盤口即時探測（`5m` vs `15m` vs `1h`）

> 即時呼叫公開免費 Gamma/CLOB API，比較 5m 與 15m/1h 的 `+2¢` 盤口深度、費率與 TWAP 結算規則。

| TF | Coin | Active | 當前 Volume | Liquidity | Up Ask | `+2¢` Ask Depth (USD) | `base_fee` | TWAP Rule |
|---|---|---|---:|---:|---:|---:|---:|---|
| `5m` | `BTC` | True | $0 | $23917 | 0.89 | **$2778.27** | `1000` | `60s_TWAP` |
| `5m` | `ETH` | True | $0 | $10170 | 0.88 | **$31.30** | `1000` | `60s_TWAP` |
| `5m` | `SOL` | True | $0 | $6466 | 0.88 | **$282.14** | `1000` | `60s_TWAP` |
| `15m` | `BTC` | True | $11 | $11620 | 0.74 | **$2705.53** | `1000` | `60s_TWAP` |
| `15m` | `ETH` | True | $0 | $9043 | 0.75 | **$139.10** | `1000` | `60s_TWAP` |
| `15m` | `SOL` | True | $2 | $3673 | 0.7 | **$88.01** | `1000` | `60s_TWAP` |
| `1h` | `BTC` | True | $2377 | $5065 | 0.53 | **$302.65** | `1000` | `60s_TWAP` |
| `1h` | `ETH` | True | $235 | $2350 | 0.34 | **$40.40** | `1000` | `60s_TWAP` |
| `1h` | `SOL` | True | $33 | $263 | 0.24 | **$29.70** | `1000` | `60s_TWAP` |
