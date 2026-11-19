# Path A (Recorder) vs Path B (Runner) Full Per-Second & ENTER Parity Report

- Database rows replayed: `260433` across `86811` 1Hz ticks
- Per-second full-decision parity (`action`, `reason`, `|dz|<=0.02`, `|dask|<=0.001`): `260225` / `260433` (**`99.9201%`** | gate `>= 99.5%`: **PASS**)
- Per-second `ENTER` union: `158` / `158` (**`100.00%`**)
- Coin-window signal union (`ENTER`): `41` / `41` (**`100.00%`** | sample gate `>= 100`: **PENDING (41/100)**)
- Single-slot window union (`ENTER`): `35` / `35` (**`100.00%`**)
- Parity gate (`per_sec >= 99.5%` & `ENTER >= 99.0%`): **PASS** (Overall ENTER sample gate: **PENDING (41/100)**)

## Per-Decision Group Agreement (`Path A Recorder` Baseline)

| Decision Group (`Action:Reason`) | Rows | Matched | Agreement |
|---|---:|---:|---:|
| `SKIP:OUTSIDE_ENTRY_WINDOW` | 118431 | 118296 | 99.8860% |
| `SKIP:WARMUP` | 67338 | 67338 | 100.0000% |
| `SKIP:Z_LOW` | 31056 | 31056 | 100.0000% |
| `SKIP:FLOW_BLOCKED` | 14568 | 14538 | 99.7941% |
| `SKIP:ASK_OUT_OF_BAND` | 10667 | 10667 | 100.0000% |
| `SKIP:NO_BOOK` | 8991 | 8950 | 99.5440% |
| `SKIP:NO_STRIKE` | 4133 | 4131 | 99.9516% |
| `SKIP:STALE_BN` | 1931 | 1931 | 100.0000% |
| `SKIP:STALE_CL_TWAP` | 1637 | 1637 | 100.0000% |
| `SKIP:STALE_BOOK` | 1431 | 1431 | 100.0000% |
| `ENTER:OK` | 158 | 158 | 100.0000% |
| `SKIP:DIVERGENCE_HIGH` | 48 | 48 | 100.0000% |
| `SKIP:STALE_CL_SPOT` | 42 | 42 | 100.0000% |
| `SKIP:SPREAD_WIDE` | 2 | 2 | 100.0000% |

## Discrepancies

- `sec` `1795044008` `BTCUSDT`: recorder=`{'action': 'SKIP', 'reason': 'NO_STRIKE', 'z': None, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'OUTSIDE_ENTRY_WINDOW', 'z': 0.0027530303874281755, 'ask': None}`
- `sec` `1795044008` `ETHUSDT`: recorder=`{'action': 'SKIP', 'reason': 'NO_STRIKE', 'z': None, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'OUTSIDE_ENTRY_WINDOW', 'z': 0.021947557596192328, 'ask': None}`
- `sec` `1795044223` `BTCUSDT`: recorder=`{'action': 'SKIP', 'reason': 'NO_BOOK', 'z': 8.88242430715186, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'NO_BOOK', 'z': 8.902520650439447, 'ask': None}`
- `sec` `1795044225` `BTCUSDT`: recorder=`{'action': 'SKIP', 'reason': 'FLOW_BLOCKED', 'z': 9.161893077061213, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'FLOW_BLOCKED', 'z': 9.182964079819788, 'ask': None}`
- `sec` `1795044226` `BTCUSDT`: recorder=`{'action': 'SKIP', 'reason': 'NO_BOOK', 'z': 9.909193168630985, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'NO_BOOK', 'z': 9.931982851378345, 'ask': None}`
- `sec` `1795044227` `BTCUSDT`: recorder=`{'action': 'SKIP', 'reason': 'NO_BOOK', 'z': 9.999607362076672, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'NO_BOOK', 'z': 10.023034703973181, 'ask': None}`
- `sec` `1795044228` `BTCUSDT`: recorder=`{'action': 'SKIP', 'reason': 'NO_BOOK', 'z': 10.110383099181757, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'NO_BOOK', 'z': 10.1341732930341, 'ask': None}`
- `sec` `1795044229` `BTCUSDT`: recorder=`{'action': 'SKIP', 'reason': 'NO_BOOK', 'z': 10.285946926432898, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'NO_BOOK', 'z': 10.310117905768047, 'ask': None}`
- `sec` `1795044230` `BTCUSDT`: recorder=`{'action': 'SKIP', 'reason': 'NO_BOOK', 'z': 10.471804982691333, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'NO_BOOK', 'z': 10.496375647601317, 'ask': None}`
- `sec` `1795044231` `BTCUSDT`: recorder=`{'action': 'SKIP', 'reason': 'FLOW_BLOCKED', 'z': 10.627913321236463, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'FLOW_BLOCKED', 'z': 10.652903827977674, 'ask': None}`
- `sec` `1795044232` `BTCUSDT`: recorder=`{'action': 'SKIP', 'reason': 'FLOW_BLOCKED', 'z': 10.815253422077728, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'FLOW_BLOCKED', 'z': 10.840686261794872, 'ask': None}`
- `sec` `1795044233` `BTCUSDT`: recorder=`{'action': 'SKIP', 'reason': 'FLOW_BLOCKED', 'z': 11.01371065069235, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'FLOW_BLOCKED', 'z': 11.039610176735657, 'ask': None}`
- `sec` `1795044233` `SOLUSDT`: recorder=`{'action': 'SKIP', 'reason': 'NO_BOOK', 'z': 6.91205739671084, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'NO_BOOK', 'z': 6.9321617317491615, 'ask': None}`
- `sec` `1795044234` `BTCUSDT`: recorder=`{'action': 'SKIP', 'reason': 'FLOW_BLOCKED', 'z': 11.224006674451287, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'FLOW_BLOCKED', 'z': 11.250411032049461, 'ask': None}`
- `sec` `1795044234` `SOLUSDT`: recorder=`{'action': 'SKIP', 'reason': 'NO_BOOK', 'z': 6.934400528119775, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'NO_BOOK', 'z': 6.9550994484154565, 'ask': None}`
- `sec` `1795044235` `BTCUSDT`: recorder=`{'action': 'SKIP', 'reason': 'FLOW_BLOCKED', 'z': 11.449318377474103, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'FLOW_BLOCKED', 'z': 11.47625277848282, 'ask': None}`
- `sec` `1795044235` `SOLUSDT`: recorder=`{'action': 'SKIP', 'reason': 'NO_BOOK', 'z': 7.071728721590169, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'NO_BOOK', 'z': 7.092837561289765, 'ask': None}`
- `sec` `1795044236` `BTCUSDT`: recorder=`{'action': 'SKIP', 'reason': 'FLOW_BLOCKED', 'z': 11.760402739870068, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'FLOW_BLOCKED', 'z': 11.7878932023821, 'ask': None}`
- `sec` `1795044236` `SOLUSDT`: recorder=`{'action': 'SKIP', 'reason': 'NO_BOOK', 'z': 7.174749489476351, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'NO_BOOK', 'z': 7.196293108505357, 'ask': None}`
- `sec` `1795044237` `BTCUSDT`: recorder=`{'action': 'SKIP', 'reason': 'FLOW_BLOCKED', 'z': 12.088226692951176, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'FLOW_BLOCKED', 'z': 12.116359871628188, 'ask': None}`
