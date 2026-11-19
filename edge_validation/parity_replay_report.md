# Path A (Recorder) vs Path B (Runner) Full Per-Second & ENTER Parity Report

- Database rows replayed: `195813` across `65271` 1Hz ticks
- Per-second full-decision parity (`action`, `reason`, `|dz|<=0.02`, `|dask|<=0.001`): `195566` / `195813` (**`99.8739%`** | gate `>= 99.5%`: **PASS**)
- Per-second `ENTER` union: `155` / `155` (**`100.00%`**)
- Coin-window signal union (`ENTER`): `38` / `38` (**`100.00%`** | sample gate `>= 100`: **PENDING (38/100)**)
- Single-slot window union (`ENTER`): `32` / `32` (**`100.00%`**)
- Parity gate (`per_sec >= 99.5%` & `ENTER >= 99.0%`): **PASS** (Overall ENTER sample gate: **PENDING (38/100)**)

## Per-Decision Group Agreement (`Path A Recorder` Baseline)

| Decision Group (`Action:Reason`) | Rows | Matched | Agreement |
|---|---:|---:|---:|
| `SKIP:OUTSIDE_ENTRY_WINDOW` | 88293 | 88160 | 99.8494% |
| `SKIP:WARMUP` | 53859 | 53859 | 100.0000% |
| `SKIP:Z_LOW` | 23246 | 23246 | 100.0000% |
| `SKIP:FLOW_BLOCKED` | 10623 | 10593 | 99.7176% |
| `SKIP:ASK_OUT_OF_BAND` | 8014 | 8014 | 100.0000% |
| `SKIP:NO_BOOK` | 6691 | 6650 | 99.3872% |
| `SKIP:STALE_BN` | 1931 | 1931 | 100.0000% |
| `SKIP:NO_STRIKE` | 1564 | 1562 | 99.8721% |
| `SKIP:STALE_CL_TWAP` | 1135 | 1135 | 100.0000% |
| `SKIP:STALE_BOOK` | 220 | 179 | 81.3636% |
| `ENTER:OK` | 155 | 155 | 100.0000% |
| `SKIP:DIVERGENCE_HIGH` | 48 | 48 | 100.0000% |
| `SKIP:STALE_CL_SPOT` | 32 | 32 | 100.0000% |
| `SKIP:SPREAD_WIDE` | 2 | 2 | 100.0000% |

## Discrepancies

- `sec` `1795009500` `BTCUSDT`: recorder=`{'action': 'SKIP', 'reason': 'OUTSIDE_ENTRY_WINDOW', 'z': 0.22607634411573652, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'STALE_BOOK', 'z': None, 'ask': None}`
- `sec` `1795025403` `SOLUSDT`: recorder=`{'action': 'SKIP', 'reason': 'STALE_BOOK', 'z': None, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'OUTSIDE_ENTRY_WINDOW', 'z': 0.19396393875381437, 'ask': None}`
- `sec` `1795028401` `SOLUSDT`: recorder=`{'action': 'SKIP', 'reason': 'STALE_BOOK', 'z': None, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'NO_STRIKE', 'z': None, 'ask': None}`
- `sec` `1795029302` `SOLUSDT`: recorder=`{'action': 'SKIP', 'reason': 'STALE_BOOK', 'z': None, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'OUTSIDE_ENTRY_WINDOW', 'z': 0.3099937812142712, 'ask': None}`
- `sec` `1795030201` `SOLUSDT`: recorder=`{'action': 'SKIP', 'reason': 'STALE_BOOK', 'z': None, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'NO_STRIKE', 'z': None, 'ask': None}`
- `sec` `1795030501` `ETHUSDT`: recorder=`{'action': 'SKIP', 'reason': 'STALE_BOOK', 'z': None, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'NO_STRIKE', 'z': None, 'ask': None}`
- `sec` `1795031403` `ETHUSDT`: recorder=`{'action': 'SKIP', 'reason': 'STALE_BOOK', 'z': None, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'OUTSIDE_ENTRY_WINDOW', 'z': 0.22883356388214604, 'ask': None}`
- `sec` `1795031701` `ETHUSDT`: recorder=`{'action': 'SKIP', 'reason': 'STALE_BOOK', 'z': None, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'NO_STRIKE', 'z': None, 'ask': None}`
- `sec` `1795032602` `SOLUSDT`: recorder=`{'action': 'SKIP', 'reason': 'STALE_BOOK', 'z': None, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'NO_STRIKE', 'z': None, 'ask': None}`
- `sec` `1795032903` `BTCUSDT`: recorder=`{'action': 'SKIP', 'reason': 'STALE_BOOK', 'z': None, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'OUTSIDE_ENTRY_WINDOW', 'z': 0.2238873108147815, 'ask': None}`
- `sec` `1795032904` `SOLUSDT`: recorder=`{'action': 'SKIP', 'reason': 'STALE_BOOK', 'z': None, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'OUTSIDE_ENTRY_WINDOW', 'z': 0.39943551606787875, 'ask': None}`
- `sec` `1795033502` `ETHUSDT`: recorder=`{'action': 'SKIP', 'reason': 'STALE_BOOK', 'z': None, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'OUTSIDE_ENTRY_WINDOW', 'z': 0.13057721442451056, 'ask': None}`
- `sec` `1795034104` `ETHUSDT`: recorder=`{'action': 'SKIP', 'reason': 'STALE_BOOK', 'z': None, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'OUTSIDE_ENTRY_WINDOW', 'z': 0.007621118064793129, 'ask': None}`
- `sec` `1795037402` `SOLUSDT`: recorder=`{'action': 'SKIP', 'reason': 'STALE_BOOK', 'z': None, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'OUTSIDE_ENTRY_WINDOW', 'z': 0.10723977411885531, 'ask': None}`
- `sec` `1795037702` `BTCUSDT`: recorder=`{'action': 'SKIP', 'reason': 'STALE_BOOK', 'z': None, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'OUTSIDE_ENTRY_WINDOW', 'z': 0.41543141443523823, 'ask': None}`
- `sec` `1795037703` `SOLUSDT`: recorder=`{'action': 'SKIP', 'reason': 'STALE_BOOK', 'z': None, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'OUTSIDE_ENTRY_WINDOW', 'z': 0.11773985767979257, 'ask': None}`
- `sec` `1795038002` `SOLUSDT`: recorder=`{'action': 'SKIP', 'reason': 'STALE_BOOK', 'z': None, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'NO_STRIKE', 'z': None, 'ask': None}`
- `sec` `1795038603` `SOLUSDT`: recorder=`{'action': 'SKIP', 'reason': 'STALE_BOOK', 'z': None, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'OUTSIDE_ENTRY_WINDOW', 'z': 0.21813963375656895, 'ask': None}`
- `sec` `1795040401` `ETHUSDT`: recorder=`{'action': 'SKIP', 'reason': 'STALE_BOOK', 'z': None, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'NO_STRIKE', 'z': None, 'ask': None}`
- `sec` `1795041001` `BTCUSDT`: recorder=`{'action': 'SKIP', 'reason': 'STALE_BOOK', 'z': None, 'ask': None}` vs runner=`{'action': 'SKIP', 'reason': 'NO_STRIKE', 'z': None, 'ask': None}`
