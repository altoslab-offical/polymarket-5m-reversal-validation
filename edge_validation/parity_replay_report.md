# Path A (Recorder) vs Path B (Runner) Full Per-Second & ENTER Parity Report

- Database rows replayed: `108339` across `36113` 1Hz ticks
- Per-second full-decision parity (`action`, `reason`, `|dz|<=0.02`, `|dask|<=0.001`): `108339` / `108339` (**`100.0000%`** | gate `>= 99.5%`: **PASS**)
- Per-second `ENTER` union: `14` / `14` (**`100.00%`**)
- Coin-window signal union (`ENTER`): `5` / `5` (**`100.00%`** | sample gate `>= 100`: **PENDING (5/100)**)
- Single-slot window union (`ENTER`): `5` / `5` (**`100.00%`**)
- Parity gate (`per_sec >= 99.5%` & `ENTER >= 99.0%`): **PASS** (Overall ENTER sample gate: **PENDING (5/100)**)

## Per-Decision Group Agreement (`Path A Recorder` Baseline)

| Decision Group (`Action:Reason`) | Rows | Matched | Agreement |
|---|---:|---:|---:|
| `SKIP:WARMUP` | 88830 | 88830 | 100.0000% |
| `SKIP:OUTSIDE_ENTRY_WINDOW` | 12295 | 12295 | 100.0000% |
| `SKIP:Z_LOW` | 3685 | 3685 | 100.0000% |
| `SKIP:FLOW_BLOCKED` | 1407 | 1407 | 100.0000% |
| `SKIP:ASK_OUT_OF_BAND` | 1087 | 1087 | 100.0000% |
| `SKIP:NO_BOOK` | 704 | 704 | 100.0000% |
| `SKIP:STALE_BN` | 159 | 159 | 100.0000% |
| `SKIP:STALE_CL_TWAP` | 110 | 110 | 100.0000% |
| `SKIP:NO_STRIKE` | 22 | 22 | 100.0000% |
| `ENTER:OK` | 14 | 14 | 100.0000% |
| `SKIP:STALE_BOOK` | 14 | 14 | 100.0000% |
| `SKIP:STALE_CL_SPOT` | 10 | 10 | 100.0000% |
| `SKIP:DIVERGENCE_HIGH` | 2 | 2 | 100.0000% |

## Discrepancies

Zero discrepancies between Path A (Recorder) and Path B (Runner) across all replayed rows.
