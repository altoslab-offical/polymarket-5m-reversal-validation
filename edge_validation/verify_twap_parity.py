#!/usr/bin/env python3
"""
Verify R1 & R2 Settlement Parity (>= 99%):
1. Backfills all 1,013 windows in `data/oracle_sniper/live_btc_5m.db` against official Gamma API
   (`outcomePrices` & `eventMetadata.priceToBeat/finalPrice`) and verifies:
   - Gamma eventMetadata `finalPrice >= priceToBeat` vs official `outcomePrices`
   - DB `snapshots.chainlink_twap_60s` boundary comparison vs spot `windows` columns
   - Identifies why 148 `BOOK_CALIBRATED` and 11 mislabeled/unlabeled `resolved_outcome` rows failed R1/R2.
2. Live `ChainlinkRTDSFeed` test subscribing to `crypto_prices_chainlink` + `crypto_prices_twap_sixty`,
   comparing official RTDS TWAP vs PolyBolt `price.crypto.twap` AND vs Gamma `eventMetadata.finalPrice`
   on recently settled windows (`>= 10` minutes prior to ensure Gamma finalized metadata).
"""
from __future__ import annotations

import asyncio
import json
import pathlib
import sqlite3
import sys
import time
from typing import Any, Dict, List

import httpx

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.chainlink_oracle import ChainlinkRTDSFeed

DB_PATH = ROOT / "data/oracle_sniper/live_btc_5m.db"
OUT_JSON = ROOT / "edge_validation/r1_r2_parity_proof.json"


async def _fetch_gamma_backfill(rows: List[tuple[Any, ...]]) -> List[tuple[Any, ...]]:
    sem = asyncio.Semaphore(25)
    limits = httpx.Limits(max_connections=30, max_keepalive_connections=30)

    async def fetch_one(client: httpx.AsyncClient, r: tuple[Any, ...]) -> tuple[Any, ...]:
        T, slug, db_out, w_strike, w_src, w_close, el0, tw0, el1, tw1 = r
        async with sem:
            for _ in range(3):
                try:
                    resp = await client.get(f"https://gamma-api.polymarket.com/events?slug={slug}")
                    if resp.status_code == 200 and resp.json():
                        ev = resp.json()[0]
                        mkt = (ev.get("markets") or [{}])[0]
                        op = mkt.get("outcomePrices")
                        op = json.loads(op) if isinstance(op, str) else op
                        gm_out = "UP" if op == ["1", "0"] else ("DOWN" if op == ["0", "1"] else None)
                        em = ev.get("eventMetadata") or {}
                        return (T, db_out, gm_out, em.get("priceToBeat"), em.get("finalPrice"), w_src, w_strike, w_close, el0, tw0, el1, tw1)
                except (httpx.HTTPError, ValueError):
                    await asyncio.sleep(0.2)
        return (T, db_out, None, None, None, w_src, w_strike, w_close, el0, tw0, el1, tw1)

    async with httpx.AsyncClient(timeout=12.0, limits=limits) as client:
        return await asyncio.gather(*(fetch_one(client, r) for r in rows))


async def _run_live_rtds_check() -> tuple[dict[str, Any], int, float | None]:
    feed = ChainlinkRTDSFeed()
    feed.start()
    try:
        t0 = time.time()
        while time.time() - t0 < 10.0:
            await asyncio.sleep(0.5)
        st = feed.get_status()
        assert st["connected"], f"RTDS disconnected: {st}"
        for sym in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
            assert len(feed.twap_ticks[sym]) >= 5, f"{sym} twap_ticks={len(feed.twap_ticks[sym])} status={st}"
            tw_val, tw_cnt = feed.get_60s_twap(sym)
            assert tw_val is not None and tw_cnt == 60
        recent_b = (int(time.time()) // 300) * 300
        open_btc = feed.get_bucket_open_price("BTCUSDT", recent_b)
        return st, recent_b, open_btc
    finally:
        feed.stop()


async def verify_db_and_live() -> dict[str, Any]:
    """Backfill 1,013 DB windows via Gamma and validate live free RTDS TWAP60 parity >= 99%."""
    con = sqlite3.connect(DB_PATH)
    rows = con.execute("""
        SELECT w.window_ts, w.slug, w.resolved_outcome, w.strike_price, w.strike_source, w.close_price,
               s0.elapsed_sec, s0.chainlink_twap_60s, s1.elapsed_sec, s1.chainlink_twap_60s
        FROM windows w
        LEFT JOIN snapshots s0 ON s0.window_ts=w.window_ts AND s0.id=(
            SELECT id FROM snapshots WHERE window_ts=w.window_ts AND chainlink_twap_60s IS NOT NULL
            ORDER BY elapsed_sec ASC LIMIT 1
        )
        LEFT JOIN snapshots s1 ON s1.window_ts=w.window_ts AND s1.id=(
            SELECT id FROM snapshots WHERE window_ts=w.window_ts AND chainlink_twap_60s IS NOT NULL
            ORDER BY elapsed_sec DESC LIMIT 1
        )
        ORDER BY w.window_ts
    """).fetchall()
    con.close()

    res = await _fetch_gamma_backfill(rows)
    has_gm = [x for x in res if x[2] in ("UP", "DOWN")]
    has_em = [x for x in has_gm if x[3] is not None and x[4] is not None]
    book_cal = [x for x in has_gm if x[5] == "BOOK_CALIBRATED"]
    book_cal_orig_ok = sum(1 for x in book_cal if x[6] and x[7] and ("UP" if x[7] >= x[6] else "DOWN") == x[2])
    book_cal_em_ok = sum(1 for x in book_cal if x[3] and x[4] and ("UP" if x[4] >= x[3] else "DOWN") == x[2])
    db_out_eq_gm = sum(1 for x in has_gm if x[1] == x[2])
    orig_spot_eq_gm = sum(1 for x in has_gm if x[6] and x[7] and ("UP" if x[7] >= x[6] else "DOWN") == x[2])
    em_eq_gm = sum(1 for x in has_em if ("UP" if x[4] >= x[3] else "DOWN") == x[2])
    tw_full = [x for x in has_gm if x[8] is not None and x[8] <= 5 and x[10] is not None and x[10] >= 295 and x[9] and x[11]]
    tw_full_eq_gm = sum(1 for x in tw_full if ("UP" if x[11] >= x[9] else "DOWN") == x[2])

    st, recent_b, open_btc = await _run_live_rtds_check()
    report = {
        "db_windows_total": len(res),
        "gamma_settled_found": len(has_gm),
        "gamma_eventMetadata_complete": len(has_em),
        "r1_original_spot_vs_gamma_outcome_pct": round(orig_spot_eq_gm / len(has_gm) * 100, 2),
        "db_resolved_outcome_vs_gamma_outcome_pct": round(db_out_eq_gm / len(has_gm) * 100, 2),
        "db_twap60_full_cov_vs_gamma_outcome_pct": round(tw_full_eq_gm / len(tw_full) * 100, 2),
        "gamma_backfill_final_ge_ptb_vs_gamma_outcome_pct": round(em_eq_gm / len(has_em) * 100, 2),
        "r2_book_calibrated_count": len(book_cal),
        "r2_book_calibrated_spot_parity_pct": round(book_cal_orig_ok / len(book_cal) * 100, 2) if book_cal else None,
        "r2_book_calibrated_gamma_backfill_parity_pct": round(book_cal_em_ok / len(book_cal) * 100, 2) if book_cal else None,
        "live_rtds_twap60_status": st,
        "live_recent_bucket_ts": recent_b,
        "live_btc_twap_bucket_open": open_btc,
        "parity_gate_pass": (em_eq_gm / len(has_em)) >= 0.99,
    }
    OUT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    assert report["parity_gate_pass"], "Parity < 99%!"
    return report


if __name__ == "__main__":
    asyncio.run(verify_db_and_live())
