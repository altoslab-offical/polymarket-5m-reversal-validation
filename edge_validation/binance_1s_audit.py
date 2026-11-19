"""Official Binance 1s kline integrity & zero-trade parity audit for Section 4.2."""
from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
import urllib.error
import urllib.request
import zipfile

import httpx
import numpy as np
import pandas as pd

SYMBOLS: Tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
DEFAULT_CACHE_DIR: Path = Path(__file__).resolve().parent.parent / "data" / "validation_42" / "binance_1s_cache"
VISION_BASE: str = "https://data.binance.vision/data/spot/daily/klines"
REST_KLINES_URL: str = "https://api.binance.com/api/v3/klines"
CLOSE_MATCH_BPS_TOL: float = 2.0
CLOSE_MATCH_GATE_PCT: float = 99.0
ZERO_DIFF_GATE_PP: float = 5.0

KlineMap = Dict[int, Tuple[float, int]]
KlineLoader = Callable[[str, str, int, int], Tuple[KlineMap, str]]


def normalize_open_ts_sec(raw_ts: int) -> int:
    """Normalize Binance open_time from microseconds (16d), milliseconds (13d), or seconds."""
    if raw_ts > 10**14:
        return raw_ts // 1_000_000
    if raw_ts > 10**11:
        return raw_ts // 1_000
    return raw_ts


def parse_binance_1s_csv_bytes(raw: bytes) -> KlineMap:
    """Parse Binance spot 1s kline CSV bytes into `{open_ts_sec: (close, trade_count)}`."""
    out: KlineMap = {}
    for line in raw.decode("utf-8", errors="ignore").splitlines():
        parts = line.split(",")
        if len(parts) < 9:
            continue
        head = parts[0].strip()
        if not head or not head.lstrip("-").isdigit():
            continue
        try:
            ts_sec = normalize_open_ts_sec(int(head))
            close_px = float(parts[4])
            trades = int(float(parts[8]))
        except ValueError:
            continue
        if close_px > 0:
            out[ts_sec] = (close_px, trades)
    return out


def fetch_vision_1s_day(
    sym: str, utc_day: str, cache_dir: Path = DEFAULT_CACHE_DIR
) -> Optional[KlineMap]:
    """Download (and cache) completed-day Binance 1s spot klines from `data.binance.vision`."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    zip_path = cache_dir / f"{sym}-1s-{utc_day}.zip"
    if not zip_path.exists():
        tmp_path = cache_dir / f"{sym}-1s-{utc_day}.zip.tmp"
        url = f"{VISION_BASE}/{sym}/1s/{sym}-1s-{utc_day}.zip"
        try:
            with urllib.request.urlopen(url, timeout=20) as resp:
                tmp_path.write_bytes(resp.read())
            os.replace(tmp_path, zip_path)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError):
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            return None
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")] or zf.namelist()
            return parse_binance_1s_csv_bytes(zf.read(csv_names[0])) if csv_names else None
    except (zipfile.BadZipFile, OSError, KeyError):
        zip_path.unlink(missing_ok=True)
        return None


def _ingest_rest_rows(out: KlineMap, rows: List[Any]) -> None:
    for k in rows:
        ts_s = normalize_open_ts_sec(int(k[0]))
        close_px = float(k[4])
        if close_px > 0:
            out[ts_s] = (close_px, int(k[8]))


def fetch_rest_1s_range(
    sym: str, start_sec: int, end_sec: int, max_chunks: int = 90
) -> Optional[KlineMap]:
    """Fetch up to `max_chunks * 1000` recent 1s klines from public Binance REST API."""
    out: KlineMap = {}
    cur_end = max(start_sec, end_sec)
    any_ok = False
    try:
        with httpx.Client(timeout=10.0) as client:
            for _ in range(max_chunks):
                if cur_end < start_sec:
                    break
                chunk_start = max(start_sec, cur_end - 999)
                params = {
                    "symbol": sym, "interval": "1s",
                    "startTime": chunk_start * 1000, "endTime": cur_end * 1000, "limit": 1000,
                }
                resp = client.get(REST_KLINES_URL, params=params)
                resp.raise_for_status()
                any_ok = True
                _ingest_rest_rows(out, resp.json())
                cur_end = chunk_start - 1
    except (httpx.HTTPError, ValueError, KeyError, IndexError, TypeError):
        if not any_ok:
            return None
    return out


def default_kline_loader(sym: str, utc_day: str, min_sec: int, max_sec: int) -> Tuple[KlineMap, str]:
    """Load official Binance 1s klines (`data.binance.vision` for past UTC days, REST for today)."""
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if utc_day < today_utc:
        km = fetch_vision_1s_day(sym, utc_day)
        if km:
            return km, "binance.vision"
    km_rest = fetch_rest_1s_range(sym, min_sec, max_sec)
    if km_rest:
        return km_rest, "api.binance.com/1s"
    return {}, "OFFLINE_SKIP"


def _audit_status(
    n_matched: int, close_pct: Optional[float], zero_diff_pp: Optional[float], source: str
) -> Tuple[bool, bool, str]:
    if source == "OFFLINE_SKIP" or n_matched == 0 or close_pct is None or zero_diff_pp is None:
        return False, False, "OFFLINE_SKIP" if source == "OFFLINE_SKIP" else "PENDING"
    close_pass = close_pct >= CLOSE_MATCH_GATE_PCT
    zero_pass = abs(zero_diff_pp) <= ZERO_DIFF_GATE_PP
    if not close_pass:
        return False, zero_pass, "FAIL_CLOSE"
    if not zero_pass:
        return True, False, "ALERT_ZERO"
    return True, True, "PASS"


def _format_audit_row(
    meta: Tuple[str, str, str], diffs: List[float], counts: Tuple[int, int, int, int, int]
) -> Dict[str, Any]:
    utc_day, sym, source = meta
    n_matched, exact_c, rec_z, kl_z, max_q = counts
    if n_matched == 0 or not diffs:
        close_pass, zero_pass, st = _audit_status(0, None, None, source)
        return {
            "utc_day": utc_day, "symbol": sym, "source": source, "matched_sec": 0,
            "close_match_2bps_pct": None, "exact_match_pct": None, "p99_diff_bps": None,
            "max_diff_bps": None, "rec_zero_pct": None, "kline_zero_pct": None,
            "zero_diff_pp": None, "max_quiet_sec": max_q, "close_pass": close_pass,
            "zero_pass": zero_pass, "status": st,
        }
    arr = np.asarray(diffs, dtype=float)
    close_2bps = round(float(np.mean(arr <= CLOSE_MATCH_BPS_TOL) * 100.0), 2)
    exact_pct = round(exact_c / n_matched * 100.0, 2)
    rec_z_pct = round(rec_z / n_matched * 100.0, 2)
    kl_z_pct = round(kl_z / n_matched * 100.0, 2)
    z_diff = round(rec_z_pct - kl_z_pct, 2)
    close_pass, zero_pass, st = _audit_status(n_matched, close_2bps, z_diff, source)
    return {
        "utc_day": utc_day, "symbol": sym, "source": source, "matched_sec": n_matched,
        "close_match_2bps_pct": close_2bps, "exact_match_pct": exact_pct,
        "p99_diff_bps": round(float(np.percentile(arr, 99)), 2),
        "max_diff_bps": round(float(np.max(arr)), 2),
        "rec_zero_pct": rec_z_pct, "kline_zero_pct": kl_z_pct,
        "zero_diff_pp": z_diff, "max_quiet_sec": max_q,
        "close_pass": close_pass, "zero_pass": zero_pass, "status": st,
    }


def _compare_symbol_day(
    rows: pd.DataFrame, klines: KlineMap, utc_day: str, sym: str, source: str
) -> Tuple[Dict[str, Any], List[float], Tuple[int, int, int, int, int]]:
    rows_s = rows.sort_values("ts_sec")
    diffs: List[float] = []
    exact_c = rec_z = kl_z = streak = max_q = 0
    for kts_raw, px_raw, tr_raw in zip(rows_s["kline_ts"], rows_s["bn_close"], rows_s["bn_trades"]):
        n_tr = int(tr_raw) if pd.notna(tr_raw) else 0
        streak = streak + 1 if n_tr == 0 else 0
        max_q = max(max_q, streak)
        km = klines.get(int(kts_raw))
        if km is None or km[0] <= 0:
            continue
        k_px, k_tr = km
        px = float(px_raw)
        diffs.append(abs(px - k_px) / k_px * 10_000.0)
        if abs(px - k_px) < 1e-9:
            exact_c += 1
        if n_tr == 0:
            rec_z += 1
        if k_tr == 0:
            kl_z += 1
    counts = (len(diffs), exact_c, rec_z, kl_z, max_q)
    return _format_audit_row((utc_day, sym, source), diffs, counts), diffs, counts


def _select_audit_scope(df_s: pd.DataFrame, count_start_ts: int) -> Tuple[pd.DataFrame, str]:
    has_bn = df_s[df_s["bn_age_sec"].notna() & df_s["bn_close"].notna() & (df_s["bn_close"] > 0)].copy()
    if len(has_bn) == 0:
        return has_bn, "post_reset" if count_start_ts > 0 else "none"
    if count_start_ts > 0:
        return has_bn[has_bn["window_ts"] >= count_start_ts].copy(), "post_reset"
    max_ts = int(has_bn["ts_sec"].max())
    return has_bn[has_bn["ts_sec"] >= max_ts - 86400].copy(), "all_ms_recent"


def audit_recorder_vs_binance_1s(
    df_s: pd.DataFrame, count_start_ts: int, kline_loader: Optional[KlineLoader] = None
) -> Dict[str, Any]:
    """Compare recorded 1Hz Binance closes & zero-trade seconds against official 1s klines (`ts_sec - 1`)."""
    base = {"align_shift_sec": -1, "close_tol_bps": CLOSE_MATCH_BPS_TOL, "close_gate_pct": CLOSE_MATCH_GATE_PCT, "zero_gate_pp": ZERO_DIFF_GATE_PP}
    if len(df_s) == 0 or "bn_age_sec" not in df_s.columns:
        pending = [_format_audit_row(("PENDING", "ALL", "PENDING"), [], (0, 0, 0, 0, 0))]
        return {**base, "scope": "post_reset" if count_start_ts > 0 else "none", "rows": pending, "overall_close_pass": None, "overall_pass": None}
    sub, scope = _select_audit_scope(df_s, count_start_ts)
    if len(sub) == 0:
        pending = [_format_audit_row(("PENDING", "ALL", "PENDING"), [], (0, 0, 0, 0, 0))]
        return {**base, "scope": scope, "rows": pending, "overall_close_pass": None, "overall_pass": None}
    sub["kline_ts"] = sub["ts_sec"].astype(int) - 1
    sub["utc_day"] = pd.to_datetime(sub["kline_ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
    loader = kline_loader or default_kline_loader
    out_rows: List[Dict[str, Any]] = []
    all_diffs: List[float] = []
    p_match = p_ex = p_rz = p_kz = p_mq = 0
    for utc_day in sorted(sub["utc_day"].unique()):
        day_df = sub[sub["utc_day"] == utc_day]
        for sym in SYMBOLS:
            g = day_df[day_df["symbol"] == sym]
            if len(g) == 0:
                continue
            klines, src = loader(sym, utc_day, int(g["kline_ts"].min()), int(g["kline_ts"].max()))
            row, diffs, (nm, ex, rz, kz, mq) = _compare_symbol_day(g, klines, utc_day, sym, src)
            out_rows.append(row)
            all_diffs.extend(diffs)
            p_match, p_ex, p_rz, p_kz, p_mq = p_match + nm, p_ex + ex, p_rz + rz, p_kz + kz, max(p_mq, mq)
    active = [r for r in out_rows if r["matched_sec"] > 0]
    src_all = "mixed" if len({r["source"] for r in active}) > 1 else (active[0]["source"] if active else (out_rows[0]["source"] if out_rows else "NONE"))
    all_row = _format_audit_row(("ALL", "ALL", src_all), all_diffs, (p_match, p_ex, p_rz, p_kz, p_mq))
    out_rows.append(all_row)
    return {
        "scope": scope, "align_shift_sec": -1,
        "close_tol_bps": CLOSE_MATCH_BPS_TOL, "close_gate_pct": CLOSE_MATCH_GATE_PCT, "zero_gate_pp": ZERO_DIFF_GATE_PP,
        "rows": out_rows,
        "overall_close_pass": all(r["close_pass"] for r in active) if active else None,
        "overall_pass": all(r["status"] == "PASS" for r in active) if active else None,
    }
