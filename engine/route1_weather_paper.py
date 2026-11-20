"""Route 1 $40U Paper-Money Daily Weather Dual-Engine Monitor & CLI (simulation only)."""
from __future__ import annotations

import argparse
import asyncio
import contextlib
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
import statistics
import time
from typing import Any, Optional
from zoneinfo import ZoneInfo

import aiohttp

from engine.route1_stealth_experts import DATA_API_BASE, GAMMA_API_BASE, StealthExpertScanner, _atomic_write_json
from engine.route1_weather_model import (
    TempBracket,
    WeatherEventSpec,
    WeatherOpportunity,
    bracket_contains_value,
    calibrate_member_maxima_c,
    compute_bracket_probabilities,
    compute_local_day_metar_max_c,
    extract_event_station_and_date,
    parse_bracket_range,
    parse_metar_temp_c,
    parse_metar_timestamp_utc,
    round_half_up,
    sweep_orderbook_vwap,
    evaluate_weather_opportunities,
)

PAPER_ONLY = True

ROOT_DIR = Path(__file__).resolve().parent.parent
ROUTE1_PAPER_STATE_PATH = ROOT_DIR / "data" / "route1_weather_paper_40u.json"
ROUTE1_STATION_CACHE_PATH = ROOT_DIR / "data" / "route1_station_cache.json"
ROUTE1_ENSEMBLE_CACHE_PATH = ROOT_DIR / "data" / "route1_ensemble_cache.json"

AVIATION_METAR_URL = "https://aviationweather.gov/api/data/metar"
AVIATION_STATION_URL = "https://aviationweather.gov/api/data/stationinfo"
HKO_RHRREAD_URL = "https://data.weather.gov.hk/weatherAPI/opendata/weather.php?dataType=rhrread&lang=en"
OPEN_METEO_ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
NWS_POINTS_URL = "https://api.weather.gov/points"
CLOB_BOOKS_URL = "https://clob.polymarket.com/books"

ENSEMBLE_TTL_SEC = 2700.0
MAX_OPEN_EVENTS = 6
MAX_DEPLOYED_FRAC = 0.60
MAX_TRADES_KEPT = 500
HKO_STATION = {"lat": 22.302, "lon": 114.174, "tz": "Asia/Hong_Kong", "name": "Hong Kong Observatory"}


def _utc_today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class Route1WeatherLedger:
    """Atomic JSON paper ledger tracking $40U balance, event caps, Brier calibration, and attribution."""

    def __init__(self, path: Path = ROUTE1_PAPER_STATE_PATH, initial_equity_u: float = 40.0) -> None:
        self.path = path
        self.paper_only = PAPER_ONLY
        self.initial_equity_u = float(initial_equity_u)
        self.last_io_error: str = ""
        self.state: dict[str, Any] = self._load()

    def _fresh(self, initial_u: Optional[float] = None) -> dict[str, Any]:
        init = float(initial_u if initial_u is not None else self.initial_equity_u)
        today = _utc_today_str()
        return {
            "paper_only": True,
            "initial_equity_u": init,
            "cash_u": init,
            "peak_equity_u": init,
            "wins": 0,
            "losses": 0,
            "daily": {"date": today, "start_equity_u": init, "pnl_usd": 0.0, "trades": 0, "halted": False, "halt_reason": ""},
            "open_positions": {},
            "closed_trades": [],
            "calibration": {"n": 0, "model_brier_sum": 0.0, "market_brier_sum": 0.0},
            "attribution": {
                "ENSEMBLE_SOLO": {"opened": 0, "trades": 0, "wins": 0, "pnl_usd": 0.0},
                "EXPERT_CONFLUENCE": {"opened": 0, "trades": 0, "wins": 0, "pnl_usd": 0.0},
                "NOWCAST_LOCK": {"opened": 0, "trades": 0, "wins": 0, "pnl_usd": 0.0},
            },
            "equity_curve": [{"ts": int(time.time()), "equity_u": init}],
            "last_scan_ts": 0,
            "last_scan_iso": "",
            "last_opportunities": [],
            "last_cities_summary": [],
        }

    def _load(self) -> dict[str, Any]:
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text(encoding="utf-8"))
                data["paper_only"] = True
                return data
            except (OSError, json.JSONDecodeError) as exc:
                self.last_io_error = f"load_error: {exc}"
        fresh = self._fresh()
        self._save(fresh)
        return fresh

    def _save(self, payload: Optional[dict[str, Any]] = None) -> None:
        target = payload if payload is not None else self.state
        target["paper_only"] = True
        _atomic_write_json(self.path, target)

    def deployed_u(self) -> float:
        """Total capital currently deployed across open paper positions."""
        positions = self.state.get("open_positions") or {}
        return round(sum(float(p.get("cost_usd") or 0.0) for p in positions.values()), 4)

    def equity_u(self) -> float:
        """Total paper equity (cash + deployed open cost basis)."""
        return round(float(self.state.get("cash_u") or 0.0) + self.deployed_u(), 4)

    def _roll_daily_if_needed(self) -> None:
        today = _utc_today_str()
        daily = self.state.get("daily") or {}
        if daily.get("date") != today:
            eq = self.equity_u()
            self.state["daily"] = {
                "date": today,
                "start_equity_u": eq,
                "pnl_usd": 0.0,
                "trades": 0,
                "halted": False,
                "halt_reason": "",
            }

    def reset(self, initial_equity_u: float = 40.0) -> dict[str, Any]:
        """Reset Route 1 paper account cleanly to `initial_equity_u`."""
        self.initial_equity_u = float(initial_equity_u)
        self.state = self._fresh(initial_u=self.initial_equity_u)
        self._save()
        return self.state

    def can_open(self, event_slug: str, cost_usd: float) -> tuple[bool, str]:
        """Check duplicate-event, concurrent slots, cash, deployed cap, and daily-loss breaker."""
        self._roll_daily_if_needed()
        daily = self.state.get("daily") or {}
        if daily.get("halted"):
            return False, str(daily.get("halt_reason") or "DAILY_LOSS_HALT")
        if event_slug in (self.state.get("open_positions") or {}):
            return False, "DUPLICATE_EVENT"
        if len(self.state.get("open_positions") or {}) >= MAX_OPEN_EVENTS:
            return False, "MAX_OPEN_EVENTS"
        if cost_usd <= 0.0 or cost_usd > float(self.state.get("cash_u") or 0.0) + 1e-9:
            return False, "INSUFFICIENT_CASH"
        if (self.deployed_u() + cost_usd) > (self.equity_u() * MAX_DEPLOYED_FRAC + 1e-6):
            return False, "MAX_DEPLOYED_CAP"
        return True, "OK"

    def open_paper_position(self, opp: WeatherOpportunity) -> Optional[dict[str, Any]]:
        """Deduct paper cash and record an open simulated weather position."""
        ok, _ = self.can_open(opp.event_slug, opp.cost_usd)
        if not ok:
            return None
        now_ts = int(time.time())
        pos = {
            **asdict(opp),
            "opened_at_ts": now_ts,
            "opened_at_iso": datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "status": "OPEN",
        }
        self.state["cash_u"] = round(float(self.state["cash_u"]) - opp.cost_usd, 4)
        self.state["open_positions"][opp.event_slug] = pos
        attr = self.state.setdefault("attribution", {}).setdefault(opp.attribution, {"opened": 0, "trades": 0, "wins": 0, "pnl_usd": 0.0})
        attr["opened"] = int(attr.get("opened") or 0) + 1
        self._save()
        return pos

    def settle_position(self, event_slug: str, won: bool, official_note: str = "") -> Optional[dict[str, Any]]:
        """Settle an open position ($1.00/sh win, $0.00/sh loss), updating PnL and Brier calibration."""
        self._roll_daily_if_needed()
        positions = self.state.get("open_positions") or {}
        pos = positions.pop(event_slug, None)
        if pos is None:
            return None
        shares = float(pos.get("shares") or 0.0)
        cost = float(pos.get("cost_usd") or 0.0)
        payout = round(shares if won else 0.0, 4)
        pnl = round(payout - cost, 4)
        now_ts = int(time.time())

        self.state["cash_u"] = round(float(self.state["cash_u"]) + payout, 4)
        eq = self.equity_u()
        self.state["peak_equity_u"] = round(max(float(self.state.get("peak_equity_u") or eq), eq), 4)
        self.state["wins"] = int(self.state.get("wins") or 0) + (1 if won else 0)
        self.state["losses"] = int(self.state.get("losses") or 0) + (0 if won else 1)

        daily = self.state["daily"]
        daily["pnl_usd"] = round(float(daily.get("pnl_usd") or 0.0) + pnl, 4)
        daily["trades"] = int(daily.get("trades") or 0) + 1
        start_eq = float(daily.get("start_equity_u") or self.initial_equity_u)
        loss_limit = max(8.0, 0.20 * start_eq) if start_eq >= 40.0 else 0.20 * start_eq
        if daily["pnl_usd"] <= -loss_limit + 1e-9:
            daily["halted"] = True
            daily["halt_reason"] = f"Daily loss {daily['pnl_usd']:.2f} <= -{loss_limit:.2f}"

        outcome = 1.0 if won else 0.0
        cal = self.state.setdefault("calibration", {"n": 0, "model_brier_sum": 0.0, "market_brier_sum": 0.0})
        cal["n"] = int(cal.get("n") or 0) + 1
        cal["model_brier_sum"] = round(float(cal.get("model_brier_sum") or 0.0) + (float(pos["model_prob"]) - outcome) ** 2, 6)
        cal["market_brier_sum"] = round(float(cal.get("market_brier_sum") or 0.0) + (float(pos["exec_ask"]) - outcome) ** 2, 6)

        attr_key = str(pos.get("attribution") or "ENSEMBLE_SOLO")
        attr = self.state.setdefault("attribution", {}).setdefault(attr_key, {"opened": 0, "trades": 0, "wins": 0, "pnl_usd": 0.0})
        attr["trades"] = int(attr.get("trades") or 0) + 1
        attr["wins"] = int(attr.get("wins") or 0) + (1 if won else 0)
        attr["pnl_usd"] = round(float(attr.get("pnl_usd") or 0.0) + pnl, 4)

        closed = {
            **pos,
            "status": "WON" if won else "LOST",
            "won": won,
            "payout_usd": payout,
            "pnl_usd": pnl,
            "official_note": official_note,
            "settled_at_ts": now_ts,
            "settled_at_iso": datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        }
        self.state["closed_trades"] = [closed, *(self.state.get("closed_trades") or [])][:MAX_TRADES_KEPT]
        self.state["equity_curve"] = [*(self.state.get("equity_curve") or [])[-499:], {"ts": now_ts, "equity_u": eq}]
        self._save()
        return closed

    def update_scan_snapshot(self, opportunities: list[dict[str, Any]], cities_summary: list[dict[str, Any]]) -> None:
        """Persist latest weather opportunities and city diagnostics."""
        now_ts = int(time.time())
        self.state["last_scan_ts"] = now_ts
        self.state["last_scan_iso"] = datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        self.state["last_opportunities"] = opportunities[:40]
        self.state["last_cities_summary"] = cities_summary[:60]
        self._save()

    def summary(self) -> dict[str, Any]:
        """Return compact telemetry summary for API & UI headers."""
        eq = self.equity_u()
        init = float(self.state.get("initial_equity_u") or 40.0)
        wins = int(self.state.get("wins") or 0)
        losses = int(self.state.get("losses") or 0)
        n_closed = wins + losses
        cal = self.state.get("calibration") or {}
        cal_n = int(cal.get("n") or 0)
        return {
            "paper_only": True,
            "initial_equity_u": init,
            "equity_u": eq,
            "cash_u": float(self.state.get("cash_u") or 0.0),
            "deployed_u": self.deployed_u(),
            "net_pnl_usd": round(eq - init, 4),
            "net_pnl_pct": round(((eq - init) / init * 100.0) if init > 0 else 0.0, 2),
            "open_count": len(self.state.get("open_positions") or {}),
            "max_open_events": MAX_OPEN_EVENTS,
            "wins": wins,
            "losses": losses,
            "win_rate_pct": round((wins / n_closed * 100.0) if n_closed > 0 else 0.0, 1),
            "model_brier": round(float(cal.get("model_brier_sum") or 0.0) / cal_n, 4) if cal_n > 0 else None,
            "market_brier": round(float(cal.get("market_brier_sum") or 0.0) / cal_n, 4) if cal_n > 0 else None,
            "brier_n": cal_n,
            "daily": self.state.get("daily") or {},
            "attribution": self.state.get("attribution") or {},
            "last_scan_iso": self.state.get("last_scan_iso") or "",
        }


def _parse_outcome_yes_price(market_obj: dict[str, Any]) -> Optional[float]:
    raw = market_obj.get("outcomePrices")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return None
    if isinstance(raw, list) and raw:
        try:
            return float(raw[0])
        except (TypeError, ValueError):
            return None
    return None


def _extract_ensemble_series(daily_block: dict[str, Any], hourly_block: dict[str, Any], target_date_str: str) -> tuple[list[float], list[list[float]], list[int], list[str]]:
    dates = daily_block.get("time") or []
    daily_vals: list[float] = []
    if target_date_str in dates:
        d_idx = dates.index(target_date_str)
        for k, vals in daily_block.items():
            if k.startswith("temperature_2m_max_member") and isinstance(vals, list) and d_idx < len(vals) and vals[d_idx] is not None:
                daily_vals.append(float(vals[d_idx]))

    h_times: list[str] = hourly_block.get("time") or []
    day_h_indices = [i for i, ts in enumerate(h_times) if str(ts).startswith(target_date_str)]
    hourly_members: list[list[float]] = []
    if day_h_indices:
        for k, vals in hourly_block.items():
            if k.startswith("temperature_2m_member") and isinstance(vals, list) and len(vals) >= len(h_times):
                row = [float(vals[i]) for i in day_h_indices if vals[i] is not None]
                if len(row) == len(day_h_indices):
                    hourly_members.append(row)
    return daily_vals, hourly_members, day_h_indices, [str(h_times[i]) for i in day_h_indices]


class Route1MonitorRunner:
    """Simulation-only orchestrator running Weather Dual-Engine and 8-Domain Stealth Experts."""

    def __init__(
        self,
        ledger: Optional[Route1WeatherLedger] = None,
        expert_scanner: Optional[StealthExpertScanner] = None,
        station_cache_path: Path = ROUTE1_STATION_CACHE_PATH,
        ensemble_cache_path: Path = ROUTE1_ENSEMBLE_CACHE_PATH,
    ) -> None:
        self.paper_only = PAPER_ONLY
        self.ledger = ledger or Route1WeatherLedger()
        self.expert_scanner = expert_scanner or StealthExpertScanner()
        self.station_cache_path = station_cache_path
        self.ensemble_cache_path = ensemble_cache_path
        self.station_cache: dict[str, dict[str, Any]] = self._load_station_cache()
        self._ensemble_cache: dict[str, tuple[float, dict[str, Any]]] = self._load_ensemble_cache()
        self._nws_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
        self.last_errors: list[str] = []
        self._running = False
        self._task: Optional[asyncio.Task[None]] = None
        self._last_expert_scan_ts: float = float(self.expert_scanner.state.get("updated_at_ts") or 0)

    def _load_station_cache(self) -> dict[str, dict[str, Any]]:
        if self.station_cache_path.exists():
            try:
                return json.loads(self.station_cache_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                self.last_errors.append(f"station_cache_load: {exc}")
        return {"HKO": HKO_STATION}

    def _save_station_cache(self) -> None:
        _atomic_write_json(self.station_cache_path, self.station_cache)

    def _load_ensemble_cache(self) -> dict[str, tuple[float, dict[str, Any]]]:
        if not self.ensemble_cache_path.exists():
            return {}
        try:
            raw = json.loads(self.ensemble_cache_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            self.last_errors.append(f"ensemble_cache_load: {exc}")
            return {}
        now = time.time()
        out: dict[str, tuple[float, dict[str, Any]]] = {}
        for k, entry in (raw or {}).items():
            ts = float(entry.get("ts") or 0.0)
            data = entry.get("data")
            if (now - ts) < ENSEMBLE_TTL_SEC and isinstance(data, dict):
                out[k] = (ts, data)
        return out

    def _save_ensemble_cache(self) -> None:
        now = time.time()
        payload = {
            k: {"ts": ts, "data": data}
            for k, (ts, data) in self._ensemble_cache.items()
            if (now - ts) < ENSEMBLE_TTL_SEC
        }
        _atomic_write_json(self.ensemble_cache_path, payload)

    async def _http_get_json_with_headers(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> tuple[Optional[Any], dict[str, str]]:
        hdrs = headers or {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        try:
            async with session.get(url, params=params, headers=hdrs) as resp:
                resp_hdrs = {k: str(v) for k, v in resp.headers.items()}
                if resp.status != 200:
                    self.last_errors.append(f"HTTP {resp.status} {url}")
                    return None, resp_hdrs
                return await resp.json(), resp_hdrs
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
            self.last_errors.append(f"HTTP_ERR {type(exc).__name__}: {exc}")
            return None, {}

    async def _http_get_json(self, session: aiohttp.ClientSession, url: str, params: Optional[dict[str, Any]] = None) -> Optional[Any]:
        data, _ = await self._http_get_json_with_headers(session, url, params=params)
        return data

    async def _http_post_json(self, session: aiohttp.ClientSession, url: str, body: Any) -> Optional[Any]:
        hdrs = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/json", "Accept": "application/json"}
        try:
            async with session.post(url, json=body, headers=hdrs) as resp:
                if resp.status != 200:
                    self.last_errors.append(f"HTTP {resp.status} {url}")
                    return None
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
            self.last_errors.append(f"HTTP_ERR {type(exc).__name__}: {exc}")
            return None

    async def _http_get_text(self, session: aiohttp.ClientSession, url: str, params: Optional[dict[str, Any]] = None) -> str:
        try:
            async with session.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"}) as resp:
                if resp.status != 200:
                    self.last_errors.append(f"HTTP {resp.status} {url}")
                    return ""
                return await resp.text()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            self.last_errors.append(f"HTTP_ERR {type(exc).__name__}: {exc}")
            return ""

    async def fetch_active_weather_events(self, session: aiohttp.ClientSession, max_cities: Optional[int] = None) -> list[WeatherEventSpec]:
        """Discover active Polymarket daily temperature events ending within [-1d, +2d]."""
        raw_events: list[dict[str, Any]] = []
        for off in (0, 50, 100):
            chunk = await self._http_get_json(
                session,
                f"{GAMMA_API_BASE}/events",
                params={"closed": "false", "active": "true", "tag_slug": "weather", "order": "volume", "ascending": "false", "limit": "50", "offset": str(off)},
            )
            if not chunk:
                break
            raw_events.extend(chunk)

        today_utc = datetime.now(timezone.utc).date()
        specs: list[WeatherEventSpec] = []
        for ev in raw_events:
            spec = self._parse_gamma_weather_event(ev, today_utc)
            if spec is not None:
                specs.append(spec)
                if max_cities and len(specs) >= max_cities:
                    break
        return specs

    def _parse_gamma_weather_event(self, ev: dict[str, Any], today_utc: date) -> Optional[WeatherEventSpec]:
        title = str(ev.get("title") or "")
        if "highest temperature" not in title.lower():
            return None
        liq = float(ev.get("liquidity") or 0.0)
        if liq < 1500.0:
            return None
        ms = ev.get("markets") or []
        if not ms:
            return None
        slug = str(ev.get("slug") or "")
        icao, is_hko, obs_date = extract_event_station_and_date(slug, title, str(ms[0].get("description") or ""))
        if obs_date is None or (obs_date - today_utc).days not in (-1, 0, 1, 2):
            return None
        if not is_hko and not icao:
            return None
        brackets = self._parse_event_brackets(ms)
        if not brackets:
            return None
        city = title.replace("Highest temperature in ", "").split(" on ")[0].strip()
        return WeatherEventSpec(
            event_slug=slug,
            city=city,
            obs_date=obs_date,
            timezone_name="UTC",
            unit=brackets[0].unit,
            liquidity_usd=liq,
            brackets=tuple(brackets),
            icao=icao,
            is_hko=is_hko,
            title=title,
        )

    def _parse_event_brackets(self, markets: list[dict[str, Any]]) -> list[TempBracket]:
        out: list[TempBracket] = []
        for m in markets:
            if m.get("closed"):
                continue
            label = str(m.get("groupItemTitle") or m.get("question") or "").strip()
            try:
                lo, hi, unit = parse_bracket_range(label)
            except ValueError:
                continue
            raw_tids = m.get("clobTokenIds") or "[]"
            tids = json.loads(raw_tids) if isinstance(raw_tids, str) else raw_tids
            if len(tids) < 2:
                continue
            fee_rate = float((m.get("feeSchedule") or {}).get("rate") or 0.05)
            out.append(
                TempBracket(
                    condition_id=str(m.get("conditionId") or ""),
                    label=label,
                    unit=unit,
                    low=lo,
                    high=hi,
                    yes_token_id=str(tids[0]),
                    no_token_id=str(tids[1]),
                    best_bid=float(m["bestBid"]) if m.get("bestBid") is not None else None,
                    best_ask=float(m["bestAsk"]) if m.get("bestAsk") is not None else None,
                    fee_rate=fee_rate,
                    order_min_size=float(m.get("orderMinSize") or 5.0),
                    volume=float(m.get("volume") or 0.0),
                )
            )
        return out

    async def _resolve_stations(self, session: aiohttp.ClientSession, specs: list[WeatherEventSpec]) -> dict[str, dict[str, Any]]:
        missing = sorted({s.icao for s in specs if s.icao and s.icao not in self.station_cache})
        if missing:
            for i in range(0, len(missing), 25):
                batch = ",".join(missing[i : i + 25])
                rows = await self._http_get_json(session, AVIATION_STATION_URL, params={"ids": batch, "format": "json"})
                for r in rows or []:
                    code = str(r.get("icaoId") or r.get("id") or "").upper()
                    if code and r.get("lat") is not None and r.get("lon") is not None:
                        self.station_cache[code] = {"lat": float(r["lat"]), "lon": float(r["lon"]), "tz": "UTC", "name": str(r.get("site") or code)}
            self._save_station_cache()
        return self.station_cache

    async def _fetch_metar_lines(self, session: aiohttp.ClientSession, icaos: list[str]) -> dict[str, list[str]]:
        grouped: dict[str, list[str]] = {c: [] for c in icaos}
        metar_sem = asyncio.Semaphore(5)

        async def _chunk(batch_codes: list[str]) -> None:
            async with metar_sem:
                txt = await self._http_get_text(
                    session, AVIATION_METAR_URL, params={"ids": ",".join(batch_codes), "format": "raw", "hours": "36"}
                )
            for ln in txt.splitlines():
                parts = ln.strip().split()
                if not parts:
                    continue
                code = parts[1] if parts[0] in ("METAR", "SPECI") and len(parts) > 1 else parts[0]
                if code in grouped:
                    grouped[code].append(ln.strip())

        await asyncio.gather(*(_chunk(icaos[i : i + 4]) for i in range(0, len(icaos), 4)))
        return grouped

    async def _fetch_hko_temp_c(self, session: aiohttp.ClientSession) -> Optional[float]:
        data = await self._http_get_json(session, HKO_RHRREAD_URL)
        for item in ((data or {}).get("temperature") or {}).get("data") or []:
            if "Hong Kong Observatory" in str(item.get("place") or ""):
                try:
                    return float(item["value"])
                except (KeyError, TypeError, ValueError):
                    return None
        return None

    async def _prefetch_ensembles_batched(self, session: aiohttp.ClientSession, specs: list[WeatherEventSpec]) -> None:
        now = time.time()
        unique: dict[str, tuple[str, float, float]] = {}
        for sp in specs:
            st_key = "HKO" if sp.is_hko else (sp.icao or "")
            st = self.station_cache.get(st_key)
            if not st:
                continue
            lat, lon = float(st["lat"]), float(st["lon"])
            ckey = f"{lat:.3f},{lon:.3f}"
            cached = self._ensemble_cache.get(ckey)
            if ckey not in unique and (not cached or (now - cached[0]) >= ENSEMBLE_TTL_SEC):
                unique[ckey] = (st_key, lat, lon)

        targets = list(unique.values())
        if not targets:
            return
        tz_dirty, ens_dirty = False, False
        for i in range(0, len(targets), 8):
            chunk = targets[i : i + 8]
            data, hdrs = await self._http_get_json_with_headers(
                session,
                OPEN_METEO_ENSEMBLE_URL,
                params={
                    "latitude": ",".join(f"{lat:.4f}" for _, lat, _ in chunk),
                    "longitude": ",".join(f"{lon:.4f}" for _, _, lon in chunk),
                    "daily": "temperature_2m_max",
                    "models": "ecmwf_ifs025,gfs025",
                    "timezone": "auto",
                    "past_days": "1",
                    "forecast_days": "3",
                },
            )
            if data is None:
                break
            items = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
            for (st_key, lat, lon), item in zip(chunk, items):
                if isinstance(item, dict) and "daily" in item:
                    self._ensemble_cache[f"{lat:.3f},{lon:.3f}"] = (time.time(), item)
                    ens_dirty = True
                    tz = str(item.get("timezone") or "UTC")
                    if st_key in self.station_cache and self.station_cache[st_key].get("tz") != tz:
                        self.station_cache[st_key]["tz"] = tz
                        tz_dirty = True
            rem_hdr = hdrs.get("X-RateLimit-Remaining") or hdrs.get("x-ratelimit-remaining")
            if rem_hdr is not None and float(rem_hdr) < 75.0:
                self.last_errors.append(f"OPEN_METEO_PACED remaining={rem_hdr}")
                break
            if i + 8 < len(targets):
                await asyncio.sleep(0.35)
        if tz_dirty:
            self._save_station_cache()
        if ens_dirty:
            self._save_ensemble_cache()

    def _get_cached_ensemble(self, lat: float, lon: float) -> Optional[dict[str, Any]]:
        cached = self._ensemble_cache.get(f"{lat:.3f},{lon:.3f}")
        if cached and (time.time() - cached[0]) < ENSEMBLE_TTL_SEC:
            return cached[1]
        return None

    async def _fetch_nws_anchor_c(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        spec: WeatherEventSpec,
    ) -> Optional[float]:
        if not spec.icao or not spec.icao.startswith("K"):
            return None
        st = self.station_cache.get(spec.icao)
        if not st:
            return None
        hdrs = {"User-Agent": "Route1WeatherMonitor/1.0", "Accept": "application/geo+json"}
        fc_url = st.get("nws_forecast_url")
        if not fc_url:
            async with sem:
                pt, _ = await self._http_get_json_with_headers(
                    session, f"{NWS_POINTS_URL}/{float(st['lat']):.4f},{float(st['lon']):.4f}", headers=hdrs
                )
            fc_url = ((pt or {}).get("properties") or {}).get("forecast")
            if not fc_url:
                return None
            st["nws_forecast_url"] = fc_url
            self._save_station_cache()

        cached = self._nws_cache.get(spec.icao)
        if cached and (time.time() - cached[0]) < ENSEMBLE_TTL_SEC:
            periods = cached[1]
        else:
            async with sem:
                fc, _ = await self._http_get_json_with_headers(session, str(fc_url), headers=hdrs)
            periods = ((fc or {}).get("properties") or {}).get("periods") or []
            if periods:
                self._nws_cache[spec.icao] = (time.time(), periods)

        target_iso = spec.obs_date.isoformat()
        for p in periods:
            if p.get("isDaytime") and str(p.get("startTime") or "")[:10] == target_iso:
                temp = p.get("temperature")
                if temp is None:
                    return None
                unit = str(p.get("temperatureUnit") or "F").upper()
                return (float(temp) - 32.0) * 5.0 / 9.0 if unit == "F" else float(temp)
        return None

    def _compute_station_bias_c(
        self,
        spec: WeatherEventSpec,
        ens_daily: dict[str, Any],
        tz_name: str,
        now_local: datetime,
        metar_by_icao: dict[str, list[str]],
        hko_temp_c: Optional[float],
    ) -> float:
        if spec.is_hko:
            if hko_temp_c is not None and now_local.hour >= 14:
                today_vals, _, _, _ = _extract_ensemble_series(ens_daily, {}, now_local.date().isoformat())
                if today_vals:
                    med = statistics.median(today_vals)
                    return max(0.0, hko_temp_c - med)
            return 0.0
        if not spec.icao:
            return 0.0
        ref_date = now_local.date() if now_local.hour >= 17 else (now_local.date() - timedelta(days=1))
        ref_vals, _, _, _ = _extract_ensemble_series(ens_daily, {}, ref_date.isoformat())
        if not ref_vals:
            return 0.0
        obs_ref_max, ref_cnt = compute_local_day_metar_max_c(metar_by_icao.get(spec.icao, []), tz_name, ref_date)
        if obs_ref_max is not None and ref_cnt >= 16:
            return round(obs_ref_max - statistics.median(ref_vals), 3)
        return 0.0

    async def _fetch_clob_asks_for_candidates(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        spec: WeatherEventSpec,
        probs: dict[str, float],
    ) -> dict[str, float]:
        token_ids: list[str] = []
        for br in spec.brackets:
            p_yes = probs.get(br.label, 0.0)
            y_gap = (p_yes - br.best_ask) if br.best_ask is not None else 0.0
            n_gap = ((1.0 - p_yes) - (1.0 - br.best_bid)) if br.best_bid is not None else 0.0
            if y_gap >= 0.03 or n_gap >= 0.03:
                token_ids.extend([br.yes_token_id, br.no_token_id])
        if not token_ids:
            return {}

        out: dict[str, float] = {}
        async with sem:
            books = await self._http_post_json(session, CLOB_BOOKS_URL, [{"token_id": tid} for tid in token_ids])
        for bk in books or []:
            tid = str(bk.get("asset_id") or "")
            asks = sorted(
                ((float(x["price"]), float(x["size"])) for x in (bk.get("asks") or []) if float(x.get("size") or 0) > 0),
                key=lambda t: t[0],
            )
            px = sweep_orderbook_vwap(asks, target_shares=6.0, max_slip=0.03)
            if tid and px is not None:
                out[tid] = px
        return out

    async def _check_expert_alignment(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        spec: WeatherEventSpec,
        probs: dict[str, float],
        expert_wallets: set[str],
    ) -> dict[str, tuple[int, int]]:
        if not expert_wallets:
            return {}
        cand = [
            br for br in spec.brackets
            if (probs.get(br.label, 0.0) - (br.best_ask or 1.0)) >= 0.03
            or ((1.0 - probs.get(br.label, 0.0)) - (1.0 - (br.best_bid or 0.0))) >= 0.03
        ]
        top3 = sorted(spec.brackets, key=lambda b: probs.get(b.label, 0.0), reverse=True)[:3]
        unique_brs = list({b.condition_id: b for b in [*cand, *top3]}.values())[:5]
        out: dict[str, tuple[int, int]] = {}

        async def _inspect(br: TempBracket) -> None:
            async with sem:
                res = await self._http_get_json(session, f"{DATA_API_BASE}/holders", params={"condition": br.condition_id, "limit": "20", "include_pnl": "false"})
            yes_n, no_n = 0, 0
            for grp in (res or {}).get("data") or []:
                for h in grp.get("holders") or []:
                    w = str(h.get("proxy_wallet") or "").lower()
                    if w in expert_wallets:
                        if int(h.get("outcome_index") or 0) == 0:
                            yes_n += 1
                        else:
                            no_n += 1
            out[br.condition_id] = (yes_n, no_n)

        await asyncio.gather(*(_inspect(br) for br in unique_brs))
        return out

    async def _settle_open_positions(self, session: aiohttp.ClientSession) -> list[dict[str, Any]]:
        settled: list[dict[str, Any]] = []
        slugs = list((self.ledger.state.get("open_positions") or {}).keys())
        for slug in slugs:
            pos = self.ledger.state["open_positions"].get(slug)
            if not pos:
                continue
            evs = await self._http_get_json(session, f"{GAMMA_API_BASE}/events", params={"slug": slug})
            if not evs:
                continue
            ev = evs[0]
            target_cid = str(pos.get("condition_id") or "")
            for m in ev.get("markets") or []:
                if str(m.get("conditionId") or "") != target_cid or not m.get("closed"):
                    continue
                yes_px = _parse_outcome_yes_price(m)
                if yes_px is None or (0.05 < yes_px < 0.95):
                    continue
                yes_won = yes_px >= 0.95
                pos_won = yes_won if pos.get("side") == "YES" else (not yes_won)
                rec = self.ledger.settle_position(slug, won=pos_won, official_note=f"Gamma settled YES={yes_px}")
                if rec:
                    settled.append(rec)
                break
        return settled

    async def _evaluate_single_event(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        spec: WeatherEventSpec,
        metar_by_icao: dict[str, list[str]],
        hko_temp_c: Optional[float],
        expert_wallets: set[str],
    ) -> tuple[Optional[WeatherOpportunity], dict[str, Any]]:
        st_key = "HKO" if spec.is_hko else (spec.icao or "")
        st = self.station_cache.get(st_key)
        if not st:
            return None, {"city": spec.city, "date": spec.obs_date.isoformat(), "status": "NO_STATION"}
        ens = self._get_cached_ensemble(float(st["lat"]), float(st["lon"]))
        if not ens:
            return None, {"city": spec.city, "date": spec.obs_date.isoformat(), "status": "NO_ENSEMBLE"}

        tz_name = str(ens.get("timezone") or st.get("tz") or "UTC")
        tz = ZoneInfo(tz_name)
        now_local = datetime.now(timezone.utc).astimezone(tz)
        date_str = spec.obs_date.isoformat()
        ens_daily = ens.get("daily") or {}
        daily_vals, hourly_members, _, h_times = _extract_ensemble_series(ens_daily, ens.get("hourly") or {}, date_str)
        if not daily_vals:
            return None, {"city": spec.city, "date": date_str, "status": "NO_DATE_FORECAST"}

        station_bias_c = self._compute_station_bias_c(spec, ens_daily, tz_name, now_local, metar_by_icao, hko_temp_c)
        nws_anchor_c = await self._fetch_nws_anchor_c(session, sem, spec)
        calibrated_daily = calibrate_member_maxima_c(daily_vals, station_bias_c=station_bias_c, nws_anchor_c=nws_anchor_c)

        obs_max_c, obs_curr_c = self._get_live_obs(spec, tz_name, now_local.date(), metar_by_icao, hko_temp_c)
        probs, locked = compute_bracket_probabilities(
            spec.brackets,
            calibrated_daily,
            obs_max_c=obs_max_c,
            obs_current_c=obs_curr_c,
            local_hour=now_local.hour if now_local.date() == spec.obs_date else None,
            return_lock_flag=True,
        )
        clob_asks = await self._fetch_clob_asks_for_candidates(session, sem, spec, probs)
        counts_map = await self._check_expert_alignment(session, sem, spec, probs, expert_wallets) if clob_asks else {}
        opps = evaluate_weather_opportunities(
            spec,
            probs,
            equity_u=self.ledger.equity_u(),
            nowcast_locked=locked,
            clob_asks=clob_asks,
            expert_counts_by_cid=counts_map,
            obs_max_display=obs_max_c,
        )
        top_br = max(probs.items(), key=lambda kv: kv[1])
        city_info = {
            "city": spec.city,
            "date": date_str,
            "station": st_key,
            "tz": tz_name,
            "unit": spec.unit,
            "liquidity_usd": round(spec.liquidity_usd),
            "obs_max_c": obs_max_c,
            "station_bias_c": round(station_bias_c, 2),
            "nws_anchor_c": round(nws_anchor_c, 2) if nws_anchor_c is not None else None,
            "top_bracket": top_br[0],
            "top_prob": round(top_br[1], 3),
            "nowcast_locked": locked,
            "has_edge": bool(opps),
        }
        return (opps[0] if opps else None), city_info

    def _get_live_obs(
        self,
        spec: WeatherEventSpec,
        tz_name: str,
        local_today: date,
        metar_by_icao: dict[str, list[str]],
        hko_temp_c: Optional[float],
    ) -> tuple[Optional[float], Optional[float]]:
        if spec.obs_date != local_today:
            return None, None
        if spec.is_hko:
            return hko_temp_c, hko_temp_c
        lines = metar_by_icao.get(spec.icao or "", [])
        max_c, _ = compute_local_day_metar_max_c(lines, tz_name, spec.obs_date)
        latest_c: Optional[float] = None
        best_ts: Optional[datetime] = None
        tz = ZoneInfo(tz_name)
        for ln in lines:
            tc = parse_metar_temp_c(ln)
            ts = parse_metar_timestamp_utc(ln)
            if tc is None or ts is None or ts.astimezone(tz).date() != spec.obs_date:
                continue
            if best_ts is None or ts > best_ts:
                best_ts = ts
                latest_c = tc
        return max_c, latest_c

    async def scan_weather_once(self, max_cities: Optional[int] = None) -> dict[str, Any]:
        """Run one complete weather scan, evaluate EV, open/settle paper positions, and save."""
        self.last_errors = []
        sem = asyncio.Semaphore(4)
        expert_wallets = self.expert_scanner.get_weather_expert_wallets()
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            settled = await self._settle_open_positions(session)
            specs = await self.fetch_active_weather_events(session, max_cities=max_cities)
            await self._resolve_stations(session, specs)
            await self._prefetch_ensembles_batched(session, specs)
            icaos = sorted({s.icao for s in specs if s.icao})
            metar_by_icao = await self._fetch_metar_lines(session, icaos) if icaos else {}
            hko_temp = await self._fetch_hko_temp_c(session) if any(s.is_hko for s in specs) else None
            results = await asyncio.gather(
                *(self._evaluate_single_event(session, sem, sp, metar_by_icao, hko_temp, expert_wallets) for sp in specs)
            )

        opps = sorted([opp for opp, _ in results if opp is not None], key=lambda o: o.net_ev, reverse=True)
        cities_summary = [info for _, info in results]
        opened: list[dict[str, Any]] = []
        opp_dicts: list[dict[str, Any]] = []
        for opp in opps:
            pos = self.ledger.open_paper_position(opp)
            d = asdict(opp)
            d["paper_action"] = "OPENED" if pos is not None else "SKIPPED_RISK_CAP"
            opp_dicts.append(d)
            if pos is not None:
                opened.append(pos)

        self.ledger.update_scan_snapshot(opp_dicts, cities_summary)
        return {
            "status": "ok",
            "paper_only": True,
            "cities_scanned": len(specs),
            "opportunities_found": len(opps),
            "opened": len(opened),
            "settled": len(settled),
            "summary": self.ledger.summary(),
        }

    async def scan_experts_once(self, max_per_category: int = 8) -> dict[str, Any]:
        """Run 8-domain stealth expert & crowding scan."""
        res = await self.expert_scanner.scan_all_domains(max_per_category=max_per_category)
        self._last_expert_scan_ts = float(res.get("updated_at_ts") or time.time())
        return res

    async def _loop(self) -> None:
        while self._running:
            try:
                if (time.time() - self._last_expert_scan_ts) >= 21600.0:
                    await self.scan_experts_once()
                await self.scan_weather_once()
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001 - top-level daemon loop keeps alive
                self.last_errors.append(f"loop_err: {exc}")
            await asyncio.sleep(600.0)

    def start(self) -> None:
        """Start background 10-min weather + 6-hour expert monitor loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """Stop background monitoring cleanly."""
        self._running = False
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    def compact_status(self) -> dict[str, Any]:
        """Return compact Route 1 snapshot for /api/status."""
        return {
            "paper_only": True,
            "running": self._running,
            "summary": self.ledger.summary(),
            "stealth_experts_count": len(self.expert_scanner.state.get("stealth_experts") or []),
            "crowded_whales_count": len(self.expert_scanner.state.get("crowded_whales") or []),
            "errors_count": len(self.last_errors),
        }

    def telegram_weather_text(self) -> str:
        """Format plain-Unicode Telegram status summary for /weather."""
        s = self.ledger.summary()
        open_pos = list((self.ledger.state.get("open_positions") or {}).values())
        lines = [
            f"🌤 Route 1 氣象模擬監控 (PAPER ${s['initial_equity_u']:.0f}U)",
            f"• 權益: ${s['equity_u']:.2f}U | 現金: ${s['cash_u']:.2f}U | 已部署: ${s['deployed_u']:.2f}U",
            f"• 戰績: {s['wins']}W-{s['losses']}L ({s['win_rate_pct']:.1f}%) | 淨損益: {s['net_pnl_usd']:+.2f}U ({s['net_pnl_pct']:+.1f}%)",
            f"• 校準(Brier): 模型={s['model_brier']} vs 市場={s['market_brier']} (n={s['brier_n']})",
            f"• 掃描城市: {len(self.ledger.state.get('last_cities_summary') or [])} | 進行中持倉: {len(open_pos)}",
        ]
        for p in open_pos[:4]:
            lines.append(f"  - {p['city']} {p['obs_date']} [{p['bracket_label']} {p['side']}] @{p['exec_ask']:.2f} EV+{p['net_ev']*100:.1f}¢ (${p['cost_usd']:.2f})")
        return "\n".join(lines)

    def telegram_experts_text(self) -> str:
        """Format plain-Unicode Telegram summary for /experts."""
        st = self.expert_scanner.state
        stealth = st.get("stealth_experts") or []
        crowded = st.get("crowded_whales") or []
        lines = [
            f"🕵 Route 1 八領域隱形專家掃描 ({st.get('updated_at_iso') or '未掃描'})",
            f"• 隱形專家白名單: {len(stealth)} | 擁擠網紅名單(可反割): {len(crowded)}",
        ]
        for e in stealth[:5]:
            lines.append(f"  ✅ [{e['category']}] {e['name']} | Score {e['copy_score']} | 30s衝擊 {e['impact_30s_cents']}¢ | 延遲ROI {e['lagged_60s_roi_pct']:+.1f}%")
        for c in crowded[:3]:
            lines.append(f"  ⚠️ [{c['category']}] {c['name']} | 30s衝擊 {c['impact_30s_cents']}¢ (擁擠瀑布)")
        return "\n".join(lines)


async def _run_cli(args: argparse.Namespace) -> None:
    runner = Route1MonitorRunner()
    if args.with_experts:
        exp = await runner.scan_experts_once(max_per_category=args.expert_limit)
        print(f"[Experts] stealth={len(exp['stealth_experts'])} crowded={len(exp['crowded_whales'])} watchlist={len(exp['watchlist'])} rejected={exp['rejected_summary']}")
    w_res = await runner.scan_weather_once(max_cities=args.max_cities)
    print(json.dumps(w_res, indent=2, ensure_ascii=False))
    if runner.last_errors:
        print(f"[Warnings] {len(runner.last_errors)} errors, sample={runner.last_errors[:5]}")


def main() -> None:
    """CLI entrypoint for Route 1 manual smoke/scheduled scans."""
    parser = argparse.ArgumentParser(description="Route 1 Weather + Stealth Experts Paper Monitor")
    parser.add_argument("--once", action="store_true", help="Run single weather scan and exit")
    parser.add_argument("--with-experts", action="store_true", help="Also run 8-domain stealth expert scan")
    parser.add_argument("--max-cities", type=int, default=None, help="Limit number of cities scanned")
    parser.add_argument("--expert-limit", type=int, default=8, help="Max candidates evaluated per domain")
    args = parser.parse_args()
    asyncio.run(_run_cli(args))


if __name__ == "__main__":
    main()
