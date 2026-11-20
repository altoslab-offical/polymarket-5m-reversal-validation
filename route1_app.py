#!/usr/bin/env python3
"""Standalone isolated FastAPI server for Route 1 Weather + Stealth Experts (port 8089).

Zero imports from any 5m sniper / scanner / oracle / CLOB module and zero shared event loop
with `app.py` (`8088`) or `live_recorder_42.py`.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse, Response

from engine.route1_weather_paper import Route1MonitorRunner

STATIC_DIR = Path(__file__).resolve().parent / "static"
ROUTE1_PORT = int(os.environ.get("ROUTE1_PORT", "8089"))
route1_monitor = Route1MonitorRunner()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Manage background Route 1 weather/expert scan loop independently."""
    route1_monitor.start()
    try:
        yield
    finally:
        await route1_monitor.stop()


app = FastAPI(title="Route 1 Weather Paper Monitor (Isolated)", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse, response_model=None)
@app.get("/route1", response_class=HTMLResponse, response_model=None)
async def route1_dashboard() -> Response:
    """Serve standalone Route 1 dashboard HTML."""
    html_file = STATIC_DIR / "route1.html"
    if html_file.exists():
        return FileResponse(html_file)
    return HTMLResponse("<h1>Route 1 dashboard loading...</h1>")


@app.get("/health")
async def route1_health() -> dict[str, Any]:
    """Return process isolation & liveness telemetry on port 8089."""
    return {
        "status": "ok",
        "pid": os.getpid(),
        "port": ROUTE1_PORT,
        "paper_only": True,
        "isolated_from_5m_sniper": True,
        "compact": route1_monitor.compact_status(),
    }


@app.get("/weather", response_class=PlainTextResponse)
async def route1_weather_summary() -> PlainTextResponse:
    """Return plain-text Route 1 weather paper summary (no Telegram polling)."""
    return PlainTextResponse(route1_monitor.telegram_weather_text())


@app.get("/experts", response_class=PlainTextResponse)
async def route1_experts_summary() -> PlainTextResponse:
    """Return plain-text stealth experts summary (no Telegram polling)."""
    return PlainTextResponse(route1_monitor.telegram_experts_text())


@app.get("/api/route1/status")
async def api_route1_status() -> dict[str, Any]:
    """Return full Route 1 paper account, city summary, and stealth-expert counts."""
    exp_st = route1_monitor.expert_scanner.state
    return {
        "status": "ok",
        "paper_only": True,
        "summary": route1_monitor.ledger.summary(),
        "open_positions": list((route1_monitor.ledger.state.get("open_positions") or {}).values()),
        "cities_summary": route1_monitor.ledger.state.get("last_cities_summary") or [],
        "experts_meta": {
            "updated_at_iso": exp_st.get("updated_at_iso") or "",
            "stealth_count": len(exp_st.get("stealth_experts") or []),
            "crowded_count": len(exp_st.get("crowded_whales") or []),
            "categories": exp_st.get("categories") or {},
        },
        "errors_count": len(route1_monitor.last_errors),
    }


@app.get("/api/route1/weather/opportunities")
async def api_route1_weather_opportunities() -> dict[str, Any]:
    """Return latest scanned weather opportunities and city diagnostics."""
    return {
        "status": "ok",
        "last_scan_iso": route1_monitor.ledger.state.get("last_scan_iso") or "",
        "opportunities": route1_monitor.ledger.state.get("last_opportunities") or [],
        "cities_summary": route1_monitor.ledger.state.get("last_cities_summary") or [],
    }


@app.get("/api/route1/weather/trades")
async def api_route1_weather_trades(limit: int = 100) -> dict[str, Any]:
    """Return open/closed weather paper trades and equity curve."""
    capped = max(1, min(limit, 500))
    return {
        "status": "ok",
        "summary": route1_monitor.ledger.summary(),
        "open_positions": list((route1_monitor.ledger.state.get("open_positions") or {}).values()),
        "closed_trades": (route1_monitor.ledger.state.get("closed_trades") or [])[:capped],
        "equity_curve": (route1_monitor.ledger.state.get("equity_curve") or [])[-200:],
    }


@app.post("/api/route1/weather/scan")
async def api_route1_weather_scan(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Trigger immediate Route 1 weather opportunity scan."""
    max_cities = (payload or {}).get("max_cities")
    return await route1_monitor.scan_weather_once(max_cities=int(max_cities) if max_cities else None)


@app.get("/api/route1/experts/status")
async def api_route1_experts_status() -> dict[str, Any]:
    """Return persisted 8-domain stealth expert scanner state."""
    return {"status": "ok", **route1_monitor.expert_scanner.state}


@app.post("/api/route1/experts/scan")
async def api_route1_experts_scan(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Trigger immediate 8-domain stealth expert leaderboard scan."""
    limit = int((payload or {}).get("max_per_category", 8))
    res = await route1_monitor.scan_experts_once(max_per_category=max(2, min(limit, 20)))
    return {"status": "ok", **res}


@app.post("/api/route1/paper/reset")
async def api_route1_paper_reset(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Reset isolated Route 1 paper ledger to bounded initial equity."""
    init_u = float((payload or {}).get("initial_equity_u", 40.0))
    state = route1_monitor.ledger.reset(initial_equity_u=max(10.0, min(init_u, 10000.0)))
    return {"status": "ok", "summary": route1_monitor.ledger.summary(), "state": state}
