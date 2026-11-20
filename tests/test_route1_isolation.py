#!/usr/bin/env python3
"""Process-isolation & HTTP route tests for standalone `route1_app.py` (port 8089)."""
from __future__ import annotations

import ast
import asyncio
import json
import pathlib
import subprocess
import sys

REPO = pathlib.Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

FORBIDDEN_5M_MODULES: tuple[str, ...] = (
    "oracle_sniper",
    "signal_core",
    "oracle_sniper_runner",
    "oracle_sniper_feeds",
    "oracle_sniper_ledger",
    "oracle_sniper_notify",
    "scanner",
    "chainlink_oracle",
    "polymarket_auto_trader",
    "master_strategy",
    "v8_strategy",
    "v12_rsi_strategy",
)


def _extract_imports(py_path: pathlib.Path) -> list[str]:
    tree = ast.parse(py_path.read_text(encoding="utf-8"))
    mods: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            mods.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            mods.append(node.module)
    return mods


def test_route1_static_and_runtime_isolation() -> None:
    """Ensure `route1_app.py` and Route 1 modules import ZERO 5m sniper/scanner modules."""
    route1_files = (
        REPO / "route1_app.py",
        REPO / "engine/route1_weather_paper.py",
        REPO / "engine/route1_stealth_experts.py",
        REPO / "engine/route1_weather_model.py",
    )
    for fp in route1_files:
        for mod in _extract_imports(fp):
            assert not any(bad in mod for bad in FORBIDDEN_5M_MODULES), f"Forbidden 5m import {mod} in {fp.name}"

    out = subprocess.check_output(
        [sys.executable, "-c", "import json, sys, route1_app; print(json.dumps([m for m in sys.modules if m.startswith('engine.')]))"],
        cwd=REPO,
        text=True,
    )
    loaded_engine_mods = json.loads(out)
    for loaded in loaded_engine_mods:
        if any(bad in loaded for bad in FORBIDDEN_5M_MODULES):
            raise AssertionError(f"5m module leaked into clean route1_app runtime: {loaded}")
    assert "engine.route1_weather_paper" in loaded_engine_mods


def test_route1_isolated_endpoints_respond() -> None:
    """Verify isolated Route 1 handlers (`/health`, `/weather`, `/experts`, `/api/route1/status`)."""
    import route1_app

    health = asyncio.run(route1_app.route1_health())
    assert health["status"] == "ok"
    assert health["isolated_from_5m_sniper"] is True
    assert health["port"] == 8089

    w_resp = asyncio.run(route1_app.route1_weather_summary())
    e_resp = asyncio.run(route1_app.route1_experts_summary())
    assert w_resp.status_code == 200 and "Route 1" in w_resp.body.decode("utf-8")
    assert e_resp.status_code == 200 and "Route 1" in e_resp.body.decode("utf-8")

    st = asyncio.run(route1_app.api_route1_status())
    assert st["status"] == "ok" and st["paper_only"] is True and "summary" in st
