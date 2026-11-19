from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict
import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import pandas as pd

from engine.backtester import StrategyConfig, compute_raw_streak_distribution, run_backtest
from engine.indicators import enrich_indicators
from engine.master_strategy import (
    MASTER_REPORT_PATH,
    LivePaperTrader,
    MasterStrategySpec,
    generate_master_2year_validation,
)
from engine.multi_strategy_arena import MultiStrategyArena
from engine.notifier import NotificationManager
from engine.oracle_sniper import TWAP_V2_FILTERED_CONFIG
from engine.oracle_sniper_ledger import PAPER_40U_RISK_POLICY, PAPER_40U_STATE_PATH, SniperLedger
from engine.oracle_sniper_runner import PAPER_40U_EXEC_POLICY, OracleSniperRunner
from engine.polymarket_auto_trader import PolymarketAutoTrader
from engine.polymarket_sniper import fetch_polymarket_crypto_markets
from engine.review_manager import ReviewManager
from engine.scanner import LiveScanner
from engine.vpn_guardian import VpnGuardian
from engine.watchdog import SystemWatchdog

ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
STATIC_DIR = ROOT_DIR / "static"

notifier = NotificationManager()
paper_trader = LivePaperTrader()
auto_trader = PolymarketAutoTrader()
vpn_guardian = VpnGuardian(auto_trader=auto_trader, notifier=notifier)
review_manager = ReviewManager(notifier=notifier, auto_trader=auto_trader, paper_trader=paper_trader)
scanner = LiveScanner(
    notifier,
    paper_trader,
    auto_trader,
    vpn_guardian=vpn_guardian,
    review_manager=review_manager,
)
review_manager.scanner = scanner
arena = MultiStrategyArena()
scanner.arena = arena
sniper = OracleSniperRunner(
    auto_trader,
    scanner.chainlink_feed,
    notifier,
    cfg=TWAP_V2_FILTERED_CONFIG,
    ledger=SniperLedger(path=PAPER_40U_STATE_PATH, policy=PAPER_40U_RISK_POLICY),
    policy=PAPER_40U_EXEC_POLICY,
)
scanner.sniper = sniper
watchdog = SystemWatchdog(
    risk_controller=scanner.risk_controller,
    arena=arena,
    notifier=notifier,
    scanner=scanner,
)
scanner.watchdog = watchdog

DF_CACHE: dict[str, pd.DataFrame] = {}
STREAK_DIST_CACHE: dict[str, dict[str, Any]] = {}
MASTER_REPORT_CACHE: dict[str, Any] | None = None
_REPLAY_CURSOR: int = 0


def get_cached_df(symbol: str) -> pd.DataFrame:
    sym = symbol.upper()
    if sym not in DF_CACHE:
        p = DATA_DIR / f"{sym}_5m.parquet"
        if not p.exists():
            raise HTTPException(status_code=404, detail=f"Data file for {sym} not found at {p}")
        raw = pd.read_parquet(p).sort_values("open_time").reset_index(drop=True)
        DF_CACHE[sym] = enrich_indicators(raw)
        STREAK_DIST_CACHE[sym] = compute_raw_streak_distribution(DF_CACHE[sym])
    return DF_CACHE[sym]


def get_master_report(force_regen: bool = False) -> dict[str, Any]:
    global MASTER_REPORT_CACHE
    if not force_regen and MASTER_REPORT_CACHE is not None:
        return MASTER_REPORT_CACHE
    if not force_regen and MASTER_REPORT_PATH.exists():
        try:
            MASTER_REPORT_CACHE = json.loads(MASTER_REPORT_PATH.read_text(encoding="utf-8"))
            return MASTER_REPORT_CACHE
        except Exception:
            pass
    dfs = {sym: get_cached_df(sym) for sym in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]}
    MASTER_REPORT_CACHE = generate_master_2year_validation(dfs)
    return MASTER_REPORT_CACHE


@asynccontextmanager
async def lifespan(app: FastAPI):
    for sym in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
        try:
            get_cached_df(sym)
        except Exception:
            pass
    get_master_report()
    await vpn_guardian.start()
    await auto_trader.check_geoblock_status()
    w = await auto_trader.sync_live_wallet_balance(include_portfolio=True)
    has_pending_seq = isinstance(paper_trader.state.get("pending_step2"), dict) and bool(
        paper_trader.state["pending_step2"].get("stake_usd")
    )
    if (
        w.get("total_equity_pusd", 0.0) >= 2.50
        and scanner._get_active_global_sequence()[0] is None
        and not bool(scanner._pending_oracle_settlements)
        and not has_pending_seq
    ):
        paper_trader.sync_with_live_wallet(
            float(w["total_equity_pusd"]),
            float(w.get("cash_pusd", 0.0)),
            is_dry_run=auto_trader.config.dry_run,
        )
    await auto_trader.refresh_all_live_5m_markets()
    await scanner.scan_once()
    scanner.start()
    await watchdog.start()
    review_manager.set_auto_graduate(False)
    sniper.start()

    async def _review_periodic_task():
        while True:
            try:
                await asyncio.sleep(300)
                await review_manager.check_and_trigger_graduation_or_circuit_breaker()
                await review_manager.check_and_run_periodic_review()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                print(f"[Review Loop Warning] {exc}")

    review_task = asyncio.create_task(_review_periodic_task())
    yield
    review_task.cancel()
    await sniper.stop()
    await watchdog.stop()
    scanner.stop()
    await vpn_guardian.stop()
    paper_trader.save()


app = FastAPI(title="Polymarket 5m 三幣共振・訂單流衰竭狙擊系統", lifespan=lifespan)
STATIC_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


class BacktestRequest(BaseModel):
    symbol: str = "ETHUSDT"
    rsi_period: int = 6
    oversold_rsi: float = 15.0
    overbought_rsi: float = 85.0
    min_streak: int = 4
    min_streak_ret_pct: float = 0.28
    use_bb_filter: bool = True
    min_vol_ratio: float = 1.15
    min_wick_ratio: float = 0.12
    direction: str = "both"
    win_mode: str = "target_bounce"
    target_bounce_pct: float = 0.10
    max_martingale_steps: int = 2
    martingale_multiplier: float = 2.15
    payout_ratio: float = 0.95
    base_stake: float = 10.0
    apply_to_live: bool = False


@app.get("/", response_class=HTMLResponse)
async def index():
    html_file = STATIC_DIR / "index.html"
    if html_file.exists():
        return FileResponse(html_file)
    return HTMLResponse("<h1>Dashboard loading...</h1>")


@app.get("/api/status")
async def api_status():
    if not scanner.live_state:
        await scanner.scan_once()
    if not auto_trader.last_geoblock_status:
        await auto_trader.check_geoblock_status()
    live_5m = await auto_trader.refresh_all_live_5m_markets()
    return {
        "live": scanner.live_state,
        "resonance": scanner.resonance_state,
        "active_sequences": scanner.active_sequences,
        "paper_account": paper_trader.state,
        "notifications": notifier.history[:50],
        "notify_config": notifier.config.__dict__,
        "clob_config": auto_trader.get_public_config(),
        "clob_geoblock": auto_trader.last_geoblock_status,
        "vpn_guardian": vpn_guardian.stats,
        "clob_orders": auto_trader.order_history[:30],
        "clob_active_positions": auto_trader.active_clob_positions,
        "live_5m_markets": live_5m,
        "review_state": {
            "next_review_sec": review_manager.get_time_to_next_review_sec(),
            "last_review_ts_ms": review_manager.state.get("last_review_ts_ms"),
            "next_review_ts_ms": review_manager.state.get("next_review_ts_ms"),
            "review_count": review_manager.state.get("review_count", 0),
            "latest_report": review_manager.state.get("latest_report"),
        },
        "confidence_state": review_manager.evaluate_confidence(),
        "arena_leaderboard": arena.get_leaderboard() if arena else {},
        "lanes": {sym: asdict(lane) for sym, lane in scanner.risk_controller.lanes.items()} if hasattr(scanner, "risk_controller") else {},
        "chainlink_oracle": scanner.chainlink_feed.get_status() if hasattr(scanner, "chainlink_feed") else {},
        "watchdog": watchdog.get_status() if watchdog else {},
        "sniper": sniper.status(),
    }


@app.get("/api/sniper/status")
async def api_sniper_status():
    return sniper.status()


@app.get("/api/sniper/trades")
async def api_sniper_trades(limit: int = 50):
    return {"stats": sniper.ledger.stats(), "trades": sniper.ledger.state["trades"][: max(1, min(limit, 500))]}


@app.post("/api/sniper/enable")
async def api_sniper_enable(payload: dict[str, Any] | None = None):
    enabled = bool((payload or {}).get("enabled", True))
    sniper.set_enabled(enabled)
    return {"status": "ok", "enabled": sniper.ledger.enabled}


@app.get("/api/arena/leaderboard")
async def api_arena_leaderboard():
    if arena:
        return arena.get_leaderboard()
    return {}


@app.get("/api/lanes/status")
async def api_lanes_status():
    if hasattr(scanner, "risk_controller"):
        return {sym: asdict(lane) for sym, lane in scanner.risk_controller.lanes.items()}
    return {}


@app.get("/api/watchdog/status")
async def api_watchdog_status():
    if watchdog:
        return watchdog.get_status()
    return {}


@app.post("/api/watchdog/send_daily_summary")
async def api_send_daily_summary():
    if watchdog:
        msg = await watchdog.send_daily_strategy_summary()
        return {"status": "ok", "message": msg}
    return {"status": "error", "detail": "Watchdog not initialized"}


@app.get("/api/polymarket/markets")
async def api_polymarket_markets():
    return await fetch_polymarket_crypto_markets()


from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse

@app.get("/polymarket.pac", response_class=PlainTextResponse)
async def polymarket_pac():
    pac_script = """function FindProxyForURL(url, host) {
    // 1. Immediately bypass localhost, LAN, Google/Language-Server, and Binance so IDE & market feeds never touch proxy
    if (
        isPlainHostName(host) ||
        host === "127.0.0.1" ||
        host === "localhost" ||
        shExpMatch(host, "*.google.com") ||
        shExpMatch(host, "*.googleapis.com") ||
        shExpMatch(host, "*.gstatic.com") ||
        shExpMatch(host, "*.binance.com") ||
        shExpMatch(host, "*.binance.vision")
    ) {
        return "DIRECT";
    }
    // 2. Route ALL Polymarket Web UI, Gamma/Data/CLOB APIs, Sunbreak Geoblock SDK, Deposit/Bridge (fun.xyz/relay.link)
    //    and IP check endpoints through local High-Availability HK Gateway (127.0.0.1:10809) so Taiwan IP never leaks
    if (
        dnsDomainIs(host, "polymarket.com") ||
        shExpMatch(host, "*.polymarket.com") ||
        shExpMatch(host, "*.poly.market") ||
        shExpMatch(host, "*.polymarket.dev") ||
        shExpMatch(host, "*.polymarket.sh") ||
        dnsDomainIs(host, "sunbreak.com") ||
        shExpMatch(host, "*.sunbreak.com") ||
        dnsDomainIs(host, "fun.xyz") ||
        shExpMatch(host, "*.fun.xyz") ||
        dnsDomainIs(host, "relay.link") ||
        shExpMatch(host, "*.relay.link") ||
        dnsDomainIs(host, "ipapi.co") ||
        shExpMatch(host, "*.ipapi.co") ||
        dnsDomainIs(host, "ip-api.com") ||
        shExpMatch(host, "*.ip-api.com") ||
        dnsDomainIs(host, "ipinfo.io") ||
        shExpMatch(host, "*.ipinfo.io")
    ) {
        return "PROXY 127.0.0.1:10809; SOCKS5 127.0.0.1:10809; PROXY 43.99.60.244:8089; DIRECT";
    }
    return "DIRECT";
}"""
    return PlainTextResponse(pac_script, media_type="application/x-ns-proxy-autoconfig")


@app.get("/api/polymarket/vpn_status")
async def api_polymarket_vpn_status():
    stats = await vpn_guardian.probe_all_nodes()
    vpn_guardian.ensure_macos_pac_active(force_refresh=False)
    return {"status": "ok", "vpn_guardian": stats}


@app.get("/api/polymarket/geoblock")
async def api_polymarket_geoblock():
    await vpn_guardian.probe_all_nodes()
    geo = auto_trader.last_geoblock_status
    return {
        "status": "ok",
        "geoblock": geo,
        "vpn_guardian": vpn_guardian.stats,
        "installed_vpn_apps": auto_trader.get_installed_vpn_apps(),
    }


@app.post("/api/polymarket/launch_vpn")
async def api_polymarket_launch_vpn(payload: dict[str, Any]):
    app_id = str(payload.get("app_id", "protonvpn"))
    return auto_trader.launch_vpn_app(app_id)


@app.post("/api/polymarket/clob_config")
async def api_polymarket_save_clob_config(payload: dict[str, Any]):
    pub = auto_trader.save_config(payload)
    derive_res = None
    if auto_trader.config.private_key and (
        payload.get("derive_creds", True)
        or not (auto_trader.config.api_key and auto_trader.config.api_secret and auto_trader.config.api_passphrase)
    ):
        derive_res = await auto_trader.derive_and_verify_credentials()
    geo = await auto_trader.check_geoblock_status()
    return {
        "status": "ok",
        "clob_config": auto_trader.get_public_config(),
        "derive_result": derive_res,
        "geoblock": geo,
    }


@app.post("/api/polymarket/derive_creds")
async def api_polymarket_derive_creds():
    res = await auto_trader.derive_and_verify_credentials()
    return {
        "status": "ok" if res.get("ok") else "error",
        "result": res,
        "clob_config": auto_trader.get_public_config(),
    }


@app.post("/api/polymarket/test_order")
async def api_polymarket_test_order(payload: dict[str, Any] | None = None):
    data = payload or {}
    sym = str(data.get("symbol", "ETHUSDT")).upper()
    direction = str(data.get("direction", "LONG_BOUNCE"))
    stake_u = float(data.get("stake_usd", paper_trader.state.get("next_step1_stake_u", 2.55)))
    rec = await auto_trader.execute_5m_entry(
        symbol=sym,
        direction=direction,
        stake_usd=stake_u,
        step=1,
        reason="MANUAL_TEST_ORDER",
    )
    return {
        "status": "ok",
        "order": rec,
        "clob_orders": auto_trader.order_history[:30],
    }


@app.post("/api/polymarket/redeem")
async def api_polymarket_redeem():
    res = await scanner._check_and_notify_redeem()
    return {
        "status": "ok",
        "redeem": res,
        "paper_account": paper_trader.state,
        "live_wallet": auto_trader.live_wallet_state,
    }


@app.get("/api/polymarket/clob_balance")
async def api_polymarket_clob_balance():
    w = await auto_trader.sync_live_wallet_balance(include_portfolio=True)
    has_pending_seq = isinstance(paper_trader.state.get("pending_step2"), dict) and bool(
        paper_trader.state["pending_step2"].get("stake_usd")
    )
    if (
        w.get("total_equity_pusd", 0.0) >= 2.50
        and scanner._get_active_global_sequence()[0] is None
        and not bool(scanner._pending_oracle_settlements)
        and not has_pending_seq
    ):
        paper_trader.sync_with_live_wallet(
            float(w["total_equity_pusd"]),
            float(w.get("cash_pusd", 0.0)),
            is_dry_run=auto_trader.config.dry_run,
        )
    return {
        "status": "ok",
        "ok": True,
        "cash_pusd": w.get("cash_pusd", 0.0),
        "conditional_shares": w.get("conditional_shares", {}),
        "total_equity_pusd": w.get("total_equity_pusd", 0.0),
        "paper_account": paper_trader.state,
    }


@app.get("/api/review/latest")
async def api_review_latest():
    return {
        "status": "ok",
        "state": review_manager.state,
        "next_review_sec": review_manager.get_time_to_next_review_sec(),
    }


@app.post("/api/review/run")
async def api_review_run(payload: dict[str, Any] | None = None):
    report = review_manager.generate_review(force=True)
    if bool((payload or {}).get("notify_telegram", False)):
        await review_manager.dispatch_telegram_review(report)
    return {
        "status": "ok",
        "report": report,
        "state": review_manager.state,
        "confidence_state": review_manager.evaluate_confidence(),
    }


@app.get("/api/confidence/status")
async def api_confidence_status():
    return {
        "status": "ok",
        "confidence_state": review_manager.evaluate_confidence(),
    }


@app.post("/api/confidence/toggle_auto")
async def api_confidence_toggle_auto(payload: dict[str, Any] | None = None):
    enabled = bool((payload or {}).get("enabled", True))
    conf = review_manager.set_auto_graduate(enabled)
    if enabled:
        await review_manager.check_and_trigger_graduation_or_circuit_breaker()
        conf = review_manager.evaluate_confidence()
    return {
        "status": "ok",
        "confidence_state": conf,
        "clob_config": auto_trader.get_public_config(),
    }


@app.post("/api/confidence/graduate_now")
async def api_confidence_graduate_now():
    conf = await review_manager.promote_to_real_money(reason="使用者於戰情室手動一鍵啟動真金實盤")
    w = await auto_trader.sync_live_wallet_balance()
    if w.get("total_equity_pusd", 0.0) >= 2.55:
        paper_trader.sync_with_live_wallet(
            float(w["total_equity_pusd"]),
            float(w.get("cash_pusd", 0.0)),
            is_dry_run=auto_trader.config.dry_run,
        )
    return {
        "status": "ok",
        "confidence_state": conf,
        "clob_config": auto_trader.get_public_config(),
        "paper_account": paper_trader.state,
    }


@app.get("/api/master/report")
async def api_master_report():
    return get_master_report()


@app.get("/api/master/paper")
async def api_master_paper():
    return paper_trader.state


@app.post("/api/master/paper/reset")
async def api_master_paper_reset(payload: dict[str, Any] | None = None):
    global _REPLAY_CURSOR
    _REPLAY_CURSOR = 0
    initial_u = float((payload or {}).get("initial_u", 10.0))
    target_u = float((payload or {}).get("target_u", 1000.0))
    state = paper_trader.reset_account(initial_u=initial_u, target_u=target_u)
    return {"status": "ok", "paper_account": state}


@app.post("/api/master/paper/simulate_step")
async def api_master_paper_simulate_step(payload: dict[str, Any] | None = None):
    global _REPLAY_CURSOR
    steps_count = int((payload or {}).get("count", 1))
    steps_count = max(1, min(100, steps_count))

    rep = get_master_report()
    seqs = list(reversed(rep.get("recent_100_sequences", [])))
    if not seqs:
        raise HTTPException(status_code=400, detail="No sequences available for simulation")

    executed = []
    for _ in range(steps_count):
        s = seqs[_REPLAY_CURSOR % len(seqs)]
        _REPLAY_CURSOR += 1
        actual_steps = int(s["actual_steps_to_win"])
        won_step = actual_steps if actual_steps <= paper_trader.spec.max_martingale_steps else None
        exit_cents = float(s.get("exit_cents", 100.0 if won_step else 0.0))
        exit_type = str(s.get("exit_type", "100¢_EXPIRATION" if won_step else "0¢_STOP_LOSS"))
        rec = paper_trader.record_completed_sequence(
            symbol=s["symbol"],
            direction=s["direction"],
            entry_price=float(s["price"]),
            resolve_price=float(s["price"]) * (1.0012 if s["direction"] == "LONG_BOUNCE" else 0.9988),
            rsi_6=float(s["rsi_6"]),
            streak=int(s["streak"]),
            won_at_step=won_step,
            exit_cents=exit_cents,
            exit_type=exit_type,
            timestamp_ms=int(s["timestamp_ms"]),
            is_simulation_replay=True,
        )
        executed.append(rec)

    return {
        "status": "ok",
        "executed_count": len(executed),
        "last_trade": executed[-1] if executed else None,
        "paper_account": paper_trader.state,
    }


@app.get("/api/presets")
async def api_presets():
    preset_path = DATA_DIR / "optimal_presets.json"
    if preset_path.exists():
        return json.loads(preset_path.read_text(encoding="utf-8"))
    return {}


@app.get("/api/streak_stats/{symbol}")
async def api_streak_stats(symbol: str):
    sym = symbol.upper()
    get_cached_df(sym)
    return STREAK_DIST_CACHE.get(sym, {})


@app.post("/api/backtest")
async def api_backtest(req: BacktestRequest):
    sym = req.symbol.upper()
    df = get_cached_df(sym)
    cfg_dict = req.model_dump(exclude={"apply_to_live"})
    cfg_dict["symbol"] = sym
    cfg = StrategyConfig(**cfg_dict)
    result = run_backtest(df, cfg)
    result["raw_streak_distribution"] = STREAK_DIST_CACHE.get(sym, {})

    if req.apply_to_live:
        scanner.configs[sym] = cfg
        scanner.expected_win_rates[sym] = float(result["summary"]["sequence_win_rate_pct"])

    return result


@app.post("/api/notify_config")
async def api_update_notify_config(payload: dict[str, Any]):
    updated = notifier.save_config(payload)
    return {"status": "ok", "config": updated.__dict__}


@app.post("/api/test_notify")
async def api_test_notify(payload: dict[str, Any] | None = None):
    sym = (payload or {}).get("symbol", "ETHUSDT")
    live_sym = scanner.live_state.get(sym, {})
    price = live_sym.get("price", 2410.0)
    forming = live_sym.get("forming_candle", {})
    rsi = forming.get("rsi_6", 13.8)
    s1 = paper_trader.state.get("next_step1_stake_u", 1.05)
    sh1 = paper_trader.state.get("next_step1_shares", 2.1)
    s2 = paper_trader.state.get("next_step2_stake_u", 2.25)
    sh2 = paper_trader.state.get("next_step2_shares", 4.4)
    tot_eq = paper_trader.state.get("total_equity_u", 10.0)
    esc_price = round(price * 1.001, 2)
    event = await notifier.dispatch_signal({
        "symbol": sym,
        "event_type": "SIGNAL_ENTRY",
        "direction": "LONG_BOUNCE",
        "step": 1,
        "max_steps": 2,
        "stake_usd": s1,
        "price": price,
        "rsi": rsi,
        "streak": 4,
        "streak_ret_pct": -0.42,
        "expected_win_rate_pct": 91.47,
        "message": (
            f"【Polymarket 5m 實戰指令】掛 Maker 限價單 BUY UP @ 51¢ 買入 {sh1} 份（${s1:.2f}U）| "
            f"盤中觸及 ${esc_price} (+0.10%) 掛 72¢~78¢ 保利單，收紅領 100¢ | 備用二碼 {sh2} 份（${s2:.2f}U）| 當前總權益 ${tot_eq:.2f}U"
        ),
    })
    return {"status": "ok", "event": event}
