"""Live runner for the Chainlink late-window sniper (BTC / ETH / SOL, one position at a time).

Every second: refresh the market hub, build a fair-value view per coin, and either manage the open position
(early take-profit / structural stop) or rank the coins that pass every entry gate and fire a single FAK order.
Settlement is resolved from Polymarket's official Gamma outcome and winnings are redeemed automatically.
"""
from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
import pathlib
import time
from typing import Any, Optional

from engine.master_strategy import compute_vault_locked_u
from engine.oracle_sniper import (
    DEFAULT_SNIPER_CONFIG,
    Candidate,
    FairValueEstimate,
    OpenPosition,
    OrderBookTop,
    SniperConfig,
    check_frozen_config_parity,
    compute_shares,
    effective_exit_proceeds,
    entry_limit_price,
    estimate_fair_value,
    estimate_twap_fair_value,
    evaluate_entry,
    evaluate_exit,
    pick_best_candidate,
    taker_fee_per_share,
)
from engine.oracle_sniper_feeds import SYMBOLS, SniperMarketHub, SymbolState
from engine.oracle_sniper_ledger import SniperLedger
from engine.oracle_sniper_notify import alert_message, entry_message, exit_message

MAX_ENTRY_ATTEMPTS_PER_WINDOW = 3
ATTEMPT_SPACING_SEC = 5.0
EXIT_RETRY_SPACING_SEC = 2.0
MAX_EXIT_ATTEMPTS = 5
CHAINLINK_MAX_AGE_SEC = 4.5
BOOK_MAX_AGE_SEC = 3.5
WALLET_REFRESH_SEC = 30.0
PRODUCTION_FEE_RATES: dict[str, float] = {"BTCUSDT": 0.05, "ETHUSDT": 0.01, "SOLUSDT": 0.01}

UNCERTAIN_FILL_STATUSES = ("ERROR", "UNKNOWN", "DUST_FILL")
RECONCILE_DELAYS_SEC = (6.0, 12.0, 20.0, 30.0)

View = tuple[FairValueEstimate, OrderBookTop]


@dataclass(frozen=True)
class SniperExecutionPolicy:
    """Concurrency, pre-order data-freshness, frozen-config parity, and pilot constraints."""

    max_concurrent_positions: int = 1
    per_symbol_fees: Optional[dict[str, float]] = None
    require_rtds_connected: bool = False
    require_twap_fresh: bool = False
    max_chainlink_age_sec: float = CHAINLINK_MAX_AGE_SEC
    max_binance_age_sec: Optional[float] = None
    frozen_config_path: Optional[pathlib.Path] = None
    frozen_model_name: Optional[str] = None
    pilot_fixed_shares: Optional[float] = None


PRODUCTION_EXEC_POLICY = SniperExecutionPolicy(
    max_concurrent_positions=3,
    per_symbol_fees=PRODUCTION_FEE_RATES,
    require_rtds_connected=True,
    require_twap_fresh=True,
    max_chainlink_age_sec=3.0,
    max_binance_age_sec=2.0,
    frozen_config_path=pathlib.Path(__file__).resolve().parent.parent / "edge_validation" / "frozen_config.json",
    frozen_model_name="v2_filtered",
    pilot_fixed_shares=5.0,
)


class OracleSniperRunner:
    """Multi-coin Chainlink late-window execution engine (supports 1-slot legacy or 3-slot 1-per-coin Section 3)."""

    def __init__(
        self,
        auto_trader: Any,
        chainlink_feed: Any,
        notifier: Any,
        cfg: SniperConfig = DEFAULT_SNIPER_CONFIG,
        ledger: Optional[SniperLedger] = None,
        hub: Optional[SniperMarketHub] = None,
        policy: SniperExecutionPolicy = SniperExecutionPolicy(),
    ) -> None:
        self.auto_trader = auto_trader
        self.chainlink = chainlink_feed
        self.notifier = notifier
        self.cfg = cfg
        self.ledger = ledger or SniperLedger()
        self.hub = hub or SniperMarketHub(auto_trader, cfg)
        self.policy = policy
        self.running = False
        self._task: Optional[asyncio.Task[None]] = None
        self._bg: set[asyncio.Task[Any]] = set()
        self._positions: dict[str, OpenPosition] = {}
        self._trades: dict[str, dict[str, Any]] = {}
        self._traded_windows: set[int] = set()
        self._traded_coin_windows: set[tuple[str, int]] = set()
        self._attempts: dict[tuple[str, int], int] = {}
        self._win_attempts: dict[int, int] = {}
        self._open_cache: dict[tuple[str, int], Optional[float]] = {}
        self._last_attempt_ts = 0.0
        self._exit_attempts: dict[str, int] = {}
        self._next_exit_ts: dict[str, float] = {}
        self._last_wallet_ts = 0.0
        self.heartbeat_ts = 0.0
        self.last_error: Optional[str] = None
        self.diag: dict[str, dict[str, Any]] = {}

    @property
    def _pos(self) -> Optional[OpenPosition]:
        return next(iter(self._positions.values()), None)

    @property
    def _trade(self) -> Optional[dict[str, Any]]:
        return next(iter(self._trades.values()), None)

    def _fee_for(self, sym: str) -> float:
        fees = self.policy.per_symbol_fees or {}
        return float(fees.get(sym, self.cfg.fee_rate))

    def _cfg_for(self, sym: str) -> SniperConfig:
        fee = self._fee_for(sym)
        return self.cfg if abs(fee - self.cfg.fee_rate) < 1e-12 else SniperConfig(**{**asdict(self.cfg), "fee_rate": fee})

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        """Start feeds and the 1 Hz loop (idempotent)."""
        if self._task is not None and not self._task.done():
            return
        self.running = True
        self.hub.start()
        self._seed_traded_windows()
        self._recover_open_position()
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        """Stop the loop and the feeds (open positions stay persisted in the ledger)."""
        self.running = False
        if self._task is not None:
            self._task.cancel()
        for t in list(self._bg):
            t.cancel()
        await self.hub.stop()

    def _spawn(self, coro: Any) -> None:
        task = asyncio.create_task(coro)
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)
        task.add_done_callback(self._log_task_error)

    @staticmethod
    def _log_task_error(task: "asyncio.Task[Any]") -> None:
        if not task.cancelled() and task.exception() is not None:
            print(f"[Sniper Warning] background task failed: {task.exception()!r}")

    async def _run_loop(self) -> None:
        while self.running:
            t0 = time.time()
            try:
                await self._tick(t0)
                self.last_error = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - the loop must survive any single-tick failure
                self.last_error = f"{type(exc).__name__}: {exc}"
                print(f"[Sniper Tick Error] {self.last_error}")
            await asyncio.sleep(max(0.05, 1.0 - (time.time() - t0)))

    # ------------------------------------------------------------------ tick
    async def _tick(self, now: float) -> None:
        await self.hub.refresh_windows(now)
        window_ts = self.hub.states[SYMBOLS[0]].window_ts
        elapsed = now - window_ts
        self.heartbeat_ts = now
        self._maybe_refresh_wallet(now)
        total, _cash, _vault = self._equity()
        if self._wallet_fresh(now):
            self.ledger.ensure_baseline(total)
            self.ledger.note_equity(total)
            self.ledger.roll_day_if_needed(total)
        views = self._build_views(now, elapsed)
        for sym in list(self._positions):
            pos = self._positions.get(sym)
            if pos is None:
                continue
            if pos.window_ts == window_ts:
                await self._manage_position(sym, views, elapsed, now)
            else:
                self._begin_settlement(sym)
        if self._entry_allowed(window_ts, now):
            await self._seek_entry(views, window_ts, elapsed, now)

    # ------------------------------------------------------------------ market views
    def _rtds_open(self, sym: str, window_ts: int) -> Optional[float]:
        """Official 60s Chainlink TWAP at the 5m boundary (falls back to spot ticks only in unit-test dummies)."""
        key = (sym, window_ts)
        if key in self._open_cache:
            return self._open_cache[key]
        boundary_ms = window_ts * 1000.0
        twap_fn = getattr(self.chainlink, "get_settlement_twap_at", None)
        best = twap_fn(sym, window_ts) if callable(twap_fn) else None
        if best is None:
            twap_buf = getattr(self.chainlink, "twap_ticks", {}).get(sym) or ()
            src_ticks = twap_buf if twap_buf else (self.chainlink.ticks.get(sym) or ())
            dist = 4001.0
            for ts_ms, val in src_ticks:
                d = ts_ms - boundary_ms
                if -1000.0 <= d <= 4000.0 and abs(d) < dist:
                    best, dist = float(val), abs(d)
        if best is not None or time.time() - window_ts > 8.0:
            self._open_cache[key] = best  # final: later ticks can no longer qualify
            if len(self._open_cache) > 60:
                self._open_cache.pop(next(iter(self._open_cache)))
        return best

    def _strike(self, sym: str, st: SymbolState) -> tuple[Optional[float], str]:
        if st.price_to_beat:
            return float(st.price_to_beat), "GAMMA_OFFICIAL"
        opened = self._rtds_open(sym, st.window_ts)
        if opened is None:
            return None, "UNSET"
        has_twap = bool(getattr(self.chainlink, "twap_ticks", {}).get(sym))
        return opened, ("POLY_TWAP_60S" if has_twap else "RTDS_BUCKET_OPEN")

    def _book_for(self, st: SymbolState, now: float, elapsed: float) -> OrderBookTop:
        if elapsed < 1.5:
            return OrderBookTop()
        b = st.book
        up_ok = (now - st.up_local_ts) <= BOOK_MAX_AGE_SEC
        dn_ok = (now - st.down_local_ts) <= BOOK_MAX_AGE_SEC
        return OrderBookTop(
            b.up_bid if up_ok else None, b.up_ask if up_ok else None,
            b.down_bid if dn_ok else None, b.down_ask if dn_ok else None,
        )

    def _realized_twap_240(self, sym: str, window_ts: int, elapsed: float) -> tuple[Optional[float], float]:
        if elapsed <= 240.0:
            return None, 0.0
        start_ms = (window_ts + 240) * 1000.0
        end_ms = (window_ts + elapsed) * 1000.0
        vals = [float(v) for ts_ms, v in (self.chainlink.ticks.get(sym) or ()) if start_ms <= ts_ms <= end_ms]
        return ((sum(vals) / len(vals)) if vals else None), max(0.0, elapsed - 240.0)

    def _entry_feed_block_reason(self, sym: str, st: SymbolState, now: float) -> str:
        if self.policy.require_rtds_connected and not bool(getattr(self.chainlink, "connected", True)):
            return "RTDS_DISCONNECTED"
        if self.policy.require_twap_fresh:
            tw_fn = getattr(self.chainlink, "get_latest_twap", None)
            if not callable(tw_fn):
                return "NO_TWAP_FEED"
            tw_px, _tw_age = tw_fn(sym, max_age_sec=self.policy.max_chainlink_age_sec)
            if tw_px is None:
                return "CHAINLINK_TWAP_STALE"
        if self.policy.max_binance_age_sec is not None:
            if st.binance_price is None or (now - st.binance_ts) > self.policy.max_binance_age_sec:
                return "BINANCE_STALE"
        return ""

    def _build_views(self, now: float, elapsed: float) -> dict[str, View]:
        views: dict[str, View] = {}
        cl_lim = max(CHAINLINK_MAX_AGE_SEC, self.policy.max_chainlink_age_sec)
        for sym, st in self.hub.states.items():
            cl_px, age = self.chainlink.get_latest_price(sym, max_age_sec=cl_lim)
            if cl_px is None:
                self.diag[sym] = {"status": "CHAINLINK_STALE", "cl_age_sec": age, "entry_blocked": "CHAINLINK_STALE"}
                continue
            lead = st.cex_lead_price(now)
            st.tracker.on_price(now, lead or cl_px)
            vol = st.tracker.get_realized_vol_1s(floor=self.cfg.min_1s_vol_floor)
            gap = st.tracker.update_oracle_basis(cl_px, lead)
            cvd, _obi, flow = st.tracker.get_orderflow_metrics(now)
            strike, src = self._strike(sym, st)
            if strike is None:
                self.diag[sym] = {"status": "NO_STRIKE", "entry_blocked": "NO_STRIKE"}
                continue
            c_sym = self._cfg_for(sym)
            if c_sym.use_twap_projection:
                a_mean, a_sec = self._realized_twap_240(sym, st.window_ts, elapsed)
                fv = estimate_twap_fair_value(strike, cl_px, 300.0 - elapsed, vol, (a_mean, a_sec), (flow, gap, cvd), c_sym)
            else:
                fv = estimate_fair_value(strike, cl_px, 300.0 - elapsed, vol, flow, gap, c_sym.student_t_df, cvd)
            if fv is None:
                continue
            book = self._book_for(st, now, elapsed)
            views[sym] = (fv, book)
            row = self._diag_row(fv, book, src, age, st)
            row["entry_blocked"] = self._entry_feed_block_reason(sym, st, now)
            self.diag[sym] = row
        return views

    @staticmethod
    def _diag_row(fv: FairValueEstimate, book: OrderBookTop, src: str, age: float, st: SymbolState) -> dict[str, Any]:
        return {
            "status": "OK", "strike_src": src, "cl_age_sec": age, "z": round(fv.raw_z_score, 2),
            "p_up": round(fv.p_true_up, 3), "gap_bps": round(fv.oracle_gap_bps, 2), "flow": round(fv.orderflow_score, 2),
            "up": [book.up_bid, book.up_ask], "down": [book.down_bid, book.down_ask], "has_tokens": bool(st.up_token),
        }

    # ------------------------------------------------------------------ equity / wallet
    def _equity(self) -> tuple[float, float, float]:
        w = self.auto_trader.live_wallet_state or {}
        cash = float(w.get("cash_pusd", 0.0))
        total = cash + float(w.get("portfolio_pusd", 0.0))
        vault, _next, _lbl = compute_vault_locked_u(
            max(float(self.ledger.state.get("peak_equity_u", 0.0)), total), float(self.ledger.state.get("vault_locked_u", 0.0))
        )
        self.ledger.state["vault_locked_u"] = vault
        return total, cash, vault

    def _maybe_refresh_wallet(self, now: float) -> None:
        if now - self._last_wallet_ts >= WALLET_REFRESH_SEC:
            self._last_wallet_ts = now
            self._spawn(self.auto_trader.sync_live_wallet_balance(include_portfolio=True))

    def _wallet_fresh(self, now: float) -> bool:
        updated_ms = float((self.auto_trader.live_wallet_state or {}).get("updated_at_ms", 0) or 0)
        return updated_ms > 0 and (now * 1000.0 - updated_ms) < 180_000.0

    def _perf_lines(self) -> str:
        total, cash, vault = self._equity()
        return self.ledger.performance_lines(total, cash, vault)

    # ------------------------------------------------------------------ entry
    def _config_parity_ok(self) -> bool:
        if not self.policy.frozen_config_path or not self.policy.frozen_model_name:
            return True
        ok, _diffs = check_frozen_config_parity(self.cfg, self.policy.frozen_config_path, self.policy.frozen_model_name)
        return ok

    def _entry_allowed(self, window_ts: int, now: float) -> bool:
        if len(self._positions) >= self.policy.max_concurrent_positions:
            return False
        if self.policy.max_concurrent_positions == 1 and window_ts in self._traded_windows:
            return False
        if self.ledger.guard_reason() is not None or self.ledger.state.get("pending_entry"):
            return False
        if self.policy.max_concurrent_positions == 1 and self._win_attempts.get(window_ts, 0) >= MAX_ENTRY_ATTEMPTS_PER_WINDOW:
            return False
        if now - self._last_attempt_ts < ATTEMPT_SPACING_SEC or not self._wallet_fresh(now):
            return False
        if not self._config_parity_ok():
            self.ledger.bump_skip("FROZEN_CONFIG_DRIFT")
            return False
        return bool((self.auto_trader.last_geoblock_status or {}).get("can_trade_api", False))

    def _symbol_can_enter(self, sym: str, window_ts: int) -> bool:
        if sym in self._positions or (sym, window_ts) in self._traded_coin_windows:
            return False
        if any(t.get("symbol") == sym for t in self.ledger.state.get("settling", [])):
            return False
        if self.ledger.symbol_paused(sym) or self._attempts.get((sym, window_ts), 0) >= MAX_ENTRY_ATTEMPTS_PER_WINDOW:
            return False
        blk = (self.diag.get(sym) or {}).get("entry_blocked")
        if blk:
            self.ledger.bump_skip(str(blk))
            return False
        return True

    async def _seek_entry(self, views: dict[str, View], window_ts: int, elapsed: float, now: float) -> None:
        cands: list[Candidate] = []
        for sym, (fv, book) in views.items():
            if not self._symbol_can_enter(sym, window_ts):
                continue
            cand = evaluate_entry(sym, self._cfg_for(sym), elapsed, fv, book)
            if cand is not None:
                cands.append(cand)
        if not cands:
            return
        if self.policy.max_concurrent_positions == 1:
            best = pick_best_candidate(cands)
            if best is not None:
                await self._enter(best, window_ts, now, len(cands))
            return
        remaining = self.policy.max_concurrent_positions - len(self._positions)
        ranked = sorted(cands, key=lambda c: c.net_ev + (0.01 if c.symbol == "BTCUSDT" else 0.0), reverse=True)
        for cand in ranked[:remaining]:
            if self.ledger.guard_reason() is not None:
                break
            await self._enter(cand, window_ts, now, len(cands))

    def _plan_size(self, cand: Candidate, limit_px: float) -> tuple[float, str]:
        total, cash, vault = self._equity()
        tradable = max(0.0, total - vault)
        if self.policy.pilot_fixed_shares is not None:
            shares = float(self.policy.pilot_fixed_shares)
            if tradable < shares * cand.total_cost_per_share + 0.05:
                return 0.0, "BELOW_MIN_LOT"
        else:
            shares = compute_shares(self._cfg_for(cand.symbol), tradable, cand.total_cost_per_share)
            if shares <= 0.0:
                return 0.0, "BELOW_MIN_LOT"
        if (cash - vault) < shares * limit_px * 1.03 + 0.05:
            return 0.0, "INSUFFICIENT_CASH"
        return shares, ""

    async def _enter(self, cand: Candidate, window_ts: int, now: float, n_cands: int) -> None:
        self._attempts[(cand.symbol, window_ts)] = self._attempts.get((cand.symbol, window_ts), 0) + 1
        self._win_attempts[window_ts] = self._win_attempts.get(window_ts, 0) + 1
        self._last_attempt_ts = now
        limit_px = entry_limit_price(self._cfg_for(cand.symbol), cand.ask, cand.p_true)
        shares, why = self._plan_size(cand, limit_px)
        st = self.hub.states[cand.symbol]
        token = st.up_token if cand.side == "UP" else st.down_token
        if shares <= 0.0 or not token:
            self.ledger.bump_skip(why or "NO_TOKEN")
            return
        intent = {
            "symbol": cand.symbol, "side": cand.side, "token_id": token, "window_ts": window_ts, "slug": st.slug,
            "condition_id": st.condition_id, "strike": st.price_to_beat, "ts": now, "target_shares": shares,
            "limit_px": limit_px, "est_cost_usd": round(shares * limit_px * 1.02, 4), "n_cands": n_cands,
            "cand": asdict(cand),
        }
        self.ledger.set_pending_entry(intent)
        t_send = time.monotonic()
        fill = await self.auto_trader.sniper_buy(token, shares, cand.ask, limit_px)
        lat_ms = round((time.monotonic() - t_send) * 1000.0, 1)
        self.ledger.record_attempt({
            "ts": now, "window_ts": window_ts, "symbol": cand.symbol, "side": cand.side, "sim_ask": cand.ask,
            "limit_px": limit_px, "filled": bool(fill.get("filled")), "actual_price": fill.get("avg_price"),
            "status": fill.get("status"), "latency_ms": lat_ms,
        })
        if fill.get("filled"):
            self._register_entry(cand, window_ts, now, n_cands, fill, token, limit_px)
            self.ledger.set_pending_entry(None)
            return
        self.ledger.bump_skip("NO_FILL")
        side_ts = st.up_local_ts if cand.side == "UP" else st.down_local_ts
        print(
            f"[Sniper] {cand.symbol} {cand.side} not filled: {fill.get('status')} {fill.get('error')} | "
            f"ask={cand.ask:.3f} cap={limit_px:.2f} bid={cand.bid} z={cand.z:.2f} p={cand.p_true:.3f} "
            f"t={cand.elapsed_sec:.0f}s book_age={now - side_ts:.1f}s"
        )
        if fill.get("status") in UNCERTAIN_FILL_STATUSES:
            self._traded_windows.add(window_ts)
            self._traded_coin_windows.add((cand.symbol, window_ts))
            self._spawn(self._reconcile_pending(intent))
        else:
            self.ledger.set_pending_entry(None)

    async def _reconcile_pending(self, intent: dict[str, Any]) -> None:
        """Establish whether an order of uncertain outcome filled; adopt the position or clear the intent."""
        verified_zero = False
        for delay in RECONCILE_DELAYS_SEC:
            await asyncio.sleep(delay)
            try:
                held = await self.auto_trader.sniper_token_balance(intent["token_id"])
            except Exception as exc:  # noqa: BLE001 - balance unavailable: keep trying, never assume
                print(f"[Sniper Reconcile] balance unavailable: {exc}")
                continue
            if held >= 1.0:
                px = float(intent["limit_px"])
                fill = {"filled": True, "shares": held, "usd": round(held * px, 4), "avg_price": px, "order_id": "",
                        "status": "ADOPTED_FROM_BALANCE", "error": None}
                cand = Candidate(**intent["cand"])
                self._register_entry(cand, int(intent["window_ts"]), float(intent["ts"]), int(intent["n_cands"]), fill,
                                     intent["token_id"], px)
                self.ledger.set_pending_entry(None)
                return
            verified_zero = True
        if verified_zero:
            self.ledger.set_pending_entry(None)
            return
        self._spawn(self._notify(alert_message(
            [f"{intent['symbol']} 下單結果無法確認（餘額查詢持續失敗），已暫停新開單；請至 Polymarket 確認後用 /resume 恢復"],
            self._perf_lines()), "SNIPER_ALERT", "SYSTEM"))

    def _register_entry(
        self, cand: Candidate, window_ts: int, now: float, n_cands: int, fill: dict[str, Any], token: str, limit_px: float
    ) -> None:
        st = self.hub.states[cand.symbol]
        fee_r = self._fee_for(cand.symbol)
        shares = float(fill["shares"])
        price = float(fill.get("avg_price") or cand.ask)
        cost_ps = price + taker_fee_per_share(price, fee_r)
        cost_usd = round(float(fill.get("usd") or shares * price) + shares * taker_fee_per_share(price, fee_r), 4)
        self._positions[cand.symbol] = OpenPosition(cand.symbol, cand.side, window_ts, shares, price, cost_ps, cand.p_true, cand.z)
        trade = {
            "id": f"{window_ts}-{cand.symbol}-{int(now * 1000)}", "symbol": cand.symbol, "side": cand.side, "window_ts": window_ts,
            "slug": st.slug, "token_id": token, "condition_id": st.condition_id, "shares": shares,
            "shares_open": shares, "entry_price": price, "sim_ask": cand.ask, "cost_per_share": cost_ps,
            "cost_usd": cost_usd, "fee_rate": fee_r, "entry_ts": now, "entry_elapsed": cand.elapsed_sec,
            "p_true": cand.p_true, "z": cand.z, "net_ev": cand.net_ev, "candidates": n_cands, "limit_px": limit_px,
            "order_id": fill.get("order_id", ""), "strike": st.price_to_beat, "partial_proceeds_usd": 0.0,
            "fill_status": fill.get("status"),
        }
        self._trades[cand.symbol] = trade
        self._traded_windows.add(window_ts)
        self._traded_coin_windows.add((cand.symbol, window_ts))
        self._exit_attempts[cand.symbol] = 0
        self.ledger.open_trade(trade)
        self._spawn(self._notify(entry_message(trade, self._perf_lines()), "SNIPER_ENTRY", cand.symbol))
        self._spawn(self.auto_trader.sync_live_wallet_balance())

    # ------------------------------------------------------------------ exit
    async def _manage_position(self, sym: str, views: dict[str, View], elapsed: float, now: float) -> None:
        pos, trade = self._positions.get(sym), self._trades.get(sym)
        if pos is None or trade is None or sym not in views:
            return
        if now < self._next_exit_ts.get(sym, 0.0) or self._exit_attempts.get(sym, 0) >= MAX_EXIT_ATTEMPTS:
            return
        fv, book = views[sym]
        decision = evaluate_exit(self._cfg_for(sym), pos, elapsed, fv, book)
        if decision is None:
            return
        self._exit_attempts[sym] = self._exit_attempts.get(sym, 0) + 1
        self._next_exit_ts[sym] = now + EXIT_RETRY_SPACING_SEC
        slip = 0.05 if decision.reason == "EMERGENCY_STOP_LOSS" else 0.03
        sell = await self.auto_trader.sniper_sell(trade["token_id"], pos.shares, max(0.01, round(decision.bid - slip, 2)))
        if sell.get("status") == "NO_BALANCE":
            self._reconcile_gone_position(sym, decision, now)
            return
        if not sell.get("filled"):
            self.ledger.bump_skip("EXIT_NO_FILL")
            print(f"[Sniper] exit {decision.reason} not filled: {sell.get('status')} {sell.get('error')}")
            return
        self._apply_sale(sym, decision.reason, sell, now)

    def _reconcile_gone_position(self, sym: str, decision: Any, now: float) -> None:
        """Verified zero balance while the ledger says we hold shares: an earlier sell must have filled."""
        trade = self._trades.get(sym)
        if trade is None:
            return
        trade["reconciled_from_balance"] = True
        self._apply_sale(sym, decision.reason, {"shares": float(trade["shares_open"]), "avg_price": decision.bid}, now)
        self._spawn(self._notify(alert_message(
            [f"{trade['symbol']} 賣出回應遺失但餘額已為 0，已依 ${decision.bid:.2f} 對帳結案（以錢包為準，請核對）"],
            self._perf_lines()), "SNIPER_ALERT", "SYSTEM"))

    def _apply_sale(self, sym: str, reason: str, sell: dict[str, Any], now: float) -> None:
        pos, trade = self._positions.get(sym), self._trades.get(sym)
        if pos is None or trade is None:
            return
        sold = float(sell["shares"])
        fee_r = float(trade.get("fee_rate", self._fee_for(sym)))
        net_ps = effective_exit_proceeds(float(sell["avg_price"]), fee_r)
        trade["partial_proceeds_usd"] = round(float(trade["partial_proceeds_usd"]) + sold * net_ps, 4)
        trade["shares_open"] = round(max(0.0, float(trade["shares_open"]) - sold), 4)
        pos.shares = trade["shares_open"]
        if float(trade["shares_open"]) >= 0.5:
            self.ledger.open_trade(trade)
            return
        pnl = float(trade["partial_proceeds_usd"]) - float(trade["cost_usd"])
        info = {"exit_reason": reason, "exit_price": float(sell["avg_price"]), "exit_shares": sold, "pnl_usd": round(pnl, 4),
                "exit_ts": now, "payout_per_share": net_ps}
        self._finish_trade(trade, info)

    # ------------------------------------------------------------------ settlement
    def _begin_settlement(self, sym: Optional[str] = None) -> None:
        targets = [sym] if sym is not None else list(self._positions.keys())
        for target_sym in targets:
            self._positions.pop(target_sym, None)
            trade = self._trades.pop(target_sym, None)
            if trade is not None:
                self.ledger.mark_settling(trade)
                self._spawn(self._settle(trade))

    def _chainlink_price_at(self, sym: str, ts: float) -> Optional[float]:
        buf = self.chainlink.ticks.get(sym) or []
        best, dist = None, 5001.0
        for ts_ms, val in buf:
            d = abs(ts_ms - ts * 1000.0)
            if d < dist:
                best, dist = float(val), d
        return best

    async def _await_resolution(self, trade: dict[str, Any]) -> dict[str, Any]:
        for i in range(70):
            res = await self.auto_trader.query_live_market_resolution(trade["slug"], trade["side"], max_attempts=3)
            if res.get("resolved"):
                return res
            await asyncio.sleep(10.0 if i < 12 else 30.0)
        end_px = self._chainlink_price_at(trade["symbol"], trade["window_ts"] + 300)
        strike = trade.get("strike") or self.chainlink.get_bucket_open_price(trade["symbol"], trade["window_ts"])
        if end_px is None or strike is None:
            return {"resolved": False}
        up_won = end_px >= float(strike)
        won = up_won if trade["side"] == "UP" else (not up_won)
        return {"resolved": True, "polymarket_won": won, "final_price": end_px, "price_to_beat": strike, "provisional": True}

    async def _settle(self, trade: dict[str, Any]) -> None:
        await asyncio.sleep(max(0.0, trade["window_ts"] + 300 - time.time()) + 3.0)
        res = await self._await_resolution(trade)
        if not res.get("resolved"):
            print(f"[Sniper Warning] {trade['id']} unresolved after retries; left OPEN for manual review")
            return
        won = bool(res.get("polymarket_won"))
        payout = 1.0 if won else 0.0
        proceeds = float(trade.get("partial_proceeds_usd", 0.0)) + float(trade["shares_open"]) * payout
        info = {
            "exit_reason": f"SETTLED_{trade['side'] if won else ('DOWN' if trade['side'] == 'UP' else 'UP')}",
            "exit_price": payout, "exit_shares": float(trade["shares_open"]), "payout_per_share": payout,
            "pnl_usd": round(proceeds - float(trade["cost_usd"]), 4), "exit_ts": time.time(),
            "final_price": res.get("final_price"), "strike": res.get("price_to_beat") or trade.get("strike"),
            "provisional": bool(res.get("provisional", False)),
        }
        self._finish_trade(trade, info)
        if won:
            self._spawn(self._redeem_and_sync())

    async def _redeem_and_sync(self) -> None:
        for delay in (5.0, 40.0, 90.0):
            await asyncio.sleep(delay)
            res = await asyncio.wait_for(self.auto_trader.auto_redeem_winning_positions(), timeout=45.0)
            await self.auto_trader.sync_live_wallet_balance(include_portfolio=True)
            if float(res.get("claimed_pusd", 0.0)) > 0.0:
                return

    # ------------------------------------------------------------------ bookkeeping / notify
    def _finish_trade(self, trade: dict[str, Any], info: dict[str, Any]) -> None:
        sym = str(trade.get("symbol") or "")
        if self._trades.get(sym) is trade:
            self._positions.pop(sym, None)
            self._trades.pop(sym, None)
        alerts = self.ledger.close_trade(trade, info)
        closed = self.ledger.state["trades"][0]
        self._spawn(self._notify(exit_message(closed, self._perf_lines()), "SNIPER_EXIT", str(closed["symbol"])))
        if alerts:
            self._spawn(self._notify(alert_message(alerts, self._perf_lines()), "SNIPER_ALERT", "SYSTEM"))
        self._spawn(self.auto_trader.sync_live_wallet_balance(include_portfolio=True))

    async def _notify(self, text: str, event_type: str, symbol: str) -> None:
        await self.notifier.dispatch_signal({
            "symbol": symbol, "event_type": event_type, "direction": "LONG_BOUNCE", "price": 0.0,
            "message": text, "source": "oracle_sniper",
        })

    def _seed_traded_windows(self) -> None:
        """A restart inside a window must not allow a second entry in that same coin+window."""
        st = self.ledger.state
        rows = list(st.get("trades", [])[:50]) + list(st.get("settling", []))
        rows.extend((st.get("open_positions") or {}).values())
        for key in ("open_position", "pending_entry"):
            if st.get(key):
                rows.append(st[key])
        for r in rows:
            if r.get("window_ts"):
                w = int(r["window_ts"])
                self._traded_windows.add(w)
                if r.get("symbol"):
                    self._traded_coin_windows.add((str(r["symbol"]), w))

    def _recover_open_position(self) -> None:
        pending_entry = self.ledger.state.get("pending_entry")
        if pending_entry:
            self._spawn(self._reconcile_pending(dict(pending_entry)))
        for pending in list(self.ledger.state.get("settling", [])):
            self._spawn(self._settle(dict(pending)))
        ops = dict(self.ledger.state.get("open_positions") or {})
        if not ops and self.ledger.state.get("open_position"):
            op0 = self.ledger.state["open_position"]
            ops[str(op0["symbol"])] = op0
        for sym, op in ops.items():
            if sym in self._positions:
                continue
            self._trades[sym] = dict(op)
            self._positions[sym] = OpenPosition(
                op["symbol"], op["side"], int(op["window_ts"]), float(op["shares_open"]), float(op["entry_price"]),
                float(op["cost_per_share"]), float(op["p_true"]), float(op["z"]),
            )
            self._traded_windows.add(int(op["window_ts"]))
            self._traded_coin_windows.add((sym, int(op["window_ts"])))

    # ------------------------------------------------------------------ control / status
    def set_enabled(self, enabled: bool) -> None:
        """Pause or resume new entries (open positions are still managed and settled)."""
        self.ledger.set_enabled(enabled)

    def telegram_text(self) -> str:
        """Compact Telegram status block (state, positions, per-coin live model view, performance)."""
        guard = self.ledger.guard_reason()
        state = "🟢 運行中" if guard is None else f"⏸ 暫停（{guard}）"
        if self._positions:
            pos_txt = "｜".join(f"{p.symbol[:3]} {p.side} {p.shares:.2f}股 @ ${p.entry_price:.3f}" for p in self._positions.values())
        else:
            pos_txt = "空倉（等待 150–255s 狙擊窗口）"
        lines = [f"🎯 新策略｜Chainlink TWAP V2｜{state}", f"• 持倉（{len(self._positions)}/{self.policy.max_concurrent_positions}）：{pos_txt}"]
        for sym in SYMBOLS:
            d = self.diag.get(sym, {})
            if d.get("status") == "OK":
                up, dn = d["up"], d["down"]
                lines.append(
                    f"• {sym[:3]} Z={d['z']:+.2f} P(UP)={d['p_up'] * 100:.0f}% 買價 漲{up[1] or '--'}/跌{dn[1] or '--'}"
                )
            else:
                lines.append(f"• {sym[:3]} {d.get('status', '等待資料')}")
        lines.append(self._perf_lines())
        return "\n".join(lines)

    def status(self) -> dict[str, Any]:
        """Snapshot for APIs, Telegram and patrol reports."""
        now = time.time()
        total, cash, vault = self._equity()
        pos = asdict(self._pos) if self._pos else None
        return {
            "enabled": self.ledger.enabled, "running": self.running,
            "guard_reason": self.ledger.guard_reason(),
            "frozen_parity_ok": self._config_parity_ok(),
            "pending_entry": self.ledger.state.get("pending_entry"),
            "load_alert": self.ledger.state.get("load_alert"),
            "heartbeat_age_sec": round(now - self.heartbeat_ts, 1) if self.heartbeat_ts else None,
            "last_error": self.last_error,
            "equity": {"total_u": round(total, 4), "cash_u": round(cash, 4), "vault_u": vault,
                       "initial_u": self.ledger.state.get("initial_equity_u")},
            "stats": self.ledger.stats(),
            "daily_report": self.ledger.generate_daily_report(),
            "open_position": pos, "open_trade": self._trade,
            "open_positions": {k: asdict(v) for k, v in self._positions.items()},
            "symbol_pause_until_ms": dict(self.ledger.state["pause"]["symbol_until_ms"]),
            "global_pause_until_ms": self.ledger.state["pause"]["global_until_ms"],
            "recent_trades": self.ledger.state["trades"][:10],
            "diag": self.diag, "feeds": self.hub.health(),
            "config": {k: v for k, v in asdict(self.cfg).items() if k in (
                "min_z_score", "min_true_prob", "min_entry_price", "max_entry_price", "min_net_edge",
                "entry_start_elapsed_sec", "entry_end_elapsed_sec", "min_shares", "risk_fraction", "use_twap_projection")},
        }
