"""Live runner for the Chainlink late-window sniper (BTC / ETH / SOL, one position at a time).

Every second: refresh the market hub, build a fair-value view per coin, and either manage the open position
(early take-profit / structural stop) or rank the coins that pass every entry gate and fire a single FAK order.
Settlement is resolved from Polymarket's official Gamma outcome and winnings are redeemed automatically.
"""
from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
import json
import os
import pathlib
import time
from typing import Any, Callable, Optional

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
    evaluate_exit,
    pick_best_candidate,
    taker_fee_per_share,
)
from engine.oracle_sniper_feeds import SYMBOLS, SniperMarketHub, SymbolState
from engine.oracle_sniper_ledger import SniperLedger
from engine.oracle_sniper_notify import alert_message, entry_message, exit_message, status_message
from engine.signal_core import (
    DEFAULT_MAX_AGES,
    MarketInputs,
    SignalDecision,
    choose_best_candidate,
    compute_fv,
    compute_z_twap_proj_diagnostic,
    directional_book_age,
    evaluate as evaluate_signal,
    simulate_fak_buy,
    simulate_fak_sell,
)

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
    max_book_age_sec: float = BOOK_MAX_AGE_SEC
    frozen_config_path: Optional[pathlib.Path] = None
    frozen_model_name: Optional[str] = None
    pilot_fixed_shares: Optional[float] = None
    paper_simulation: bool = False
    paper_fill_delay_sec: float = 1.0
    paper_initial_equity_u: float = 40.0
    enforce_vol_warmup: bool = False
    vol_cache_path: Optional[pathlib.Path] = None
    clock: Optional[Callable[[], float]] = None
    mono_clock_ms: Optional[Callable[[], float]] = None
    live_shadow_log_path: Optional[pathlib.Path] = None


_DEFAULT_FROZEN_PATH = pathlib.Path(__file__).resolve().parent.parent / "edge_validation" / "frozen_config.json"
_DEFAULT_VOL_CACHE_PATH = pathlib.Path(__file__).resolve().parent.parent / "data" / "sniper_vol_hist.json"
_DEFAULT_SHADOW_LOG_PATH = pathlib.Path(__file__).resolve().parent.parent / "data" / "validation_42" / "runner_shadow_events.jsonl"

PRODUCTION_EXEC_POLICY = SniperExecutionPolicy(
    max_concurrent_positions=1,
    per_symbol_fees=PRODUCTION_FEE_RATES,
    require_rtds_connected=True,
    require_twap_fresh=True,
    max_chainlink_age_sec=3.0,
    max_binance_age_sec=2.0,
    max_book_age_sec=BOOK_MAX_AGE_SEC,
    frozen_config_path=_DEFAULT_FROZEN_PATH,
    frozen_model_name="v2_filtered",
    pilot_fixed_shares=5.0,
    enforce_vol_warmup=True,
    vol_cache_path=_DEFAULT_VOL_CACHE_PATH,
)

PAPER_40U_EXEC_POLICY = SniperExecutionPolicy(
    max_concurrent_positions=1,
    per_symbol_fees=PRODUCTION_FEE_RATES,
    require_rtds_connected=True,
    require_twap_fresh=True,
    max_chainlink_age_sec=3.0,
    max_binance_age_sec=2.0,
    max_book_age_sec=BOOK_MAX_AGE_SEC,
    frozen_config_path=_DEFAULT_FROZEN_PATH,
    frozen_model_name="v2_filtered",
    pilot_fixed_shares=5.0,
    paper_simulation=True,
    paper_fill_delay_sec=1.0,
    paper_initial_equity_u=40.0,
    enforce_vol_warmup=True,
    vol_cache_path=_DEFAULT_VOL_CACHE_PATH,
    live_shadow_log_path=_DEFAULT_SHADOW_LOG_PATH,
)

PAPER_43U_EXEC_POLICY = SniperExecutionPolicy(
    max_concurrent_positions=1,
    per_symbol_fees=PRODUCTION_FEE_RATES,
    require_rtds_connected=True,
    require_twap_fresh=True,
    max_chainlink_age_sec=3.0,
    max_binance_age_sec=2.0,
    max_book_age_sec=BOOK_MAX_AGE_SEC,
    frozen_config_path=_DEFAULT_FROZEN_PATH,
    frozen_model_name="v2_filtered",
    pilot_fixed_shares=None,
    paper_simulation=True,
    paper_fill_delay_sec=1.0,
    paper_initial_equity_u=43.0,
    enforce_vol_warmup=True,
)


class OracleSniperRunner:
    """Single-slot Chainlink late-window execution engine driven by shared `signal_core`."""

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
        self._custom_clock = policy.clock
        self._custom_mono_ms = policy.mono_clock_ms
        self.hub = hub or SniperMarketHub(auto_trader, cfg, clock=self._now, mono_clock_ms=policy.mono_clock_ms)
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
        self._last_vol_save_ts = 0.0
        self.heartbeat_ts = 0.0
        self.last_error: Optional[str] = None
        self.diag: dict[str, dict[str, Any]] = {}
        self._decisions: dict[str, SignalDecision] = {}
        self._last_ages: dict[str, dict[str, float]] = {}
        self._last_lags: dict[str, dict[str, Optional[float]]] = {}
        self._warm_logged_windows: set[tuple[int, str]] = set()
        self._load_vol_cache()

    def _now(self) -> float:
        return time.time() if self._custom_clock is None else float(self._custom_clock())

    def _mono_ms(self) -> float:
        if self._custom_mono_ms is not None:
            return float(self._custom_mono_ms())
        if self._custom_clock is not None:
            return float(self._custom_clock()) * 1000.0
        return self.hub.mono_ms()

    def _max_ages(self) -> dict[str, float]:
        cl_lim = float(min(CHAINLINK_MAX_AGE_SEC, self.policy.max_chainlink_age_sec))
        bn_lim = float(self.policy.max_binance_age_sec) if self.policy.max_binance_age_sec is not None else 9999.0
        return {"cl_twap": cl_lim, "cl_spot": cl_lim, "bn": bn_lim, "book": float(self.policy.max_book_age_sec)}

    def _load_vol_cache(self) -> None:
        path = self.policy.vol_cache_path
        if not path or not path.exists():
            return
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return
        now_sec = int(self._now())
        for sym, st in self.hub.states.items():
            entry = raw.get(sym) if isinstance(raw, dict) else None
            if isinstance(entry, dict):
                st.micro.load_persisted_dict(entry, now_sec)

    def _maybe_save_vol_cache(self, now: float) -> None:
        path = self.policy.vol_cache_path
        if not path or (now - self._last_vol_save_ts) < 60.0:
            return
        self._last_vol_save_ts = now
        payload = {sym: st.micro.to_persisted_dict() for sym, st in self.hub.states.items()}
        payload["saved_at"] = now
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        os.replace(tmp, path)

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
            t0 = self._now()
            try:
                await self._tick(t0)
                self.last_error = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - the loop must survive any single-tick failure
                self.last_error = f"{type(exc).__name__}: {exc}"
                print(f"[Sniper Tick Error] {self.last_error}")
            await asyncio.sleep(max(0.05, 1.0 - (self._now() - t0)))

    # ------------------------------------------------------------------ tick
    async def _tick(self, now: float) -> None:
        await self.hub.refresh_windows(now)
        self._maybe_save_vol_cache(now)
        window_ts = self.hub.states[SYMBOLS[0]].window_ts
        elapsed = now - window_ts
        self.heartbeat_ts = now
        self._maybe_refresh_wallet(now)
        total, _cash, _vault = self._equity()
        if self._wallet_fresh(now):
            self.ledger.ensure_baseline(total)
            self.ledger.note_equity(total)
            self.ledger.roll_day_if_needed(total)
        views, decisions = self.evaluate_second(now, window_ts)
        for sym, dec in decisions.items():
            c_sym = self._cfg_for(sym)
            if dec.action == "SKIP" and c_sym.entry_start_elapsed_sec <= elapsed <= c_sym.entry_end_elapsed_sec:
                self.ledger.bump_skip(dec.reason)
        for sym in list(self._positions):
            pos = self._positions.get(sym)
            if pos is None:
                continue
            if pos.window_ts == window_ts:
                await self._manage_position(sym, views, elapsed, now)
            else:
                self._begin_settlement(sym)
        if self._entry_allowed(window_ts, now):
            await self._seek_entry(decisions, window_ts, elapsed, now)

    # ------------------------------------------------------------------ market views
    def _rtds_open(self, sym: str, window_ts: int) -> Optional[float]:
        """Official 60s Chainlink TWAP at the 5m boundary (never locks T-1s before T+8s)."""
        key = (sym, window_ts)
        if key in self._open_cache:
            return self._open_cache[key]
        now_val = self._now()
        boundary_ms = window_ts * 1000.0
        twap_fn = getattr(self.chainlink, "get_settlement_twap_at", None)
        best = twap_fn(sym, window_ts) if callable(twap_fn) else None
        is_on_or_after = best is not None
        if best is None:
            twap_buf = getattr(self.chainlink, "twap_ticks", {}).get(sym) or ()
            src_ticks = twap_buf if twap_buf else (self.chainlink.ticks.get(sym) or ())
            best_rank = (9, 4001.0)
            for ts_ms, val in src_ticks:
                d = ts_ms - boundary_ms
                if 0.0 <= d <= 4000.0 and (0, d) < best_rank:
                    best, best_rank, is_on_or_after = float(val), (0, d), True
                elif -1000.0 <= d < 0.0 and (now_val - window_ts) > 8.0 and (1, abs(d)) < best_rank:
                    best, best_rank = float(val), (1, abs(d))
        if best is None and (now_val - window_ts) > 8.0 and callable(twap_fn):
            try:
                best = twap_fn(sym, window_ts, allow_pre_boundary=True)
            except TypeError:
                best = None
        if is_on_or_after or (now_val - window_ts) > 8.0:
            self._open_cache[key] = best  # final only after >= boundary tick or window_ts + 8s
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

    def _side_book_age_sec(self, st: SymbolState, side: str, now: float, now_m: float) -> float:
        mono = st.up_recv_mono_ms if side == "UP" else st.dn_recv_mono_ms
        if mono > 0.0:
            return max(0.0, (now_m - mono) / 1000.0)
        loc = st.up_local_ts if side == "UP" else st.down_local_ts
        return max(0.0, now - loc) if loc > 0.0 else 9999.0

    def _book_for(self, st: SymbolState, now: float, elapsed: float) -> OrderBookTop:
        if elapsed < 1.5:
            return OrderBookTop()
        b, now_m = st.book, self._mono_ms()
        up_ok = self._side_book_age_sec(st, "UP", now, now_m) <= self.policy.max_book_age_sec
        dn_ok = self._side_book_age_sec(st, "DOWN", now, now_m) <= self.policy.max_book_age_sec
        return OrderBookTop(
            b.up_bid if up_ok else None, b.up_ask if up_ok else None,
            b.down_bid if dn_ok else None, b.down_ask if dn_ok else None,
        )

    @staticmethod
    def _levels_or_top(raw_sorted: list[tuple[float, float]], top_px: Optional[float]) -> list[tuple[float, float]]:
        if raw_sorted:
            return raw_sorted
        return [(float(top_px), 100.0)] if top_px is not None else []

    def _read_chainlink_ages(self, sym: str) -> tuple[Optional[float], Optional[float], float, float, Optional[float], Optional[float]]:
        sp_m = getattr(self.chainlink, "get_latest_price_metrics", None)
        if callable(sp_m):
            cl_spot, sp_ms, sp_lag = sp_m(sym, max_age_sec=9999.0)
            sp_age = round(sp_ms / 1000.0, 3)
        else:
            cl_spot, sp_age = self.chainlink.get_latest_price(sym, max_age_sec=9999.0)
            sp_lag = None
        tw_m = getattr(self.chainlink, "get_latest_twap_metrics", None)
        tw_fn = getattr(self.chainlink, "get_latest_twap", None)
        if callable(tw_m):
            cl_twap, tw_ms, tw_lag = tw_m(sym, max_age_sec=9999.0)
            tw_age = round(tw_ms / 1000.0, 3)
        elif callable(tw_fn):
            cl_twap, tw_age = tw_fn(sym, max_age_sec=9999.0)
            tw_lag = None
        elif self.policy.require_twap_fresh:
            cl_twap, tw_age, tw_lag = None, 9999.0, None
        else:
            cl_twap, tw_age, tw_lag = cl_spot, sp_age, sp_lag
        if self.policy.require_rtds_connected and not bool(getattr(self.chainlink, "connected", True)):
            return cl_spot, cl_twap, 9999.0, 9999.0, sp_lag, tw_lag
        return cl_spot, cl_twap, sp_age, tw_age, sp_lag, tw_lag

    def _assemble_inputs(self, sym: str, st: SymbolState, now: float, window_ts: int) -> tuple[MarketInputs, str]:
        cl_spot, cl_twap, sp_age, tw_age, sp_lag, tw_lag = self._read_chainlink_ages(sym)
        now_m = self._mono_ms()
        if st.bn_recv_mono_ms > 0.0:
            bn_age = max(0.0, (now_m - st.bn_recv_mono_ms) / 1000.0)
        else:
            bn_age = max(0.0, now - st.binance_ts) if (st.binance_price is not None and st.binance_ts > 0.0) else 9999.0
        st.sync_micro_at(now, cl_spot, window_ts, bn_age_sec=bn_age)
        bn_px = st.micro.prices[-1] if st.micro.prices else st.binance_price
        if self.policy.max_binance_age_sec is None and st.binance_price is None and cl_spot is not None:
            bn_px, bn_age = cl_spot, 0.0
        strike, src = self._strike(sym, st)
        up_age = self._side_book_age_sec(st, "UP", now, now_m)
        dn_age = self._side_book_age_sec(st, "DOWN", now, now_m)
        bk_age = directional_book_age(cl_spot, strike, up_age, dn_age)
        bk_lag = st.up_last_lag_ms if (cl_spot is None or strike is None or cl_spot >= strike) else st.dn_last_lag_ms
        self._last_lags[sym] = {
            "cl_twap_ms": tw_lag, "cl_spot_ms": sp_lag,
            "bn_ms": st.bn_last_lag_ms,
            "book_ms": bk_lag,
        }
        cvd, flow = st.micro.get_orderflow_metrics(int(now))
        gap, r240 = st.micro._last_basis_gap_bps, st.micro.get_realized_twap_240(window_ts, now, fallback_spot=cl_spot)
        up_asks, dn_asks = self._levels_or_top(st.sorted_asks("UP"), st.book.up_ask), self._levels_or_top(st.sorted_asks("DOWN"), st.book.down_ask)
        up_bids, dn_bids = self._levels_or_top(st.sorted_bids("UP"), st.book.up_bid), self._levels_or_top(st.sorted_bids("DOWN"), st.book.down_bid)
        ages = {"cl_twap": round(tw_age, 3), "cl_spot": round(sp_age, 3), "bn": round(bn_age, 3), "book": round(bk_age, 3)}
        return MarketInputs(sym, window_ts, now, strike, cl_twap, cl_spot, bn_px, tuple(st.micro.prices), up_asks, dn_asks, up_bids, dn_bids, ages, flow, cvd, gap, r240), src

    def _maybe_log_shadow(self, win_ts: int, now: float, elapsed: float, dec: SignalDecision, inp: MarketInputs) -> None:
        path = self.policy.live_shadow_log_path
        if path is None:
            return
        cfg_sym = self._cfg_for(inp.symbol)
        in_win = cfg_sym.entry_start_elapsed_sec <= elapsed <= cfg_sym.entry_end_elapsed_sec
        events: list[dict[str, Any]] = []
        key = (win_ts, inp.symbol)
        if in_win and dec.reason != "WARMUP" and key not in self._warm_logged_windows:
            self._warm_logged_windows.add(key)
            for old_k in [k for k in self._warm_logged_windows if k[0] < win_ts - 3600]:
                self._warm_logged_windows.discard(old_k)
            events.append({"type": "WARM_WINDOW", "ts_sec": int(now), "window_ts": win_ts, "symbol": inp.symbol, "vol_samples": len(inp.vol_hist)})
        if dec.action == "ENTER":
            events.append({
                "type": "ENTER", "ts_sec": int(now), "window_ts": win_ts, "symbol": inp.symbol, "side": dec.side,
                "z": round(float(dec.z or 0.0), 6), "p_model": round(float(dec.p_model or 0.0), 6),
                "sigma": float(dec.sigma or 0.0), "ask_top": float(dec.ask_top or 0.0),
                "ages_ms": {k: round(float(v) * 1000.0, 2) for k, v in inp.ages.items()},
                "lags_ms": self._last_lags.get(inp.symbol, {}),
            })
        if not events:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            for ev in events:
                fh.write(json.dumps(ev) + "\n")

    def evaluate_second(
        self, now: float, window_ts: Optional[int] = None
    ) -> tuple[dict[str, View], dict[str, SignalDecision]]:
        """Build unified MarketInputs and run pure `signal_core.evaluate()` for all symbols."""
        win_ts = self.hub.roll_window(now) if window_ts is None else window_ts
        elapsed = now - win_ts
        views: dict[str, View] = {}
        decisions: dict[str, SignalDecision] = {}
        lims = self._max_ages()
        for sym, st in self.hub.states.items():
            cfg_sym = self._cfg_for(sym)
            inp, src = self._assemble_inputs(sym, st, now, win_ts)
            dec = evaluate_signal(inp, cfg_sym, max_ages=lims, enforce_warmup=self.policy.enforce_vol_warmup)
            decisions[sym] = dec
            self._last_ages[sym] = dict(inp.ages)
            self._maybe_log_shadow(win_ts, now, elapsed, dec, inp)
            book = self._book_for(st, now, elapsed)
            fv = dec.fv
            if fv is None and sym in self._positions and float(inp.ages.get("cl_spot", 9999.0)) <= float(lims.get("cl_spot", 3.0)):
                res = compute_fv(inp, cfg_sym)
                fv = res[1] if res is not None else None
            if fv is not None:
                views[sym] = (fv, book)
            self.diag[sym] = self._diag_row(dec, book, src, inp.ages, st, len(inp.vol_hist), self._last_lags.get(sym))
        self._decisions = decisions
        return views, decisions

    def _build_views(self, now: float, elapsed: float) -> dict[str, View]:
        views, _decisions = self.evaluate_second(now, self.hub.states[SYMBOLS[0]].window_ts or (int(now) // 300) * 300)
        return views

    @staticmethod
    def _diag_row(
        dec: SignalDecision,
        book: OrderBookTop,
        src: str,
        ages: dict[str, float],
        st: SymbolState,
        vol_n: int,
        lags_ms: Optional[dict[str, Optional[float]]] = None,
    ) -> dict[str, Any]:
        status = "OK" if dec.fv is not None else dec.reason
        row: dict[str, Any] = {
            "status": status, "decision": dec.action, "reason": dec.reason, "strike_src": src,
            "cl_age_sec": ages.get("cl_spot"), "ages": ages,
            "ages_ms": {k: round(float(v) * 1000.0, 2) for k, v in ages.items()},
            "lags_ms": lags_ms or {}, "vol_samples": vol_n,
            "up": [book.up_bid, book.up_ask], "down": [book.down_bid, book.down_ask], "has_tokens": bool(st.up_token),
        }
        if dec.fv is not None:
            row.update({
                "z": round(dec.fv.raw_z_score, 2), "p_up": round(dec.fv.p_true_up, 3),
                "gap_bps": round(dec.fv.oracle_gap_bps, 2), "flow": round(dec.fv.orderflow_score, 2),
                "sigma": dec.sigma,
            })
        if dec.action == "SKIP":
            row["entry_blocked"] = dec.reason
        return row

    # ------------------------------------------------------------------ equity / wallet
    def _equity(self) -> tuple[float, float, float]:
        if self.policy.paper_simulation:
            cash = float(self.ledger.state.get("paper_cash_u", self.policy.paper_initial_equity_u))
            open_cost = sum(float(t.get("cost_usd", 0.0)) for t in self._trades.values())
            open_cost += sum(float(t.get("cost_usd", 0.0)) for t in self.ledger.state.get("settling", []))
            total = cash + open_cost
        else:
            w = self.auto_trader.live_wallet_state or {}
            cash = float(w.get("cash_pusd", 0.0))
            total = cash + float(w.get("portfolio_pusd", 0.0))
        vault, _next, _lbl = compute_vault_locked_u(
            max(float(self.ledger.state.get("peak_equity_u", 0.0)), total), float(self.ledger.state.get("vault_locked_u", 0.0))
        )
        self.ledger.state["vault_locked_u"] = vault
        return total, cash, vault

    def _maybe_refresh_wallet(self, now: float) -> None:
        if self.policy.paper_simulation:
            return
        if now - self._last_wallet_ts >= WALLET_REFRESH_SEC:
            self._last_wallet_ts = now
            self._spawn(self.auto_trader.sync_live_wallet_balance(include_portfolio=True))

    def _wallet_fresh(self, now: float) -> bool:
        if self.policy.paper_simulation:
            return True
        updated_ms = float((self.auto_trader.live_wallet_state or {}).get("updated_at_ms", 0) or 0)
        return updated_ms > 0 and (now * 1000.0 - updated_ms) < 180_000.0

    def _perf_lines(self) -> str:
        total, cash, vault = self._equity()
        return self.ledger.performance_lines(total, cash, vault)

    def _perf_summary(self) -> dict[str, Any]:
        total, cash, vault = self._equity()
        return self.ledger.performance_summary(total, cash, vault)

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
        if self.policy.paper_simulation:
            return True
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
            return False
        return True

    async def _seek_entry(
        self, decisions: dict[str, SignalDecision], window_ts: int, elapsed: float, now: float
    ) -> None:
        eligible = [dec for sym, dec in decisions.items() if self._symbol_can_enter(sym, window_ts) and dec.action == "ENTER"]
        if not eligible:
            return
        if self.policy.max_concurrent_positions == 1:
            best = choose_best_candidate(eligible)
            if best is not None:
                await self._enter(best, window_ts, now, len(eligible))
            return
        remaining = self.policy.max_concurrent_positions - len(self._positions)
        cands = [d.candidate for d in eligible if d.candidate is not None]
        ranked = sorted(cands, key=lambda c: c.net_ev + (0.01 if c.symbol == "BTCUSDT" else 0.0), reverse=True)
        for cand in ranked[:remaining]:
            if self.ledger.guard_reason() is not None:
                break
            await self._enter(cand, window_ts, now, len(eligible))

    def _plan_size(self, cand: Candidate, limit_px: float) -> tuple[float, str]:
        total, cash, vault = self._equity()
        tradable = max(0.0, total - vault)
        if self.policy.pilot_fixed_shares is not None:
            shares = float(self.policy.pilot_fixed_shares)
            if tradable < shares * cand.total_cost_per_share + 0.05:
                return 0.0, "BELOW_MIN_LOT"
        else:
            c_sym = self._cfg_for(cand.symbol)
            if total >= 200.0 and c_sym.risk_fraction > 0.03:
                c_sym = SniperConfig(**{**asdict(c_sym), "risk_fraction": 0.03})
            shares = compute_shares(c_sym, tradable, cand.total_cost_per_share)
            if shares <= 0.0:
                return 0.0, "BELOW_MIN_LOT"
        if (cash - vault) < shares * limit_px * 1.03 + 0.05:
            return 0.0, "INSUFFICIENT_CASH"
        return shares, ""

    async def _paper_buy(self, cand: Candidate, shares: float, limit_px: float) -> dict[str, Any]:
        """Simulate realistic delayed (+1s) depth-aware FAK fill in paper mode (zero real orders)."""
        if self.policy.paper_fill_delay_sec > 0.0:
            await asyncio.sleep(self.policy.paper_fill_delay_sec)
        st = self.hub.states[cand.symbol]
        live_ask = st.book.up_ask if cand.side == "UP" else st.book.down_ask
        asks = self._levels_or_top(st.sorted_asks(cand.side), live_ask)
        now_val = self._now()
        bk_age = self._side_book_age_sec(st, cand.side, now_val, self._mono_ms())
        if not asks:
            return {"filled": False, "shares": 0.0, "filled_partial": 0.0, "usd": 0.0, "avg_price": 0.0, "order_id": "", "status": "PAPER_NO_BOOK", "error": "ask_missing"}
        if 9000.0 > bk_age > self.policy.max_book_age_sec and self.policy.paper_fill_delay_sec >= 0.5:
            return {"filled": False, "shares": 0.0, "filled_partial": 0.0, "usd": 0.0, "avg_price": 0.0, "order_id": "", "status": "PAPER_STALE_BOOK", "error": f"book_age={bk_age:.1f}s>{self.policy.max_book_age_sec}s"}
        res = simulate_fak_buy(asks, shares, limit_px)
        if not res.filled or res.avg_px is None:
            return {"filled": False, "shares": 0.0, "filled_partial": res.filled_shares, "usd": 0.0, "avg_price": res.avg_px or 0.0, "order_id": "", "status": f"PAPER_{res.status}", "error": f"top={asks[0][0]:.3f} filled={res.filled_shares:.2f}/{shares:.2f} lim={limit_px:.2f}"}
        return {"filled": True, "shares": shares, "filled_partial": shares, "usd": round(shares * res.avg_px, 4), "avg_price": float(res.avg_px), "order_id": f"paper-{int(now_val * 1000)}", "status": "PAPER_FILLED_1S", "error": None}

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
        ages = dict(self._last_ages.get(cand.symbol) or {})
        intent = {
            "symbol": cand.symbol, "side": cand.side, "token_id": token, "window_ts": window_ts, "slug": st.slug,
            "condition_id": st.condition_id, "strike": st.price_to_beat, "ts": now, "target_shares": shares,
            "limit_px": limit_px, "est_cost_usd": round(shares * limit_px * 1.02, 4), "n_cands": n_cands,
            "ages": ages, "cand": asdict(cand),
        }
        if not self.policy.paper_simulation:
            self.ledger.set_pending_entry(intent)
        t_send = time.monotonic()
        fill = await (self._paper_buy(cand, shares, limit_px) if self.policy.paper_simulation else self.auto_trader.sniper_buy(token, shares, cand.ask, limit_px))
        lat_ms = round((time.monotonic() - t_send) * 1000.0, 1)
        self.ledger.record_attempt({
            "ts": now, "window_ts": window_ts, "symbol": cand.symbol, "side": cand.side, "sim_ask": cand.ask,
            "limit_px": limit_px, "filled": bool(fill.get("filled")), "actual_price": fill.get("avg_price"),
            "filled_partial": fill.get("filled_partial", 0.0), "ages": ages, "status": fill.get("status"), "latency_ms": lat_ms,
        })
        if fill.get("filled"):
            fill_with_ages = {**fill, "ages": ages}
            self._register_entry(cand, window_ts, now, n_cands, fill_with_ages, token, limit_px)
            if not self.policy.paper_simulation:
                self.ledger.set_pending_entry(None)
            return
        self.ledger.bump_skip(str(fill.get("status") or "NO_FILL") if self.policy.paper_simulation else "NO_FILL")
        side_ts = st.up_local_ts if cand.side == "UP" else st.down_local_ts
        print(
            f"[Sniper] {cand.symbol} {cand.side} not filled: {fill.get('status')} {fill.get('error')} | "
            f"ask={cand.ask:.3f} cap={limit_px:.2f} bid={cand.bid} z={cand.z:.2f} p={cand.p_true:.3f} "
            f"t={cand.elapsed_sec:.0f}s book_age={now - side_ts:.1f}s"
        )
        if not self.policy.paper_simulation and fill.get("status") in UNCERTAIN_FILL_STATUSES:
            self._traded_windows.add(window_ts)
            self._traded_coin_windows.add((cand.symbol, window_ts))
            self._spawn(self._reconcile_pending(intent))
        elif not self.policy.paper_simulation:
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
            self._perf_lines(), paper_mode=bool(self.policy.paper_simulation), summary=self._perf_summary()), "SNIPER_ALERT", "SYSTEM"))

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
        seq = len(self.ledger.state.get("trades", [])) + len(self.ledger.state.get("settling", [])) + len(self._positions)
        ages = fill.get("ages") if isinstance(fill.get("ages"), dict) else (self._last_ages.get(cand.symbol) or {})
        trade = {
            "id": f"{window_ts}-{cand.symbol}-{int(now * 1000)}", "seq": seq, "symbol": cand.symbol, "side": cand.side, "window_ts": window_ts,
            "slug": st.slug, "token_id": token, "condition_id": st.condition_id, "shares": shares,
            "shares_open": shares, "entry_price": price, "sim_ask": cand.ask, "cost_per_share": cost_ps,
            "cost_usd": cost_usd, "fee_rate": fee_r, "entry_ts": now, "entry_elapsed": cand.elapsed_sec,
            "p_true": cand.p_true, "z": cand.z, "net_ev": cand.net_ev, "candidates": n_cands, "limit_px": limit_px,
            "order_id": fill.get("order_id", ""), "strike": st.price_to_beat or self._open_cache.get((cand.symbol, window_ts)), "partial_proceeds_usd": 0.0,
            "fill_status": fill.get("status"), "paper_mode": bool(self.policy.paper_simulation),
            "paper_initial_u": round(float(self.policy.paper_initial_equity_u)),
            "ages": dict(ages),
        }
        if self.policy.paper_simulation:
            self.ledger.paper_debit(cost_usd)
        self._trades[cand.symbol] = trade
        self._traded_windows.add(window_ts)
        self._traded_coin_windows.add((cand.symbol, window_ts))
        self._exit_attempts[cand.symbol] = 0
        self.ledger.open_trade(trade)
        self._spawn(self._notify(entry_message(trade, self._perf_lines(), summary=self._perf_summary()), "SNIPER_ENTRY", cand.symbol))
        if not self.policy.paper_simulation:
            self._spawn(self.auto_trader.sync_live_wallet_balance())

    async def _paper_sell(self, sym: str, pos: OpenPosition, trade: dict[str, Any], min_bid: float) -> dict[str, Any]:
        if self.policy.paper_fill_delay_sec > 0.0:
            await asyncio.sleep(self.policy.paper_fill_delay_sec)
        st = self.hub.states[sym]
        live_bid = st.book.up_bid if pos.side == "UP" else st.book.down_bid
        bids = self._levels_or_top(st.sorted_bids(pos.side), live_bid)
        res = simulate_fak_sell(bids, pos.shares, min_bid)
        if not res.filled or res.avg_px is None:
            return {"filled": False, "shares": 0.0, "avg_price": 0.0, "status": f"PAPER_EXIT_{res.status}", "error": f"bid={live_bid}<{min_bid} filled={res.filled_shares}"}
        return {"filled": True, "shares": pos.shares, "avg_price": float(res.avg_px), "status": "PAPER_SOLD_1S", "error": None}

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
        st = self.hub.states[sym]
        r240 = st.micro.get_realized_twap_240(pos.window_ts, now, fallback_spot=fv.chainlink_price)
        z_tp = compute_z_twap_proj_diagnostic(fv.strike_price, fv.chainlink_price, 300.0 - elapsed, fv.vol_1s, r240, pos.side)
        if z_tp is not None:
            trade["z_twap_proj"] = round(z_tp, 4)
        self._exit_attempts[sym] = self._exit_attempts.get(sym, 0) + 1
        self._next_exit_ts[sym] = now + EXIT_RETRY_SPACING_SEC
        slip = 0.05 if decision.reason == "EMERGENCY_STOP_LOSS" else 0.03
        min_bid = max(0.01, round(decision.bid - slip, 2))
        sell = await (self._paper_sell(sym, pos, trade, min_bid) if self.policy.paper_simulation else self.auto_trader.sniper_sell(trade["token_id"], pos.shares, min_bid))
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
            self._perf_lines(), paper_mode=bool(self.policy.paper_simulation), summary=self._perf_summary()), "SNIPER_ALERT", "SYSTEM"))

    def _apply_sale(self, sym: str, reason: str, sell: dict[str, Any], now: float) -> None:
        pos, trade = self._positions.get(sym), self._trades.get(sym)
        if pos is None or trade is None:
            return
        sold = float(sell["shares"])
        fee_r = float(trade.get("fee_rate", self._fee_for(sym)))
        net_ps = effective_exit_proceeds(float(sell["avg_price"]), fee_r)
        got_usd = round(sold * net_ps, 4)
        if self.policy.paper_simulation:
            self.ledger.paper_credit(got_usd)
        trade["partial_proceeds_usd"] = round(float(trade["partial_proceeds_usd"]) + got_usd, 4)
        trade["shares_open"] = round(max(0.0, float(trade["shares_open"]) - sold), 4)
        pos.shares = trade["shares_open"]
        if float(trade["shares_open"]) >= 0.5:
            self.ledger.open_trade(trade)
            return
        pnl = round(float(trade["partial_proceeds_usd"]) - float(trade["cost_usd"]), 4)
        info: dict[str, Any] = {
            "exit_reason": reason, "exit_price": float(sell["avg_price"]), "exit_shares": sold, "pnl_usd": pnl,
            "exit_ts": now, "payout_per_share": net_ps, "sl_triggered": (reason == "EMERGENCY_STOP_LOSS"),
            "pnl_with_sl_usd": pnl, "z_twap_proj": trade.get("z_twap_proj"),
        }
        self._finish_trade(trade, info)
        if reason == "EMERGENCY_STOP_LOSS" and self.running:
            self._spawn(self._record_hold_shadow(dict(trade)))

    async def _record_hold_shadow(self, trade: dict[str, Any]) -> None:
        wait = max(0.0, float(trade["window_ts"]) + 303.0 - self._now())
        if wait > 0.0:
            await asyncio.sleep(wait)
        res = await self._await_resolution(trade)
        if not res.get("resolved"):
            return
        won = bool(res.get("polymarket_won"))
        hold_pnl = round((float(trade["shares"]) if won else 0.0) - float(trade["cost_usd"]), 4)
        for t in self.ledger.state.get("trades", []):
            if t.get("id") == trade.get("id"):
                t["pnl_hold_to_settle_usd"] = hold_pnl
                t["sl_was_wrong"] = won
                self.ledger.save()
                break

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
        rem_proceeds = round(float(trade["shares_open"]) * payout, 4)
        if self.policy.paper_simulation and rem_proceeds > 0.0:
            self.ledger.paper_credit(rem_proceeds)
        proceeds = float(trade.get("partial_proceeds_usd", 0.0)) + rem_proceeds
        pnl_val = round(proceeds - float(trade["cost_usd"]), 4)
        info = {
            "exit_reason": f"SETTLED_{trade['side'] if won else ('DOWN' if trade['side'] == 'UP' else 'UP')}",
            "exit_price": payout, "exit_shares": float(trade["shares_open"]), "payout_per_share": payout,
            "pnl_usd": pnl_val, "exit_ts": self._now(),
            "final_price": res.get("final_price"), "strike": res.get("price_to_beat") or trade.get("strike"),
            "provisional": bool(res.get("provisional", False)),
            "sl_triggered": False, "sl_was_wrong": False,
            "pnl_with_sl_usd": pnl_val, "pnl_hold_to_settle_usd": pnl_val,
        }
        self._finish_trade(trade, info)
        if won and not self.policy.paper_simulation:
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
        self._spawn(self._notify(exit_message(closed, self._perf_lines(), summary=self._perf_summary()), "SNIPER_EXIT", str(closed["symbol"])))
        if alerts:
            self._spawn(self._notify(alert_message(alerts, self._perf_lines(), paper_mode=bool(self.policy.paper_simulation), summary=self._perf_summary()), "SNIPER_ALERT", "SYSTEM"))
        if not self.policy.paper_simulation:
            self._spawn(self.auto_trader.sync_live_wallet_balance(include_portfolio=True))

    async def _notify(self, text: str, event_type: str, symbol: str) -> None:
        await self.notifier.dispatch_signal({
            "symbol": symbol, "event_type": event_type, "direction": "LONG_BOUNCE", "price": 0.0,
            "message": text, "parse_mode": "HTML", "source": "oracle_sniper",
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
        """Compact tactical status block (state, positions, per-coin live model view, performance)."""
        guard = self.ledger.guard_reason()
        state = "🟢 運行中" if guard is None else f"⏸ 暫停（{guard}）"
        if self._positions:
            pos_txt = " | ".join(f"{p.symbol[:3]} {p.side} {p.shares:.2f} sh @ ${p.entry_price:.3f}" for p in self._positions.values())
        else:
            pos_txt = "空倉（等待 150–255s 狙擊窗口）"
        init_u = int(round(float(self.policy.paper_initial_equity_u)))
        mode_hdr = f"PAPER ${init_u}U" if self.policy.paper_simulation else "LIVE 實盤"
        coin_lines: list[str] = []
        for sym in SYMBOLS:
            d = self.diag.get(sym, {})
            if d.get("status") == "OK":
                up, dn = d["up"], d["down"]
                coin_lines.append(f"{sym[:3]} | Z {d['z']:+.2f} | P(UP) {d['p_up'] * 100:.0f}% | 漲 {up[1] or '--'} / 跌 {dn[1] or '--'}")
            else:
                coin_lines.append(f"{sym[:3]} | {d.get('status', '等待資料')} ({d.get('vol_samples', 0)}/1800s)")
        return status_message(mode_hdr, state, pos_txt, coin_lines, self._perf_lines())

    def status(self) -> dict[str, Any]:
        """Snapshot for APIs, Telegram and patrol reports."""
        now = self._now()
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
