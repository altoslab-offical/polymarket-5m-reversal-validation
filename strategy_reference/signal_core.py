"""Pure, shared signal evaluation, Binance-1s volatility/microstructure, and depth-aware FAK core."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import math
from typing import Any, Deque, NamedTuple, Optional, Sequence

from engine.oracle_sniper import (
    Candidate,
    FairValueEstimate,
    SniperConfig,
    analyze_trade_ev,
    estimate_fair_value,
    estimate_twap_fair_value,
)

MIN_VOL_SAMPLES: int = 1800
MAX_GAP_FILL_SEC: int = 5
DEFAULT_MAX_AGES: dict[str, float] = {
    "cl_twap": 3.0,
    "cl_spot": 3.0,
    "bn": 2.0,
    "book": 3.5,
}
SYMBOL_TIE_ORDER: dict[str, int] = {"BTCUSDT": 0, "ETHUSDT": 1, "SOLUSDT": 2}


@dataclass(frozen=True)
class MarketInputs:
    """Single-second market snapshot shared identically by Recorder and Runner."""

    symbol: str
    window_ts: int
    now_ts: float
    strike_twap: Optional[float]
    cl_twap: Optional[float]
    cl_spot: Optional[float]
    bn_close_1s: Optional[float]
    vol_hist: Sequence[float]
    up_asks: Sequence[tuple[float, float]]
    down_asks: Sequence[tuple[float, float]]
    up_bids: Sequence[tuple[float, float]]
    down_bids: Sequence[tuple[float, float]]
    ages: dict[str, float]
    orderflow_score: float = 0.0
    cvd_imbalance: float = 0.0
    oracle_gap_bps: float = 0.0
    realized_twap_240: tuple[Optional[float], float] = (None, 0.0)


@dataclass(frozen=True)
class SignalDecision:
    """Deterministic outcome of evaluating one coin at one second."""

    action: str  # "ENTER" | "SKIP"
    reason: str
    side: Optional[str]
    z: Optional[float]
    p_model: Optional[float]
    sigma: Optional[float]
    ask_top: Optional[float]
    bid_top: Optional[float] = None
    net_ev: Optional[float] = None
    fv: Optional[FairValueEstimate] = None
    candidate: Optional[Candidate] = None


class FakFillResult(NamedTuple):
    """Multi-level FAK order simulation outcome."""

    filled: bool
    avg_px: Optional[float]
    status: str  # "FILLED" | "MISSED_PRICE" | "MISSED_DEPTH" | "NO_BOOK" | "SHARES_ZERO"
    filled_shares: float


def compute_sigma_from_vol_hist(vol_hist: Sequence[float], lookback_sec: int, floor: float) -> float:
    """Realised 1s volatility from contiguous Binance 1s closes (max of sample & EWMA variance)."""
    n_keep = max(2, lookback_sec + 1)
    pts = vol_hist[-n_keep:] if len(vol_hist) > n_keep else vol_hist
    rets = [
        math.log(pts[i] / pts[i - 1])
        for i in range(1, len(pts))
        if pts[i - 1] > 0.0 and pts[i] > 0.0
    ]
    if len(rets) < 8:
        return max(floor, 0.000055)
    mean_r = sum(rets) / len(rets)
    sample_var = sum((r - mean_r) ** 2 for r in rets) / max(1, len(rets) - 1)
    ewma_var = rets[0] ** 2
    for r in rets[1:]:
        ewma_var = 0.12 * (r * r) + 0.88 * ewma_var
    return max(floor, math.sqrt(max(sample_var, ewma_var)))


def directional_book_age(
    cl_spot: Optional[float], strike_twap: Optional[float], up_age: float, down_age: float
) -> float:
    """Age of the implied target outcome side's book (falls back to max when price direction unknown)."""
    if cl_spot is not None and strike_twap is not None and strike_twap > 0.0:
        return up_age if cl_spot >= strike_twap else down_age
    return max(up_age, down_age)


def _freshness_skip(ages: dict[str, float], lims: dict[str, float]) -> Optional[str]:
    checks = (
        ("cl_twap", "STALE_CL_TWAP"),
        ("cl_spot", "STALE_CL_SPOT"),
        ("bn", "STALE_BN"),
        ("book", "STALE_BOOK"),
    )
    for key, code in checks:
        if float(ages.get(key, 9999.0)) > float(lims.get(key, 9999.0)):
            return code
    return None


def _side_gate_failure(
    cfg: SniperConfig, side: str, fv: FairValueEstimate, p_dir: float, ask_top: Optional[float], bid_top: Optional[float]
) -> tuple[Optional[str], Optional[float]]:
    sign = 1.0 if side == "UP" else -1.0
    if cfg.use_flow_gates:
        if sign * fv.oracle_gap_bps < -cfg.max_adverse_oracle_gap_bps:
            return "FLOW_BLOCKED", None
        if sign * fv.orderflow_score < cfg.min_orderflow_score or sign * fv.cvd_imbalance < cfg.min_directional_cvd:
            return "FLOW_BLOCKED", None
    if p_dir < cfg.min_true_prob:
        return "PROB_LOW", None
    if ask_top is None:
        return "NO_BOOK", None
    if ask_top < cfg.min_entry_price or ask_top > cfg.max_entry_price:
        return "ASK_OUT_OF_BAND", None
    if bid_top is not None and (ask_top < bid_top or (ask_top - bid_top) > cfg.max_entry_spread):
        return "SPREAD_WIDE", None
    if (p_dir - ask_top) > cfg.max_model_market_divergence:
        return "DIVERGENCE_HIGH", None
    ev = analyze_trade_ev(p_dir, ask_top, cfg.fee_rate)
    if ev.net_ev_per_share < cfg.min_net_edge:
        return "EV_LOW", ev.net_ev_per_share
    return None, ev.net_ev_per_share


def compute_fv(inp: MarketInputs, cfg: SniperConfig) -> Optional[tuple[float, FairValueEstimate]]:
    """Compute canonical sigma and fair-value estimate when strike and spot exist."""
    elapsed = inp.now_ts - float(inp.window_ts)
    rem = 300.0 - elapsed
    if inp.strike_twap is None or inp.strike_twap <= 0.0 or inp.cl_spot is None or inp.cl_spot <= 0.0 or rem <= 0.0:
        return None
    sigma = compute_sigma_from_vol_hist(inp.vol_hist, cfg.vol_lookback_sec, cfg.min_1s_vol_floor)
    micro = (inp.orderflow_score, inp.oracle_gap_bps, inp.cvd_imbalance)
    fv = (
        estimate_twap_fair_value(inp.strike_twap, inp.cl_spot, rem, sigma, inp.realized_twap_240, micro, cfg)
        if cfg.use_twap_projection
        else estimate_fair_value(
            inp.strike_twap, inp.cl_spot, rem, sigma, inp.orderflow_score, inp.oracle_gap_bps, cfg.student_t_df, inp.cvd_imbalance
        )
    )
    return (sigma, fv) if fv is not None else None


def evaluate(
    inp: MarketInputs,
    cfg: SniperConfig,
    max_ages: Optional[dict[str, float]] = None,
    enforce_warmup: bool = True,
) -> SignalDecision:
    """Single canonical entry evaluation used identically by Recorder and Runner."""
    if enforce_warmup and len(inp.vol_hist) < MIN_VOL_SAMPLES:
        return SignalDecision("SKIP", "WARMUP", None, None, None, None, None)
    stale_code = _freshness_skip(inp.ages, max_ages or DEFAULT_MAX_AGES)
    if stale_code is not None:
        return SignalDecision("SKIP", stale_code, None, None, None, None, None)
    if inp.strike_twap is None or inp.strike_twap <= 0.0:
        return SignalDecision("SKIP", "NO_STRIKE", None, None, None, None, None)
    if inp.cl_spot is None or inp.cl_spot <= 0.0 or inp.bn_close_1s is None or inp.bn_close_1s <= 0.0:
        return SignalDecision("SKIP", "NO_PRICE", None, None, None, None, None)
    elapsed = inp.now_ts - float(inp.window_ts)
    res = compute_fv(inp, cfg)
    if res is None:
        return SignalDecision("SKIP", "NO_PRICE", None, None, None, None, None)
    sigma, fv = res
    if elapsed < cfg.entry_start_elapsed_sec or elapsed > cfg.entry_end_elapsed_sec:
        return SignalDecision("SKIP", "OUTSIDE_ENTRY_WINDOW", None, abs(fv.raw_z_score), max(fv.p_true_up, fv.p_true_down), sigma, None, fv=fv)
    if abs(fv.raw_z_score) < cfg.min_z_score:
        return SignalDecision("SKIP", "Z_LOW", None, abs(fv.raw_z_score), max(fv.p_true_up, fv.p_true_down), sigma, None, fv=fv)
    side = "UP" if fv.raw_z_score >= cfg.min_z_score else "DOWN"
    z_dir = abs(fv.raw_z_score)
    p_dir = fv.p_true_up if side == "UP" else fv.p_true_down
    asks = inp.up_asks if side == "UP" else inp.down_asks
    bids = inp.up_bids if side == "UP" else inp.down_bids
    ask_top = float(asks[0][0]) if asks else None
    bid_top = float(bids[0][0]) if bids else None
    err, net_ev = _side_gate_failure(cfg, side, fv, p_dir, ask_top, bid_top)
    if err is not None:
        return SignalDecision("SKIP", err, side, z_dir, p_dir, sigma, ask_top, bid_top, net_ev, fv=fv)
    assert ask_top is not None
    ev = analyze_trade_ev(p_dir, ask_top, cfg.fee_rate)
    cand = Candidate(
        inp.symbol, side, ask_top, bid_top, p_dir, z_dir, ev.net_ev_per_share,
        ev.fee_per_share, ev.total_cost_per_share, elapsed,
    )
    return SignalDecision("ENTER", "OK", side, z_dir, p_dir, sigma, ask_top, bid_top, ev.net_ev_per_share, fv, cand)


def _sweep_fak_levels(levels: Sequence[tuple[float, float]], shares: float, limit_px: float, is_buy: bool) -> FakFillResult:
    if shares <= 0.0:
        return FakFillResult(False, None, "SHARES_ZERO", 0.0)
    if not levels:
        return FakFillResult(False, None, "NO_BOOK", 0.0)
    need = float(shares)
    value = 0.0
    for px, sz in levels:
        if (is_buy and px > limit_px + 1e-12) or (not is_buy and px < limit_px - 1e-12):
            break
        if sz <= 0.0:
            continue
        take = min(need, float(sz))
        value += take * float(px)
        need -= take
        if need <= 1e-9:
            return FakFillResult(True, round(value / float(shares), 6), "FILLED", float(shares))
    filled = round(float(shares) - need, 6)
    if filled <= 1e-9:
        return FakFillResult(False, None, "MISSED_PRICE", 0.0)
    return FakFillResult(False, round(value / filled, 6), "MISSED_DEPTH", filled)


def simulate_fak_buy(asks: Sequence[tuple[float, float]], shares: float, limit_px: float) -> FakFillResult:
    """Depth-aware FAK buy simulation across ascending `(price, size)` ask levels."""
    return _sweep_fak_levels(asks, shares, limit_px, is_buy=True)


def simulate_fak_sell(bids: Sequence[tuple[float, float]], shares: float, min_bid: float) -> FakFillResult:
    """Depth-aware FAK sell simulation across descending `(price, size)` bid levels."""
    return _sweep_fak_levels(bids, shares, min_bid, is_buy=False)


def compute_z_twap_proj_diagnostic(
    strike: float,
    cl_spot: float,
    remaining_sec: float,
    sigma_1s: float,
    realized_240: tuple[Optional[float], float],
    side: str,
) -> Optional[float]:
    """Diagnostic Section-6 TWAP-projected directional Z during the final 60s window (not a live threshold)."""
    if strike <= 0.0 or cl_spot <= 0.0 or remaining_sec > 60.0:
        return None
    a = max(0.0, min(59.5, float(realized_240[1])))
    r = max(0.5, float(remaining_sec))
    mean_src = float(realized_240[0]) if (realized_240[0] and realized_240[0] > 0.0) else cl_spot
    mean_end = (a * mean_src + r * cl_spot) / max(1e-6, a + r)
    sigma_px = max(1e-7, float(sigma_1s)) * strike
    sd_end = max(1e-9, (r / 60.0) * sigma_px * math.sqrt(r / 3.0))
    sign = 1.0 if side == "UP" else -1.0
    return sign * (mean_end - strike) / sd_end


def choose_best_candidate(decisions: Sequence[SignalDecision], priority_symbol: str = "BTCUSDT") -> Optional[Candidate]:
    """Single-slot selection matching Section 4.2 (`net_ev + 1c` BTC bonus, deterministic coin tie-break)."""
    cands = [d.candidate for d in decisions if d.action == "ENTER" and d.candidate is not None]
    if not cands:
        return None
    return max(
        cands,
        key=lambda c: (c.net_ev + (0.01 if c.symbol == priority_symbol else 0.0), -SYMBOL_TIE_ORDER.get(c.symbol, 99)),
    )


class Bn1sMicroTracker:
    """Deterministic 1Hz Binance volatility, taker CVD, EWMA oracle basis, and T+240s TWAP accumulator."""

    def __init__(self, vol_lookback_sec: int = MIN_VOL_SAMPLES, cvd_window_sec: int = 20) -> None:
        self.vol_lookback_sec = vol_lookback_sec
        self.cvd_window_sec = cvd_window_sec
        self.prices: Deque[float] = deque(maxlen=max(MIN_VOL_SAMPLES, vol_lookback_sec) + 5)
        self.last_sec: int = 0
        self.last_real_bn_sec: int = 0
        self._trades_1s: Deque[tuple[int, float, float]] = deque()
        self._ewma_basis_bps: Optional[float] = None
        self._last_basis_sec: int = 0
        self._last_basis_gap_bps: float = 0.0
        self._twap_240: dict[int, list[tuple[int, float]]] = {}

    def ingest_bn_1s(
        self,
        ts_sec: int,
        bn_close: Optional[float],
        buy_usd: float = 0.0,
        sell_usd: float = 0.0,
        real_trade: bool = True,
        feed_alive: bool = False,
    ) -> None:
        """Record 1Hz Binance close; keep history while trade or quote heartbeat is alive (<=5s gap)."""
        alive = bool(real_trade or feed_alive)
        if real_trade and (buy_usd > 0.0 or sell_usd > 0.0):
            if self._trades_1s and self._trades_1s[-1][0] == ts_sec:
                self._trades_1s[-1] = (ts_sec, buy_usd, sell_usd)
            elif not self._trades_1s or ts_sec > self._trades_1s[-1][0]:
                self._trades_1s.append((ts_sec, buy_usd, sell_usd))
            self._prune_trades(ts_sec)
        if not alive and self.last_real_bn_sec > 0 and (ts_sec - self.last_real_bn_sec) > MAX_GAP_FILL_SEC:
            bn_close = None
        if bn_close is None or bn_close <= 0.0:
            if self.last_sec > 0 and (ts_sec - self.last_real_bn_sec) > MAX_GAP_FILL_SEC:
                self.prices.clear()
                self.last_sec = 0
            return
        if self.last_sec > 0 and ts_sec > self.last_sec:
            missing = ts_sec - self.last_sec - 1
            real_gap = (ts_sec - self.last_real_bn_sec - 1) if (alive and self.last_real_bn_sec > 0) else 0
            if missing > MAX_GAP_FILL_SEC or real_gap > MAX_GAP_FILL_SEC:
                self.prices.clear()
            elif missing > 0 and self.prices:
                last_p = self.prices[-1]
                for _ in range(missing):
                    self.prices.append(last_p)
        if self.last_sec == ts_sec and self.prices:
            self.prices[-1] = float(bn_close)
        elif ts_sec >= self.last_sec:
            self.prices.append(float(bn_close))
        self.last_sec = max(self.last_sec, ts_sec)
        if alive:
            self.last_real_bn_sec = max(self.last_real_bn_sec, ts_sec)

    def _prune_trades(self, now_sec: int) -> None:
        cutoff = now_sec - self.cvd_window_sec
        while self._trades_1s and self._trades_1s[0][0] < cutoff:
            self._trades_1s.popleft()

    def get_orderflow_metrics(self, now_sec: int) -> tuple[float, float]:
        """Return `(cvd_imbalance, orderflow_score)` computed strictly from Binance 1s taker flow."""
        self._prune_trades(now_sec)
        buy = sum(b for _, b, _ in self._trades_1s)
        sell = sum(s for _, _, s in self._trades_1s)
        tot = buy + sell
        if tot <= 1000.0:
            return 0.0, 0.0
        cvd = (buy - sell) / tot
        return cvd, max(-1.0, min(1.0, 0.70 * cvd))

    def update_basis_1s(self, ts_sec: int, cl_spot: Optional[float], bn_close: Optional[float]) -> float:
        """Compute 1Hz de-meaned Binance-minus-Chainlink basis in bps (identical across Recorder & Runner)."""
        if ts_sec <= self._last_basis_sec:
            return self._last_basis_gap_bps
        if not cl_spot or not bn_close or cl_spot <= 0.0 or bn_close <= 0.0:
            return self._last_basis_gap_bps
        raw_bps = ((float(bn_close) - float(cl_spot)) / float(cl_spot)) * 10000.0
        if self._ewma_basis_bps is None:
            self._ewma_basis_bps = raw_bps
            gap = 0.0
        else:
            self._ewma_basis_bps = 0.015 * raw_bps + 0.985 * self._ewma_basis_bps
            gap = raw_bps - self._ewma_basis_bps
        self._last_basis_sec = ts_sec
        self._last_basis_gap_bps = gap
        return gap

    def record_cl_240(self, window_ts: int, ts_sec: int, cl_spot: Optional[float]) -> None:
        """Accumulate 1Hz Chainlink spot observations during `[window_ts + 240, window_ts + 299]`."""
        if cl_spot is None or cl_spot <= 0.0 or ts_sec < window_ts + 240:
            return
        pts = self._twap_240.setdefault(window_ts, [])
        if pts and pts[-1][0] == ts_sec:
            pts[-1] = (ts_sec, float(cl_spot))
        elif not pts or ts_sec > pts[-1][0]:
            pts.append((ts_sec, float(cl_spot)))
        for old_w in [w for w in self._twap_240 if w < window_ts - 600]:
            self._twap_240.pop(old_w, None)

    def get_realized_twap_240(
        self, window_ts: int, ts_sec: float, fallback_spot: Optional[float] = None
    ) -> tuple[Optional[float], float]:
        """Return `(realized_mean_since_240, elapsed_since_240)` aligned to float remaining_sec."""
        elapsed = float(ts_sec) - float(window_ts)
        if elapsed < 240.0:
            return None, 0.0
        pts = self._twap_240.get(window_ts) or []
        mean_val = (sum(v for _, v in pts) / len(pts)) if pts else fallback_spot
        return mean_val, max(0.0, elapsed - 240.0)

    def to_persisted_dict(self) -> dict[str, Any]:
        """Serialize contiguous Binance 1s history for warm restart continuity."""
        return {
            "last_sec": self.last_sec,
            "last_real_bn_sec": self.last_real_bn_sec,
            "prices": [round(p, 8) for p in self.prices],
        }

    def load_persisted_dict(self, data: dict[str, Any], now_sec: int) -> bool:
        """Restore history ONLY when contiguous (`0 <= now_sec - last_sec <= 5s`), forward-filling short gaps."""
        saved_last = int(data.get("last_sec") or 0)
        saved_real = int(data.get("last_real_bn_sec") or saved_last)
        raw = [float(x) for x in (data.get("prices") or []) if float(x) > 0.0]
        gap = now_sec - saved_last
        if not raw or saved_last <= 0 or gap < 0 or gap > MAX_GAP_FILL_SEC or (now_sec - saved_real) > MAX_GAP_FILL_SEC:
            self.prices.clear()
            self.last_sec = 0
            self.last_real_bn_sec = 0
            return False
        self.prices.clear()
        self.prices.extend(raw)
        if gap > 1:
            for _ in range(gap - 1):
                self.prices.append(self.prices[-1])
        self.last_sec = now_sec - 1 if gap >= 1 else saved_last
        self.last_real_bn_sec = saved_real
        return True
