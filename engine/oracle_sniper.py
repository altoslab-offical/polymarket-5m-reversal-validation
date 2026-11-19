"""Chainlink-anchored late-window sniper: fee math, fat-tail pricing model and entry/exit rules.

Ported from the user's repo `altoslab-offical/polymarket-btc-5m` (src/fees.py, src/pricing_model.py,
src/strategy.py) and generalised to BTC/ETH/SOL with a single-slot candidate ranker.  Everything here is
pure (no I/O) so the exact same code path is used by the live runner and by the historical replay test.
"""
from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass
import json
import math
import pathlib
from typing import Any, Deque, Optional

# --------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class SniperConfig:
    """Strategy thresholds (identical to the validated repo defaults) plus chip/cost-control sizing."""

    entry_start_elapsed_sec: float = 150.0
    entry_end_elapsed_sec: float = 255.0
    min_z_score: float = 1.30
    min_true_prob: float = 0.80
    min_entry_price: float = 0.68
    max_entry_price: float = 0.82
    max_entry_spread: float = 0.06
    min_net_edge: float = 0.035
    max_model_market_divergence: float = 0.15
    student_t_df: float = 4.5
    vol_lookback_sec: int = 180
    min_1s_vol_floor: float = 0.000050
    cvd_window_sec: int = 20
    min_orderflow_score: float = 0.0
    min_directional_cvd: float = -0.05
    max_adverse_oracle_gap_bps: float = 6.0
    enable_emergency_exit: bool = True
    stop_loss_z_score: float = -0.35
    take_profit_bid: float = 0.98
    fee_rate: float = 0.07
    # --- chip control (position sizing) ---
    min_shares: float = 5.0
    risk_fraction: float = 0.05
    # --- cost control (execution) ---
    slippage_ladder: tuple[float, ...] = (0.04, 0.03, 0.02)
    base_slippage: float = 0.01
    # --- TWAP v2 model extensions (opt-in; defaults preserve V1 75/75 parity) ---
    use_twap_projection: bool = False
    twap_gaussian_weight: float = 0.70
    twap_student_t_df: float = 6.0
    use_flow_gates: bool = True
    early_tp_enabled: bool = True


DEFAULT_SNIPER_CONFIG = SniperConfig()

TWAP_V2_FILTERED_CONFIG = SniperConfig(
    student_t_df=6.0,
    vol_lookback_sec=1800,
    min_1s_vol_floor=0.000035,
    max_model_market_divergence=0.25,
    use_twap_projection=True,
    twap_gaussian_weight=0.70,
    twap_student_t_df=6.0,
    use_flow_gates=True,
    early_tp_enabled=False,
)

TWAP_V2_PURE_CONFIG = SniperConfig(
    student_t_df=6.0,
    vol_lookback_sec=1800,
    min_1s_vol_floor=0.000035,
    max_model_market_divergence=0.30,
    use_twap_projection=True,
    twap_gaussian_weight=1.0,
    twap_student_t_df=6.0,
    use_flow_gates=False,
    early_tp_enabled=False,
)

# Shadow model for Section 4.2 high-band exploration ([0.84, 0.95]); NEVER included in the Bonferroni gate.
TWAP_V2_HIGHBAND_SHADOW_CONFIG = SniperConfig(
    min_entry_price=0.84,
    max_entry_price=0.95,
    student_t_df=6.0,
    vol_lookback_sec=1800,
    min_1s_vol_floor=0.000035,
    max_model_market_divergence=0.30,
    use_twap_projection=True,
    twap_gaussian_weight=1.0,
    twap_student_t_df=6.0,
    use_flow_gates=False,
    early_tp_enabled=False,
)

# --------------------------------------------------------------------------------------
# Fee / EV math
# --------------------------------------------------------------------------------------


def taker_fee_per_share(price: float, fee_rate: float = 0.07) -> float:
    """Polymarket crypto taker fee per share: `rate * p * (1 - p)` rounded to 5 decimals."""
    if price <= 0.0 or price >= 1.0:
        return 0.0
    rounded = round(fee_rate * price * (1.0 - price), 5)
    return rounded if rounded >= 0.00001 else 0.0


def effective_exit_proceeds(bid_price: float, fee_rate: float = 0.07) -> float:
    """Net pUSD per share when selling at the bid as a taker."""
    if bid_price <= 0.0:
        return 0.0
    return max(0.0, bid_price - taker_fee_per_share(bid_price, fee_rate))


@dataclass(frozen=True)
class EVAnalysis:
    """Per-share expected value for buying a binary share as a taker."""

    entry_price: float
    fee_per_share: float
    total_cost_per_share: float
    p_true: float
    net_ev_per_share: float


def analyze_trade_ev(p_true: float, entry_price: float, fee_rate: float = 0.07) -> EVAnalysis:
    """Net EV = P_true - (ask + dynamic taker fee) for a $1 / $0 payout share."""
    fee = taker_fee_per_share(entry_price, fee_rate)
    cost = entry_price + fee
    return EVAnalysis(entry_price, fee, cost, p_true, p_true - cost)


# --------------------------------------------------------------------------------------
# Fat-tail pricing model
# --------------------------------------------------------------------------------------


def normal_cdf(x: float) -> float:
    """Standard normal CDF."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def student_t_cdf(x: float, df: float = 4.5, steps: int = 32) -> float:
    """Student-t CDF via angular substitution and Simpson integration (no scipy)."""
    if x == 0.0:
        return 0.5
    if df > 200.0:
        return normal_cdf(x)
    upper = math.atan(abs(x) / math.sqrt(df))
    n = steps if steps % 2 == 0 else steps + 1
    h = upper / n
    total = 1.0 + (math.cos(upper) ** (df - 1.0))
    for i in range(1, n):
        weight = 4.0 if (i % 2 == 1) else 2.0
        total += weight * (math.cos(i * h) ** (df - 1.0))
    integral = (h / 3.0) * total
    log_norm = math.lgamma((df + 1.0) / 2.0) - 0.5 * math.log(math.pi) - math.lgamma(df / 2.0)
    half_prob = min(0.5, math.exp(log_norm) * integral)
    return 0.5 + half_prob if x > 0 else 0.5 - half_prob


def fat_tail_prob_from_z(z_score: float, df: float = 4.5) -> float:
    """Conservative blend of Gaussian and raw Student-t probabilities (validated in the source repo)."""
    return 0.35 * normal_cdf(z_score) + 0.65 * student_t_cdf(z_score, df=df)


@dataclass
class FairValueEstimate:
    """Model output for one symbol at one instant."""

    strike_price: float
    chainlink_price: float
    cex_lead_price: Optional[float]
    oracle_gap_bps: float
    remaining_sec: float
    vol_1s: float
    horizon_vol_pct: float
    raw_z_score: float
    adjusted_z_score: float
    orderflow_score: float
    p_true_up: float
    p_true_down: float
    cvd_imbalance: float = 0.0


class RollingMicrostructureTracker:
    """Rolling 1s prices (realised vol), CEX CVD / OBI and the USDT-vs-Chainlink basis."""

    def __init__(self, vol_lookback_sec: int = 180, cvd_window_sec: int = 20) -> None:
        self.vol_lookback_sec = vol_lookback_sec
        self.cvd_window_sec = cvd_window_sec
        self._sec_prices: Deque[tuple[int, float]] = deque(maxlen=vol_lookback_sec + 5)
        self._trades: Deque[tuple[float, float, int]] = deque()
        self._ewma_obi: float = 0.0
        self._obi_initialized: bool = False
        self._ewma_basis_bps: Optional[float] = None

    def update_oracle_basis(self, chainlink_price: Optional[float], cex_price: Optional[float]) -> float:
        """De-meaned CEX-minus-Chainlink gap in bps (slow EWMA removes the structural USDT basis)."""
        if not chainlink_price or not cex_price or chainlink_price <= 0:
            return 0.0
        raw_bps = ((cex_price - chainlink_price) / chainlink_price) * 10000.0
        if self._ewma_basis_bps is None:
            self._ewma_basis_bps = raw_bps
            return 0.0
        alpha = 0.015
        self._ewma_basis_bps = alpha * raw_bps + (1.0 - alpha) * self._ewma_basis_bps
        return raw_bps - self._ewma_basis_bps

    def on_price(self, ts: float, price: float) -> None:
        """Record the latest price of the current second."""
        if price <= 0:
            return
        sec_bucket = int(ts)
        if self._sec_prices and self._sec_prices[-1][0] == sec_bucket:
            self._sec_prices[-1] = (sec_bucket, price)
        elif not self._sec_prices or sec_bucket > self._sec_prices[-1][0]:
            self._sec_prices.append((sec_bucket, price))

    def on_trade(self, ts: float, price: float, qty: float, is_buyer_maker: bool) -> None:
        """Record an aggressive CEX trade (`is_buyer_maker=True` means the seller crossed the spread)."""
        sign = -1 if is_buyer_maker else 1
        self._trades.append((ts, price * qty, sign))
        self._prune_trades(ts)

    def on_book_ticker(self, bid_qty: float, ask_qty: float) -> None:
        """Smooth level-1 order-book imbalance."""
        denom = bid_qty + ask_qty
        if denom <= 0:
            return
        inst_obi = (bid_qty - ask_qty) / denom
        if not self._obi_initialized:
            self._ewma_obi = inst_obi
            self._obi_initialized = True
        else:
            self._ewma_obi = 0.18 * inst_obi + 0.82 * self._ewma_obi

    def _prune_trades(self, now_ts: float) -> None:
        cutoff = now_ts - self.cvd_window_sec
        while self._trades and self._trades[0][0] < cutoff:
            self._trades.popleft()

    def _scaled_returns(self) -> list[float]:
        pts = list(self._sec_prices)
        out: list[float] = []
        for i in range(1, len(pts)):
            dt = pts[i][0] - pts[i - 1][0]
            p0, p1 = pts[i - 1][1], pts[i][1]
            if 0 < dt <= 15 and p0 > 0 and p1 > 0:
                out.append(math.log(p1 / p0) / math.sqrt(dt))
        return out

    def get_realized_vol_1s(self, floor: float = 0.000035) -> float:
        """Conservative 1s volatility: max(sample variance, fast EWMA variance), floored."""
        if len(self._sec_prices) < 10:
            return max(floor, 0.000055)
        rets = self._scaled_returns()
        if len(rets) < 8:
            return max(floor, 0.000055)
        mean_r = sum(rets) / len(rets)
        sample_var = sum((r - mean_r) ** 2 for r in rets) / max(1, len(rets) - 1)
        ewma_var = rets[0] ** 2
        for r in rets[1:]:
            ewma_var = 0.12 * (r * r) + 0.88 * ewma_var
        return max(floor, math.sqrt(max(sample_var, ewma_var)))

    def get_orderflow_metrics(self, now_ts: float) -> tuple[float, float, float]:
        """Return (cvd_imbalance, smoothed_obi, combined_orderflow_score), each in [-1, 1]."""
        self._prune_trades(now_ts)
        buy_vol = sum(n for _, n, s in self._trades if s > 0)
        sell_vol = sum(n for _, n, s in self._trades if s < 0)
        total_vol = buy_vol + sell_vol
        cvd_imb = ((buy_vol - sell_vol) / total_vol) if total_vol > 1000.0 else 0.0
        combined = 0.70 * cvd_imb + 0.30 * self._ewma_obi
        return cvd_imb, self._ewma_obi, max(-1.0, min(1.0, combined))


def estimate_fair_value(
    strike_price: float,
    chainlink_price: float,
    remaining_sec: float,
    vol_1s: float,
    orderflow_score: float = 0.0,
    dynamic_gap_bps: Optional[float] = None,
    student_t_df: float = 4.5,
    cvd_imbalance: float = 0.0,
) -> Optional[FairValueEstimate]:
    """Fair probability that Chainlink S_T >= S_0, in Chainlink space with CEX lead/flow adjustments."""
    if strike_price <= 0 or chainlink_price <= 0:
        return None
    tau = max(0.5, remaining_sec)
    horizon_vol = vol_1s * math.sqrt(tau)
    raw_z = math.log(chainlink_price / strike_price) / horizon_vol if horizon_vol > 0 else 0.0
    gap_bps = dynamic_gap_bps if dynamic_gap_bps is not None else 0.0
    gap_z_pull = (0.55 * (gap_bps / 10000.0) / horizon_vol) if horizon_vol > 0 else 0.0
    gap_z_pull = max(-0.30, min(0.30, gap_z_pull))
    drift_z_adj = 0.10 * max(-1.0, min(1.0, orderflow_score))
    adjusted_z = raw_z + gap_z_pull + drift_z_adj
    p_up = max(0.005, min(0.995, fat_tail_prob_from_z(adjusted_z, df=student_t_df)))
    return FairValueEstimate(
        strike_price=strike_price,
        chainlink_price=chainlink_price,
        cex_lead_price=None,
        oracle_gap_bps=gap_bps,
        remaining_sec=remaining_sec,
        vol_1s=vol_1s,
        horizon_vol_pct=horizon_vol * 100.0,
        raw_z_score=raw_z,
        adjusted_z_score=adjusted_z,
        orderflow_score=orderflow_score,
        p_true_up=p_up,
        p_true_down=1.0 - p_up,
        cvd_imbalance=cvd_imbalance,
    )


# --------------------------------------------------------------------------------------
# Entry / exit rules
# --------------------------------------------------------------------------------------


@dataclass
class OrderBookTop:
    """Executable top-of-book (VWAP-filtered) for both outcome tokens."""

    up_bid: Optional[float] = None
    up_ask: Optional[float] = None
    down_bid: Optional[float] = None
    down_ask: Optional[float] = None


@dataclass(frozen=True)
class Candidate:
    """A symbol that passed every entry gate on this tick."""

    symbol: str
    side: str
    ask: float
    bid: Optional[float]
    p_true: float
    z: float
    net_ev: float
    fee_per_share: float
    total_cost_per_share: float
    elapsed_sec: float


@dataclass(frozen=True)
class SideView:
    """Direction-normalised view of the model + book for one candidate side."""

    side: str
    z: float
    flow: float
    cvd: float
    gap_bps: float
    p_true: float
    bid: Optional[float]
    ask: Optional[float]


def _side_view(cfg: SniperConfig, fv: FairValueEstimate, book: OrderBookTop) -> Optional[SideView]:
    if fv.raw_z_score >= cfg.min_z_score:
        return SideView(
            "UP", fv.raw_z_score, fv.orderflow_score, fv.cvd_imbalance, fv.oracle_gap_bps,
            fv.p_true_up, book.up_bid, book.up_ask,
        )
    if fv.raw_z_score <= -cfg.min_z_score:
        return SideView(
            "DOWN", -fv.raw_z_score, -fv.orderflow_score, -fv.cvd_imbalance, -fv.oracle_gap_bps,
            fv.p_true_down, book.down_bid, book.down_ask,
        )
    return None


def estimate_twap_fair_value(
    strike_price: float,
    chainlink_price: float,
    remaining_sec: float,
    vol_1s: float,
    realized_240: tuple[Optional[float], float] = (None, 0.0),
    micro: tuple[float, Optional[float], float] = (0.0, None, 0.0),
    cfg: Optional[SniperConfig] = None,
) -> Optional[FairValueEstimate]:
    """TWAP-projection probability that final 60s Chainlink TWAP [T+240..T+300] settles >= strike."""
    if strike_price <= 0 or chainlink_price <= 0:
        return None
    use_cfg = cfg or TWAP_V2_FILTERED_CONFIG
    realized_twap_mean, realized_twap_elapsed = realized_240
    orderflow_score, dynamic_gap_bps, cvd_imbalance = micro
    rem = max(0.5, remaining_sec)
    vol = max(1e-7, vol_1s)
    if rem > 60.0:
        eff_tau = max(1.0, rem - 40.0)
        horizon_vol = vol * math.sqrt(eff_tau)
        raw_z = ((chainlink_price - strike_price) / strike_price) / horizon_vol
    else:
        a = max(0.0, min(59.5, realized_twap_elapsed))
        mean_src = realized_twap_mean if (realized_twap_mean and realized_twap_mean > 0) else chainlink_price
        denom = max(1e-6, a + rem)
        mean_twap = (a * mean_src + rem * chainlink_price) / denom
        horizon_vol = max(1e-7, (rem / 60.0) * vol * math.sqrt(rem / 3.0))
        raw_z = ((mean_twap - strike_price) / strike_price) / horizon_vol
    gap_bps = dynamic_gap_bps if dynamic_gap_bps is not None else 0.0
    drift_z_adj = 0.05 * max(-1.0, min(1.0, orderflow_score))
    adjusted_z = raw_z + drift_z_adj
    gw = max(0.0, min(1.0, use_cfg.twap_gaussian_weight))
    p_blend = gw * normal_cdf(adjusted_z) + (1.0 - gw) * fat_tail_prob_from_z(adjusted_z, df=use_cfg.twap_student_t_df)
    p_up = max(0.005, min(0.995, p_blend))
    return FairValueEstimate(
        strike_price=strike_price,
        chainlink_price=chainlink_price,
        cex_lead_price=None,
        oracle_gap_bps=gap_bps,
        remaining_sec=remaining_sec,
        vol_1s=vol_1s,
        horizon_vol_pct=horizon_vol * 100.0,
        raw_z_score=raw_z,
        adjusted_z_score=adjusted_z,
        orderflow_score=orderflow_score,
        p_true_up=p_up,
        p_true_down=1.0 - p_up,
        cvd_imbalance=cvd_imbalance,
    )


def _flow_gates_pass(cfg: SniperConfig, view: SideView) -> bool:
    if not cfg.use_flow_gates:
        return True
    if view.gap_bps < -cfg.max_adverse_oracle_gap_bps:
        return False
    return not (view.flow < cfg.min_orderflow_score or view.cvd < cfg.min_directional_cvd)


def _price_gates_pass(cfg: SniperConfig, view: SideView) -> bool:
    ask, bid = view.ask, view.bid
    if view.p_true < cfg.min_true_prob or ask is None:
        return False
    if ask < cfg.min_entry_price or ask > cfg.max_entry_price:
        return False
    if bid is not None and (ask < bid or (ask - bid) > cfg.max_entry_spread):
        return False
    return (view.p_true - ask) <= cfg.max_model_market_divergence


def evaluate_entry(
    symbol: str, cfg: SniperConfig, elapsed_sec: float, fv: FairValueEstimate, book: OrderBookTop
) -> Optional[Candidate]:
    """Return a `Candidate` when every entry gate passes for `symbol` at this instant, else None."""
    if elapsed_sec < cfg.entry_start_elapsed_sec or elapsed_sec > cfg.entry_end_elapsed_sec:
        return None
    view = _side_view(cfg, fv, book)
    if view is None or not _flow_gates_pass(cfg, view) or not _price_gates_pass(cfg, view):
        return None
    assert view.ask is not None
    ev = analyze_trade_ev(view.p_true, view.ask, cfg.fee_rate)
    if ev.net_ev_per_share < cfg.min_net_edge:
        return None
    return Candidate(
        symbol, view.side, view.ask, view.bid, view.p_true, view.z, ev.net_ev_per_share,
        ev.fee_per_share, ev.total_cost_per_share, elapsed_sec,
    )


def pick_best_candidate(cands: list[Candidate], priority_symbol: str = "BTCUSDT") -> Optional[Candidate]:
    """Single-slot ranking: highest net EV, with a 1c bonus for the historically validated symbol."""
    if not cands:
        return None
    return max(cands, key=lambda c: c.net_ev + (0.01 if c.symbol == priority_symbol else 0.0))


@dataclass
class OpenPosition:
    """The (single) open sniper position."""

    symbol: str
    side: str
    window_ts: int
    shares: float
    entry_price: float
    total_cost_per_share: float
    p_true_at_entry: float
    z_at_entry: float


@dataclass(frozen=True)
class ExitDecision:
    """Instruction to sell the open position before expiry."""

    reason: str
    bid: float
    net_proceeds_per_share: float


def _directional(fv: FairValueEstimate, side: str) -> tuple[float, float, float]:
    sign = 1.0 if side == "UP" else -1.0
    p_now = fv.p_true_up if side == "UP" else fv.p_true_down
    return sign * fv.raw_z_score, sign * fv.orderflow_score, p_now


def evaluate_exit(
    cfg: SniperConfig,
    pos: OpenPosition,
    elapsed_sec: float,
    fv: FairValueEstimate,
    book: OrderBookTop,
) -> Optional[ExitDecision]:
    """EV-consistent early take-profit and anti-whipsaw structural stop-loss."""
    bid = book.up_bid if pos.side == "UP" else book.down_bid
    ask = book.up_ask if pos.side == "UP" else book.down_ask
    if bid is None or bid <= 0.01:
        return None
    dir_z, dir_flow, p_now = _directional(fv, pos.side)
    net = effective_exit_proceeds(bid, cfg.fee_rate)
    if cfg.early_tp_enabled and net > pos.total_cost_per_share and (bid >= 0.99 or (bid >= cfg.take_profit_bid and net >= p_now - 0.002)):
        return ExitDecision("EARLY_TAKE_PROFIT", bid, net)
    if not cfg.enable_emergency_exit or elapsed_sec >= 292.0:
        return None
    spread_ok = ask is None or 0.0 <= (ask - bid) <= 0.08
    if dir_z < cfg.stop_loss_z_score and dir_flow < -0.25 and spread_ok and net >= 0.12:
        return ExitDecision("EMERGENCY_STOP_LOSS", bid, net)
    return None


# --------------------------------------------------------------------------------------
# Chip control (sizing) and execution price caps
# --------------------------------------------------------------------------------------


def compute_shares(cfg: SniperConfig, tradable_equity_u: float, cost_per_share: float) -> float:
    """Shares for one trade: risk_fraction of tradable equity, never below the 5-share exchange lot.

    Returns 0.0 when tradable equity cannot even cover the minimum lot (+ 5c buffer).
    """
    if cost_per_share <= 0.0:
        return 0.0
    min_lot_cost = cfg.min_shares * cost_per_share + 0.05
    if tradable_equity_u < min_lot_cost:
        return 0.0
    risk_shares = math.floor((tradable_equity_u * cfg.risk_fraction / cost_per_share) * 100.0) / 100.0
    return round(max(cfg.min_shares, risk_shares), 2)


def entry_limit_price(cfg: SniperConfig, ask: float, p_true: float) -> float:
    """Highest price we will pay: ask+1c, or up to ask+4c while net EV at that price stays >= min_net_edge."""
    limit_px = round(min(cfg.max_entry_price, ask + cfg.base_slippage), 2)
    for extra in cfg.slippage_ladder:
        cand = round(min(cfg.max_entry_price, ask + extra), 2)
        if (p_true - cand - taker_fee_per_share(cand, cfg.fee_rate)) >= cfg.min_net_edge:
            return cand
    return limit_px


_FROZEN_MODEL_KEYS: dict[str, str] = {
    "v1": "sniper_config",
    "v2_filtered": "sniper_config_v2_filtered",
    "v2_pure": "sniper_config_v2_pure",
    "v2_highband_shadow": "shadow_config_v2_highband",
}


def cfg_from_dict(raw: dict[str, Any]) -> SniperConfig:
    """Reconstruct a SniperConfig from JSON dict, normalizing tuple fields."""
    return SniperConfig(**{k: (tuple(v) if k == "slippage_ladder" else v) for k, v in raw.items()})


def load_frozen_sniper_config(frozen_path: pathlib.Path, model_name: str = "v2_filtered") -> tuple[SniperConfig, dict[str, float]]:
    """Load an exact pre-registered SniperConfig and per-symbol fee schedule from `frozen_config.json`."""
    raw = json.loads(frozen_path.read_text(encoding="utf-8"))
    key = _FROZEN_MODEL_KEYS.get(model_name)
    if key is None or key not in raw:
        raise KeyError(f"Unknown or missing frozen model {model_name!r} in {frozen_path}")
    fees = {str(k): float(v) for k, v in raw.get("effective_fees_post_nov20", {}).items()}
    return cfg_from_dict(raw[key]), fees


def check_frozen_config_parity(cfg: SniperConfig, frozen_path: pathlib.Path, model_name: str = "v2_filtered") -> tuple[bool, list[str]]:
    """Verify live `cfg` matches `frozen_config.json` bit-for-bit across all strategy parameters."""
    if not frozen_path.exists():
        return False, ["frozen_config_missing"]
    frozen_cfg, _fees = load_frozen_sniper_config(frozen_path, model_name)
    cur_d, exp_d = asdict(cfg), asdict(frozen_cfg)
    diffs = [f"{k}: live={cur_d.get(k)!r} != frozen={exp_d.get(k)!r}" for k in exp_d if cur_d.get(k) != exp_d.get(k)]
    return (len(diffs) == 0), diffs
