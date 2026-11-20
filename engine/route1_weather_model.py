"""Pure meteorological nowcasting, METAR resolution, and EV sizing for Route 1 (zero I/O)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import math
import re
import statistics
from typing import Optional
from zoneinfo import ZoneInfo

PAPER_ONLY = True

MONTH_NAME_MAP: dict[str, int] = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}

_METAR_T_GROUP_RE = re.compile(r"\bT([01])(\d{3})\d{4}\b")
_METAR_BODY_TEMP_RE = re.compile(r"(?:^|\s)(M?\d{2})/(?:M?\d{2}|//)(?:\s|$)")
_METAR_TIME_RE = re.compile(r"\b(\d{2})(\d{2})(\d{2})Z\b")
_SLUG_DATE_RE = re.compile(r"on-([a-z]+)-(\d{1,2})-(\d{4})(?:$|-)")
_ICAO_IDS_URL_RE = re.compile(r"[?&]ids=([A-Z]{4})\b")
_ICAO_PAREN_RE = re.compile(r"\(([A-Z]{4})\)")
_BRACKET_RANGE_RE = re.compile(r"^(-?\d+)\s*-\s*(-?\d+)\s*°?\s*([CF])", re.IGNORECASE)
_BRACKET_BOUND_RE = re.compile(
    r"^(-?\d+)\s*°?\s*([CF])\s+or\s+(below|lower|under|higher|above|over)",
    re.IGNORECASE,
)
_BRACKET_SINGLE_RE = re.compile(r"^(-?\d+)\s*°?\s*([CF])$", re.IGNORECASE)


@dataclass(frozen=True)
class TempBracket:
    """Single temperature bracket market inside a daily city temperature NegRisk event."""

    condition_id: str
    label: str
    unit: str
    low: Optional[int]
    high: Optional[int]
    yes_token_id: str
    no_token_id: str
    best_bid: Optional[float]
    best_ask: Optional[float]
    fee_rate: float = 0.05
    order_min_size: float = 5.0
    volume: float = 0.0


@dataclass(frozen=True)
class WeatherEventSpec:
    """Daily city temperature event metadata mapped to an official resolution station."""

    event_slug: str
    city: str
    obs_date: date
    timezone_name: str
    unit: str
    liquidity_usd: float
    brackets: tuple[TempBracket, ...]
    icao: Optional[str] = None
    is_hko: bool = False
    lat: Optional[float] = None
    lon: Optional[float] = None
    title: str = ""


@dataclass(frozen=True)
class WeatherOpportunity:
    """Actionable or audited bracket candidate produced by the dual-engine evaluator."""

    event_slug: str
    city: str
    obs_date: str
    condition_id: str
    bracket_label: str
    side: str
    token_id: str
    model_prob: float
    market_mid: float
    exec_ask: float
    fee_per_share: float
    cost_per_share: float
    net_ev: float
    shares: float
    cost_usd: float
    nowcast_locked: bool
    expert_alignment: str
    agree_count: int
    oppose_count: int
    attribution: str
    fee_rate: float
    obs_max_display: Optional[float] = None


def round_half_up(value: float) -> int:
    """Round to nearest integer, rounding exact .5 ties upward toward +infinity."""
    return int(math.floor(value + 0.5 + 1e-9))


def c_to_f(celsius: float) -> float:
    """Convert degrees Celsius to degrees Fahrenheit."""
    return celsius * 9.0 / 5.0 + 32.0


def parse_metar_temp_c(raw_line: str) -> Optional[float]:
    """Parse routine METAR temperature in Celsius (excludes SPECI reports)."""
    line = (raw_line or "").strip()
    if not line or line.startswith("SPECI ") or line.startswith("SPECI\t"):
        return None
    t_match = _METAR_T_GROUP_RE.search(line)
    if t_match:
        sign = -1.0 if t_match.group(1) == "1" else 1.0
        return sign * (int(t_match.group(2)) / 10.0)
    body = line.split(" RMK", 1)[0]
    b_match = _METAR_BODY_TEMP_RE.search(body)
    if not b_match:
        return None
    raw_t = b_match.group(1)
    return -float(int(raw_t[1:])) if raw_t.startswith("M") else float(int(raw_t))


def parse_metar_timestamp_utc(raw_line: str, ref_utc: Optional[datetime] = None) -> Optional[datetime]:
    """Extract UTC observation datetime from DDHHMMZ token relative to `ref_utc`."""
    match = _METAR_TIME_RE.search(raw_line or "")
    if not match:
        return None
    day = int(match.group(1))
    hour = int(match.group(2))
    minute = int(match.group(3))
    if not (1 <= day <= 31 and 0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    ref = (ref_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
    cand = _safe_utc_datetime(ref.year, ref.month, day, hour, minute)
    if cand is None or cand > ref + timedelta(days=2):
        prev_m = ref.replace(day=1) - timedelta(days=1)
        cand = _safe_utc_datetime(prev_m.year, prev_m.month, day, hour, minute)
    elif cand < ref - timedelta(days=27):
        next_m = (ref.replace(day=28) + timedelta(days=4)).replace(day=1)
        cand = _safe_utc_datetime(next_m.year, next_m.month, day, hour, minute)
    return cand


def _safe_utc_datetime(year: int, month: int, day: int, hour: int, minute: int) -> Optional[datetime]:
    try:
        return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
    except ValueError:
        return None


def compute_local_day_metar_max_c(
    raw_lines: list[str],
    timezone_name: str,
    target_local_date: date,
    ref_utc: Optional[datetime] = None,
) -> tuple[Optional[float], int]:
    """Compute highest routine METAR temperature in Celsius for `target_local_date`."""
    tz = ZoneInfo(timezone_name)
    best_c: Optional[float] = None
    count = 0
    for line in raw_lines:
        temp_c = parse_metar_temp_c(line)
        if temp_c is None:
            continue
        ts_utc = parse_metar_timestamp_utc(line, ref_utc=ref_utc)
        if ts_utc is None or ts_utc.astimezone(tz).date() != target_local_date:
            continue
        count += 1
        best_c = temp_c if best_c is None else max(best_c, temp_c)
    return best_c, count


def parse_bracket_range(label: str) -> tuple[Optional[int], Optional[int], str]:
    """Parse Polymarket bracket label like '19°C', '14°C or below', or '66-67°F'."""
    clean = (label or "").strip().replace("−", "-")
    m_bound = _BRACKET_BOUND_RE.match(clean)
    if m_bound:
        val = int(m_bound.group(1))
        unit = m_bound.group(2).upper()
        kind = m_bound.group(3).lower()
        return (None, val, unit) if kind in ("below", "lower", "under") else (val, None, unit)
    m_range = _BRACKET_RANGE_RE.match(clean)
    if m_range:
        return int(m_range.group(1)), int(m_range.group(2)), m_range.group(3).upper()
    m_single = _BRACKET_SINGLE_RE.match(clean)
    if m_single:
        val = int(m_single.group(1))
        return val, val, m_single.group(2).upper()
    raise ValueError(f"Unsupported temperature bracket label: {label!r}")


def bracket_contains_value(rounded_val: int, low: Optional[int], high: Optional[int]) -> bool:
    """Check whether a rounded whole-degree temperature falls inside [low, high]."""
    if low is not None and rounded_val < low:
        return False
    return not (high is not None and rounded_val > high)


def extract_event_station_and_date(
    slug: str,
    title: str,
    description: str,
) -> tuple[Optional[str], bool, Optional[date]]:
    """Extract (ICAO code, is_hko, observation date) from Gamma weather metadata."""
    obs_date: Optional[date] = None
    m_date = _SLUG_DATE_RE.search((slug or "").lower())
    if m_date:
        month = MONTH_NAME_MAP.get(m_date.group(1))
        if month is not None:
            obs_date = _safe_date(int(m_date.group(3)), month, int(m_date.group(2)))

    desc = description or ""
    low_desc = desc.lower()
    if "hong kong observatory" in low_desc or "hong-kong" in (slug or "").lower() or "hong kong" in (title or "").lower():
        return None, True, obs_date

    m_url = _ICAO_IDS_URL_RE.search(desc)
    if m_url:
        return m_url.group(1), False, obs_date
    m_par = _ICAO_PAREN_RE.search(desc)
    if m_par:
        return m_par.group(1), False, obs_date
    return None, False, obs_date


def _safe_date(year: int, month: int, day: int) -> Optional[date]:
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _norm_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def calibrate_member_maxima_c(
    member_daily_max_c: list[float],
    *,
    station_bias_c: float = 0.0,
    nws_anchor_c: Optional[float] = None,
) -> list[float]:
    """Shift ensemble members by damped station METAR bias and optional 50% NWS MOS anchor."""
    if not member_daily_max_c:
        return []
    damped = 0.70 * max(-3.0, min(3.0, station_bias_c))
    shifted = [v + damped for v in member_daily_max_c]
    if nws_anchor_c is not None:
        med = statistics.median(shifted)
        anchor_adj = 0.50 * max(-4.0, min(4.0, nws_anchor_c - med))
        shifted = [v + anchor_adj for v in shifted]
    return shifted


def _effective_member_maxima_c(
    member_daily_max_c: list[float],
    hourly_member_temps_c: Optional[list[list[float]]],
    remaining_hour_indices: Optional[tuple[int, ...]],
    obs_max_c: Optional[float],
    eff_bias_c: float,
) -> list[float]:
    if hourly_member_temps_c and remaining_hour_indices is not None:
        out: list[float] = []
        for series in hourly_member_temps_c:
            vals = [series[i] + eff_bias_c for i in remaining_hour_indices if 0 <= i < len(series)]
            fut = max(vals) if vals else (obs_max_c if obs_max_c is not None else 0.0)
            out.append(max(fut, obs_max_c) if obs_max_c is not None else fut)
        if out:
            return out
    if obs_max_c is not None:
        return [max(v, obs_max_c) for v in member_daily_max_c]
    return list(member_daily_max_c)


def compute_bracket_probabilities(
    brackets: tuple[TempBracket, ...] | list[TempBracket],
    member_daily_max_c: list[float],
    *,
    hourly_member_temps_c: Optional[list[list[float]]] = None,
    remaining_hour_indices: Optional[tuple[int, ...]] = None,
    obs_max_c: Optional[float] = None,
    obs_current_c: Optional[float] = None,
    model_current_hour_mean_c: Optional[float] = None,
    local_hour: Optional[int] = None,
    return_lock_flag: bool = False,
) -> dict[str, float] | tuple[dict[str, float], bool]:
    """Compute physical kernel-smoothed probabilities across all temperature brackets."""
    if not brackets:
        return ({}, False) if return_lock_flag else {}
    unit = brackets[0].unit.upper()
    raw_bias = (obs_current_c - model_current_hour_mean_c) if (obs_current_c is not None and model_current_hour_mean_c is not None) else 0.0
    eff_bias_c = 0.7 * max(-4.0, min(4.0, raw_bias))
    maxima_c = _effective_member_maxima_c(member_daily_max_c, hourly_member_temps_c, remaining_hour_indices, obs_max_c, eff_bias_c)
    if not maxima_c:
        eq = {b.label: 1.0 / len(brackets) for b in brackets}
        return (eq, False) if return_lock_flag else eq

    sigma_c = 0.45 if obs_max_c is None else (0.15 if (local_hour or 0) >= 21 else 0.35)
    sigma_u = sigma_c if unit == "C" else sigma_c * 1.8
    samples_u = [x if unit == "C" else c_to_f(x) for x in maxima_c]
    floor_int = round_half_up(obs_max_c if unit == "C" else c_to_f(obs_max_c)) if obs_max_c is not None else None

    raw_probs: dict[str, float] = {}
    for br in brackets:
        if floor_int is not None and br.high is not None and br.high < floor_int:
            raw_probs[br.label] = 0.0
            continue
        raw_probs[br.label] = _bracket_gauss_mass(br, samples_u, sigma_u, floor_int)

    total = sum(raw_probs.values())
    if total <= 0.0:
        raw_probs[brackets[-1].label] = 1.0
        total = 1.0
    norm = {k: v / total for k, v in raw_probs.items()}

    if not return_lock_flag:
        return norm
    med_u = statistics.median([x if unit == "C" else c_to_f(x) for x in member_daily_max_c]) if member_daily_max_c else 0.0
    locked = bool(
        obs_max_c is not None
        and (
            (local_hour is not None and local_hour >= 21)
            or (remaining_hour_indices is not None and len(remaining_hour_indices) == 0)
            or (floor_int is not None and floor_int > round_half_up(med_u))
        )
    )
    return norm, locked


def _bracket_gauss_mass(
    br: TempBracket,
    samples_u: list[float],
    sigma_u: float,
    floor_int: Optional[int],
) -> float:
    lo_edge = None if br.low is None else (br.low - 0.5)
    if floor_int is not None:
        floor_edge = floor_int - 0.5
        lo_edge = floor_edge if lo_edge is None else max(lo_edge, floor_edge)
    hi_edge = None if br.high is None else (br.high + 0.5)
    if lo_edge is not None and hi_edge is not None and lo_edge >= hi_edge:
        return 0.0
    acc = 0.0
    for mu in samples_u:
        p_hi = 1.0 if hi_edge is None else _norm_cdf((hi_edge - mu) / sigma_u)
        p_lo = 0.0 if lo_edge is None else _norm_cdf((lo_edge - mu) / sigma_u)
        acc += max(0.0, p_hi - p_lo)
    tiny = 1e-12 if (floor_int is not None and bracket_contains_value(floor_int, br.low, br.high)) else 0.0
    return acc / len(samples_u) + tiny


def dynamic_taker_fee_per_share(price: float, fee_rate: float) -> float:
    """Polymarket dynamic taker fee per share rounded to 5 decimals."""
    if price <= 0.0 or price >= 1.0 or fee_rate <= 0.0:
        return 0.0
    fee = round(fee_rate * price * (1.0 - price), 5)
    return fee if fee >= 0.00001 else 0.0


def sweep_orderbook_vwap(
    levels: list[tuple[float, float]],
    target_shares: float,
    max_slip: float = 0.03,
) -> Optional[float]:
    """Compute depth-aware executable VWAP across ask/bid levels sorted best-first."""
    if not levels or target_shares <= 0.0:
        return None
    top_px = levels[0][0]
    filled = 0.0
    notional = 0.0
    for price, size in levels:
        if size <= 0.0 or abs(price - top_px) > max_slip + 1e-9:
            break
        take = min(size, target_shares - filled)
        filled += take
        notional += take * price
        if filled >= target_shares - 1e-6:
            break
    if filled < min(target_shares, 5.0):
        return None
    return round(notional / filled, 4)


def size_paper_position(
    equity_u: float,
    p_true: float,
    cost_per_share: float,
    min_shares: float = 5.0,
    kelly_mult: float = 0.40,
    max_event_frac: float = 0.15,
) -> tuple[float, float]:
    """Fractional binary Kelly sizing obeying Polymarket 5-share floor and 15% cap."""
    if equity_u <= 0.0 or cost_per_share <= 0.0 or cost_per_share >= 1.0 or p_true <= cost_per_share:
        return 0.0, 0.0
    full_kelly = (p_true - cost_per_share) / (1.0 - cost_per_share)
    cap_u = equity_u * max_event_frac
    target_u = min(cap_u, equity_u * kelly_mult * full_kelly)
    shares = math.floor((target_u / cost_per_share) * 100.0) / 100.0
    if shares < min_shares:
        min_cost = min_shares * cost_per_share
        if min_cost > cap_u + 1e-9 or min_cost > equity_u:
            return 0.0, 0.0
        shares = min_shares
    cost_usd = round(shares * cost_per_share, 4)
    return shares, cost_usd


def _side_expert_alignment(
    yes_n: int,
    no_n: int,
    side: str,
) -> tuple[str, int, int]:
    agree_n, oppose_n = (yes_n, no_n) if side == "YES" else (no_n, yes_n)
    if oppose_n >= 2 and oppose_n > agree_n:
        return "OPPOSE", agree_n, oppose_n
    if agree_n >= 1 and agree_n > oppose_n:
        return "AGREE", agree_n, oppose_n
    return "NEUTRAL", agree_n, oppose_n


def evaluate_weather_opportunities(
    event: WeatherEventSpec,
    probs: dict[str, float],
    equity_u: float,
    *,
    nowcast_locked: bool = False,
    clob_asks: Optional[dict[str, float]] = None,
    expert_counts_by_cid: Optional[dict[str, tuple[int, int]]] = None,
    obs_max_display: Optional[float] = None,
) -> list[WeatherOpportunity]:
    """Select at most 1 highest-net-EV side/bracket for a weather event."""
    best: Optional[WeatherOpportunity] = None
    counts_map = expert_counts_by_cid or {}
    ask_map = clob_asks or {}

    for br in event.brackets:
        p_yes = probs.get(br.label)
        if p_yes is None:
            continue
        yes_n, no_n = counts_map.get(br.condition_id, (0, 0))
        candidates = _bracket_side_candidates(br, p_yes, ask_map, nowcast_locked)
        for side, token_id, p_true, exec_ask, mkt_mid in candidates:
            align_info = _side_expert_alignment(yes_n, no_n, side)
            if align_info[0] == "OPPOSE":
                continue
            opp = _score_candidate(
                event=event,
                br=br,
                side=side,
                token_id=token_id,
                p_true=p_true,
                exec_ask=exec_ask,
                mkt_mid=mkt_mid,
                equity_u=equity_u,
                nowcast_locked=nowcast_locked,
                align_info=align_info,
                obs_max_display=obs_max_display,
            )
            if opp is not None and (best is None or opp.net_ev > best.net_ev):
                best = opp
    return [best] if best is not None else []


def _bracket_side_candidates(
    br: TempBracket,
    p_yes: float,
    ask_map: dict[str, float],
    nowcast_locked: bool,
) -> list[tuple[str, str, float, float, float]]:
    out: list[tuple[str, str, float, float, float]] = []
    max_px = 0.94 if nowcast_locked else 0.88
    yes_ask = ask_map.get(br.yes_token_id, br.best_ask)
    if yes_ask is not None and 0.08 <= yes_ask <= max_px:
        mid = ((br.best_bid + yes_ask) / 2.0) if br.best_bid is not None else yes_ask
        out.append(("YES", br.yes_token_id, p_yes, yes_ask, mid))
    no_ask = ask_map.get(br.no_token_id)
    if no_ask is None and br.best_bid is not None and br.best_bid > 0.0:
        no_ask = round(1.0 - br.best_bid, 4)
    if no_ask is not None and 0.08 <= no_ask <= max_px:
        mid_no = round(1.0 - br.best_ask, 4) if br.best_ask is not None else no_ask
        out.append(("NO", br.no_token_id, 1.0 - p_yes, no_ask, (mid_no + no_ask) / 2.0))
    return out


def _score_candidate(
    *,
    event: WeatherEventSpec,
    br: TempBracket,
    side: str,
    token_id: str,
    p_true: float,
    exec_ask: float,
    mkt_mid: float,
    equity_u: float,
    nowcast_locked: bool,
    align_info: tuple[str, int, int],
    obs_max_display: Optional[float],
) -> Optional[WeatherOpportunity]:
    align, agree_n, oppose_n = align_info
    effective_lock = bool(nowcast_locked and p_true >= 0.75)
    if not effective_lock and align != "AGREE" and abs(p_true - mkt_mid) > 0.28:
        return None
    fee_ps = dynamic_taker_fee_per_share(exec_ask, br.fee_rate)
    cost_ps = round(exec_ask + fee_ps, 5)
    net_ev = p_true - cost_ps
    min_ev = 0.040 if effective_lock else (0.045 if align == "AGREE" else 0.060)
    if net_ev < min_ev:
        return None
    kelly_mult = 0.55 if (effective_lock or align == "AGREE") else 0.40
    shares, cost_usd = size_paper_position(
        equity_u=equity_u,
        p_true=p_true,
        cost_per_share=cost_ps,
        min_shares=br.order_min_size,
        kelly_mult=kelly_mult,
        max_event_frac=0.15,
    )
    if shares <= 0.0:
        return None
    attr = "NOWCAST_LOCK" if effective_lock else ("EXPERT_CONFLUENCE" if align == "AGREE" else "ENSEMBLE_SOLO")
    return WeatherOpportunity(
        event_slug=event.event_slug,
        city=event.city,
        obs_date=event.obs_date.isoformat(),
        condition_id=br.condition_id,
        bracket_label=br.label,
        side=side,
        token_id=token_id,
        model_prob=round(p_true, 4),
        market_mid=round(mkt_mid, 4),
        exec_ask=round(exec_ask, 4),
        fee_per_share=fee_ps,
        cost_per_share=cost_ps,
        net_ev=round(net_ev, 4),
        shares=shares,
        cost_usd=cost_usd,
        nowcast_locked=effective_lock,
        expert_alignment=align,
        agree_count=agree_n,
        oppose_count=oppose_n,
        attribution=attr,
        fee_rate=br.fee_rate,
        obs_max_display=obs_max_display,
    )
