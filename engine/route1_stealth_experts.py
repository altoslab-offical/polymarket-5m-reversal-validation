"""8-domain stealth expert forensic miner and crowding impact detector for Route 1."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import statistics
import time
from typing import Any, Optional

import aiohttp

ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_EXPERTS_PATH = ROOT_DIR / "data" / "route1_stealth_experts.json"
DATA_API_BASE = "https://data-api.polymarket.com/v2"
GAMMA_API_BASE = "https://gamma-api.polymarket.com"

CATEGORIES: tuple[str, ...] = (
    "WEATHER",
    "ECONOMICS",
    "CULTURE",
    "SPORTS",
    "CRYPTO",
    "FINANCE",
    "TECH",
    "POLITICS",
)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(f"{path.suffix}.tmp.{os.getpid()}")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def compute_closed_position_stats(positions: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute unique-event win rate, median entry price, and Brier excess t-stat."""
    if not positions:
        return {"unique_events": 0, "positions": 0, "win_rate": 0.0, "median_price": 0.0, "mean_excess": 0.0, "t_stat": 0.0}
    seen_events: set[str] = set()
    wins = 0
    prices: list[float] = []
    excesses: list[float] = []
    for idx, p in enumerate(positions):
        ev_key = str(p.get("event_slug") or p.get("condition_id") or f"row_{idx}")
        seen_events.add(ev_key)
        avg_px = float(p.get("avg_price") or 0.0)
        cur_px = float(p.get("current_price") or 0.0)
        realized = float(p.get("realized_pnl") or 0.0)
        total_sz = float(p.get("total_size") or p.get("current_size") or 0.0)
        if 0.0 < avg_px < 1.0:
            prices.append(avg_px)
        if realized > 0.0 or cur_px >= 0.99:
            wins += 1
        if 0.0 < avg_px < 1.0:
            if cur_px >= 0.99 or cur_px <= 0.01:
                outcome = 1.0 if cur_px >= 0.5 else 0.0
                excesses.append(outcome - avg_px)
            elif total_sz > 0.0:
                excesses.append(max(-1.0, min(1.0, realized / total_sz)))
    n = len(excesses)
    mean_ex = statistics.mean(excesses) if n > 0 else 0.0
    stdev = statistics.stdev(excesses) if n >= 2 else 0.0
    t_stat = (mean_ex / (stdev / math.sqrt(n))) if (n >= 2 and stdev > 1e-9) else 0.0
    return {
        "unique_events": len(seen_events),
        "positions": len(positions),
        "win_rate": round(wins / len(positions), 4),
        "median_price": round(statistics.median(prices), 4) if prices else 0.0,
        "mean_excess": round(mean_ex, 4),
        "t_stat": round(t_stat, 3),
    }


def compute_pnl_curve_metrics(points: list[dict[str, Any]]) -> dict[str, float]:
    """Compute drawdown, 14d momentum, and daily Sharpe-like ratio from /v2/user-pnl."""
    if len(points) < 2:
        return {"max_drawdown_pct": 0.0, "recent_14d_pnl": 0.0, "sharpe_like": 0.0}
    series = [float(pt.get("realized_market_pnl", pt.get("realized_pnl", 0.0)) or 0.0) for pt in points]
    peak = series[0]
    max_dd_pct = 0.0
    for val in series:
        peak = max(peak, val)
        denom = max(abs(peak), 500.0)
        dd = max(0.0, (peak - val) / denom) * 100.0
        max_dd_pct = max(max_dd_pct, dd)
    diffs = [series[i] - series[i - 1] for i in range(1, len(series))]
    recent_idx = max(0, len(series) - 15)
    recent_14d = series[-1] - series[recent_idx]
    stdev = statistics.stdev(diffs) if len(diffs) >= 2 else 0.0
    sharpe = (statistics.mean(diffs) / stdev * math.sqrt(252.0)) if stdev > 1e-9 else 0.0
    return {
        "max_drawdown_pct": round(max_dd_pct, 2),
        "recent_14d_pnl": round(recent_14d, 2),
        "sharpe_like": round(max(-5.0, min(10.0, sharpe)), 2),
    }


def compute_crowding_impact(
    trades: list[dict[str, Any]],
    histories_by_idx: dict[int, list[dict[str, Any]]],
    fee_rate: float = 0.01,
    closed_mean_excess: float = 0.08,
) -> dict[str, float]:
    """Measure post-trade 30-90s price jump (crowding impact) and copyable ROI."""
    impacts_cents: list[float] = []
    slips_60s_cents: list[float] = []
    avg_prices: list[float] = []

    for idx, tr in enumerate(trades):
        pts = histories_by_idx.get(idx) or []
        p0 = float(tr.get("price") or 0.0)
        ts = int(tr.get("timestamp") or 0)
        if not pts or p0 <= 0.0 or p0 >= 1.0 or ts <= 0:
            continue
        sorted_pts = sorted(pts, key=lambda x: int(x.get("timestamp") or 0))
        after_30 = [float(x["price"]) for x in sorted_pts if ts <= int(x.get("timestamp") or 0) <= ts + 90]
        after_60 = [float(x["price"]) for x in sorted_pts if ts + 45 <= int(x.get("timestamp") or 0) <= ts + 180]
        p_near = after_30[0] if after_30 else float(sorted_pts[-1]["price"])
        p_60 = after_60[0] if after_60 else p_near
        impacts_cents.append(max(0.0, (p_near - p0) * 100.0))
        slips_60s_cents.append(max(0.0, (p_60 - p0) * 100.0))
        avg_prices.append(p0)

    if not impacts_cents:
        return {"impact_30s_cents": 0.0, "slip_60s_cents": 0.0, "lagged_60s_roi": round(closed_mean_excess, 4), "sample_count": 0}
    med_imp = statistics.median(impacts_cents)
    med_slip = statistics.median(slips_60s_cents)
    med_px = statistics.median(avg_prices)
    est_fee = fee_rate * med_px * (1.0 - med_px)
    lag_roi = closed_mean_excess - (med_slip / 100.0) - est_fee
    return {
        "impact_30s_cents": round(med_imp, 2),
        "slip_60s_cents": round(med_slip, 2),
        "lagged_60s_roi": round(lag_roi, 4),
        "sample_count": len(impacts_cents),
    }


def _check_rejection_reason(
    *,
    realized_pnl: float,
    rebate_ratio: float,
    biggest_ratio: float,
    mm_activity_ratio: float,
    closed_stats: dict[str, Any],
    taker_trade_count: int,
    pnl_curve_metrics: dict[str, float],
) -> Optional[str]:
    if realized_pnl <= 0.0:
        return "NEGATIVE_PNL"
    if rebate_ratio > 0.25:
        return "MM_REBATE"
    if mm_activity_ratio > 0.15:
        return "SPLIT_MERGE_MM"
    if biggest_ratio > 0.30:
        return "LOTTERY_CONCENTRATION"
    if int(closed_stats.get("unique_events") or 0) < 12:
        return "INSUFFICIENT_SAMPLE"
    if taker_trade_count < 3:
        return "PASSIVE_ONLY"
    med_px = float(closed_stats.get("median_price") or 0.0)
    if med_px > 0.90 or (0.0 < med_px < 0.08):
        return "BONDING_OR_LOTTO_PRICE"
    if float(closed_stats.get("t_stat") or 0.0) < 1.3:
        return "LOW_STAT_SIGNIFICANCE"
    if float(pnl_curve_metrics.get("recent_14d_pnl") or 0.0) < 0.0:
        return "STALE_OR_DECAYED"
    return None


def score_candidate_wallet(
    wallet: str,
    name: str,
    category: str,
    *,
    user_stats: dict[str, Any],
    closed_stats: dict[str, Any],
    mm_activity_ratio: float,
    taker_trade_count: int,
    pnl_curve_metrics: dict[str, float],
    crowding_metrics: dict[str, float],
    source: str = "LEADERBOARD",
) -> dict[str, Any]:
    """Run 8-gate forensic verification and classify as STEALTH_EXPERT, CROWDED_WHALE, or REJECTED."""
    realized = float(user_stats.get("realized_pnl") or user_stats.get("realized_market_pnl") or 0.0)
    rebates = float(user_stats.get("maker_rebate") or 0.0) + float(user_stats.get("reward_income") or 0.0)
    biggest = float(user_stats.get("biggest_win") or 0.0)
    rebate_ratio = (rebates / realized) if realized > 0.0 else 1.0
    biggest_ratio = (biggest / realized) if realized > 0.0 else 1.0

    reason = _check_rejection_reason(
        realized_pnl=realized,
        rebate_ratio=rebate_ratio,
        biggest_ratio=biggest_ratio,
        mm_activity_ratio=mm_activity_ratio,
        closed_stats=closed_stats,
        taker_trade_count=taker_trade_count,
        pnl_curve_metrics=pnl_curve_metrics,
    )
    impact = float(crowding_metrics.get("impact_30s_cents") or 0.0)
    lag_roi = float(crowding_metrics.get("lagged_60s_roi") or 0.0)
    t_stat = float(closed_stats.get("t_stat") or 0.0)
    sharpe = float(pnl_curve_metrics.get("sharpe_like") or 0.0)
    dd = float(pnl_curve_metrics.get("max_drawdown_pct") or 0.0)

    raw_score = (
        min(25.0, max(0.0, t_stat * 7.5))
        + min(20.0, max(0.0, (1.0 - rebate_ratio) * 20.0))
        + min(20.0, max(0.0, lag_roi * 180.0))
        + min(20.0, max(0.0, (2.2 - impact) * 10.0))
        + min(15.0, max(0.0, sharpe * 5.0 - dd * 0.2))
    )
    copy_score = round(max(0.0, min(100.0, raw_score)), 1)

    if reason is not None:
        classification = "REJECTED"
    elif impact >= 3.0:
        classification = "CROWDED_WHALE"
    elif impact <= 1.8 and lag_roi >= 0.02 and copy_score >= 65.0:
        classification = "STEALTH_EXPERT"
    else:
        classification = "WATCHLIST"

    return {
        "wallet": wallet.lower(),
        "name": name or wallet[:10],
        "category": category.upper(),
        "source": source,
        "classification": classification,
        "reject_reason": reason or "",
        "copy_score": copy_score,
        "realized_pnl": round(realized, 2),
        "pure_trade_pct": round(max(0.0, (1.0 - rebate_ratio) * 100.0), 1),
        "biggest_win_pct": round(biggest_ratio * 100.0, 1),
        "unique_events": int(closed_stats.get("unique_events") or 0),
        "win_rate_pct": round(float(closed_stats.get("win_rate") or 0.0) * 100.0, 1),
        "median_entry_px": float(closed_stats.get("median_price") or 0.0),
        "brier_excess": float(closed_stats.get("mean_excess") or 0.0),
        "t_stat": t_stat,
        "impact_30s_cents": impact,
        "lagged_60s_roi_pct": round(lag_roi * 100.0, 2),
        "sharpe_like": sharpe,
        "max_drawdown_pct": dd,
        "recent_14d_pnl": float(pnl_curve_metrics.get("recent_14d_pnl") or 0.0),
    }


def _extract_flat_user_stats(raw: dict[str, Any]) -> dict[str, Any]:
    data = raw.get("data") or {}
    all_pnl = data.get("all_time_pnl") or {}
    return {
        "trades": data.get("trades", 0),
        "biggest_win": data.get("biggest_win", 0.0),
        "realized_pnl": all_pnl.get("realized_pnl", all_pnl.get("realized_market_pnl", 0.0)),
        "maker_rebate": all_pnl.get("maker_rebate", 0.0),
        "reward_income": all_pnl.get("reward_income", 0.0),
    }


class StealthExpertScanner:
    """Async multi-domain Data API v2 forensic scanner and crowding classifier."""

    def __init__(self, path: Path = DEFAULT_EXPERTS_PATH, proxy_url: Optional[str] = None) -> None:
        self.path = path
        self.proxy_url = proxy_url
        self.state: dict[str, Any] = self._load()
        self.last_scan_errors: list[str] = []

    def _load(self) -> dict[str, Any]:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                self.last_scan_errors.append(f"load_failed: {exc}")
        return {
            "updated_at_ts": 0,
            "updated_at_iso": "",
            "categories": {},
            "stealth_experts": [],
            "crowded_whales": [],
            "watchlist": [],
            "rejected_summary": {},
        }

    def get_weather_expert_wallets(self) -> set[str]:
        """Return lowercase proxy_wallets verified as WEATHER stealth experts."""
        experts = self.state.get("stealth_experts") or []
        return {
            str(e.get("wallet") or "").lower()
            for e in experts
            if str(e.get("category") or "").upper() == "WEATHER" and e.get("wallet")
        }

    async def _get_json(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        url: str,
    ) -> Optional[Any]:
        async with sem:
            try:
                async with session.get(
                    url,
                    headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
                    proxy=self.proxy_url,
                ) as resp:
                    if resp.status != 200:
                        self.last_scan_errors.append(f"HTTP {resp.status} {url.split('?')[0]}")
                        return None
                    return await resp.json()
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                self.last_scan_errors.append(f"REQ_ERR {type(exc).__name__}: {exc}")
                return None

    async def _discover_category_candidates(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        category: str,
        limit: int,
    ) -> list[tuple[str, str, str]]:
        seen: dict[str, tuple[str, str, str]] = {}
        periods = ("MONTH", "ALL") if category == "WEATHER" else ("MONTH",)
        for period in periods:
            res = await self._get_json(session, sem, f"{DATA_API_BASE}/leaderboard?category={category}&time_period={period}&limit={limit}")
            for row in (res or {}).get("data") or []:
                uid = str(row.get("user_id") or "").lower()
                if uid and uid not in seen:
                    seen[uid] = (uid, str(row.get("user_name") or uid[:10]), f"LB_{period}")

        if category == "WEATHER":
            await self._mine_weather_stealth_holders(session, sem, seen)
        elif category in ("CULTURE", "ECONOMICS"):
            await self._mine_tag_stealth_holders(session, sem, category.lower(), seen)
        return list(seen.values())

    async def _mine_weather_stealth_holders(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        seen: dict[str, tuple[str, str, str]],
    ) -> None:
        evs = await self._get_json(session, sem, f"{GAMMA_API_BASE}/events?closed=false&active=true&tag_slug=weather&order=volume&ascending=false&limit=30&offset=0")
        cids: list[str] = []
        for ev in evs or []:
            if "temperature" not in str(ev.get("title") or "").lower():
                continue
            ms = sorted(ev.get("markets") or [], key=lambda m: float(m.get("volume") or 0.0), reverse=True)
            if ms and ms[0].get("conditionId"):
                cids.append(str(ms[0]["conditionId"]))
            if len(cids) >= 8:
                break
        await self._collect_holders_for_cids(session, sem, cids, seen, "STEALTH_HOLDER")

    async def _mine_tag_stealth_holders(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        tag: str,
        seen: dict[str, tuple[str, str, str]],
    ) -> None:
        evs = await self._get_json(session, sem, f"{GAMMA_API_BASE}/events?closed=false&active=true&tag_slug={tag}&order=volume&ascending=false&limit=10&offset=0")
        cids = [str(ev["markets"][0]["conditionId"]) for ev in (evs or [])[:4] if ev.get("markets") and ev["markets"][0].get("conditionId")]
        await self._collect_holders_for_cids(session, sem, cids, seen, "STEALTH_HOLDER")

    async def _collect_holders_for_cids(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        cids: list[str],
        seen: dict[str, tuple[str, str, str]],
        source_tag: str,
    ) -> None:
        tasks = [self._get_json(session, sem, f"{DATA_API_BASE}/holders?condition={cid}&limit=20&include_pnl=true") for cid in cids]
        for res in await asyncio.gather(*tasks):
            for tok_group in (res or {}).get("data") or []:
                for h in tok_group.get("holders") or []:
                    w = str(h.get("proxy_wallet") or "").lower()
                    cost = float(h.get("entry_cost_usdc") or 0.0)
                    pnl = float(h.get("realized_pnl") or 0.0)
                    if w and w not in seen and 40.0 <= cost <= 5000.0 and pnl >= 0.0:
                        seen[w] = (w, str(h.get("name") or h.get("pseudonym") or w[:10]), source_tag)

    async def _evaluate_wallet(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        wallet: str,
        name: str,
        category: str,
        source: str,
    ) -> Optional[dict[str, Any]]:
        raw_stats = await self._get_json(session, sem, f"{DATA_API_BASE}/user-stats?user={wallet}")
        if not raw_stats:
            return None
        flat = _extract_flat_user_stats(raw_stats)
        realized = float(flat.get("realized_pnl") or 0.0)
        rebates = float(flat.get("maker_rebate") or 0.0) + float(flat.get("reward_income") or 0.0)
        biggest = float(flat.get("biggest_win") or 0.0)
        if realized <= 0.0 or (rebates / realized) > 0.25 or (biggest / realized) > 0.30:
            return score_candidate_wallet(
                wallet, name, category,
                user_stats=flat,
                closed_stats={"unique_events": 0, "win_rate": 0.0, "median_price": 0.0, "mean_excess": 0.0, "t_stat": 0.0},
                mm_activity_ratio=0.0,
                taker_trade_count=0,
                pnl_curve_metrics={"max_drawdown_pct": 0.0, "recent_14d_pnl": 0.0, "sharpe_like": 0.0},
                crowding_metrics={"impact_30s_cents": 0.0, "lagged_60s_roi": 0.0, "sample_count": 0},
                source=source,
            )

        act_res, tr_res, pos_res, pnl_res = await asyncio.gather(
            self._get_json(session, sem, f"{DATA_API_BASE}/activity?user={wallet}&limit=80"),
            self._get_json(session, sem, f"{DATA_API_BASE}/trades?user={wallet}&taker_only=true&limit=30"),
            self._get_json(
                session,
                sem,
                f"{DATA_API_BASE}/positions?user={wallet}&status=CLOSED&limit=80&sort_by=TIMESTAMP&sort_direction=DESC",
            ),
            self._get_json(session, sem, f"{DATA_API_BASE}/user-pnl?user={wallet}&interval=1m&fidelity=1d"),
        )
        acts = (act_res or {}).get("data") or []
        mm_acts = sum(1 for a in acts if str(a.get("type") or "").upper() in ("SPLIT", "MERGE"))
        mm_ratio = (mm_acts / len(acts)) if acts else 0.0
        trades = [t for t in ((tr_res or {}).get("data") or []) if str(t.get("side") or "").upper() == "BUY"]
        closed_stats = compute_closed_position_stats((pos_res or {}).get("data") or [])
        curve_metrics = compute_pnl_curve_metrics(((pnl_res or {}).get("data") or {}).get("points") or [])

        sampled = trades[:5]
        hist_tasks = [
            self._get_json(
                session,
                sem,
                f"{DATA_API_BASE}/prices-history?token_id={t['token_id']}&start={int(t['timestamp'])-120}&end={int(t['timestamp'])+240}&bucket_seconds=60&limit=20",
            )
            for t in sampled
            if t.get("token_id") and t.get("timestamp")
        ]
        hist_res = await asyncio.gather(*hist_tasks)
        histories = {i: ((r or {}).get("data") or []) for i, r in enumerate(hist_res)}
        fee_r = 0.05 if category in ("SPORTS", "FINANCE", "TECH") else 0.01
        crowd = compute_crowding_impact(
            sampled, histories, fee_rate=fee_r, closed_mean_excess=float(closed_stats.get("mean_excess") or 0.0)
        )
        return score_candidate_wallet(
            wallet, name, category,
            user_stats=flat,
            closed_stats=closed_stats,
            mm_activity_ratio=mm_ratio,
            taker_trade_count=len(trades),
            pnl_curve_metrics=curve_metrics,
            crowding_metrics=crowd,
            source=source,
        )

    async def scan_all_domains(self, max_per_category: int = 8) -> dict[str, Any]:
        """Scan all 8 Polymarket domains, audit candidates, and persist results atomically."""
        self.last_scan_errors = []
        sem = asyncio.Semaphore(4)
        stealth_all: list[dict[str, Any]] = []
        crowded_all: list[dict[str, Any]] = []
        watch_all: list[dict[str, Any]] = []
        rejected_counts: dict[str, int] = {}
        cat_summary: dict[str, Any] = {}

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as session:
            for cat in CATEGORIES:
                cap = max(max_per_category, 14) if cat == "WEATHER" else max_per_category
                cands = await self._discover_category_candidates(session, sem, cat, limit=12)
                evals = await asyncio.gather(
                    *(self._evaluate_wallet(session, sem, w, nm, cat, src) for w, nm, src in cands[:cap])
                )
                rows = [r for r in evals if r is not None]
                cat_stealth = [r for r in rows if r["classification"] == "STEALTH_EXPERT"]
                cat_crowded = [r for r in rows if r["classification"] == "CROWDED_WHALE"]
                cat_watch = [r for r in rows if r["classification"] == "WATCHLIST"]
                for r in rows:
                    if r["classification"] == "REJECTED":
                        key = r["reject_reason"] or "UNKNOWN"
                        rejected_counts[key] = rejected_counts.get(key, 0) + 1
                stealth_all.extend(cat_stealth)
                crowded_all.extend(cat_crowded)
                watch_all.extend(cat_watch)
                cat_summary[cat] = {
                    "candidates_checked": len(rows),
                    "stealth_count": len(cat_stealth),
                    "crowded_count": len(cat_crowded),
                    "watchlist_count": len(cat_watch),
                }

        stealth_all.sort(key=lambda x: x["copy_score"], reverse=True)
        crowded_all.sort(key=lambda x: x["impact_30s_cents"], reverse=True)
        watch_all.sort(key=lambda x: x["copy_score"], reverse=True)
        now_ts = int(time.time())
        payload = {
            "updated_at_ts": now_ts,
            "updated_at_iso": datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "categories": cat_summary,
            "stealth_experts": stealth_all[:80],
            "crowded_whales": crowded_all[:60],
            "watchlist": watch_all[:60],
            "rejected_summary": rejected_counts,
            "scan_errors": len(self.last_scan_errors),
        }
        self.state = payload
        _atomic_write_json(self.path, payload)
        return payload
