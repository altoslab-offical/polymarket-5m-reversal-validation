from __future__ import annotations

import math
from typing import Any
import aiohttp


def _norm_cdf(x: float) -> float:
    """Standard normal cumulative distribution function Phi(x)."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def estimate_polymarket_share_pricing(
    open_price: float,
    current_price: float,
    high_price: float,
    low_price: float,
    seconds_remaining: int,
    direction: str = "LONG_BOUNCE",
    sigma_5m_pct: float = 0.18,
    target_bounce_pct: float = 0.10,
) -> dict[str, Any]:
    """
    Calculate real-time Polymarket 5m Up/Down CLOB share fair value (in cents 1c..99c),
    strike distance from 5m Open, +0.10% escape target price, and actionable CLOB order guidance.
    """
    if open_price <= 0:
        open_price = max(current_price, 1.0)

    is_up = direction in ("LONG_BOUNCE", "UP")
    delta_pct = ((current_price - open_price) / open_price) * 100.0
    tau_frac = max(0.02, min(1.0, seconds_remaining / 300.0))
    denom = max(0.01, sigma_5m_pct * math.sqrt(tau_frac))
    d2 = delta_pct / denom

    up_prob = min(0.99, max(0.01, _norm_cdf(d2)))
    down_prob = 1.0 - up_prob

    up_cents = round(up_prob * 100.0, 1)
    down_cents = round(down_prob * 100.0, 1)
    favored_cents = up_cents if is_up else down_cents

    favorable_peak_pct = (
        ((high_price - open_price) / open_price) * 100.0
        if is_up
        else ((open_price - low_price) / open_price) * 100.0
    )
    bounce_protect_triggered = favorable_peak_pct >= target_bounce_pct

    escape_target_price = (
        open_price * (1.0 + target_bounce_pct / 100.0)
        if is_up
        else open_price * (1.0 - target_bounce_pct / 100.0)
    )

    if bounce_protect_triggered and ((is_up and current_price > open_price) or (not is_up and current_price < open_price)):
        clob_action = "🎯 盤中已達 +0.10% 反彈！立即掛 72¢~78¢ 限價保利賣單，鎖定 +41%~+53% 利潤，剩餘可博 100¢ 到期結算！"
    elif bounce_protect_triggered:
        clob_action = "⚠️ 盤中曾觸及 +0.10% 反彈但現價回測開盤價，請確保 72¢ 保利單已成交或立即平倉護利！"
    else:
        side_str = "BUY UP (買漲)" if is_up else "BUY DOWN (買跌)"
        clob_action = f"📌 開盤掛 Maker 限價單 {side_str} @ 50¢~51¢（嚴禁追高超過 52.5¢），進場後預掛 78¢~83¢ 減倉單"

    return {
        "strike_open_price": round(open_price, 4),
        "current_price": round(current_price, 4),
        "delta_from_open_pct": round(delta_pct, 3),
        "favorable_peak_pct": round(favorable_peak_pct, 3),
        "escape_target_price": round(escape_target_price, 4),
        "up_share_cents": up_cents,
        "down_share_cents": down_cents,
        "favored_share_cents": favored_cents,
        "recommended_entry_cents": 50.0,
        "max_allowed_entry_cents": 52.5,
        "bounce_protect_cents": 72.0,
        "expiration_payout_cents": 100.0,
        "bounce_protect_triggered": bool(bounce_protect_triggered),
        "clob_action_guide": clob_action,
    }


async def fetch_polymarket_crypto_markets() -> list[dict[str, Any]]:
    """
    Fetch active 5m BTC/ETH/SOL Up or Down prediction markets from Polymarket Gamma API
    using deterministic 5m epoch slugs (`{btc|eth|sol}-updown-5m-{bucket_ts}`).
    """
    import time
    import json

    now_bucket = (int(time.time()) // 300) * 300
    pairs = [("ETHUSDT", "eth"), ("BTCUSDT", "btc"), ("SOLUSDT", "sol")]
    found: list[dict[str, Any]] = []

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
            for sym, prefix in pairs:
                slug = f"{prefix}-updown-5m-{now_bucket}"
                url = f"https://gamma-api.polymarket.com/markets?slug={slug}"
                async with session.get(url) as resp:
                    if resp.status == 200:
                        arr = await resp.json()
                        if arr:
                            m = arr[0]
                            raw_tokens = m.get("clobTokenIds")
                            tokens = json.loads(raw_tokens) if isinstance(raw_tokens, str) else (raw_tokens or [])
                            found.append({
                                "symbol": sym,
                                "slug": slug,
                                "title": m.get("question", slug),
                                "up_token_id": tokens[0] if len(tokens) > 0 else "",
                                "down_token_id": tokens[1] if len(tokens) > 1 else "",
                                "url": f"https://polymarket.com/event/{slug}",
                            })
        if found:
            return found
    except Exception:
        pass

    return [
        {
            "symbol": sym,
            "slug": f"{prefix}-updown-5m-{now_bucket}",
            "title": f"{sym.replace('USDT', '')} Up or Down 5m Live CLOB",
            "url": f"https://polymarket.com/event/{prefix}-updown-5m-{now_bucket}",
        }
        for sym, prefix in pairs
    ]

