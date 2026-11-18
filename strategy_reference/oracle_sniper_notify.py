"""Telegram / desktop message builders for the Chainlink sniper (pure string formatting)."""
from __future__ import annotations

from typing import Any

SIDE_ZH = {"UP": "買漲 UP", "DOWN": "買跌 DOWN"}
TAG = "新策略·實盤"
PAPER_TAG = "TWAP V2·🧪 模擬盤 $43U"


def _coin(symbol: str) -> str:
    return symbol.replace("USDT", "")


def _tag_for(obj: dict[str, Any]) -> str:
    if not obj.get("paper_mode"):
        return TAG
    init_u = int(round(float(obj.get("paper_initial_u") or 43)))
    return f"TWAP V2·🧪 模擬盤 ${init_u}U"


def entry_message(trade: dict[str, Any], perf_lines: str) -> str:
    """Message sent when a sniper order is filled."""
    sym = _coin(str(trade["symbol"]))
    left = max(0.0, 300.0 - float(trade["entry_elapsed"]))
    return (
        f"🎯 狙擊開單 {sym} {SIDE_ZH.get(str(trade['side']), trade['side'])}（{_tag_for(trade)}）\n"
        f"• 進場：第 {float(trade['entry_elapsed']):.0f}s（距結算 {left:.0f}s）｜{float(trade['shares']):.2f} 股 @ "
        f"${float(trade['entry_price']):.3f}（含費成本 ${float(trade['cost_usd']):.2f}U）\n"
        f"• 依據：Z={float(trade['z']):.2f}｜模型勝率 {float(trade['p_true']) * 100:.1f}%｜"
        f"淨EV +{float(trade['net_ev']) * 100:.1f}¢｜同刻 {int(trade.get('candidates', 1))} 幣觸發取最優\n"
        f"{perf_lines}"
    )


def exit_message(trade: dict[str, Any], perf_lines: str) -> str:
    """Message sent when a sniper position is closed (take-profit, stop or expiry settlement)."""
    sym = _coin(str(trade["symbol"]))
    pnl = float(trade["pnl_usd"])
    reason = str(trade.get("exit_reason", ""))
    icon = "✅" if pnl > 0 else "❌"
    title = {
        "EARLY_TAKE_PROFIT": "提前停利",
        "EMERGENCY_STOP_LOSS": "結構止損",
    }.get(reason, "到期結算勝出" if pnl > 0 else "到期結算未過線")
    prov = "（預言機暫定結果）" if trade.get("provisional") else ""
    detail = _exit_detail(trade)
    hold = float(trade.get("exit_ts", 0.0)) - float(trade.get("entry_ts", 0.0))
    return (
        f"{icon} 關單{title} {sym} {SIDE_ZH.get(str(trade['side']), trade['side'])}｜{pnl:+.2f}U{prov}（{_tag_for(trade)}）\n"
        f"• {detail}｜持有 {max(0.0, hold):.0f}s｜成本 ${float(trade['cost_usd']):.2f}U\n"
        f"{perf_lines}"
    )


def _exit_detail(trade: dict[str, Any]) -> str:
    reason = str(trade.get("exit_reason", ""))
    if reason in ("EARLY_TAKE_PROFIT", "EMERGENCY_STOP_LOSS"):
        return f"賣出 {float(trade.get('exit_shares', trade['shares'])):.2f} 股 @ ${float(trade.get('exit_price', 0.0)):.3f}"
    strike = trade.get("strike")
    final = trade.get("final_price")
    if strike and final:
        return f"結算：${float(strike):,.2f} ➔ ${float(final):,.2f}"
    return f"結算：{reason.replace('SETTLED_', '')}"


def alert_message(alerts: list[str], perf_lines: str) -> str:
    """Risk-guard notification."""
    body = "\n".join(f"• {a}" for a in alerts)
    return f"🛡 風控提醒（{TAG}）\n{body}\n{perf_lines}"
