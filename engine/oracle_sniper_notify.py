"""Telegram / desktop message builders for the Chainlink sniper (tactical terminal cards)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import html
import re
from typing import Any

RULE = "━━━━━━━━━━━━━━━━━━━━"
TZ_TAIPEI = timezone(timedelta(hours=8))
SIDE_META = {"UP": ("LONG", "買漲"), "DOWN": ("SHORT", "買跌")}
_HTML_TAG_RE = re.compile(r"</?(?:b|code|i|u|s)>")


def strip_html_tags(text: str) -> str:
    """Strip Telegram HTML formatting tags for plain-text desktop/Discord channels."""
    return html.unescape(_HTML_TAG_RE.sub("", text))


def _esc(val: Any) -> str:
    return html.escape(str(val), quote=False)


def _fmt_px(px: float) -> str:
    return f"${px:.4f}" if abs(px * 1000.0 - round(px * 1000.0)) >= 0.05 else f"${px:.3f}"


def _window_range(window_ts: Any) -> str:
    ts = int(window_ts or 0)
    if ts <= 0:
        return "--:--~--:--"
    start = datetime.fromtimestamp(ts, tz=TZ_TAIPEI)
    end = start + timedelta(minutes=5)
    return f"{start.strftime('%H:%M')}~{end.strftime('%H:%M')}"


def _short_oid(trade: dict[str, Any]) -> str:
    if trade.get("paper_mode"):
        return str(trade.get("fill_status") or "PAPER-SIM")
    oid = str(trade.get("order_id") or "").strip()
    return oid[:18] if oid else "--"


def _streak_label(summary: dict[str, Any]) -> str:
    s_len = int(summary.get("streak_len") or 0)
    s_type = str(summary.get("streak_type") or "")
    if s_len <= 0:
        return "--"
    if s_type == "W":
        return f"{s_len} 連勝"
    return f"{s_len} 連敗" if s_type == "L" else "--"


def _footer_html(perf_lines: str, summary: dict[str, Any] | None, unit: str = "pUSD") -> str:
    if not summary:
        return _esc(perf_lines) if perf_lines else ""
    wins = int(summary.get("wins") or 0)
    losses = int(summary.get("losses") or 0)
    trades = int(summary.get("trades") or 0)
    wr = f"{float(summary.get('win_rate_pct') or 0.0):.1f}%" if trades > 0 else "待首單"
    eq = float(summary.get("equity_u") or 0.0)
    vault = float(summary.get("vault_u") or 0.0)
    net = float(summary.get("net_u") or 0.0)
    pct = float(summary.get("pct") or 0.0)
    daily = float(summary.get("daily_pnl_usd") or 0.0)
    vault_seg = f" | 鎖倉 <code>${vault:.0f}</code>" if vault >= 1.0 else ""
    daily_seg = f" | 今日 <code>{daily:+.2f}</code>" if abs(daily) >= 0.005 else ""
    return (
        f"▸ 累計戰績：<code>{wins}W - {losses}L</code> (勝率 <code>{wr}</code>) | 權益 <code>${eq:.2f}</code>{vault_seg}\n"
        f"▸ 累計損益：<b>{net:+.2f} {unit}</b> (<code>{pct:+.1f}%</code>) | <code>{_streak_label(summary)}</code>{daily_seg}"
    )


def _exit_meta(reason: str, won: bool, is_paper: bool) -> tuple[str, str, str]:
    if reason == "EARLY_TAKE_PROFIT":
        code = "PAPER HIT" if is_paper else "TARGET HIT"
        title = "模擬止盈" if is_paper else "獲利止盈"
        return code, title, ("WIN [提前止盈]" if won else "EXIT [提前出場]")
    if reason == "EMERGENCY_STOP_LOSS":
        return ("PAPER STOP" if is_paper else "STOP TRIGGERED"), ("模擬止損" if is_paper else "結構止損"), "STOP [緊急止損]"
    if won:
        return ("PAPER WIN" if is_paper else "TARGET HIT"), ("模擬結算勝" if is_paper else "到期結算勝"), "WIN [到期結算]"
    return ("PAPER LOSS" if is_paper else "STOPPED OUT"), ("模擬結算負" if is_paper else "結算未過線"), "LOSS [到期結算]"


def entry_message(trade: dict[str, Any], perf_lines: str, summary: dict[str, Any] | None = None) -> str:
    """Build tactical Telegram HTML notification card for an executed sniper entry."""
    sym = _esc(str(trade["symbol"]).replace("USDT", ""))
    side = str(trade["side"])
    dir_en, side_zh = SIDE_META.get(side, (side, side))
    is_paper = bool(trade.get("paper_mode"))
    seq = int(trade.get("seq") or 0)
    seq_tag = f" #{seq}" if seq > 0 else ""
    init_u = int(round(float(trade.get("paper_initial_u") or 40)))
    hdr = f"[PAPER SNIPE // 🧪 模擬盤 ${init_u}U 進場{seq_tag}]" if is_paper else f"[SNIPE EXECUTED // 實盤進場{seq_tag}]"
    elapsed = float(trade["entry_elapsed"])
    left = max(0.0, 300.0 - elapsed)
    shares = float(trade["shares"])
    entry_px = float(trade["entry_price"])
    cost_usd = float(trade["cost_usd"])
    cost_ps = float(trade.get("cost_per_share") or (cost_usd / max(shares, 1e-6)))
    cands = int(trade.get("candidates", 1))
    cand_note = f" (同刻 {cands} 幣取優)" if cands > 1 else ""
    return (
        f"<b>{_esc(hdr)}</b>\n{RULE}\n"
        f"▸ 狙擊方向：<b>{sym}</b> · <b>{_esc(dir_en)}</b> [{_esc(side)} {_esc(side_zh)}]\n"
        f"▸ 目標盤口：<code>{_window_range(trade.get('window_ts'))}</code> (進場秒數 <code>{elapsed:.0f}s</code> · 剩 <code>{left:.0f}s</code>)\n"
        f"▸ 成交均價：<code>${entry_px:.3f}</code> (含費成本 <code>${cost_ps:.4f}</code>)\n"
        f"▸ 部位規模：<code>{shares:.2f} sh</code> (投入 <code>${cost_usd:.2f}</code>)\n"
        f"▸ 模型指標：<code>P_true {float(trade['p_true']) * 100:.1f}%</code> | "
        f"<code>Z {float(trade['z']):+.2f}</code> | <code>EV +{float(trade['net_ev']) * 100:.1f}¢</code>{cand_note}\n"
        f"▸ 訂單單號：<code>{_esc(_short_oid(trade))}</code>\n"
        f"{RULE}\n{_footer_html(perf_lines, summary)}"
    )


def exit_message(trade: dict[str, Any], perf_lines: str, summary: dict[str, Any] | None = None) -> str:
    """Build tactical Telegram HTML notification card when a sniper trade closes."""
    sym = _esc(str(trade["symbol"]).replace("USDT", ""))
    side = _esc(trade["side"])
    pnl = float(trade["pnl_usd"])
    won = bool(trade["won"]) if "won" in trade else pnl > 0.0
    reason = str(trade.get("exit_reason") or "")
    is_paper = bool(trade.get("paper_mode"))
    seq = int(trade.get("seq") or 0)
    seq_tag = f" #{seq}" if seq > 0 else ""
    code_en, zh_title, res_tag = _exit_meta(reason, won, is_paper)
    prov = " <i>(預言機暫定)</i>" if trade.get("provisional") else ""
    entry_px = float(trade["entry_price"])
    exit_px = float(trade["exit_price"]) if (reason in ("EARLY_TAKE_PROFIT", "EMERGENCY_STOP_LOSS") and trade.get("exit_price")) else (1.0 if won else 0.0)
    cost_usd = max(float(trade.get("cost_usd") or 0.0), 1e-6)
    roi_pct = (pnl / cost_usd) * 100.0
    hold = max(0.0, float(trade.get("exit_ts") or 0.0) - float(trade.get("entry_ts") or 0.0))
    oracle_line = ""
    if reason not in ("EARLY_TAKE_PROFIT", "EMERGENCY_STOP_LOSS") and trade.get("strike") and trade.get("final_price"):
        oracle_line = f"\n▸ 預言機價：<code>${float(trade['strike']):,.2f}</code> → <code>${float(trade['final_price']):,.2f}</code>"
    return (
        f"<b>[{code_en} // {zh_title}{seq_tag}] {pnl:+.2f} pUSD</b>{prov}\n{RULE}\n"
        f"▸ 結算結果：<b>{res_tag}</b> | 淨利 <b>{pnl:+.2f} pUSD</b> (<code>{roi_pct:+.1f}%</code>)\n"
        f"▸ 交易部位：<b>{sym}</b> · <b>{side}</b> (盤口 <code>{_window_range(trade.get('window_ts'))}</code>)\n"
        f"▸ 進出場價：<code>${entry_px:.3f}</code> → <code>{_fmt_px(exit_px)}</code> (持有 <code>{hold:.0f}s</code>)"
        f"{oracle_line}\n{RULE}\n{_footer_html(perf_lines, summary)}"
    )


def alert_message(
    alerts: list[str], perf_lines: str, paper_mode: bool = False, summary: dict[str, Any] | None = None
) -> str:
    """Build tactical Telegram HTML risk-guard notification card."""
    hdr = "[PAPER GUARD // 模擬風控]" if paper_mode else "[RISK GUARD // 風控觸發]"
    body = "\n".join(f"▸ {_esc(a)}" for a in alerts)
    return f"<b>{hdr}</b>\n{RULE}\n{body}\n{RULE}\n{_footer_html(perf_lines, summary)}"


def status_message(mode_hdr: str, state_label: str, pos_line: str, coin_lines: list[str], perf_lines: str) -> str:
    """Build clean plain-Unicode tactical `/sniper` status summary (safe without HTML parse_mode)."""
    coins_blk = "\n".join(f"▸ {line}" for line in coin_lines)
    return (
        f"[SNIPER STATUS // {mode_hdr} · {state_label}]\n{RULE}\n"
        f"▸ 持倉狀態：{pos_line}\n{coins_blk}\n{RULE}\n{perf_lines}"
    )
