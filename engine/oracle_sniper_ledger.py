"""Persistent ledger, daily-loss breaker and edge guards for the Chainlink sniper (pure, no network)."""
from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import statistics
import time
from typing import Any, Optional

ROOT_DIR = Path(__file__).resolve().parent.parent
STATE_PATH = ROOT_DIR / "data" / "oracle_sniper_state.json"
PAPER_STATE_PATH = ROOT_DIR / "data" / "oracle_sniper_paper_43u.json"
PAPER_40U_STATE_PATH = ROOT_DIR / "data" / "oracle_sniper_paper_40u.json"
MAX_TRADES_KEPT = 500
SYMBOL_ORDER = ("BTCUSDT", "ETHUSDT", "SOLUSDT")


def local_date_str(ts: Optional[float] = None, utc: bool = False) -> str:
    """Calendar date string (local by default, or UTC when `utc=True`)."""
    t = ts if ts is not None else time.time()
    return time.strftime("%Y-%m-%d", time.gmtime(t) if utc else time.localtime(t))


@dataclass(frozen=True)
class SniperRiskPolicy:
    """Configurable daily/rolling circuit-breaker rules for the sniper ledger."""

    daily_loss_pct: float = 0.16
    daily_loss_floor_u: float = 4.5
    fixed_daily_loss_cap_u: Optional[float] = None
    consec_loss_day_halt: int = 0
    rolling_window: int = 20
    global_min_win_rate: float = 0.72
    rolling_be_buffer: Optional[float] = None
    rolling_halt_permanent: bool = False
    symbol_min_trades: int = 10
    symbol_min_win_rate: float = 0.75
    pause_seconds: int = 3 * 3600
    use_utc_day: bool = False
    paper_initial_equity_u: Optional[float] = None


PRODUCTION_RISK_POLICY = SniperRiskPolicy(
    fixed_daily_loss_cap_u=10.0,
    consec_loss_day_halt=3,
    rolling_window=50,
    rolling_be_buffer=0.02,
    rolling_halt_permanent=True,
    use_utc_day=True,
)

PAPER_43U_RISK_POLICY = SniperRiskPolicy(
    fixed_daily_loss_cap_u=10.0,
    consec_loss_day_halt=3,
    rolling_window=50,
    rolling_be_buffer=0.02,
    rolling_halt_permanent=True,
    use_utc_day=True,
    paper_initial_equity_u=43.0,
)

PAPER_40U_RISK_POLICY = SniperRiskPolicy(
    fixed_daily_loss_cap_u=10.0,
    consec_loss_day_halt=3,
    rolling_window=50,
    rolling_be_buffer=0.02,
    rolling_halt_permanent=True,
    use_utc_day=True,
    paper_initial_equity_u=40.0,
)


def wilson_lower_bound(wins: int, n: int, z: float = 1.96) -> float:
    """Lower bound of the Wilson score interval (0.0 when n == 0)."""
    if n <= 0:
        return 0.0
    p = wins / n
    den = 1.0 + z * z / n
    centre = p + z * z / (2 * n)
    half = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5)
    return max(0.0, (centre - half) / den)


class SniperLedger:
    """JSON-backed multi-slot trade ledger plus daily-loss / consec-loss / rolling BE+2pp circuit breakers."""

    def __init__(self, path: Path = STATE_PATH, policy: SniperRiskPolicy = SniperRiskPolicy()) -> None:
        self.path = path
        self.policy = policy
        self.daily_loss_pct = policy.daily_loss_pct
        self.daily_loss_floor_u = policy.daily_loss_floor_u
        self.rolling_window = policy.rolling_window
        self.global_min_win_rate = policy.global_min_win_rate
        self.symbol_min_trades = policy.symbol_min_trades
        self.symbol_min_win_rate = policy.symbol_min_win_rate
        self.pause_seconds = policy.pause_seconds
        self.state: dict[str, Any] = self._load()

    def _today(self) -> str:
        return local_date_str(utc=self.policy.use_utc_day)

    # ----- persistence -----
    def _fresh(self) -> dict[str, Any]:
        init_u = float(self.policy.paper_initial_equity_u or 0.0)
        return {
            "version": 2,
            "enabled": True,
            "halt_reason": "",
            "started_at_ms": int(time.time() * 1000),
            "initial_equity_u": init_u,
            "peak_equity_u": init_u,
            "vault_locked_u": 0.0,
            "paper_cash_u": init_u,
            "daily": {"date": self._today(), "start_equity_u": init_u, "pnl_usd": 0.0, "trades": 0, "consec_losses": 0, "halted": False, "halt_reason": ""},
            "pause": {"global_until_ms": 0, "reason": "", "symbol_until_ms": {s: 0 for s in SYMBOL_ORDER}},
            "open_position": None,
            "open_positions": {},
            "pending_entry": None,
            "settling": [],
            "trades": [],
            "attempts": [],
            "skips": {},
        }

    def paper_debit(self, amount_u: float) -> float:
        """Deduct cost from simulated paper cash balance atomically."""
        cur = float(self.state.get("paper_cash_u", self.policy.paper_initial_equity_u or 0.0))
        nxt = round(max(0.0, cur - float(amount_u)), 4)
        self.state["paper_cash_u"] = nxt
        self.save()
        return nxt

    def paper_credit(self, amount_u: float) -> float:
        """Credit settlement/exit proceeds to simulated paper cash balance atomically."""
        cur = float(self.state.get("paper_cash_u", self.policy.paper_initial_equity_u or 0.0))
        nxt = round(cur + float(amount_u), 4)
        self.state["paper_cash_u"] = nxt
        self.save()
        return nxt

    def _merge_defaults(self, raw: dict[str, Any]) -> dict[str, Any]:
        merged = self._fresh()
        for key, value in raw.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = {**merged[key], **value}
            else:
                merged[key] = value
        if not merged["open_positions"] and isinstance(merged.get("open_position"), dict):
            sym = str(merged["open_position"].get("symbol") or "")
            if sym:
                merged["open_positions"] = {sym: dict(merged["open_position"])}
        symbols = merged["pause"].get("symbol_until_ms")
        merged["pause"]["symbol_until_ms"] = {s: int((symbols or {}).get(s, 0)) for s in SYMBOL_ORDER}
        return merged

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return self._fresh()
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise ValueError("state root is not an object")
            return self._merge_defaults(raw)
        except (OSError, ValueError) as exc:
            backup = self.path.with_name(f"{self.path.name}.corrupt-{int(time.time())}")
            try:
                os.replace(self.path, backup)
            except OSError as move_exc:
                print(f"[Sniper Ledger ALERT] could not move corrupt state aside: {move_exc}")
            print(f"[Sniper Ledger ALERT] state unreadable ({exc}); backed up to {backup.name}; entries DISABLED")
            fresh = self._fresh()
            fresh["enabled"] = False  # never resume trading with lost guards / positions
            fresh["load_alert"] = f"state file corrupt, backed up as {backup.name}"
            return fresh

    def save(self) -> None:
        """Atomically persist the ledger."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, self.path)

    # ----- switches / baselines -----
    @property
    def enabled(self) -> bool:
        """Master live-trading switch."""
        return bool(self.state.get("enabled", True))

    def set_enabled(self, enabled: bool) -> None:
        """Flip the master switch and persist."""
        self.state["enabled"] = bool(enabled)
        self.save()

    def ensure_baseline(self, equity_u: float) -> None:
        """Set the initial-equity baseline once, the first time a wallet reading is available."""
        if float(self.state.get("initial_equity_u", 0.0)) <= 0.0 and equity_u > 0.0:
            self.state["initial_equity_u"] = round(equity_u, 2)
            self.state["peak_equity_u"] = round(equity_u, 2)
            self.state["daily"]["start_equity_u"] = round(equity_u, 2)
            self.save()

    def note_equity(self, equity_u: float) -> None:
        """Track the equity high-water mark used by the vault ratchet."""
        if equity_u > float(self.state.get("peak_equity_u", 0.0)):
            self.state["peak_equity_u"] = round(equity_u, 2)

    # ----- daily breaker -----
    def roll_day_if_needed(self, equity_u: float) -> bool:
        """Reset daily counters when calendar date changes. Returns True when a new day started."""
        today = self._today()
        daily = self.state["daily"]
        if daily.get("date") == today:
            return False
        self.state["daily"] = {
            "date": today, "start_equity_u": round(equity_u, 2), "pnl_usd": 0.0,
            "trades": 0, "consec_losses": 0, "halted": False, "halt_reason": "",
        }
        self.save()
        return True

    def daily_loss_limit_u(self) -> float:
        """Max realised loss per day (`$10U` fixed in Section 3 policy, else 16% equity >= `$4.50`)."""
        if self.policy.fixed_daily_loss_cap_u is not None:
            return float(self.policy.fixed_daily_loss_cap_u)
        start = float(self.state["daily"].get("start_equity_u", 0.0))
        return max(self.daily_loss_floor_u, start * self.daily_loss_pct)

    def pending_exposure_u(self) -> float:
        """Cost of positions that are open or awaiting settlement (worst case: all lose)."""
        held = [t for t in self.state.get("settling", [])]
        for op in (self.state.get("open_positions") or {}).values():
            held.append(op)
        if not held and self.state.get("open_position"):
            held.append(self.state["open_position"])
        if self.state.get("pending_entry"):
            held.append({"cost_usd": float(self.state["pending_entry"].get("est_cost_usd", 0.0))})
        return sum(float(t.get("cost_usd", 0.0)) for t in held)

    # ----- guards -----
    def guard_reason(self, now_ms: Optional[int] = None) -> Optional[str]:
        """Why new entries are currently blocked (None when trading is allowed)."""
        now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        if not self.enabled:
            return str(self.state.get("halt_reason") or "DISABLED")
        if self.state["daily"].get("halted"):
            return str(self.state["daily"].get("halt_reason") or "DAILY_LOSS_LIMIT")
        if float(self.state["daily"].get("pnl_usd", 0.0)) - self.pending_exposure_u() <= -self.daily_loss_limit_u():
            return "DAILY_LOSS_LIMIT_PENDING"
        if int(self.state["pause"].get("global_until_ms", 0)) > now_ms:
            return str(self.state["pause"].get("reason") or "GLOBAL_PAUSE")
        return None

    def symbol_paused(self, symbol: str, now_ms: Optional[int] = None) -> bool:
        """True while `symbol` is serving an edge-guard cooldown."""
        now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        return int(self.state["pause"]["symbol_until_ms"].get(symbol, 0)) > now_ms

    def _closed(self, symbol: Optional[str] = None) -> list[dict[str, Any]]:
        out = [t for t in self.state["trades"] if t.get("status") == "CLOSED"]
        return [t for t in out if symbol is None or t.get("symbol") == symbol]

    def _apply_edge_guards(self, symbol: str) -> list[str]:
        """Apply per-symbol pause and rolling break-even+2pp / floor global halts."""
        alerts: list[str] = []
        now_ms = int(time.time() * 1000)
        until = now_ms + int(self.pause_seconds * 1000)
        sym_recent = self._closed(symbol)[: self.rolling_window]
        if len(sym_recent) >= self.symbol_min_trades:
            wr = sum(1 for t in sym_recent if t.get("won")) / len(sym_recent)
            if wr < self.symbol_min_win_rate and not self.symbol_paused(symbol, now_ms):
                self.state["pause"]["symbol_until_ms"][symbol] = until
                alerts.append(f"{symbol} 近 {len(sym_recent)} 單勝率 {wr:.0%} < {self.symbol_min_win_rate:.0%}，該幣暫停 3 小時")
        recent = self._closed()[: self.rolling_window]
        if len(recent) < self.rolling_window:
            return alerts
        wr = sum(1 for t in recent if t.get("won")) / len(recent)
        if self.policy.rolling_be_buffer is not None:
            be_wr = sum(min(0.98, float(t.get("cost_per_share") or 0.78)) for t in recent) / len(recent)
            floor = be_wr + float(self.policy.rolling_be_buffer)
            if wr < floor:
                if self.policy.rolling_halt_permanent:
                    self.state["enabled"] = False
                    self.state["halt_reason"] = "ROLLING_50_BE_HALT"
                else:
                    self.state["pause"].update(global_until_ms=until, reason="ROLLING_WIN_RATE_PAUSE")
                alerts.append(f"近 {len(recent)} 單勝率 {wr:.1%} < BE+2pp ({floor:.1%})，觸發全面停機")
            return alerts
        if wr < self.global_min_win_rate and int(self.state["pause"]["global_until_ms"]) <= now_ms:
            self.state["pause"].update(global_until_ms=until, reason="ROLLING_WIN_RATE_PAUSE")
            alerts.append(f"全幣近 {len(recent)} 單勝率 {wr:.0%} < {self.global_min_win_rate:.0%}，暫停新開單 3 小時")
        return alerts

    def _sync_primary_open(self) -> None:
        ops = self.state.setdefault("open_positions", {})
        self.state["open_position"] = dict(next(iter(ops.values()))) if ops else None

    # ----- trades -----
    def set_pending_entry(self, intent: Optional[dict[str, Any]]) -> None:
        """Write-ahead record of an order about to be sent (None clears it)."""
        self.state["pending_entry"] = intent
        self.save()

    def open_trade(self, trade: dict[str, Any]) -> None:
        """Store a freshly filled entry keyed by symbol (supporting up to 3 concurrent coins)."""
        trade = {**trade, "status": "OPEN"}
        self.state.setdefault("open_positions", {})[str(trade["symbol"])] = trade
        self._sync_primary_open()
        self.save()

    def mark_settling(self, trade: dict[str, Any]) -> None:
        """Move an expired-but-unresolved trade out of open positions so it does not block a new window."""
        self.state.setdefault("settling", [])
        self.state["settling"] = [t for t in self.state["settling"] if t.get("id") != trade.get("id")] + [dict(trade)]
        ops = self.state.setdefault("open_positions", {})
        sym = str(trade.get("symbol") or "")
        if sym in ops and ops[sym].get("id") == trade.get("id"):
            ops.pop(sym, None)
        self._sync_primary_open()
        self.save()

    def close_trade(self, trade: dict[str, Any], exit_info: dict[str, Any]) -> list[str]:
        """Finalise a trade, update daily PnL / consec losses, and evaluate circuit breakers."""
        closed = {**trade, **exit_info, "status": "CLOSED", "exit_ts": exit_info.get("exit_ts", time.time())}
        closed["won"] = float(closed.get("pnl_usd", 0.0)) > 0.0
        self.state["trades"].insert(0, closed)
        self.state["trades"].sort(key=lambda t: float(t.get("exit_ts", 0.0)), reverse=True)
        self.state["trades"] = self.state["trades"][:MAX_TRADES_KEPT]
        ops = self.state.setdefault("open_positions", {})
        sym = str(trade.get("symbol") or "")
        if sym in ops and ops[sym].get("id") == trade.get("id"):
            ops.pop(sym, None)
        self._sync_primary_open()
        self.state["settling"] = [t for t in self.state.get("settling", []) if t.get("id") != trade.get("id")]
        daily = self.state["daily"]
        daily["pnl_usd"] = round(float(daily.get("pnl_usd", 0.0)) + float(closed["pnl_usd"]), 4)
        daily["trades"] = int(daily.get("trades", 0)) + 1
        daily["consec_losses"] = 0 if closed["won"] else (int(daily.get("consec_losses", 0)) + 1)
        alerts = self._apply_edge_guards(sym)
        if self.policy.consec_loss_day_halt > 0 and daily["consec_losses"] >= self.policy.consec_loss_day_halt and not daily.get("halted"):
            daily["halted"] = True
            daily["halt_reason"] = f"CONSEC_LOSS_{self.policy.consec_loss_day_halt}_HALT"
            alerts.append(f"今日已連輸 {daily['consec_losses']} 單，依規範當日停止新開單")
        if daily["pnl_usd"] <= -self.daily_loss_limit_u() and not daily.get("halted"):
            daily["halted"] = True
            daily["halt_reason"] = "DAILY_LOSS_LIMIT"
            alerts.append(f"今日已實現虧損 ${daily['pnl_usd']:.2f}U 觸及上限 -${self.daily_loss_limit_u():.2f}U，今日停止新開單")
        self.save()
        return alerts

    def record_attempt(self, attempt: dict[str, Any]) -> None:
        """Log filled or missed attempt (actual vs sim ask slippage + execution latency ms)."""
        attempts = self.state.setdefault("attempts", [])
        attempts.insert(0, attempt)
        self.state["attempts"] = attempts[:MAX_TRADES_KEPT]

    def generate_daily_report(self, ref_42_mean: Optional[float] = None, date_str: Optional[str] = None) -> dict[str, Any]:
        """Daily operational report: n, PnL, slippage vs sim, miss rate, median latency, gap vs 4.2."""
        day = date_str or self._today()
        day_trades = [t for t in self._closed() if local_date_str(float(t.get("exit_ts", 0.0)), utc=self.policy.use_utc_day) == day]
        day_atts = [a for a in self.state.get("attempts", []) if local_date_str(float(a.get("ts", 0.0)), utc=self.policy.use_utc_day) == day]
        misses = sum(1 for a in day_atts if not a.get("filled"))
        lats = [float(a["latency_ms"]) for a in day_atts if a.get("latency_ms") is not None]
        slips = [round((float(t["entry_price"]) - float(t["sim_ask"])) * 100.0, 3) for t in day_trades if t.get("sim_ask") is not None]
        pnl_sh = [float(t["pnl_usd"]) / max(float(t.get("shares") or 1.0), 1e-6) for t in day_trades]
        mean_sh = (sum(pnl_sh) / len(pnl_sh)) if pnl_sh else None
        return {
            "date": day,
            "n_closed": len(day_trades),
            "wins": sum(1 for t in day_trades if t.get("won")),
            "pnl_usd": round(sum(float(t.get("pnl_usd", 0.0)) for t in day_trades), 4),
            "mean_pnl_per_share": round(mean_sh, 5) if mean_sh is not None else None,
            "gap_vs_42": round(mean_sh - ref_42_mean, 5) if (mean_sh is not None and ref_42_mean is not None) else None,
            "attempts_total": len(day_atts),
            "miss_rate": round(misses / len(day_atts), 4) if day_atts else None,
            "mean_slippage_vs_sim_cents": round(sum(slips) / len(slips), 3) if slips else None,
            "median_latency_ms": round(statistics.median(lats), 1) if lats else None,
        }

    def bump_skip(self, reason: str) -> None:
        """Count a skipped/blocked opportunity for observability."""
        skips = self.state["skips"]
        skips[reason] = int(skips.get(reason, 0)) + 1

    # ----- reporting -----
    @staticmethod
    def _current_streak(closed: list[dict[str, Any]]) -> tuple[str, int]:
        if not closed:
            return "", 0
        first_won = bool(closed[0].get("won"))
        cnt = 0
        for item in closed:
            if bool(item.get("won")) != first_won:
                break
            cnt += 1
        return ("W" if first_won else "L"), cnt

    def stats(self) -> dict[str, Any]:
        """Aggregate closed-trade statistics including active win/loss streak."""
        closed = self._closed()
        wins = sum(1 for t in closed if t.get("won"))
        n = len(closed)
        by_sym: dict[str, dict[str, Any]] = {}
        for sym in SYMBOL_ORDER:
            sc = [t for t in closed if t.get("symbol") == sym]
            by_sym[sym] = {
                "trades": len(sc), "wins": sum(1 for t in sc if t.get("won")),
                "pnl_usd": round(sum(float(t.get("pnl_usd", 0.0)) for t in sc), 4),
            }
        recent = closed[: self.rolling_window]
        s_type, s_len = self._current_streak(closed)
        return {
            "trades": n, "wins": wins, "losses": n - wins,
            "win_rate_pct": round(wins / n * 100.0, 2) if n else 0.0,
            "wilson_lower_pct": round(wilson_lower_bound(wins, n) * 100.0, 1),
            "net_pnl_usd": round(sum(float(t.get("pnl_usd", 0.0)) for t in closed), 4),
            "rolling_win_rate_pct": round(sum(1 for t in recent if t.get("won")) / len(recent) * 100.0, 1) if recent else None,
            "streak_type": s_type, "streak_len": s_len,
            "by_symbol": by_sym,
            "daily": dict(self.state["daily"]),
            "skips": dict(self.state["skips"]),
        }

    def performance_summary(self, equity_u: float, cash_u: float, vault_u: float) -> dict[str, Any]:
        """Structured equity, record, and streak summary for tactical notification cards."""
        init_u = float(self.state.get("initial_equity_u", 0.0)) or equity_u
        net_u = equity_u - init_u
        pct = net_u / max(init_u, 0.01) * 100.0
        st = self.stats()
        return {
            "equity_u": equity_u, "cash_u": cash_u, "vault_u": vault_u,
            "initial_u": init_u, "net_u": net_u, "pct": pct,
            "wins": st["wins"], "losses": st["losses"], "trades": st["trades"],
            "win_rate_pct": st["win_rate_pct"],
            "daily_pnl_usd": float(st["daily"].get("pnl_usd", 0.0)),
            "streak_type": st["streak_type"], "streak_len": st["streak_len"],
            "by_symbol": st["by_symbol"],
        }

    def performance_lines(self, equity_u: float, cash_u: float, vault_u: float) -> str:
        """Plain two-line overview (equity / record) formatted for tactical status cards."""
        sm = self.performance_summary(equity_u, cash_u, vault_u)
        wr = f"{sm['win_rate_pct']:.1f}%" if sm["trades"] else "待首單"
        by = sm["by_symbol"]
        sym_txt = " | ".join(f"{s[:3]} {by[s]['wins']}/{by[s]['trades']}" for s in SYMBOL_ORDER if by[s]["trades"])
        streak = f"{sm['streak_len']} 連{'勝' if sm['streak_type'] == 'W' else '敗'}" if sm["streak_len"] > 0 else "--"
        return (
            f"▸ 累計戰績：{sm['wins']}W - {sm['losses']}L (勝率 {wr}) | 權益 ${equity_u:.2f}"
            f"{f' | 鎖倉 ${vault_u:.0f}' if vault_u >= 1.0 else ''}\n"
            f"▸ 累計損益：{sm['net_u']:+.2f} pUSD ({sm['pct']:+.1f}%) | {streak}"
            f"{f' | {sym_txt}' if sym_txt else ''}"
        )
