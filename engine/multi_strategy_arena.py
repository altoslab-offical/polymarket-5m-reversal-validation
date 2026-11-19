"""
engine/multi_strategy_arena.py
多策略平行競技場 (Multi-Strategy Parallel Arena)
負責在模擬環境下同時並行 4 組頂級候選策略沙盒，獨立記帳、撮合與計算 CAS 複合阿爾法指數。
"""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT_DIR = Path(__file__).resolve().parent.parent
ARENA_STATE_PATH = ROOT_DIR / "data" / "arena_state.json"


class StrategyTrack(str, Enum):
    ALPHA = "ALPHA"  # V6.6 旗艦極致保本型（現行基準，極度穩健）
    BETA = "BETA"    # V6.7 多幣獨立＋動態 ATR 靈敏型（高流速主戰略）
    GAMMA = "GAMMA"  # V6.8 均衡動能共振型（拒絕影線 Wick>=0.12 摸底）
    DELTA = "DELTA"  # V6.9 突破順勢擠壓型（布林帶擠壓後放量，Maker <= 55¢）


class OrderDirection(str, Enum):
    BUY_UP = "BUY_UP"
    BUY_DOWN = "BUY_DOWN"


@dataclass
class ArenaTradeRecord:
    trade_id: str
    track: str
    symbol: str
    direction: str
    step: int
    entry_time: float
    entry_price: float
    shares: float
    cost: float
    exit_time: Optional[float] = None
    exit_price: Optional[float] = None
    net_pnl: float = 0.0
    exit_reason: str = ""  # "EARLY_LOCK_85", "EARLY_LOCK_90", "EXPIRY_100", "EXPIRY_0", "TIME_STOP_90M"


@dataclass
class StrategyLedger:
    track: str
    name: str
    description: str
    cash: float = 80.00
    equity: float = 80.00
    peak_equity: float = 80.00
    max_drawdown_pct: float = 0.0
    sequence_count: int = 0
    sequence_wins: int = 0
    sequence_losses: int = 0
    step1_wins: int = 0
    step2_wins: int = 0
    step3_wins: int = 0
    step4_wins: int = 0
    time_stop_losses: int = 0
    active_sequence_step: int = 0
    active_sequence_cost: float = 0.0
    cas_score: float = 0.0
    trades: List[Dict[str, Any]] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)

    def update_metrics(self) -> None:
        """即時計算最大回撤與 CAS 複合阿爾法指數 (Composite Alpha Score)"""
        if self.equity > self.peak_equity:
            self.peak_equity = self.equity
        dd = (self.peak_equity - self.equity) / self.peak_equity if self.peak_equity > 0 else 0.0
        if dd > self.max_drawdown_pct:
            self.max_drawdown_pct = dd

        # CAS (Composite Alpha Score) 官方精算法
        hours_elapsed = max(0.5, (time.time() - self.start_time) / 3600.0)
        cagr_norm = ((self.equity - 80.0) / 80.0) * (24.0 / hours_elapsed)
        p_step1 = (self.step1_wins / max(1, self.sequence_count))
        mdd_clamped = max(0.015, self.max_drawdown_pct)

        # 深度懲罰項（越依賴深層馬丁或爆倉，懲罰越重）
        depth_penalty = (
            (self.step2_wins * 0.15)
            + (self.step3_wins * 0.45)
            + (self.step4_wins * 1.50)
            + (self.sequence_losses * 4.0)
            + (self.time_stop_losses * 0.5)
        ) / max(1, self.sequence_count)

        if cagr_norm <= 0:
            self.cas_score = 0.0
        else:
            self.cas_score = round(
                (cagr_norm * (p_step1 ** 2) / (mdd_clamped * (1.0 + depth_penalty))) * 100.0,
                2,
            )


class MultiStrategyArena:
    def __init__(self) -> None:
        self.ledgers: Dict[str, StrategyLedger] = self._load_or_init()
        if not ARENA_STATE_PATH.exists():
            self.save()

    def _load_or_init(self) -> Dict[str, StrategyLedger]:
        defaults = {
            StrategyTrack.ALPHA.value: StrategyLedger(
                track=StrategyTrack.ALPHA.value,
                name="V6.6 旗艦極致保本型",
                description="單通道經典四引擎共振，純收盤 100¢ 或剩餘 <=120s 鎖盈，極度穩健",
            ),
            StrategyTrack.BETA.value: StrategyLedger(
                track=StrategyTrack.BETA.value,
                name="V6.7 多幣獨立靈敏型",
                description="三幣獨立通道 + 動態 ATR 適應 + 90m 強制認賠重置，高流速主戰略",
            ),
            StrategyTrack.GAMMA.value: StrategyLedger(
                track=StrategyTrack.GAMMA.value,
                name="V6.8 均衡動能共振型",
                description="大拒絕影線 (Wick>=0.12) 精準 2 連 K 摸底，兼顧勝率與頻次",
            ),
            StrategyTrack.DELTA.value: StrategyLedger(
                track=StrategyTrack.DELTA.value,
                name="V6.9 突破順勢擠壓型",
                description="布林帶頻寬擠壓後放量突破，Maker <= 55¢ 極速埋伏",
            ),
        }
        if ARENA_STATE_PATH.exists():
            try:
                data = json.loads(ARENA_STATE_PATH.read_text(encoding="utf-8"))
                for k, v in data.items():
                    if k in defaults:
                        defaults[k] = StrategyLedger(**v)
            except Exception:
                pass
        return defaults

    def save(self) -> None:
        ARENA_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        dumpable = {k: asdict(v) for k, v in self.ledgers.items()}
        ARENA_STATE_PATH.write_text(json.dumps(dumpable, indent=2, ensure_ascii=False), encoding="utf-8")

    def record_time_stop(self, symbol: str, loss_amount: float) -> None:
        """看門狗 90m 超時強制認賠時，同步記帳至 BETA/GAMMA 策略"""
        for t in [StrategyTrack.BETA.value, StrategyTrack.GAMMA.value]:
            ledger = self.ledgers.get(t)
            if ledger:
                ledger.equity = round(max(10.0, ledger.equity - loss_amount), 2)
                ledger.cash = ledger.equity
                ledger.sequence_count += 1
                ledger.sequence_losses += 1
                ledger.time_stop_losses += 1
                ledger.active_sequence_step = 0
                ledger.active_sequence_cost = 0.0
                rec = {
                    "trade_id": f"time_stop_{int(time.time()*1000)}",
                    "track": t,
                    "symbol": symbol,
                    "direction": "FORCE_RESET",
                    "step": 1,
                    "entry_time": time.time(),
                    "entry_price": 0.51,
                    "shares": round(loss_amount / 0.51, 1),
                    "cost": loss_amount,
                    "exit_time": time.time(),
                    "exit_price": 0.0,
                    "net_pnl": -loss_amount,
                    "exit_reason": "TIME_STOP_90M",
                }
                ledger.trades.insert(0, rec)
                ledger.trades = ledger.trades[:50]
                ledger.update_metrics()
        self.save()

    def process_sequence_win(
        self,
        track: str,
        symbol: str,
        direction: str,
        step: int,
        net_profit: float,
        exit_type: str,
        cost: float,
        shares: float,
    ) -> None:
        """記錄某策略獲勝完成一輪序列"""
        ledger = self.ledgers.get(track)
        if not ledger:
            return
        ledger.equity = round(ledger.equity + net_profit, 2)
        ledger.cash = ledger.equity
        ledger.sequence_count += 1
        ledger.sequence_wins += 1
        if step == 1:
            ledger.step1_wins += 1
        elif step == 2:
            ledger.step2_wins += 1
        elif step == 3:
            ledger.step3_wins += 1
        elif step >= 4:
            ledger.step4_wins += 1

        ledger.active_sequence_step = 0
        ledger.active_sequence_cost = 0.0

        rec = {
            "trade_id": f"trade_{int(time.time()*1000)}",
            "track": track,
            "symbol": symbol,
            "direction": direction,
            "step": step,
            "entry_time": time.time() - 300,
            "entry_price": 0.51,
            "shares": shares,
            "cost": cost,
            "exit_time": time.time(),
            "exit_price": 1.0 if "100" in exit_type else (0.85 if "85" in exit_type else 0.90),
            "net_pnl": net_profit,
            "exit_reason": exit_type,
        }
        ledger.trades.insert(0, rec)
        ledger.trades = ledger.trades[:50]
        ledger.update_metrics()
        self.save()

    def get_leaderboard(self) -> List[Dict[str, Any]]:
        """回傳依照 CAS 阿爾法指數排序的排行榜清單"""
        results = []
        for track_val, ledger in self.ledgers.items():
            ledger.update_metrics()
            results.append({
                "track": track_val,
                "name": ledger.name,
                "description": ledger.description,
                "equity": round(ledger.equity, 2),
                "peak_equity": round(ledger.peak_equity, 2),
                "roi_pct": round(((ledger.equity - 80.0) / 80.0) * 100.0, 2),
                "sequence_wins": ledger.sequence_wins,
                "sequence_losses": ledger.sequence_losses,
                "sequence_count": ledger.sequence_count,
                "win_rate_pct": round((ledger.sequence_wins / max(1, ledger.sequence_count)) * 100.0, 1),
                "step1_win_rate_pct": round((ledger.step1_wins / max(1, ledger.sequence_count)) * 100.0, 1),
                "step1_wins": ledger.step1_wins,
                "step2_wins": ledger.step2_wins,
                "step3_wins": ledger.step3_wins,
                "step4_wins": ledger.step4_wins,
                "time_stop_losses": ledger.time_stop_losses,
                "max_drawdown_pct": round(ledger.max_drawdown_pct * 100.0, 2),
                "cas_score": ledger.cas_score,
                "active_step": ledger.active_sequence_step,
                "target_progress_pct": round(min(100.0, ((ledger.equity - 80.0) / 120.0) * 100.0), 1),
                "status": "RUNNING",
                "recent_trades": ledger.trades[:5],
            })
        results.sort(key=lambda x: x["cas_score"], reverse=True)
        return results
