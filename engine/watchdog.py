"""
engine/watchdog.py
極輕量常駐後台看門狗守護程序 (Proactive Lightweight Watchdog)
純 Python 異步守護程序，完全零 LLM Token 消耗（絕對不調用 Argon 或 Claude！）。
負責 30 秒巡檢：
1. 掃描器即時心跳監控（防止連線或異步任務假死，自動喚醒）；
2. 每天晚上 10 點 (22:00) 定時向 Telegram 推播當日策略總結（平時開關單與盤中完全靜音，絕不亂熔斷或強制認賠）。
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from pathlib import Path
import time
from typing import Any, Optional

logger = logging.getLogger("Watchdog")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)

WATCHDOG_STATE_PATH = Path("data/watchdog_state.json")


class SystemWatchdog:
    def __init__(
        self,
        risk_controller: Any = None,
        arena: Any = None,
        notifier: Any = None,
        scanner: Any = None,
    ) -> None:
        self.risk = risk_controller
        self.arena = arena
        self.notifier = notifier
        self.scanner = scanner
        self.last_review_time = time.time()
        self.started_at = time.time()
        self.last_heartbeat = time.time()
        self.last_daily_summary_date: str = ""
        self.last_daily_summary_sent_at: str = ""
        self._load_watchdog_state()
        self.running = False
        self._task: Optional[asyncio.Task] = None
        self.warned_lanes: set[str] = set()

    def _load_watchdog_state(self) -> None:
        if WATCHDOG_STATE_PATH.exists():
            try:
                data = json.loads(WATCHDOG_STATE_PATH.read_text(encoding="utf-8"))
                self.last_daily_summary_date = str(data.get("last_daily_summary_date", ""))
                self.last_daily_summary_sent_at = str(data.get("last_daily_summary_sent_at", ""))
            except Exception as exc:
                logger.warning(f"⚠️ [Watchdog] 讀取狀態檔失敗: {exc}")

    def _save_watchdog_state(self) -> None:
        try:
            WATCHDOG_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            WATCHDOG_STATE_PATH.write_text(
                json.dumps(
                    {
                        "last_daily_summary_date": self.last_daily_summary_date,
                        "last_daily_summary_sent_at": self.last_daily_summary_sent_at,
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
        except Exception as e:
            logger.warning(f"⚠️ [Watchdog] 儲存看門狗狀態檔失敗: {e}")

    async def start(self) -> None:
        if self.running:
            return
        self.running = True
        self._task = asyncio.create_task(self._watchdog_loop())
        msg = "🛡️ [Watchdog] 極輕量常駐看門狗守護進程已啟動 (純 Python 零 Token 消耗，輪詢間隔: 30s)"
        logger.info(msg)
        print(msg, flush=True)

    async def stop(self) -> None:
        self.running = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        logger.info("🛡️ [Watchdog] 看門狗已優雅停止")

    async def _watchdog_loop(self) -> None:
        while self.running:
            try:
                await self._run_health_check()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ [Watchdog] 巡檢異常: {e}", exc_info=True)
            await asyncio.sleep(30)

    async def _run_health_check(self) -> None:
        now = time.time()
        self.last_heartbeat = now

        # 1. 檢查掃描器心跳與主任務狀態 (Scanner Heartbeat & Stuck Task Recovery)
        if self.scanner:
            last_scan = getattr(self.scanner, "_last_scan_time", now)
            scan_task = getattr(self.scanner, "_task", None)
            task_dead = bool(getattr(self.scanner, "running", False) and (scan_task is None or scan_task.done()))
            if now - last_scan > 45.0 or task_dead:
                logger.warning(
                    f"⚠️ [Watchdog] 掃描器已有 {now - last_scan:.1f} 秒未發出心跳 (task_dead={task_dead})，重建 run_loop 並立即喚醒..."
                )
                try:
                    if hasattr(self.scanner, "restart_loop"):
                        self.scanner.restart_loop()
                    await asyncio.wait_for(self.scanner.scan_once(), timeout=15.0)
                except Exception as e:
                    logger.error(f"❌ [Watchdog] 主動喚醒掃描器失敗: {e}")

            cl_feed = getattr(self.scanner, "chainlink_feed", None)
            if cl_feed is not None and getattr(self.scanner, "running", False):
                cl_st = cl_feed.get_status()
                tick_age = cl_st.get("last_tick_age_sec")
                cl_task = getattr(cl_feed, "_task", None)
                cold_start_timeout = (now - getattr(self, "started_at", 0.0) > 20.0) and (
                    not cl_st.get("connected") and int(cl_st.get("total_ticks", 0)) == 0
                )
                if (
                    (cl_task is None or cl_task.done())
                    or (tick_age is not None and float(tick_age) > 35.0)
                    or cold_start_timeout
                ):
                    logger.warning(
                        f"⚠️ [Watchdog] Chainlink RTDS 斷線/延遲 (connected={cl_st.get('connected')}, tick_age={tick_age}s)，自動重連..."
                    )
                    cl_feed.stop()
                    cl_feed.start()

        # 2. 每天晚上 10 點 (22:00) 準時向 Telegram 推播當日策略總結（平時絕不發送任何干擾或強制認賠通知）
        from datetime import datetime
        now_dt = datetime.now()
        today_str = now_dt.strftime("%Y-%m-%d")
        if now_dt.hour == 22 and self.last_daily_summary_date != today_str:
            msg = f"🌙 [Watchdog] 觸發每日 22:00 策略總結推播 (日期: {today_str})"
            logger.info(msg)
            print(msg, flush=True)
            report_text = await self.send_daily_strategy_summary()
            if report_text:
                self.last_daily_summary_date = today_str
                self.last_daily_summary_sent_at = now_dt.strftime("%Y-%m-%d %H:%M:%S")
                self._save_watchdog_state()

    async def send_daily_strategy_summary(self, dry_render: bool = False) -> str:
        """
        每天晚上 22:00 (晚上 10 點) 自動生成並向 Telegram 發送策略總結（模擬模式附帶競技場排行榜，實盤模式純淨顯示真實戰績）
        """
        if not dry_render and not self.notifier:
            return ""

        from datetime import datetime
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

        paper_st = self.scanner.paper_trader.state if (self.scanner and hasattr(self.scanner, "paper_trader")) else {}
        total_seq = int(paper_st.get("total_sequences", 0))
        won_seq = int(paper_st.get("won_sequences", 0))
        busts = int(paper_st.get("busts", 0))
        win_rate = (won_seq / max(1, total_seq)) * 100.0 if total_seq > 0 else 0.0
        win_rate_label = f"{win_rate:.1f}% ({won_seq}勝/{busts}負)" if total_seq > 0 else "0.0% (待首筆)"

        real_wallet = self.scanner.auto_trader.live_wallet_state if (self.scanner and hasattr(self.scanner, "auto_trader")) else {}
        fallback_cash = float(paper_st.get("live_cash_pusd", 0.0))
        real_cash = float(real_wallet.get("cash_pusd", fallback_cash))
        real_total = float(real_wallet.get("total_equity_pusd", real_cash))
        is_dry_run = getattr(self.scanner.auto_trader.config, "dry_run", False) if (self.scanner and hasattr(self.scanner, "auto_trader")) else False

        init_u = float(paper_st.get("initial_u", 40.62))
        tot_eq = float(paper_st.get("total_equity_u", real_total if real_total > 0 else paper_st.get("active_equity_u", 40.62)))
        vault_u = float(paper_st.get("vault_locked_u", 0.0))
        active_eq = float(paper_st.get("active_equity_u", max(0.0, tot_eq - vault_u)))
        net_gain = tot_eq - init_u
        net_gain_pct = (net_gain / max(init_u, 0.01)) * 100.0
        s1_u = float(paper_st.get("next_step1_stake_u", 2.50))
        s1_sh = float(paper_st.get("next_step1_shares", 5.0))
        mode_tag = "DRY-RUN" if is_dry_run else "LIVE"

        lines = [
            f"🌙 Polymarket V11.0 晚報 ({now_str} | {mode_tag})",
            f"• 總權益：${tot_eq:.2f}U（{net_gain:+.2f}U / {net_gain_pct:+.1f}%）",
            f"• 資金池：作戰 ${active_eq:.2f}U / 鎖倉 ${vault_u:.0f}U / 現金 ${real_cash:.2f} pUSD",
            f"• 戰績：{total_seq} 輪 | {win_rate_label}",
            f"• 下輪首碼：${s1_u:.2f}U ({s1_sh:g}份 @ 50¢)",
            "• 引擎：V11.0_ULTIMATE_RSI_MARTINGALE（純 Chainlink 預言機運行正常）",
        ]
        if is_dry_run and self.arena:
            lb = self.arena.get_leaderboard()
            if lb:
                top = lb[0]
                lines.append(f"• 競技場榜首：{top['name']} ${top['equity']:.2f}U ({top['win_rate_pct']}%)")

        sniper = getattr(self.scanner, "sniper", None) if self.scanner else None
        if sniper is not None:
            lines = [f"🌙 晚報 ({now_str})", sniper.telegram_text(), "", "🧪 舊策略（模擬觀察，不動真金）"] + lines[1:]
        report_text = "\n".join(lines)
        if dry_render:
            return report_text
        logger.info("🌙 [Watchdog] 推播每日策略總結至 Telegram")
        sent_ok = await self.notifier.send_telegram_text(report_text)
        print(f"🌙 [Watchdog] 22:00 Telegram 總結推播結果: sent_ok={sent_ok}", flush=True)
        return report_text if sent_ok else ""

    def get_status(self) -> dict[str, Any]:
        """返回看門狗即時健康狀態供前端與 API 查詢"""
        return {
            "is_running": self.running,
            "last_heartbeat_ts": self.last_heartbeat,
            "seconds_since_heartbeat": round(time.time() - self.last_heartbeat, 1),
            "warned_lanes": [],
            "check_interval_sec": 30,
            "time_stop_timeout_min": 0,
            "last_daily_summary_date": self.last_daily_summary_date,
            "last_daily_summary_sent_at": self.last_daily_summary_sent_at,
            "status_label": "🟢 30s 守護中・V11.0_ULTIMATE_RSI_MARTINGALE 純預言機實盤",
        }
