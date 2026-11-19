from __future__ import annotations

import asyncio
import json
import math
import time
from dataclasses import asdict, dataclass
from typing import Any
import aiohttp
import pandas as pd

from engine.backtester import StrategyConfig
from engine.chainlink_oracle import ChainlinkRTDSFeed
from engine.indicators import enrich_indicators
from engine.master_strategy import (
    WATERMARK_LADDER,
    LivePaperTrader,
    MasterStrategySpec,
    compute_step_stakes,
    evaluate_master_signal_at_index,
    get_watermark_tier,
)
from engine.notifier import NotificationManager
from engine.polymarket_auto_trader import PolymarketAutoTrader
from engine.polymarket_sniper import estimate_polymarket_share_pricing
from engine.v8_strategy import V8_CONFIG


@dataclass
class LaneState:
    symbol: str
    status: str = "IDLE"  # "IDLE", "ACTIVE_STEP1", "PENDING_RESCUE", "ACTIVE_RESCUE"
    current_step: int = 1
    direction: str | None = None
    pending_since: float | None = None
    cumulative_loss: float = 0.0
    active_shares: float = 0.0
    active_cost: float = 0.0
    price_to_beat: float = 0.0


class MultiLaneRiskController:
    """
    跨幣協同四碼馬丁通道控制器 (Cross-Coin 4-Step Martingale Allocation Engine):
    - 原則 1 (救援優先與跨幣接力): 當系統處於 Step >= 2 救援待命時，BTC/ETH/SOL 任一幣種率先觸發高信心反轉訊號皆可立即接力執行救援碼，絕不死鎖單一幣種！
    - 原則 2 (單線專注): 任何時刻全系統最多 1 個活躍持倉，確保四碼馬丁桌費 100% 獨立覆蓋、零穿倉風險。
    """
    def __init__(self) -> None:
        self.lanes: dict[str, LaneState] = {
            "BTCUSDT": LaneState("BTCUSDT"),
            "ETHUSDT": LaneState("ETHUSDT"),
            "SOLUSDT": LaneState("SOLUSDT"),
        }
        self.MAX_CONCURRENT_STEP1 = 1

    def can_open_step1(self, symbol: str) -> bool:
        for s, lane in self.lanes.items():
            if lane.status in ["PENDING_RESCUE", "ACTIVE_RESCUE", "ACTIVE_STEP1"]:
                return False
        return self.lanes.get(symbol, LaneState(symbol)).status == "IDLE"

    def can_open_rescue(self, symbol: str) -> bool:
        for s, lane in self.lanes.items():
            if lane.status in ["ACTIVE_STEP1", "ACTIVE_RESCUE"]:
                return False
        return True

    def reset_all_lanes(self) -> None:
        for lane in self.lanes.values():
            lane.status = "IDLE"
            lane.current_step = 1
            lane.pending_since = None
            lane.cumulative_loss = 0.0
            lane.direction = None


BINANCE_ENDPOINTS = [
    "https://api.binance.com/api/v3/klines",
    "https://api1.binance.com/api/v3/klines",
    "https://data-api.binance.vision/api/v3/klines",
]

TG_MAIN_KEYBOARD = {
    "keyboard": [
        [{"text": "🌙 每日總結 (22:00 晚報)"}, {"text": "📊 即時戰情"}, {"text": "💰 實盤錢包"}],
        [{"text": "🎯 新策略戰情"}, {"text": "🎯 信心指數監控"}, {"text": "📈 資金水位表"}, {"text": "🌐 VPN 狀態與修復"}],
        [{"text": "🎁 立即領獎"}, {"text": "📝 掛單紀錄"}, {"text": "▶️ 恢復下單"}, {"text": "⏸ 暫停下單"}],
    ],
    "resize_keyboard": True,
    "is_persistent": True,
}


class LiveScanner:
    """
    Real-time 5m Binance & Polymarket V2 CLOB Sniper for BTCUSDT, ETHUSDT, SOLUSDT.
    - Tracks live Taker Order-Flow Ratio, Cross-Coin Resonance, Polymarket Share Pricing (1c..99c)
    - Fires 4-stage Polymarket alerts + Automated CLOB V2 API order execution
    - Runs 30s Gasless Auto-Redeem & Live Wallet Balance Sync loop
    - Runs Interactive Telegram Bot controller (@btc_poly_5min_bot)
    """

    def __init__(
        self,
        notifier: NotificationManager,
        paper_trader: LivePaperTrader | None = None,
        auto_trader: PolymarketAutoTrader | None = None,
        vpn_guardian: Any = None,
        review_manager: Any = None,
    ) -> None:
        self.notifier = notifier
        self.paper_trader = paper_trader or LivePaperTrader()
        self.auto_trader = auto_trader or PolymarketAutoTrader()
        self.vpn_guardian = vpn_guardian
        self.review_manager = review_manager
        self.master_spec = self.paper_trader.spec
        self.symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

        self.configs: dict[str, StrategyConfig] = {
            sym: StrategyConfig(
                symbol=sym,
                rsi_period=6,
                oversold_rsi=self.master_spec.rsi6_oversold,
                overbought_rsi=self.master_spec.rsi6_overbought,
                min_streak=self.master_spec.min_streak,
                min_streak_ret_pct=self.master_spec.min_streak_ret_pct,
                use_bb_filter=True,
                min_vol_ratio=self.master_spec.min_vol_ratio,
                min_wick_ratio=self.master_spec.min_wick_ratio,
                direction="both",
                win_mode="target_bounce",
                target_bounce_pct=self.master_spec.target_bounce_pct,
                max_martingale_steps=self.master_spec.max_martingale_steps,
                martingale_multiplier=self.master_spec.martingale_multiplier,
                payout_ratio=0.95,
                base_stake=10.96,
            )
            for sym in self.symbols
        }
        self.expected_win_rates: dict[str, float] = {sym: 88.48 for sym in self.symbols}
        self.live_state: dict[str, dict[str, Any]] = {}
        self.resonance_state: dict[str, Any] = {}
        self.active_sequences: dict[str, dict[str, Any] | None] = {sym: None for sym in self.symbols}
        self._last_processed_closed_ts: int = 0
        self._last_preview_ts: int = 0
        self._last_bounce_protect_alert_ts: int = 0
        self._tg_update_offset: int = 909091798
        self._last_scan_time: float = time.time()
        self.chainlink_feed = ChainlinkRTDSFeed(proxy_url=self.auto_trader.config.proxy_url)
        self._pending_oracle_settlements: dict[str, asyncio.Task[None]] = {}
        self._stale_startup_position: dict[str, Any] | None = None
        self.risk_controller = MultiLaneRiskController()
        self.arena: Any = None
        self.sniper: Any = None  # OracleSniperRunner (live strategy), attached by app.py

        # Restore active sequence (+ CLOB tracker + RiskController lane) from persistent state if within 10m
        saved_pos = self.paper_trader.state.get("active_position")
        if isinstance(saved_pos, dict) and saved_pos.get("symbol") in self.symbols:
            started_ts = int(saved_pos.get("started_ts", 0) or 0)
            sym_p = str(saved_pos["symbol"])
            if (int(time.time() * 1000) - started_ts) <= 600_000:
                self.active_sequences[sym_p] = saved_pos
                is_up_p = saved_pos.get("direction") in ("LONG_BOUNCE", "UP")
                step_p = int(saved_pos.get("step", 1) or 1)
                sh_p = float(saved_pos.get("shares", 0.0) or 0.0)
                stk_p = float(saved_pos.get("stake_usd", 0.0) or 0.0)
                matched_p = bool(saved_pos.get("entry_verified_matched", False))
                self.auto_trader.active_clob_positions[sym_p] = {
                    "symbol": sym_p,
                    "slug": str(saved_pos.get("slug", "")),
                    "condition_id": str(saved_pos.get("condition_id", "")),
                    "token_id": str(saved_pos.get("token_id", "")),
                    "outcome": "UP (買漲)" if is_up_p else "DOWN (買跌)",
                    "direction": saved_pos.get("direction"),
                    "entry_price": float(saved_pos.get("entry_share_cents", 50.0) or 50.0) / 100.0,
                    "shares": sh_p if matched_p else 0.0,
                    "effective_cost_u": stk_p if matched_p else 0.0,
                    "step": step_p,
                    "entry_order_id": saved_pos.get("entry_order_id"),
                    "entry_matched": matched_p,
                    "entry_filled_shares": sh_p if matched_p else 0.0,
                    "entry_verified_matched": matched_p,
                    "entry_status": str(saved_pos.get("entry_status", "LIVE_MATCHED" if matched_p else "LIVE_LIVE")),
                    "bounce_sell_placed": bool(saved_pos.get("bounce_sell_placed", False)),
                    "bounce_sell_matched": bool(saved_pos.get("bounce_match_notified", False)),
                    "sell_order_id": None,
                    "is_dry_run": bool(self.auto_trader.config.dry_run),
                }
                lane_p = self.risk_controller.lanes.get(sym_p)
                if lane_p is not None:
                    lane_p.status = "ACTIVE_STEP1" if step_p == 1 else "ACTIVE_RESCUE"
                    lane_p.current_step = step_p
                    lane_p.active_cost = stk_p
                    lane_p.direction = saved_pos.get("direction")
                    lane_p.price_to_beat = float(saved_pos.get("entry_price", 0.0) or 0.0)
            else:
                self.paper_trader.state["active_position"] = None
                if not saved_pos.get("entry_verified_matched") and int(saved_pos.get("step", 1) or 1) >= 2:
                    self.paper_trader.state["pending_step2"] = self._seq_to_pending_payload(saved_pos)
                elif saved_pos.get("entry_verified_matched") and saved_pos.get("slug"):
                    self._stale_startup_position = dict(saved_pos)
                    lane_p = self.risk_controller.lanes.get(sym_p)
                    if lane_p is not None:
                        lane_p.status = "ACTIVE_RESCUE"
                        lane_p.current_step = int(saved_pos.get("step", 1) or 1)
                        lane_p.direction = saved_pos.get("direction")
                self.paper_trader.save()

        # Restore pending rescue state into risk_controller if no active position
        pending_s2 = self.paper_trader.state.get("pending_step2")
        if self._get_active_global_sequence()[0] is None and self._stale_startup_position is None and isinstance(pending_s2, dict) and pending_s2.get("stake_usd"):
            psym_raw = str(pending_s2.get("symbol") or "BTCUSDT")
            psym = psym_raw if psym_raw in self.risk_controller.lanes else "BTCUSDT"
            p_ms = pending_s2.get("paused_at_ms") or (time.time() * 1000)
            lane = self.risk_controller.lanes[psym]
            lane.status = "PENDING_RESCUE"
            lane.current_step = int(pending_s2.get("step", 2))
            lane.pending_since = p_ms / 1000.0
            lane.cumulative_loss = float(pending_s2.get("prior_cost_u", float(pending_s2.get("stake_usd", 2.50)) / 1.40))
        self.running = False
        self._task: asyncio.Task[None] | None = None
        self._wallet_task: asyncio.Task[None] | None = None
        self._tg_task: asyncio.Task[None] | None = None

    async def fetch_recent_klines(self, session: aiohttp.ClientSession, symbol: str, limit: int = 200) -> pd.DataFrame | None:
        params = {"symbol": symbol, "interval": "5m", "limit": limit}
        for url in BINANCE_ENDPOINTS:
            try:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                    if resp.status == 200:
                        raw = await resp.json()
                        if not raw:
                            continue
                        rows = []
                        for r in raw:
                            rows.append({
                                "open_time": int(r[0]),
                                "open": float(r[1]),
                                "high": float(r[2]),
                                "low": float(r[3]),
                                "close": float(r[4]),
                                "volume": float(r[5]),
                                "close_time": int(r[6]),
                                "quote_volume": float(r[7]) if len(r) > 7 else 0.0,
                                "trades": int(r[8]) if len(r) > 8 else 0,
                                "taker_buy_base": float(r[9]) if len(r) > 9 else float(r[5]) * 0.5,
                            })
                        df = pd.DataFrame(rows)
                        return enrich_indicators(df)
            except Exception:
                continue
        return None

    async def fetch_recent_1m_twap(self, symbol: str) -> dict[str, float]:
        """
        Fetches recent 1-minute klines to calculate the 60-second Time-Weighted Average Price (TWAP),
        matching Polymarket's official Chainlink 60s TWAP Data Stream (`{btc|eth|sol}-usd-twap-60s-streams`).
        """
        params = {"symbol": symbol, "interval": "1m", "limit": 8}
        now_ms = int(time.time() * 1000)
        async with aiohttp.ClientSession() as session:
            for url in BINANCE_ENDPOINTS:
                try:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status == 200:
                            raw = await resp.json()
                            if not raw or len(raw) < 2:
                                continue
                            last_r = raw[-1]
                            closed_r = raw[-2] if int(last_r[6]) > now_ms else raw[-1]
                            closed_twap = (float(closed_r[1]) + float(closed_r[2]) + float(closed_r[3]) + float(closed_r[4])) / 4.0
                            forming_twap = (float(last_r[1]) + float(last_r[2]) + float(last_r[3]) + float(last_r[4])) / 4.0
                            return {
                                "last_closed_1m_twap": round(closed_twap, 4),
                                "forming_1m_twap": round(forming_twap, 4),
                                "spot_close": float(last_r[4]),
                            }
                except Exception:
                    continue
        return {"last_closed_1m_twap": 0.0, "forming_1m_twap": 0.0, "spot_close": 0.0}

    def _get_active_global_sequence(self) -> tuple[str | None, dict[str, Any] | None]:
        for sym, seq in self.active_sequences.items():
            if seq is not None:
                return sym, seq
        return None, None

    async def scan_once(self) -> None:
        self._last_scan_time = time.time()
        now_ms = int(time.time() * 1000)
        dfs: dict[str, pd.DataFrame] = {}

        async with aiohttp.ClientSession() as session:
            results = await asyncio.gather(
                *[self.fetch_recent_klines(session, sym, limit=200) for sym in self.symbols],
                return_exceptions=True,
            )
            for sym, res in zip(self.symbols, results):
                if isinstance(res, pd.DataFrame) and len(res) >= 25:
                    dfs[sym] = res

        if len(dfs) < 3:
            return

        forming_r6 = [float(dfs[s].iloc[-1]["rsi_6"]) for s in self.symbols]
        mean_forming_r6 = sum(forming_r6) / 3.0
        min_res_streak = int(self.master_spec.resonance_coin_min_streak)
        sync_down_forming = sum(1 for s in self.symbols if int(dfs[s].iloc[-1]["streak_down"]) >= min_res_streak)
        sync_up_forming = sum(1 for s in self.symbols if int(dfs[s].iloc[-1]["streak_up"]) >= min_res_streak)

        self.resonance_state = {
            "mean_rsi_6": round(mean_forming_r6, 2),
            "sync_down_coins": sync_down_forming,
            "sync_up_coins": sync_up_forming,
            "oversold_resonance_active": (
                mean_forming_r6 <= self.master_spec.resonance_mean_rsi6_oversold
                and sync_down_forming >= self.master_spec.resonance_min_coins_in_streak
            ),
            "overbought_resonance_active": (
                mean_forming_r6 >= self.master_spec.resonance_mean_rsi6_overbought
                and sync_up_forming >= self.master_spec.resonance_min_coins_in_streak
            ),
            "master_strategy_name": self.master_spec.name,
        }

        last_row_btc = dfs["BTCUSDT"].iloc[-1]
        closed_idx = -2 if int(last_row_btc["close_time"]) > now_ms else -1
        closed_open_ts = int(dfs["BTCUSDT"].iloc[closed_idx]["open_time"])
        sec_remaining = max(0, int((int(last_row_btc["close_time"]) - now_ms) / 1000))
        forming_open_ts_scan = int(last_row_btc["open_time"])

        # Pre-Boundary Forward-Start Sniping (T=-3.0s..-0.5s into next bucket_offset=+1 while CLOB sits at 50-52c)
        if (
            0 <= sec_remaining <= 3
            and self._last_processed_closed_ts > 0
            and forming_open_ts_scan > self._last_processed_closed_ts
            and self._get_active_global_sequence() == (None, None)
            and not self._pending_oracle_settlements
            and min(len(dfs[s]) for s in self.symbols) >= 181
        ):
            common_pre_lb = min(180, *(len(dfs[s]) - 1 for s in self.symbols))
            pre_sliced = {
                s: dfs[s].iloc[len(dfs[s]) - 1 - common_pre_lb :].copy().reset_index(drop=True)
                for s in self.symbols
            }
            pre_pending_s2 = self.paper_trader.state.get("pending_step2") if getattr(self.master_spec, "max_martingale_steps", 4) > 1 else None
            pre_step = int(pre_pending_s2.get("step", 2)) if isinstance(pre_pending_s2, dict) and pre_pending_s2.get("stake_usd") else 1
            if evaluate_master_signal_at_index(pre_sliced, common_pre_lb, self.master_spec, entry_step=pre_step) is not None:
                await self._handle_synchronized_close(
                    pre_sliced,
                    common_pre_lb,
                    target_bucket_ts=(forming_open_ts_scan // 1000) + 300,
                )
                if self._get_active_global_sequence() != (None, None):
                    self._last_processed_closed_ts = forming_open_ts_scan

        # Ensure all 3 coins share the exact same closed 5m candle open_time (prevents 1-candle CDN lag on ETH/SOL)
        expected_closed_open_ts = ((now_ms // 300_000) - 1) * 300_000
        if closed_open_ts == expected_closed_open_ts:
            aligned_dfs: dict[str, tuple[pd.DataFrame, int]] = {}
            for sym in self.symbols:
                df_s = dfs[sym]
                match_idx = df_s.index[df_s["open_time"] == expected_closed_open_ts].tolist()
                pos = int(match_idx[-1]) if match_idx else -1
                if not match_idx or pos == len(df_s) - 1:
                    # Refetch directly from primary api.binance.com if CDN lagged at 5m boundary
                    async with aiohttp.ClientSession() as retry_sess:
                        fresh_df = await self.fetch_recent_klines(retry_sess, sym, limit=200)
                        if isinstance(fresh_df, pd.DataFrame) and len(fresh_df) >= 25:
                            df_s = fresh_df
                            dfs[sym] = fresh_df
                            match_idx = df_s.index[df_s["open_time"] == expected_closed_open_ts].tolist()
                            pos = int(match_idx[-1]) if match_idx else -1
                if match_idx:
                    aligned_dfs[sym] = (df_s, pos)
            if len(aligned_dfs) == 3:
                common_lookback = min(180, *(pos for _, pos in aligned_dfs.values()))
                sliced_dfs: dict[str, pd.DataFrame] = {}
                for sym, (df_a, idx_a) in aligned_dfs.items():
                    start_a = idx_a - common_lookback
                    sliced_dfs[sym] = df_a.iloc[start_a : idx_a + 1].copy().reset_index(drop=True)
                if self._last_processed_closed_ts == 0:
                    self._last_processed_closed_ts = expected_closed_open_ts
                elif expected_closed_open_ts > self._last_processed_closed_ts:
                    target_idx = common_lookback
                    await self._handle_synchronized_close(sliced_dfs, target_idx)
                    last_clob = self.auto_trader.order_history[0] if self.auto_trader.order_history else {}
                    sec_after_close = (now_ms - expected_closed_open_ts - 300_000) / 1000.0
                    transient_entry_fail = (
                        self._get_active_global_sequence()[0] is None
                        and str(last_clob.get("action", "")) == "BUY_ENTRY"
                        and str(last_clob.get("status", "")) in ("ERROR", "SKIPPED_NO_OPEN_FILL")
                        and int(last_clob.get("timestamp_ms", 0) or 0) >= expected_closed_open_ts + 295_000
                        and sec_after_close <= 20.0
                    )
                    if not transient_entry_fail:
                        self._last_processed_closed_ts = expected_closed_open_ts

        # 2. Check Stage-3 Real-Time Intra-Candle +0.10% Bounce-Protect Alert on active sequence
        active_sym, active_seq = self._get_active_global_sequence()
        forming_open_ts = int(last_row_btc["open_time"])

        if active_sym is not None and active_seq is not None:
            seq_bucket_s = int(active_seq.get("bucket_ts") or (forming_open_ts // 1000))
            now_s_bar = time.time()
            seq_sec_remaining = max(0, min(300, int((seq_bucket_s + 300) - now_s_bar)))
            seq_elapsed_sec = max(0, int(now_s_bar - seq_bucket_s))
            # Verify resting LIVE_LIVE order fill on every scan cycle and only dispatch entry notification once LIVE_MATCHED!
            if not active_seq.get("entry_verified_matched"):
                fill_check = await self.auto_trader.check_and_sync_position_fill(active_sym)
                was_ever_matched = (
                    bool(fill_check.get("entry_matched"))
                    or bool(active_seq.get("entry_verified_matched"))
                    or float(active_seq.get("bounce_sell_shares", 0.0)) >= 1.0
                )
                if was_ever_matched:
                    active_seq["entry_verified_matched"] = True
                    active_seq["entry_status"] = "LIVE_MATCHED"
                    filled_sh_sync = float(fill_check.get("entry_filled_shares", 0.0))
                    if filled_sh_sync >= 1.0:
                        active_seq["shares"] = round(filled_sh_sync, 2)
                        eff_cost_sync = float(fill_check.get("effective_cost_u", 0.0))
                        if eff_cost_sync > 0.0:
                            active_seq["stake_usd"] = round(eff_cost_sync, 2)
                        elif float(active_seq.get("entry_share_cents", 0.0)) > 0.0:
                            active_seq["stake_usd"] = round(filled_sh_sync * (float(active_seq["entry_share_cents"]) / 100.0), 2)
                    self.paper_trader.state["active_position"] = active_seq
                    if active_seq.get("entry_notify_pending") and isinstance(active_seq.get("pending_entry_signal"), dict):
                        sig_p = dict(active_seq["pending_entry_signal"])
                        sig_p["message"] = str(sig_p.get("message", "")).replace("LIVE_LIVE", "LIVE_MATCHED").replace("LIVE_POSTED", "LIVE_MATCHED")
                        active_seq["entry_notify_pending"] = False
                        active_seq["pending_entry_signal"] = None
                        self.paper_trader.state["active_position"] = active_seq
                        self.paper_trader.save()
                        await self.notifier.dispatch_signal(sig_p)
                    else:
                        self.paper_trader.save()
                elif seq_sec_remaining <= 210 and seq_elapsed_sec >= 90 and not active_seq.get("bounce_sell_placed"):
                    # Institutional Risk Guard #1: 90-Second Unfilled Entry Auto-Cancel (prevents late-candle adverse selection)
                    await self.auto_trader.cancel_active_orders_for_symbol(active_sym)
                    cur_s_unfilled = int(active_seq.get("step", 1))
                    if cur_s_unfilled >= 2:
                        self.paper_trader.state["pending_step2"] = self._seq_to_pending_payload(active_seq)
                    self.active_sequences[active_sym] = None
                    self.auto_trader.active_clob_positions.pop(active_sym, None)
                    self.paper_trader.state["active_position"] = None
                    self.paper_trader.save()
                    self._restore_lanes_after_unfilled_entry(active_sym, active_seq)
                    active_sym, active_seq = None, None

        if active_sym is not None and active_seq is not None:
            f_row = dfs[active_sym].iloc[-1]
            seq_bucket_s = int(active_seq.get("bucket_ts") or (forming_open_ts // 1000))
            now_s_bar = time.time()
            seq_sec_remaining = max(0, min(300, int((seq_bucket_s + 300) - now_s_bar)))
            elapsed_sec = max(0, int(now_s_bar - seq_bucket_s))
            cur_bucket_s = seq_bucket_s
            # Sync official Polymarket Gamma API priceToBeat (or Chainlink RTDS boundary open)
            if not active_seq.get("official_ptb_synced"):
                cl_open_act = self.chainlink_feed.get_bucket_open_price(active_sym, cur_bucket_s)
                if cl_open_act is not None and active_seq.get("official_price_to_beat") is None:
                    active_seq["official_price_to_beat"] = cl_open_act
                if elapsed_sec >= 6:
                    try:
                        live_mkt = await self.auto_trader.discover_live_5m_market(
                            active_sym, bucket_offset=0, target_bucket_ts=cur_bucket_s
                        )
                        if live_mkt and live_mkt.get("price_to_beat") is not None:
                            active_seq["official_price_to_beat"] = float(live_mkt["price_to_beat"])
                            active_seq["official_ptb_synced"] = True
                    except Exception as exc:
                        print(f"[Scanner Warning] sync official price_to_beat for {active_sym} failed: {exc}")

            # Pure Polymarket Chainlink Oracle Intra-Bar Pricing (zero Binance anchor distortion)
            cl_now_act, _ = self.chainlink_feed.get_latest_price(active_sym, max_age_sec=30.0)
            oracle_anchor_open = float(
                active_seq.get("official_price_to_beat")
                or active_seq.get("strike_twap_60s")
                or cl_now_act
                or f_row["open"]
            )
            cur_oracle_px = float(cl_now_act if cl_now_act is not None else oracle_anchor_open)

            pm_live = estimate_polymarket_share_pricing(
                open_price=oracle_anchor_open,
                current_price=cur_oracle_px,
                high_price=max(oracle_anchor_open, cur_oracle_px),
                low_price=min(oracle_anchor_open, cur_oracle_px),
                seconds_remaining=seq_sec_remaining,
                direction=active_seq["direction"],
                target_bounce_pct=self.master_spec.target_bounce_pct,
            )
            # V8.0 Hold-to-100c + Optional Late-Surge 95c Lock (only if bid >= 0.93 within last 60s)
            cur_step = int(active_seq.get("step", 1))
            if getattr(self.master_spec, "bounce_protect_enabled", False):
                req_fair = 82.0 if cur_step == 1 else 85.0
                path_a = (seq_sec_remaining <= getattr(self.master_spec, "early_lock_time_sec", 180)) and (float(pm_live.get("favored_share_cents", 0.0)) >= req_fair)
                path_b = bool(pm_live.get("bounce_protect_triggered", False))
                dual_track_bounce = path_a or path_b

                if not active_seq.get("bounce_sell_placed") and dual_track_bounce:
                    planned_st_b = active_seq.get("planned_stakes", [2.55])
                    prior_cost_b = 0.0 if active_seq.get("s1_already_deducted") else sum(float(x) for x in planned_st_b[: max(0, cur_step - 1)])
                    sell_rec = await self.auto_trader.execute_bounce_protect_sell(
                        active_sym,
                        active_seq["direction"],
                        bounce_triggered=True,
                        prior_cost_u=prior_cost_b,
                    )
                    if sell_rec and sell_rec.get("status") == "SKIPPED_ENTRY_UNFILLED":
                        if pm_live["bounce_protect_triggered"]:
                            await self.auto_trader.cancel_active_orders_for_symbol(active_sym)
                            if cur_step >= 2:
                                self.paper_trader.state["pending_step2"] = self._seq_to_pending_payload(active_seq)
                            self.active_sequences[active_sym] = None
                            self.auto_trader.active_clob_positions.pop(active_sym, None)
                            self.paper_trader.state["active_position"] = None
                            self.paper_trader.save()
                            self._restore_lanes_after_unfilled_entry(active_sym, active_seq)
                            active_sym, active_seq = None, None
                    elif sell_rec and (str(sell_rec.get("status", "")).startswith("LIVE_") or str(sell_rec.get("status", "")).startswith("SIM_")):
                        active_seq["entry_verified_matched"] = True
                        active_seq["bounce_sell_placed"] = True
                        active_seq["bounce_sell_status"] = sell_rec.get("status", "")
                        active_seq["bounce_sell_shares"] = sell_rec.get("shares", active_seq.get("shares", 5.0))
                        active_seq["bounce_sell_price_cents"] = float(sell_rec.get("price_cents", req_fair))
                        if sell_rec.get("status") in ("LIVE_MATCHED", "SIM_MATCHED"):
                            active_seq["bounce_match_notified"] = True
                        self.paper_trader.state["active_position"] = active_seq
                        self.paper_trader.save()
            elif (
                getattr(self.master_spec, "surge_take_profit_enabled", False)
                and active_seq.get("entry_verified_matched")
                and not active_seq.get("bounce_sell_placed")
                and 4 <= seq_sec_remaining <= int(getattr(self.master_spec, "surge_tp_trigger_remaining_sec", 60))
            ):
                min_bid_req = float(getattr(self.master_spec, "surge_tp_min_bid", 0.93))
                limit_sell_p = float(getattr(self.master_spec, "surge_tp_limit_price", 0.95))
                surge_rec = await self.auto_trader.execute_late_surge_sell(
                    active_sym,
                    min_bid=min_bid_req,
                    limit_sell_price=limit_sell_p,
                    prior_cost_u=float(active_seq.get("prior_cost_u", 0.0)),
                )
                if surge_rec and (str(surge_rec.get("status", "")).startswith("LIVE_") or str(surge_rec.get("status", "")).startswith("SIM_")):
                    active_seq["entry_verified_matched"] = True
                    active_seq["bounce_sell_placed"] = True
                    active_seq["bounce_sell_status"] = surge_rec.get("status", "")
                    active_seq["bounce_sell_shares"] = surge_rec.get("shares", active_seq.get("shares", 5.0))
                    active_seq["bounce_sell_price_cents"] = float(surge_rec.get("price_cents", limit_sell_p * 100.0))
                    if surge_rec.get("status") in ("LIVE_MATCHED", "SIM_MATCHED"):
                        active_seq["bounce_match_notified"] = True
                    self.paper_trader.state["active_position"] = active_seq
                    self.paper_trader.save()

            # Check if resting Take-Profit SELL order actually matched on CLOB
            if (
                active_sym is not None
                and active_seq is not None
                and active_seq.get("bounce_sell_placed")
                and not active_seq.get("bounce_match_notified")
                and elapsed_sec % 12 < 5
            ):
                fill_chk = await self.auto_trader.check_and_sync_position_fill(
                    active_sym,
                    bounce_triggered=bool(getattr(self.master_spec, "bounce_protect_enabled", False) and pm_live["bounce_protect_triggered"]),
                )
                if fill_chk.get("bounce_sell_matched"):
                    active_seq["bounce_match_notified"] = True
                    self.paper_trader.state["active_position"] = active_seq
                    self.paper_trader.save()

        # 3. Check Stage-1 Pre-Close Preview on forming candle
        pending_s2 = self.paper_trader.state.get("pending_step2")
        entry_step_prev = int(pending_s2.get("step", 2)) if isinstance(pending_s2, dict) and pending_s2.get("stake_usd") else 1
        forming_cand = evaluate_master_signal_at_index(dfs, len(dfs["BTCUSDT"]) - 1, self.master_spec, entry_step=entry_step_prev)
        if (
            forming_cand is not None
            and active_sym is None
            and 1 <= sec_remaining <= self.notifier.config.pre_close_warning_sec
            and self._last_preview_ts != forming_open_ts
        ):
            self._last_preview_ts = forming_open_ts
            sym_p = forming_cand["symbol"]
            tot_eq_p = float(self.paper_trader.state.get("total_equity_u", self.paper_trader.state.get("active_equity_u", 85.66)))
            vlt_eq_p = float(self.paper_trader.state.get("vault_locked_u", 0.0))
            stakes = compute_step_stakes(tot_eq_p, self.master_spec, vault_locked_u=vlt_eq_p)
            prev_stake_u = round(
                float(pending_s2["stake_usd"])
                if entry_step_prev >= 2 and isinstance(pending_s2, dict) and pending_s2.get("stake_usd")
                else stakes[0],
                2,
            )
            prev_shares = round(
                float(pending_s2.get("shares") or (prev_stake_u / (self.master_spec.entry_share_cents / 100.0)))
                if entry_step_prev >= 2 and isinstance(pending_s2, dict)
                else (stakes[0] / (self.master_spec.entry_share_cents / 100.0)),
                2,
            )
            await self.notifier.dispatch_signal({
                "symbol": sym_p,
                "event_type": "PRE_CLOSE_PREVIEW",
                "direction": forming_cand["direction"],
                "step": entry_step_prev,
                "max_steps": self.master_spec.max_martingale_steps,
                "stake_usd": prev_stake_u,
                "price": forming_cand["price"],
                "rsi": forming_cand["rsi_6"],
                "streak": forming_cand["streak"],
                "streak_ret_pct": forming_cand["streak_ret_pct"],
                "expected_win_rate_pct": 88.48,
                "message": (
                    f"⏳ 預警 {sym_p} {forming_cand['polymarket_side']}（倒數 {sec_remaining}s）\n"
                    f"• 預計：第 {entry_step_prev}/{self.master_spec.max_martingale_steps} 碼 ${prev_stake_u:.2f}U ({prev_shares:g}份) | RSI(6) {forming_cand['rsi_6']:.1f}"
                ),
            })

        # 4. Update per-symbol live snapshot
        for sym in self.symbols:
            df = dfs[sym]
            cfg = self.configs[sym]
            rsi_col = f"rsi_{cfg.rsi_period}" if f"rsi_{cfg.rsi_period}" in df.columns else "rsi_6"
            forming_row = df.iloc[-1]
            prev_closed_row = df.iloc[closed_idx]

            would_trig = None
            if forming_cand is not None and forming_cand["symbol"] == sym:
                would_trig = forming_cand["direction"]

            pm_dir = (
                self.active_sequences[sym]["direction"]
                if self.active_sequences[sym] is not None
                else ("LONG_BOUNCE" if int(forming_row["streak_down"]) >= int(forming_row["streak_up"]) else "SHORT_PULLBACK")
            )
            pm_pricing = estimate_polymarket_share_pricing(
                open_price=float(forming_row["open"]),
                current_price=float(forming_row["close"]),
                high_price=float(forming_row["high"]),
                low_price=float(forming_row["low"]),
                seconds_remaining=sec_remaining,
                direction=pm_dir,
                target_bounce_pct=self.master_spec.target_bounce_pct,
            )

            recent_candles = []
            for _, r in df.tail(24).iterrows():
                recent_candles.append({
                    "open_time": int(r["open_time"]),
                    "open": float(r["open"]),
                    "high": float(r["high"]),
                    "low": float(r["low"]),
                    "close": float(r["close"]),
                    "is_green": bool(r["is_green"]),
                    "rsi_2": round(float(r["rsi_2"]), 1),
                    "rsi_6": round(float(r["rsi_6"]), 1),
                    "rsi_14": round(float(r["rsi_14"]), 1),
                    "streak_down": int(r["streak_down"]),
                    "streak_up": int(r["streak_up"]),
                })

            self.live_state[sym] = {
                "symbol": sym,
                "updated_ms": now_ms,
                "price": float(forming_row["close"]),
                "open": float(forming_row["open"]),
                "high": float(forming_row["high"]),
                "low": float(forming_row["low"]),
                "body_pct": round(float(forming_row["body_pct"]), 3),
                "seconds_to_close": sec_remaining,
                "polymarket_clob": pm_pricing,
                "forming_candle": {
                    "rsi_2": round(float(forming_row["rsi_2"]), 2),
                    "rsi_6": round(float(forming_row["rsi_6"]), 2),
                    "rsi_14": round(float(forming_row["rsi_14"]), 2),
                    "active_rsi": round(float(forming_row[rsi_col]), 2),
                    "streak_down": int(forming_row["streak_down"]),
                    "streak_up": int(forming_row["streak_up"]),
                    "streak_ret_pct": round(float(forming_row["streak_ret_pct"]), 3),
                    "bb_pct_b": round(float(forming_row["bb_pct_b"]), 3),
                    "vol_ratio": round(float(forming_row["vol_ratio"]), 2),
                    "taker_buy_ratio": round(float(forming_row.get("taker_buy_ratio", 0.5)), 3),
                    "lower_wick_ratio": round(float(forming_row["lower_wick_ratio"]), 2),
                    "upper_wick_ratio": round(float(forming_row["upper_wick_ratio"]), 2),
                    "would_trigger": would_trig,
                },
                "last_closed_candle": {
                    "open_time": int(prev_closed_row["open_time"]),
                    "close": float(prev_closed_row["close"]),
                    "rsi_6": round(float(prev_closed_row["rsi_6"]), 2),
                    "rsi_14": round(float(prev_closed_row["rsi_14"]), 2),
                    "streak_down": int(prev_closed_row["streak_down"]),
                    "streak_up": int(prev_closed_row["streak_up"]),
                    "streak_ret_pct": round(float(prev_closed_row["streak_ret_pct"]), 3),
                },
                "active_sequence": self.active_sequences[sym],
                "strategy_config": asdict(cfg),
                "expected_win_rate_pct": self.expected_win_rates.get(sym, 88.48),
                "recent_candles": recent_candles,
            }

    @staticmethod
    def _build_pending_step_payload(
        symbol: str,
        plan: dict[str, Any],
        last_loss_open_ts: int | None = None,
        s1_already_deducted: bool = False,
    ) -> dict[str, Any]:
        """Build persisted pending_step2 state payload for the next Martingale step."""
        payload: dict[str, Any] = {
            "step": int(plan["next_step"]),
            "symbol": symbol,
            "stake_usd": round(float(plan["next_stake"]), 2),
            "shares": float(plan["next_shares"]),
            "s1_shares_base": float(plan["s1_shares_base"]),
            "prior_cost_u": round(float(plan["cum_cost"]), 4),
            "prior_shares_lost": round(float(plan["cum_shares"]), 2),
            "planned_stakes": [round(float(x), 2) for x in plan["planned_stakes"]],
            "paused_at_ms": int(time.time() * 1000),
            "s1_already_deducted": bool(s1_already_deducted),
        }
        if last_loss_open_ts is not None:
            payload["last_loss_open_ts"] = int(last_loss_open_ts)
        return payload

    @classmethod
    def _seq_to_pending_payload(cls, active_seq: dict[str, Any]) -> dict[str, Any]:
        """Convert an in-flight Martingale sequence dict into a pending rescue payload."""
        plan = {
            "next_step": int(active_seq.get("step", 2)),
            "next_stake": float(active_seq.get("stake_usd", 3.50)),
            "next_shares": float(active_seq.get("shares", 7.0)),
            "s1_shares_base": float(active_seq.get("s1_shares_base", 5.0)),
            "cum_cost": float(active_seq.get("prior_cost_u", 2.50)),
            "cum_shares": float(active_seq.get("prior_shares_lost", 5.0)),
            "planned_stakes": active_seq.get("planned_stakes", [2.50, 3.50, 7.00, 14.00]),
        }
        return cls._build_pending_step_payload(
            str(active_seq.get("symbol", "")),
            plan,
            s1_already_deducted=bool(active_seq.get("s1_already_deducted", False)),
        )

    def _compute_seq_costs(self, seq: dict[str, Any], cur_step: int) -> tuple[float, float, float, float, float]:
        entry_c = float(getattr(self.master_spec, "entry_share_cents", 50.0)) / 100.0
        planned_st = [float(x) for x in (seq.get("planned_stakes") or [2.50])]
        cur_default = planned_st[min(max(0, cur_step - 1), len(planned_st) - 1)]
        cur_stake = float(seq.get("stake_usd", cur_default))
        cur_shares = float(seq.get("shares") or round(cur_stake / max(entry_c, 0.01), 2))
        if seq.get("s1_already_deducted"):
            prior_cost = 0.0
        elif "prior_cost_u" in seq:
            prior_cost = float(seq["prior_cost_u"])
        else:
            prior_cost = sum(planned_st[: max(0, cur_step - 1)])
        prior_sh_lost = (
            float(seq["prior_shares_lost"])
            if "prior_shares_lost" in seq
            else round(prior_cost / max(entry_c, 0.01), 2)
        )
        tot_cost = round(prior_cost + cur_stake, 4)
        return cur_stake, cur_shares, prior_cost, prior_sh_lost, tot_cost

    def _plan_next_martingale_step(
        self,
        seq: dict[str, Any],
        cur_step: int,
        max_steps: int,
        post_loss_avail_u: float,
    ) -> dict[str, Any]:
        entry_c = float(getattr(self.master_spec, "entry_share_cents", 50.0)) / 100.0
        floor_u = float(getattr(self.master_spec, "emergency_capital_floor_u", 2.50))
        planned_st = [float(x) for x in (seq.get("planned_stakes") or [2.50])]
        _cur_stake, cur_sh, _prior_cost, prior_sh_lost, cum_cost = self._compute_seq_costs(seq, cur_step)
        cum_sh = round(prior_sh_lost + cur_sh, 2)
        next_step = cur_step + 1
        gate_map = {
            2: float(V8_CONFIG.step2_min_active_u),
            3: float(V8_CONFIG.step3_min_active_u),
            4: float(V8_CONFIG.step4_min_active_u),
        }
        min_gate_u = float(gate_map.get(next_step, float(V8_CONFIG.step4_min_active_u)))
        s1_sh = float(seq.get("s1_shares_base") or round(float(planned_st[0]) / max(entry_c, 0.01), 2))
        profit_buf_sh = max(1, int(math.ceil(float(V8_CONFIG.martingale_profit_buffer_ratio) * s1_sh)))
        min_prof_u = max(0.25, round(0.08 * s1_sh, 2))
        req_sh_cost = int(math.ceil((cum_cost + min_prof_u) / max(0.10, 1.0 - entry_c)))
        req_sh = max(5, int(math.ceil(cum_sh)) + profit_buf_sh, req_sh_cost)
        next_shares = float(req_sh)
        next_stake = round(next_shares * entry_c, 2)
        slip_over_50c = max(0.0, cum_cost - cum_sh * 0.50)
        can_advance = (
            cur_step < max_steps
            and (post_loss_avail_u + min(2.00, slip_over_50c)) >= min_gate_u
            and post_loss_avail_u >= (next_stake + floor_u)
        )
        return {
            "can_advance": bool(can_advance),
            "next_step": int(next_step),
            "next_stake": float(next_stake),
            "next_shares": float(next_shares),
            "s1_shares_base": float(s1_sh),
            "cum_cost": float(cum_cost),
            "cum_shares": float(cum_sh),
            "min_gate_u": float(min_gate_u),
            "planned_stakes": planned_st,
        }

    def _sim_prefix(self) -> str:
        """Return simulation badge prefix whenever auto_trader runs in dry_run mode."""
        return "[🧪模擬] " if bool(getattr(self.auto_trader.config, "dry_run", False)) else ""

    def _format_performance_summary_line(self) -> str:
        """Build a concise 2-line overall equity and win-rate summary for open/close notifications."""
        st = self.paper_trader.state
        init_u = float(st.get("initial_u", 40.62) or 40.62)
        tot_eq = float(st.get("total_equity_u", init_u) or init_u)
        vlt_u = float(st.get("vault_locked_u", 0.0) or 0.0)
        act_eq = float(st.get("active_equity_u", max(0.0, tot_eq - vlt_u)) or max(0.0, tot_eq - vlt_u))
        net_u = tot_eq - init_u
        net_pct = (net_u / max(init_u, 0.01)) * 100.0
        tot_seq = int(st.get("total_sequences", 0) or 0)
        won_seq = int(st.get("won_sequences", 0) or 0)
        busts = int(st.get("busts", 0) or 0)
        wr_str = f"{(won_seq / tot_seq * 100.0):.1f}%" if tot_seq > 0 else "待首輪結算"
        s1 = int(st.get("step1_wins", 0) or 0)
        s2 = int(st.get("step2_wins", 0) or 0)
        s3 = int(st.get("step3_wins", 0) or 0)
        s4 = int(st.get("step4_wins", 0) or 0)
        if getattr(self.auto_trader.config, "dry_run", False):
            eq_line = f"• 總覽：權益 ${tot_eq:.2f}U ({net_u:+.2f}U / {net_pct:+.1f}%)｜模擬作戰 ${act_eq:.2f}U｜鎖倉 ${vlt_u:.0f}U"
        else:
            cash_u = float(
                (self.auto_trader.live_wallet_state or {}).get("cash_pusd")
                or st.get("live_cash_pusd")
                or tot_eq
            )
            eq_line = f"• 總覽：權益 ${tot_eq:.2f}U ({net_u:+.2f}U / {net_pct:+.1f}%)｜現金 ${cash_u:.2f}U｜鎖倉 ${vlt_u:.0f}U"
        return (
            f"{eq_line}\n"
            f"• 戰績：{won_seq}勝/{busts}負 (勝率 {wr_str}｜S1={s1}/S2={s2}/S3={s3}/S4={s4})"
        )

    async def _handle_martingale_loss_progression(self, seq: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
        """Advance an unresolved Martingale loss to the next step or record a sequence bust."""
        symbol = str(ctx["symbol"])
        direction = str(ctx["direction"])
        cur_step = int(ctx["cur_step"])
        max_steps = int(ctx["max_steps"])
        bucket_ts = int(ctx["bucket_ts"])
        strike_twap = float(ctx["strike_twap"])
        ending_twap = float(ctx["ending_twap"])
        twap_note = f"（Chainlink 60s TWAP: ${strike_twap:.2f} ➔ ${ending_twap:.2f}）"
        vault_u_floor = float(self.paper_trader.state.get("vault_locked_u", 0.0))
        _, cur_sh, _, _, cum_cost = self._compute_seq_costs(seq, cur_step)

        if not self.auto_trader.config.dry_run:
            w_now = await self.auto_trader.sync_live_wallet_balance()
            post_loss_avail_u = max(0.0, float(w_now.get("cash_pusd", 0.0)) - vault_u_floor)
        else:
            w_now = self.auto_trader.live_wallet_state
            post_loss_avail_u = max(0.0, float(self.paper_trader.state.get("active_equity_u", 40.62)) - cum_cost)

        plan = self._plan_next_martingale_step(seq, cur_step, max_steps, post_loss_avail_u)
        if not plan["can_advance"]:
            return await self._record_martingale_bust(seq, ctx, plan, w_now, cur_sh)

        next_step, next_stake, next_shares = plan["next_step"], plan["next_stake"], plan["next_shares"]
        self.paper_trader.state["cooldown_until_ms"] = 0
        self.paper_trader.state["pending_step2"] = self._build_pending_step_payload(
            symbol, plan, last_loss_open_ts=int(bucket_ts * 1000), s1_already_deducted=False
        )
        self.active_sequences[symbol] = None
        self.auto_trader.active_clob_positions.pop(symbol, None)
        self.paper_trader.state["active_position"] = None
        self.paper_trader.save()
        if hasattr(self, "risk_controller") and self.risk_controller:
            self.risk_controller.reset_all_lanes()
            lane = self.risk_controller.lanes.get(symbol)
            if lane:
                lane.status, lane.current_step = "PENDING_RESCUE", next_step
                lane.pending_since, lane.cumulative_loss, lane.direction = time.time(), float(plan["cum_cost"]), direction
        ctx_tag = "（預言機延遲確認）" if ctx.get("from_oracle_waiter") else ""
        cur_stake_u = float(seq.get("stake_usd", 0.0) or 0.0)
        await self.notifier.dispatch_signal({
            "symbol": symbol,
            "event_type": "STEP_LOSS_RESCUE",
            "direction": direction,
            "step": cur_step,
            "max_steps": max_steps,
            "stake_usd": cur_stake_u,
            "price": ending_twap,
            "rsi": round(float(ctx.get("bust_rsi_6") or seq.get("entry_rsi_6", 50.0)), 2),
            "streak": int(ctx.get("bust_streak") or seq.get("initial_streak", 3)),
            "streak_ret_pct": round(float(ctx.get("bust_streak_ret_pct", 0.0)), 3),
            "expected_win_rate_pct": 88.48,
            "message": (
                f"{self._sim_prefix()}🛡 關單待救援 {symbol}｜第 {cur_step}/{max_steps} 碼未過線 (-${cur_stake_u:.2f}U){ctx_tag}\n"
                f"• 結算：${strike_twap:.2f} ➔ ${ending_twap:.2f}（累積待回收 ${plan['cum_cost']:.2f}U）\n"
                f"• 下輪：鎖定第 {next_step}/{max_steps} 碼 ${next_stake:.2f}U ({next_shares:.0f}份 @ 50¢)\n"
                f"{self._format_performance_summary_line()}"
            ),
        })
        return {"advanced": True, "plan": plan}

    async def _record_martingale_bust(
        self, seq: dict[str, Any], ctx: dict[str, Any], plan: dict[str, Any], w_now: dict[str, Any], cur_sh: float
    ) -> dict[str, Any]:
        """Finalize and persist a sequence bust when no further Martingale rescue step is available."""
        symbol, direction = str(ctx["symbol"]), str(ctx["direction"])
        cur_step, max_steps, bucket_ts = int(ctx["cur_step"]), int(ctx["max_steps"]), int(ctx["bucket_ts"])
        strike_twap, ending_twap = float(ctx["strike_twap"]), float(ctx["ending_twap"])
        provisional_oracle = bool(ctx.get("provisional_oracle", False))
        market_slug = str(ctx.get("market_slug", ""))
        floor_u = float(getattr(self.master_spec, "emergency_capital_floor_u", 2.50))
        twap_note = f"（Chainlink 60s TWAP: ${strike_twap:.2f} ➔ ${ending_twap:.2f}）"
        tot_loss_u = -round(plan["cum_cost"], 2)
        rec = self.paper_trader.record_completed_sequence(
            symbol=symbol, direction=direction, entry_price=float(seq.get("entry_price", strike_twap)),
            resolve_price=ending_twap, rsi_6=float(seq.get("entry_rsi_6", 50.0)),
            streak=int(seq.get("initial_streak", 3)), won_at_step=None, exit_cents=0.0,
            exit_type="0¢_STOP_LOSS", timestamp_ms=int(bucket_ts * 1000), actual_pnl_u=tot_loss_u,
            trigger_branch=str(seq.get("trigger_branch", "")), tier=str(seq.get("tier", "STANDARD")),
            cvd_3=float(seq.get("cvd_3", 0.0)),
            oracle_source="POLYMARKET_GAMMA_OFFICIAL" if not provisional_oracle else "POLYMARKET_CHAINLINK_RTDS",
            actual_stake_u=float(seq.get("stake_usd", abs(tot_loss_u))),
            actual_entry_cents=float(seq.get("entry_share_cents", 50.0)),
        )
        self.active_sequences[symbol] = None
        self.auto_trader.active_clob_positions.pop(symbol, None)
        self.paper_trader.state["active_position"] = None
        self.paper_trader.state["pending_step2"] = None
        if hasattr(self, "risk_controller") and self.risk_controller:
            self.risk_controller.reset_all_lanes()
        if w_now.get("total_equity_pusd", 0.0) >= floor_u:
            self.paper_trader.sync_with_live_wallet(
                float(w_now["total_equity_pusd"]), float(w_now.get("cash_pusd", 0.0)), is_dry_run=self.auto_trader.config.dry_run
            )
        else:
            self.paper_trader.save()
        bust_close, bust_rsi, bust_stk = ctx.get("bust_close_price"), ctx.get("bust_rsi_6"), ctx.get("bust_streak")
        await self.notifier.dispatch_signal({
            "symbol": symbol, "event_type": "SEQUENCE_BUST", "direction": direction, "step": cur_step,
            "max_steps": max_steps, "stake_usd": float(seq.get("stake_usd", abs(tot_loss_u))),
            "price": float(bust_close if bust_close is not None else ending_twap),
            "rsi": round(float(bust_rsi if bust_rsi is not None else seq.get("entry_rsi_6", 50.0)), 2),
            "streak": int(bust_stk if bust_stk is not None else seq.get("initial_streak", 3)),
            "streak_ret_pct": round(float(ctx.get("bust_streak_ret_pct", 0.0)), 3), "expected_win_rate_pct": 88.48,
            "message": (
                f"{self._sim_prefix()}⚠️ 關單止損 {symbol}｜第 {cur_step}/{max_steps} 碼 ({rec['pnl_u']:+.2f}U)\n"
                f"• 結算：${strike_twap:.2f} ➔ ${ending_twap:.2f}\n"
                f"{self._format_performance_summary_line()}"
            ),
        })
        if self.review_manager is not None:
            await self.review_manager.check_and_trigger_graduation_or_circuit_breaker()
        if provisional_oracle and market_slug and not ctx.get("from_oracle_waiter"):
            asyncio.create_task(self._reconcile_provisional_oracle(
                market_slug=market_slug, symbol=symbol, direction=direction, timestamp_ms=int(bucket_ts * 1000),
                provisional_won=False, shares=float(cur_sh), tot_cost=abs(tot_loss_u),
                cur_step=cur_step, max_steps=max_steps, seq_snapshot=dict(seq),
            ))
        return {"advanced": False, "plan": plan, "record": rec}

    def _restore_lanes_after_unfilled_entry(self, active_sym: str, active_seq: dict[str, Any] | None) -> None:
        """Reset risk_controller lanes after cancelling an unfilled order, or restore PENDING_RESCUE for Step >= 2."""
        if not (hasattr(self, "risk_controller") and self.risk_controller):
            return
        self.risk_controller.reset_all_lanes()
        if not isinstance(active_seq, dict):
            return
        cur_step = int(active_seq.get("step", 1) or 1)
        if cur_step >= 2:
            lane = self.risk_controller.lanes.get(active_sym)
            if lane:
                lane.status = "PENDING_RESCUE"
                lane.current_step = cur_step
                lane.pending_since = time.time()
                lane.cumulative_loss = float(active_seq.get("prior_cost_u", 0.0) or 0.0)
                lane.direction = active_seq.get("direction")

    async def _handle_synchronized_close(
        self,
        dfs: dict[str, pd.DataFrame],
        closed_idx: int,
        target_bucket_ts: int | None = None,
    ) -> None:
        active_sym, active_seq = self._get_active_global_sequence()
        tb_ratio = self.master_spec.target_bounce_pct / 100.0
        max_steps = int(getattr(self.master_spec, "max_martingale_steps", 4))
        bp_enabled = bool(getattr(self.master_spec, "bounce_protect_enabled", False))

        if active_sym is not None and active_seq is not None:
            closed_row = dfs[active_sym].iloc[closed_idx]
            seq_bucket_ts = int(active_seq.get("bucket_ts") or 0)
            closed_bucket_ts = int(closed_row["open_time"]) // 1000
            if seq_bucket_ts > 0 and closed_bucket_ts < seq_bucket_ts:
                return
            direction = active_seq["direction"]
            cur_step = int(active_seq["step"])
            is_long = direction == "LONG_BOUNCE"
            had_intra_bounce = False

            fill_state = await self.auto_trader.check_and_sync_position_fill(
                active_sym,
                bounce_triggered=bool(had_intra_bounce),
            )
            entry_matched = (
                bool(fill_state.get("entry_matched", False))
                or bool(active_seq.get("entry_verified_matched", False))
                or float(active_seq.get("bounce_sell_shares", 0.0)) >= 1.0
            )
            bounce_sell_matched = (
                bool(fill_state.get("bounce_sell_matched", False))
                or bool(active_seq.get("bounce_match_notified", False))
            )
            market_slug = str(fill_state.get("slug") or active_seq.get("slug") or "")

            # Case A: Entry BUY order never filled on Polymarket CLOB -> cancel & reset silently, zero fake PnL!
            if not entry_matched and not bounce_sell_matched:
                await self.auto_trader.cancel_active_orders_for_symbol(active_sym)
                if cur_step >= 2:
                    self.paper_trader.state["pending_step2"] = self._seq_to_pending_payload(active_seq)
                self.active_sequences[active_sym] = None
                self.auto_trader.active_clob_positions.pop(active_sym, None)
                self.paper_trader.state["active_position"] = None
                self.paper_trader.save()
                self._restore_lanes_after_unfilled_entry(active_sym, active_seq)
                active_sym, active_seq = None, None
            else:
                await self.auto_trader.cancel_active_orders_for_symbol(active_sym)

                bucket_ts = int(active_seq.get("bucket_ts") or (int(closed_row["open_time"]) // 1000))
                cl_open = self.chainlink_feed.get_bucket_open_price(active_sym, bucket_ts)
                strike_twap = float(
                    active_seq.get("official_price_to_beat")
                    or cl_open
                    or active_seq.get("strike_twap_60s")
                    or 0.0
                )
                cl_twap_end, cl_ticks = self.chainlink_feed.get_60s_twap(
                    active_sym,
                    end_ts_ms=(bucket_ts + 300) * 1000,
                    min_ticks=12,
                )
                ending_twap = float(cl_twap_end) if cl_twap_end is not None else strike_twap

                # V9.0 Pure Polymarket Chainlink Oracle Settlement (ZERO Binance price/TWAP dependency):
                # 1) Query Official Polymarket Gamma `/events` resolution;
                # 2) Else settle provisionally from real-time Chainlink RTDS 60s TWAP (>=12 ticks) & verify async;
                # 3) Else defer cleanly to background official Gamma settlement loop without touching Binance!
                polymarket_won = False
                provisional_oracle = False
                if not bounce_sell_matched:
                    pm_res = await self.auto_trader.query_live_market_resolution(market_slug, direction, max_attempts=3)
                    if pm_res.get("resolved"):
                        polymarket_won = bool(pm_res.get("polymarket_won"))
                        if pm_res.get("price_to_beat") is not None:
                            strike_twap = float(pm_res["price_to_beat"])
                        if pm_res.get("final_price") is not None:
                            ending_twap = float(pm_res["final_price"])
                    elif cl_twap_end is not None and strike_twap > 0.0 and cl_ticks >= 12:
                        ending_twap = float(cl_twap_end)
                        polymarket_won = bool(ending_twap >= strike_twap) if is_long else bool(ending_twap < strike_twap)
                        provisional_oracle = bool(market_slug)
                    else:
                        if market_slug and market_slug not in self._pending_oracle_settlements:
                            _, cur_sh_d, _, _, tot_c_d = self._compute_seq_costs(active_seq, cur_step)
                            sh_d = float(fill_state.get("entry_filled_shares") or cur_sh_d)
                            task_d = asyncio.create_task(
                                self._await_official_polymarket_settlement(
                                    market_slug=market_slug,
                                    symbol=active_sym,
                                    direction=direction,
                                    cur_step=cur_step,
                                    max_steps=max_steps,
                                    shares=sh_d,
                                    tot_cost=tot_c_d,
                                    seq_snapshot=dict(active_seq),
                                    timestamp_ms=int(bucket_ts * 1000),
                                )
                            )
                            self._pending_oracle_settlements[market_slug] = task_d
                        self.active_sequences[active_sym] = None
                        self.auto_trader.active_clob_positions.pop(active_sym, None)
                        self.paper_trader.state["active_position"] = None
                        self.paper_trader.save()
                        if hasattr(self, "risk_controller") and self.risk_controller:
                            self.risk_controller.reset_all_lanes()
                            lane_d = self.risk_controller.lanes.get(active_sym)
                            if lane_d:
                                lane_d.status = "ACTIVE_RESCUE"
                                lane_d.current_step = cur_step
                                lane_d.direction = direction
                        print(f"[V11.0 Chainlink Defer] {active_sym} ({market_slug}) awaiting official Polymarket Gamma/Chainlink settlement (ticks={cl_ticks}).")
                        return

                # Case B: Won via Stage-3 Surge-Lock OR won 100¢ via Official Polymarket Chainlink Oracle!
                if bounce_sell_matched or polymarket_won:
                    prev_tier = int(self.paper_trader.state.get("watermark_tier", 1))
                    default_b_cents = float(active_seq.get("bounce_sell_price_cents", 95.0))
                    exit_cents = default_b_cents if bounce_sell_matched else 100.0
                    exit_type = f"{int(round(exit_cents))}¢_SURGE_LOCK" if bounce_sell_matched else "100¢_EXPIRATION"

                    floor_u_b = float(getattr(self.master_spec, "emergency_capital_floor_u", 2.50))
                    _, cur_sh_b, _, _, tot_cost = self._compute_seq_costs(active_seq, cur_step)
                    sh = float(active_seq.get("bounce_sell_shares") or fill_state.get("entry_filled_shares") or cur_sh_b)
                    actual_pnl = round(sh * (exit_cents / 100.0) - tot_cost, 4)

                    rec = self.paper_trader.record_completed_sequence(
                        symbol=active_sym,
                        direction=direction,
                        entry_price=float(active_seq["entry_price"]),
                        resolve_price=ending_twap,
                        rsi_6=float(active_seq["entry_rsi_6"]),
                        streak=int(active_seq["initial_streak"]),
                        won_at_step=cur_step,
                        exit_cents=exit_cents,
                        exit_type=exit_type,
                        timestamp_ms=int(bucket_ts * 1000),
                        actual_pnl_u=actual_pnl,
                        trigger_branch=str(active_seq.get("trigger_branch", "")),
                        tier=str(active_seq.get("tier", "STANDARD")),
                        cvd_3=float(active_seq.get("cvd_3", 0.0)),
                        oracle_source="POLYMARKET_GAMMA_OFFICIAL" if not provisional_oracle else "POLYMARKET_CHAINLINK_RTDS",
                        actual_stake_u=float(active_seq.get("stake_usd", tot_cost)),
                        actual_entry_cents=float(active_seq.get("entry_share_cents", 50.0)),
                    )
                    self.active_sequences[active_sym] = None
                    self.auto_trader.active_clob_positions.pop(active_sym, None)
                    self.paper_trader.state["active_position"] = None
                    self.paper_trader.state["pending_step2"] = None
                    # Wave-Reset Protection: if won via mid-candle bounce while candle still closed against us, wait 1 candle (300s) so Step 1 never re-enters the same un-reset streak!
                    still_adverse_streak = (int(closed_row["streak_down"]) >= 1) if is_long else (int(closed_row["streak_up"]) >= 1)
                    self.paper_trader.state["cooldown_until_ms"] = ((int(bucket_ts) * 1000) + 310_000) if (bounce_sell_matched and still_adverse_streak) else 0

                    # Protect ledger equity from pre-redemption wallet cash dips while 100c winning tokens await relayer claim
                    if exit_cents == 100.0:
                        self.paper_trader.state["pending_redeem_until_ms"] = int(time.time() * 1000) + 600_000

                    # Sync real wallet balance immediately if bounce sell already returned pUSD
                    w_state = await self.auto_trader.sync_live_wallet_balance(include_portfolio=(exit_cents < 100.0))
                    if exit_cents < 100.0 and w_state.get("total_equity_pusd", 0.0) >= floor_u_b:
                        self.paper_trader.sync_with_live_wallet(
                            float(w_state["total_equity_pusd"]),
                            float(w_state.get("cash_pusd", 0.0)),
                            is_dry_run=self.auto_trader.config.dry_run,
                        )
                        if not self.auto_trader.config.dry_run and self.paper_trader.state.get("trade_log"):
                            real_tot_now = float(self.paper_trader.state.get("total_equity_u", rec["equity_after_u"]))
                            self.paper_trader.state["trade_log"][0]["equity_after_u"] = real_tot_now
                            rec["equity_after_u"] = real_tot_now
                            self.paper_trader.save()
                    else:
                        self.paper_trader.save()

                    # Trigger background auto-redeem check after 100¢ expiration win
                    if exit_cents == 100.0:
                        asyncio.create_task(self._delayed_post_win_redeem())

                    if hasattr(self, "risk_controller") and self.risk_controller:
                        self.risk_controller.reset_all_lanes()
                    if hasattr(self, "arena") and self.arena:
                        for trk in ["ALPHA", "BETA", "GAMMA"]:
                            self.arena.process_sequence_win(
                                track=trk,
                                symbol=active_sym,
                                direction=direction,
                                step=cur_step,
                                net_profit=round(float(rec["pnl_u"]), 2),
                                exit_type=exit_type,
                                cost=float(active_seq["stake_usd"]),
                                shares=float(active_seq.get("shares", 5.0)),
                            )

                    new_tier = int(self.paper_trader.state.get("watermark_tier", 1))
                    tier_up_note = (
                        f"\n🚀 晉升 Lv.{new_tier}：下輪首碼 ${self.paper_trader.state.get('next_step1_stake_u', 2.50):.2f}U"
                        if new_tier > prev_tier
                        else ""
                    )
                    tot_eq_disp = float(self.paper_trader.state.get("total_equity_u", rec["equity_after_u"]))
                    vlt_eq_disp = float(self.paper_trader.state.get("vault_locked_u", 0.0))
                    act_eq_disp = float(self.paper_trader.state.get("active_equity_u", tot_eq_disp))
                    await self.notifier.dispatch_signal({
                        "symbol": active_sym,
                        "event_type": "SEQUENCE_WIN",
                        "direction": direction,
                        "step": cur_step,
                        "max_steps": max_steps,
                        "stake_usd": active_seq["stake_usd"],
                        "price": float(closed_row["close"]),
                        "rsi": round(float(closed_row["rsi_6"]), 2),
                        "streak": active_seq["initial_streak"],
                        "streak_ret_pct": round(float(closed_row["streak_ret_pct"]), 3),
                        "expected_win_rate_pct": 88.48,
                        "message": (
                            f"{self._sim_prefix()}✅ 關單勝出 {active_sym}｜第 {cur_step}/{max_steps} 碼 (+${rec['pnl_u']:.2f}U)\n"
                            f"• 結算：${strike_twap:.2f} ➔ ${ending_twap:.2f} ({int(exit_cents)}¢ 到期)\n"
                            f"{self._format_performance_summary_line()}{tier_up_note}"
                        ),
                    })
                    if self.review_manager is not None and not provisional_oracle:
                        await self.review_manager.check_and_trigger_graduation_or_circuit_breaker()
                    if provisional_oracle and not bounce_sell_matched and market_slug:
                        asyncio.create_task(
                            self._reconcile_provisional_oracle(
                                market_slug=market_slug,
                                symbol=active_sym,
                                direction=direction,
                                timestamp_ms=int(bucket_ts * 1000),
                                provisional_won=True,
                                shares=sh,
                                tot_cost=tot_cost,
                                cur_step=cur_step,
                                max_steps=max_steps,
                                seq_snapshot=dict(active_seq),
                            )
                        )
                    active_sym, active_seq = None, None
                else:
                    # Case C: Official Polymarket Chainlink Oracle settlement loss -> V11.0 4-Step Guaranteed-Profit progression or bust
                    await self._handle_martingale_loss_progression(
                        active_seq,
                        {
                            "symbol": active_sym,
                            "direction": direction,
                            "cur_step": cur_step,
                            "max_steps": max_steps,
                            "bucket_ts": bucket_ts,
                            "strike_twap": strike_twap,
                            "ending_twap": ending_twap,
                            "provisional_oracle": provisional_oracle,
                            "market_slug": market_slug,
                            "bust_rsi_6": float(closed_row["rsi_6"]),
                            "bust_streak": int(closed_row["streak_down"] if is_long else closed_row["streak_up"]),
                            "bust_streak_ret_pct": float(closed_row["streak_ret_pct"]),
                            "bust_close_price": float(closed_row["close"]),
                            "from_oracle_waiter": False,
                        },
                    )
                    active_sym, active_seq = None, None

        cooldown_until_ms = int(self.paper_trader.state.get("cooldown_until_ms", 0) or 0)
        if cooldown_until_ms > 0 and int(time.time() * 1000) < cooldown_until_ms:
            return
        if self._pending_oracle_settlements:
            return

        # Idle state (and not waiting on a mid-sequence rescue bullet): sync live wallet balance before sizing new sequence
        floor_u = float(getattr(self.master_spec, "emergency_capital_floor_u", 2.50))
        micro_gate_u = float(getattr(self.master_spec, "micro_equity_tier_s_only_u", 24.0))
        has_pending_rescue = isinstance(self.paper_trader.state.get("pending_step2"), dict) and bool(
            self.paper_trader.state["pending_step2"].get("stake_usd")
        )
        if not has_pending_rescue:
            prev_tier = int(self.paper_trader.state.get("watermark_tier", 1))
            wallet_age_ms = int(time.time() * 1000) - int(self.auto_trader.live_wallet_state.get("updated_at_ms", 0) or 0)
            if wallet_age_ms > 45_000 or float(self.auto_trader.live_wallet_state.get("total_equity_pusd", 0.0)) <= 0.0:
                try:
                    w_state = await asyncio.wait_for(self.auto_trader.sync_live_wallet_balance(), timeout=3.0)
                except Exception:
                    w_state = self.auto_trader.live_wallet_state
            else:
                w_state = self.auto_trader.live_wallet_state
            if w_state.get("total_equity_pusd", 0.0) >= floor_u:
                self.paper_trader.sync_with_live_wallet(
                    float(w_state["total_equity_pusd"]),
                    float(w_state.get("cash_pusd", 0.0)),
                    is_dry_run=self.auto_trader.config.dry_run,
                )
                new_tier = int(self.paper_trader.state.get("watermark_tier", 1))
                if new_tier > prev_tier:
                    await self._notify_watermark_upgrade(prev_tier, new_tier)

        # Evaluate V12.0 Pure Polymarket Chainlink Oracle + Multi-Bar Exhaustion Signal
        tot_eq_chk = float(self.paper_trader.state.get("total_equity_u", self.paper_trader.state.get("active_equity_u", 40.62)))
        vault_chk = float(self.paper_trader.state.get("vault_locked_u", 0.0))
        act_eq_chk = max(0.0, tot_eq_chk - vault_chk)
        act_pk_chk = float(self.paper_trader.state.get("active_peak_u", act_eq_chk))
        if act_eq_chk < floor_u:
            print(f"[V9.0 Exchange Floor] Active equity ${act_eq_chk:.2f}U < ${floor_u:.2f}U (5 shares @ 50¢ min). Halting new entries.")
            return

        pending_s2 = self.paper_trader.state.get("pending_step2") if max_steps > 1 else None
        entry_step = int(pending_s2.get("step", 2)) if isinstance(pending_s2, dict) and pending_s2.get("stake_usd") else 1
        cand = evaluate_master_signal_at_index(dfs, closed_idx, self.master_spec, entry_step=entry_step)
        if cand is not None:
            sym = cand["symbol"]
            is_tier_s_cand = bool(cand.get("is_tier_s", False))
            is_tier_s_plus_cand = bool(cand.get("is_tier_s_plus", False))
            micro_gate_u = float(getattr(self.master_spec, "micro_equity_tier_s_only_u", 0.0))
            if entry_step == 1 and micro_gate_u > 0.0 and act_eq_chk < micro_gate_u and not (is_tier_s_cand or is_tier_s_plus_cand):
                print(
                    f"[V12.0 Micro-Equity Gate] Active ${act_eq_chk:.2f}U < ${micro_gate_u:.2f}U: "
                    f"skipping Step-1 STANDARD {sym} {cand['direction']} (restricted to TIER_S/TIER_S+ only)."
                )
                return

            # Cross-Coin Lane Check
            if hasattr(self, "risk_controller") and self.risk_controller:
                if entry_step == 1 and not self.risk_controller.can_open_step1(sym):
                    return
                if entry_step >= 2 and not self.risk_controller.can_open_rescue(sym):
                    return

            now_bucket_ts = (int(time.time()) // 300) * 300
            cur_bucket_ts = int(target_bucket_ts) if target_bucket_ts is not None else now_bucket_ts
            req_offset = 1 if cur_bucket_ts > now_bucket_ts else 0
            official_ptb = None
            mkt_now = None
            try:
                mkt_now = await self.auto_trader.discover_live_5m_market(
                    sym, bucket_offset=req_offset, target_bucket_ts=cur_bucket_ts, fast_cached_only=True
                )
                if mkt_now is None:
                    mkt_now = await self.auto_trader.discover_live_5m_market(
                        sym, bucket_offset=req_offset, target_bucket_ts=cur_bucket_ts, fast_cached_only=False
                    )
                if mkt_now and mkt_now.get("price_to_beat") is not None:
                    official_ptb = float(mkt_now["price_to_beat"])
            except Exception as exc:
                print(f"[Scanner Warning] discover official price_to_beat for {sym} failed: {exc}")
            is_long_cand = cand["direction"] == "LONG_BOUNCE"

            # V9.0 100% Pure Polymarket Chainlink Oracle Alignment Check (ZERO Binance price/TWAP usage!)
            cl_live, cl_age = self.chainlink_feed.get_latest_price(sym, max_age_sec=20.0)
            if cl_live is None:
                print(f"[V9.0 Chainlink Guard] Skipped {sym} {cand['direction']}: No live Chainlink RTDS tick (age={cl_age:.1f}s)")
                return
            cl_twap_open, _ = self.chainlink_feed.get_60s_twap(sym, end_ts_ms=cur_bucket_ts * 1000, min_ticks=5)
            cl_open = self.chainlink_feed.get_bucket_open_price(sym, cur_bucket_ts)
            if official_ptb is not None:
                ref_oracle_p = float(official_ptb)
            elif cur_bucket_ts <= now_bucket_ts and cl_open is not None:
                ref_oracle_p = float(cl_open)
            elif cl_live is not None:
                ref_oracle_p = float(cl_live)
            else:
                ref_oracle_p = float(cl_twap_open or 0.0)
            max_gap = float(getattr(self.master_spec, "max_adverse_ptb_gap_pct", 0.12))
            if is_long_cand:
                adverse_gap_pct = ((ref_oracle_p - cl_live) / max(ref_oracle_p, 1e-9)) * 100.0
            else:
                adverse_gap_pct = ((cl_live - ref_oracle_p) / max(ref_oracle_p, 1e-9)) * 100.0
            if adverse_gap_pct > max_gap:
                print(
                    f"[V9.0 Chainlink Filter] Skipped {sym} {cand['direction']}: "
                    f"chainlink_live={cl_live:.4f}, oracle_ptb={ref_oracle_p:.4f}, "
                    f"adverse_behind_gap={adverse_gap_pct:+.3f}% > +{max_gap:.2f}%"
                )
                return
            # Reject if Polymarket Chainlink live implied probability on our side already collapsed < 43¢
            if mkt_now:
                our_gamma_cents = float(mkt_now.get("up_price_cents", 50.0) if is_long_cand else mkt_now.get("down_price_cents", 50.0))
                if 1.0 <= our_gamma_cents < 43.0:
                    print(
                        f"[V9.0 Oracle Guard] Skipped {sym} {cand['direction']}: "
                        f"Polymarket live {cand['polymarket_side']} price already dropped to {our_gamma_cents:.1f}¢ (<43¢)"
                    )
                    return

            stakes = compute_step_stakes(
                tot_eq_chk,
                self.master_spec,
                vault_locked_u=vault_chk,
                is_tier_s=is_tier_s_cand,
                active_peak_u=act_pk_chk,
                is_tier_s_plus=is_tier_s_plus_cand,
            )
            if stakes[0] < floor_u:
                return
            entry_c_d = float(getattr(self.master_spec, "entry_share_cents", 50.0)) / 100.0
            s1_already_deducted = False
            prior_cost_u = 0.0
            prior_shares_lost = 0.0
            s1_shares_base = round(float(stakes[0]) / max(entry_c_d, 0.01), 2)
            if isinstance(pending_s2, dict) and pending_s2.get("stake_usd"):
                entry_stake = float(pending_s2["stake_usd"])
                planned_stakes = pending_s2.get("planned_stakes", [round(x, 2) for x in stakes])
                s1_already_deducted = bool(pending_s2.get("s1_already_deducted", False))
                prior_cost_u = float(pending_s2.get("prior_cost_u", 0.0))
                prior_shares_lost = float(pending_s2.get("prior_shares_lost", round(prior_cost_u / max(entry_c_d, 0.01), 2)))
                s1_shares_base = float(pending_s2.get("s1_shares_base", round(float(planned_stakes[0]) / max(entry_c_d, 0.01), 2)))
            else:
                entry_step = 1
                entry_stake = stakes[0]
                planned_stakes = [round(x, 2) for x in stakes]

            dd_thresh = float(getattr(self.master_spec, "dd_compress_threshold_pct", 15.0)) / 100.0
            dd_mult = float(getattr(self.master_spec, "dd_compress_multiplier", 0.55))
            wm_label = self.paper_trader.state.get("watermark_label", "階段I・V11保利馬丁起跑")
            if is_tier_s_plus_cand:
                tier_tag = "💎TIER_S+ (91.8% SeqWR | 11.5%)"
            elif is_tier_s_cand:
                tier_tag = "🔥TIER_S (90.5% SeqWR | 9.0%)"
            else:
                tier_tag = "📊STANDARD (88.2% SeqWR | 5.0%)"
            dd_tag = f" [📉回撤壓縮{dd_mult:.2f}x]" if act_eq_chk < (1.0 - dd_thresh) * act_pk_chk else ""
            eng_tag = f"V11.0・{tier_tag}{dd_tag}"
            strike_twap_60s = ref_oracle_p

            self._last_scan_time = time.time()
            try:
                clob_rec = await self.auto_trader.execute_5m_entry(
                    symbol=sym,
                    direction=cand["direction"],
                    stake_usd=entry_stake,
                    step=entry_step,
                    reason="SIGNAL_ENTRY" if entry_step == 1 else f"V11_MARTINGALE_STEP{entry_step}",
                    vault_locked_u=vault_chk,
                    prior_cost_u=prior_cost_u,
                    s1_shares_base=s1_shares_base,
                    market=mkt_now,
                    target_bucket_ts=cur_bucket_ts,
                )
            except Exception as exc:
                self._last_scan_time = time.time()
                print(f"[V11.0 Entry Error] {sym} {cand['direction']} execute_5m_entry failed: {exc}")
                return
            self._last_scan_time = time.time()
            st_clob = str(clob_rec.get("status", ""))
            if not (st_clob.startswith("LIVE_") or st_clob.startswith("SIM_")):
                if st_clob.startswith("BLOCK_") or st_clob.startswith("SKIPPED_"):
                    print(f"[V11.0 Entry Guard] {sym} {cand['direction']} skipped cleanly: {st_clob} ({clob_rec.get('error', '')})")
                elif self.vpn_guardian is not None:
                    asyncio.create_task(self.vpn_guardian.probe_all_nodes())
                return

            if hasattr(self, "risk_controller") and self.risk_controller:
                self.risk_controller.reset_all_lanes()
                lane = self.risk_controller.lanes.get(sym)
                if lane:
                    lane.status = "ACTIVE_STEP1" if entry_step == 1 else "ACTIVE_RESCUE"
                    lane.current_step = entry_step
                    lane.active_cost = clob_rec["effective_cost_u"]
                    lane.direction = cand["direction"]
                    lane.price_to_beat = strike_twap_60s

            if entry_step == 1:
                s1_shares_base = float(clob_rec.get("shares") or s1_shares_base)
            is_s1_matched = "MATCHED" in str(clob_rec.get("status", ""))
            clob_tag = f" [CLOB V2: {clob_rec['status']} | {clob_rec['shares']}份 (${clob_rec['effective_cost_u']:.2f}U) | Chainlink基準=${strike_twap_60s:.2f}]"
            s1_sig_payload = {
                "symbol": sym,
                "event_type": "SIGNAL_ENTRY" if entry_step == 1 else "MARTINGALE_ADD",
                "direction": cand["direction"],
                "step": entry_step,
                "max_steps": max_steps,
                "stake_usd": clob_rec["effective_cost_u"],
                "price": cl_live,
                "rsi": cand["rsi_6"],
                "streak": cand["streak"],
                "streak_ret_pct": cand["streak_ret_pct"],
                "expected_win_rate_pct": 88.48,
                "message": (
                    f"{self._sim_prefix()}{'🟢 開單進場' if entry_step == 1 else '🔥 加碼開單'} {sym} {cand['polymarket_side']}｜第 {entry_step}/{max_steps} 碼 ({eng_tag})\n"
                    f"• 注碼：${clob_rec['effective_cost_u']:.2f}U ({clob_rec['shares']:g}份 @ {float(clob_rec.get('price_cents', 50.0)):g}¢)｜基準：${strike_twap_60s:,.2f}\n"
                    f"• 訊號：{cand.get('trigger_branch', '')} ({cand.get('tier', 'STD')}) | RSI 2/6/14={cand.get('rsi_2', 50.0):.0f}/{cand['rsi_6']:.1f}/{cand.get('rsi_14', 50.0):.0f}\n"
                    f"{self._format_performance_summary_line()}"
                ),
            }

            seq_obj = {
                "symbol": sym,
                "direction": cand["direction"],
                "polymarket_side": cand["polymarket_side"],
                "trigger_branch": cand.get("trigger_branch", ""),
                "tier": cand.get("tier", "STANDARD"),
                "cvd_3": float(cand.get("cvd_3", 0.0)),
                "entry_share_cents": round(float(clob_rec.get("price", 0.50)) * 100.0, 2),
                "step": entry_step,
                "max_steps": max_steps,
                "stake_usd": clob_rec["effective_cost_u"],
                "shares": clob_rec["shares"],
                "s1_shares_base": s1_shares_base,
                "prior_cost_u": prior_cost_u,
                "prior_shares_lost": prior_shares_lost,
                "slug": clob_rec.get("slug", ""),
                "condition_id": str(clob_rec.get("condition_id", "")),
                "token_id": str(clob_rec.get("token_id", "")),
                "bucket_ts": cur_bucket_ts,
                "strike_twap_60s": strike_twap_60s,
                "official_price_to_beat": official_ptb,
                "official_ptb_synced": official_ptb is not None,
                "planned_stakes": planned_stakes,
                "s1_already_deducted": s1_already_deducted,
                "entry_price": strike_twap_60s,
                "entry_rsi_6": cand["rsi_6"],
                "initial_streak": cand["streak"],
                "started_ts": int(cur_bucket_ts * 1000),
                "entry_order_id": clob_rec.get("order_id"),
                "entry_status": clob_rec.get("status"),
                "entry_verified_matched": is_s1_matched,
                "entry_notify_pending": not is_s1_matched,
                "pending_entry_signal": None if is_s1_matched else s1_sig_payload,
                "bounce_sell_placed": False,
                "bounce_match_notified": False,
            }
            self.active_sequences[sym] = seq_obj
            self.paper_trader.state["active_position"] = seq_obj
            self.paper_trader.state["pending_step2"] = None
            self.paper_trader.save()

            if is_s1_matched:
                await self.notifier.dispatch_signal(s1_sig_payload)

    async def _await_official_polymarket_settlement(
        self,
        market_slug: str,
        symbol: str,
        direction: str,
        cur_step: int,
        max_steps: int,
        shares: float,
        tot_cost: float,
        seq_snapshot: dict[str, Any],
        timestamp_ms: int,
    ) -> None:
        """
        V9.0 Pure Polymarket Official Settlement Waiter:
        Used only when Gamma `/events` has not yet marked `resolved` AND Chainlink RTDS
        collected fewer than 12 end-window ticks (e.g. cold startup). Never falls back to Binance!
        """
        try:
            pm_res: dict[str, Any] = {}
            for _ in range(24):
                pm_res = await self.auto_trader.query_live_market_resolution(market_slug, direction, max_attempts=2)
                if pm_res.get("resolved"):
                    break
                await asyncio.sleep(15.0)

            strike_twap = float(pm_res.get("price_to_beat") or seq_snapshot.get("strike_twap_60s") or 0.0)
            if pm_res.get("resolved"):
                actual_won = bool(pm_res.get("polymarket_won"))
                ending_twap = float(pm_res.get("final_price") or strike_twap)
            else:
                bucket_ts = int(seq_snapshot.get("bucket_ts") or (timestamp_ms // 1000))
                cl_end, _ = self.chainlink_feed.get_60s_twap(symbol, end_ts_ms=(bucket_ts + 300) * 1000, min_ticks=1)
                if cl_end is not None and strike_twap > 0.0:
                    ending_twap = float(cl_end)
                    actual_won = bool(ending_twap >= strike_twap) if direction == "LONG_BOUNCE" else bool(ending_twap < strike_twap)
                else:
                    # Unknown timeout without resolution -> release lock cleanly without fake PnL
                    print(f"[V9.0 Oracle Timeout] {symbol} ({market_slug}) unresolved after 6m; releasing lock.")
                    return

            floor_u = float(getattr(self.master_spec, "emergency_capital_floor_u", 2.50))
            if not actual_won:
                bucket_ts_res = int(seq_snapshot.get("bucket_ts") or (timestamp_ms // 1000))
                await self._handle_martingale_loss_progression(
                    seq_snapshot,
                    {
                        "symbol": symbol,
                        "direction": direction,
                        "cur_step": cur_step,
                        "max_steps": max_steps,
                        "bucket_ts": bucket_ts_res,
                        "strike_twap": strike_twap,
                        "ending_twap": ending_twap,
                        "provisional_oracle": not bool(pm_res.get("resolved")),
                        "market_slug": market_slug,
                        "from_oracle_waiter": True,
                    },
                )
                return

            actual_pnl = round(shares * 1.0 - tot_cost, 4)
            rec = self.paper_trader.record_completed_sequence(
                symbol=symbol,
                direction=direction,
                entry_price=float(seq_snapshot.get("entry_price", strike_twap)),
                resolve_price=ending_twap,
                rsi_6=float(seq_snapshot.get("entry_rsi_6", 50.0)),
                streak=int(seq_snapshot.get("initial_streak", 3)),
                won_at_step=cur_step,
                exit_cents=100.0,
                exit_type="100¢_EXPIRATION",
                timestamp_ms=timestamp_ms,
                actual_pnl_u=actual_pnl,
                trigger_branch=str(seq_snapshot.get("trigger_branch", "")),
                tier=str(seq_snapshot.get("tier", "STANDARD")),
                cvd_3=float(seq_snapshot.get("cvd_3", 0.0)),
                oracle_source="POLYMARKET_GAMMA_OFFICIAL" if pm_res.get("resolved") else "POLYMARKET_CHAINLINK_RTDS",
                actual_stake_u=float(seq_snapshot.get("stake_usd", tot_cost)),
                actual_entry_cents=float(seq_snapshot.get("entry_share_cents", 50.0)),
            )
            self.paper_trader.state["pending_redeem_until_ms"] = int(time.time() * 1000) + 600_000
            self.paper_trader.save()
            asyncio.create_task(self._delayed_post_win_redeem())
            if hasattr(self, "risk_controller") and self.risk_controller:
                self.risk_controller.reset_all_lanes()
            w_now = await self.auto_trader.sync_live_wallet_balance()
            if w_now.get("total_equity_pusd", 0.0) >= floor_u:
                self.paper_trader.sync_with_live_wallet(
                    float(w_now["total_equity_pusd"]),
                    float(w_now.get("cash_pusd", 0.0)),
                    is_dry_run=self.auto_trader.config.dry_run,
                )
            else:
                self.paper_trader.save()
            if self.review_manager is not None:
                await self.review_manager.check_and_trigger_graduation_or_circuit_breaker()
            await self.notifier.dispatch_signal({
                "symbol": symbol,
                "event_type": "SEQUENCE_WIN",
                "direction": direction,
                "step": cur_step,
                "max_steps": max_steps,
                "stake_usd": float(seq_snapshot.get("stake_usd", tot_cost)),
                "price": ending_twap,
                "rsi": round(float(seq_snapshot.get("entry_rsi_6", 50.0)), 2),
                "streak": int(seq_snapshot.get("initial_streak", 3)),
                "streak_ret_pct": 0.0,
                "expected_win_rate_pct": 88.48,
                "message": (
                    f"{self._sim_prefix()}✅ 關單勝出 {symbol}｜第 {cur_step}/{max_steps} 碼 ({rec['pnl_u']:+.2f}U)\n"
                    f"• 結算：${strike_twap:.2f} ➔ ${ending_twap:.2f} (100¢ 到期)\n"
                    f"{self._format_performance_summary_line()}"
                ),
            })
        except Exception as exc:
            print(f"[V9.0 Oracle Waiter Error] {symbol} ({market_slug}): {exc}")
        finally:
            self._pending_oracle_settlements.pop(market_slug, None)
            self.active_sequences[symbol] = None
            self.auto_trader.active_clob_positions.pop(symbol, None)
            self.paper_trader.state["active_position"] = None
            self.paper_trader.save()
            if self._get_active_global_sequence()[1] is None and hasattr(self, "risk_controller") and self.risk_controller:
                has_stuck_active = any(
                    ln.status in ("ACTIVE_STEP1", "ACTIVE_RESCUE")
                    for ln in self.risk_controller.lanes.values()
                )
                if has_stuck_active:
                    pending_s2 = self.paper_trader.state.get("pending_step2")
                    if isinstance(pending_s2, dict) and pending_s2.get("symbol"):
                        p_sym = str(pending_s2["symbol"])
                        prev_ln = self.risk_controller.lanes.get(p_sym)
                        prev_dir = prev_ln.direction if prev_ln else None
                        self.risk_controller.reset_all_lanes()
                        p_lane = self.risk_controller.lanes.get(p_sym)
                        if p_lane:
                            p_lane.status = "PENDING_RESCUE"
                            p_lane.current_step = int(pending_s2.get("step", 2) or 2)
                            p_lane.pending_since = float(pending_s2.get("paused_at_ms", 0) or 0) / 1000.0 or time.time()
                            p_lane.cumulative_loss = float(pending_s2.get("prior_cost_u", 0.0) or 0.0)
                            p_lane.direction = prev_dir
                    else:
                        self.risk_controller.reset_all_lanes()

    async def _reconcile_provisional_oracle(
        self,
        market_slug: str,
        symbol: str,
        direction: str,
        timestamp_ms: int,
        provisional_won: bool,
        shares: float,
        tot_cost: float,
        cur_step: int = 1,
        max_steps: int = 4,
        seq_snapshot: dict[str, Any] | None = None,
    ) -> None:
        """
        V11.0 Async Polymarket Chainlink Oracle Reconciliation:
        Waits for Gamma `/events` official Chainlink resolution on borderline/lagging closes,
        corrects `paper_trader`, and safely advances multi-step Martingale rescue if a Step < max_steps
        provisional win flips to an official loss.
        """
        try:
            await asyncio.sleep(45.0)
            pm_res = await self.auto_trader.query_live_market_resolution(market_slug, direction, max_attempts=6)
            if pm_res.get("resolved") and pm_res.get("final_price") is None:
                await asyncio.sleep(20.0)
                pm_res2 = await self.auto_trader.query_live_market_resolution(market_slug, direction, max_attempts=3)
                if pm_res2.get("resolved"):
                    pm_res = pm_res2
            if not pm_res.get("resolved"):
                return
            actual_won = bool(pm_res.get("polymarket_won"))
            ending_p = float(pm_res.get("final_price") or 0.0)
            ptb_p = float(pm_res.get("price_to_beat") or 0.0)
            if (
                provisional_won
                and not actual_won
                and cur_step < max_steps
                and isinstance(seq_snapshot, dict)
                and self._get_active_global_sequence()[0] is None
                and not self.paper_trader.state.get("pending_step2")
            ):
                reverted = self.paper_trader.revert_provisional_win_for_rescue(
                    timestamp_ms=timestamp_ms, symbol=symbol
                )
                if reverted is not None:
                    await self._handle_martingale_loss_progression(
                        seq_snapshot,
                        {
                            "symbol": symbol,
                            "direction": direction,
                            "cur_step": cur_step,
                            "max_steps": max_steps,
                            "bucket_ts": int(seq_snapshot.get("bucket_ts") or (timestamp_ms // 1000)),
                            "strike_twap": ptb_p if ptb_p > 0.0 else float(seq_snapshot.get("strike_twap_60s") or 0.0),
                            "ending_twap": ending_p,
                            "provisional_oracle": False,
                            "market_slug": market_slug,
                            "from_oracle_waiter": True,
                        },
                    )
                    return

            actual_pnl = round((shares * 1.0 - tot_cost) if actual_won else (-tot_cost), 4)
            adj = self.paper_trader.reconcile_trade_outcome(
                timestamp_ms=timestamp_ms,
                symbol=symbol,
                actual_won=actual_won,
                resolve_price=ending_p,
                actual_pnl_u=actual_pnl,
                price_to_beat=ptb_p if ptb_p > 0.0 else None,
                step=cur_step,
            )
            if self.review_manager is not None:
                await self.review_manager.check_and_trigger_graduation_or_circuit_breaker()
            if adj and getattr(self.notifier.config, "telegram_notify_orders", False) and not self._legacy_sim_only():
                res_txt = "勝出 (100¢)" if actual_won else "未過線 (0¢)"
                await self.notifier.send_telegram_text(
                    f"⚖️ 對帳修正 {symbol}｜{res_txt}\n"
                    f"• 基準 ${ptb_p:.2f} ➔ 收盤 ${ending_p:.2f}\n"
                    f"• 修正：{adj['pnl_delta']:+.2f}U ➔ 權益 ${adj['new_total_u']:.2f}U",
                    reply_markup=TG_MAIN_KEYBOARD,
                )
        except Exception as exc:
            print(f"[Scanner Warning] reconcile provisional oracle failed for {symbol}: {exc}")

    def _legacy_sim_only(self) -> bool:
        """True when V11 simulation notifications are explicitly muted in notify_config."""
        return bool(getattr(self.notifier.config, "mute_legacy_simulation", False))

    async def _notify_watermark_upgrade(self, old_tier: int, new_tier: int) -> None:
        if not getattr(self.notifier.config, "telegram_notify_orders", False) or self._legacy_sim_only():
            return
        st = self.paper_trader.state
        await self.notifier.send_telegram_text(
            f"🚀 水位晉升 Lv.{old_tier} ➔ Lv.{new_tier}\n"
            f"• 權益：${st.get('total_equity_u', 40.62):.2f}U（作戰 ${st.get('active_equity_u', 40.62):.2f}U / 鎖倉 ${st.get('vault_locked_u', 0.0):.0f}U）\n"
            f"• 注碼：S1 ${st.get('next_step1_stake_u', 2.50):.2f}U ({st.get('next_step1_shares', 5.0):g}份) / S2 ${st.get('next_step2_stake_u', 3.50):.2f}U",
            reply_markup=TG_MAIN_KEYBOARD,
        )

    async def _delayed_post_win_redeem(self) -> None:
        # Wait 90s and 180s after a 5m candle expiration win to auto-redeem once Chainlink resolves
        for delay in (60, 90, 120):
            await asyncio.sleep(delay)
            await self._check_and_notify_redeem()

    async def _check_and_notify_redeem(self) -> dict[str, Any]:
        prev_tier = int(self.paper_trader.state.get("watermark_tier", 1))
        res = await self.auto_trader.auto_redeem_winning_positions()
        claimed = float(res.get("claimed_pusd", 0.0))
        if claimed > 0:
            self.paper_trader.state["pending_redeem_until_ms"] = 0
        w_state = await self.auto_trader.sync_live_wallet_balance(include_portfolio=True)
        has_pending_seq = isinstance(self.paper_trader.state.get("pending_step2"), dict) and bool(
            self.paper_trader.state["pending_step2"].get("stake_usd")
        )
        floor_u_r = float(getattr(self.master_spec, "emergency_capital_floor_u", 2.50))
        if w_state.get("total_equity_pusd", 0.0) >= floor_u_r and self._get_active_global_sequence()[0] is None and not has_pending_seq:
            self.paper_trader.sync_with_live_wallet(
                float(w_state["total_equity_pusd"]),
                float(w_state.get("cash_pusd", 0.0)),
                is_dry_run=self.auto_trader.config.dry_run,
            )
            new_tier = int(self.paper_trader.state.get("watermark_tier", 1))
            if new_tier > prev_tier:
                await self._notify_watermark_upgrade(prev_tier, new_tier)
        if claimed > 0 and getattr(self.notifier.config, "telegram_notify_orders", False):
            await self.notifier.send_telegram_text(
                f"💰 自動贖回 +${claimed:.2f} pUSD\n"
                f"• 現金：${w_state.get('cash_pusd', 0.0):.2f}U｜真實總權益：${w_state.get('total_equity_pusd', 0.0):.2f}U",
                reply_markup=TG_MAIN_KEYBOARD,
            )
        return res

    async def _run_wallet_and_redeem_loop(self) -> None:
        while self.running:
            try:
                await self._check_and_notify_redeem()
            except Exception as exc:
                print(f"[Scanner Warning] periodic redeem check error: {exc}")
            await asyncio.sleep(35.0)

    def _build_tg_status_text(self) -> str:
        st_acc = self.paper_trader.state
        mode_tag = "LIVE" if (self.auto_trader.config.enabled and not self.auto_trader.config.dry_run) else "DRY-RUN"
        geo = self.auto_trader.last_geoblock_status
        lines = [
            f"⚡ V11.0 戰情｜{mode_tag} ({geo.get('country', 'HK')} {geo.get('ip', '')})",
            f"• 權益：${st_acc.get('total_equity_u', 40.62):.2f}U（作戰 ${st_acc.get('active_equity_u', 40.62):.2f}U / 鎖倉 ${st_acc.get('vault_locked_u', 0.0):.0f}U）",
            f"• 共振：RSI(6)={self.resonance_state.get('mean_rsi_6', 50.0):.1f} | 跌:{self.resonance_state.get('sync_down_coins', 0)} 漲:{self.resonance_state.get('sync_up_coins', 0)}",
        ]
        for sym in self.symbols:
            st = self.live_state.get(sym)
            if not st:
                continue
            fc = st["forming_candle"]
            sd, su = fc["streak_down"], fc["streak_up"]
            sk = f"跌{sd}" if sd > 0 else (f"漲{su}" if su > 0 else "平")
            lines.append(f"• {sym[:3]} ${st['price']:,.2f} | R6={fc['rsi_6']:.1f} | {sk} | TBR={fc['taker_buy_ratio']*100:.0f}% | {st['seconds_to_close']}s")
        active_sym, active_seq = self._get_active_global_sequence()
        if active_sym and active_seq:
            lines.append(f"• 持倉：{active_sym} {active_seq['polymarket_side']} 第{active_seq['step']}/4碼 ${active_seq['stake_usd']:.2f}U")
        elif isinstance(st_acc.get("pending_step2"), dict) and st_acc["pending_step2"].get("stake_usd"):
            ps2 = st_acc["pending_step2"]
            lines.append(f"• 待命：第 {ps2.get('step', 2)}/4 碼救援 ${float(ps2['stake_usd']):.2f}U ({ps2.get('shares', 7.0):g}份)")
        else:
            lines.append(f"• 空倉：下輪首碼 ${st_acc.get('next_step1_stake_u', 2.50):.2f}U ({st_acc.get('next_step1_shares', 5.0):g}份)")
        return "\n".join(lines)

    def _build_tg_ladder_text(self) -> str:
        st = self.paper_trader.state
        cur_tier = int(st.get("watermark_tier", 1))
        tot_eq = float(st.get("total_equity_u", st.get("active_equity_u", 40.62)))
        vault_u = float(st.get("vault_locked_u", 0.0))
        act_eq = float(st.get("active_equity_u", max(0.0, tot_eq - vault_u)))
        entry_c = float(getattr(self.master_spec, "entry_share_cents", 50.0)) / 100.0
        lines = [
            f"📈 V11.0 階梯表｜總 ${tot_eq:.2f}U (作戰 ${act_eq:.2f}U / 鎖倉 ${vault_u:.0f}U)",
            f"• 當前：Lv.{cur_tier} | 鎖利階梯 $220➔$115 / $1k➔$615 / $5k➔$3,115 / $20k➔$13,115",
        ]
        for row in WATERMARK_LADDER:
            t = int(row["tier"])
            if t == 0 and cur_tier >= 1:
                continue
            s1_u = round(row["step1_shares"] * entry_c, 2)
            mark = "👉" if t == cur_tier else ("✅" if t < cur_tier else "⬜")
            lines.append(f"{mark} {row['label']} | 首碼 ${s1_u:.2f}U ({row['step1_shares']:g}份)")
        return "\n".join(lines)

    async def _build_tg_wallet_text(self) -> str:
        w = await self.auto_trader.sync_live_wallet_balance()
        floor_u_w = float(getattr(self.master_spec, "emergency_capital_floor_u", 2.50))
        has_pending_seq = isinstance(self.paper_trader.state.get("pending_step2"), dict) and bool(
            self.paper_trader.state["pending_step2"].get("stake_usd")
        )
        if w.get("total_equity_pusd", 0.0) >= floor_u_w and self._get_active_global_sequence()[0] is None and not has_pending_seq:
            self.paper_trader.sync_with_live_wallet(
                float(w["total_equity_pusd"]),
                float(w.get("cash_pusd", 0.0)),
                is_dry_run=self.auto_trader.config.dry_run,
            )
        st = self.paper_trader.state
        init_u = float(st.get("initial_u", 40.62))
        tot_eq = float(w.get("total_equity_pusd", w.get("cash_pusd", 40.62)))
        vault_u = float(st.get("vault_locked_u", 0.0))
        act_eq = max(0.0, tot_eq - vault_u)
        next_ms_u = float(st.get("next_vault_milestone_u", 220.0))
        net_gain = tot_eq - init_u
        net_gain_pct = (net_gain / max(init_u, 0.01)) * 100.0
        s1_u = float(st.get("next_step1_stake_u", 2.50))
        return (
            f"💰 實盤錢包｜${tot_eq:.2f}U ({net_gain:+.2f}U / {net_gain_pct:+.1f}%)\n"
            f"• 現金：${w.get('cash_pusd', 0.0):.2f} pUSD｜持倉：${w.get('portfolio_pusd', 0.0):.2f}U\n"
            f"• 分配：作戰 ${act_eq:.2f}U｜鎖倉 ${vault_u:.0f}U｜下階 ${next_ms_u:,.0f}U ({(tot_eq / max(next_ms_u, 1.0)) * 100.0:.1f}%)\n"
            f"• 注碼：首碼 ${s1_u:.2f}U ({st.get('next_step1_shares', 5.0):g}份 @ 50¢ 0% Fee)"
        )

    def _is_sniper_active(self) -> bool:
        return bool(self.sniper is not None and getattr(self.sniper.ledger, "enabled", False))

    def _build_full_tg_status(self) -> str:
        if not self._is_sniper_active():
            return (
                "🧪 V11.0 完整 5m K 線策略（模擬盤運行中）\n"
                + self._build_tg_status_text()
                + "\n(🎯 尾盤 Sniper：⏸ 已暫停)"
            )
        return self.sniper.telegram_text() + "\n\n🧪 舊策略（模擬觀察）\n" + self._build_tg_status_text()

    async def _handle_tg_command(self, text: str) -> None:
        cmd = text.strip()
        if cmd in ("/start", "/help", "/menu", "📋 主選單"):
            cash_now = float(self.auto_trader.live_wallet_state.get("cash_pusd", 40.62))
            if self._is_sniper_active():
                mode_tag = "新策略 LIVE｜舊策略 SIM"
            elif self.auto_trader.config.enabled and not self.auto_trader.config.dry_run:
                mode_tag = "V11.0 完整K線 LIVE"
            else:
                mode_tag = "🧪 V11.0 完整 5m K 線策略（模擬盤運行中｜尾盤 Sniper：⏸ 已暫停）"
            sim_eq = float(self.paper_trader.state.get("total_equity_u", cash_now))
            msg = (
                f"🚀 Polymarket｜{mode_tag}\n"
                f"• 模擬權益：${sim_eq:.2f}U｜實盤錢包：${cash_now:.2f} pUSD｜水位：Lv.{self.paper_trader.state.get('watermark_tier', 1)}\n"
                "• 點擊下方選單即可即時查詢或控制："
            )
            await self.notifier.send_telegram_text(msg, reply_markup=TG_MAIN_KEYBOARD)
        elif cmd in ("/summary", "/daily", "/report", "🌙 每日總結 (22:00 晚報)", "📊 每日總結"):
            if hasattr(self, "watchdog") and self.watchdog:
                await self.watchdog.send_daily_strategy_summary()
            else:
                await self.notifier.send_telegram_text(self._build_tg_status_text(), reply_markup=TG_MAIN_KEYBOARD)
        elif cmd in ("/sniper", "🎯 新策略戰情"):
            txt = self.sniper.telegram_text() if self.sniper is not None else "🎯 新策略尚未啟動。"
            await self.notifier.send_telegram_text(txt, reply_markup=TG_MAIN_KEYBOARD)
        elif cmd in ("/status", "📊 即時戰情"):
            await self.notifier.send_telegram_text(self._build_full_tg_status(), reply_markup=TG_MAIN_KEYBOARD)
        elif cmd in ("/confidence", "🎯 信心指數監控"):
            if self.review_manager is not None:
                c = self.review_manager.evaluate_confidence()
                pillars = c.get("pillars", {})
                p_list = pillars.values() if isinstance(pillars, dict) else pillars
                p_lines = [
                    f"  {'🟢' if float(p.get('score', 0)) >= float(p.get('max', 1)) * 0.9 else '🟡'} {p.get('label')}: {p.get('score', 0)}/{p.get('max', 1)}"
                    for p in p_list
                ]
                mode_str = "LIVE" if c.get("is_live_real_money") else "DRY-RUN"
                c_txt = (
                    f"🎯 V11.0 信心指數｜{c.get('confidence_score', 0):.0f}% ({mode_str})\n"
                    + "\n".join(p_lines)
                )
                await self.notifier.send_telegram_text(c_txt, reply_markup=TG_MAIN_KEYBOARD)
            else:
                await self.notifier.send_telegram_text("🎯 信心監控載入中。", reply_markup=TG_MAIN_KEYBOARD)
        elif cmd in ("/wallet", "/balance", "💰 實盤錢包"):
            w_txt = await self._build_tg_wallet_text()
            await self.notifier.send_telegram_text(w_txt, reply_markup=TG_MAIN_KEYBOARD)
        elif cmd in ("/ladder", "/watermark", "📈 資金水位表"):
            await self.notifier.send_telegram_text(self._build_tg_ladder_text(), reply_markup=TG_MAIN_KEYBOARD)
        elif cmd in ("/vpn", "🌐 VPN 狀態與修復"):
            if self.vpn_guardian is not None:
                st = await self.vpn_guardian.probe_all_nodes()
                self.vpn_guardian.ensure_macos_pac_active(force_refresh=True)
                mode_label = (
                    "Surfshark直連 (PAC已關)"
                    if st.get("direct_vpn_mode") and not st.get("direct_blocked")
                    else ("緊急備援護航 (直連IP受限)" if st.get("direct_blocked") else ("PAC：✅" if st.get("macos_pac_enabled") else "PAC：關"))
                )
                vpn_txt = (
                    f"🌐 VPN｜{st.get('primary_country', 'HK')} {st.get('primary_ip')} ({st.get('primary_latency_ms')}ms)\n"
                    f"• 網頁：{'✅' if st.get('web_deposit_unlocked') else '⚠️'}｜CLOB：{'✅' if st.get('clob_api_unlocked') else '❌'}｜{mode_label}"
                )
                await self.notifier.send_telegram_text(vpn_txt, reply_markup=TG_MAIN_KEYBOARD)
            else:
                geo = await self.auto_trader.check_geoblock_status()
                await self.notifier.send_telegram_text(f"🌐 VPN：{geo.get('advice', '')}", reply_markup=TG_MAIN_KEYBOARD)
        elif cmd in ("/claim", "/redeem", "🎁 立即領獎"):
            res = await self._check_and_notify_redeem()
            if float(res.get("claimed_pusd", 0.0)) <= 0:
                cur_cash = float(res.get("cash_pusd_after", self.auto_trader.live_wallet_state.get("cash_pusd", 40.62)))
                await self.notifier.send_telegram_text(f"✅ 無待領份額（現金 ${cur_cash:.2f} pUSD）。", reply_markup=TG_MAIN_KEYBOARD)
        elif cmd in ("/orders", "📝 掛單紀錄"):
            orders = self.auto_trader.order_history[:5]
            if not orders:
                txt = "📝 近期無新掛單。"
            else:
                lines = ["📝 最近 5 筆掛單"] + [
                    f"• {o['symbol']} {o['outcome']} {o['shares']}份 @ {o['price_cents']}¢ (${o.get('effective_cost_u', 0)}U) [{o['status']}]"
                    for o in orders
                ]
                txt = "\n".join(lines)
            await self.notifier.send_telegram_text(txt, reply_markup=TG_MAIN_KEYBOARD)
        elif cmd in ("/pause", "⏸ 暫停下單"):
            if self.sniper is not None:
                self.sniper.set_enabled(False)
            await self.notifier.send_telegram_text("⏸ 已暫停新策略開新單（持倉仍會管理與結算，掃描持續中）。", reply_markup=TG_MAIN_KEYBOARD)
        elif cmd in ("/resume", "▶️ 恢復下單"):
            if self.sniper is not None:
                self.sniper.set_enabled(True)
            init_u = int(round(float(getattr(self.sniper.policy, "paper_initial_equity_u", 40.0)))) if self.sniper else 40
            lbl = f"🧪 ${init_u}U 模擬盤" if (self.sniper and self.sniper.policy.paper_simulation) else "實盤自動下單"
            v11_note = "V11.0 模擬盤推播開啟中。" if not self._legacy_sim_only() else "舊策略維持靜音模擬。"
            await self.notifier.send_telegram_text(f"▶️ 已恢復尾盤 Sniper（TWAP V2 {lbl}）。{v11_note}", reply_markup=TG_MAIN_KEYBOARD)
        else:
            w_txt = await self._build_tg_wallet_text()
            await self.notifier.send_telegram_text(self._build_tg_status_text() + "\n" + w_txt, reply_markup=TG_MAIN_KEYBOARD)

    async def _handle_tg_update(self, upd: dict[str, Any]) -> None:
        self._tg_update_offset = int(upd["update_id"]) + 1
        member = upd.get("my_chat_member")
        if member:
            ch = member.get("chat", {})
            cid = str(ch.get("id", ""))
            status = str((member.get("new_chat_member") or {}).get("status", ""))
            if cid and status in ("administrator", "member"):
                if self.notifier.add_telegram_chat_id(cid):
                    title = str(ch.get("title") or cid)
                    await self.notifier.send_telegram_text(
                        f"🔔 頻道/群組 `{title}` 推播綁定成功！之後 TWAP V2 開單／關單／結算與風控通知將同步發送至此。",
                        chat_ids=[cid],
                    )
            elif cid and status in ("left", "kicked"):
                self.notifier.remove_telegram_chat_id(cid)
            return

        chan_post = upd.get("channel_post") or upd.get("edited_channel_post")
        if chan_post:
            ch = chan_post.get("chat", {})
            cid = str(ch.get("id", ""))
            if cid and self.notifier.add_telegram_chat_id(cid):
                title = str(ch.get("title") or cid)
                await self.notifier.send_telegram_text(
                    f"🔔 頻道 `{title}` 已自動綁定成功！之後 TWAP V2 開單／關單／結算將同步推播至此。",
                    chat_ids=[cid],
                )
            text = str(chan_post.get("text", "")).strip()
            if text == "/sniper":
                txt = self.sniper.telegram_text() if self.sniper is not None else "🎯 新策略尚未啟動。"
                await self.notifier.send_telegram_text(txt, chat_ids=[cid])
            elif text == "/status":
                await self.notifier.send_telegram_text(self._build_full_tg_status(), chat_ids=[cid])
            return

        msg = upd.get("message") or upd.get("edited_message")
        if not msg:
            return
        chat_id = str(msg.get("chat", {}).get("id", ""))
        ch_type = str(msg.get("chat", {}).get("type", ""))
        if chat_id and not self.notifier.config.telegram_chat_id:
            self.notifier.save_config({"telegram_chat_id": chat_id, "telegram_enabled": True})
        elif chat_id and ch_type in ("group", "supergroup") and self.notifier.add_telegram_chat_id(chat_id):
            await self.notifier.send_telegram_text("🔔 群組推播已自動綁定！", chat_ids=[chat_id])
        msg_date = int(msg.get("date", 0) or 0)
        if msg_date > 0 and msg_date < int(time.time()) - 60:
            return
        text = str(msg.get("text", "")).strip()
        if text:
            await self._handle_tg_command(text)

    async def _run_telegram_bot_loop(self) -> None:
        proxies = ["http://127.0.0.1:10809", "http://38.147.163.5:8888", None]
        allowed = json.dumps(["message", "edited_message", "channel_post", "edited_channel_post", "my_chat_member"])
        while self.running:
            token = self.notifier.config.telegram_bot_token
            if not token:
                await asyncio.sleep(5.0)
                continue
            url = f"https://api.telegram.org/bot{token}/getUpdates"
            handled = False
            for px in proxies:
                try:
                    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as session:
                        async with session.get(url, params={"offset": self._tg_update_offset, "timeout": 6, "allowed_updates": allowed}, proxy=px) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                for upd in data.get("result", []):
                                    await self._handle_tg_update(upd)
                                handled = True
                                break
                except Exception:
                    continue
            if not handled:
                await asyncio.sleep(3.0)
            await asyncio.sleep(1.0)

    async def _preflight_warmup(self) -> None:
        """Bounded T-7.5s pre-flight warm-up so proxy slowdowns never overrun T-1.5s boundary firing."""
        await asyncio.gather(
            asyncio.wait_for(self.auto_trader.ensure_warm_v2_client(), timeout=3.5),
            asyncio.wait_for(self.auto_trader.refresh_all_live_5m_markets(include_next_bucket=True), timeout=3.5),
        )

    def restart_loop(self) -> None:
        """Cancel a stalled scanner loop task and spawn a fresh `run_loop` task."""
        old_task = self._task
        self._task = None
        if old_task is not None and not old_task.done():
            old_task.cancel()
        if self.running:
            self._task = asyncio.create_task(self.run_loop())

    async def run_loop(self, interval_sec: float = 4.0) -> None:
        self.running = True
        last_warm_bucket = -1
        while self.running:
            try:
                await asyncio.wait_for(self.scan_once(), timeout=45.0)
            except Exception as e:
                print(f"[V8 Scanner Loop Error] {type(e).__name__}: {e}")
            now = time.time()
            cur_bucket = int(now // 300)
            sec_into_bucket = now % 300
            sec_to_next_close = 300.0 - sec_into_bucket
            # Pre-flight warm-up at T-7.5s, then wake at T-1.5s (Pre-Boundary Forward-Start Sniper) and T+0.15s fallback!
            if sec_to_next_close <= 7.5:
                if sec_to_next_close > 7.2:
                    await asyncio.sleep(sec_to_next_close - 7.0)
                if cur_bucket != last_warm_bucket and self.running:
                    last_warm_bucket = cur_bucket
                    try:
                        await asyncio.wait_for(self._preflight_warmup(), timeout=4.2)
                    except Exception as exc:
                        print(f"[Scanner Warning] warm v2 client / refresh markets error: {exc}")
                rem = 300.0 - (time.time() % 300)
                if rem > 1.7:
                    await asyncio.sleep(rem - 1.5)
                elif 0.05 <= rem <= 1.7:
                    await asyncio.sleep(rem + 0.15)
                else:
                    await asyncio.sleep(0.20)
            else:
                await asyncio.sleep(min(interval_sec, max(0.5, sec_to_next_close - 7.3)))

    def start(self) -> None:
        self.chainlink_feed.start()
        if self._stale_startup_position is not None:
            stale = self._stale_startup_position
            self._stale_startup_position = None
            slug_s = str(stale.get("slug") or "")
            if slug_s and slug_s not in self._pending_oracle_settlements:
                cur_s = int(stale.get("step", 1) or 1)
                max_s = int(stale.get("max_martingale_steps") or getattr(self.master_spec, "max_martingale_steps", 4))
                _, cur_sh_s, _, _, tot_c_s = self._compute_seq_costs(stale, cur_s)
                self._pending_oracle_settlements[slug_s] = asyncio.create_task(
                    self._await_official_polymarket_settlement(
                        market_slug=slug_s,
                        symbol=str(stale["symbol"]),
                        direction=str(stale["direction"]),
                        cur_step=cur_s,
                        max_steps=max_s,
                        shares=float(stale.get("shares") or cur_sh_s),
                        tot_cost=tot_c_s,
                        seq_snapshot=stale,
                        timestamp_ms=int(stale.get("started_ts") or int(time.time() * 1000)),
                    )
                )
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run_loop())
        if self._wallet_task is None or self._wallet_task.done():
            self._wallet_task = asyncio.create_task(self._run_wallet_and_redeem_loop())
        if self._tg_task is None or self._tg_task.done():
            self._tg_task = asyncio.create_task(self._run_telegram_bot_loop())

    def stop(self) -> None:
        self.running = False
        self.chainlink_feed.stop()
        for t in (self._task, self._wallet_task, self._tg_task):
            if t is not None:
                t.cancel()
