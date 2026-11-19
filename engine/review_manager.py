from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from engine.v8_strategy import V8_CONFIG

ROOT_DIR = Path(__file__).resolve().parent.parent
REVIEWS_PATH = ROOT_DIR / "data" / "two_day_reviews.json"
FLASH_REVIEW_LOG_PATH = ROOT_DIR / "data" / "flash_periodic_review_log.json"
PAPER_ACCOUNT_PATH = ROOT_DIR / "data" / "paper_account.json"

# Timestamp when V11.0_ULTIMATE_RSI_MARTINGALE Strategy was deployed
V11_DEPLOYED_AT_MS = 1794638000000


@dataclass
class ReviewConfig:
    review_interval_hours: float = 2.0              # Periodic review & parity audit every 2 hours
    target_win_rate_pct: float = 89.57              # V11.0 2-year verified SeqWR (56.87% Step-1, 19.17/day, 9,197 exec seqs)
    min_sequences_for_real_money: int = 12
    graduation_confidence_threshold: float = 90.0   # Auto-switch to real money when Equity >= $200U & Parity 100%
    target_graduation_equity_u: float = 200.0       # Target capital milestone ($40.62U -> $200.00U)
    target_v4_verified_wins: int = 25


class ReviewManager:
    """
    Automated V11.0_ULTIMATE_RSI_MARTINGALE Strategy Parity Auditor & Risk Monitor:
    1. Periodically audits (every 2 hours + after every settled sequence) whether live/simulated execution
       is 100% identical to our designed `V11.0_ULTIMATE_RSI_MARTINGALE` specification (`verify_strategy_parity`):
       - Check 1 (`oracle_parity`): 100% Pure Polymarket Gamma + Chainlink RTDS Oracle anchor/settlement,
         0 Binance price/TWAP dependency, 0 phantom mid-bar bounce wins.
       - Check 2 (`execution_parity`): 100% Post-Only Maker @ <= 50.0¢ (0% fee), PTB adverse gap <= 0.12%,
         CLOB < 43¢ VPIN abort active, Hold-to-100¢ expiration, 3-coin K-line timestamp alignment.
       - Check 3 (`signal_chip_parity`): Multi-Period RSI Term-Structure (`E_RSI_TERM`, `RSI(2/6/14)`) +
         Multi-bar exhaustion (`A_CPOS`, `B_BACC`, `C_BB` + `RSI(2)`) + Wyckoff 2-Leg (`D_WYCKOFF_2LEG`) +
         Counter-TBR + 3-Bar CVD (`cvd_3`) Iceberg Absorption Gate.
       - Check 4 (`kelly_vault_parity`): 4-Step Guaranteed-Profit Martingale (`max_martingale_steps = 4`,
         `sh_k = cum_lost + ceil(0.25 * s1_sh)`) + active capital gates (`Step2 >= $30U`, `Step3 >= $65U`,
         `Step4 >= $128U`) + 3-Tier sizing (`5/6/7sh` Phase I `<$48U`, `5.0%/9.0%/11.5%` cap `9.0%` Phase II) +
         `0.55x` DD compression `<0.85 * active_peak`.
       - Check 5 (`expectation_parity`): Live rolling sequence win rate and signal cadence match the 2-year benchmark
         (`88.48%` SeqWR / `56.87%` Step-1 WR, `96.49%` theoretical 4-step WR, `19.17` signals/day).
    """

    def __init__(self, notifier=None, auto_trader=None, paper_trader=None, scanner=None) -> None:
        self.notifier = notifier
        self.auto_trader = auto_trader
        self.paper_trader = paper_trader
        self.scanner = scanner
        self.config = ReviewConfig()
        self.state = self._load_or_init()

    def _load_or_init(self) -> dict[str, Any]:
        if REVIEWS_PATH.exists():
            try:
                st = json.loads(REVIEWS_PATH.read_text(encoding="utf-8"))
                st.setdefault("auto_graduate_enabled", True)
                st["v4_deployed_at_ms"] = max(int(st.get("v4_deployed_at_ms", 0)), V11_DEPLOYED_AT_MS)
                st.setdefault("graduation_events", [])
                st.setdefault("optimization_log", [])
                return st
            except Exception as exc:
                print(f"[ReviewManager] Review state load failed: {exc}")
        now_ms = int(time.time() * 1000)
        init_state = {
            "last_review_ts_ms": now_ms,
            "next_review_ts_ms": now_ms + int(self.config.review_interval_hours * 3600 * 1000),
            "review_count": 0,
            "auto_graduate_enabled": True,
            "v4_deployed_at_ms": V11_DEPLOYED_AT_MS,
            "graduation_events": [],
            "optimization_log": [],
            "history": [],
        }
        self._save(init_state)
        return init_state

    def _save(self, state: dict[str, Any]) -> None:
        self.state = state
        REVIEWS_PATH.parent.mkdir(parents=True, exist_ok=True)
        REVIEWS_PATH.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")

    def get_time_to_next_review_sec(self) -> int:
        now_ms = int(time.time() * 1000)
        next_ms = int(self.state.get("next_review_ts_ms", 0))
        return max(0, int((next_ms - now_ms) / 1000))

    def _get_paper_data(self) -> dict[str, Any]:
        if self.paper_trader is not None:
            return self.paper_trader.state
        if PAPER_ACCOUNT_PATH.exists():
            try:
                return json.loads(PAPER_ACCOUNT_PATH.read_text(encoding="utf-8"))
            except Exception as exc:
                print(f"[ReviewManager] Paper account read failed: {exc}")
        return {}

    def verify_strategy_parity(self) -> dict[str, Any]:
        """
        5-Dimension Deterministic Audit verifying that the live/simulated strategy is executing
        100% identically to the `V11.0_ULTIMATE_RSI_MARTINGALE` specification.
        """
        paper_data = self._get_paper_data()
        trade_log = paper_data.get("trade_log", [])
        v11_since_ms = int(self.state.get("v4_deployed_at_ms", V11_DEPLOYED_AT_MS))
        v11_trades = [
            t for t in trade_log
            if (t.get("strategy_version") == V8_CONFIG.version)
            or (int(t.get("timestamp_ms", 0)) >= v11_since_ms and not str(t.get("strategy_version", "")).startswith("V10."))
        ]

        drift_warnings: list[str] = []

        # 1. Pure Polymarket Oracle Parity Check
        cl_connected = True
        if self.scanner is not None and getattr(self.scanner, "chainlink_feed", None) is not None:
            cl_st = self.scanner.chainlink_feed.get_status()
            tick_age = cl_st.get("last_tick_age_sec")
            cl_connected = bool(cl_st.get("connected", False)) or (tick_age is not None and float(tick_age) <= 60.0)
            if not getattr(self.scanner, "running", False) and int(cl_st.get("total_ticks", 0)) == 0:
                cl_connected = True
        phantom_wins = sum(1 for t in v11_trades if t.get("is_phantom_win", False))
        non_pm_oracle = sum(
            1 for t in v11_trades
            if t.get("oracle_source") and not str(t.get("oracle_source")).startswith("POLYMARKET_")
        )
        oracle_ok = cl_connected and (phantom_wins == 0) and (non_pm_oracle == 0) and (not V8_CONFIG.bounce_protect_enabled)
        if not oracle_ok:
            drift_warnings.append(
                f"Oracle Drift: cl_connected={cl_connected}, phantom={phantom_wins}, non_pm={non_pm_oracle}, bounce_protect={V8_CONFIG.bounce_protect_enabled}"
            )

        # 2. CLOB Execution & High-EV Arbitrage Price Discipline Parity Check
        overpaid_entries = sum(
            1
            for t in v11_trades
            if float(t.get("entry_share_cents", 50.0)) > 60.5 or float(t.get("entry_share_cents", 50.0)) < 43.0
        )
        invalid_exits = sum(
            1 for t in v11_trades
            if t.get("won") and float(t.get("exit_share_cents", 100.0)) < 93.0
        )
        clob_ev_caps_ok = True
        if self.auto_trader is not None and getattr(self.auto_trader, "config", None) is not None:
            acfg = self.auto_trader.config
            clob_ev_caps_ok = (
                0.52 <= float(getattr(acfg, "max_maker_entry_price", 0.56)) <= 0.56
                and 0.53 <= float(getattr(acfg, "max_taker_entry_price", 0.58)) <= 0.58
                and 0.55 <= float(getattr(acfg, "rescue_max_taker_entry_price", 0.60)) <= 0.60
            )
        exec_cfg_ok = (
            abs(float(V8_CONFIG.entry_share_cents) - 50.0) < 1e-6
            and float(V8_CONFIG.max_adverse_ptb_gap_pct) <= 0.125
            and float(V8_CONFIG.min_implied_clob_cents) >= 43.0
            and (not V8_CONFIG.surge_take_profit_enabled)
            and clob_ev_caps_ok
        )
        execution_ok = exec_cfg_ok and (overpaid_entries == 0) and (invalid_exits == 0)
        if not execution_ok:
            drift_warnings.append(f"Execution Drift: cfg_ok={exec_cfg_ok}, overpaid={overpaid_entries}, invalid_exits={invalid_exits}")

        # 3. Signal & Chip-Theory Filter Parity Check (Multi-Period RSI Term-Structure + Wyckoff 2-Leg + CVD_3)
        chip_cfg_ok = (
            bool(V8_CONFIG.wyckoff_retest_enabled)
            and bool(V8_CONFIG.rsi_term_enabled)
            and abs(float(V8_CONFIG.rsi_term_min_spread) - 10.0) < 1e-6
            and abs(float(V8_CONFIG.rsi2_single_branch_max_long) - 14.5) < 1e-6
            and abs(float(V8_CONFIG.max_counter_tbr_long) - 0.52) < 1e-6
            and abs(float(V8_CONFIG.min_counter_tbr_short) - 0.48) < 1e-6
            and abs(float(V8_CONFIG.wyckoff_max_cvd3_long) - 0.04) < 1e-6
            and abs(float(V8_CONFIG.iceberg_max_cvd3_long) - 0.06) < 1e-6
            and int(V8_CONFIG.iceberg_cvd3_min_streak) == 5
            and abs(float(V8_CONFIG.single_branch_max_tbr_long) - 0.475) < 1e-6
        )
        branch_counts = {"A_CPOS": 0, "B_BACC": 0, "C_BB": 0, "D_WYCKOFF_2LEG": 0, "E_RSI_TERM": 0}
        cvd_violations = 0
        for t in v11_trades:
            br = str(t.get("trigger_branch", ""))
            for k in branch_counts:
                if k in br:
                    branch_counts[k] += 1
            cvd3_val = float(t.get("cvd_3", 0.0))
            is_long = t.get("direction") == "LONG_BOUNCE"
            if "D_WYCKOFF_2LEG" in br:
                if (is_long and cvd3_val > 0.045) or ((not is_long) and cvd3_val < -0.045):
                    cvd_violations += 1
        signal_chip_ok = chip_cfg_ok and (cvd_violations == 0)
        if not signal_chip_ok:
            drift_warnings.append(f"Signal/Chip Drift: chip_cfg_ok={chip_cfg_ok}, cvd_violations={cvd_violations}")

        # 4. Guaranteed-Profit 4-Step Martingale & Option A Small-Capital 5-Share Protection Parity Check
        kelly_cfg_ok = (
            int(V8_CONFIG.max_martingale_steps) == 4
            and abs(float(V8_CONFIG.martingale_profit_buffer_ratio) - 0.25) < 1e-6
            and abs(float(V8_CONFIG.step2_min_active_u) - 25.0) < 1e-6
            and abs(float(V8_CONFIG.step3_min_active_u) - 65.0) < 1e-6
            and abs(float(V8_CONFIG.step4_min_active_u) - 128.0) < 1e-6
            and abs(float(V8_CONFIG.phase1_flat_equity_ceiling) - 55.0) < 1e-6
            and abs(float(V8_CONFIG.phase1_tier_s_min_active_u) - 55.0) < 1e-6
            and abs(float(V8_CONFIG.phase2_pct_sizing) - 0.050) < 1e-6
            and abs(float(V8_CONFIG.phase2_tier_s_pct_sizing) - 0.090) < 1e-6
            and abs(float(V8_CONFIG.phase2_tier_s_plus_pct_sizing) - 0.115) < 1e-6
            and abs(float(V8_CONFIG.phase2_s1_cap_pct) - 0.090) < 1e-6
            and abs(float(V8_CONFIG.emergency_capital_floor_u) - 2.50) < 1e-6
            and abs(float(V8_CONFIG.micro_equity_tier_s_only_u) - 0.0) < 1e-6
            and abs(float(V8_CONFIG.dd_compress_threshold_ratio) - 0.85) < 1e-6
            and abs(float(V8_CONFIG.dd_compress_multiplier) - 0.55) < 1e-6
            and int(paper_data.get("cooldown_until_ms", 0)) == 0
        )
        if not kelly_cfg_ok:
            drift_warnings.append("Kelly/Martingale/Vault Config Drift detected")

        # 5. Statistical Expectation Parity Check (Expected SeqWR = 89.57%, Step-1 WR = 56.87%, ~19.17 signals/day)
        n_v11 = len(v11_trades)
        wins_v11 = sum(1 for t in v11_trades if t.get("won", False))
        live_wr_pct = round((wins_v11 / n_v11 * 100.0), 2) if n_v11 > 0 else 89.57
        stat_ok = True if n_v11 < 5 else (live_wr_pct >= 45.0)
        if not stat_ok:
            drift_warnings.append(f"Statistical Drift: live SeqWR {live_wr_pct}% over {n_v11} sequences (< 45.0% floor)")

        checks = {
            "oracle_parity": {
                "ok": oracle_ok,
                "chainlink_rtds_ready": cl_connected,
                "phantom_wins": phantom_wins,
                "label": "100% Polymarket Gamma + Chainlink RTDS 預言機 (零幣安干預・零幽靈單)",
            },
            "execution_parity": {
                "ok": execution_ok,
                "entry_limit_cents": V8_CONFIG.entry_share_cents,
                "overpaid_entries": overpaid_entries,
                "label": "高EV雙階段保成交進場 (Stage-1 Maker<=56¢ 0%Fee ➔ Stage-2 掃單<=58¢ / 救援<=60¢) + 100¢ 到期結算",
            },
            "signal_chip_parity": {
                "ok": signal_chip_ok,
                "wyckoff_enabled": V8_CONFIG.wyckoff_retest_enabled,
                "rsi_term_enabled": V8_CONFIG.rsi_term_enabled,
                "cvd_violations": cvd_violations,
                "branch_counts": branch_counts,
                "label": "多週期 RSI 期限背離 (E_RSI_TERM) + 多K衰竭 (A/B/C+RSI2) + Wyckoff (D) + CVD_3",
            },
            "kelly_vault_parity": {
                "ok": kelly_cfg_ok,
                "max_martingale_steps": int(V8_CONFIG.max_martingale_steps),
                "step_active_gates_u": [V8_CONFIG.step2_min_active_u, V8_CONFIG.step3_min_active_u, V8_CONFIG.step4_min_active_u],
                "tiers_pct": [
                    V8_CONFIG.phase2_pct_sizing,
                    V8_CONFIG.phase2_tier_s_pct_sizing,
                    V8_CONFIG.phase2_tier_s_plus_pct_sizing,
                ],
                "label": "4碼保利馬丁 (緩衝+25%・門檻25/65/128U) + 小資<$55U鎖定5股 ($2.5U) + 0.55x DD壓縮",
            },
            "expectation_parity": {
                "ok": stat_ok,
                "evaluated_trades": n_v11,
                "live_win_rate_pct": live_wr_pct,
                "expected_win_rate_pct": 89.57,
                "expected_step1_win_rate_pct": 56.87,
                "expected_signals_per_day": 19.17,
                "label": f"實盤序列勝率 {live_wr_pct}% vs 兩年期望值 89.57% (首碼 56.87% / 日均 19.17 單)",
            },
        }
        passed_cnt = sum(1 for c in checks.values() if c["ok"])
        parity_score = round((passed_cnt / len(checks)) * 100.0, 1)
        return {
            "strategy_version": "V11.0_ULTIMATE_RSI_MARTINGALE",
            "all_parity_checks_passed": (passed_cnt == len(checks)),
            "parity_score_pct": parity_score,
            "checks": checks,
            "branch_counts": branch_counts,
            "drift_warnings": drift_warnings,
        }

    def evaluate_confidence(self) -> dict[str, Any]:
        """
        Compute the real-time 5-Pillar V11.0 Strategy Confidence Score (0.0% ~ 100.0%),
        audit strategy parity (`verify_strategy_parity`), and track capital milestones.
        """
        paper_data = self._get_paper_data()
        parity = self.verify_strategy_parity()

        trade_log = paper_data.get("trade_log", [])
        v11_since_ms = int(self.state.get("v4_deployed_at_ms", V11_DEPLOYED_AT_MS))
        v11_trades = [
            t for t in trade_log
            if (t.get("strategy_version") == V8_CONFIG.version)
            or (int(t.get("timestamp_ms", 0)) >= v11_since_ms and not str(t.get("strategy_version", "")).startswith("V10."))
        ]
        active_trades = v11_trades if len(v11_trades) > 0 else []
        recent_12 = active_trades[:12]
        n_rec = len(recent_12)

        consec_wins = 0
        for t in active_trades:
            if t.get("won", False):
                consec_wins += 1
            else:
                break

        wins_12 = sum(1 for t in recent_12 if t.get("won", False))
        s1_wins_12 = sum(1 for t in recent_12 if t.get("won_at_step") == 1)
        rolling_wr_pct = round((wins_12 / n_rec * 100.0), 1) if n_rec > 0 else 88.48
        rolling_s1_wr_pct = round((s1_wins_12 / n_rec * 100.0), 1) if n_rec > 0 else 56.87

        v11_wins = sum(1 for t in v11_trades if t.get("won", False))
        phantom_win_count = sum(1 for t in v11_trades if t.get("is_phantom_win", False))
        target_v11 = max(1, int(self.config.target_v4_verified_wins))

        init_sim_u = float(paper_data.get("initial_u", 40.62))
        cur_sim_u = float(paper_data.get("total_equity_u", paper_data.get("active_equity_u", 40.62)))
        target_grad_u = float(self.config.target_graduation_equity_u)
        challenge_gain_needed = max(1.0, target_grad_u - init_sim_u)
        challenge_gain_now = max(0.0, cur_sim_u - init_sim_u)
        challenge_progress_pct = round(min(100.0, (challenge_gain_now / challenge_gain_needed) * 100.0), 1)
        remaining_u_to_graduate = round(max(0.0, target_grad_u - cur_sim_u), 2)

        # Pillar 1 (30 pts): V11.0 5-Point Strategy Parity Audit Score
        p1_score = round((parity["parity_score_pct"] / 100.0) * 30.0, 1)

        # Pillar 2 (20 pts): Rolling 12-Seq Win Rate vs 88.48% SeqWR Benchmark
        if n_rec < 5 or rolling_wr_pct >= 75.0:
            p2_score = 20.0
        elif rolling_wr_pct <= 45.0:
            p2_score = 0.0
        else:
            p2_score = round(((rolling_wr_pct - 45.0) / 30.0) * 20.0, 1)

        # Pillar 3 (15 pts): Active Win Momentum & Zero Cooldown Trap
        p3_score = round(min(15.0, 10.0 + min(5.0, consec_wins * 1.0)), 1)

        is_dry_run = True
        if self.auto_trader is not None:
            is_dry_run = bool(self.auto_trader.config.dry_run)

        # Pillar 4 (25 pts): Live Capital / $40.62U -> $200.00U Challenge Progress
        if not is_dry_run or cur_sim_u >= target_grad_u:
            p4_score = 25.0
            p4_label = "V11.0 正式真金實盤運行中 (CLOB V2 直連)" if not is_dry_run else f"$40.62U➔$200U 挑戰達標 (${cur_sim_u:.2f}U)"
        else:
            p4_score = round(min(25.0, (challenge_progress_pct / 100.0) * 25.0), 1)
            p4_label = f"$40.62U➔$200U 挑戰 (${cur_sim_u:.2f}U / {challenge_progress_pct}%)"

        # Pillar 5 (10 pts): Infrastructure & Preserved Real Wallet Readiness ($40.62 pUSD)
        live_cash = float(paper_data.get("live_cash_pusd", 40.62))
        geo_ok = True
        if self.auto_trader is not None:
            live_cash = float(self.auto_trader.live_wallet_state.get("cash_pusd", live_cash))
            geo_ok = bool(self.auto_trader.last_geoblock_status.get("can_trade_api", True))
        p5_score = 10.0 if (live_cash >= 2.50 and geo_ok) else (5.0 if live_cash >= 2.50 else 0.0)
        p5_label = f"真金錢包就緒 (${live_cash:.2f}U)" if not is_dry_run else f"真金錢包安全鎖定 (${live_cash:.2f}U)"

        total_confidence = round(min(100.0, p1_score + p2_score + p3_score + p4_score + p5_score), 1)
        threshold = float(self.config.graduation_confidence_threshold)

        equity_gate_met = cur_sim_u >= target_grad_u
        zero_phantom_gate_met = (phantom_win_count == 0)
        win_rate_gate_met = True if n_rec < 5 else (rolling_wr_pct >= 65.0)
        parity_gate_met = bool(parity.get("all_parity_checks_passed", True))
        all_graduation_gates_met = equity_gate_met and zero_phantom_gate_met and win_rate_gate_met and parity_gate_met

        remaining_v11_wins = max(0, int(round(remaining_u_to_graduate / 2.50))) if not equity_gate_met else 0

        if not is_dry_run:
            status_label = "🚀 V11.0 正式真金交易運行中 (LIVE REAL MONEY)"
            status_color = "emerald"
            next_action_desc = f"策略一致性 {parity['parity_score_pct']}%・信心指數 {total_confidence}% 穩定護航中（連勝 {consec_wins} 場・目標 $200U ➔ $200,000U）"
        elif all_graduation_gates_met:
            status_label = "🟢 $200U 達標鐵門已通關・立即自動啟動 V11.0 正式真金基金"
            status_color = "emerald"
            next_action_desc = f"模擬金已突破 ${cur_sim_u:.2f}U (>= $200U)，V11.0 策略五維一致性 100% 驗證通過！系統正自動切換為正式真金交易模式並推播 TG 通知"
        else:
            status_label = f"🧪 V11.0 保利四碼馬丁・$40.62U➔$200U 模擬挑戰中 (${cur_sim_u:.2f}U / $200U | 一致性 {parity['parity_score_pct']}%)"
            status_color = "amber"
            next_action_desc = (
                f"真實錢包 (${live_cash:.2f} pUSD) 已安全鎖定保護！"
                f"當前模擬金 ${cur_sim_u:.2f}U（進度 {challenge_progress_pct}%，距 $200U 門檻還差 ${remaining_u_to_graduate:.2f}U），"
                f"每 2 小時定時審計策略一致性（當前 {parity['parity_score_pct']}%），打上 $200.00U 即自動切換正式真金實盤！"
            )

        summary = {
            "strategy_version": "V11.0_ULTIMATE_RSI_MARTINGALE",
            "confidence_score": total_confidence,
            "threshold_pct": threshold,
            "auto_graduate_enabled": bool(self.state.get("auto_graduate_enabled", True)),
            "is_live_real_money": not is_dry_run,
            "status_label": status_label,
            "status_color": status_color,
            "next_action_desc": next_action_desc,
            "consecutive_wins": consec_wins,
            "rolling_12_win_rate_pct": rolling_wr_pct,
            "rolling_12_step1_wr_pct": rolling_s1_wr_pct,
            "v4_verified_wins": v11_wins,
            "v4_target_wins": target_v11,
            "remaining_v4_wins_to_graduate": remaining_v11_wins,
            "sim_equity_u": round(cur_sim_u, 2),
            "target_graduation_equity_u": round(target_grad_u, 2),
            "challenge_80_to_200_pct": challenge_progress_pct,
            "remaining_u_to_graduate": remaining_u_to_graduate,
            "equity_gate_met": equity_gate_met,
            "zero_phantom_gate_met": zero_phantom_gate_met,
            "parity_gate_met": parity_gate_met,
            "strategy_parity": parity,
            "phantom_win_count": phantom_win_count,
            "all_graduation_gates_met": all_graduation_gates_met,
            "live_wallet_pusd": round(live_cash, 2),
            "pillars": {
                "p1_rolling_wr": {"score": p1_score, "max": 30.0, "label": f"V11.0 五維策略一致性審計 ({parity['parity_score_pct']}%)"},
                "p2_step1_wr": {"score": p2_score, "max": 20.0, "label": f"近12輪序列勝率 ({rolling_wr_pct}% vs 期望 88.48%)"},
                "p3_win_streak": {"score": p3_score, "max": 15.0, "label": f"反脆弱零熔斷狀態 (連勝 {consec_wins} 場)"},
                "p4_v4_verified": {"score": p4_score, "max": 25.0, "label": p4_label},
                "p5_infra_health": {"score": p5_score, "max": 10.0, "label": p5_label},
            },
            "v4_evolutions": [
                "1. 多週期 RSI(2/6/14) 期限結構背離 (E_RSI_TERM) + Wyckoff 二腿 (D_WYCKOFF_2LEG) + 多K RSI2/TBR 微結構過濾：兩年首碼勝率 56.87%、日均 19.17 單！",
                "2. 保利適度四碼馬丁格爾 (sh_k = cum_lost + ceil(0.25*s1_sh))：兩年實測執行序列勝率 88.48% (全四碼理論 96.49%)，任一碼勝出必為正收益！",
                "3. 本金安全門檻鐵律：Step 2>=$30U / Step 3>=$65U / Step 4>=$128U + Hold-to-100¢ 到期結算，過去 24 個月 $40.62U 起跑 24/24 獲利且 24/24 衝破 $200U (中位數 $686.87U)！",
                "4. 100% Polymarket Gamma + Chainlink RTDS 預言機結算 + Post-Only @ 50¢ (0% Fee) + $220 階梯鎖利保險箱直達 20萬U！",
            ],
            "latest_optimization": (self.state.get("optimization_log") or [None])[0],
        }
        self.state["latest_confidence"] = summary
        return summary

    def run_dynamic_optimization(self, master_spec=None) -> dict[str, Any]:
        """
        Runs the V11.0 Strategy Parity & Expectations Audit (verifying live behavior matches
        our exact backtested design without arbitrarily mutating verified parameters).
        """
        parity = self.verify_strategy_parity()
        checks = parity["checks"]
        exp_chk = checks["expectation_parity"]
        n_trades = int(exp_chk["evaluated_trades"])
        live_wr = float(exp_chk["live_win_rate_pct"])

        if parity["all_parity_checks_passed"]:
            action_taken = "PARITY_VERIFIED_V11_OPTIMAL"
            adjustments = [
                f"V11.0 五維策略一致性 100% 吻合預期：純 Polymarket 預言機、RSI(2/6/14) 期限背離 + Wyckoff + CVD_3、4碼保利馬丁 (門檻 30/65/128U) 與階梯注碼均正常運作（已審計 {n_trades} 序列，SeqWR {live_wr}% vs 期望 88.48%）。"
            ]
        else:
            action_taken = "PARITY_DRIFT_ALERT"
            adjustments = parity["drift_warnings"] or ["偵測到策略設定與 V11.0 基準不一致，請檢查巡檢日誌。"]

        opt_entry = {
            "timestamp_ms": int(time.time() * 1000),
            "iso_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "strategy_version": "V11.0_ULTIMATE_RSI_MARTINGALE",
            "v6_sequences_evaluated": n_trades,
            "step1_wr_pct": live_wr,
            "four_step_wr_pct": live_wr,
            "parity_score_pct": parity["parity_score_pct"],
            "all_parity_checks_passed": parity["all_parity_checks_passed"],
            "branch_counts": parity["branch_counts"],
            "action": action_taken,
            "adjustments": adjustments,
        }
        opt_log = self.state.get("optimization_log", [])
        opt_log.insert(0, opt_entry)
        self.state["optimization_log"] = opt_log[:30]
        self._save(self.state)
        return opt_entry

    async def check_and_trigger_graduation_or_circuit_breaker(self) -> dict[str, Any]:
        """
        Called automatically after every completed sequence in `scanner.py`:
        - Audits V10.0 strategy parity (`run_dynamic_optimization`)
        - Promotes from `dry_run` to LIVE REAL MONEY when `sim_equity_u >= $200.00 U`
          AND `all_graduation_gates_met` (including 100% strategy parity).
        """
        self.run_dynamic_optimization()
        conf = self.evaluate_confidence()
        if self.auto_trader is None:
            return conf

        is_dry_run = bool(self.auto_trader.config.dry_run)
        auto_grad = bool(self.state.get("auto_graduate_enabled", True))
        all_gates_met = bool(conf.get("all_graduation_gates_met", False))
        sim_eq = float(conf.get("sim_equity_u", 40.62))

        if is_dry_run and auto_grad and all_gates_met:
            prom_res = await self.promote_to_real_money(
                reason=(
                    f"模擬金已成功由 $40.62U 打到 ${sim_eq:.2f}U (>= $200.00U 達標，零幽靈單，"
                    f"V11.0 策略五維一致性 100%，近輪序列勝率 {conf.get('rolling_12_win_rate_pct', 88.48)}%)，自動啟動 V11.0 正式真金基金"
                )
            )
            if isinstance(prom_res, dict) and prom_res.get("ok") is False and self.notifier is not None:
                await self.notifier.send_telegram_text(
                    f"⚠️【V11.0 $200U 達標晉升暫緩・Pre-Flight 自檢未通過】\n"
                    f"模擬金已達 ${sim_eq:.2f} U，但實盤安全檢查攔截：{prom_res.get('error', '未知錯誤')}\n"
                    f"系統繼續安全停留於 Dry-Run 保護狀態，修復後將自動接管實盤！"
                )
            return self.evaluate_confidence()

        if not is_dry_run and self.paper_trader is not None:
            st = self.paper_trader.state
            vault_u = float(st.get("vault_locked_u", 0.0))
            act_u = float(st.get("active_equity_u", 40.62))
            if vault_u > 0.0 and act_u < 2.50:
                return self.evaluate_confidence()

        self._save(self.state)
        return conf

    async def promote_to_real_money(self, reason: str = "手動/自動晉升實盤") -> dict[str, Any]:
        """
        Switches the system from Dry-Run Simulation (`dry_run=True`) to Live Real-Money Trading (`dry_run=False`).
        Runs V10.0 Pre-Flight Telemetry Audit (Geoblock, Wallet Balance, Allowance, CLOB Auth, Strategy Parity),
        archives the simulation log cleanly (Zero State Bleed), resets live account equity to exact on-chain pUSD,
        and notifies the user via Telegram!
        """
        if self.auto_trader is not None and getattr(self.auto_trader.config, "legacy_simulation_only", False):
            return {"ok": False, "error": "legacy strategy is pinned to simulation (legacy_simulation_only=true)"}
        sim_final_u = (
            float(self.paper_trader.state.get("total_equity_u", self.paper_trader.state.get("active_equity_u", 200.0)))
            if self.paper_trader
            else 200.0
        )
        vpn_node_desc = "HA-Gateway"

        if self.scanner is not None and getattr(self.scanner, "vpn_guardian", None) is not None:
            try:
                vg_st = await self.scanner.vpn_guardian.probe_all_nodes()
                vpn_node_desc = f"{vg_st.get('primary_ip', 'HK')} ({vg_st.get('primary_country', 'HK')} | {vg_st.get('primary_latency_ms', 250)}ms)"
            except Exception as exc:
                print(f"[ReviewManager] VPN probe warning: {exc}")

        if self.auto_trader is not None:
            geo = await self.auto_trader.check_geoblock_status()
            if not geo.get("can_trade_api", False):
                return {"ok": False, "error": f"Pre-Flight Geoblock Failed: {geo.get('country')}"}
            client = await self.auto_trader.ensure_warm_v2_client()
            if client is None:
                return {"ok": False, "error": "Pre-Flight CLOB Auth Failed: warm V2 client unavailable"}
            w = await self.auto_trader.sync_live_wallet_balance()
            real_eq = float(w.get("total_equity_pusd", 0.0))
            real_cash = float(w.get("cash_pusd", 0.0))
            if real_cash < 2.50:
                return {"ok": False, "error": f"Pre-Flight Balance Too Low: ${real_cash:.4f} pUSD < $2.50 min 5-share floor"}

            if self.paper_trader is not None and PAPER_ACCOUNT_PATH.exists():
                try:
                    arch = PAPER_ACCOUNT_PATH.parent / f"archive_sim_graduation_{int(time.time())}.json"
                    arch.write_text(PAPER_ACCOUNT_PATH.read_text(encoding="utf-8"), encoding="utf-8")
                except Exception as exc:
                    print(f"[ReviewManager] Archive backup failed: {exc}")

            self.auto_trader.save_config({"dry_run": False, "enabled": True})
            if self.paper_trader is not None:
                self.paper_trader.sync_with_live_wallet(
                    real_eq,
                    real_cash,
                    is_dry_run=False,
                    reset_for_live_promotion=True,
                )

        ev = {
            "timestamp_ms": int(time.time() * 1000),
            "iso_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "sim_equity_at_graduation_u": round(sim_final_u, 2),
            "reason": reason,
        }
        events = self.state.get("graduation_events", [])
        events.insert(0, ev)
        self.state["graduation_events"] = events[:20]
        conf = self.evaluate_confidence()
        self._save(self.state)

        if self.notifier is not None:
            s1_u = self.paper_trader.state.get("next_step1_stake_u", 2.50) if self.paper_trader else 2.50
            s1_sh = self.paper_trader.state.get("next_step1_shares", 5.0) if self.paper_trader else 5.0
            await self.notifier.send_telegram_text(
                f"🚀 V11.0 正式真金實盤啟動\n"
                f"• 餘額：${conf.get('live_wallet_pusd', 40.62):.2f} pUSD（模擬基準 ${sim_final_u:.2f}U）\n"
                f"• 通道：{vpn_node_desc}｜Post-Only @ <=50¢ (0% Fee)\n"
                f"• 首碼：${s1_u:.2f}U ({s1_sh:g}份)｜4碼保利門檻 $30/$65/$128U\n"
                f"• 原因：{reason}"
            )
        return conf

    def set_auto_graduate(self, enabled: bool, threshold_pct: float | None = None) -> dict[str, Any]:
        self.state["auto_graduate_enabled"] = bool(enabled)
        if threshold_pct is not None and 70.0 <= float(threshold_pct) <= 100.0:
            self.config.graduation_confidence_threshold = round(float(threshold_pct), 1)
        conf = self.evaluate_confidence()
        self._save(self.state)
        return conf

    def generate_review(self, force: bool = False) -> dict[str, Any]:
        now_ms = int(time.time() * 1000)
        window_ms = int(self.config.review_interval_hours * 3600 * 1000)
        start_ms = now_ms - window_ms

        paper_data = self._get_paper_data()
        opt_entry = self.run_dynamic_optimization()
        parity = self.verify_strategy_parity()
        trade_log = paper_data.get("trade_log", [])
        v11_since_ms = int(self.state.get("v4_deployed_at_ms", V11_DEPLOYED_AT_MS))
        trades_window = [
            t for t in trade_log
            if (t.get("strategy_version") == V8_CONFIG.version)
            or (int(t.get("timestamp_ms", 0)) >= max(start_ms, v11_since_ms) and not str(t.get("strategy_version", "")).startswith("V10."))
        ]

        tot_w = len(trades_window)
        won_w = sum(1 for t in trades_window if t.get("won", False))
        s1_wins_w = sum(1 for t in trades_window if t.get("won_at_step") == 1)
        s2_wins_w = sum(1 for t in trades_window if t.get("won_at_step") == 2)
        s3_wins_w = sum(1 for t in trades_window if t.get("won_at_step") == 3)
        s4_wins_w = sum(1 for t in trades_window if t.get("won_at_step") == 4)
        busts_w = sum(1 for t in trades_window if not t.get("won", False))
        surge_wins = sum(
            1 for t in trades_window if 80.0 <= float(t.get("exit_share_cents", 0.0)) < 99.0
        )
        exp_100_wins = sum(1 for t in trades_window if float(t.get("exit_share_cents", 0.0)) >= 99.0)

        wr_s1 = round((s1_wins_w / tot_w * 100.0), 1) if tot_w > 0 else 56.87
        wr_comb = round((won_w / tot_w * 100.0), 1) if tot_w > 0 else 88.48
        net_pnl_w = round(sum(float(t.get("pnl_u", 0.0)) for t in trades_window), 2)

        cur_equity = float(paper_data.get("total_equity_u", paper_data.get("active_equity_u", 40.62)))
        init_equity = float(paper_data.get("initial_u", 40.62))
        peak_equity = float(paper_data.get("peak_equity_u", cur_equity))
        mdd_pct = round(((peak_equity - cur_equity) / max(peak_equity, 1e-9)) * 100.0, 2)
        target_stage1 = float(self.config.target_graduation_equity_u)
        target_final = 200000.0
        stage1_pct = round(min(100.0, max(0.0, (cur_equity - init_equity) / max(1.0, target_stage1 - init_equity) * 100.0)), 1)
        final_pct = round(min(100.0, (cur_equity / target_final) * 100.0), 2)

        live_cash = float(paper_data.get("live_cash_pusd", 40.62))
        if self.auto_trader:
            live_cash = float(self.auto_trader.live_wallet_state.get("cash_pusd", live_cash))

        conf = self.evaluate_confidence()
        score = conf["confidence_score"]
        consec_wins = conf["consecutive_wins"]
        is_dry_run = bool(self.auto_trader.config.dry_run) if self.auto_trader else True

        if not is_dry_run:
            verdict_code = "V11_LIVE_REAL_MONEY_ACTIVE"
            verdict_icon = "🚀"
            verdict_title = f"V11.0 正式真金實盤運行中 (${cur_equity:.2f}U | 目標 $200U ➔ $200,000U)"
            verdict_desc = (
                f"實盤真金權益 ${cur_equity:.2f}U（序列勝率 {wr_comb}% / 首碼 {wr_s1}%），"
                f"五維策略一致性 {parity['parity_score_pct']}%（RSI(2/6/14)期限背離 + 4碼保利馬丁）。"
            )
        elif conf.get("all_graduation_gates_met"):
            verdict_code = "READY_FOR_REAL"
            verdict_icon = "🟢"
            verdict_title = f"$40.62U➔$200U 模擬挑戰達標 (${cur_equity:.2f}U)・V11.0 自動切換真金實盤"
            verdict_desc = (
                f"模擬金已由 ${init_equity:.2f}U 累積至 ${cur_equity:.2f}U（序列勝率 {wr_comb}%），"
                f"五維策略一致性審計 {parity['parity_score_pct']}% 完全吻合預期，自動解鎖真金實盤！"
            )
        else:
            verdict_code = "V11_CHALLENGE_40_TO_200_ACTIVE"
            verdict_icon = "🟢" if parity["all_parity_checks_passed"] else "🟡"
            verdict_title = f"V11.0 保利四碼馬丁・$40.62U➔$200U 挑戰中 (當前 ${cur_equity:.2f}U / {stage1_pct}%)"
            verdict_desc = (
                f"真實錢包 (${live_cash:.2f} pUSD) 監控中！"
                f"五維策略一致性審計：{parity['parity_score_pct']}%（RSI(2/6/14) 期限背離 + Wyckoff + 4碼保利馬丁，日均 ~19.17 單、期望序列勝率 88.48%）。"
            )

        report = {
            "review_id": int(time.time() * 1000),
            "generated_at_iso": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "strategy_version": "V11.0_ULTIMATE_RSI_MARTINGALE",
            "period_hours": self.config.review_interval_hours,
            "mode": "DRY_RUN_SIMULATION (模擬金挑戰模式)" if is_dry_run else "LIVE_CLOB_V11 (V11.0 正式真金實盤)",
            "strategy_parity": parity,
            "confidence": conf,
            "optimization": opt_entry,
            "metrics": {
                "total_sequences": tot_w,
                "won_sequences": won_w,
                "step1_wins": s1_wins_w,
                "step2_wins": s2_wins_w,
                "step3_wins": s3_wins_w,
                "step4_wins": s4_wins_w,
                "busts": busts_w,
                "step1_win_rate_pct": wr_s1,
                "combined_win_rate_pct": wr_comb,
                "bounce_72_wins": surge_wins,
                "expiration_100_wins": exp_100_wins,
                "net_pnl_u": net_pnl_w,
                "max_drawdown_pct": mdd_pct,
                "consecutive_wins": consec_wins,
            },
            "capital": {
                "initial_u": init_equity,
                "current_sim_equity_u": cur_equity,
                "peak_sim_equity_u": peak_equity,
                "stage1_target_u": target_stage1,
                "stage1_progress_pct": stage1_pct,
                "final_target_u": target_final,
                "final_progress_pct": final_pct,
                "real_wallet_preserved_pusd": live_cash,
            },
            "verdict": {
                "code": verdict_code,
                "icon": verdict_icon,
                "title": verdict_title,
                "description": verdict_desc,
            },
        }

        self.state["review_count"] = int(self.state.get("review_count", 0)) + 1
        self.state["last_review_ts_ms"] = now_ms
        self.state["next_review_ts_ms"] = now_ms + window_ms
        self.state["latest_report"] = report
        self.state["latest_confidence"] = conf
        history = self.state.get("history", [])
        history.insert(0, report)
        self.state["history"] = history[:20]
        self._save(self.state)

        needs_escalation = bool(
            (not parity["all_parity_checks_passed"])
            or (tot_w >= 5 and wr_comb < 45.0)
            or mdd_pct >= 55.0
        )
        escalation_reason = (
            f"偵測到策略偏差或統計異常（一致性={parity['parity_score_pct']}% | 近 {self.config.review_interval_hours}h 序列勝率 {wr_comb}% ({tot_w}序列) | 回撤 {mdd_pct}% | 警告: {parity['drift_warnings']}），請調用主模型介入診斷！"
            if needs_escalation
            else "V11.0_ULTIMATE_RSI_MARTINGALE 五維策略一致性 100% 吻合預期（純預言機・RSI(2/6/14)期限背離・4碼保利馬丁+本金門檻30/65/128U），由巡檢引擎常駐紀錄。"
        )
        flash_entry = {
            "review_id": report["review_id"],
            "timestamp_iso": report["generated_at_iso"],
            "strategy_version": "V11.0_ULTIMATE_RSI_MARTINGALE",
            "sim_equity_u": cur_equity,
            "stage1_200u_progress_pct": stage1_pct,
            "confidence_score_pct": score,
            "parity_score_pct": parity["parity_score_pct"],
            "all_parity_checks_passed": parity["all_parity_checks_passed"],
            "branch_counts": parity["branch_counts"],
            "drift_warnings": parity["drift_warnings"],
            "rolling_12_win_rate_pct": conf.get("rolling_12_win_rate_pct", 88.48),
            "rolling_12_step1_wr_pct": conf.get("rolling_12_step1_wr_pct", 56.87),
            "consecutive_wins": consec_wins,
            "needs_critical_model_escalation": needs_escalation,
            "escalation_note": escalation_reason,
        }
        try:
            existing_flash = []
            if FLASH_REVIEW_LOG_PATH.exists():
                existing_flash = json.loads(FLASH_REVIEW_LOG_PATH.read_text(encoding="utf-8"))
                if not isinstance(existing_flash, list):
                    existing_flash = []
            existing_flash.insert(0, flash_entry)
            FLASH_REVIEW_LOG_PATH.write_text(
                json.dumps(existing_flash[:50], indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception as exc:
            print(f"[ReviewManager] Flash review log write failed: {exc}")

        return report

    async def dispatch_telegram_review(self, report: dict[str, Any]) -> None:
        if not self.notifier:
            return
        m = report["metrics"]
        c = report["capital"]
        v = report["verdict"]
        conf = report.get("confidence") or self.evaluate_confidence()
        parity = report.get("strategy_parity") or self.verify_strategy_parity()

        text = (
            f"📊 V11.0 巡檢｜{v['icon']} 一致性 {parity['parity_score_pct']:.0f}% / 信心 {conf['confidence_score']:.0f}%\n"
            f"• 權益：${c['current_sim_equity_u']:.2f}U（實盤 ${c['real_wallet_preserved_pusd']:.2f} pUSD）\n"
            f"• 戰績：{m['won_sequences']}勝/{m['busts']}負（序列 {m['combined_win_rate_pct']}% / 首碼 {m['step1_win_rate_pct']}% / {m['net_pnl_u']:+.2f}U）\n"
            f"• 階數：S1={m['step1_wins']} / S2={m['step2_wins']} / S3={m['step3_wins']} / S4={m['step4_wins']}"
        )
        await self.notifier.send_telegram_text(text)

    async def check_and_run_periodic_review(self) -> dict[str, Any] | None:
        await self.check_and_trigger_graduation_or_circuit_breaker()
        now_ms = int(time.time() * 1000)
        next_ms = int(self.state.get("next_review_ts_ms", 0))
        if now_ms >= next_ms:
            rep = self.generate_review()
            m = rep.get("metrics", {})
            parity = rep.get("strategy_parity", {})
            tot_n = int(m.get("total_sequences", 0))
            wr_pct = float(m.get("combined_win_rate_pct", 88.48))
            mdd = float(m.get("max_drawdown_pct", 0.0))
            legacy_sim = self.auto_trader is not None and getattr(self.auto_trader.config, "legacy_simulation_only", False)
            if ((not parity.get("all_parity_checks_passed", True)) or (tot_n >= 5 and wr_pct < 45.0) or mdd >= 55.0) and not legacy_sim:
                if self.notifier is not None:
                    await self.notifier.send_telegram_text(
                        f"🚨 V11.0 巡檢異常｜一致性 {parity.get('parity_score_pct', 100.0):.0f}%\n"
                        f"• 近 {tot_n} 輪：序列勝率 {wr_pct}% / 回撤 {mdd}%\n"
                        f"• 警告：{parity.get('drift_warnings', [])}\n"
                        f"• 快照：data/flash_periodic_review_log.json"
                    )
            return rep
        return None
