"""Multi-channel notification dispatcher for macOS, Telegram, Discord, and webhooks."""
from __future__ import annotations

import json
import os
import subprocess
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
import aiohttp

from engine.oracle_sniper_notify import strip_html_tags


ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT_DIR / "data" / "notify_config.json"
SIGNAL_LOG_PATH = ROOT_DIR / "data" / "signal_history.json"
LEGACY_EVENT_TYPES = {
    "PRE_CLOSE_PREVIEW",
    "SIGNAL_ENTRY",
    "MARTINGALE_ADD",
    "SEQUENCE_WIN",
    "STEP_LOSS_RESCUE",
    "SEQUENCE_BUST",
    "AUTO_REDEEM",
}
IMPORTANT_TG_EVENTS = {
    "SIGNAL_ENTRY",
    "MARTINGALE_ADD",
    "SEQUENCE_WIN",
    "STEP_LOSS_RESCUE",
    "SEQUENCE_BUST",
    "AUTO_REDEEM",
    "SNIPER_ENTRY",
    "SNIPER_EXIT",
    "SNIPER_ALERT",
}


@dataclass
class NotifyConfig:
    """Configuration for desktop, Telegram, Discord, and webhook notifications."""

    macos_desktop: bool = True
    macos_sound: bool = True
    telegram_enabled: bool = True
    telegram_bot_token: str = field(default_factory=lambda: os.environ.get("TELEGRAM_BOT_TOKEN", ""))
    telegram_chat_id: str = field(default_factory=lambda: os.environ.get("TELEGRAM_CHAT_ID", ""))
    telegram_extra_chat_ids: list[str] = field(default_factory=list)
    telegram_notify_orders: bool = True  # True = 即時推播每筆開單/加碼/關單結算與整體表現，靜音 2h 內部巡檢
    discord_enabled: bool = False
    discord_webhook_url: str = ""
    custom_webhook_enabled: bool = False
    custom_webhook_url: str = ""
    pre_close_warning_sec: int = 20  # Alert 20s before 5m candle closes if signal criteria met
    # True = the legacy RSI-martingale strategy runs in simulation only, so its (paper) signals are recorded in the
    # history but never pushed to Telegram / desktop; only `source == "oracle_sniper"` live events are delivered.
    mute_legacy_simulation: bool = True


class NotificationManager:
    """Manages signal history persistence and multi-channel trade notifications."""

    def __init__(self) -> None:
        """Initialize configuration and load recent signal history."""
        self.config = self._load_config()
        self.history: list[dict[str, Any]] = self._load_history()

    def _load_config(self) -> NotifyConfig:
        if CONFIG_PATH.exists():
            try:
                raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
                valid_keys = {k for k in NotifyConfig.__dataclass_fields__}
                filtered = {k: v for k, v in raw.items() if k in valid_keys}
                return NotifyConfig(**filtered)
            except Exception as exc:
                print(f"[Notifier Warning] load notify config failed: {exc}")
        cfg = NotifyConfig()
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        CONFIG_PATH.write_text(json.dumps(asdict(cfg), indent=2, ensure_ascii=False), encoding="utf-8")
        return cfg

    def save_config(self, new_cfg: dict[str, Any]) -> NotifyConfig:
        """Update and persist notification configuration fields."""
        current = asdict(self.config)
        for k, v in new_cfg.items():
            if k in current:
                current[k] = v
        self.config = NotifyConfig(**current)
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        CONFIG_PATH.write_text(json.dumps(current, indent=2, ensure_ascii=False), encoding="utf-8")
        return self.config

    def _load_history(self) -> list[dict[str, Any]]:
        if SIGNAL_LOG_PATH.exists():
            try:
                return json.loads(SIGNAL_LOG_PATH.read_text(encoding="utf-8"))[:200]
            except Exception as exc:
                print(f"[Notifier Warning] load signal history failed: {exc}")
        return []

    def _save_history(self) -> None:
        SIGNAL_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        SIGNAL_LOG_PATH.write_text(
            json.dumps(self.history[:200], indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def clear_history(self) -> None:
        """Clear persisted signal notification history."""
        self.history = []
        self._save_history()

    def _send_macos_notification(self, title: str, subtitle: str, body: str, play_sound: bool = True) -> bool:
        try:
            safe_title = title.replace('"', '\\"')
            safe_sub = subtitle.replace('"', '\\"')
            safe_body = body.replace('"', '\\"').replace("\n", " | ")
            script = f'display notification "{safe_body}" with title "{safe_title}" subtitle "{safe_sub}"'
            subprocess.Popen(["osascript", "-e", script], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if play_sound:
                subprocess.Popen(
                    ["afplay", "/System/Library/Sounds/Glass.aiff"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            return True
        except Exception:
            return False

    @staticmethod
    async def _post_json(url: str, payload: dict[str, Any], proxy: str | None = None, timeout_s: int = 6) -> int:
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout_s)) as session:
                async with session.post(url, json=payload, proxy=proxy) as resp:
                    return int(resp.status)
        except Exception:
            return 0

    def add_telegram_chat_id(self, chat_id: str) -> bool:
        """Register an extra Telegram channel or group chat_id; returns True if newly added."""
        cid = str(chat_id or "").strip()
        if not cid or cid == str(self.config.telegram_chat_id).strip():
            return False
        cur = [str(x).strip() for x in (self.config.telegram_extra_chat_ids or []) if str(x).strip()]
        if cid in cur:
            return False
        self.save_config({"telegram_extra_chat_ids": [*cur, cid]})
        return True

    def remove_telegram_chat_id(self, chat_id: str) -> None:
        """Unregister a Telegram channel/group chat_id if bot was removed."""
        cid = str(chat_id or "").strip()
        cur = [str(x).strip() for x in (self.config.telegram_extra_chat_ids or []) if str(x).strip()]
        if cid in cur:
            self.save_config({"telegram_extra_chat_ids": [x for x in cur if x != cid]})

    async def send_telegram_text(
        self,
        text: str,
        reply_markup: dict[str, Any] | None = None,
        parse_mode: str | None = None,
        chat_ids: list[str] | None = None,
    ) -> bool:
        """Broadcast a plain or formatted message to all configured Telegram chats/channels."""
        if not (self.config.telegram_enabled and self.config.telegram_bot_token):
            return False
        if chat_ids is not None:
            targets = [str(c).strip() for c in chat_ids if str(c).strip()]
        else:
            raw_ids = [self.config.telegram_chat_id, *(self.config.telegram_extra_chat_ids or [])]
            targets = list(dict.fromkeys(str(c).strip() for c in raw_ids if str(c).strip()))
        if not targets:
            return False
        tg_url = f"https://api.telegram.org/bot{self.config.telegram_bot_token}/sendMessage"
        any_ok = False
        primary = str(self.config.telegram_chat_id).strip()
        for cid in targets:
            payload: dict[str, Any] = {"chat_id": cid, "text": text, "disable_web_page_preview": True}
            if parse_mode:
                payload["parse_mode"] = parse_mode
            # Telegram broadcast channels reject ReplyKeyboardMarkup with HTTP 400; attach it only on private DM.
            if reply_markup and cid == primary and not cid.startswith("-100"):
                payload["reply_markup"] = reply_markup
            for px in ("http://127.0.0.1:10809", "http://38.147.163.5:8888", None):
                if await self._post_json(tg_url, payload, proxy=px, timeout_s=6) == 200:
                    any_ok = True
                    break
        return any_ok

    @staticmethod
    def _format_event_header(event: dict[str, Any]) -> tuple[str, str, str]:
        symbol = str(event.get("symbol", "BTCUSDT"))
        ev_type = str(event.get("event_type", "SIGNAL_ENTRY"))
        direction = str(event.get("direction", "LONG_BOUNCE"))
        step = int(event.get("step", 1))
        max_steps = int(event.get("max_steps", 4))
        price = float(event.get("price", 0.0))
        rsi = float(event.get("rsi", 50.0))
        stake = float(event.get("stake_usd", 2.50))

        dir_short = "🟢 BUY UP" if direction == "LONG_BOUNCE" else "🔴 BUY DOWN"
        titles = {
            "PRE_CLOSE_PREVIEW": f"⏳ 預警 {symbol} {dir_short}",
            "SIGNAL_ENTRY": f"🟢 開單進場 {symbol} {dir_short} (1/{max_steps}碼)",
            "MARTINGALE_ADD": f"🔥 加碼開單 {symbol} {dir_short} ({step}/{max_steps}碼)",
            "SEQUENCE_WIN": f"✅ 關單勝出 {symbol} ({step}/{max_steps}碼)",
            "STEP_LOSS_RESCUE": f"🛡 關單待救援 {symbol} ({step}/{max_steps}碼)",
            "SEQUENCE_BUST": f"⚠️ 關單止損 {symbol} ({step}/{max_steps}碼)",
            "AUTO_REDEEM": "💰 自動贖回入帳",
        }
        title = titles.get(ev_type, f"🔔 {symbol} 通知")
        subtitle = f"${price:,.2f} | RSI {rsi:.1f} | ${stake:,.2f}U"
        custom_msg = str(event.get("message", "")).strip()
        body = custom_msg if custom_msg else f"{title}\n• 現價：${price:,.2f} | 注碼：${stake:,.2f}U | RSI：{rsi:.1f}"
        return title, subtitle, body

    async def _dispatch_webhooks(
        self,
        title: str,
        subtitle: str,
        body: str,
        direction: str,
        ev_type: str,
        event_record: dict[str, Any],
        results: dict[str, bool],
    ) -> None:
        if self.config.discord_enabled and self.config.discord_webhook_url:
            color = 0xF59E0B if ev_type == "SEQUENCE_WIN" else (0x10B981 if direction == "LONG_BOUNCE" else 0xEF4444)
            payload = {"embeds": [{"title": title, "description": f"**{subtitle}**\n\n{body}", "color": color}]}
            st_code = await self._post_json(self.config.discord_webhook_url, payload, timeout_s=8)
            results["discord"] = st_code in (200, 204)

        if self.config.custom_webhook_enabled and self.config.custom_webhook_url:
            st_code = await self._post_json(self.config.custom_webhook_url, event_record, timeout_s=8)
            results["custom_webhook"] = 200 <= st_code < 300

    async def dispatch_signal(self, event: dict[str, Any]) -> dict[str, Any]:
        """Record a trading signal event and dispatch notifications across enabled channels."""
        now_ms = int(time.time() * 1000)
        event_record = {
            "id": f"{event.get('symbol', 'SYS')}-{now_ms}",
            "timestamp_ms": now_ms,
            **event,
        }
        self.history.insert(0, event_record)
        self.history = self.history[:200]
        self._save_history()

        legacy_type = str(event.get("event_type", "SIGNAL_ENTRY")) in LEGACY_EVENT_TYPES
        if getattr(self.config, "mute_legacy_simulation", False) and legacy_type and event.get("source") != "oracle_sniper":
            event_record["delivery"] = {"muted": "legacy_simulation"}
            return event_record

        ev_type = str(event.get("event_type", "SIGNAL_ENTRY"))
        direction = str(event.get("direction", "LONG_BOUNCE"))
        title, subtitle, body = self._format_event_header(event)
        parse_mode = event.get("parse_mode")
        plain_body = strip_html_tags(body) if parse_mode == "HTML" else body
        results: dict[str, bool] = {}

        if self.config.macos_desktop:
            results["macos"] = self._send_macos_notification(
                title=title,
                subtitle=subtitle,
                body=plain_body,
                play_sound=self.config.macos_sound,
            )

        if (
            not event.get("skip_telegram", False)
            and ev_type in IMPORTANT_TG_EVENTS
            and self.config.telegram_enabled
            and getattr(self.config, "telegram_notify_orders", True)
            and self.config.telegram_bot_token
            and self.config.telegram_chat_id
        ):
            results["telegram"] = await (
                self.send_telegram_text(body, parse_mode=parse_mode) if parse_mode else self.send_telegram_text(body)
            )

        await self._dispatch_webhooks(title, subtitle, plain_body, direction, ev_type, event_record, results)
        event_record["delivery"] = results
        return event_record
