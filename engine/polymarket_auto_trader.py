from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
import json
import math
import os
from pathlib import Path
import subprocess
import time
from typing import Any

from eth_account import Account
import httpx
from polymarket import (
    AcceptedOrder,
    ApiKeyCreds,
    AsyncSecureClient,
    BuilderApiKey,
    InsufficientAllowanceError,
    InsufficientLiquidityError,
    RateLimitError,
    RequestRejectedError,
    UserInputError,
)

CONFIG_PATH = Path(__file__).resolve().parent.parent / "data" / "polymarket_clob_config.json"
CLOB_HOST = "https://clob.polymarket.com"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
GEOBLOCK_URL = "https://polymarket.com/api/geoblock"
POLYGON_CHAIN_ID = 137

# Errors that prove the exchange did NOT accept the order (safe to retry / no fill possible).
DEFINITE_REJECTIONS = (
    RequestRejectedError,
    InsufficientLiquidityError,
    InsufficientAllowanceError,
    RateLimitError,
    UserInputError,
)

def resolve_up_down_indices(outcomes: list[Any]) -> tuple[int, int] | None:
    """Indices of the Up and Down outcomes by LABEL (never by array position); None if not exactly Up/Down."""
    labels = [str(x).strip().lower() for x in outcomes]
    if len(labels) < 2 or sorted(labels[:2]) != ["down", "up"]:
        return None
    return labels.index("up"), labels.index("down")


FRONTEND_ONLY_RESTRICTED_COUNTRIES = {"IE", "JP", "NL", "KR", "MT"}
COMPLETELY_BLOCKED_COUNTRIES = {
    "IR", "SY", "CU", "KP", "AU", "BY", "BE", "BI", "BR", "CF", "CD", "ET",
    "FR", "DE", "IQ", "IT", "LB", "LY", "MM", "NZ", "NI", "PL", "RU", "SG",
    "SO", "SK", "SS", "SD", "TW", "TH", "GB", "US", "UM", "VE", "YE", "ZW",
}

VERIFIED_FREE_PROXIES = [
    "http://43.99.60.244:8089",                  # HK - blocked: False (~245ms warm)
    "socks5://88888:88888@8.210.36.244:1080",    # HK - blocked: False (~247ms warm)
    "http://176.111.37.5:39811",                 # SE - blocked: False (~450ms warm)
    "http://176.111.37.216:39811",               # SE - blocked: False (~480ms warm)
    "socks5://101.36.104.46:10808",              # JP - CLOB API unblocked
]


@dataclass
class PolymarketClobConfig:
    enabled: bool = True
    dry_run: bool = True
    private_key: str = ""
    signer_address: str = ""
    funder_address: str = ""
    signature_type: int = 1
    api_key: str = ""
    api_secret: str = ""
    api_passphrase: str = ""
    builder_key: str = ""
    builder_secret: str = ""
    builder_passphrase: str = ""
    proxy_url: str = ""
    entry_limit_price: float = 0.50
    max_maker_entry_price: float = 0.56
    max_taker_entry_price: float = 0.58
    rescue_max_taker_entry_price: float = 0.60
    maker_wait_sec: float = 3.0
    bounce_protect_limit_price: float = 0.95
    auto_take_profit_on_bounce: bool = False
    enforce_min_5_shares: bool = True
    # When True the legacy RSI-martingale pipeline is pinned to simulation (dry_run) no matter what
    # Telegram /resume, review auto-graduation or the REST API request; the Chainlink sniper trades live
    # through the dedicated `sniper_buy` / `sniper_sell` methods which never look at `dry_run`.
    legacy_simulation_only: bool = False
    # Explicit opt-in for the Chainlink sniper's real-money orders (`sniper_buy` / `sniper_sell`).
    sniper_live: bool = False


class PolymarketAutoTrader:
    """
    Official Polymarket V2 Unified SDK (`polymarket-client`) Automated Execution & Auto-Redeem Engine:
    - Maintains a Persistent Pre-Warmed HTTP/2 Keep-Alive `AsyncSecureClient` session (~245ms order latency!)
    - Supports direct System VPN mode (e.g., Surfshark WireGuard when `proxy_url == ""`) with safe failover
    - Places V2 Limit BUY @ 51¢ on Stage-2 Entry / Martingale Step-2..4 Add
    - Places V2 Limit SELL @ 85¢/90¢ on Stage-3 Intra-Candle Bounce-Protect
    - Automatically executes Gasless Relayer Redemptions (`plan_collateral_return` + `redeem_positions`)
      so 100¢ expiration winnings automatically return to liquid pUSD cash and compound!
    """

    def __init__(self) -> None:
        self.config = self._load_config()
        if self.config.legacy_simulation_only:
            self.config.dry_run = True
        if self.config.proxy_url == "socks5://43.245.199.34:1080":
            self.config.proxy_url = ""
        self.backup_proxies: list[str] = list(VERIFIED_FREE_PROXIES)
        self.direct_geoblock_info: dict[str, Any] = {}
        self._apply_env_proxy()
        self._warm_client: AsyncSecureClient | None = None
        self._warm_client_proxy: str = ""
        self._warm_lock = asyncio.Lock()
        self.order_history: list[dict[str, Any]] = []
        self.redeem_history: list[dict[str, Any]] = []
        self.active_clob_positions: dict[str, dict[str, Any]] = {}
        self.last_geoblock_status: dict[str, Any] = {}
        self.live_5m_markets_cache: dict[str, dict[str, Any]] = {}
        self._bucket_market_cache: dict[tuple[str, int], tuple[float, dict[str, Any]]] = {}
        self._cache_ts: float = 0.0
        self.live_wallet_state: dict[str, Any] = {
            "cash_pusd": 85.6635,
            "portfolio_pusd": 0.0,
            "total_equity_pusd": 85.6635,
            "is_fully_approved": True,
            "updated_at_ms": 0,
        }

    def _load_config(self) -> PolymarketClobConfig:
        if CONFIG_PATH.exists():
            try:
                raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
                valid = {k: v for k, v in raw.items() if k in PolymarketClobConfig.__dataclass_fields__}
                valid["max_maker_entry_price"] = max(0.56, float(valid.get("max_maker_entry_price", 0.56)))
                valid["max_taker_entry_price"] = max(0.58, float(valid.get("max_taker_entry_price", 0.58)))
                valid["rescue_max_taker_entry_price"] = max(0.60, float(valid.get("rescue_max_taker_entry_price", 0.60)))
                return PolymarketClobConfig(**valid)
            except Exception:
                pass
        return PolymarketClobConfig()

    def _is_direct_mode(self) -> bool:
        """Return True when configured to use System VPN / Direct routing (`proxy_url` is empty or `DIRECT`)."""
        return self.config.proxy_url.strip().upper() in ("", "DIRECT")

    @staticmethod
    def _clear_env_proxy() -> None:
        """Remove proxy environment variables so httpx/SDK clients route directly through system VPN."""
        for key in ("ALL_PROXY", "HTTPS_PROXY", "HTTP_PROXY", "all_proxy", "https_proxy", "http_proxy"):
            os.environ.pop(key, None)

    def _apply_env_proxy(self) -> None:
        proxy = self.config.proxy_url.strip()
        if proxy and proxy.upper() != "DIRECT":
            os.environ["ALL_PROXY"] = proxy
            os.environ["HTTPS_PROXY"] = proxy
            os.environ["HTTP_PROXY"] = proxy
        else:
            self._clear_env_proxy()

    def save_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        for k, v in payload.items():
            if hasattr(self.config, k):
                if k in ("private_key", "api_secret", "api_passphrase", "builder_secret", "builder_passphrase") and isinstance(v, str) and "***" in v:
                    continue
                setattr(self.config, k, v)
        if self.config.legacy_simulation_only:
            self.config.dry_run = True
        self.config.private_key = self.config.private_key.strip()
        if self.config.private_key and not self.config.private_key.startswith("0x"):
            self.config.private_key = "0x" + self.config.private_key
        self.config.funder_address = self.config.funder_address.strip()
        self.config.proxy_url = self.config.proxy_url.strip()
        self._apply_env_proxy()

        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        CONFIG_PATH.write_text(json.dumps(asdict(self.config), indent=2, ensure_ascii=False), encoding="utf-8")
        return self.get_public_config()

    def get_signer_address(self) -> str:
        if not self.config.private_key:
            return self.config.signer_address or ""
        try:
            acct = Account.from_key(self.config.private_key)
            return acct.address
        except Exception:
            return self.config.signer_address or "INVALID_PRIVATE_KEY"

    def auto_detect_clipboard_private_key(self) -> bool:
        if self.config.private_key:
            return True
        try:
            raw_clip = subprocess.check_output(["pbpaste"], timeout=1.0).decode("utf-8", errors="ignore").strip()
            cand = raw_clip[2:] if raw_clip.startswith(("0x", "0X")) else raw_clip
            if len(cand) == 64 and all(c in "0123456789abcdefABCDEF" for c in cand):
                pk = "0x" + cand
                acct = Account.from_key(pk)
                if not self.config.signer_address or acct.address.lower() == self.config.signer_address.lower():
                    self.save_config({"private_key": pk, "signer_address": acct.address})
                    return True
        except Exception:
            return False

    def get_public_config(self) -> dict[str, Any]:
        self.auto_detect_clipboard_private_key()
        d = asdict(self.config)
        d["has_private_key"] = bool(self.config.private_key)
        d["has_api_creds"] = bool(self.config.api_key and self.config.api_secret and self.config.api_passphrase)
        d["has_builder_creds"] = bool(self.config.builder_key and self.config.builder_secret and self.config.builder_passphrase)
        d["signer_address"] = self.get_signer_address()
        d["live_wallet"] = self.live_wallet_state
        d["private_key"] = (
            f"{self.config.private_key[:6]}...{self.config.private_key[-4:]}"
            if len(self.config.private_key) > 12
            else ("***" if self.config.private_key else "")
        )
        d["api_secret"] = f"{self.config.api_secret[:5]}***" if self.config.api_secret else ""
        d["api_passphrase"] = f"{self.config.api_passphrase[:4]}***" if self.config.api_passphrase else ""
        d["builder_secret"] = f"{self.config.builder_secret[:5]}***" if self.config.builder_secret else ""
        d["builder_passphrase"] = f"{self.config.builder_passphrase[:4]}***" if self.config.builder_passphrase else ""
        d["installed_vpn_apps"] = self.get_installed_vpn_apps()
        return d

    @staticmethod
    def get_installed_vpn_apps() -> list[dict[str, Any]]:
        return [
            {
                "id": "surfshark",
                "name": "Surfshark VPN (系統級 WireGuard 專線)",
                "path": "/Applications/Surfshark.app",
                "installed": Path("/Applications/Surfshark.app").exists(),
                "best_nodes": "香港 (HK - ~30ms 網頁+API全通)、日本 (JP - API全通)、瑞士 (CH)、挪威 (NO)；注意：杜拜虛擬節點若顯示 IT 則被封鎖",
            },
            {
                "id": "protonvpn",
                "name": "ProtonVPN (免費無限流量)",
                "path": "/Applications/ProtonVPN.app",
                "installed": Path("/Applications/ProtonVPN.app").exists(),
                "best_nodes": "羅馬尼亞 (RO - 網頁+API全通)、日本 (JP - API可下單)、荷蘭 (NL - API可下單)",
            },
            {
                "id": "tunnelbear",
                "name": "TunnelBear (免費自選45國節點)",
                "path": "/Applications/TunnelBear.app",
                "installed": Path("/Applications/TunnelBear.app").exists(),
                "best_nodes": "香港 (HK)、瑞士 (CH)、挪威 (NO)、羅馬尼亞 (RO) — 網頁註冊+API下單 100% 全通",
            },
        ]

    @staticmethod
    def launch_vpn_app(app_id: str) -> dict[str, Any]:
        mapping = {
            "surfshark": "/Applications/Surfshark.app",
            "protonvpn": "/Applications/ProtonVPN.app",
            "tunnelbear": "/Applications/TunnelBear.app",
        }
        target = mapping.get(app_id.lower())
        if not target or not Path(target).exists():
            return {"ok": False, "error": f"找不到應用程式：{target}"}
        try:
            subprocess.Popen(["open", "-a", target])
            return {"ok": True, "message": f"已在 macOS 啟動 {Path(target).stem}！"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _get_failover_proxy_chain(self) -> list[str]:
        """Return ordered proxy/direct candidates (`DIRECT` first in System VPN mode unless known blocked)."""
        backups = ["http://127.0.0.1:10809"] + list(self.backup_proxies) + list(VERIFIED_FREE_PROXIES)
        if self._is_direct_mode():
            direct_blocked = bool(self.direct_geoblock_info) and not bool(
                self.direct_geoblock_info.get("can_trade_api", True)
            )
            chain = (backups + ["DIRECT"]) if direct_blocked else (["DIRECT"] + backups)
        else:
            chain = [self.config.proxy_url.strip()] + backups + ["DIRECT"]
        seen: set[str] = set()
        out: list[str] = []
        for p in chain:
            if p and p not in seen and p != "socks5://43.245.199.34:1080":
                seen.add(p)
                out.append(p)
        return out

    async def _create_v2_client(self, override_proxy: str | None = None) -> AsyncSecureClient:
        if not self.config.private_key:
            raise ValueError("尚未設定 Polymarket 私鑰 (Private Key)")
        if override_proxy is not None:
            px = override_proxy.strip()
            if not px or px.upper() == "DIRECT":
                self._clear_env_proxy()
            else:
                os.environ["ALL_PROXY"] = px
                os.environ["HTTPS_PROXY"] = px
                os.environ["HTTP_PROXY"] = px
        else:
            self._apply_env_proxy()
        creds = None
        if self.config.api_key and self.config.api_secret and self.config.api_passphrase:
            creds = ApiKeyCreds(
                key=self.config.api_key,
                secret=self.config.api_secret,
                passphrase=self.config.api_passphrase,
            )
        bkey = None
        if self.config.builder_key and self.config.builder_secret and self.config.builder_passphrase:
            bkey = BuilderApiKey(
                key=self.config.builder_key,
                secret=self.config.builder_secret,
                passphrase=self.config.builder_passphrase,
            )
        return await AsyncSecureClient.create(
            private_key=self.config.private_key,
            wallet=self.config.funder_address or None,
            credentials=creds,
            api_key=bkey,
        )

    async def _invalidate_warm_client(self) -> None:
        old = self._warm_client
        self._warm_client = None
        self._warm_client_proxy = ""
        if old is not None:
            try:
                await asyncio.wait_for(old.__aexit__(None, None, None), timeout=2.0)
            except Exception:
                pass

    async def _run_with_failover(
        self,
        action_fn: Any,
        warm_timeout: float,
        cold_timeout: float,
        max_failovers: int = 4,
        record: dict[str, Any] | None = None,
    ) -> bool:
        """Execute an async V2 client callback on the warm session first, falling back cleanly on error or geoblock."""
        failed_proxy = ""
        warm = await self.ensure_warm_v2_client()
        if warm is not None:
            failed_proxy = self._warm_client_proxy
            try:
                done = await asyncio.wait_for(action_fn(warm), timeout=warm_timeout)
                err_txt = str((record or {}).get("error") or "").lower()
                is_geo_reject = (
                    (record or {}).get("status") == "REJECTED"
                    and any(tok in err_txt for tok in ("403", "restricted", "geoblock", "region", "blocked"))
                )
                if done is not False and not is_geo_reject:
                    return True
                await self._invalidate_warm_client()
            except Exception as exc:
                if record is not None:
                    record["error"] = str(exc)
                await self._invalidate_warm_client()

        try:
            for cand_proxy in self._get_failover_proxy_chain()[:max_failovers]:
                if failed_proxy and cand_proxy == failed_proxy:
                    continue
                try:
                    client = await asyncio.wait_for(self._create_v2_client(override_proxy=cand_proxy), timeout=8.5)
                    async with client:
                        done = await asyncio.wait_for(action_fn(client), timeout=cold_timeout)
                        if done is not False:
                            return True
                except Exception as exc:
                    if record is not None:
                        record["status"] = "ERROR"
                        record["error"] = str(exc)
                    continue
        finally:
            if self._is_direct_mode():
                self._apply_env_proxy()
        return False

    def _update_wallet_state(self, cash_u: float, port_u: float, proxy: str) -> dict[str, Any]:
        """Update cached live pUSD cash, portfolio value, and warm proxy timestamp."""
        self.live_wallet_state = {
            "cash_pusd": round(float(cash_u), 4),
            "portfolio_pusd": round(float(port_u), 4),
            "total_equity_pusd": round(float(cash_u) + float(port_u), 4),
            "is_fully_approved": True,
            "warm_proxy": proxy,
            "updated_at_ms": int(time.time() * 1000),
        }
        return self.live_wallet_state

    async def ensure_warm_v2_client(self, force_proxy: str | None = None) -> AsyncSecureClient | None:
        """
        Ensures a persistent pre-warmed `AsyncSecureClient` is entered and Keep-Alive ready (~245ms latency).
        Also refreshes liquid `cash_pusd` directly from the collateral check.
        """
        if not self.config.private_key:
            return None
        async with self._warm_lock:
            if self._warm_client is not None and (not force_proxy or self._warm_client_proxy == force_proxy):
                return self._warm_client
            await self._invalidate_warm_client()
            candidates = [force_proxy] if force_proxy is not None else self._get_failover_proxy_chain()[:4]
            try:
                for cand_raw in candidates:
                    cand_proxy = (cand_raw or "").strip() or "DIRECT"
                    c = None
                    try:
                        c = await asyncio.wait_for(self._create_v2_client(override_proxy=cand_proxy), timeout=4.0)
                        await asyncio.wait_for(c.__aenter__(), timeout=3.5)
                        # Warm the TLS + HTTP/2 connection pool immediately and sync liquid cash_pusd
                        bal = await asyncio.wait_for(c.get_balance_allowance(asset_type="COLLATERAL"), timeout=3.5)
                        cash_u = round(float(bal.balance) / 1e6, 4)
                        port_u = float(self.live_wallet_state.get("portfolio_pusd", 0.0)) if self.active_clob_positions else 0.0
                        self._update_wallet_state(cash_u, port_u, cand_proxy)
                        self._warm_client = c
                        self._warm_client_proxy = cand_proxy
                        if (
                            not self._is_direct_mode()
                            and cand_proxy != "DIRECT"
                            and cand_proxy != self.config.proxy_url
                            and not cand_proxy.startswith("http://127.0.0.1")
                        ):
                            self.save_config({"proxy_url": cand_proxy})
                        return self._warm_client
                    except Exception:
                        if c is not None:
                            try:
                                await asyncio.wait_for(c.__aexit__(None, None, None), timeout=2.0)
                            except Exception:
                                pass
                        continue
            finally:
                if self._is_direct_mode():
                    self._apply_env_proxy()
            return None

    async def sync_live_wallet_balance(self, include_portfolio: bool = False) -> dict[str, Any]:
        """
        Query real pUSD collateral balance (+ optional open positions value) via persistent warm client + failover.
        Skips slow Data-API `get_portfolio_value()` when idle unless `include_portfolio=True`.
        """
        if not self.config.private_key:
            return self.live_wallet_state

        need_portfolio = include_portfolio or bool(self.active_clob_positions)

        # 1. Fast path: use persistent pre-warmed client (~245ms)
        warm = await self.ensure_warm_v2_client()
        if warm is not None:
            try:
                bal = await asyncio.wait_for(warm.get_balance_allowance(asset_type="COLLATERAL"), timeout=3.5)
                cash_u = round(float(bal.balance) / 1e6, 4)
                port_u = float(self.live_wallet_state.get("portfolio_pusd", 0.0)) if not need_portfolio else 0.0
                if need_portfolio:
                    try:
                        pv = await asyncio.wait_for(warm.get_portfolio_value(), timeout=3.5)
                        port_u = round(float(pv.value), 4)
                    except Exception:
                        port_u = float(self.live_wallet_state.get("portfolio_pusd", 0.0))
                return self._update_wallet_state(cash_u, port_u, self._warm_client_proxy)
            except Exception as e:
                self.live_wallet_state["last_error"] = str(e)
                await self._invalidate_warm_client()

        # 2. Bounded cold failover path across top 2 healthy channels
        try:
            for cand_proxy in self._get_failover_proxy_chain()[:2]:
                client = None
                try:
                    client = await asyncio.wait_for(self._create_v2_client(override_proxy=cand_proxy), timeout=3.5)
                    await asyncio.wait_for(client.__aenter__(), timeout=3.5)
                    bal = await asyncio.wait_for(client.get_balance_allowance(asset_type="COLLATERAL"), timeout=3.5)
                    cash_u = round(float(bal.balance) / 1e6, 4)
                    port_u = 0.0 if need_portfolio else float(self.live_wallet_state.get("portfolio_pusd", 0.0))
                    if need_portfolio:
                        try:
                            pv = await asyncio.wait_for(client.get_portfolio_value(), timeout=3.5)
                            port_u = round(float(pv.value), 4)
                        except Exception:
                            pass
                    self._update_wallet_state(cash_u, port_u, cand_proxy)
                    if (
                        not self._is_direct_mode()
                        and cand_proxy != "DIRECT"
                        and cand_proxy != self.config.proxy_url
                        and not cand_proxy.startswith("http://127.0.0.1")
                    ):
                        self.save_config({"proxy_url": cand_proxy})
                    return self.live_wallet_state
                except Exception as e:
                    self.live_wallet_state["last_error"] = str(e)
                    continue
                finally:
                    if client is not None:
                        try:
                            await asyncio.wait_for(client.__aexit__(None, None, None), timeout=2.0)
                        except Exception:
                            pass
        finally:
            if self._is_direct_mode():
                self._apply_env_proxy()
        return self.live_wallet_state

    async def auto_redeem_winning_positions(self) -> dict[str, Any]:
        """
        Gasless Auto-Claim / Auto-Redeem Engine:
        1) Executes `plan_collateral_return` + `execute_collateral_return_plan` via Polymarket Relayer
        2) Checks `list_positions(status="REDEEMABLE")` and calls `redeem_positions`
        Automatically turns resolved 100¢ winning positions into liquid pUSD cash!
        """
        if not self.config.private_key:
            return {"ok": False, "claimed_pusd": 0.0, "operations": 0, "error": "No private key"}

        claimed_total = 0.0
        ops_count = 0
        tx_hashes: list[str] = []
        errors: list[str] = []

        try:
            for cand_proxy in self._get_failover_proxy_chain()[:3]:
                try:
                    client = await self._create_v2_client(override_proxy=cand_proxy)
                    async with client:
                        # Ensure BuilderApiKey exists for gasless relayer
                        if not (self.config.builder_key and self.config.builder_secret and self.config.builder_passphrase):
                            try:
                                bkey = await client.create_builder_api_key()
                                self.save_config({
                                    "builder_key": bkey.key,
                                    "builder_secret": bkey.secret,
                                    "builder_passphrase": bkey.passphrase,
                                })
                            except Exception:
                                pass

                        # 1. Collateral Return Plan (sweeps V2 / CTF resolved outcomes into pUSD)
                        try:
                            plan = await client.plan_collateral_return()
                            while float(plan.net_pusd_out) > 0 and plan.operation_count > 0:
                                net_out = float(plan.net_pusd_out)
                                handle = await client.execute_collateral_return_plan(plan=plan)
                                outcome = await handle.wait()
                                claimed_total += net_out
                                ops_count += int(plan.operation_count)
                                tx_h = getattr(outcome, "transaction_hash", None) or getattr(handle, "transaction_hash", None)
                                if tx_h:
                                    tx_hashes.append(str(tx_h))
                                if not plan.truncated:
                                    break
                                plan = await client.plan_collateral_return()
                        except Exception as e:
                            errors.append(f"collateral_return: {e}")

                        # 2. Check explicit REDEEMABLE positions from Data API
                        try:
                            paginator = client.list_positions(status="REDEEMABLE")
                            page = await paginator.first_page()
                            for pos in getattr(page, "items", []) or []:
                                cid = getattr(pos, "condition_id", None)
                                pid = getattr(pos, "position_id", None)
                                cur_val = float(getattr(pos, "current_value", 0.0) or 0.0)
                                if cur_val <= 0:
                                    continue
                                try:
                                    if cid:
                                        h = await client.redeem_positions(condition_id=str(cid))
                                    elif pid:
                                        h = await client.redeem_positions(position_id=str(pid))
                                    else:
                                        continue
                                    out = await h.wait()
                                    claimed_total += cur_val
                                    ops_count += 1
                                    tx_h = getattr(out, "transaction_hash", None) or getattr(h, "transaction_hash", None)
                                    if tx_h:
                                        tx_hashes.append(str(tx_h))
                                except Exception as inner_e:
                                    errors.append(f"redeem_pos({cid or pid}): {inner_e}")
                        except Exception:
                            pass

                        # Refresh wallet balance after claim
                        bal = await client.get_balance_allowance(asset_type="COLLATERAL")
                        cash_u = round(float(bal.balance) / 1e6, 4)
                        self.live_wallet_state["cash_pusd"] = cash_u
                        self.live_wallet_state["total_equity_pusd"] = round(cash_u + float(self.live_wallet_state.get("portfolio_pusd", 0.0)), 4)
                        self.live_wallet_state["updated_at_ms"] = int(time.time() * 1000)
                        break
                except Exception as e:
                    errors.append(str(e))
                    continue
        finally:
            if self._is_direct_mode():
                self._apply_env_proxy()

        res = {
            "ok": True,
            "timestamp_ms": int(time.time() * 1000),
            "claimed_pusd": round(claimed_total, 4),
            "operations": ops_count,
            "tx_hashes": tx_hashes,
            "cash_pusd_after": self.live_wallet_state.get("cash_pusd", 0.0),
            "errors": errors,
        }
        if claimed_total > 0:
            self.redeem_history.insert(0, res)
            self.redeem_history = self.redeem_history[:50]
        return res

    async def _probe_geoblock_with_proxy(self, proxy: str | None) -> dict[str, Any] | None:
        eff_proxy = None if proxy in (None, "", "DIRECT") else proxy
        try:
            async with httpx.AsyncClient(proxy=eff_proxy, trust_env=False, timeout=6.0) as client:
                resp = await client.get(GEOBLOCK_URL)
                if resp.status_code != 200:
                    return None
                data = resp.json()
                blocked = bool(data.get("blocked", True))
                country = str(data.get("country", "UNKNOWN")).upper()
                region = str(data.get("region", ""))
                ip = str(data.get("ip", ""))
                api_only_ok = country in FRONTEND_ONLY_RESTRICTED_COUNTRIES
                can_trade_api = (not blocked) or api_only_ok
                return {
                    "blocked": blocked,
                    "can_trade_api": can_trade_api,
                    "api_only_unrestricted": api_only_ok,
                    "ip": ip,
                    "country": country,
                    "region": region,
                    "proxy_used": eff_proxy or "Surfshark / System VPN Direct",
                }
        except Exception:
            return None

    async def check_geoblock_status(self) -> dict[str, Any]:
        is_direct = self._is_direct_mode()
        proxy = None if is_direct else self.config.proxy_url.strip()
        probe = await self._probe_geoblock_with_proxy(proxy)
        if is_direct:
            self.direct_geoblock_info = dict(probe) if probe else {}

        backup_probe: dict[str, Any] | None = None
        if not probe or not probe.get("can_trade_api"):
            for cand_proxy in list(self.backup_proxies) + list(VERIFIED_FREE_PROXIES):
                if cand_proxy in (proxy, "DIRECT", ""):
                    continue
                cand_probe = await self._probe_geoblock_with_proxy(cand_proxy)
                if cand_probe and cand_probe.get("can_trade_api"):
                    if is_direct:
                        backup_probe = cand_probe
                    else:
                        self.save_config({"proxy_url": cand_proxy})
                        probe = cand_probe
                    break

        if probe:
            blocked = probe["blocked"]
            can_trade_api = probe["can_trade_api"]
            api_only_ok = probe["api_only_unrestricted"]
            ip = probe["ip"]
            country = probe["country"]
            region = probe["region"]
            proxy_used = probe["proxy_used"]

            if is_direct and not can_trade_api:
                bk_note = (
                    f"（切換期間已由緊急備援 {backup_probe['ip']} / {backup_probe['country']} 護航不漏單）"
                    if backup_probe
                    else "（目前無備援通道，請立即切換 VPN 國家）"
                )
                advice = (
                    f"⚠️ 偵測到系統 VPN 直連出口 ({ip} / {country}) 被 Polymarket 列為封鎖地區"
                    f"（Surfshark「杜拜」虛擬機房實測位於義大利 IT）。"
                    f"請將 Surfshark 切換至 **香港 (HK)**、**日本 (JP)**、**瑞士 (CH)** 或 **挪威 (NO)** 即可 100% 原生直連！{bk_note}"
                )
                self.last_geoblock_status = {
                    "ok": True,
                    "blocked": True,
                    "direct_blocked": True,
                    "can_trade_api": bool(backup_probe),
                    "api_only_unrestricted": False,
                    "ip": ip,
                    "country": country,
                    "region": region,
                    "proxy_used": proxy_used,
                    "backup_ready": bool(backup_probe),
                    "advice": advice,
                    "checked_at_ms": int(time.time() * 1000),
                }
                return self.last_geoblock_status

            if not blocked:
                mode_tag = "Surfshark / 系統 VPN 直連" if is_direct else proxy_used
                advice = f"✅ {mode_tag} ({ip} / {country}) 100% 完全解鎖！Polymarket 網頁端與 CLOB V2 API 皆可原生直連下單。"
            elif api_only_ok:
                mode_tag = "Surfshark / 系統 VPN 直連" if is_direct else proxy_used
                advice = f"🟢 {mode_tag} ({ip} / {country}) 屬於「CLOB V2 API 完全開放」地區！（免費代理已關閉）"
            else:
                advice = f"⛔ 當前 IP ({ip} / {country}-{region}) 被 Polymarket 列為禁止下單地區（HTTP 403）。"

            self.last_geoblock_status = {
                "ok": True,
                "blocked": blocked,
                "direct_blocked": bool(is_direct and not can_trade_api),
                "can_trade_api": can_trade_api,
                "api_only_unrestricted": api_only_ok,
                "ip": ip,
                "country": country,
                "region": region,
                "proxy_used": proxy_used,
                "advice": advice,
                "checked_at_ms": int(time.time() * 1000),
            }
            return self.last_geoblock_status

        self.last_geoblock_status = {
            "ok": False,
            "blocked": True,
            "can_trade_api": False,
            "ip": "連線失敗",
            "country": "ERR",
            "region": "",
            "proxy_used": proxy or "Surfshark / System VPN Direct",
            "advice": "⚠️ 無法連線至 Polymarket Geoblock 檢測端點",
            "checked_at_ms": int(time.time() * 1000),
        }
        return self.last_geoblock_status

    async def derive_and_verify_credentials(self) -> dict[str, Any]:
        if not self.config.private_key:
            return {"ok": False, "error": "請先輸入您的 Polymarket 私鑰 (Private Key)"}
        try:
            wallet_state = await self.sync_live_wallet_balance()
            geo = await self.check_geoblock_status()
            return {
                "ok": True,
                "signer_address": self.get_signer_address(),
                "funder_address": self.config.funder_address or self.get_signer_address(),
                "api_key": self.config.api_key,
                "usdc_balance": wallet_state.get("cash_pusd", 0.0),
                "portfolio_balance": wallet_state.get("portfolio_pusd", 0.0),
                "geoblock": geo,
            }
        except Exception as e:
            return {"ok": False, "error": f"API 驗證失敗：{e}"}

    async def discover_live_5m_market(
        self,
        symbol: str,
        bucket_offset: int = 0,
        target_bucket_ts: int | None = None,
        fast_cached_only: bool = False,
    ) -> dict[str, Any] | None:
        sym_up = symbol.upper()
        prefix_map = {"BTCUSDT": "btc", "ETHUSDT": "eth", "SOLUSDT": "sol"}
        prefix = prefix_map.get(sym_up, "eth")
        now_bucket = (
            int(target_bucket_ts)
            if target_bucket_ts is not None
            else ((int(time.time()) // 300) + bucket_offset) * 300
        )
        cache_key = (sym_up, now_bucket)
        cached = self._bucket_market_cache.get(cache_key)
        if cached is not None:
            cached_ts, cached_mkt = cached
            has_tokens = bool(cached_mkt.get("up_token_id")) and bool(cached_mkt.get("down_token_id"))
            if (time.time() - cached_ts) < 45.0 and has_tokens and (
                cached_mkt.get("price_to_beat") is not None or fast_cached_only
            ):
                return cached_mkt
        slug = f"{prefix}-updown-5m-{now_bucket}"

        proxy = None if self._is_direct_mode() else (self.config.proxy_url.strip() or None)
        # Gamma API is public read-only and works fastest/cleanest via direct connection (proxy=None),
        # while some SOCKS5 proxies get 403 on Gamma /events. Try direct first, then proxy fallback.
        for cand_proxy in ([None, proxy] if proxy else [None]):
            try:
                async with httpx.AsyncClient(
                    proxy=cand_proxy,
                    trust_env=(cand_proxy is not None),
                    timeout=5.0,
                ) as client:
                    # Query /events?slug=... so we also get eventMetadata.priceToBeat (official Chainlink 60s TWAP strike!)
                    ev_resp = await client.get("https://gamma-api.polymarket.com/events", params={"slug": slug})
                    m = None
                    price_to_beat = None
                    if ev_resp.status_code == 200:
                        ev_arr = ev_resp.json()
                        if ev_arr and isinstance(ev_arr, list):
                            ev_obj = ev_arr[0]
                            ev_meta = ev_obj.get("eventMetadata") or {}
                            if isinstance(ev_meta, str):
                                try:
                                    ev_meta = json.loads(ev_meta)
                                except Exception:
                                    ev_meta = {}
                            if ev_meta and ev_meta.get("priceToBeat") is not None:
                                try:
                                    price_to_beat = float(ev_meta["priceToBeat"])
                                except Exception:
                                    price_to_beat = None
                            mkts = ev_obj.get("markets") or []
                            if mkts:
                                m = mkts[0]

                    # Dual-Track Chainlink Anchor Fallback: when current bar's priceToBeat is temporarily None,
                    # bar[t-1]'s eventMetadata.finalPrice == bar[t].priceToBeat!
                    if price_to_beat is None and bucket_offset <= 0:
                        try:
                            prev_slug = f"{prefix}-updown-5m-{now_bucket - 300}"
                            prev_resp = await client.get("https://gamma-api.polymarket.com/events", params={"slug": prev_slug})
                            if prev_resp.status_code == 200 and prev_resp.json():
                                p_meta = prev_resp.json()[0].get("eventMetadata") or {}
                                if isinstance(p_meta, str):
                                    p_meta = json.loads(p_meta)
                                if p_meta.get("finalPrice") is not None:
                                    price_to_beat = float(p_meta["finalPrice"])
                        except Exception:
                            pass

                    if not m:
                        resp = await client.get(GAMMA_MARKETS_URL, params={"slug": slug})
                        if resp.status_code != 200:
                            continue
                        arr = resp.json()
                        if not arr:
                            continue
                        m = arr[0]

                    raw_tokens = m.get("clobTokenIds")
                    tokens = json.loads(raw_tokens) if isinstance(raw_tokens, str) else (raw_tokens or [])
                    raw_outcomes = m.get("outcomes")
                    outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else (raw_outcomes or ["Up", "Down"])
                    raw_prices = m.get("outcomePrices")
                    prices = json.loads(raw_prices) if isinstance(raw_prices, str) else (raw_prices or ["0.50", "0.50"])

                    idx = resolve_up_down_indices(outcomes)
                    if len(tokens) < 2 or idx is None:
                        continue  # never guess which token is Up/Down
                    up_i, dn_i = idx

                    mkt_dict = {
                        "symbol": sym_up,
                        "slug": slug,
                        "bucket_ts": now_bucket,
                        "question": m.get("question", slug),
                        "condition_id": m.get("conditionId", ""),
                        "up_token_id": str(tokens[up_i]),
                        "down_token_id": str(tokens[dn_i]),
                        "outcomes": outcomes,
                        "up_price_cents": round(float(prices[up_i]) * 100.0, 1) if len(prices) > up_i else 50.0,
                        "down_price_cents": round(float(prices[dn_i]) * 100.0, 1) if len(prices) > dn_i else 50.0,
                        "price_to_beat": price_to_beat,
                        "resolution_source": str(m.get("resolutionSource") or ""),
                        "min_tick_size": float(m.get("orderPriceMinTickSize") or 0.01),
                        "min_order_size": float(m.get("orderMinSize") or 5.0),
                        "neg_risk": bool(m.get("negRisk", False)),
                        "event_url": f"https://polymarket.com/event/{slug}",
                    }
                    now_ts = time.time()
                    self._bucket_market_cache[cache_key] = (now_ts, mkt_dict)
                    stale_keys = [k for k, (ts_k, _) in self._bucket_market_cache.items() if (now_ts - ts_k) > 600.0]
                    for sk in stale_keys:
                        self._bucket_market_cache.pop(sk, None)
                    return mkt_dict
            except Exception:
                continue
        return None

    async def refresh_all_live_5m_markets(self, include_next_bucket: bool = False) -> dict[str, dict[str, Any]]:
        now = time.time()
        if not include_next_bucket and now - self._cache_ts < 8.0 and self.live_5m_markets_cache:
            return self.live_5m_markets_cache

        coros = [
            self.discover_live_5m_market("BTCUSDT", 0),
            self.discover_live_5m_market("ETHUSDT", 0),
            self.discover_live_5m_market("SOLUSDT", 0),
        ]
        if include_next_bucket:
            coros.extend([
                self.discover_live_5m_market("BTCUSDT", 1),
                self.discover_live_5m_market("ETHUSDT", 1),
                self.discover_live_5m_market("SOLUSDT", 1),
            ])
        results = await asyncio.gather(*coros, return_exceptions=True)
        out: dict[str, dict[str, Any]] = {}
        for sym, r in zip(["BTCUSDT", "ETHUSDT", "SOLUSDT"], results[:3]):
            if isinstance(r, dict):
                out[sym] = r
        if out:
            self.live_5m_markets_cache = out
            self._cache_ts = now
        return self.live_5m_markets_cache

    async def _check_cond_token_shares(self, client: AsyncSecureClient, token_id: str) -> float:
        """Query live conditional token share balance across V2 and legacy asset types."""
        c_sh = 0.0
        for c_type in ("CONDITIONAL-V2", "CONDITIONAL"):
            try:
                b = await client.get_balance_allowance(asset_type=c_type, token_id=token_id)
                if b is not None and int(b.balance) > 0:
                    c_sh = max(c_sh, round(float(b.balance) / 1e6, 2))
            except Exception as exc:
                print(f"[CLOB Warning] check conditional balance ({c_type}) failed: {exc}")
        return c_sh

    @staticmethod
    def _extract_order_match_info(resp: Any, fallback_shares: float) -> tuple[bool, float, float, str]:
        """Extract matched status, shares, USDC spend, and order_id from a CLOB order response."""
        st_upper = str(getattr(resp, "status", "POSTED")).upper()
        oid_str = str(getattr(resp, "order_id", "") or "")
        t_raw = float(getattr(resp, "taking_amount", 0.0) or 0.0)
        m_raw = float(getattr(resp, "making_amount", 0.0) or 0.0)
        t_norm = round(t_raw / 1e6, 4) if t_raw > 10000 else t_raw
        m_norm = round(m_raw / 1e6, 4) if m_raw > 10000 else m_raw
        is_matched = ("MATCHED" in st_upper) or (t_norm >= 1.0)
        filled_sh = round(t_norm, 2) if t_norm >= 1.0 else (round(fallback_shares, 2) if is_matched else 0.0)
        return is_matched, filled_sh, m_norm, oid_str

    async def _poll_limit_order_fill(
        self,
        client: AsyncSecureClient,
        oid_str: str,
        token_id: str,
        fallback_shares: float,
        polls: int,
        interval_sec: float = 1.5,
    ) -> tuple[bool, float]:
        """Poll an accepted CLOB limit order up to `polls * interval_sec` and verify fill."""
        for _ in range(max(1, polls)):
            await asyncio.sleep(interval_sec)
            try:
                ord_info = await client.get_order(order_id=oid_str)
                sz_m = float(getattr(ord_info, "size_matched", 0.0) or 0.0)
                ord_st = str(getattr(ord_info, "status", "")).upper()
                if sz_m >= 1.0 or "MATCHED" in ord_st:
                    return True, (round(sz_m, 2) if sz_m >= 1.0 else round(fallback_shares, 2))
            except Exception:
                c_sh = await self._check_cond_token_shares(client, token_id)
                if c_sh >= 1.0:
                    return True, c_sh
            c_sh = await self._check_cond_token_shares(client, token_id)
            if c_sh >= 1.0:
                return True, c_sh
        return False, 0.0

    @staticmethod
    def _parse_order_book_top(
        ob: Any,
    ) -> tuple[float | None, float | None, float, float | None]:
        """Extract sorted (best_ask, best_bid, best_bid_size, mid_price) from an orderbook."""
        asks = sorted(
            [(float(a.price), float(getattr(a, "size", 0.0) or 0.0)) for a in (getattr(ob, "asks", None) or [])],
            key=lambda x: x[0],
        )
        bids = sorted(
            [(float(b.price), float(getattr(b, "size", 0.0) or 0.0)) for b in (getattr(ob, "bids", None) or [])],
            key=lambda x: -x[0],
        )
        best_ask = asks[0][0] if asks else None
        best_bid = bids[0][0] if bids else None
        best_bid_sz = bids[0][1] if bids else 0.0
        mid_px = (best_bid + best_ask) / 2.0 if (best_bid is not None and best_ask is not None) else best_ask
        return best_ask, best_bid, best_bid_sz, mid_px

    async def execute_5m_entry(
        self,
        symbol: str,
        direction: str,
        stake_usd: float,
        step: int = 1,
        reason: str = "SIGNAL_ENTRY",
        vault_locked_u: float = 0.0,
        prior_cost_u: float = 0.0,
        s1_shares_base: float = 5.0,
        market: dict[str, Any] | None = None,
        target_bucket_ts: int | None = None,
    ) -> dict[str, Any]:
        """
        Places a Polymarket V2 5m Up/Down CLOB BUY order using adaptive 2-stage execution:
        1) Top-of-book Maker attempt (0% fee) when spread/queue permits (left resting as LIVE_LIVE if ask is skewed);
        2) Marketable Limit sweep up to `max_taker_entry_price` (0.58 / 0.60 on rescue) so tilted pools fill reliably.
        """
        if market is None:
            market = await self.discover_live_5m_market(
                symbol, bucket_offset=0, target_bucket_ts=target_bucket_ts, fast_cached_only=True
            )
            if not market:
                market = await self.discover_live_5m_market(symbol, bucket_offset=0, target_bucket_ts=target_bucket_ts)
            if not market:
                market = await self.discover_live_5m_market(symbol, bucket_offset=1, target_bucket_ts=target_bucket_ts)

        is_up = direction in ("LONG_BOUNCE", "UP")
        outcome_label = "UP (買漲)" if is_up else "DOWN (買跌)"
        token_id = (
            market["up_token_id"] if (market and is_up) else (market["down_token_id"] if market else "UNKNOWN_TOKEN_ID")
        )
        slug = market["slug"] if market else f"{symbol.lower()[:3]}-updown-5m-live"
        condition_id = market.get("condition_id", "") if market else ""

        base_limit = float(self.config.entry_limit_price)
        max_maker_px = float(getattr(self.config, "max_maker_entry_price", 0.52))
        max_taker_px = float(
            getattr(self.config, "rescue_max_taker_entry_price", 0.55)
            if step >= 2
            else getattr(self.config, "max_taker_entry_price", 0.54)
        )
        price = round(min(max_maker_px, max(0.43, base_limit)), 2)
        raw_shares = round(float(stake_usd) / price, 2)
        shares = max(5.0, raw_shares) if self.config.enforce_min_5_shares else raw_shares

        eff_vault_u = max(0.0, float(vault_locked_u))
        if eff_vault_u <= 0.0:
            try:
                pa_path = CONFIG_PATH.parent / "paper_account.json"
                if pa_path.exists():
                    pa_data = json.loads(pa_path.read_text(encoding="utf-8"))
                    eff_vault_u = max(0.0, float(pa_data.get("vault_locked_u", 0.0)))
            except Exception as exc:
                print(f"[CLOB Warning] read paper_account vault_locked_u failed: {exc}")

        raw_cash = float(self.live_wallet_state.get("cash_pusd", 0.0))
        avail_cash = max(0.0, raw_cash - eff_vault_u)

        def _compute_min_rescue_shares(unit_px: float, is_taker: bool) -> float:
            fee_per_sh = (0.07 * unit_px * (1.0 - unit_px)) if is_taker else 0.0
            all_in_unit = max(0.01, unit_px + fee_per_sh)
            min_prof_u = max(0.25, round(0.08 * s1_shares_base, 2))
            return float(math.ceil((prior_cost_u + min_prof_u) / max(0.10, 1.0 - all_in_unit)))

        def _recalc_affordable_shares(unit_px: float, is_taker: bool) -> float:
            fee_per_sh = (0.07 * unit_px * (1.0 - unit_px)) if is_taker else 0.0
            all_in_unit = max(0.01, unit_px + fee_per_sh)
            sh_req = float(shares)
            if step >= 2 and prior_cost_u > 0.0:
                sh_cost_rec = _compute_min_rescue_shares(unit_px, is_taker)
                sh_req = max(sh_req, sh_cost_rec)
                if not self.config.dry_run and raw_cash > 0.0:
                    max_aff = math.floor(((avail_cash - 0.05) / all_in_unit) * 100.0) / 100.0
                    if max_aff < sh_cost_rec:
                        return 0.0
                    sh_req = min(sh_req, max_aff)
                return round(max(5.0, sh_req), 2)
            if not self.config.dry_run and raw_cash > 0.0 and avail_cash >= round(5.0 * all_in_unit, 2):
                max_aff = math.floor(((avail_cash - 0.05) / all_in_unit) * 100.0) / 100.0
                sh_req = min(sh_req, max(5.0, max_aff))
            return round(max(5.0, sh_req), 2)

        def _insufficient_rescue_record(unit_px: float, needed_u: float) -> dict[str, Any]:
            return {
                "timestamp_ms": int(time.time() * 1000),
                "symbol": symbol,
                "slug": slug,
                "condition_id": condition_id,
                "question": market.get("question", slug) if market else slug,
                "event_url": market.get("event_url", "https://polymarket.com/crypto") if market else "https://polymarket.com/crypto",
                "action": "BUY_ENTRY",
                "reason": reason,
                "step": step,
                "direction": direction,
                "outcome": outcome_label,
                "token_id": token_id,
                "price": unit_px,
                "price_cents": round(unit_px * 100.0, 1),
                "raw_requested_shares": raw_shares,
                "shares": 0.0,
                "effective_cost_u": 0.0,
                "mode": "LIVE_CLOB_V2",
                "status": "SKIPPED_INSUFFICIENT_RESCUE_CASH",
                "order_id": None,
                "error": f"Insufficient rescue cash: avail=${avail_cash:.2f}U < required ${needed_u:.2f}U (prior=${prior_cost_u:.2f}U)",
            }

        if not self.config.dry_run and raw_cash > 0.0:
            if eff_vault_u > 0.0 and avail_cash < 2.55:
                record_blocked: dict[str, Any] = {
                    "timestamp_ms": int(time.time() * 1000),
                    "symbol": symbol,
                    "slug": slug,
                    "condition_id": condition_id,
                    "question": market.get("question", slug) if market else slug,
                    "event_url": market.get("event_url", "https://polymarket.com/crypto") if market else "https://polymarket.com/crypto",
                    "action": "BUY_ENTRY",
                    "reason": reason,
                    "step": step,
                    "direction": direction,
                    "outcome": outcome_label,
                    "token_id": token_id,
                    "price": price,
                    "price_cents": round(price * 100.0, 1),
                    "raw_requested_shares": raw_shares,
                    "shares": 0.0,
                    "effective_cost_u": 0.0,
                    "mode": "LIVE_CLOB_V2",
                    "status": "VAULT_PROTECTED",
                    "order_id": None,
                    "error": f"已觸發鎖倉保底金保護：鎖死本金 ${eff_vault_u:.2f}U 絕不動用（錢包現金 ${raw_cash:.2f}U，可動用作戰餘額 ${avail_cash:.2f}U < $2.55U）",
                }
                self.order_history.insert(0, record_blocked)
                self.order_history = self.order_history[:100]
                return record_blocked
            if step >= 2 and prior_cost_u > 0.0:
                min_rescue_sh = _compute_min_rescue_shares(price, is_taker=False)
                req_rescue_cash = round(min_rescue_sh * price, 2) + 0.05
                if avail_cash < req_rescue_cash:
                    try:
                        await asyncio.wait_for(self.auto_redeem_winning_positions(), timeout=4.5)
                        w_ref = await asyncio.wait_for(self.sync_live_wallet_balance(), timeout=3.5)
                        raw_cash = float(w_ref.get("cash_pusd", raw_cash))
                        avail_cash = max(0.0, raw_cash - eff_vault_u)
                    except Exception as exc:
                        print(f"[CLOB Warning] rescue redeem/wallet refresh failed ({symbol}): {exc}")
                    if avail_cash < req_rescue_cash:
                        rec_skip = _insufficient_rescue_record(price, req_rescue_cash)
                        self.order_history.insert(0, rec_skip)
                        self.order_history = self.order_history[:100]
                        return rec_skip
            shares = _recalc_affordable_shares(price, is_taker=False)
            if shares <= 0.0:
                rec_skip = _insufficient_rescue_record(price, round(_compute_min_rescue_shares(price, is_taker=False) * price, 2) + 0.05)
                self.order_history.insert(0, rec_skip)
                self.order_history = self.order_history[:100]
                return rec_skip

        effective_cost_u = round(shares * price, 2)

        record: dict[str, Any] = {
            "timestamp_ms": int(time.time() * 1000),
            "symbol": symbol,
            "slug": slug,
            "condition_id": condition_id,
            "question": market.get("question", slug) if market else slug,
            "event_url": market.get("event_url", "https://polymarket.com/crypto") if market else "https://polymarket.com/crypto",
            "action": "BUY_ENTRY",
            "reason": reason,
            "step": step,
            "direction": direction,
            "outcome": outcome_label,
            "token_id": token_id,
            "price": price,
            "price_cents": round(price * 100.0, 1),
            "raw_requested_shares": raw_shares,
            "shares": shares,
            "effective_cost_u": effective_cost_u,
            "mode": "DRY_RUN" if (self.config.dry_run or not self.config.enabled) else "LIVE_CLOB_V2",
            "status": "PENDING",
            "order_id": None,
            "error": None,
        }

        if self.config.dry_run:
            sim_px = price
            if token_id and token_id != "UNKNOWN_TOKEN_ID":
                try:
                    best_bid, best_ask = await asyncio.wait_for(self.get_live_token_best_bid_ask(token_id), timeout=2.5)
                    mid_px = (best_bid + best_ask) / 2.0 if (best_bid is not None and best_ask is not None) else (best_ask or best_bid)
                    if (best_ask is not None and best_ask < 0.43) or (mid_px is not None and mid_px < 0.43):
                        record["status"] = "BLOCK_ADVERSE_CLOB_ODDS"
                        record["shares"] = 0.0
                        record["effective_cost_u"] = 0.0
                        record["error"] = f"Live CLOB implied prob collapsed (ask={best_ask}, mid={mid_px} < 0.43): adverse Chainlink move"
                        self.order_history.insert(0, record)
                        self.order_history = self.order_history[:100]
                        return record
                    if best_ask is not None and best_ask > max_taker_px and (best_bid is None or best_bid > max_taker_px):
                        record["status"] = "BLOCK_SKEWED_CLOB_ASK"
                        record["shares"] = 0.0
                        record["effective_cost_u"] = 0.0
                        record["error"] = f"Live CLOB skewed: ask={best_ask} exceeds max_taker_px={max_taker_px}"
                        self.order_history.insert(0, record)
                        self.order_history = self.order_history[:100]
                        return record
                    sim_px = round(max(0.43, min(max_taker_px, best_ask if best_ask is not None else price)), 2)
                except Exception as exc:
                    print(f"[DRY_RUN Warning] live book fetch failed: {exc}")
            shares = _recalc_affordable_shares(sim_px, is_taker=(sim_px > 0.50))
            effective_cost_u = round(shares * sim_px, 2)
            record["price"] = sim_px
            record["price_cents"] = round(sim_px * 100.0, 1)
            record["shares"] = shares
            record["effective_cost_u"] = effective_cost_u
            record["status"] = "SIM_MATCHED"
            record["order_id"] = f"sim-{int(time.time() * 1000)}"
            record["mode"] = "DRY_RUN_SIM"
            self.active_clob_positions[symbol] = {
                "symbol": symbol,
                "slug": slug,
                "condition_id": condition_id,
                "token_id": token_id,
                "outcome": outcome_label,
                "direction": direction,
                "entry_price": sim_px,
                "shares": shares,
                "effective_cost_u": effective_cost_u,
                "step": step,
                "entry_order_id": record["order_id"],
                "entry_matched": True,
                "entry_filled_shares": shares,
                "bounce_sell_placed": False,
                "bounce_sell_matched": False,
                "sell_order_id": None,
                "is_dry_run": True,
            }
            self.order_history.insert(0, record)
            self.order_history = self.order_history[:100]
            return record

        if not self.config.enabled:
            record["status"] = "PAUSED"
            record["order_id"] = f"paused-{int(time.time() * 1000)}"
            self.order_history.insert(0, record)
            self.order_history = self.order_history[:100]
            return record

        async def _submit_entry_with_client(client: AsyncSecureClient) -> bool:
            nonlocal price, shares, effective_cost_u
            old_pos = self.active_clob_positions.get(symbol)
            if old_pos and old_pos.get("entry_order_id"):
                try:
                    await client.cancel_order(order_id=str(old_pos["entry_order_id"]))
                except Exception as exc:
                    print(f"[CLOB Warning] cancel stale order {old_pos['entry_order_id']} failed: {exc}")

            # Step A: Live Orderbook Inspection & VPIN/Skew Guards
            best_ask, best_bid, best_bid_sz, mid_px = None, None, 0.0, None
            try:
                ob = await asyncio.wait_for(client.get_order_book(token_id=token_id), timeout=2.0)
                best_ask, best_bid, best_bid_sz, mid_px = self._parse_order_book_top(ob)
            except Exception as exc:
                print(f"[CLOB Warning] pre-entry orderbook query failed for {symbol}: {exc}")

            if (best_ask is not None and best_ask < 0.43) or (mid_px is not None and mid_px < 0.43):
                record["status"] = "BLOCK_ADVERSE_CLOB_ODDS"
                record["shares"] = 0.0
                record["effective_cost_u"] = 0.0
                record["error"] = f"Live CLOB VPIN block: best_ask={best_ask}, mid={mid_px} < 0.43"
                return True

            is_matched = False
            filled_sh = 0.0
            making_val = 0.0
            oid_str = ""
            ob_refreshed = False

            def _park_resting_maker_order(oid: str, m_px: float, m_sh: float) -> bool:
                """Record a resting Post-Only Maker limit BUY as LIVE_LIVE for 90s intra-candle fill tracking."""
                record["price"] = m_px
                record["price_cents"] = round(m_px * 100.0, 1)
                record["shares"] = m_sh
                record["effective_cost_u"] = round(m_sh * m_px, 2)
                record["status"] = "LIVE_LIVE"
                record["order_id"] = oid
                record["error"] = None
                self.active_clob_positions[symbol] = {
                    "symbol": symbol,
                    "slug": slug,
                    "condition_id": condition_id,
                    "token_id": token_id,
                    "outcome": outcome_label,
                    "direction": direction,
                    "entry_price": m_px,
                    "requested_shares": m_sh,
                    "shares": 0.0,
                    "effective_cost_u": 0.0,
                    "step": step,
                    "entry_order_id": oid,
                    "entry_matched": False,
                    "entry_filled_shares": 0.0,
                    "entry_verified_matched": False,
                    "entry_status": "LIVE_LIVE",
                    "bounce_sell_placed": False,
                    "bounce_sell_matched": False,
                    "sell_order_id": None,
                }
                return True

            # Step B: Stage 1 Fast Top-of-Book Maker Attempt (left resting as LIVE_LIVE if best_ask > max_taker_px)
            spread_1tick = (best_bid is not None and best_ask is not None and round(best_ask - best_bid, 4) <= 0.01)
            maker_target_px = round((best_ask - 0.01), 4) if best_ask is not None else price
            can_sweep_now = (best_ask is not None and round(best_ask, 4) <= round(max_taker_px, 4))
            go_straight_to_sweep = can_sweep_now and (
                round(best_ask, 4) <= 0.50
                or (
                    step >= 2
                    and spread_1tick
                    and best_bid_sz >= 40.0
                )
                or (maker_target_px > round(max_maker_px, 4))
                or (spread_1tick and best_bid_sz >= 80.0)
            )
            if not go_straight_to_sweep:
                maker_px = round(max(0.43, min(max_maker_px, (best_ask - 0.01) if best_ask is not None else price)), 2)
                maker_sh = _recalc_affordable_shares(maker_px, is_taker=False)
                if maker_sh <= 0.0:
                    record["status"] = "SKIPPED_INSUFFICIENT_RESCUE_CASH"
                    record["shares"] = 0.0
                    record["effective_cost_u"] = 0.0
                    record["error"] = f"Insufficient rescue cash for Maker @ {maker_px:.2f} (prior=${prior_cost_u:.2f}U)"
                    return True
                try:
                    m_resp = await client.place_limit_order(
                        token_id=token_id,
                        price=f"{maker_px:.2f}",
                        size=f"{maker_sh:.2f}",
                        side="BUY",
                        post_only=True,
                    )
                    if isinstance(m_resp, AcceptedOrder) or getattr(m_resp, "ok", False):
                        is_matched, filled_sh, making_val, oid_str = self._extract_order_match_info(m_resp, maker_sh)
                        if not is_matched and oid_str:
                            wait_polls = max(1, int(round(float(getattr(self.config, "maker_wait_sec", 3.0)) / 1.5)))
                            is_matched, filled_sh = await self._poll_limit_order_fill(
                                client, oid_str, token_id, maker_sh, polls=wait_polls, interval_sec=1.5
                            )
                        if not is_matched and filled_sh < 1.0 and oid_str:
                            # Refresh live best_ask BEFORE cancelling so a skewed tick-up preserves our resting Maker
                            try:
                                ob_ref = await asyncio.wait_for(client.get_order_book(token_id=token_id), timeout=1.5)
                                ask_now, _, _, _ = self._parse_order_book_top(ob_ref)
                                ob_refreshed = True
                                if ask_now is not None:
                                    best_ask = ask_now
                            except Exception as exc:
                                print(f"[CLOB Warning] Stage-1 post-wait orderbook refresh ({symbol}): {exc}")
                            if best_ask is not None and round(best_ask, 4) > round(max_taker_px, 4):
                                return _park_resting_maker_order(oid_str, maker_px, maker_sh)
                            try:
                                await client.cancel_order(order_id=oid_str)
                            except Exception as exc:
                                print(f"[CLOB Warning] cancel maker {oid_str} failed: {exc}")
                            final_c_sh = await self._check_cond_token_shares(client, token_id)
                            if final_c_sh >= 1.0:
                                is_matched = True
                                filled_sh = final_c_sh
                        if is_matched and filled_sh >= 1.0:
                            price = maker_px
                            shares = filled_sh
                except Exception as exc:
                    print(f"[CLOB Info] Stage-1 Maker skipped to Stage-2 sweep ({symbol}): {exc}")

            # Step C: Stage 2 Guaranteed-Fill Marketable Limit Sweep (post_only=False)
            if not is_matched and filled_sh < 1.0:
                if (not ob_refreshed and not go_straight_to_sweep) or best_ask is None:
                    try:
                        ob_ref = await asyncio.wait_for(client.get_order_book(token_id=token_id), timeout=1.5)
                        ask_now, _, _, _ = self._parse_order_book_top(ob_ref)
                        if ask_now is not None:
                            best_ask = ask_now
                    except Exception as exc:
                        print(f"[CLOB Warning] Stage-2 orderbook refresh fallback ({symbol}): {exc}")

                if best_ask is not None and best_ask < 0.43:
                    record["status"] = "BLOCK_ADVERSE_CLOB_ODDS"
                    record["shares"] = 0.0
                    record["effective_cost_u"] = 0.0
                    record["error"] = f"Live CLOB VPIN block before sweep: best_ask={best_ask} < 0.43"
                    return True
                if best_ask is not None and round(best_ask, 4) > round(max_taker_px, 4):
                    fallback_maker_px = round(max(0.43, min(max_maker_px, best_ask - 0.01)), 2)
                    fallback_maker_sh = _recalc_affordable_shares(fallback_maker_px, is_taker=False)
                    if fallback_maker_sh <= 0.0:
                        record["status"] = "SKIPPED_INSUFFICIENT_RESCUE_CASH"
                        record["shares"] = 0.0
                        record["effective_cost_u"] = 0.0
                        record["error"] = f"Insufficient rescue cash for skewed Maker @ {fallback_maker_px:.2f}"
                        return True
                    try:
                        fb_resp = await client.place_limit_order(
                            token_id=token_id,
                            price=f"{fallback_maker_px:.2f}",
                            size=f"{fallback_maker_sh:.2f}",
                            side="BUY",
                            post_only=True,
                        )
                        if isinstance(fb_resp, AcceptedOrder) or getattr(fb_resp, "ok", False):
                            is_matched, filled_sh, making_val, oid_str = self._extract_order_match_info(
                                fb_resp, fallback_maker_sh
                            )
                            if is_matched and filled_sh >= 1.0:
                                price = fallback_maker_px
                                shares = filled_sh
                            elif oid_str:
                                return _park_resting_maker_order(oid_str, fallback_maker_px, fallback_maker_sh)
                    except Exception as exc:
                        print(f"[CLOB Warning] Skewed fallback Post-Only Maker failed ({symbol}): {exc}")
                    if not (is_matched and filled_sh >= 1.0):
                        record["status"] = "BLOCK_SKEWED_CLOB_ASK"
                        record["shares"] = 0.0
                        record["effective_cost_u"] = 0.0
                        record["error"] = f"Live CLOB skewed before sweep: ask={best_ask} > {max_taker_px}"
                        return True

                if not (is_matched and filled_sh >= 1.0):
                    sweep_px = round(max(0.43, min(max_taker_px, best_ask if best_ask is not None else max(price, 0.51))), 2)
                    sweep_sh = _recalc_affordable_shares(sweep_px, is_taker=True)
                    if sweep_sh <= 0.0:
                        record["status"] = "SKIPPED_INSUFFICIENT_RESCUE_CASH"
                        record["shares"] = 0.0
                        record["effective_cost_u"] = 0.0
                        record["error"] = f"Insufficient rescue cash for Sweep @ {sweep_px:.2f} (prior=${prior_cost_u:.2f}U)"
                        return True
                    price = sweep_px
                    shares = sweep_sh
                    record["price"] = price
                    record["price_cents"] = round(price * 100.0, 1)
                    record["shares"] = shares
                    record["effective_cost_u"] = round(shares * price, 2)

                    resp = await client.place_limit_order(
                        token_id=token_id,
                        price=f"{sweep_px:.2f}",
                        size=f"{sweep_sh:.2f}",
                        side="BUY",
                        post_only=False,
                    )
                    if isinstance(resp, AcceptedOrder) or getattr(resp, "ok", False):
                        is_matched, filled_sh, making_val, oid_str = self._extract_order_match_info(resp, sweep_sh)
                        if not is_matched and oid_str:
                            is_matched, filled_sh = await self._poll_limit_order_fill(
                                client, oid_str, token_id, sweep_sh, polls=4, interval_sec=1.5
                            )
                        if not is_matched and filled_sh < 1.0:
                            if oid_str:
                                try:
                                    await client.cancel_order(order_id=oid_str)
                                except Exception as exc:
                                    print(f"[CLOB Warning] cancel sweep {oid_str} failed: {exc}")
                            final_c_sh = await self._check_cond_token_shares(client, token_id)
                            if final_c_sh >= 1.0:
                                is_matched = True
                                filled_sh = final_c_sh
                            else:
                                record["status"] = "SKIPPED_NO_OPEN_FILL"
                                record["order_id"] = oid_str
                                record["shares"] = 0.0
                                record["effective_cost_u"] = 0.0
                                record["error"] = f"Sweep BUY {sweep_px:.2f} unfilled after 6.0s timeout"
                                return True
                    else:
                        record["status"] = "REJECTED"
                        record["shares"] = 0.0
                        record["effective_cost_u"] = 0.0
                        record["error"] = str(resp)
                        return True

            # Step D: Record Verified Fill
            filled_sh = round(max(filled_sh, 0.0), 2)
            effective_cost_u = round(making_val, 2) if making_val > 0.0 else round(filled_sh * price, 2)
            record["price"] = price
            record["price_cents"] = round(price * 100.0, 1)
            record["shares"] = filled_sh
            record["effective_cost_u"] = effective_cost_u
            record["status"] = "LIVE_MATCHED"
            record["order_id"] = oid_str
            record["error"] = None
            self.active_clob_positions[symbol] = {
                "symbol": symbol,
                "slug": slug,
                "condition_id": condition_id,
                "token_id": token_id,
                "outcome": outcome_label,
                "direction": direction,
                "entry_price": price,
                "shares": filled_sh,
                "effective_cost_u": effective_cost_u,
                "step": step,
                "entry_order_id": record["order_id"],
                "entry_matched": is_matched,
                "entry_filled_shares": filled_sh,
                "bounce_sell_placed": False,
                "bounce_sell_matched": False,
                "sell_order_id": None,
            }
            return True

        await self._run_with_failover(
            _submit_entry_with_client,
            warm_timeout=18.5,
            cold_timeout=18.5,
            max_failovers=4,
            record=record,
        )

        self.order_history.insert(0, record)
        self.order_history = self.order_history[:100]
        asyncio.create_task(self.sync_live_wallet_balance())
        return record

    async def check_and_sync_position_fill(self, symbol: str, bounce_triggered: bool = False) -> dict[str, Any]:
        """
        Check real on-chain / CLOB fill status of the active position's BUY entry order and SELL bounce order.
        Eliminates any asymmetry between signals and real Polymarket fills!
        """
        pos = self.active_clob_positions.get(symbol)
        if not pos:
            return {"exists": False, "entry_filled_shares": 0.0, "entry_matched": False, "bounce_sell_placed": False, "bounce_sell_matched": False}

        if pos.get("is_dry_run") or self.config.dry_run:
            if bounce_triggered and pos.get("bounce_sell_placed") and not pos.get("bounce_sell_matched"):
                pos["bounce_sell_matched"] = True
                actual_sh = float(pos.get("entry_filled_shares", 5.0))
                cur_step = int(pos.get("step", 1))
                sell_p = round(0.90 if cur_step >= 2 else max(0.85, float(self.config.bounce_protect_limit_price)), 2)
                pos["bounce_sell_price"] = sell_p
                self.order_history.insert(0, {
                    "timestamp_ms": int(time.time() * 1000),
                    "symbol": symbol,
                    "slug": pos.get("slug", ""),
                    "action": "SELL_BOUNCE_PROTECT",
                    "reason": "INTRA_CANDLE_SURGE_MATCHED",
                    "direction": pos.get("direction", ""),
                    "outcome": pos.get("outcome", ""),
                    "token_id": str(pos.get("token_id", "")),
                    "price": sell_p,
                    "price_cents": round(sell_p * 100.0, 1),
                    "shares": actual_sh,
                    "effective_Proceeds_u": round(actual_sh * sell_p, 2),
                    "mode": "DRY_RUN_SIM",
                    "status": "SIM_MATCHED",
                    "order_id": f"sim-sell-{int(time.time() * 1000)}",
                    "error": None,
                })
                self.order_history = self.order_history[:100]
            return {
                "exists": True,
                "entry_filled_shares": float(pos.get("entry_filled_shares", 0.0)),
                "entry_matched": bool(pos.get("entry_matched", True)),
                "bounce_sell_placed": bool(pos.get("bounce_sell_placed", False)),
                "bounce_sell_matched": bool(pos.get("bounce_sell_matched", False)),
                "bounce_sell_price": float(pos.get("bounce_sell_price", 0.90 if int(pos.get("step", 1)) >= 2 else 0.85)),
                "effective_cost_u": round(float(pos.get("entry_filled_shares", 0.0)) * float(pos.get("entry_price", 0.50)), 2),
                "slug": str(pos.get("slug", "")),
            }

        token_id = str(pos.get("token_id", ""))
        entry_oid = pos.get("entry_order_id")
        sell_oid = pos.get("sell_order_id")
        filled_shares = float(pos.get("entry_filled_shares", 0.0))

        async def _sync_with_client(client: AsyncSecureClient) -> None:
            nonlocal filled_shares
            cond_bal_shares = 0.0
            cond_bal_queried_ok = False
            for cond_type in ("CONDITIONAL-V2", "CONDITIONAL"):
                try:
                    b = await client.get_balance_allowance(asset_type=cond_type, token_id=token_id)
                    if b is not None:
                        cond_bal_queried_ok = True
                        if int(b.balance) > 0:
                            cond_bal_shares = max(cond_bal_shares, round(float(b.balance) / 1e6, 2))
                except Exception:
                    pass

            if entry_oid:
                try:
                    ord_info = await client.get_order(order_id=str(entry_oid))
                    sz_matched = float(getattr(ord_info, "size_matched", 0.0) or 0.0)
                    if sz_matched > 0:
                        filled_shares = max(filled_shares, round(sz_matched, 2))
                except Exception:
                    pass

            if cond_bal_shares > 0:
                filled_shares = max(filled_shares, cond_bal_shares)

            pos["entry_filled_shares"] = filled_shares
            pos["entry_matched"] = filled_shares >= 1.0
            if filled_shares >= 1.0:
                pos["entry_verified_matched"] = True
                pos["entry_status"] = "LIVE_MATCHED"
                pos["shares"] = filled_shares
                pos["effective_cost_u"] = round(filled_shares * float(pos.get("entry_price", 0.50)), 2)

            if sell_oid:
                try:
                    s_info = await client.get_order(order_id=str(sell_oid))
                    s_matched = float(getattr(s_info, "size_matched", 0.0) or 0.0)
                    s_status = str(getattr(s_info, "status", "")).upper()
                    if s_matched >= 1.0 or "MATCHED" in s_status:
                        pos["bounce_sell_matched"] = True
                except Exception:
                    # In V2 SDK, get_order raises UnexpectedResponseError when the SELL order is 100% matched and closed.
                    # If we previously held >= 1.0 shares and our on-chain conditional token balance is now < 1.0, the shares were sold!
                    if cond_bal_queried_ok and cond_bal_shares < 1.0 and filled_shares >= 1.0:
                        pos["bounce_sell_matched"] = True

        await self._run_with_failover(
            _sync_with_client,
            warm_timeout=6.0,
            cold_timeout=6.5,
            max_failovers=3,
        )

        return {
            "exists": True,
            "entry_filled_shares": float(pos.get("entry_filled_shares", 0.0)),
            "entry_matched": bool(pos.get("entry_matched", False)),
            "bounce_sell_placed": bool(pos.get("bounce_sell_placed", False)),
            "bounce_sell_matched": bool(pos.get("bounce_sell_matched", False)),
            "effective_cost_u": round(float(pos.get("entry_filled_shares", 0.0)) * float(pos.get("entry_price", 0.50)), 2),
            "slug": str(pos.get("slug", "")),
        }

    async def query_live_market_resolution(self, slug: str, direction: str, max_attempts: int = 8) -> dict[str, Any]:
        """
        Queries Polymarket Gamma API `/events?slug={slug}` to read the authoritative Chainlink 60s TWAP outcome
        (`outcomePrices`, `eventMetadata.priceToBeat`, & `umaResolutionStatus`) instead of relying on Binance 1-second spot close ticks!
        """
        if not slug:
            return {"resolved": False, "polymarket_won": None, "our_outcome_price": 0.5, "resolution_source": "chainlink-twap-60s"}

        is_up = direction in ("LONG_BOUNCE", "UP")
        proxy = None if self._is_direct_mode() else (self.config.proxy_url.strip() or None)
        cand_proxies = [None, proxy] if proxy else [None]

        attempts = max(1, int(max_attempts))
        for attempt in range(attempts):
            for cand_proxy in cand_proxies:
                try:
                    async with httpx.AsyncClient(
                        proxy=cand_proxy,
                        trust_env=(cand_proxy is not None),
                        timeout=5.0,
                    ) as hc:
                        resp = await hc.get("https://gamma-api.polymarket.com/events", params={"slug": slug})
                        if resp.status_code == 200:
                            arr = resp.json()
                            if arr:
                                ev_obj = arr[0]
                                ev_meta = ev_obj.get("eventMetadata") or {}
                                if isinstance(ev_meta, str):
                                    try:
                                        ev_meta = json.loads(ev_meta)
                                    except Exception:
                                        ev_meta = {}
                                price_to_beat = ev_meta.get("priceToBeat") if isinstance(ev_meta, dict) else None
                                final_price = ev_meta.get("finalPrice") if isinstance(ev_meta, dict) else None
                                m = ev_obj.get("markets", [{}])[0]
                                raw_prices = m.get("outcomePrices")
                                prices = json.loads(raw_prices) if isinstance(raw_prices, str) else (raw_prices or ["0.5", "0.5"])
                                up_p = float(prices[0]) if len(prices) >= 1 else 0.5
                                down_p = float(prices[1]) if len(prices) >= 2 else 0.5
                                our_p = up_p if is_up else down_p
                                res_src = str(m.get("resolutionSource") or "https://data.chain.link/streams/btc-usd-twap-60s-streams")
                                uma_st = str(m.get("umaResolutionStatus") or "")
                                is_finalized = (
                                    uma_st.lower() == "resolved"
                                    or final_price is not None
                                    or up_p >= 0.95
                                    or up_p <= 0.05
                                )

                                if final_price is not None and price_to_beat is not None:
                                    ptb_f = float(price_to_beat)
                                    fin_f = float(final_price)
                                    won_chainlink = (fin_f >= ptb_f) if is_up else (fin_f < ptb_f)
                                    return {
                                        "resolved": True,
                                        "polymarket_won": won_chainlink,
                                        "up_price": 1.0 if (fin_f >= ptb_f) else 0.0,
                                        "down_price": 0.0 if (fin_f >= ptb_f) else 1.0,
                                        "our_outcome_price": 1.0 if won_chainlink else 0.0,
                                        "price_to_beat": ptb_f,
                                        "final_price": fin_f,
                                        "resolution_source": res_src,
                                        "uma_status": uma_st or "resolved",
                                    }

                                if is_finalized and our_p >= 0.80:
                                    return {
                                        "resolved": True,
                                        "polymarket_won": True,
                                        "up_price": up_p,
                                        "down_price": down_p,
                                        "our_outcome_price": our_p,
                                        "price_to_beat": float(price_to_beat) if price_to_beat is not None else None,
                                        "resolution_source": res_src,
                                        "uma_status": uma_st,
                                    }
                                if is_finalized and our_p <= 0.20:
                                    return {
                                        "resolved": True,
                                        "polymarket_won": False,
                                        "up_price": up_p,
                                        "down_price": down_p,
                                        "our_outcome_price": our_p,
                                        "price_to_beat": float(price_to_beat) if price_to_beat is not None else None,
                                        "resolution_source": res_src,
                                        "uma_status": uma_st,
                                    }
                except Exception:
                    pass
            if attempt < attempts - 1:
                await asyncio.sleep(2.0)

        return {"resolved": False, "polymarket_won": None, "our_outcome_price": 0.5, "resolution_source": "chainlink-twap-60s"}

    async def cancel_active_orders_for_symbol(self, symbol: str) -> None:
        pos = self.active_clob_positions.get(symbol)
        if not pos or pos.get("is_dry_run"):
            return

        async def _cancel_with_client(client: AsyncSecureClient) -> None:
            for k in ("entry_order_id", "sell_order_id"):
                oid = pos.get(k)
                if oid:
                    try:
                        await client.cancel_order(order_id=str(oid))
                    except Exception:
                        pass

        await self._run_with_failover(
            _cancel_with_client,
            warm_timeout=4.5,
            cold_timeout=4.5,
            max_failovers=2,
        )

    async def execute_bounce_protect_sell(
        self,
        symbol: str,
        direction: str,
        bounce_triggered: bool = False,
        prior_cost_u: float = 0.0,
    ) -> dict[str, Any] | None:
        """
        Places a Stage-3 Limit SELL order at `bounce_protect_limit_price` (default $0.82 / $0.85+)
        ONLY if the entry BUY order actually filled on Polymarket (`entry_filled_shares >= 1.0`).
        Dynamically enforces a minimum net-profit floor (`min_prof_price`) so a bounce-protect sell
        can NEVER execute at a price that produces a net sequence loss.
        """
        if not self.config.auto_take_profit_on_bounce:
            return None

        pos = self.active_clob_positions.get(symbol)
        if not pos or pos.get("bounce_sell_placed"):
            return None

        fill_state = await self.check_and_sync_position_fill(symbol)
        actual_shares = float(fill_state.get("entry_filled_shares", 0.0))

        if actual_shares < 1.0:
            if not bounce_triggered or int(pos.get("step", 1)) >= 2:
                # Entry BUY order is still resting on the CLOB book waiting to fill (or is Step 2 recovery) -> keep it alive!
                return None
            # Step 1 price already bounced +0.10% without filling our entry BUY order -> cancel unfilled buy order to avoid late adverse fill
            await self.cancel_active_orders_for_symbol(symbol)
            pos["bounce_sell_placed"] = False
            pos["aborted_unfilled"] = True
            return {
                "status": "SKIPPED_ENTRY_UNFILLED",
                "symbol": symbol,
                "shares": 0.0,
                "order_id": None,
                "error": None,
            }

        token_id = str(pos["token_id"])
        cur_step = int(pos.get("step", 1))
        entry_p = float(pos.get("entry_price", 0.52) or 0.52)
        tot_seq_cost_u = float(prior_cost_u) + actual_shares * entry_p
        min_prof_price = round((tot_seq_cost_u + 0.45) / max(actual_shares, 1.0), 2)
        if min_prof_price > 0.97:
            # Hold to 100¢ expiration rather than selling below cumulative sequence cost
            return None
        base_target = 0.85 if cur_step >= 2 else 0.82
        sell_price = round(min(0.97, max(base_target, min_prof_price)), 2)
        pos["bounce_sell_price"] = sell_price

        record: dict[str, Any] = {
            "timestamp_ms": int(time.time() * 1000),
            "symbol": symbol,
            "slug": pos.get("slug", ""),
            "action": "SELL_BOUNCE_PROTECT",
            "reason": "INTRA_CANDLE_SURGE_LOCK",
            "direction": direction,
            "outcome": pos.get("outcome", ""),
            "token_id": token_id,
            "price": sell_price,
            "price_cents": round(sell_price * 100.0, 1),
            "shares": actual_shares,
            "effective_Proceeds_u": round(actual_shares * sell_price, 2),
            "mode": "DRY_RUN" if (self.config.dry_run or not self.config.enabled) else "LIVE_CLOB_V2",
            "status": "PENDING",
            "order_id": None,
            "error": None,
        }

        if self.config.dry_run:
            pos["bounce_sell_placed"] = True
            if bounce_triggered:
                pos["bounce_sell_matched"] = True
                record["status"] = "SIM_MATCHED"
                record["order_id"] = f"sim-sell-{int(time.time() * 1000)}"
                record["mode"] = "DRY_RUN_SIM"
                self.order_history.insert(0, record)
                self.order_history = self.order_history[:100]
                return record
            record["status"] = "SIM_RESTING"
            return record

        if not self.config.enabled:
            record["status"] = "PAUSED"
            return record

        async def _submit_sell_with_client(client: AsyncSecureClient) -> bool:
            nonlocal sell_price
            if pos.get("entry_order_id"):
                try:
                    await client.cancel_order(order_id=str(pos["entry_order_id"]))
                except Exception:
                    pass

            resp = await client.place_limit_order(
                token_id=token_id,
                price=f"{sell_price:.2f}",
                size=f"{actual_shares:.2f}",
                side="SELL",
            )
            if isinstance(resp, AcceptedOrder) or getattr(resp, "ok", False):
                st_upper = str(getattr(resp, "status", "POSTED")).upper()
                oid_s = str(getattr(resp, "order_id", ""))
                is_s_matched = "MATCHED" in st_upper

                # On Step 1 active bounce, verify fill after 1.2s; if resting, step down to max(0.76, min_prof_price) to cross spread as Taker!
                if not is_s_matched and bounce_triggered and oid_s and cur_step == 1:
                    await asyncio.sleep(1.2)
                    try:
                        s_info = await client.get_order(order_id=oid_s)
                        sz_s = float(getattr(s_info, "size_matched", 0.0) or 0.0)
                        st_s = str(getattr(s_info, "status", "")).upper()
                        if sz_s >= 1.0 or "MATCHED" in st_s:
                            is_s_matched = True
                            st_upper = "MATCHED"
                    except Exception:
                        try:
                            b_c = await client.get_balance_allowance(asset_type="CONDITIONAL-V2", token_id=token_id)
                            if b_c is not None and int(b_c.balance) == 0:
                                is_s_matched = True
                                st_upper = "MATCHED"
                        except Exception:
                            pass

                    if not is_s_matched:
                        chase_sell = round(max(0.76, min_prof_price), 2)
                        if chase_sell < sell_price:
                            try:
                                await client.cancel_order(order_id=oid_s)
                            except Exception:
                                pass
                            c_resp = await client.place_limit_order(
                                token_id=token_id,
                                price=f"{chase_sell:.2f}",
                                size=f"{actual_shares:.2f}",
                                side="SELL",
                            )
                            if isinstance(c_resp, AcceptedOrder) or getattr(c_resp, "ok", False):
                                sell_price = chase_sell
                                pos["bounce_sell_price"] = chase_sell
                                record["price"] = chase_sell
                                record["price_cents"] = round(chase_sell * 100.0, 1)
                                record["effective_Proceeds_u"] = round(actual_shares * chase_sell, 2)
                                oid_s = str(getattr(c_resp, "order_id", ""))
                                st_upper = str(getattr(c_resp, "status", "POSTED")).upper()
                                if "MATCHED" in st_upper:
                                    is_s_matched = True

                record["status"] = "LIVE_MATCHED" if is_s_matched else f"LIVE_{st_upper}"
                record["order_id"] = oid_s
                record["error"] = None
                pos["bounce_sell_placed"] = True
                pos["sell_order_id"] = oid_s
                if is_s_matched:
                    pos["bounce_sell_matched"] = True
                return True
            else:
                record["status"] = "REJECTED"
                record["error"] = str(resp)
                return True

        await self._run_with_failover(
            _submit_sell_with_client,
            warm_timeout=6.0,
            cold_timeout=6.5,
            max_failovers=4,
            record=record,
        )

        self.order_history.insert(0, record)
        self.order_history = self.order_history[:100]
        return record

    async def get_live_token_best_bid_ask(self, token_id: str) -> tuple[float | None, float | None]:
        """Fetch top-of-book best bid & best ask via pre-warmed HTTP/2 CLOB client (~60ms)."""
        if not token_id or token_id == "UNKNOWN_TOKEN_ID":
            return None, None
        client = await self.ensure_warm_v2_client()
        if client is None:
            return None, None
        try:
            ob = await asyncio.wait_for(client.get_order_book(token_id=token_id), timeout=3.0)
            bids = sorted([float(b.price) for b in (ob.bids or [])], reverse=True)
            asks = sorted([float(a.price) for a in (ob.asks or [])])
            return (bids[0] if bids else None), (asks[0] if asks else None)
        except Exception:
            return None, None

    async def execute_late_surge_sell(
        self,
        symbol: str,
        min_bid: float = 0.93,
        limit_sell_price: float = 0.95,
        prior_cost_u: float = 0.0,
    ) -> dict[str, Any] | None:
        """
        Argon V8.0 Tail-End Pin Risk Guard:
        Only in the final <60s of the 5m contract, if live Best Bid >= 93c and entry shares are verified filled,
        place a 0%-fee Post-Only Maker SELL @ 95c while strictly enforcing Martingale net-profit recovery.
        """
        pos = self.active_clob_positions.get(symbol)
        if not pos or pos.get("bounce_sell_placed") or pos.get("bounce_sell_matched"):
            return None

        fill_state = await self.check_and_sync_position_fill(symbol)
        shares = float(fill_state.get("entry_filled_shares") or 0.0)
        if shares < 1.0:
            return {
                "status": "SKIPPED_ENTRY_UNFILLED",
                "shares": 0.0,
                "symbol": symbol,
                "error": "Entry order unfilled; skipping late surge sell",
            }

        token_id = str(pos.get("token_id", ""))
        best_bid, _ = await self.get_live_token_best_bid_ask(token_id)
        if best_bid is None or best_bid < min_bid:
            return None

        sell_px = round(min(0.99, max(limit_sell_price, best_bid + 0.01)), 2)
        entry_cost_u = float(pos.get("effective_cost_u") or round(shares * float(pos.get("entry_price", 0.50)), 2))
        tot_seq_cost = float(prior_cost_u) + entry_cost_u
        est_taker_fee = 0.07 * limit_sell_price * (1.0 - limit_sell_price)
        net_proceeds = shares * max(0.01, limit_sell_price - est_taker_fee)
        if net_proceeds < tot_seq_cost + 0.20:
            return {
                "status": "SKIPPED_BELOW_MARTINGALE_PROFIT_FLOOR",
                "shares": shares,
                "symbol": symbol,
                "error": f"Surge net proceeds ${net_proceeds:.2f}U < seq cost ${tot_seq_cost:.2f}U + $0.20U",
            }

        record = {
            "timestamp_ms": int(time.time() * 1000),
            "symbol": symbol,
            "slug": pos.get("slug", ""),
            "action": "SELL_LATE_SURGE_95C",
            "reason": f"FINAL_60S_SURGE_BID_{round(best_bid*100,1)}C",
            "direction": pos.get("direction", ""),
            "outcome": pos.get("outcome", ""),
            "token_id": token_id,
            "price": sell_px,
            "price_cents": round(sell_px * 100.0, 1),
            "shares": shares,
            "effective_Proceeds_u": round(shares * sell_px, 2),
            "mode": "DRY_RUN_SIM" if self.config.dry_run else "LIVE_CLOB_V2",
            "status": "PENDING",
            "order_id": "",
            "error": None,
        }
        if self.config.dry_run:
            pos["bounce_sell_placed"] = True
            pos["bounce_sell_matched"] = True
            pos["bounce_sell_price"] = sell_px
            record["status"] = "SIM_MATCHED"
            record["order_id"] = f"sim-surge-{int(time.time() * 1000)}"
            self.order_history.insert(0, record)
            self.order_history = self.order_history[:100]
            return record

        if not self.config.enabled:
            return None

        client = await self.ensure_warm_v2_client()
        if client is None:
            return None
        try:
            resp = await asyncio.wait_for(
                client.place_limit_order(
                    token_id=token_id,
                    price=f"{sell_px:.2f}",
                    size=f"{shares:.2f}",
                    side="SELL",
                    post_only=True,
                ),
                timeout=6.0,
            )
            if isinstance(resp, AcceptedOrder) or getattr(resp, "ok", False):
                pos["bounce_sell_placed"] = True
                pos["bounce_sell_price"] = sell_px
                pos["sell_order_id"] = str(getattr(resp, "order_id", ""))
                record["order_id"] = pos["sell_order_id"]
                record["status"] = "LIVE_POST_ONLY"
            else:
                record["status"] = "REJECTED"
                record["error"] = str(resp)
        except Exception as e:
            record["status"] = "ERROR"
            record["error"] = str(e)
        self.order_history.insert(0, record)
        self.order_history = self.order_history[:100]
        return record

    # ------------------------------------------------------------------
    # Chainlink late-window sniper execution (independent of legacy `dry_run`)
    # ------------------------------------------------------------------
    def _sniper_ready(self) -> str | None:
        """Why live sniper orders are refused right now (None when allowed)."""
        if not self.config.sniper_live:
            return "config.sniper_live is false"
        if not self.config.private_key:
            return "no private key"
        geo = self.last_geoblock_status or {}
        if not geo.get("can_trade_api", False):
            return "geoblock status missing or blocked"
        return None

    async def _strict_conditional_shares(self, client: AsyncSecureClient, token_id: str) -> float:
        """Floor-to-cent conditional share balance. RAISES when no balance query succeeds (never a silent 0.0)."""
        best, answered, last_exc = 0.0, False, None
        for c_type in ("CONDITIONAL-V2", "CONDITIONAL"):
            try:
                b = await client.get_balance_allowance(asset_type=c_type, token_id=token_id)
            except Exception as exc:  # noqa: BLE001 - remember and try the other asset type
                last_exc = exc
                continue
            answered = True
            if b is not None:
                best = max(best, math.floor(float(b.balance) / 1e6 * 100.0 + 1e-9) / 100.0)
        if not answered:
            raise RuntimeError(f"conditional balance unavailable: {last_exc}")
        return best

    @staticmethod
    def _amount_u(raw: Any) -> float:
        """Normalise SDK amounts that may arrive as human decimals or 6-decimal base units."""
        val = float(raw or 0.0)
        return round(val / 1e6, 6) if val > 10000 else val

    @staticmethod
    def _order_error_text(resp: Any) -> str:
        return str(getattr(resp, "message", None) or getattr(resp, "code", None) or resp)

    @staticmethod
    def _empty_fill(status: str, error: str | None = None) -> dict[str, Any]:
        return {
            "filled": False, "shares": 0.0, "usd": 0.0, "avg_price": 0.0,
            "order_id": "", "status": status, "error": error,
        }

    async def sniper_buy(self, token_id: str, shares: float, ref_ask: float, limit_price: float) -> dict[str, Any]:
        """Marketable FAK BUY capped at `limit_price` (spend = shares * ref_ask).

        Never retried blindly: if the response is lost the conditional-token balance decides whether the order
        filled, so one signal can never buy twice.
        """
        refusal = self._sniper_ready()
        if refusal:
            return self._empty_fill("DISABLED", refusal)
        client = await self.ensure_warm_v2_client()
        if client is None:
            return self._empty_fill("NO_CLIENT", "warm V2 client unavailable")
        # Spend up to shares * limit_price (the hard cap) so a fill near the cap still yields >= `shares`.
        amount = round(float(shares) * float(limit_price), 2)
        if amount < 1.0:
            return self._empty_fill("TOO_SMALL", f"order value ${amount:.2f} < $1.00")
        try:
            resp = await asyncio.wait_for(
                client.place_market_order(
                    token_id=token_id, side="BUY", amount=amount, max_price=float(limit_price), order_type="FAK"
                ),
                timeout=8.0,
            )
        except DEFINITE_REJECTIONS as exc:  # nothing was accepted: normal "no liquidity under the cap" outcome
            return self._empty_fill("REJECTED", f"{type(exc).__name__}: {exc}")
        except Exception as exc:  # noqa: BLE001 - fill state unknown: resolve by balance, never by blind retry
            return await self._sniper_resolve_lost_buy(client, token_id, limit_price, exc)
        return await self._sniper_parse_buy(client, token_id, resp, limit_price)

    async def _sniper_resolve_lost_buy(
        self, client: AsyncSecureClient, token_id: str, limit_price: float, exc: Exception
    ) -> dict[str, Any]:
        await asyncio.sleep(0.6)
        held, verified = await self._sniper_verify_balance(client, token_id)
        if held >= 1.0:
            return {
                "filled": True, "shares": held, "usd": round(held * limit_price, 4), "avg_price": limit_price,
                "order_id": "", "status": "RECOVERED_FROM_BALANCE", "error": str(exc),
            }
        status = "ERROR" if verified else "UNKNOWN"
        return self._empty_fill(status, f"{type(exc).__name__}: {exc}")

    async def _sniper_verify_balance(self, client: AsyncSecureClient, token_id: str) -> tuple[float, bool]:
        """Read the conditional balance with the current client, else with a freshly rebuilt one.

        Returns (shares_held, verified). `verified=False` means the fill state could not be established.
        """
        try:
            return await asyncio.wait_for(self._strict_conditional_shares(client, token_id), timeout=5.0), True
        except Exception:  # noqa: BLE001 - transport is suspect: rebuild the session and retry once
            await self._invalidate_warm_client()
        fresh = await self.ensure_warm_v2_client()
        if fresh is None:
            return 0.0, False
        try:
            return await asyncio.wait_for(self._strict_conditional_shares(fresh, token_id), timeout=5.0), True
        except Exception:  # noqa: BLE001 - still unknown
            return 0.0, False

    async def _sniper_parse_buy(
        self, client: AsyncSecureClient, token_id: str, resp: Any, limit_price: float
    ) -> dict[str, Any]:
        if not getattr(resp, "ok", False):
            return self._empty_fill("REJECTED", self._order_error_text(resp))
        order_id = str(getattr(resp, "order_id", "") or "")
        got = self._amount_u(getattr(resp, "taking_amount", 0.0))
        spent = self._amount_u(getattr(resp, "making_amount", 0.0))
        if 0.0 < got < 1.0:
            return {**self._empty_fill("DUST_FILL", f"partial fill of only {got:.4f} shares"), "shares": got, "usd": spent}
        if got >= 1.0:
            avg = round(spent / got, 4) if spent > 0 else limit_price
            return {
                "filled": True, "shares": round(got, 4), "usd": round(spent, 4), "avg_price": avg,
                "order_id": order_id, "status": "FILLED", "error": None,
            }
        if order_id and str(getattr(resp, "status", "")).lower() == "live":
            try:
                await client.cancel_order(order_id=order_id)
            except Exception as exc:  # noqa: BLE001 - best-effort cancel of a stray resting FAK
                print(f"[Sniper Warning] cancel stray BUY {order_id} failed: {exc}")
        return self._empty_fill("UNMATCHED", "FAK matched no shares")

    async def sniper_sell(self, token_id: str, shares: float, min_price: float) -> dict[str, Any]:
        """Marketable FAK SELL of up to `shares` with a hard price floor (partial fills allowed)."""
        refusal = self._sniper_ready()
        if refusal:
            return self._empty_fill("DISABLED", refusal)
        client = await self.ensure_warm_v2_client()
        if client is None:
            return self._empty_fill("NO_CLIENT", "warm V2 client unavailable")
        try:
            held = await asyncio.wait_for(self._strict_conditional_shares(client, token_id), timeout=5.0)
        except Exception as exc:  # noqa: BLE001 - balance unknown: never sell by ledger quantity
            return self._empty_fill("ERROR", f"balance unavailable: {exc}")
        qty = math.floor((min(float(shares), held) + 1e-9) * 100.0) / 100.0
        if qty < 0.1:
            return self._empty_fill("NO_BALANCE", f"verified conditional balance {held} shares")
        try:
            resp = await asyncio.wait_for(
                client.place_market_order(
                    token_id=token_id, side="SELL", shares=qty, min_price=float(min_price), order_type="FAK"
                ),
                timeout=8.0,
            )
        except DEFINITE_REJECTIONS as exc:  # nothing sold: keep the position, caller retries or holds to expiry
            return self._empty_fill("REJECTED", f"{type(exc).__name__}: {exc}")
        except Exception as exc:  # noqa: BLE001 - transport trouble: rebuild the session, caller re-checks balance
            await self._invalidate_warm_client()
            return self._empty_fill("ERROR", f"{type(exc).__name__}: {exc}")
        return self._sniper_parse_sell(resp, qty)

    def _sniper_parse_sell(self, resp: Any, qty: float) -> dict[str, Any]:
        if not getattr(resp, "ok", False):
            return self._empty_fill("REJECTED", self._order_error_text(resp))
        making = self._amount_u(getattr(resp, "making_amount", 0.0))
        taking = self._amount_u(getattr(resp, "taking_amount", 0.0))
        # A share trades below $1, so the share count is always the larger of the two amounts.
        shares_sold, proceeds = max(making, taking), min(making, taking)
        if shares_sold < 0.1:
            return self._empty_fill("UNMATCHED", "FAK matched no shares")
        return {
            "filled": True, "shares": round(shares_sold, 4), "usd": round(proceeds, 4),
            "avg_price": round(proceeds / shares_sold, 4) if proceeds > 0 else 0.0,
            "order_id": str(getattr(resp, "order_id", "") or ""), "status": "FILLED", "error": None,
        }

    async def sniper_token_balance(self, token_id: str) -> float:
        """Verified conditional-token balance (floored shares). RAISES when it cannot be established."""
        client = await self.ensure_warm_v2_client()
        if client is None:
            raise RuntimeError("warm V2 client unavailable")
        return await asyncio.wait_for(self._strict_conditional_shares(client, token_id), timeout=6.0)
