from __future__ import annotations

import hashlib
import hmac
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
import aiohttp


BINANCE_FAPI_BASE = "https://fapi.binance.com"
BINANCE_FAPI_TESTNET = "https://testnet.binancefuture.com"

# Minimum notional order size (USDT) and quantity step size on Binance USD-M Futures
FUTURES_SYMBOL_RULES: dict[str, dict[str, float]] = {
    "SOLUSDT": {"min_notional": 5.0, "qty_step": 0.1, "price_tick": 0.01},
    "ETHUSDT": {"min_notional": 20.0, "qty_step": 0.001, "price_tick": 0.01},
    "BTCUSDT": {"min_notional": 100.0, "qty_step": 0.001, "price_tick": 0.1},
}


@dataclass
class LiveExecutorConfig:
    enabled: bool = False
    testnet: bool = True
    api_key: str = ""
    api_secret: str = ""
    leverage: int = 20
    take_profit_pct: float = 0.35  # Must exceed 0.07% round-trip fee to have positive expectancy on Futures
    stop_loss_pct: float = 0.45


class BinanceFuturesExecutor:
    """
    Production-ready async Binance USD-M Futures order executor.
    Handles HMAC-SHA256 signing, leverage setup, MIN_NOTIONAL routing for 10u accounts,
    and automatic TAKE_PROFIT_MARKET / STOP_MARKET bracket protection.
    """

    def __init__(self, cfg: LiveExecutorConfig | None = None) -> None:
        self.cfg = cfg or LiveExecutorConfig()

    @property
    def base_url(self) -> str:
        return BINANCE_FAPI_TESTNET if self.cfg.testnet else BINANCE_FAPI_BASE

    def _sign(self, params: dict[str, Any]) -> str:
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = 5000
        query = urlencode(params)
        signature = hmac.new(
            self.cfg.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return f"{query}&signature={signature}"

    def _headers(self) -> dict[str, str]:
        return {"X-MBX-APIKEY": self.cfg.api_key}

    async def get_usdt_balance(self) -> float:
        if not (self.cfg.api_key and self.cfg.api_secret):
            return 0.0
        qs = self._sign({})
        url = f"{self.base_url}/fapi/v2/balance?{qs}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=self._headers(), timeout=aiohttp.ClientTimeout(total=8)) as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"Binance API error: {data}")
                for asset in data:
                    if asset.get("asset") == "USDT":
                        return float(asset.get("availableBalance", 0.0))
        return 0.0

    async def set_leverage(self, session: aiohttp.ClientSession, symbol: str, leverage: int) -> dict[str, Any]:
        qs = self._sign({"symbol": symbol, "leverage": leverage})
        url = f"{self.base_url}/fapi/v1/leverage?{qs}"
        async with session.post(url, headers=self._headers()) as resp:
            return await resp.json()

    def round_step(self, value: float, step: float) -> float:
        inv = round(1.0 / step)
        return int(value * inv) / inv

    async def execute_bracket_order(
        self,
        symbol: str,
        direction: str,
        margin_usdt: float,
        mark_price: float,
    ) -> dict[str, Any]:
        """
        Place a leveraged entry order + TAKE_PROFIT_MARKET + STOP_MARKET.
        For a 10u account where margin_usdt might be $1~$3, automatically checks MIN_NOTIONAL
        ($5 for SOLUSDT, $20 for ETHUSDT, $100 for BTCUSDT).
        """
        if not self.cfg.enabled or not self.cfg.api_key or not self.cfg.api_secret:
            return {"status": "skipped", "reason": "Live executor disabled or missing API keys"}

        rules = FUTURES_SYMBOL_RULES.get(symbol, FUTURES_SYMBOL_RULES["SOLUSDT"])
        notional = margin_usdt * self.cfg.leverage

        if notional < rules["min_notional"]:
            return {
                "status": "rejected_min_notional",
                "symbol": symbol,
                "notional_usdt": round(notional, 2),
                "required_min_notional": rules["min_notional"],
                "suggestion": "10U 小資金階段建議只開 SOLUSDT (最低名目價值 $5U)，或提高槓桿至 25x~50x。",
            }

        raw_qty = notional / mark_price
        qty = self.round_step(raw_qty, rules["qty_step"])
        if qty <= 0:
            return {"status": "error", "reason": f"Calculated quantity {qty} is zero"}

        is_long = direction == "LONG_BOUNCE"
        entry_side = "BUY" if is_long else "SELL"
        exit_side = "SELL" if is_long else "BUY"

        tp_mult = (1.0 + self.cfg.take_profit_pct / 100.0) if is_long else (1.0 - self.cfg.take_profit_pct / 100.0)
        sl_mult = (1.0 - self.cfg.stop_loss_pct / 100.0) if is_long else (1.0 + self.cfg.stop_loss_pct / 100.0)

        tp_price = round(self.round_step(mark_price * tp_mult, rules["price_tick"]), 4)
        sl_price = round(self.round_step(mark_price * sl_mult, rules["price_tick"]), 4)

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            await self.set_leverage(session, symbol, self.cfg.leverage)

            # 1. Market Entry
            entry_qs = self._sign({
                "symbol": symbol,
                "side": entry_side,
                "type": "MARKET",
                "quantity": qty,
            })
            async with session.post(f"{self.base_url}/fapi/v1/order?{entry_qs}", headers=self._headers()) as resp:
                entry_res = await resp.json()
                if resp.status != 200:
                    return {"status": "error", "step": "entry", "response": entry_res}

            # 2. Take Profit Market Order (closePosition=true)
            tp_qs = self._sign({
                "symbol": symbol,
                "side": exit_side,
                "type": "TAKE_PROFIT_MARKET",
                "stopPrice": tp_price,
                "closePosition": "true",
                "workingType": "MARK_PRICE",
            })
            async with session.post(f"{self.base_url}/fapi/v1/order?{tp_qs}", headers=self._headers()) as resp:
                tp_res = await resp.json()

            # 3. Stop Loss Market Order (closePosition=true)
            sl_qs = self._sign({
                "symbol": symbol,
                "side": exit_side,
                "type": "STOP_MARKET",
                "stopPrice": sl_price,
                "closePosition": "true",
                "workingType": "MARK_PRICE",
            })
            async with session.post(f"{self.base_url}/fapi/v1/order?{sl_qs}", headers=self._headers()) as resp:
                sl_res = await resp.json()

            return {
                "status": "filled",
                "symbol": symbol,
                "direction": direction,
                "qty": qty,
                "notional_usdt": round(qty * mark_price, 2),
                "tp_price": tp_price,
                "sl_price": sl_price,
                "entry_order": entry_res,
                "tp_order": tp_res,
                "sl_order": sl_res,
            }
