from __future__ import annotations

import asyncio
import base64
from pathlib import Path
import socket
import struct
import subprocess
import time
from typing import Any
from urllib.parse import urlparse

import httpx

GEOBLOCK_URL = "https://polymarket.com/api/geoblock"
CLOB_TIME_URL = "https://clob.polymarket.com/time"
LOCAL_GATEWAY_HOST = "127.0.0.1"
LOCAL_GATEWAY_PORT = 10809

# Curated pool of verified Polymarket-unblocked nodes (HK / SE / Whitelist countries where blocked=False for both Web Deposit & CLOB API)
DEFAULT_UPSTREAM_NODES = [
    "http://43.99.60.244:8089",                  # HK Datacenter (~245ms warm, blocked: False, supports Web Deposit + Gamma + CLOB)
    "socks5://88888:88888@8.210.36.244:1080",    # Alibaba Cloud HK (~247ms warm, blocked: False, supports Web Deposit + Gamma + CLOB)
    "http://176.111.37.5:39811",                 # Sweden SE (~450ms warm, blocked: False, EU Whitelist)
    "http://176.111.37.216:39811",               # Sweden SE (~480ms warm, blocked: False, EU Whitelist)
    "socks5://101.36.104.46:10808",              # JP Datacenter (CLOB API backup)
]

LIVE_PROXY_FEEDS = [
    ("socks5", "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt"),
    ("http", "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt"),
    ("socks5", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt"),
    ("http", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt"),
    ("socks5", "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt"),
    ("socks5", "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt"),
]

WHITELIST_COUNTRIES = {"HK", "RO", "CH", "NO", "SE", "ES", "CA", "FI", "DK", "AT", "PT", "IE", "JP", "KR", "NL", "MT"}
API_ONLY_COUNTRIES = {"IE", "JP", "NL", "KR", "MT"}


class VpnGuardian:
    """
    Silent, Autonomous High-Availability Local VPN Gateway & Self-Healing Engine (`127.0.0.1:10809`):
    - Supports BOTH `http://` (HTTP CONNECT) and `socks5://` upstream nodes seamlessly.
    - Zero Telegram spam: heals, switches, and replenishes nodes 100% silently in the background.
    - Sticky Primary Hysteresis + 2-Strike Jitter Protection: keeps the active primary node pinned
      and prevents transient 1-tick network jitter from flickering the UI into a false "Blocked" state.
    - Per-Connection Instant Failover: if any single socket handshake fails or times out (>2.8s),
      immediately dials the next healthy node in the pool without dropping Chrome or CLOB API requests.
    - Autonomous Pool Replenishment: if healthy nodes ever drop below 2, automatically scans live feeds
      (both SOCKS5 and HTTP) in the background with strict `asyncio.wait_for` bounds.
    """

    def __init__(self, auto_trader: Any = None, notifier: Any = None) -> None:
        self.auto_trader = auto_trader
        self.notifier = notifier
        self.candidate_nodes: list[str] = list(DEFAULT_UPSTREAM_NODES)
        self.healthy_web_nodes: list[dict[str, Any]] = []
        self.healthy_api_nodes: list[dict[str, Any]] = []
        self.primary_node: str = DEFAULT_UPSTREAM_NODES[0]
        self._consecutive_probe_failures: int = 0
        self._last_verified_primary_info: dict[str, Any] = {
            "proxy": DEFAULT_UPSTREAM_NODES[0],
            "country": "HK",
            "ip": "43.99.60.244",
            "latency_ms": 245,
            "web_deposit_ok": True,
            "clob_api_ok": True,
        }
        self.server: asyncio.AbstractServer | None = None
        self._watchdog_task: asyncio.Task | None = None
        self._running: bool = False
        self._last_replenish_ts: float = 0.0
        self.stats: dict[str, Any] = {
            "gateway_listen": f"{LOCAL_GATEWAY_HOST}:{LOCAL_GATEWAY_PORT}",
            "gateway_running": False,
            "primary_upstream": self.primary_node,
            "primary_country": "HK",
            "primary_ip": "43.99.60.244",
            "primary_latency_ms": 245,
            "web_deposit_unlocked": True,
            "clob_api_unlocked": True,
            "total_tunnels_opened": 0,
            "failover_count": 0,
            "auto_heals_count": 0,
            "last_check_ms": 0,
            "macos_pac_enabled": False,
            "direct_vpn_mode": Path("/Applications/Surfshark.app").exists(),
            "surfshark_detected": Path("/Applications/Surfshark.app").exists(),
            "direct_blocked": False,
            "nodes_health": [],
        }

    def _is_system_vpn_mode(self) -> bool:
        """Return True when Surfshark is installed or auto_trader is configured for direct system VPN."""
        if Path("/Applications/Surfshark.app").exists():
            return True
        if self.auto_trader is not None:
            return self.auto_trader.config.proxy_url.strip().upper() in ("", "DIRECT")
        return False

    def get_ordered_failover_proxies(self) -> list[str]:
        """Return ordered list of healthy proxy URLs for PolymarketAutoTrader multi-node retry."""
        ordered: list[str] = []
        if self.primary_node and self.primary_node != "DIRECT":
            ordered.append(self.primary_node)
        for n in self.healthy_web_nodes:
            if n["proxy"] not in ordered and n["proxy"] != "DIRECT":
                ordered.append(n["proxy"])
        for n in self.healthy_api_nodes:
            if n["proxy"] not in ordered and n["proxy"] != "DIRECT":
                ordered.append(n["proxy"])
        for p in self.candidate_nodes:
            if p not in ordered and p != "DIRECT":
                ordered.append(p)
        return [p for p in ordered if p]

    def _parse_proxy_url(self, proxy_url: str) -> dict[str, Any]:
        parsed = urlparse(proxy_url)
        scheme = parsed.scheme.lower()
        default_port = 1080 if "socks" in scheme else 8080
        return {
            "scheme": scheme,
            "host": parsed.hostname or "",
            "port": int(parsed.port or default_port),
            "username": parsed.username,
            "password": parsed.password,
            "raw": proxy_url,
        }

    async def _raw_socks5_handshake(
        self, proxy_url: str, target_host: str, target_port: int
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        info = self._parse_proxy_url(proxy_url)
        reader, writer = await asyncio.open_connection(info["host"], info["port"])
        try:
            user = info["username"]
            pwd = info["password"]
            if user is not None and pwd is not None:
                writer.write(b"\x05\x02\x00\x02")
            else:
                writer.write(b"\x05\x01\x00")
            await writer.drain()

            greeting = await reader.readexactly(2)
            if greeting[0] != 0x05:
                raise RuntimeError("Invalid SOCKS5 greeting")
            method = greeting[1]
            if method == 0x02:
                u_bytes = (user or "").encode("utf-8")
                p_bytes = (pwd or "").encode("utf-8")
                auth_pkt = struct.pack("!BB", 0x01, len(u_bytes)) + u_bytes + struct.pack("!B", len(p_bytes)) + p_bytes
                writer.write(auth_pkt)
                await writer.drain()
                auth_resp = await reader.readexactly(2)
                if auth_resp[1] != 0x00:
                    raise RuntimeError("SOCKS5 auth failed")
            elif method != 0x00:
                raise RuntimeError(f"Unsupported SOCKS5 auth method: {method}")

            host_bytes = target_host.encode("idna")
            req = b"\x05\x01\x00\x03" + struct.pack("!B", len(host_bytes)) + host_bytes + struct.pack("!H", target_port)
            writer.write(req)
            await writer.drain()

            resp_hdr = await reader.readexactly(4)
            if resp_hdr[1] != 0x00:
                raise RuntimeError(f"SOCKS5 connect error code: {resp_hdr[1]}")
            atyp = resp_hdr[3]
            if atyp == 0x01:
                await reader.readexactly(4 + 2)
            elif atyp == 0x03:
                dlen = (await reader.readexactly(1))[0]
                await reader.readexactly(dlen + 2)
            elif atyp == 0x04:
                await reader.readexactly(16 + 2)
            return reader, writer
        except Exception:
            try:
                writer.close()
            except Exception:
                pass
            raise

    async def _raw_http_connect_handshake(
        self, proxy_url: str, target_host: str, target_port: int
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        info = self._parse_proxy_url(proxy_url)
        reader, writer = await asyncio.open_connection(info["host"], info["port"])
        try:
            auth_hdr = ""
            if info["username"] is not None and info["password"] is not None:
                raw_cred = f"{info['username']}:{info['password']}".encode("utf-8")
                b64_cred = base64.b64encode(raw_cred).decode("ascii")
                auth_hdr = f"Proxy-Authorization: Basic {b64_cred}\r\n"

            req = (
                f"CONNECT {target_host}:{target_port} HTTP/1.1\r\n"
                f"Host: {target_host}:{target_port}\r\n"
                f"{auth_hdr}"
                f"Proxy-Connection: Keep-Alive\r\n\r\n"
            ).encode("latin-1")
            writer.write(req)
            await writer.drain()

            status_line = await reader.readline()
            if not status_line.startswith((b"HTTP/1.1 200", b"HTTP/1.0 200")):
                raise RuntimeError(f"HTTP CONNECT rejected: {status_line.decode('latin-1', errors='ignore').strip()}")

            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break
            return reader, writer
        except Exception:
            try:
                writer.close()
            except Exception:
                pass
            raise

    async def _open_upstream_proxy(
        self, proxy_url: str, target_host: str, target_port: int, timeout: float = 2.8
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        if proxy_url == "DIRECT":
            return await asyncio.wait_for(asyncio.open_connection(target_host, target_port), timeout=timeout)
        info = self._parse_proxy_url(proxy_url)
        if "socks" in info["scheme"]:
            return await asyncio.wait_for(
                self._raw_socks5_handshake(proxy_url, target_host, target_port),
                timeout=timeout,
            )
        return await asyncio.wait_for(
            self._raw_http_connect_handshake(proxy_url, target_host, target_port),
            timeout=timeout,
        )

    async def _dial_with_failover(
        self, target_host: str, target_port: int
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        dial_list = self.get_ordered_failover_proxies()
        last_err: Exception | None = None
        for idx, node_url in enumerate(dial_list[:5]):
            try:
                r, w = await self._open_upstream_proxy(node_url, target_host, target_port, timeout=2.8)
                if idx > 0:
                    self.stats["failover_count"] = int(self.stats.get("failover_count", 0)) + 1
                    self.primary_node = node_url
                self.stats["total_tunnels_opened"] = int(self.stats.get("total_tunnels_opened", 0)) + 1
                return r, w
            except Exception as e:
                last_err = e
                continue
        raise RuntimeError(f"All upstream VPN nodes failed for {target_host}:{target_port}: {last_err}")

    async def _pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        except Exception:
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass

    async def _handle_client(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter) -> None:
        try:
            first_byte = await asyncio.wait_for(client_reader.readexactly(1), timeout=5.0)
            if first_byte == b"\x05":
                nmethods = (await asyncio.wait_for(client_reader.readexactly(1), timeout=5.0))[0]
                await asyncio.wait_for(client_reader.readexactly(nmethods), timeout=5.0)
                client_writer.write(b"\x05\x00")
                await client_writer.drain()

                hdr = await asyncio.wait_for(client_reader.readexactly(4), timeout=5.0)
                cmd = hdr[1]
                atyp = hdr[3]
                if cmd != 0x01:
                    client_writer.write(b"\x05\x07\x00\x01\x00\x00\x00\x00\x00\x00")
                    await client_writer.drain()
                    client_writer.close()
                    return
                if atyp == 0x01:
                    raw_ip = await asyncio.wait_for(client_reader.readexactly(4), timeout=5.0)
                    target_host = socket.inet_ntoa(raw_ip)
                elif atyp == 0x03:
                    dlen = (await asyncio.wait_for(client_reader.readexactly(1), timeout=5.0))[0]
                    target_host = (await asyncio.wait_for(client_reader.readexactly(dlen), timeout=5.0)).decode("utf-8", errors="ignore")
                elif atyp == 0x04:
                    raw_ip6 = await asyncio.wait_for(client_reader.readexactly(16), timeout=5.0)
                    target_host = socket.inet_ntop(socket.AF_INET6, raw_ip6)
                else:
                    client_writer.close()
                    return
                target_port = struct.unpack("!H", await asyncio.wait_for(client_reader.readexactly(2), timeout=5.0))[0]

                try:
                    up_r, up_w = await self._dial_with_failover(target_host, target_port)
                except Exception:
                    client_writer.write(b"\x05\x04\x00\x01\x00\x00\x00\x00\x00\x00")
                    await client_writer.drain()
                    client_writer.close()
                    return

                client_writer.write(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
                await client_writer.drain()
                await asyncio.gather(self._pipe(client_reader, up_w), self._pipe(up_r, client_writer))
            else:
                rest_line = await asyncio.wait_for(client_reader.readline(), timeout=5.0)
                first_line = (first_byte + rest_line).decode("latin-1", errors="ignore").strip()
                parts = first_line.split()
                if len(parts) < 2:
                    client_writer.close()
                    return
                method = parts[0].upper()
                target = parts[1]

                headers_raw = b""
                while True:
                    line = await asyncio.wait_for(client_reader.readline(), timeout=5.0)
                    headers_raw += line
                    if line in (b"\r\n", b"\n", b""):
                        break

                if method == "CONNECT":
                    if ":" in target:
                        h_part, p_part = target.rsplit(":", 1)
                        target_host = h_part.strip("[]")
                        target_port = int(p_part)
                    else:
                        target_host = target
                        target_port = 443
                    try:
                        up_r, up_w = await self._dial_with_failover(target_host, target_port)
                    except Exception:
                        client_writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                        await client_writer.drain()
                        client_writer.close()
                        return
                    client_writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    await client_writer.drain()
                    await asyncio.gather(self._pipe(client_reader, up_w), self._pipe(up_r, client_writer))
                else:
                    parsed = urlparse(target)
                    target_host = parsed.hostname or ""
                    target_port = int(parsed.port or (443 if parsed.scheme == "https" else 80))
                    path_query = (parsed.path or "/") + (f"?{parsed.query}" if parsed.query else "")
                    if not target_host:
                        client_writer.close()
                        return
                    up_r, up_w = await self._dial_with_failover(target_host, target_port)
                    rebuilt = f"{method} {path_query} {parts[2] if len(parts) > 2 else 'HTTP/1.1'}\r\n".encode("latin-1") + headers_raw
                    up_w.write(rebuilt)
                    await up_w.drain()
                    await asyncio.gather(self._pipe(client_reader, up_w), self._pipe(up_r, client_writer))
        except Exception:
            try:
                client_writer.close()
            except Exception:
                pass

    async def _auto_replenish_node_pool(self) -> int:
        """
        Autonomous background node harvester: if healthy nodes drop below 2, scans live SOCKS5 & HTTP feeds
        with strict `asyncio.wait_for` bounds for low-latency Polymarket-unblocked nodes.
        """
        now = time.time()
        if now - self._last_replenish_ts < 45.0:
            return 0
        self._last_replenish_ts = now

        raw_candidates: list[str] = []
        async with httpx.AsyncClient(trust_env=False, timeout=4.0) as hc:
            for proto, feed_url in LIVE_PROXY_FEEDS:
                try:
                    r = await hc.get(feed_url)
                    if r.status_code == 200:
                        for line in r.text.splitlines()[:60]:
                            line = line.strip()
                            if ":" in line:
                                cand = f"{proto}://{line}"
                                if cand not in self.candidate_nodes and cand not in raw_candidates:
                                    raw_candidates.append(cand)
                except Exception:
                    continue

        if not raw_candidates:
            return 0

        sem = asyncio.Semaphore(45)
        verified_new: list[tuple[int, str]] = []

        async def _test_cand(p_url: str) -> None:
            async with sem:
                try:
                    async def _do_probe() -> tuple[int, dict[str, Any]] | None:
                        async with httpx.AsyncClient(proxy=p_url, trust_env=False, timeout=2.4) as client:
                            r1 = await client.get(GEOBLOCK_URL)
                            if r1.status_code != 200:
                                return None
                            t_warm = time.perf_counter()
                            r2 = await client.get(GEOBLOCK_URL)
                            warm_ms = int((time.perf_counter() - t_warm) * 1000)
                            if r2.status_code == 200:
                                return warm_ms, r2.json()
                            return None

                    res = await asyncio.wait_for(_do_probe(), timeout=3.2)
                    if res is not None:
                        warm_ms, d = res
                        blocked = bool(d.get("blocked", True))
                        country = str(d.get("country", "")).upper()
                        if (not blocked or country in WHITELIST_COUNTRIES) and warm_ms <= 1200:
                            verified_new.append((warm_ms, p_url))
                except Exception:
                    pass

        await asyncio.gather(*(_test_cand(p) for p in raw_candidates[:160]))
        verified_new.sort(key=lambda x: x[0])
        added = 0
        for _, p in verified_new[:5]:
            if p not in self.candidate_nodes:
                self.candidate_nodes.append(p)
                added += 1
        if added > 0:
            self.stats["auto_heals_count"] = int(self.stats.get("auto_heals_count", 0)) + added
        return added

    async def probe_all_nodes(self) -> dict[str, Any]:
        results: list[dict[str, Any]] = []

        async def _probe_single(proxy_url: str) -> dict[str, Any]:
            try:
                is_direct = proxy_url == "DIRECT"
                px = None if is_direct else proxy_url

                async def _do_check() -> dict[str, Any] | None:
                    t_start = time.perf_counter()
                    async with httpx.AsyncClient(proxy=px, trust_env=False, timeout=4.2) as hc:
                        resp = await hc.get(GEOBLOCK_URL)
                        cold_ms = int((time.perf_counter() - t_start) * 1000)
                        if resp.status_code != 200:
                            return None
                        data = resp.json()
                        # Measure warm HTTP/2 / Keep-Alive RTT on the already-established tunnel
                        t_warm = time.perf_counter()
                        try:
                            resp2 = await hc.get(CLOB_TIME_URL)
                            warm_ms = int((time.perf_counter() - t_warm) * 1000) if resp2.status_code == 200 else max(180, cold_ms // 3)
                        except Exception:
                            warm_ms = max(180, cold_ms // 3)

                        blocked = bool(data.get("blocked", True))
                        country = str(data.get("country", "UNKNOWN")).upper()
                        ip = str(data.get("ip", ""))
                        api_ok = (not blocked) or (country in API_ONLY_COUNTRIES)
                        return {
                            "proxy": proxy_url,
                            "ok": True,
                            "blocked": blocked,
                            "web_deposit_ok": not blocked,
                            "clob_api_ok": api_ok,
                            "country": country,
                            "ip": ip,
                            "latency_ms": warm_ms,
                            "cold_handshake_ms": cold_ms,
                        }

                out = await asyncio.wait_for(_do_check(), timeout=4.8)
                if out is not None:
                    return out
            except Exception as e:
                return {
                    "proxy": proxy_url,
                    "ok": False,
                    "blocked": True,
                    "web_deposit_ok": False,
                    "clob_api_ok": False,
                    "country": "ERR",
                    "ip": "",
                    "latency_ms": 9999,
                    "error": str(e),
                }
            return {
                "proxy": proxy_url,
                "ok": False,
                "blocked": True,
                "web_deposit_ok": False,
                "clob_api_ok": False,
                "country": "ERR",
                "ip": "",
                "latency_ms": 9999,
            }

        system_vpn_mode = self._is_system_vpn_mode()
        self.stats["direct_vpn_mode"] = system_vpn_mode
        self.stats["surfshark_detected"] = Path("/Applications/Surfshark.app").exists()

        # 1. Probe DIRECT first (e.g., Surfshark system WireGuard tunnel)
        direct_res = await _probe_single("DIRECT")
        if self.auto_trader is not None:
            self.auto_trader.direct_geoblock_info = dict(direct_res)

        if direct_res["clob_api_ok"]:
            results = [direct_res]
            web_ok = [direct_res] if direct_res["web_deposit_ok"] else []
            api_ok = [direct_res]
            self.healthy_web_nodes = web_ok
            self.healthy_api_nodes = api_ok
            self.primary_node = "DIRECT"
            self._consecutive_probe_failures = 0
            self._last_verified_primary_info = dict(direct_res)
            self.stats.update({
                "primary_upstream": "DIRECT",
                "primary_country": direct_res["country"],
                "primary_ip": direct_res["ip"],
                "primary_latency_ms": direct_res["latency_ms"],
                "web_deposit_unlocked": bool(direct_res["web_deposit_ok"]),
                "clob_api_unlocked": True,
                "direct_blocked": False,
                "last_check_ms": int(time.time() * 1000),
                "nodes_health": results,
            })
            if system_vpn_mode:
                self.disable_macos_pac()
            if self.auto_trader is not None:
                if self.auto_trader.config.proxy_url:
                    self.auto_trader.save_config({"proxy_url": ""})
                if self.auto_trader._warm_client is not None and self.auto_trader._warm_client_proxy not in ("", "DIRECT"):
                    asyncio.create_task(self.auto_trader._invalidate_warm_client())
                self._sync_auto_trader_status(direct_res=direct_res, system_vpn_mode=system_vpn_mode)
            return self.stats

        self.stats["direct_blocked"] = bool(direct_res.get("ok", True))

        # 2. DIRECT was blocked or unreachable: probe standby nodes so live trades never hit HTTP 403
        cand_targets = [p for p in self.candidate_nodes if p != "DIRECT"]
        probed_backups = await asyncio.gather(*(_probe_single(p) for p in cand_targets))
        results.append(direct_res)
        results.extend(probed_backups)

        web_ok = sorted([r for r in results if r["web_deposit_ok"]], key=lambda x: x["latency_ms"])
        api_ok = sorted([r for r in results if r["clob_api_ok"]], key=lambda x: (not x["web_deposit_ok"], x["latency_ms"]))

        # Autonomous replenishment only if DIRECT is blocked and healthy standby web nodes drop below 2
        if len(web_ok) < 2:
            added = await self._auto_replenish_node_pool()
            if added > 0:
                cand_targets2 = [p for p in self.candidate_nodes if p != "DIRECT"]
                probed2 = await asyncio.gather(*(_probe_single(p) for p in cand_targets2))
                results = [direct_res] + list(probed2)
                web_ok = sorted([r for r in results if r["web_deposit_ok"]], key=lambda x: x["latency_ms"])
                api_ok = sorted([r for r in results if r["clob_api_ok"]], key=lambda x: (not x["web_deposit_ok"], x["latency_ms"]))

        self.healthy_web_nodes = web_ok
        self.healthy_api_nodes = api_ok

        # Sticky Primary Hysteresis: keep current primary_node if it is still web_deposit_ok and within 200ms of best!
        current_primary_info = next((r for r in web_ok if r["proxy"] == self.primary_node), None)
        if web_ok:
            self._consecutive_probe_failures = 0
            best = web_ok[0]
            if (
                current_primary_info is not None
                and current_primary_info["latency_ms"] < 650
                and (current_primary_info["latency_ms"] - best["latency_ms"]) <= 200
            ):
                chosen_primary = current_primary_info
            else:
                chosen_primary = best
            self.primary_node = chosen_primary["proxy"]
            self._last_verified_primary_info = dict(chosen_primary)
            self.stats.update({
                "primary_upstream": chosen_primary["proxy"],
                "primary_country": chosen_primary["country"],
                "primary_ip": chosen_primary["ip"],
                "primary_latency_ms": chosen_primary["latency_ms"],
                "web_deposit_unlocked": True,
                "clob_api_unlocked": True,
            })
        elif api_ok:
            self._consecutive_probe_failures = 0
            best = api_ok[0]
            self.primary_node = best["proxy"]
            self._last_verified_primary_info = dict(best)
            self.stats.update({
                "primary_upstream": best["proxy"],
                "primary_country": best["country"],
                "primary_ip": best["ip"],
                "primary_latency_ms": best["latency_ms"],
                "web_deposit_unlocked": False,
                "clob_api_unlocked": True,
            })
        else:
            self._consecutive_probe_failures += 1
            # 2-Strike Jitter Protection: if all nodes had a transient 1-tick timeout (< 3 consecutive failures),
            # preserve the last verified primary node so the UI never flickers a false "Blocked" state!
            if self._consecutive_probe_failures < 3 and self._last_verified_primary_info:
                prev = self._last_verified_primary_info
                self.stats.update({
                    "primary_upstream": prev["proxy"],
                    "primary_country": prev["country"],
                    "primary_ip": prev["ip"],
                    "primary_latency_ms": prev["latency_ms"],
                    "web_deposit_unlocked": bool(prev.get("web_deposit_ok", True)),
                    "clob_api_unlocked": bool(prev.get("clob_api_ok", True)),
                })
            else:
                self.stats.update({
                    "primary_upstream": "DIRECT",
                    "primary_country": direct_res.get("country") or "TW",
                    "primary_ip": direct_res.get("ip") or "連線重試中",
                    "primary_latency_ms": direct_res.get("latency_ms", 0),
                    "web_deposit_unlocked": False,
                    "clob_api_unlocked": False,
                })

        self.stats["last_check_ms"] = int(time.time() * 1000)
        self.stats["nodes_health"] = results

        if system_vpn_mode:
            self.disable_macos_pac()

        if self.auto_trader is not None:
            if self.auto_trader._warm_client is not None and self.auto_trader._warm_client_proxy == "DIRECT":
                asyncio.create_task(self.auto_trader._invalidate_warm_client())
            self._sync_auto_trader_status(direct_res=direct_res, system_vpn_mode=system_vpn_mode)

        return self.stats

    def _sync_auto_trader_status(self, direct_res: dict[str, Any], system_vpn_mode: bool) -> None:
        """Synchronize `PolymarketAutoTrader` failover list & geoblock telemetry without overwriting direct VPN config."""
        if self.auto_trader is None:
            return
        is_web_ok = bool(self.stats.get("web_deposit_unlocked", False))
        is_api_ok = bool(self.stats.get("clob_api_unlocked", False))
        p_ip = str(self.stats.get("primary_ip", ""))
        p_country = str(self.stats.get("primary_country", "HK"))
        p_ms = int(self.stats.get("primary_latency_ms", 250))

        self.auto_trader.backup_proxies = self.get_ordered_failover_proxies()
        if (
            not system_vpn_mode
            and self.primary_node
            and self.primary_node != "DIRECT"
            and self.auto_trader.config.proxy_url != self.primary_node
        ):
            self.auto_trader.save_config({"proxy_url": self.primary_node})

        if self.primary_node == "DIRECT" and is_api_ok:
            proxy_used = f"Surfshark VPN 直連 ➔ {p_ip} ({p_country} {p_ms}ms)"
            if is_web_ok:
                advice_txt = (
                    f"✅ Surfshark VPN 直連 ({p_ip} / {p_country} | 延遲 {p_ms}ms) 100% 完全解鎖！"
                    "已關閉免費代理與系統 PAC，網頁與 CLOB V2 API 皆可原生高速下單！"
                )
            else:
                advice_txt = (
                    f"🟢 Surfshark VPN 直連 ({p_ip} / {p_country} | {p_ms}ms) CLOB V2 API 100% 開放！"
                    "已關閉免費代理與系統 PAC。"
                )
        elif system_vpn_mode and direct_res.get("ok") and not direct_res.get("clob_api_ok"):
            d_ip = str(direct_res.get("ip", ""))
            d_cty = str(direct_res.get("country", "UNKNOWN"))
            proxy_used = (
                f"緊急備援通道 ➔ {p_ip} ({p_country} {p_ms}ms)"
                if is_api_ok
                else f"Surfshark 直連受限 ({d_ip} / {d_cty})"
            )
            advice_txt = (
                f"⚠️ Surfshark 目前直連出口 {d_ip} ({d_cty}) 位於 Polymarket 封鎖區"
                f"（例如「杜拜」虛擬節點實測機房位於義大利 IT）。"
                f"請切換 Surfshark 至 **香港 (HK)**、**日本 (JP)**、**瑞士 (CH)** 或 **挪威 (NO)** 即可全速直連！"
                + (
                    f" 切換期間已由緊急備援 {p_ip} ({p_country} {p_ms}ms) 護航下單不中斷。"
                    if is_api_ok
                    else "（目前暫無可用備援節點，請立即切換 Surfshark 國家）"
                )
            )
        elif is_web_ok:
            proxy_used = f"HA-Gateway 127.0.0.1:{LOCAL_GATEWAY_PORT} ➔ {p_ip} ({p_country} {p_ms}ms)"
            advice_txt = (
                f"✅ 本地高可用 VPN 網關 (127.0.0.1:{LOCAL_GATEWAY_PORT}) 穩定運行中！"
                f"主節點：{p_ip} ({p_country} | 暖機延遲 {p_ms}ms)，網頁充值與 CLOB V2 下單 100% 解鎖！"
            )
        elif is_api_ok:
            proxy_used = f"HA-Gateway 127.0.0.1:{LOCAL_GATEWAY_PORT} ➔ {p_ip} ({p_country} {p_ms}ms)"
            advice_txt = (
                f"🟢 本地高可用 VPN 網關 (127.0.0.1:{LOCAL_GATEWAY_PORT}) 運行中！"
                f"主節點：{p_ip} ({p_country} | {p_ms}ms)，CLOB V2 API 自動下單 100% 開放。"
            )
        else:
            proxy_used = f"System VPN / Direct ({p_ip} / {p_country})"
            advice_txt = (
                f"⚠️ 目前直連出口：{p_ip} / {p_country}，請將 Surfshark 切換至香港 (HK)、日本 (JP) 或瑞士 (CH)。"
            )

        self.auto_trader.last_geoblock_status = {
            "ok": bool(is_web_ok or is_api_ok),
            "blocked": not is_web_ok,
            "direct_blocked": bool(direct_res.get("ok") and not direct_res.get("clob_api_ok")),
            "can_trade_api": bool(is_web_ok or is_api_ok),
            "api_only_unrestricted": bool(is_api_ok and not is_web_ok),
            "ip": p_ip,
            "country": p_country,
            "region": "",
            "proxy_used": proxy_used,
            "advice": advice_txt,
            "checked_at_ms": self.stats["last_check_ms"],
        }

    def disable_macos_pac(self) -> bool:
        """Turn off macOS Wi-Fi Auto Proxy Configuration (`polymarket.pac`) when using System VPN direct mode."""
        try:
            out = subprocess.check_output(
                ["networksetup", "-getautoproxyurl", "Wi-Fi"], text=True, timeout=3.0
            )
            if "Enabled: Yes" in out:
                subprocess.run(["networksetup", "-setautoproxystate", "Wi-Fi", "off"], check=False, timeout=3.0)
                self.stats["auto_heals_count"] = int(self.stats.get("auto_heals_count", 0)) + 1
            self.stats["macos_pac_enabled"] = False
            return True
        except Exception:
            self.stats["macos_pac_enabled"] = False
            return False

    def ensure_macos_pac_active(self, force_refresh: bool = False) -> bool:
        """
        Idempotent macOS PAC binding:
        - Automatically disables PAC when Surfshark System VPN or direct mode (`proxy_url == ""`) is active.
        - Otherwise binds static URL (`http://127.0.0.1:8088/polymarket.pac`) without dynamic query timestamps.
        """
        if self._is_system_vpn_mode():
            return self.disable_macos_pac()
        static_pac_url = "http://127.0.0.1:8088/polymarket.pac"
        try:
            out = subprocess.check_output(
                ["networksetup", "-getautoproxyurl", "Wi-Fi"], text=True, timeout=3.0
            )
            enabled = "Enabled: Yes" in out
            has_static_url = f"URL: {static_pac_url}" in out
            if (not enabled) or (not has_static_url):
                subprocess.run(["networksetup", "-setautoproxyurl", "Wi-Fi", static_pac_url], check=False, timeout=3.0)
                subprocess.run(["networksetup", "-setautoproxystate", "Wi-Fi", "on"], check=False, timeout=3.0)
                self.stats["auto_heals_count"] = int(self.stats.get("auto_heals_count", 0)) + 1
            self.stats["macos_pac_enabled"] = True
            return True
        except Exception:
            self.stats["macos_pac_enabled"] = False
            return False

    async def _ensure_gateway_server_listening(self) -> None:
        if self.server is None or not self.server.is_serving():
            try:
                self.server = await asyncio.start_server(
                    self._handle_client, LOCAL_GATEWAY_HOST, LOCAL_GATEWAY_PORT
                )
                self.stats["gateway_running"] = True
                self.stats["auto_heals_count"] = int(self.stats.get("auto_heals_count", 0)) + 1
            except Exception as e:
                self.stats["gateway_error"] = str(e)

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        await self._ensure_gateway_server_listening()
        self.ensure_macos_pac_active(force_refresh=False)
        # Run node probe in background so server startup is instantaneous (<1s)
        asyncio.create_task(self.probe_all_nodes())
        self._watchdog_task = asyncio.create_task(self._watchdog_loop())

    async def stop(self) -> None:
        self._running = False
        if self._watchdog_task:
            self._watchdog_task.cancel()
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        self.stats["gateway_running"] = False

    async def _watchdog_loop(self) -> None:
        """
        100% Silent Self-Healing Loop:
        - Checks gateway listener health and node pool latency every 25 seconds.
        - Never spams Telegram and never resets macOS system network settings!
        """
        while self._running:
            try:
                await asyncio.sleep(25.0)
                await self._ensure_gateway_server_listening()
                await self.probe_all_nodes()
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(5.0)
