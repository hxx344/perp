#!/usr/bin/env python3
"""Public order book spread monitor for Aster and Lighter.

This module connects to the public WebSocket feeds for both Aster and Lighter,
collects top-of-book quotes, computes cross-exchange spreads, and optionally
pushes the resulting metrics to the hedge coordinator dashboard.

Usage (local console output)::

    python strategies/aster_lighter_spread_monitor.py \
        --aster-ticker ETH \
        --lighter-symbol ETH-PERP

To publish the live table to the coordinator dashboard as a dedicated agent::

    python strategies/aster_lighter_spread_monitor.py \
        --aster-ticker ETH \
        --lighter-symbol ETH-PERP \
        --coordinator-url http://localhost:8899 \
        --agent-id spread-monitor
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from types import SimpleNamespace
from typing import Any, Deque, Dict, List, Optional, Tuple, cast
from urllib.parse import urljoin

import aiohttp
import lighter
from lighter import ApiClient, Configuration

from exchanges.aster import AsterMarketDataWebSocket
from exchanges.lighter_custom_websocket import LighterCustomWebSocketManager
from helpers.logger import TradingLogger

DEFAULT_HISTORY_LIMIT = 120
DEFAULT_POLL_INTERVAL = 1.0
DEFAULT_DEPTH_LEVELS = 10
DEFAULT_LIGHTER_BASE_URL = "https://mainnet.zklighter.elliot.ai"
DEFAULT_LIGHTER_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"

LOGGER = logging.getLogger("spread.monitor")


def _decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _format_decimal(value: Optional[Decimal], *, places: int = 6) -> str:
    if value is None:
        return "--"
    try:
        quant = value.quantize(Decimal(1).scaleb(-places))
    except (InvalidOperation, ValueError):
        quant = value
    text = f"{quant:f}"
    if "e" in text.lower():
        text = f"{quant.normalize():f}"
    return text


@dataclass
class SpreadSnapshot:
    timestamp: float
    aster_bid: Optional[Decimal]
    aster_bid_qty: Optional[Decimal]
    aster_ask: Optional[Decimal]
    aster_ask_qty: Optional[Decimal]
    lighter_bid: Optional[Decimal]
    lighter_bid_qty: Optional[Decimal]
    lighter_ask: Optional[Decimal]
    lighter_ask_qty: Optional[Decimal]
    spread_aster_bid_minus_lighter_ask: Optional[Decimal]
    spread_lighter_bid_minus_aster_ask: Optional[Decimal]
    mid_price_diff: Optional[Decimal]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "iso_time": datetime.fromtimestamp(self.timestamp, tz=timezone.utc).isoformat(),
            "aster_bid": _format_decimal(self.aster_bid),
            "aster_bid_qty": _format_decimal(self.aster_bid_qty, places=4),
            "aster_ask": _format_decimal(self.aster_ask),
            "aster_ask_qty": _format_decimal(self.aster_ask_qty, places=4),
            "lighter_bid": _format_decimal(self.lighter_bid),
            "lighter_bid_qty": _format_decimal(self.lighter_bid_qty, places=4),
            "lighter_ask": _format_decimal(self.lighter_ask),
            "lighter_ask_qty": _format_decimal(self.lighter_ask_qty, places=4),
            "spread_aster_bid_minus_lighter_ask": _format_decimal(self.spread_aster_bid_minus_lighter_ask),
            "spread_lighter_bid_minus_aster_ask": _format_decimal(self.spread_lighter_bid_minus_aster_ask),
            "mid_price_diff": _format_decimal(self.mid_price_diff),
        }


class AsterPublicDataClient:
    """Thin wrapper around the public Aster depth stream."""

    _BASE_URL = "https://fapi.asterdex.com"
    _WS_URL = "wss://fstream.asterdex.com"

    def __init__(self, ticker: str, logger: TradingLogger, *, depth_levels: int = DEFAULT_DEPTH_LEVELS) -> None:
        self.ticker = ticker.upper().strip()
        self.logger = logger
        self.depth_levels = max(5, min(depth_levels, 50))
        self.contract_id: Optional[str] = None
        self.tick_size: Decimal = Decimal("0")
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[AsterMarketDataWebSocket] = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=10)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def initialize(self) -> Tuple[str, Decimal]:
        if not self.ticker:
            raise ValueError("Aster ticker must not be empty")

        session = await self._ensure_session()
        async with session.get(f"{self._BASE_URL}/fapi/v1/exchangeInfo") as response:
            try:
                payload = await response.json()
            except aiohttp.ContentTypeError as exc:
                text = await response.text()
                raise RuntimeError(f"Aster exchangeInfo response not JSON: {text}") from exc

            if response.status != 200:
                raise RuntimeError(f"Failed to fetch Aster exchangeInfo ({response.status}): {payload}")

        symbols = payload.get("symbols") if isinstance(payload, dict) else None
        if not symbols:
            raise RuntimeError("Aster exchangeInfo missing symbols array")

        for symbol in symbols:
            if not isinstance(symbol, dict):
                continue
            status = symbol.get("status")
            base_asset = str(symbol.get("baseAsset", "")).upper()
            quote_asset = str(symbol.get("quoteAsset", "")).upper()
            if status != "TRADING" or base_asset != self.ticker or quote_asset != "USDT":
                continue

            contract_id = str(symbol.get("symbol", "")).strip()
            if not contract_id:
                continue

            tick_size = Decimal("0")
            for filter_info in symbol.get("filters", []):
                if not isinstance(filter_info, dict):
                    continue
                if filter_info.get("filterType") != "PRICE_FILTER":
                    continue
                tick_raw = str(filter_info.get("tickSize", "0")).strip()
                try:
                    tick_size = Decimal(tick_raw)
                except (InvalidOperation, ValueError):
                    tick_size = Decimal("0")
                break

            if tick_size <= 0:
                raise RuntimeError(f"Invalid tick size resolved for Aster ticker {self.ticker}")

            self.contract_id = contract_id
            self.tick_size = tick_size
            await self._ensure_stream()
            return contract_id, tick_size

        raise RuntimeError(f"Unable to locate trading symbol for Aster ticker {self.ticker}")

    async def _ensure_stream(self) -> bool:
        if not self.contract_id:
            return False

        symbol = self.contract_id.lower()
        try:
            if self._ws is None or self._ws.symbol != symbol:
                if self._ws is not None:
                    await self._ws.stop()
                self._ws = AsterMarketDataWebSocket(
                    symbol=symbol,
                    base_ws_url=self._WS_URL,
                    logger=self.logger,
                    depth_levels=self.depth_levels,
                )
                await self._ws.start()

            if self._ws:
                return await self._ws.wait_until_ready(timeout=2.0)
        except Exception as exc:  # pragma: no cover - defensive logging only
            self.logger.log(f"Failed to initialize Aster market data stream: {exc}", "WARNING")
        return False

    async def fetch_order_book(
        self, limit: int = DEFAULT_DEPTH_LEVELS
    ) -> Tuple[List[Tuple[Decimal, Decimal]], List[Tuple[Decimal, Decimal]]]:
        limit = max(1, min(limit, self.depth_levels))
        ws_ready = await self._ensure_stream()
        if ws_ready and self._ws is not None:
            bids, asks = await self._ws.get_depth(limit)
            if bids and asks:
                return bids[:limit], asks[:limit]

        if not self.contract_id:
            raise RuntimeError("Aster contract not initialized")

        session = await self._ensure_session()
        params = {"symbol": self.contract_id, "limit": str(limit)}
        async with session.get(f"{self._BASE_URL}/fapi/v1/depth", params=params) as response:
            try:
                payload = await response.json()
            except aiohttp.ContentTypeError as exc:
                text = await response.text()
                raise RuntimeError(f"Aster depth response not JSON: {text}") from exc

            if response.status != 200:
                raise RuntimeError(f"Failed to fetch Aster depth ({response.status}): {payload}")

        bids_raw = payload.get("bids", []) if isinstance(payload, dict) else []
        asks_raw = payload.get("asks", []) if isinstance(payload, dict) else []

        def _parse(levels: List[List[Any]]) -> List[Tuple[Decimal, Decimal]]:
            parsed: List[Tuple[Decimal, Decimal]] = []
            for entry in levels:
                if not isinstance(entry, list) or len(entry) < 2:
                    continue
                try:
                    price = Decimal(str(entry[0]))
                    qty = Decimal(str(entry[1]))
                except (InvalidOperation, ValueError, TypeError):
                    continue
                if price <= 0 or qty < 0:
                    continue
                parsed.append((price, qty))
                if len(parsed) >= limit:
                    break
            return parsed

        return _parse(bids_raw), _parse(asks_raw)

    async def fetch_bbo(self) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
        bids, asks = await self.fetch_order_book(limit=1)
        bid_price, bid_qty = (bids[0] if bids else (None, None))
        ask_price, ask_qty = (asks[0] if asks else (None, None))
        return bid_price, bid_qty, ask_price, ask_qty

    async def aclose(self) -> None:
        if self._ws is not None:
            try:
                await self._ws.stop()
            except Exception:
                pass
            self._ws = None
        if self._session is not None:
            try:
                await self._session.close()
            except Exception:
                pass
            self._session = None


class LighterPublicDataClient:
    """Public order book helper for Lighter using the custom websocket manager."""

    def __init__(
        self,
        symbol: str,
        logger: TradingLogger,
        *,
        base_url: str = DEFAULT_LIGHTER_BASE_URL,
        ws_url: str = DEFAULT_LIGHTER_WS_URL,
        depth_levels: int = 50,
    ) -> None:
        self.symbol = symbol.strip()
        self.logger = logger
        self.base_url = base_url.rstrip("/")
        self.ws_url = ws_url
        self.depth_levels = max(5, min(depth_levels, 200))
        self.market_id: Optional[int] = None
        self.tick_size: Decimal = Decimal("0")
        self._api_client: Optional[ApiClient] = None
        self._ws_manager: Optional[LighterCustomWebSocketManager] = None
        self._ws_task: Optional[asyncio.Task] = None

    async def _ensure_api_client(self) -> ApiClient:
        if self._api_client is None:
            configuration = Configuration(host=self.base_url)
            self._api_client = ApiClient(configuration=configuration)
        return self._api_client

    async def initialize(self) -> Tuple[str, Decimal]:
        if not self.symbol:
            raise ValueError("Lighter symbol must not be empty")

        api_client = await self._ensure_api_client()
        order_api = lighter.OrderApi(api_client)
        order_books = await order_api.order_books()
        markets = getattr(order_books, "order_books", [])

        target_market = None
        for market in markets:
            if getattr(market, "symbol", None) == self.symbol:
                target_market = market
                break

        if target_market is None:
            raise RuntimeError(f"Unable to resolve Lighter market for symbol {self.symbol}")

        try:
            market_id = int(getattr(target_market, "market_id"))
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Invalid Lighter market id for {self.symbol}: {exc}") from exc

        price_decimals = getattr(target_market, "supported_price_decimals", None)
        if price_decimals is None:
            raise RuntimeError(f"Lighter market {self.symbol} missing supported price decimals")
        try:
            tick_size = Decimal("1") / (Decimal("10") ** Decimal(int(price_decimals)))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise RuntimeError(f"Failed to compute tick size for Lighter market {self.symbol}: {exc}") from exc

        self.market_id = market_id
        self.tick_size = tick_size

        config_stub = SimpleNamespace(
            market_index=market_id,
            contract_id=str(market_id),
            account_index=None,
            lighter_client=None,
        )
        self._ws_manager = LighterCustomWebSocketManager(cast(Dict[str, Any], config_stub))
        self._ws_manager.ws_url = self.ws_url
        self._ws_manager.set_logger(self.logger)
        self._ws_task = asyncio.create_task(self._ws_manager.connect())
        ready = await self._ws_manager.wait_until_ready(timeout=10.0)
        if not ready:
            raise RuntimeError("Lighter public market stream did not become ready in time")
        return str(market_id), tick_size

    async def fetch_order_book(
        self, limit: int = DEFAULT_DEPTH_LEVELS
    ) -> Tuple[List[Tuple[Decimal, Decimal]], List[Tuple[Decimal, Decimal]]]:
        manager = self._ws_manager
        if manager is None:
            raise RuntimeError("Lighter public data stream not initialised")

        await manager.wait_until_ready(timeout=5.0)

        async with manager.order_book_lock:
            bids_dict = dict(manager.order_book.get("bids", {}))
            asks_dict = dict(manager.order_book.get("asks", {}))

        if not bids_dict or not asks_dict:
            return [], []

        sorted_bids = sorted(bids_dict.items(), key=lambda item: item[0], reverse=True)[:limit]
        sorted_asks = sorted(asks_dict.items(), key=lambda item: item[0])[:limit]

        bids: List[Tuple[Decimal, Decimal]] = []
        asks: List[Tuple[Decimal, Decimal]] = []

        for price, qty in sorted_bids:
            price_dec = _decimal(price)
            qty_dec = _decimal(qty)
            if price_dec is None or qty_dec is None or price_dec <= 0 or qty_dec < 0:
                continue
            bids.append((price_dec, qty_dec))

        for price, qty in sorted_asks:
            price_dec = _decimal(price)
            qty_dec = _decimal(qty)
            if price_dec is None or qty_dec is None or price_dec <= 0 or qty_dec < 0:
                continue
            asks.append((price_dec, qty_dec))

        return bids[:limit], asks[:limit]

    async def fetch_bbo(self) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
        bids, asks = await self.fetch_order_book(limit=1)
        bid_price, bid_qty = (bids[0] if bids else (None, None))
        ask_price, ask_qty = (asks[0] if asks else (None, None))
        return bid_price, bid_qty, ask_price, ask_qty

    async def aclose(self) -> None:
        if self._ws_manager is not None:
            try:
                await self._ws_manager.disconnect()
            except Exception:
                pass
            self._ws_manager = None
        if self._ws_task is not None:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            self._ws_task = None
        if self._api_client is not None:
            try:
                await self._api_client.close()
            except Exception:
                pass
            self._api_client = None


class SpreadMonitor:
    def __init__(
        self,
        *,
        aster_ticker: str,
        lighter_symbol: str,
        coordinator_url: Optional[str] = None,
        agent_id: str = "spread-monitor",
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        history_limit: int = DEFAULT_HISTORY_LIMIT,
        depth_levels: int = DEFAULT_DEPTH_LEVELS,
        log_to_console: bool = True,
        lighter_base_url: str = DEFAULT_LIGHTER_BASE_URL,
        lighter_ws_url: str = DEFAULT_LIGHTER_WS_URL,
    ) -> None:
        self.aster_ticker = aster_ticker.upper().strip()
        self.lighter_symbol = lighter_symbol.strip()
        self.coordinator_url = (coordinator_url or "").strip() or None
        self.agent_id = agent_id.strip() or "spread-monitor"
        self.poll_interval = max(0.2, float(poll_interval))
        self.depth_levels = max(1, int(depth_levels))
        self.history: Deque[SpreadSnapshot] = deque(maxlen=max(10, int(history_limit)))
        self.log_to_console = log_to_console
        self.lighter_base_url = lighter_base_url
        self.lighter_ws_url = lighter_ws_url

        ticker_label = f"{self.aster_ticker}_{self.lighter_symbol}".replace("/", "-")
        self.aster_logger = TradingLogger("aster-md", ticker_label, log_to_console=False)
        self.lighter_logger = TradingLogger("lighter-md", ticker_label, log_to_console=False)

        self.aster_client = AsterPublicDataClient(
            ticker=self.aster_ticker,
            logger=self.aster_logger,
            depth_levels=self.depth_levels,
        )
        self.lighter_client = LighterPublicDataClient(
            symbol=self.lighter_symbol,
            logger=self.lighter_logger,
            base_url=self.lighter_base_url,
            ws_url=self.lighter_ws_url,
            depth_levels=max(self.depth_levels, 20),
        )
        self._http_session: Optional[aiohttp.ClientSession] = None
        self._console_header_emitted = False
        self._running = False
        self._last_push_error: Optional[str] = None
        self._aster_contract_id: Optional[str] = None
        self._lighter_market_id: Optional[str] = None

    async def _ensure_http_session(self) -> aiohttp.ClientSession:
        if self._http_session is None or self._http_session.closed:
            timeout = aiohttp.ClientTimeout(total=10)
            self._http_session = aiohttp.ClientSession(timeout=timeout)
        return self._http_session

    async def initialize(self) -> None:
        LOGGER.info(
            "Resolving market metadata for Aster ticker %s and Lighter symbol %s",
            self.aster_ticker,
            self.lighter_symbol,
        )
        self._aster_contract_id, _ = await self.aster_client.initialize()
        self._lighter_market_id, _ = await self.lighter_client.initialize()
        LOGGER.info(
            "Market metadata resolved | Aster contract=%s, Lighter market=%s",
            self._aster_contract_id,
            self._lighter_market_id,
        )
        if self.coordinator_url:
            await self._ensure_http_session()

    async def close(self) -> None:
        try:
            await self.aster_client.aclose()
        except Exception:
            pass
        try:
            await self.lighter_client.aclose()
        except Exception:
            pass
        if self._http_session is not None:
            try:
                await self._http_session.close()
            except Exception:
                pass
            self._http_session = None

    async def _compute_snapshot(self) -> Optional[SpreadSnapshot]:
        try:
            aster_bid, aster_bid_qty, aster_ask, aster_ask_qty = await self.aster_client.fetch_bbo()
        except Exception as exc:
            LOGGER.warning("Failed to fetch Aster order book: %s", exc)
            return None

        try:
            lighter_bid, lighter_bid_qty, lighter_ask, lighter_ask_qty = await self.lighter_client.fetch_bbo()
        except Exception as exc:
            LOGGER.warning("Failed to fetch Lighter order book: %s", exc)
            return None

        if not all([aster_bid, aster_ask, lighter_bid, lighter_ask]):
            return None

        spread_sell_on_aster = None
        spread_buy_on_aster = None
        mid_diff = None

        if aster_bid is not None and lighter_ask is not None:
            spread_sell_on_aster = aster_bid - lighter_ask
        if lighter_bid is not None and aster_ask is not None:
            spread_buy_on_aster = lighter_bid - aster_ask

        if aster_bid is not None and aster_ask is not None and lighter_bid is not None and lighter_ask is not None:
            try:
                aster_mid = (aster_bid + aster_ask) / Decimal("2")
                lighter_mid = (lighter_bid + lighter_ask) / Decimal("2")
                mid_diff = aster_mid - lighter_mid
            except (InvalidOperation, TypeError):
                mid_diff = None

        snapshot = SpreadSnapshot(
            timestamp=time.time(),
            aster_bid=aster_bid,
            aster_bid_qty=aster_bid_qty,
            aster_ask=aster_ask,
            aster_ask_qty=aster_ask_qty,
            lighter_bid=_decimal(lighter_bid),
            lighter_bid_qty=_decimal(lighter_bid_qty),
            lighter_ask=_decimal(lighter_ask),
            lighter_ask_qty=_decimal(lighter_ask_qty),
            spread_aster_bid_minus_lighter_ask=spread_sell_on_aster,
            spread_lighter_bid_minus_aster_ask=spread_buy_on_aster,
            mid_price_diff=mid_diff,
        )
        return snapshot

    def _print_snapshot(self, snapshot: SpreadSnapshot) -> None:
        if not self.log_to_console:
            return

        if not self._console_header_emitted:
            header = (
                "Time                | Aster Bid  / Ask        | Lighter Bid / Ask      | "
                "AsterBid-LighterAsk | LighterBid-AsterAsk | Mid Diff"
            )
            print(header)
            print("-" * len(header))
            self._console_header_emitted = True

        ts = datetime.fromtimestamp(snapshot.timestamp).strftime("%H:%M:%S")
        line = (
            f"{ts:>8}            | "
            f"{_format_decimal(snapshot.aster_bid):>12} / {_format_decimal(snapshot.aster_ask):<12} | "
            f"{_format_decimal(snapshot.lighter_bid):>12} / {_format_decimal(snapshot.lighter_ask):<12} | "
            f"{_format_decimal(snapshot.spread_aster_bid_minus_lighter_ask):>20} | "
            f"{_format_decimal(snapshot.spread_lighter_bid_minus_aster_ask):>20} | "
            f"{_format_decimal(snapshot.mid_price_diff):>10}"
        )
        print(line)

    async def _push_to_coordinator(self, snapshot: SpreadSnapshot) -> None:
        if not self.coordinator_url:
            return

        session = await self._ensure_http_session()
        payload = {
            "agent_id": self.agent_id,
            "instrument": f"{self.aster_ticker}/{self.lighter_symbol}",
            "spread_metrics": {
                "updated_at": snapshot.timestamp,
                "latest": snapshot.to_dict(),
                "history": [snap.to_dict() for snap in list(self.history)],
                "depth_levels": self.depth_levels,
                "instrument": f"{self.aster_ticker}/{self.lighter_symbol}",
                "aster": {
                    "ticker": self.aster_ticker,
                    "contract_id": self._aster_contract_id,
                },
                "lighter": {
                    "symbol": self.lighter_symbol,
                    "market_id": self._lighter_market_id,
                },
            },
        }
        try:
            endpoint = urljoin(self.coordinator_url.rstrip("/") + "/", "update")
            async with session.post(endpoint, json=payload) as response:
                if response.status >= 400:
                    text = await response.text()
                    raise RuntimeError(f"HTTP {response.status}: {text}")
            self._last_push_error = None
        except Exception as exc:
            message = str(exc)
            if message != self._last_push_error:
                LOGGER.warning("Failed to push spread metrics to coordinator: %s", message)
                self._last_push_error = message

    async def run(self) -> None:
        await self.initialize()
        self._running = True
        try:
            while self._running:
                snapshot = await self._compute_snapshot()
                if snapshot is not None:
                    self.history.append(snapshot)
                    self._print_snapshot(snapshot)
                    await self._push_to_coordinator(snapshot)
                await asyncio.sleep(self.poll_interval)
        except asyncio.CancelledError:
            raise
        finally:
            await self.close()

    def stop(self) -> None:
        self._running = False


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor Aster/Lighter public spread data")
    parser.add_argument("--aster-ticker", required=True, help="Spot/margin ticker on Aster (e.g., ETH)")
    parser.add_argument("--lighter-symbol", required=True, help="Perpetual symbol on Lighter (e.g., ETH-PERP)")
    parser.add_argument("--coordinator-url", help="Optional hedge coordinator base URL for dashboard updates")
    parser.add_argument("--agent-id", default="spread-monitor", help="Identifier reported to the coordinator")
    parser.add_argument("--interval", type=float, default=DEFAULT_POLL_INTERVAL, help="Update interval in seconds")
    parser.add_argument("--history", type=int, default=DEFAULT_HISTORY_LIMIT, help="Number of rows to retain in history")
    parser.add_argument("--depth", type=int, default=DEFAULT_DEPTH_LEVELS, help="Order book depth levels to sample")
    parser.add_argument("--lighter-base-url", default=DEFAULT_LIGHTER_BASE_URL, help="Override Lighter REST base URL")
    parser.add_argument("--lighter-ws-url", default=DEFAULT_LIGHTER_WS_URL, help="Override Lighter WebSocket URL")
    parser.add_argument("--no-console", action="store_true", help="Disable console table output")
    parser.add_argument("--log-level", default="INFO", help="Python logging level (default INFO)")
    return parser.parse_args(argv)


async def _async_main(args: argparse.Namespace) -> None:
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    monitor = SpreadMonitor(
        aster_ticker=args.aster_ticker,
        lighter_symbol=args.lighter_symbol,
        coordinator_url=args.coordinator_url,
        agent_id=args.agent_id,
        poll_interval=args.interval,
        history_limit=args.history,
        depth_levels=args.depth,
        log_to_console=not args.no_console,
        lighter_base_url=args.lighter_base_url,
        lighter_ws_url=args.lighter_ws_url,
    )

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal(signame: str) -> None:
        LOGGER.info("Received %s; shutting down spread monitor", signame)
        monitor.stop()
        stop_event.set()

    for signame in ("SIGINT", "SIGTERM"):
        if hasattr(signal, signame):
            loop.add_signal_handler(getattr(signal, signame), lambda s=signame: _handle_signal(s))

    run_task = asyncio.create_task(monitor.run())
    await stop_event.wait()
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    try:
        asyncio.run(_async_main(args))
    except KeyboardInterrupt:
        LOGGER.info("Spread monitor interrupted by user")
    except Exception as exc:
        LOGGER.error("Spread monitor terminated with error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
