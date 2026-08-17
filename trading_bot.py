"""
Modular Trading Bot - Supports multiple exchanges
"""

import os
import time
import asyncio
import traceback
import inspect
from contextlib import suppress
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP, ROUND_FLOOR, InvalidOperation
from typing import Optional, Dict, Any, List, Tuple, TYPE_CHECKING, Literal, Iterable, cast

from exchanges import ExchangeFactory
from helpers import TradingLogger
from helpers.lark_bot import LarkBot
from helpers.telegram_bot import TelegramBot

try:  # pragma: no cover - optional dependency for coordinator integration
    import aiohttp  # type: ignore
except ImportError:  # pragma: no cover
    aiohttp = None  # type: ignore


Action = Literal["RUN", "PAUSE"]


@dataclass
class TradingConfig:
    """Configuration class for trading parameters."""
    ticker: str
    contract_id: str
    quantity: Decimal
    take_profit: Decimal
    tick_size: Decimal
    direction: str
    max_orders: int
    wait_time: int
    exchange: str
    grid_step: Decimal
    stop_price: Decimal
    pause_price: Decimal
    boost_mode: bool
    maker_depth_level: int = 10
    coordinator_url: Optional[str] = None
    coordinator_vps_id: Optional[str] = None
    coordinator_alias: Optional[str] = None
    coordinator_user: Optional[str] = None
    coordinator_password: Optional[str] = None
    coordinator_poll_interval: float = 5.0
    coordinator_metrics_interval: float = 15.0

    @property
    def close_order_side(self) -> str:
        """Get the close order side based on bot direction."""
        return 'buy' if self.direction == "sell" else 'sell'


@dataclass
class OrderMonitor:
    """Thread-safe order monitoring state."""
    order_id: Optional[str] = None
    filled: bool = False
    filled_price: Optional[Decimal] = None
    filled_qty: Decimal = Decimal("0")

    def reset(self):
        """Reset the monitor state."""
        self.order_id = None
        self.filled = False
        self.filled_price = None
        self.filled_qty = Decimal("0")


class TradingBot:
    """Modular Trading Bot - Main trading logic supporting multiple exchanges."""

    def __init__(self, config: TradingConfig):
        self.config = config
        self.logger = TradingLogger(config.exchange, config.ticker, log_to_console=True)

        # Create exchange client
        try:
            client = ExchangeFactory.create_exchange(
                config.exchange,
                cast(Any, config)
            )
            self.exchange_client = cast(Any, client)
        except ValueError as e:
            raise ValueError(f"Failed to create exchange client: {e}")

        # Trading state
        self.active_close_orders = []
        self._active_close_amount = Decimal("0")
        self.last_close_orders = 0
        self.last_open_order_time = 0
        self.last_log_time = 0
        self.current_order_status = None
        self.order_filled_event = asyncio.Event()
        self.order_canceled_event = asyncio.Event()
        self.shutdown_requested = False
        self.loop = None

        # Coordinator integration
        self.coordinator_enabled = bool(
            self.config.coordinator_url and self.config.coordinator_vps_id
        )
        self._coordinator_base_url = (
            self.config.coordinator_url.rstrip("/")
            if self.config.coordinator_url
            else None
        )
        self._coordinator_credentials = (
            self.config.coordinator_user,
            self.config.coordinator_password,
        )
        self._coordinator_session: Optional[Any] = None
        self._coordinator_tasks: List[asyncio.Task] = []
        self._coordinator_action_tasks: set[asyncio.Task] = set()
        self._coordinator_mode: Action = cast(Action, "RUN")
        self._coordinator_reason: Optional[str] = None
        self._coordinator_trade_volume = Decimal("0")
        self._coordinator_pause_logged = False
        self._command_poll_interval = max(self.config.coordinator_poll_interval or 5.0, 1.0)
        self._metrics_interval = max(self.config.coordinator_metrics_interval or 15.0, 5.0)
        self._coordinator_auth = None
        self._last_mismatch_alert_state = None
        self._mismatch_notified_state = None
        self._manual_balance_lock = asyncio.Lock()
        self._coordinator_registered = False
        self._coordinator_register_lock = asyncio.Lock()
        self._coordinator_last_register_attempt = 0.0
        self._residual_close_amount = Decimal("0")

        self.order_filled_amount = Decimal("0")

        # Register order callback
        self._setup_websocket_handlers()

    async def graceful_shutdown(self, reason: str = "Unknown"):
        """Perform graceful shutdown of the trading bot."""
        self.logger.log(f"Starting graceful shutdown: {reason}", "INFO")
        self.shutdown_requested = True

        try:
            await self._shutdown_coordinator()
            # Disconnect from exchange
            await self.exchange_client.disconnect()
            self.logger.log("Graceful shutdown completed", "INFO")

        except Exception as e:
            self.logger.log(f"Error during graceful shutdown: {e}", "ERROR")

    def _setup_websocket_handlers(self):
        """Setup WebSocket handlers for order updates."""
        def order_update_handler(message):
            """Handle order updates from WebSocket."""
            try:
                # Check if this is for our contract
                if str(message.get('contract_id')) != str(self.config.contract_id):
                    return

                order_id = message.get('order_id')
                status = message.get('status')
                side = message.get('side', '')
                order_type = message.get('order_type', '')
                filled_size = Decimal(message.get('filled_size'))
                if order_type == "OPEN":
                    self.current_order_status = status

                if status == 'FILLED':
                    self._record_trade_volume(filled_size, message.get('price'))
                    if order_type == "OPEN":
                        self.order_filled_amount = filled_size
                        # Ensure thread-safe interaction with asyncio event loop
                        if self.loop is not None:
                            self.loop.call_soon_threadsafe(self.order_filled_event.set)
                        else:
                            # Fallback (should not happen after run() starts)
                            self.order_filled_event.set()

                    self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                    f"{message.get('size')} @ {message.get('price')}", "INFO")
                    self.logger.log_transaction(order_id, side, message.get('size'), message.get('price'), status)
                elif status == "CANCELED":
                    if order_type == "OPEN":
                        self.order_filled_amount = filled_size
                        if self.loop is not None:
                            self.loop.call_soon_threadsafe(self.order_canceled_event.set)
                        else:
                            self.order_canceled_event.set()

                        if self.order_filled_amount > 0:
                            self.logger.log_transaction(order_id, side, self.order_filled_amount, message.get('price'), status)

                    # PATCH
                    if self.config.exchange == "extended":
                        self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                        f"{Decimal(message.get('size')) - filled_size} @ {message.get('price')}", "INFO")
                    else:
                        self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                        f"{message.get('size')} @ {message.get('price')}", "INFO")
                elif status == "PARTIALLY_FILLED":
                    self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                    f"{filled_size} @ {message.get('price')}", "INFO")
                else:
                    self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                    f"{message.get('size')} @ {message.get('price')}", "INFO")

            except Exception as e:
                self.logger.log(f"Error handling order update: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

        # Setup order update handler
        self.exchange_client.setup_order_update_handler(order_update_handler)

    @staticmethod
    def _safe_decimal_or_none(value: Any) -> Optional[Decimal]:
        try:
            if value is None:
                return None
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return None

    @staticmethod
    def _safe_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
        parsed = TradingBot._safe_decimal_or_none(value)
        return parsed if parsed is not None else default

    def _close_quantity_step(self) -> Optional[Decimal]:
        step_candidate = getattr(self.exchange_client, "quantity_step", None)
        if step_candidate is not None:
            try:
                step_decimal = Decimal(str(step_candidate))
                if step_decimal > 0:
                    return step_decimal
            except (InvalidOperation, ValueError, TypeError):
                pass

        multiplier_candidate = getattr(self.exchange_client, "base_amount_multiplier", None)
        if multiplier_candidate is not None:
            try:
                multiplier_decimal = Decimal(str(multiplier_candidate))
                if multiplier_decimal > 0:
                    return Decimal("1") / multiplier_decimal
            except (InvalidOperation, ZeroDivisionError, ValueError, TypeError):
                pass

        return None

    def _prepare_close_quantity(self, amount: Decimal) -> Optional[Decimal]:
        amount = self._safe_decimal(amount, default=Decimal("0"))
        if amount <= 0:
            return None

        step = self._close_quantity_step()
        total = amount + self._residual_close_amount

        if step is None or step <= 0:
            self._residual_close_amount = Decimal("0")
            return total

        if total < step:
            self._residual_close_amount = total
            return None

        steps = (total / step).to_integral_value(rounding=ROUND_FLOOR)
        adjusted = steps * step
        residual = total - adjusted
        if residual < 0:
            residual = Decimal("0")
        self._residual_close_amount = residual

        if adjusted <= 0:
            return None
        return adjusted

    def _filled_quantity_for_close(self, order_result: Any) -> Decimal:
        candidates = [
            getattr(self, "order_filled_amount", None),
            getattr(order_result, "filled_size", None),
            getattr(getattr(self, "exchange_client", None), "current_order", None),
            getattr(order_result, "size", None),
            self.config.quantity,
        ]
        for candidate in candidates:
            if hasattr(candidate, "filled_size"):
                candidate = getattr(candidate, "filled_size", None)
            parsed = self._safe_decimal_or_none(candidate)
            if parsed is not None and parsed > 0:
                return parsed
        return Decimal("0")

    def _quantize_close_amount(self, amount: Decimal) -> Decimal:
        amount = self._safe_decimal(amount, default=Decimal("0"))
        if amount <= 0:
            return Decimal("0")
        step = self._close_quantity_step()
        if step is None or step <= 0:
            return amount
        steps = (amount / step).to_integral_value(rounding=ROUND_FLOOR)
        return steps * step

    def _record_trade_volume(self, quantity: Any, price: Any) -> None:
        if not self.coordinator_enabled:
            return
        qty = self._safe_decimal_or_none(quantity)
        px = self._safe_decimal_or_none(price)
        if qty is None or px is None:
            return
        self._coordinator_trade_volume += qty * px

    def _position_direction_from_amount(
        self,
        amount: Optional[Decimal],
        *,
        close_side_hint: Optional[str] = None,
    ) -> str:
        hint = (close_side_hint or "").lower()
        client = getattr(self, "exchange_client", None)
        signed_positions = bool(getattr(client, "positions_are_signed", client is None))

        if signed_positions and amount is not None:
            if amount > 0:
                return "long"
            if amount < 0:
                return "short"

        if hint == "buy":
            return "short"
        if hint == "sell":
            return "long"

        if self.config.direction == "sell":
            return "short"
        if self.config.direction == "buy":
            return "long"
        return "flat"

    def _closing_side_for_position(
        self,
        position_amt: Optional[Decimal] = None,
        orders: Optional[Iterable[Any]] = None,
    ) -> str:
        default_side = self.config.close_order_side

        client = getattr(self, "exchange_client", None)
        signed_positions = bool(getattr(client, "positions_are_signed", client is None))
        if signed_positions and position_amt is not None:
            if position_amt > 0:
                return "sell"
            if position_amt < 0:
                return "buy"

        if default_side in {"buy", "sell"}:
            return default_side

        best_side: Optional[str] = None
        best_total = Decimal("0")

        if orders is not None:
            totals: Dict[str, Decimal] = {}

            for order in orders:
                side_raw = getattr(order, "side", None)
                if side_raw is None and isinstance(order, dict):
                    side_raw = order.get("side")

                side = str(side_raw).lower() if side_raw else None
                if side not in {"buy", "sell"}:
                    continue

                size_decimal = self._remaining_order_quantity(order)
                if size_decimal is None or size_decimal <= 0:
                    continue

                totals[side] = totals.get(side, Decimal("0")) + size_decimal
                if totals[side] > best_total:
                    best_total = totals[side]
                    best_side = side

        if best_side:
            return best_side

        return default_side

    @staticmethod
    def _normalize_bool(value: Any) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "yes", "1"}:
                return True
            if lowered in {"false", "no", "0"}:
                return False
        return None

    def _is_close_order(self, order: Any) -> bool:
        order_type = getattr(order, "order_type", None)
        if order_type is None and isinstance(order, dict):
            order_type = order.get("order_type") or order.get("type")
        if order_type is not None:
            normalized_type = str(order_type).strip().lower()
            if normalized_type in {"close", "closing", "take_profit", "tp", "reduce", "reduce_only"}:
                return True
            if normalized_type in {"open", "opening", "entry"}:
                return False

        reduce_only = getattr(order, "reduce_only", None)
        if reduce_only is None and isinstance(order, dict):
            reduce_only = order.get("reduce_only")
        normalized_reduce_only = self._normalize_bool(reduce_only)
        if normalized_reduce_only is not None:
            return normalized_reduce_only

        return True

    def _remaining_order_quantity(self, order: Any) -> Optional[Decimal]:
        def value(name: str) -> Any:
            candidate = getattr(order, name, None)
            if candidate is None and isinstance(order, dict):
                candidate = order.get(name)
            return candidate

        size = self._safe_decimal_or_none(value("size"))
        if size is None or size <= 0:
            return None

        remaining = self._safe_decimal_or_none(value("remaining_size"))
        if remaining is not None and remaining > 0:
            return remaining

        filled = self._safe_decimal_or_none(value("filled_size"))
        if filled is not None and filled > 0:
            return max(size - filled, Decimal("0"))

        return size

    def _prepare_close_orders(
        self,
        orders: List[Any],
        position_amt: Optional[Decimal] = None,
    ) -> Tuple[List[Dict[str, Any]], Decimal, Optional[str]]:
        filtered_orders: List[Any] = []
        for order in orders:
            if not self._is_close_order(order):
                continue
            filtered_orders.append(order)

        close_side = self._closing_side_for_position(position_amt=position_amt, orders=filtered_orders)
        close_orders: List[Dict[str, Any]] = []
        total = Decimal("0")

        for order in filtered_orders:
            side_raw = getattr(order, "side", None)
            if side_raw is None and isinstance(order, dict):
                side_raw = order.get("side")
            side = str(side_raw).lower() if side_raw else None

            if close_side and side and side != close_side:
                continue

            size_decimal = self._remaining_order_quantity(order)
            if size_decimal is None or size_decimal <= 0:
                continue

            price = getattr(order, "price", None)
            if price is None and isinstance(order, dict):
                price = order.get("price")

            order_id = getattr(order, "order_id", None)
            if order_id is None and isinstance(order, dict):
                order_id = order.get("id") or order.get("order_id")

            close_orders.append({
                "id": order_id,
                "price": price,
                "size": size_decimal,
                "side": side or close_side,
            })
            total += size_decimal

        return close_orders, total, close_side

    def _classify_mismatch_severity(self, mismatch_amount: Decimal) -> Optional[str]:
        warning_threshold = self.config.quantity * Decimal("2")
        critical_threshold = self.config.quantity * Decimal("4")
        if mismatch_amount > critical_threshold:
            return "critical"
        if mismatch_amount > warning_threshold:
            return "warning"
        return None

    def _format_mismatch_message(
        self,
        severity: str,
        position_amt: Decimal,
        active_close_amount: Decimal,
        mismatch_amount: Decimal,
    ) -> str:
        label = "ERROR" if severity == "critical" else "WARNING"
        header = f"\n\n{label}: [{self.config.exchange.upper()}_{self.config.ticker.upper()}] Position mismatch detected\n"
        body = "###### ERROR ###### ERROR ###### ERROR ###### ERROR #####\n"
        body += "Please manually rebalance your position and take-profit orders\n"
        body += "请手动平衡当前仓位和正在关闭的仓位\n"
        body += (
            f"current position: {position_amt} | active closing amount: {active_close_amount} | "
            f"Order quantity: {len(self.active_close_orders)}\n"
            f"difference: {mismatch_amount}\n"
        )
        body += "###### ERROR ###### ERROR ###### ERROR ###### ERROR #####\n"
        return header + body

    async def _report_mismatch_alert(
        self,
        *,
        severity: str,
        position: Decimal,
        active_close: Decimal,
        mismatch: Decimal,
    ) -> None:
        if not self.coordinator_enabled or not self.config.coordinator_vps_id:
            return
        if not await self._ensure_coordinator_registered():
            return
        message = (
            f"Position {position} vs closing {active_close} (diff {mismatch})"
            if severity != "resolved"
            else "Position mismatch resolved"
        )
        payload = {
            "vps_id": self.config.coordinator_vps_id,
            "type": "position_mismatch",
            "severity": severity,
            "message": message,
            "symbol": self.config.ticker,
            "position": str(position),
            "active_closing_amount": str(active_close),
            "difference": str(mismatch),
            "timestamp": time.time(),
        }
        await self._coordinator_request("POST", "/alerts", json=payload)

    async def _update_mismatch_alert_state(
        self,
        *,
        severity: Optional[str],
        position: Decimal,
        active_close: Decimal,
        mismatch: Decimal,
    ) -> None:
        if not self.coordinator_enabled or not self.config.coordinator_vps_id:
            self._last_mismatch_alert_state = severity
            return

        if severity is None:
            if self._last_mismatch_alert_state is not None:
                await self._report_mismatch_alert(
                    severity="resolved",
                    position=position,
                    active_close=active_close,
                    mismatch=mismatch,
                )
            self._last_mismatch_alert_state = None
            return

        if severity != self._last_mismatch_alert_state:
            await self._report_mismatch_alert(
                severity=severity,
                position=position,
                active_close=active_close,
                mismatch=mismatch,
            )
            self._last_mismatch_alert_state = severity

    async def _perform_manual_balance(self, payload: Dict[str, Any]) -> None:
        async with self._manual_balance_lock:
            if self.shutdown_requested:
                return
            reason = payload.get("reason")
            reason_text = (
                f"（原因：{reason}）" if isinstance(reason, str) and reason.strip() else ""
            )
            self.logger.log(f"收到协调机手动平衡指令{reason_text}", "WARNING")
            try:
                active_orders = await self.exchange_client.get_active_orders(self.config.contract_id)
                position_amt = await self.exchange_client.get_account_positions()
                self.active_close_orders, active_close_amount, close_side = self._prepare_close_orders(
                    active_orders,
                    position_amt,
                )
                self._active_close_amount = active_close_amount

                if self.shutdown_requested:
                    return

                adjusted = await self._attempt_auto_balance(
                    position_amt,
                    active_close_amount,
                    close_side_hint=close_side,
                )
                if adjusted:
                    await asyncio.sleep(3)
                    refreshed_orders = await self.exchange_client.get_active_orders(self.config.contract_id)
                    position_amt = await self.exchange_client.get_account_positions()
                    self.active_close_orders, active_close_amount, close_side = self._prepare_close_orders(
                        refreshed_orders,
                        position_amt,
                    )
                    self._active_close_amount = active_close_amount
                    mismatch_amount = abs(abs(position_amt) - active_close_amount)
                    severity = self._classify_mismatch_severity(mismatch_amount)
                    await self._update_mismatch_alert_state(
                        severity=severity,
                        position=position_amt,
                        active_close=active_close_amount,
                        mismatch=mismatch_amount,
                    )
                    self._mismatch_notified_state = severity
                    self.logger.log("手动平衡已执行，已刷新仓位数据", "INFO")
                else:
                    mismatch_amount = abs(abs(position_amt) - active_close_amount)
                    severity = self._classify_mismatch_severity(mismatch_amount)
                    await self._update_mismatch_alert_state(
                        severity=severity,
                        position=position_amt,
                        active_close=active_close_amount,
                        mismatch=mismatch_amount,
                    )
                    self._mismatch_notified_state = severity
                    self.logger.log("手动平衡无需调整（仓位与平仓单匹配）", "INFO")
            except Exception as exc:
                self.logger.log(f"手动平衡失败: {exc}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            finally:
                # Force下一轮日志快速刷新，以便尽快复检仓位状态
                self.last_log_time = 0

    async def _ensure_coordinator_session(self) -> Optional[Any]:
        if not self.coordinator_enabled or self._coordinator_base_url is None:
            return None
        if aiohttp is None:
            self.logger.log("aiohttp 未安装，协调机功能已禁用", "WARNING")
            self.coordinator_enabled = False
            return None
        if self._coordinator_session is None or getattr(self._coordinator_session, "closed", False):
            timeout = aiohttp.ClientTimeout(total=10)
            self._coordinator_session = aiohttp.ClientSession(timeout=timeout)
            user, password = self._coordinator_credentials
            if user:
                self._coordinator_auth = aiohttp.BasicAuth(user, password or "")
        return self._coordinator_session

    async def _coordinator_request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Optional[Any]:
        session = await self._ensure_coordinator_session()
        if session is None:
            return None
        if not path.startswith("/"):
            path = "/" + path
        url = f"{self._coordinator_base_url}{path}"
        try:
            async with session.request(
                method,
                url,
                auth=self._coordinator_auth,
                params=params,
                json=json,
            ) as response:
                if response.status >= 400:
                    text = await response.text()
                    if response.status == 404 and "not registered" in text.lower():
                        self.logger.log("协调机提示节点未注册，准备重新注册", "WARNING")
                        self._coordinator_registered = False
                        self._coordinator_last_register_attempt = 0.0
                        return None
                    raise RuntimeError(f"{method} {path} -> {response.status}: {text}")
                content_type = response.headers.get("Content-Type", "")
                if "application/json" in content_type:
                    return await response.json()
                return await response.text()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.logger.log(f"协调机请求异常 {method} {path}: {exc}", "WARNING")
            return None

    async def _ensure_coordinator_registered(self) -> bool:
        if not self.coordinator_enabled or self._coordinator_base_url is None:
            return False
        if self._coordinator_registered:
            return True
        async with self._coordinator_register_lock:
            if self._coordinator_registered:
                return True
            now = time.time()
            retry_interval = max(self._command_poll_interval, 3.0)
            if now - self._coordinator_last_register_attempt < retry_interval:
                return False
            self._coordinator_last_register_attempt = now
            success = await self._init_coordinator()
            return success

    async def _init_coordinator(self) -> bool:
        if not self.coordinator_enabled:
            return False
        payload = {"vps_id": self.config.coordinator_vps_id}
        if self.config.coordinator_alias:
            payload["display_name"] = self.config.coordinator_alias
        response = await self._coordinator_request("POST", "/register", json=payload)
        if not isinstance(response, dict):
            self.logger.log("无法注册协调机，稍后将自动重试", "WARNING")
            self._coordinator_registered = False
            return False
        action = str(response.get("mode") or response.get("action") or "RUN").upper()
        if action not in {"RUN", "PAUSE"}:
            action = "RUN"
        self._coordinator_mode = cast(Action, action)
        self._coordinator_reason = response.get("reason") if isinstance(response.get("reason"), str) else None
        self._coordinator_registered = True
        self.logger.log(
            f"已连接协调机，当前模式: {self._coordinator_mode}"
            + (f", 原因: {self._coordinator_reason}" if self._coordinator_reason else ""),
            "INFO",
        )
        return True

    def _start_coordinator_tasks(self) -> None:
        if not self.coordinator_enabled or self._coordinator_tasks:
            return
        self._coordinator_tasks.append(asyncio.create_task(self._poll_coordinator_commands()))
        self._coordinator_tasks.append(asyncio.create_task(self._push_coordinator_metrics()))

    async def _poll_coordinator_commands(self) -> None:
        while not self.shutdown_requested and self.coordinator_enabled:
            if not await self._ensure_coordinator_registered():
                await asyncio.sleep(self._command_poll_interval)
                continue
            data = await self._coordinator_request(
                "GET",
                "/command",
                params={"vps_id": self.config.coordinator_vps_id},
            )
            if isinstance(data, dict):
                action = str(data.get("action", "RUN")).upper()
                if action not in {"RUN", "PAUSE"}:
                    action = "RUN"
                reason = data.get("reason") if isinstance(data.get("reason"), str) else None
                if action != self._coordinator_mode:
                    self._coordinator_mode = cast(Action, action)
                    self._coordinator_reason = reason
                    self._coordinator_pause_logged = False
                    level = "WARNING" if action == "PAUSE" else "INFO"
                    message = "协调机指令: 暂停交易" if action == "PAUSE" else "协调机指令: 恢复交易"
                    if reason:
                        message += f"，原因: {reason}"
                    self.logger.log(message, level)
                actions_payload = data.get("actions")
                if isinstance(actions_payload, list) and actions_payload:
                    for action_payload in actions_payload:
                        if self.shutdown_requested:
                            break
                        task = asyncio.create_task(
                            self._handle_coordinator_action(action_payload)
                        )
                        self._coordinator_action_tasks.add(task)
                        task.add_done_callback(self._coordinator_action_tasks.discard)
            else:
                await asyncio.sleep(self._command_poll_interval)
                continue
            await asyncio.sleep(self._command_poll_interval)

    async def _build_coordinator_metrics_payload(self) -> Optional[Dict[str, Any]]:
        if not self.coordinator_enabled:
            return None
        account_metrics: Dict[str, Any] = {}
        metrics_getter = getattr(self.exchange_client, "get_account_metrics", None)
        if callable(metrics_getter):
            try:
                metrics_result = metrics_getter()
                if inspect.isawaitable(metrics_result):
                    metrics_result = await metrics_result
                if isinstance(metrics_result, dict):
                    account_metrics = metrics_result
            except Exception as exc:
                self.logger.log(f"获取账户指标失败: {exc}", "WARNING")

        position_candidate = account_metrics.get("position_size")
        if position_candidate is not None:
            position = self._safe_decimal(position_candidate)
        else:
            try:
                position = await self.exchange_client.get_account_positions()
            except Exception as exc:
                self.logger.log(f"获取仓位失败（协调机上报将使用0）: {exc}", "WARNING")
                position = Decimal("0")

        close_side_hint: Optional[str] = None
        position_symbol: Optional[str] = None
        symbol_candidate = account_metrics.get("position_symbol") or account_metrics.get("symbol")
        if isinstance(symbol_candidate, str) and symbol_candidate.strip():
            position_symbol = symbol_candidate.strip()
        position_value = self._safe_decimal_or_none(account_metrics.get("position_value"))
        manual_preview: Optional[Dict[str, Any]] = None
        position_snapshot_method = getattr(self.exchange_client, "get_position_snapshot", None)
        if callable(position_snapshot_method) and (position_symbol is None or position_value is None):
            try:
                snapshot_result = position_snapshot_method()
                if inspect.isawaitable(snapshot_result):
                    snapshot = await snapshot_result
                else:
                    snapshot = snapshot_result
                if isinstance(snapshot, dict):
                    snapshot_symbol = snapshot.get("symbol") or snapshot.get("position_symbol")
                    if position_symbol is None and isinstance(snapshot_symbol, str) and snapshot_symbol.strip():
                        position_symbol = snapshot_symbol.strip()

                    if position_value is None:
                        value_candidate = snapshot.get("value")
                        if value_candidate is None:
                            value_candidate = snapshot.get("position_value")
                        if value_candidate is None:
                            value_candidate = snapshot.get("notional")
                        value_decimal = self._safe_decimal_or_none(value_candidate)
                        if value_decimal is not None:
                            position_value = value_decimal
            except Exception as exc:
                self.logger.log(f"获取仓位详情失败: {exc}", "WARNING")

        try:
            raw_active_orders = await self.exchange_client.get_active_orders(self.config.contract_id)
            if isinstance(raw_active_orders, list):
                active_orders: List[Any] = list(raw_active_orders)
            elif raw_active_orders is None:
                active_orders = []
            else:
                active_orders = list(raw_active_orders)
        except Exception as exc:
            self.logger.log(f"获取委托单失败（预览将为空）: {exc}", "WARNING")
            active_orders = []

        try:
            close_orders, close_total, close_side = self._prepare_close_orders(active_orders, position)
            self._active_close_amount = close_total
            close_side_hint = close_side
            mismatch_amount = abs(abs(position) - close_total)
            severity = self._classify_mismatch_severity(mismatch_amount)
            preview_orders: List[Dict[str, Any]] = []
            for order in close_orders:
                preview_orders.append({
                    "id": order.get("id"),
                    "side": order.get("side"),
                    "size": str(order.get("size")) if order.get("size") is not None else None,
                    "price": str(order.get("price")) if order.get("price") is not None else None,
                })
            manual_preview = {
                "position": str(position),
                "position_direction": self._position_direction_from_amount(
                    position,
                    close_side_hint=close_side,
                ),
                "position_abs": str(abs(position)),
                "active_close_amount": str(close_total),
                "difference": str(mismatch_amount),
                "close_side": close_side,
                "order_count": len(preview_orders),
                "orders": preview_orders,
                "severity": severity,
                "timestamp": time.time(),
                "residual_close_amount": str(self._residual_close_amount),
            }
        except Exception as exc:
            self.logger.log(f"生成手动平衡预览失败: {exc}", "WARNING")

        balance_candidate = account_metrics.get("available_balance")
        if balance_candidate is None:
            balance_candidate = account_metrics.get("availableBalance")
        if balance_candidate is None:
            balance_candidate = account_metrics.get("balance")
        balance = self._safe_decimal(balance_candidate)

        total_value_candidate = account_metrics.get("total_account_value")
        if total_value_candidate is None:
            total_value_candidate = account_metrics.get("total_asset_value")
        if total_value_candidate is None:
            total_value_candidate = account_metrics.get("total_value")
        total_value = self._safe_decimal_or_none(total_value_candidate)

        balance_getter = getattr(self.exchange_client, "get_available_balance", None)
        if balance_candidate is None and callable(balance_getter):
            try:
                balance_result = balance_getter()
                if inspect.isawaitable(balance_result):
                    balance_result = await balance_result
                balance = self._safe_decimal(balance_result, default=balance)
            except Exception as exc:
                self.logger.log(f"获取余额失败: {exc}", "WARNING")

        position_direction = self._position_direction_from_amount(
            position,
            close_side_hint=close_side_hint,
        )

        payload: Dict[str, Any] = {
            "vps_id": self.config.coordinator_vps_id,
            "position": str(position),
            "position_direction": position_direction,
            "trading_volume": str(self._coordinator_trade_volume),
            "balance": str(balance),
            "total_value": str(total_value) if total_value is not None else None,
            "timestamp": time.time(),
        }
        payload["active_close_amount"] = str(self._active_close_amount)
        payload["residual_close_amount"] = str(self._residual_close_amount)
        if position_symbol:
            payload["position_symbol"] = position_symbol
        if position_value is not None:
            payload["position_value"] = str(position_value)
        if manual_preview is not None:
            payload["manual_balance_preview"] = manual_preview
        return payload

    async def _push_coordinator_metrics(self) -> None:
        while not self.shutdown_requested and self.coordinator_enabled:
            if not await self._ensure_coordinator_registered():
                await asyncio.sleep(self._metrics_interval)
                continue
            payload = await self._build_coordinator_metrics_payload()
            if payload is not None:
                await self._coordinator_request("POST", "/metrics", json=payload)
            await asyncio.sleep(self._metrics_interval)

    async def _handle_coordinator_action(self, payload: Dict[str, Any]) -> None:
        try:
            action_type = str(payload.get("type", "")).upper()
        except Exception:
            return

        if action_type == "MANUAL_BALANCE":
            await self._perform_manual_balance(payload)

    async def _shutdown_coordinator(self, *, disable: bool = False) -> None:
        for task in self._coordinator_tasks:
            task.cancel()
        for task in self._coordinator_tasks:
            with suppress(asyncio.CancelledError):
                await task
        self._coordinator_tasks.clear()
        for task in list(self._coordinator_action_tasks):
            task.cancel()
        for task in list(self._coordinator_action_tasks):
            with suppress(asyncio.CancelledError):
                await task
        self._coordinator_action_tasks.clear()
        if self._coordinator_session is not None:
            try:
                await self._coordinator_session.close()
            except Exception:
                pass
        self._coordinator_session = None
        self._coordinator_auth = None
        if disable:
            self.coordinator_enabled = False
        self._coordinator_registered = False

    def _calculate_wait_time(self) -> int:
        """Calculate wait time between orders."""
        cool_down_time = self.config.wait_time

        if len(self.active_close_orders) < self.last_close_orders:
            self.last_close_orders = len(self.active_close_orders)
            return 0

        self.last_close_orders = len(self.active_close_orders)
        if len(self.active_close_orders) >= self.config.max_orders:
            return 1

        if len(self.active_close_orders) / self.config.max_orders >= 2/3:
            cool_down_time = 2 * self.config.wait_time
        elif len(self.active_close_orders) / self.config.max_orders >= 1/3:
            cool_down_time = self.config.wait_time
        elif len(self.active_close_orders) / self.config.max_orders >= 1/6:
            cool_down_time = self.config.wait_time / 2
        else:
            cool_down_time = self.config.wait_time / 4

        # if the program detects active_close_orders during startup, it is necessary to consider cooldown_time
        if self.last_open_order_time == 0 and len(self.active_close_orders) > 0:
            self.last_open_order_time = time.time()

        if time.time() - self.last_open_order_time > cool_down_time:
            return 0
        else:
            return 1

    async def _place_and_monitor_open_order(self) -> bool:
        """Place an order and monitor its execution."""
        try:
            # Reset state before placing order
            self.order_filled_event.clear()
            self.current_order_status = 'OPEN'
            self.order_filled_amount = Decimal("0")

            # Place the order
            order_result = await self.exchange_client.place_open_order(
                self.config.contract_id,
                self.config.quantity,
                self.config.direction
            )

            if not order_result.success:
                return False

            if order_result.status == 'FILLED':
                return await self._handle_order_result(order_result)
            elif not self.order_filled_event.is_set():
                try:
                    await asyncio.wait_for(self.order_filled_event.wait(), timeout=10)
                except asyncio.TimeoutError:
                    pass

            # Handle order result
            return await self._handle_order_result(order_result)

        except Exception as e:
            self.logger.log(f"Error placing order: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            return False

    async def _handle_order_result(self, order_result) -> bool:
        """Handle the result of an order placement."""
        order_id = order_result.order_id
        filled_price = order_result.price

        if self.order_filled_event.is_set() or order_result.status == 'FILLED':
            filled_quantity = self._filled_quantity_for_close(order_result)
            prepared_quantity = self._prepare_close_quantity(filled_quantity)
            self.order_filled_amount = Decimal("0")
            if prepared_quantity is None:
                self.logger.log(
                    "[CLOSE] Filled amount below minimum order size; queued for next accumulation",
                    "INFO",
                )
                self.last_open_order_time = time.time()
                return True

            if self.config.boost_mode:
                close_order_result = await self.exchange_client.place_market_order(
                    self.config.contract_id,
                    prepared_quantity,
                    self.config.close_order_side
                )
            else:
                self.last_open_order_time = time.time()
                # Place close order
                close_side = self.config.close_order_side
                if close_side == 'sell':
                    close_price = filled_price * (1 + self.config.take_profit/100)
                else:
                    close_price = filled_price * (1 - self.config.take_profit/100)

                close_order_result = await self.exchange_client.place_close_order(
                    self.config.contract_id,
                    prepared_quantity,
                    close_price,
                    close_side
                )
                if self.config.exchange == "lighter":
                    await asyncio.sleep(1)

            if not close_order_result.success:
                self.logger.log(f"[CLOSE] Failed to place close order: {close_order_result.error_message}", "ERROR")
                raise Exception(f"[CLOSE] Failed to place close order: {close_order_result.error_message}")

            return True

        else:
            new_order_price = await self.exchange_client.get_order_price(self.config.direction)

            def should_wait(direction: str, new_order_price: Decimal, order_result_price: Decimal) -> bool:
                if direction == "buy":
                    return new_order_price <= order_result_price
                elif direction == "sell":
                    return new_order_price >= order_result_price
                return False

            if self.config.exchange == "lighter":
                current_order_status = self.exchange_client.current_order.status
            else:
                order_info = await self.exchange_client.get_order_info(order_id)
                current_order_status = order_info.status

            while (
                should_wait(self.config.direction, new_order_price, order_result.price)
                and current_order_status == "OPEN"
            ):
                self.logger.log(f"[OPEN] [{order_id}] Waiting for order to be filled @ {order_result.price}", "INFO")
                await asyncio.sleep(5)
                if self.config.exchange == "lighter":
                    current_order_status = self.exchange_client.current_order.status
                else:
                    order_info = await self.exchange_client.get_order_info(order_id)
                    if order_info is not None:
                        current_order_status = order_info.status
                new_order_price = await self.exchange_client.get_order_price(self.config.direction)

            self.order_canceled_event.clear()
            # Cancel the order if it's still open
            self.logger.log(f"[OPEN] [{order_id}] Cancelling order and placing a new order", "INFO")
            if self.config.exchange == "lighter":
                cancel_result = await self.exchange_client.cancel_order(order_id)
                start_time = time.time()
                while (time.time() - start_time < 10 and self.exchange_client.current_order.status != 'CANCELED' and
                        self.exchange_client.current_order.status != 'FILLED'):
                    await asyncio.sleep(0.1)

                if self.exchange_client.current_order.status not in ['CANCELED', 'FILLED']:
                    raise Exception(f"[OPEN] Error cancelling order: {self.exchange_client.current_order.status}")
                else:
                    self.order_filled_amount = self.exchange_client.current_order.filled_size
            else:
                try:
                    cancel_result = await self.exchange_client.cancel_order(order_id)
                    if not cancel_result.success:
                        self.order_canceled_event.set()
                        self.logger.log(f"[CLOSE] Failed to cancel order {order_id}: {cancel_result.error_message}", "WARNING")
                    else:
                        self.current_order_status = "CANCELED"

                except Exception as e:
                    self.order_canceled_event.set()
                    self.logger.log(f"[CLOSE] Error canceling order {order_id}: {e}", "ERROR")

                if self.config.exchange == "backpack" or self.config.exchange == "extended":
                    self.order_filled_amount = cancel_result.filled_size
                else:
                    # Wait for cancel event or timeout
                    if not self.order_canceled_event.is_set():
                        try:
                            await asyncio.wait_for(self.order_canceled_event.wait(), timeout=5)
                        except asyncio.TimeoutError:
                            order_info = await self.exchange_client.get_order_info(order_id)
                            self.order_filled_amount = order_info.filled_size

            if self.order_filled_amount > 0:
                prepared_quantity = self._prepare_close_quantity(self.order_filled_amount)
                self.order_filled_amount = Decimal("0")
                if prepared_quantity is None:
                    self.logger.log(
                        "[CLOSE] Filled amount below minimum order size; queued for next accumulation",
                        "INFO",
                    )
                    self.last_open_order_time = time.time()
                    return True

                close_side = self.config.close_order_side
                if self.config.boost_mode:
                    close_order_result = await self.exchange_client.place_close_order(
                        self.config.contract_id,
                        prepared_quantity,
                        filled_price,
                        close_side
                    )
                else:
                    if close_side == 'sell':
                        close_price = filled_price * (1 + self.config.take_profit/100)
                    else:
                        close_price = filled_price * (1 - self.config.take_profit/100)

                    close_order_result = await self.exchange_client.place_close_order(
                        self.config.contract_id,
                        prepared_quantity,
                        close_price,
                        close_side
                    )
                    if self.config.exchange == "lighter":
                        await asyncio.sleep(1)

                self.last_open_order_time = time.time()
                if not close_order_result.success:
                    self.logger.log(f"[CLOSE] Failed to place close order: {close_order_result.error_message}", "ERROR")

            return True

        return False

    async def _log_status_periodically(self):
        """Log status information periodically, including positions."""
        if time.time() - self.last_log_time > 60 or self.last_log_time == 0:
            print("--------------------------------")
            try:
                # Get active orders and current position
                active_orders = await self.exchange_client.get_active_orders(self.config.contract_id)
                position_amt = await self.exchange_client.get_account_positions()

                # Filter close orders based on actual position direction
                self.active_close_orders, active_close_amount, close_side = self._prepare_close_orders(
                    active_orders,
                    position_amt,
                )
                position_abs = abs(position_amt)
                mismatch_amount = abs(position_abs - active_close_amount)

                # Attempt automatic balancing when mismatch is moderate
                auto_balanced = False
                lower_threshold = 2 * self.config.quantity
                upper_threshold = 4 * self.config.quantity
                if lower_threshold < mismatch_amount <= upper_threshold:
                    auto_balanced = await self._attempt_auto_balance(
                        position_amt,
                        active_close_amount,
                        close_side_hint=close_side,
                    )
                    if auto_balanced:
                        # Give exchange a moment to reflect newly submitted orders
                        await asyncio.sleep(3)

                        # Refresh orders and positions after balancing attempt
                        active_orders = await self.exchange_client.get_active_orders(self.config.contract_id)
                        position_amt = await self.exchange_client.get_account_positions()
                        self.active_close_orders, active_close_amount, close_side = self._prepare_close_orders(
                            active_orders,
                            position_amt,
                        )
                        self._active_close_amount = active_close_amount
                        position_abs = abs(position_amt)
                        mismatch_amount = abs(position_abs - active_close_amount)

                self.logger.log(f"Current Position: {position_amt} | Active closing amount: {active_close_amount} | "
                                f"Order quantity: {len(self.active_close_orders)}")
                self.last_log_time = time.time()
                # Check for position mismatch
                severity = self._classify_mismatch_severity(mismatch_amount)
                if severity in {"warning", "critical"}:
                    message = self._format_mismatch_message(
                        severity,
                        position_amt,
                        active_close_amount,
                        mismatch_amount,
                    )
                    log_level = "ERROR" if severity == "critical" else "WARNING"
                    self.logger.log(message, log_level)
                    if severity == "critical" and self._mismatch_notified_state != "critical":
                        await self.send_notification(message.lstrip())
                        self._mismatch_notified_state = "critical"
                    elif severity == "warning" and self._mismatch_notified_state != "warning":
                        self._mismatch_notified_state = "warning"
                else:
                    if self._mismatch_notified_state is not None:
                        self._mismatch_notified_state = None

                await self._update_mismatch_alert_state(
                    severity=severity,
                    position=position_amt,
                    active_close=active_close_amount,
                    mismatch=mismatch_amount,
                )

                mismatch_detected = severity is not None

                return mismatch_detected

            except Exception as e:
                self.logger.log(f"Error in periodic status check: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

            print("--------------------------------")

    async def _attempt_auto_balance(
        self,
        position_amt: Decimal,
        active_close_amount: Decimal,
        close_side_hint: Optional[str] = None,
    ) -> bool:
        """Try to automatically balance position and close orders when mismatch is moderate."""
        try:
            if self.shutdown_requested:
                return False
            position_abs = abs(position_amt)
            difference = position_abs - active_close_amount
            normalized_hint = (close_side_hint or "").strip().lower()
            if normalized_hint in {"buy", "sell"}:
                close_side = normalized_hint
            else:
                close_side = self._closing_side_for_position(position_amt)
            if close_side not in {"buy", "sell"}:
                close_side = self.config.close_order_side

            if difference == 0:
                return False

            if difference > 0:
                close_quantity = self._quantize_close_amount(difference)
                if close_quantity <= 0:
                    self.logger.log(
                        "[AUTO-BALANCE] Difference below minimum order size; waiting for more fills",
                        "INFO",
                    )
                    return False
                # Need additional close orders
                if len(self.active_close_orders) >= self.config.max_orders:
                    self.logger.log("[AUTO-BALANCE] Max close orders reached; cannot add more.", "WARNING")
                    return False

                best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)
                tp_multiplier = self.config.take_profit / Decimal('100')
                if close_side == 'sell':
                    target_price = best_bid * (Decimal('1') + tp_multiplier)
                else:
                    target_price = best_ask * (Decimal('1') - tp_multiplier)

                if self.config.tick_size > 0:
                    target_price = target_price.quantize(self.config.tick_size, rounding=ROUND_HALF_UP)

                if self.shutdown_requested:
                    return False

                balance_result = await self.exchange_client.place_close_order(
                    self.config.contract_id,
                    close_quantity,
                    target_price,
                    close_side
                )

                if balance_result.success:
                    self.logger.log(
                        f"[AUTO-BALANCE] Added close order: {close_quantity} @ {target_price}",
                        "INFO"
                    )
                    return True

                self.logger.log(
                    f"[AUTO-BALANCE] Failed to add close order: {balance_result.error_message}",
                    "WARNING"
                )
                return False

            # difference < 0, cancel excess close orders
            excess = abs(difference)
            if not self.active_close_orders:
                return False

            canceled_amount = Decimal('0')
            reverse_sort = True if close_side == 'sell' else False
            for order in sorted(self.active_close_orders, key=lambda o: o['price'], reverse=reverse_sort):
                if canceled_amount >= excess or self.shutdown_requested:
                    break

                cancel_result = await self.exchange_client.cancel_order(order['id'])
                if cancel_result.success:
                    size = Decimal(order['size'])
                    canceled_amount += size
                    self.logger.log(
                        f"[AUTO-BALANCE] Cancelled close order {order['id']} size {size}",
                        "INFO"
                    )
                else:
                    self.logger.log(
                        f"[AUTO-BALANCE] Failed to cancel order {order['id']}: {cancel_result.error_message}",
                        "WARNING"
                    )

            return canceled_amount > 0

        except Exception as e:
            self.logger.log(f"[AUTO-BALANCE] Error while balancing positions: {e}", "ERROR")
            self.logger.log(f"[AUTO-BALANCE] Traceback: {traceback.format_exc()}", "ERROR")
            return False

    async def _meet_grid_step_condition(self) -> bool:
        if self.active_close_orders:
            picker = min if self.config.direction == "buy" else max
            next_close_order = picker(self.active_close_orders, key=lambda o: o["price"])
            next_close_price = next_close_order["price"]

            best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)
            if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
                raise ValueError("No bid/ask data available")

            if self.config.direction == "buy":
                new_order_close_price = best_ask * (1 + self.config.take_profit/100)
                if next_close_price / new_order_close_price > 1 + self.config.grid_step/100:
                    return True
                else:
                    return False
            elif self.config.direction == "sell":
                new_order_close_price = best_bid * (1 - self.config.take_profit/100)
                if new_order_close_price / next_close_price > 1 + self.config.grid_step/100:
                    return True
                else:
                    return False
            else:
                raise ValueError(f"Invalid direction: {self.config.direction}")
        else:
            return True

    async def _check_price_condition(self) -> Tuple[bool, bool]:
        stop_trading = False
        pause_trading = False

        if self.config.pause_price == self.config.stop_price == -1:
            return stop_trading, pause_trading

        best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)
        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            raise ValueError("No bid/ask data available")

        if self.config.stop_price != -1:
            if self.config.direction == "buy":
                if best_ask >= self.config.stop_price:
                    stop_trading = True
            elif self.config.direction == "sell":
                if best_bid <= self.config.stop_price:
                    stop_trading = True

        if self.config.pause_price != -1:
            if self.config.direction == "buy":
                if best_ask >= self.config.pause_price:
                    pause_trading = True
            elif self.config.direction == "sell":
                if best_bid <= self.config.pause_price:
                    pause_trading = True

        return stop_trading, pause_trading

    async def send_notification(self, message: str):
        lark_token = os.getenv("LARK_TOKEN")
        if lark_token:
            async with LarkBot(lark_token) as lark_bot:
                await lark_bot.send_text(message)

        telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if telegram_token and telegram_chat_id:
            with TelegramBot(telegram_token, telegram_chat_id) as tg_bot:
                tg_bot.send_text(message)

    async def run(self):
        """Main trading loop."""
        try:
            self.config.contract_id, self.config.tick_size = await self.exchange_client.get_contract_attributes()

            # Log current TradingConfig
            self.logger.log("=== Trading Configuration ===", "INFO")
            self.logger.log(f"Ticker: {self.config.ticker}", "INFO")
            self.logger.log(f"Contract ID: {self.config.contract_id}", "INFO")
            self.logger.log(f"Quantity: {self.config.quantity}", "INFO")
            self.logger.log(f"Take Profit: {self.config.take_profit}%", "INFO")
            self.logger.log(f"Direction: {self.config.direction}", "INFO")
            self.logger.log(f"Max Orders: {self.config.max_orders}", "INFO")
            self.logger.log(f"Wait Time: {self.config.wait_time}s", "INFO")
            self.logger.log(f"Exchange: {self.config.exchange}", "INFO")
            self.logger.log(f"Grid Step: {self.config.grid_step}%", "INFO")
            self.logger.log(f"Stop Price: {self.config.stop_price}", "INFO")
            self.logger.log(f"Pause Price: {self.config.pause_price}", "INFO")
            self.logger.log(f"Boost Mode: {self.config.boost_mode}", "INFO")
            self.logger.log("=============================", "INFO")

            # Capture the running event loop for thread-safe callbacks
            self.loop = asyncio.get_running_loop()
            # Connect to exchange
            await self.exchange_client.connect()

            # wait for connection to establish
            await asyncio.sleep(5)

            if self.coordinator_enabled:
                await self._ensure_coordinator_registered()
                self._start_coordinator_tasks()

            # Main trading loop
            while not self.shutdown_requested:
                if self.coordinator_enabled and self._coordinator_mode == "PAUSE":
                    if not self._coordinator_pause_logged:
                        pause_reason = self._coordinator_reason or "协调机未提供原因"
                        self.logger.log(f"协调机暂停中，等待恢复（原因：{pause_reason}）", "WARNING")
                        self._coordinator_pause_logged = True
                    await asyncio.sleep(self._command_poll_interval)
                    continue
                self._coordinator_pause_logged = False

                # Update active orders and cached close order view
                active_orders = await self.exchange_client.get_active_orders(self.config.contract_id)
                try:
                    snapshot_position = await self.exchange_client.get_account_positions()
                except Exception:
                    snapshot_position = None
                self.active_close_orders, active_close_amount, _ = self._prepare_close_orders(
                    active_orders,
                    snapshot_position,
                )
                self._active_close_amount = active_close_amount

                # Periodic logging
                mismatch_detected = await self._log_status_periodically()

                stop_trading, pause_trading = await self._check_price_condition()
                if stop_trading:
                    msg = f"\n\nWARNING: [{self.config.exchange.upper()}_{self.config.ticker.upper()}] \n"
                    msg += "Stopped trading due to stop price triggered\n"
                    msg += "价格已经达到停止交易价格，脚本将停止交易\n"
                    await self.send_notification(msg.lstrip())
                    await self.graceful_shutdown(msg)
                    continue

                if pause_trading:
                    await asyncio.sleep(5)
                    continue

                if not mismatch_detected:
                    wait_time = self._calculate_wait_time()

                    if wait_time > 0:
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        meet_grid_step_condition = await self._meet_grid_step_condition()
                        if not meet_grid_step_condition:
                            await asyncio.sleep(1)
                            continue

                        await self._place_and_monitor_open_order()
                        self.last_close_orders += 1

        except KeyboardInterrupt:
            self.logger.log("Bot stopped by user")
            await self.graceful_shutdown("User interruption (Ctrl+C)")
        except Exception as e:
            self.logger.log(f"Critical error: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            await self.graceful_shutdown(f"Critical error: {e}")
            raise
        finally:
            try:
                await self._shutdown_coordinator()
            except Exception as e:
                self.logger.log(f"Error shutting down coordinator: {e}", "ERROR")

            # Ensure all connections are closed even if graceful shutdown fails
            try:
                await self.exchange_client.disconnect()
            except Exception as e:
                self.logger.log(f"Error disconnecting from exchange: {e}", "ERROR")
