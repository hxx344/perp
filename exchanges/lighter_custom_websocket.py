"""
Custom Lighter WebSocket implementation without using the official SDK.
Based on the sample code provided by the user.
"""

import asyncio
import json
import time
from typing import Dict, Any, List, Optional, Tuple, Callable
import websockets


class LighterCustomWebSocketManager:
    """Custom WebSocket manager for Lighter order updates and order book without SDK."""

    def __init__(self, config: Dict[str, Any], order_update_callback: Optional[Callable] = None):
        self.config = config
        self.order_update_callback = order_update_callback
        self.logger = None
        self.running = False
        self.ws = None
        self.ready_event = asyncio.Event()
        self._stop = False  # stop flag to gracefully exit connect loop

        # Order book state
        self.order_book = {"bids": {}, "asks": {}}
        self.best_bid = None
        self.best_ask = None
        self.snapshot_loaded = False
        self.order_book_offset = None
        self.order_book_sequence_gap = False
        self.order_book_lock = asyncio.Lock()
        # Hard caps and housekeeping
        self.max_levels = 50  # keep top-N levels per side to bound memory
        self._cleanup_every_messages = 200  # aggressive periodic trim
        self.last_account_orders_at: Optional[float] = None
        self.last_account_orders_entries: List[Dict[str, Any]] = []
        self.last_account_orders_client_ids: List[Any] = []

        # WebSocket URL
        self.ws_url = "wss://mainnet.zklighter.elliot.ai/stream"
        market_index = getattr(config, "market_index", None)
        self.market_index = market_index if market_index is not None else getattr(config, "contract_id", None)
        self.account_index = getattr(config, "account_index", None)
        self.lighter_client = getattr(config, "lighter_client", None)

    def set_logger(self, logger):
        """Set the logger instance."""
        self.logger = logger

    def _log(self, message: str, level: str = "INFO"):
        """Log message using the logger if available."""
        if self.logger:
            self.logger.log(message, level)

    def update_order_book(self, side: str, updates: List[Dict[str, Any]]):
        """Update the order book with new price/size information."""
        if side not in ["bids", "asks"]:
            self._log(f"Invalid side parameter: {side}. Must be 'bids' or 'asks'", "ERROR")
            return

        ob = self.order_book[side]

        if not isinstance(updates, list):
            self._log(f"Invalid updates format for {side}: expected list, got {type(updates)}", "ERROR")
            return

        for update in updates:
            try:
                if not isinstance(update, dict):
                    self._log(f"Invalid update format: expected dict, got {type(update)}", "ERROR")
                    continue

                if "price" not in update or "size" not in update:
                    self._log(f"Missing required fields in update: {update}", "ERROR")
                    continue

                price = float(update["price"])
                size = float(update["size"])

                # Validate price and size are reasonable
                if price <= 0:
                    self._log(f"Invalid price in update: {price}", "ERROR")
                    continue

                if size < 0:
                    self._log(f"Invalid size in update: {size}", "ERROR")
                    continue

                if size == 0:
                    # Remove level; if it was best, recompute lazily
                    removed = ob.pop(price, None)
                    if removed is not None:
                        if side == "bids" and self.best_bid is not None and price >= self.best_bid:
                            # recompute best bid
                            self.best_bid = max(ob.keys()) if ob else None
                        if side == "asks" and self.best_ask is not None and price <= self.best_ask:
                            # recompute best ask
                            self.best_ask = min(ob.keys()) if ob else None
                else:
                    # Upsert level and adjust best incrementally
                    ob[price] = size
                    if side == "bids":
                        if self.best_bid is None or price > self.best_bid:
                            self.best_bid = price
                    else:
                        if self.best_ask is None or price < self.best_ask:
                            self.best_ask = price
            except (KeyError, ValueError, TypeError) as e:
                self._log(f"Error processing order book update: {e}, update: {update}", "ERROR")
                continue

        # Hard cap: if levels explode between cleanups, trim immediately
        if len(ob) > self.max_levels * 2:
            self._trim_side_inplace(side)

    def validate_order_book_offset(self, new_offset: int) -> bool:
        """Validate that the new offset is sequential and handle gaps."""
        if self.order_book_offset is None:
            # First offset, always valid
            self.order_book_offset = new_offset
            return True

        # Check if the new offset is sequential (should be +1)
        expected_offset = self.order_book_offset + 1
        if new_offset == expected_offset:
            # Sequential update, update our offset
            self.order_book_offset = new_offset
            self.order_book_sequence_gap = False
            return True
        elif new_offset > expected_offset:
            # Gap detected - we missed some updates
            self._log(f"Order book sequence gap detected! Expected offset {expected_offset}, got {new_offset}", "WARNING")
            self.order_book_sequence_gap = True
            return False
        else:
            # Out of order or duplicate update
            self._log(f"Out of order update received! Expected offset {expected_offset}, got {new_offset}", "WARNING")
            return True  # Don't reconnect for out-of-order updates, just ignore them

    def handle_order_book_cutoff(self, data: Dict[str, Any]) -> bool:
        """Handle cases where order book updates might be cutoff or incomplete."""
        order_book = data.get("order_book", {})

        # Validate required fields
        if not order_book or "code" not in order_book or "offset" not in order_book:
            self._log("Incomplete order book update - missing required fields", "WARNING")
            return False

        # Check if the order book has the expected structure
        if "asks" not in order_book or "bids" not in order_book:
            self._log("Incomplete order book update - missing bids/asks", "WARNING")
            return False

        # Validate that asks and bids are lists
        if not isinstance(order_book["asks"], list) or not isinstance(order_book["bids"], list):
            self._log("Invalid order book structure - asks/bids should be lists", "WARNING")
            return False

        return True

    def validate_order_book_integrity(self) -> bool:
        """Validate that the order book is internally consistent."""
        try:
            if not self.order_book["bids"] or not self.order_book["asks"]:
                # Empty order book is valid
                return True

            # Get best bid and best ask
            best_bid = max(self.order_book["bids"].keys())
            best_ask = min(self.order_book["asks"].keys())

            # Check if best bid is higher than best ask (inconsistent)
            if best_bid >= best_ask:
                self._log(f"Order book inconsistency detected! Best bid: {best_bid}, Best ask: {best_ask}", "WARNING")
                return False

            return True
        except (ValueError, KeyError) as e:
            self._log(f"Error validating order book integrity: {e}", "ERROR")
            return False

    async def request_fresh_snapshot(self):
        """Request a fresh order book snapshot when we detect inconsistencies."""
        try:
            if not self.ws:
                return

            # Unsubscribe and resubscribe to get a fresh snapshot
            unsubscribe_msg = json.dumps({"type": "unsubscribe", "channel": f"order_book/{self.market_index}"})
            await self.ws.send(unsubscribe_msg)

            # Wait a moment for the unsubscribe to process
            await asyncio.sleep(1)

            # Resubscribe to get a fresh snapshot
            subscribe_msg = json.dumps({"type": "subscribe", "channel": f"order_book/{self.market_index}"})
            await self.ws.send(subscribe_msg)

            self._log("Requested fresh order book snapshot", "INFO")
        except Exception as e:
            self._log(f"Error requesting fresh snapshot: {e}", "ERROR")
            raise

    def get_best_levels(self) -> Tuple[Tuple[Optional[float], Optional[float]], Tuple[Optional[float], Optional[float]]]:
        """Get the best bid and ask levels using maintained best prices (O(1))."""
        try:
            bb = self.best_bid
            ba = self.best_ask
            bb_size = self.order_book["bids"].get(bb) if bb is not None else None
            ba_size = self.order_book["asks"].get(ba) if ba is not None else None
            return (bb, bb_size), (ba, ba_size)
        except Exception as e:
            self._log(f"Error getting best levels: {e}", "ERROR")
            return (None, None), (None, None)

    async def wait_until_ready(self, timeout: float = 5.0) -> bool:
        """Wait until both bid and ask levels are available."""
        if self.ready_event.is_set():
            return True

        try:
            await asyncio.wait_for(self.ready_event.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def _trim_side_inplace(self, side: str) -> None:
        """Trim a single side to max_levels keeping best prices."""
        try:
            ob = self.order_book[side]
            if len(ob) <= self.max_levels:
                return
            # Use nlargest/nsmallest to avoid full sort
            import heapq
            if side == "bids":
                keep_prices = set(heapq.nlargest(self.max_levels, ob.keys()))
            else:
                keep_prices = set(heapq.nsmallest(self.max_levels, ob.keys()))
            # Rebuild dict with only kept levels
            self.order_book[side] = {p: ob[p] for p in keep_prices}
            # Recompute bests for the side
            if side == "bids":
                self.best_bid = max(self.order_book["bids"].keys()) if self.order_book["bids"] else None
            else:
                self.best_ask = min(self.order_book["asks"].keys()) if self.order_book["asks"] else None
        except Exception as e:
            self._log(f"Error trimming order book side {side}: {e}", "ERROR")

    def cleanup_old_order_book_levels(self):
        """Clean up old order book levels to prevent memory bloat (bounded to max_levels)."""
        try:
            self._trim_side_inplace("bids")
            self._trim_side_inplace("asks")
        except Exception as e:
            self._log(f"Error cleaning up order book levels: {e}", "ERROR")

    async def reset_order_book(self):
        """Reset the order book state when reconnecting."""
        async with self.order_book_lock:
            self.order_book["bids"].clear()
            self.order_book["asks"].clear()
            self.snapshot_loaded = False
            self.best_bid = None
            self.best_ask = None
            self.order_book_offset = None
            self.order_book_sequence_gap = False
            self.ready_event.clear()

    def handle_order_update(self, order_data_list: List[Dict[str, Any]]):
        """Handle order update from WebSocket."""
        try:
            now = time.time()
            self.last_account_orders_at = now
            self.last_account_orders_entries = list(order_data_list or [])
            self.last_account_orders_client_ids = []
            for entry in order_data_list or []:
                if not isinstance(entry, dict):
                    continue
                client_idx = entry.get("client_order_index") or entry.get("clientOrderIndex")
                if client_idx is not None:
                    try:
                        normalized = int(str(client_idx))
                    except (TypeError, ValueError):
                        normalized = client_idx
                    self.last_account_orders_client_ids.append(normalized)

            if self.last_account_orders_client_ids:
                self._log(
                    (
                        "Account orders ids observed: "
                        f"{self.last_account_orders_client_ids}"
                    ),
                    "DEBUG",
                )

            # Call the order update callback if it exists
            if self.order_update_callback:
                self.order_update_callback(order_data_list)

        except Exception as e:
            self._log(f"Error handling order update: {e}", "ERROR")

    def _candidate_market_keys(self) -> List[Any]:
        """Return possible keys used by the stream for this market."""
        keys: List[Any] = []

        def _append_variants(value: Any) -> None:
            if value is None:
                return
            for variant in (value, str(value)):
                if variant not in keys:
                    keys.append(variant)
            # Try integer conversion when the original looked like a number
            try:
                numeric = int(str(value))
            except (TypeError, ValueError):
                return
            if numeric not in keys:
                keys.append(numeric)

        _append_variants(self.market_index)
        _append_variants(getattr(self.config, "market_index", None))
        _append_variants(getattr(self.config, "contract_id", None))

        return keys

    def _extract_orders_for_market(self, orders_payload: Any) -> List[Dict[str, Any]]:
        """Extract account order updates for the configured market."""
        keys_to_try = self._candidate_market_keys()

        if isinstance(orders_payload, dict):
            for key in keys_to_try:
                if key in orders_payload:
                    return orders_payload[key] or []

            # Fallback: compare via string forms without exact key match
            target = next((str(k) for k in keys_to_try if k is not None), None)
            if target is not None:
                for incoming_key, value in orders_payload.items():
                    try:
                        if str(incoming_key) == target:
                            return value or []
                    except Exception:
                        continue

            return []

        if isinstance(orders_payload, list):
            normalized_targets = {
                str(k): True for k in keys_to_try if k is not None
            }
            matched: List[Dict[str, Any]] = []
            for entry in orders_payload:
                if not isinstance(entry, dict):
                    continue
                market_candidate = entry.get("market_index") or entry.get("marketId")
                if market_candidate is None:
                    continue
                try:
                    market_key = str(market_candidate)
                except Exception:
                    continue
                if market_key in normalized_targets:
                    matched.append(entry)

            return matched

        return []

    async def connect(self):
        """Connect to Lighter WebSocket using custom implementation."""
        cleanup_counter = 0
        timeout_count = 0
        reconnect_delay = 1  # Start with 1 second delay
        max_reconnect_delay = 30  # Maximum delay of 30 seconds

        while not self._stop:
            try:
                # Reset order book state before connecting
                await self.reset_order_book()

                async with websockets.connect(self.ws_url) as self.ws:
                    # Subscribe to order book updates
                    await self.ws.send(json.dumps({
                        "type": "subscribe",
                        "channel": f"order_book/{self.market_index}"
                    }))

                    # Subscribe to account orders updates
                    account_orders_channel = f"account_orders/{self.market_index}/{self.account_index}"

                    # Get auth token for the subscription
                    try:
                        if self.lighter_client:
                            # Set auth token to expire in 10 minutes (duration, not absolute timestamp)
                            ten_minutes_duration = 10 * 60
                            auth_token, err = self.lighter_client.create_auth_token_with_expiry(ten_minutes_duration)
                            if err is not None:
                                self._log(f"Failed to create auth token for account orders subscription: {err}", "WARNING")
                            else:
                                auth_message = {
                                    "type": "subscribe",
                                    "channel": account_orders_channel,
                                    "auth": auth_token
                                }
                                await self.ws.send(json.dumps(auth_message))
                                self._log("Subscribed to account orders with auth token (expires in 10 minutes)", "INFO")
                    except Exception as e:
                        self._log(f"Error creating auth token for account orders subscription: {e}", "WARNING")

                    if self._stop:
                        # Stop requested after establishing connection
                        await self.ws.close()
                        break

                    self.running = True
                    # Reset reconnect delay on successful connection
                    reconnect_delay = 1
                    self._log("WebSocket connected using custom implementation", "INFO")

                    # Main message processing loop
                    while self.running:
                        try:
                            msg = await asyncio.wait_for(self.ws.recv(), timeout=1)

                            try:
                                data = json.loads(msg)
                            except json.JSONDecodeError as e:
                                self._log(f"JSON parsing error in Lighter websocket: {e}", "ERROR")
                                continue

                            # Reset timeout counter on successful message
                            timeout_count = 0

                            async with self.order_book_lock:
                                if data.get("type") == "subscribed/order_book":
                                    # Initial snapshot - clear and populate the order book
                                    self.order_book["bids"].clear()
                                    self.order_book["asks"].clear()

                                    # Handle the initial snapshot
                                    order_book = data.get("order_book", {})
                                    if order_book and "offset" in order_book:
                                        # Set the initial offset from the snapshot
                                        self.order_book_offset = order_book["offset"]
                                        self._log(f"Initial order book offset set to: {self.order_book_offset}", "INFO")

                                    self.update_order_book("bids", order_book.get("bids", []))
                                    self.update_order_book("asks", order_book.get("asks", []))
                                    self.snapshot_loaded = True

                                    self._log(f"Lighter order book snapshot loaded with "
                                              f"{len(self.order_book['bids'])} bids and "
                                              f"{len(self.order_book['asks'])} asks", "INFO")

                                    # Ensure best levels set from current dicts in O(logN)
                                    try:
                                        self.best_bid = max(self.order_book["bids"].keys()) if self.order_book["bids"] else None
                                        self.best_ask = min(self.order_book["asks"].keys()) if self.order_book["asks"] else None
                                    except Exception:
                                        self.best_bid = None
                                        self.best_ask = None
                                    if self.best_bid is not None and self.best_ask is not None:
                                        self.ready_event.set()

                                elif data.get("type") == "update/order_book" and self.snapshot_loaded:
                                    # Check for cutoff/incomplete updates first
                                    if not self.handle_order_book_cutoff(data):
                                        self._log("Skipping incomplete order book update", "WARNING")
                                        continue

                                    # Extract offset from the message
                                    order_book = data.get("order_book", {})
                                    if not order_book or "offset" not in order_book:
                                        self._log("Order book update missing offset, skipping", "WARNING")
                                        continue

                                    new_offset = order_book["offset"]

                                    # Validate offset sequence
                                    if not self.validate_order_book_offset(new_offset):
                                        # Sequence gap detected, try to request fresh snapshot first
                                        if self.order_book_sequence_gap:
                                            self._log("Sequence gap detected, requesting fresh snapshot...", "WARNING")
                                            # Release lock before network I/O
                                            break
                                        else:
                                            # For out-of-order updates, just continue
                                            continue

                                    # Update the order book with new data
                                    self.update_order_book("bids", order_book.get("bids", []))
                                    self.update_order_book("asks", order_book.get("asks", []))

                                    # Validate order book integrity after update
                                    if not self.validate_order_book_integrity():
                                        self._log("Order book integrity check failed, requesting fresh snapshot...", "WARNING")
                                        # Release lock before network I/O
                                        break

                                    # ready when both sides present
                                    if self.best_bid is not None and self.best_ask is not None:
                                        self.ready_event.set()

                                elif data.get("type") == "ping":
                                    # Respond to ping with pong
                                    await self.ws.send(json.dumps({"type": "pong"}))
                                elif data.get("type") == "update/account_orders":
                                    orders_payload = data.get("orders", {})
                                    orders = self._extract_orders_for_market(orders_payload)
                                    if not orders and orders_payload:
                                        self._log(
                                            "Received account_orders update but no entries matched configured market; "
                                            "check market_index/contract_id mapping",
                                            "DEBUG",
                                        )
                                    else:
                                        self._log(
                                            f"Account orders update matched {len(orders)} entries for market {self.market_index}",
                                            "DEBUG",
                                        )
                                    self.handle_order_update(orders)
                                elif data.get("type") == "update/order_book" and not self.snapshot_loaded:
                                    # Ignore updates until we have the initial snapshot
                                    continue
                                else:
                                    try:
                                        payload_preview = json.dumps(data, default=str)
                                    except Exception:
                                        payload_preview = str(data)
                                    self._log(
                                        (
                                            f"Unknown message type: {data.get('type', 'unknown')} "
                                            f"payload={payload_preview}"
                                        ),
                                        "DEBUG",
                                    )

                            # Periodic cleanup outside the lock
                            cleanup_counter += 1
                            if cleanup_counter >= self._cleanup_every_messages:  # frequent cleanup to bound memory
                                self.cleanup_old_order_book_levels()
                                cleanup_counter = 0

                            # Handle sequence gap and integrity issues outside the lock
                            if self.order_book_sequence_gap:
                                try:
                                    await self.request_fresh_snapshot()
                                    self.order_book_sequence_gap = False
                                except Exception as e:
                                    self._log(f"Failed to request fresh snapshot: {e}", "ERROR")
                                    self._log("Reconnecting due to sequence gap...", "WARNING")
                                    break

                        except asyncio.TimeoutError:
                            timeout_count += 1
                            if timeout_count % 30 == 0:
                                self._log(f"No message from Lighter websocket for {timeout_count} seconds "
                                          f"(abnormal behavior)", "WARNING")
                            continue
                        except websockets.exceptions.ConnectionClosed as e:
                            self._log(f"Lighter websocket connection closed: {e}", "WARNING")
                            self._log("Connection lost, will attempt to reconnect...", "INFO")
                            break  # Break inner loop to reconnect
                        except websockets.exceptions.WebSocketException as e:
                            self._log(f"Lighter websocket error: {e}", "ERROR")
                            break  # Break inner loop to reconnect
                        except Exception as e:
                            self._log(f"Error in Lighter websocket: {e}", "ERROR")
                            import traceback
                            self._log(f"Full traceback: {traceback.format_exc()}", "ERROR")
                            break  # Break inner loop to reconnect

            except Exception as e:
                self._log(f"Failed to connect to Lighter websocket: {e}", "ERROR")

            # Wait before reconnecting with exponential backoff
            if self.running and not self._stop:
                self._log(f"Waiting {reconnect_delay} seconds before reconnecting...", "INFO")
                await asyncio.sleep(reconnect_delay)
                # Exponential backoff: double the delay, but cap at max_reconnect_delay
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

        self._log("Connect loop exited", "INFO")

    async def disconnect(self):
        """Disconnect from WebSocket."""
        self._stop = True
        self.running = False
        if self.ws:
            try:
                await self.ws.close()
            except Exception as e:
                self._log(f"Error closing websocket: {e}", "ERROR")
        self._log("WebSocket disconnected", "INFO")
