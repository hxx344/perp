"""
Lighter exchange client implementation.
"""

import os
import json
import asyncio
import time
import logging
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, ROUND_UP
from typing import Dict, Any, List, Optional, Tuple, Iterable

from .base import BaseExchangeClient, OrderResult, OrderInfo, query_retry
from helpers.logger import TradingLogger

# Import official Lighter SDK for API client
from lighter.signer_client import SignerClient
from lighter.api_client import ApiClient
from lighter.configuration import Configuration
from lighter.api.account_api import AccountApi
from lighter.api.order_api import OrderApi

# Import custom WebSocket implementation
from .lighter_custom_websocket import LighterCustomWebSocketManager

# Suppress Lighter SDK debug logs
logging.getLogger('lighter').setLevel(logging.WARNING)
logging.getLogger('lighter.api_client').setLevel(logging.WARNING)
logging.getLogger('lighter.account_api').setLevel(logging.WARNING)
logging.getLogger('lighter.signer_client').setLevel(logging.WARNING)
logging.getLogger('lighter.ws_client').setLevel(logging.WARNING)

# Also suppress root logger DEBUG messages that might be coming from Lighter SDK
root_logger = logging.getLogger()
if root_logger.level == logging.DEBUG:
    root_logger.setLevel(logging.INFO)


DEFAULT_LIGHTER_BASE_URL = "https://mainnet.zklighter.elliot.ai"
DEFAULT_SPOT_MARKET_ID = "2048"


class LighterClient(BaseExchangeClient):
    """Lighter exchange client implementation."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize Lighter client."""
        super().__init__(config)

        # Lighter credentials from environment
        account_index_raw = os.getenv('LIGHTER_ACCOUNT_INDEX', '0')
        try:
            self.account_index = int(account_index_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Invalid LIGHTER_ACCOUNT_INDEX '{account_index_raw}'"
            ) from exc

        self.api_private_keys = self._load_api_private_keys()
        base_url_env = (os.getenv('LIGHTER_BASE_URL') or DEFAULT_LIGHTER_BASE_URL).strip()
        self.base_url = base_url_env or DEFAULT_LIGHTER_BASE_URL

        # Initialize logger
        self.logger = TradingLogger(exchange="lighter", ticker=self.config.ticker, log_to_console=False)
        self._order_update_handler = None

        # Initialize Lighter client (will be done in connect)
        self.lighter_client = None

        # Initialize API client (will be done in connect)
        self.api_client = None

        # Websocket manager placeholder (started after contract metadata is loaded)
        self.ws_manager: Optional[LighterCustomWebSocketManager] = None

        # Market configuration
        self.base_amount_multiplier = None
        self.price_multiplier = None
        self.orders_cache = {}
        self.current_order_client_id = None
        self.current_order = None
        self.market_detail = None
        self.market_index: Optional[int] = None
        self.market_type = getattr(self.config, "market_type", None)
        self.base_asset_id = getattr(self.config, "base_asset_id", None)
        self.quote_asset_id = getattr(self.config, "quote_asset_id", None)
        self.base_asset_symbol = getattr(self.config, "base_asset_symbol", None)
        self.default_initial_margin_fraction: Optional[int] = None
        self.min_initial_margin_fraction: Optional[int] = None
        self.default_leverage: Optional[int] = None
        self.max_leverage: Optional[int] = None
        self._last_confirmed_tier_name: Optional[str] = None
        self.spot_min_base_amount: Optional[Decimal] = None
        self.spot_min_quote_amount: Optional[Decimal] = None
        debug_flag = os.getenv("LIGHTER_DEBUG_ORDERS") or ""
        self._debug_orders: bool = debug_flag.strip().lower() in {"1", "true", "yes", "on"}

    def _require_lighter_client(self) -> SignerClient:
        client = self.lighter_client
        if client is None:
            raise RuntimeError("Lighter client not initialized")
        return client

    def prune_caches(self, *, max_orders: int = 1000) -> None:
        """Bound in-memory caches to avoid unbounded growth.

        Currently trims orders_cache to at most `max_orders` entries.
        """
        try:
            cache = self.orders_cache
            if not isinstance(cache, dict):
                return
            if len(cache) > max_orders:
                # Keep latest N by insertion order where possible (Py3.7+ dict preserves insertion order)
                excess = len(cache) - max_orders
                for k in list(cache.keys())[:excess]:
                    cache.pop(k, None)
        except Exception:
            pass

    @staticmethod
    def _fraction_to_leverage(fraction: Any) -> Optional[int]:
        try:
            fraction_int = int(fraction)
        except (TypeError, ValueError):
            return None

        if fraction_int <= 0:
            return None

        try:
            leverage = int(Decimal("10000") / Decimal(fraction_int))
        except (InvalidOperation, ValueError):
            return None

        return leverage if leverage > 0 else 1

    def get_leverage_limits(self) -> Dict[str, Optional[int]]:
        """Return default and maximum leverage derived from market risk parameters."""

        return {
            "default": self.default_leverage,
            "max": self.max_leverage,
            "default_initial_margin_fraction": self.default_initial_margin_fraction,
            "min_initial_margin_fraction": self.min_initial_margin_fraction,
        }

    @staticmethod
    def _parse_decimal(value: Any) -> Optional[Decimal]:
        if value is None:
            return None
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return None

    def _decimal_or_zero(self, value: Any) -> Decimal:
        parsed = self._parse_decimal(value)
        return parsed if parsed is not None else Decimal("0")

    def _resolve_market_index(self) -> int:
        if self.market_index is not None:
            return self.market_index

        contract_identifier = getattr(self.config, "contract_id", None)
        if contract_identifier in (None, ""):
            raise ValueError("Lighter market index unavailable: contract id not initialized")

        try:
            index = int(contract_identifier)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid Lighter contract id '{contract_identifier}'") from exc

        self.market_index = index
        return index

    def _first_available_decimal(self, payload: Dict[str, Any], keys: Iterable[str]) -> Optional[Decimal]:
        for key in keys:
            if key in payload:
                parsed = self._parse_decimal(payload.get(key))
                if parsed is not None:
                    return parsed
        return None

    @staticmethod
    def _possible_keys(base_key: str) -> List[str]:
        variants = {base_key, base_key.lower(), base_key.upper(), base_key.replace("_", "")}
        if "_" in base_key:
            parts = base_key.split("_")
            camel = parts[0] + "".join(part.capitalize() for part in parts[1:])
            pascal = "".join(part.capitalize() for part in parts)
            variants.update({camel, pascal})
        return list(variants)

    def _extract_mapping_value(self, payload: Any, key: str) -> Any:
        if payload is None:
            return None
        if isinstance(payload, dict):
            for candidate in self._possible_keys(key):
                if candidate in payload and payload[candidate] is not None:
                    return payload[candidate]
            return None
        if hasattr(payload, key):
            value = getattr(payload, key)
            if value is not None:
                return value
        camel_key = "".join(part.capitalize() for part in key.split("_"))
        if camel_key and hasattr(payload, camel_key):
            value = getattr(payload, camel_key)
            if value is not None:
                return value
        if hasattr(payload, "to_dict"):
            try:
                as_dict = payload.to_dict()
            except Exception:
                as_dict = None
            if isinstance(as_dict, dict):
                return self._extract_mapping_value(as_dict, key)
        if hasattr(payload, "additional_properties"):
            extra = getattr(payload, "additional_properties", None)
            if isinstance(extra, dict):
                return self._extract_mapping_value(extra, key)
        return None

    def _is_spot_market(self) -> bool:
        market_type = self.market_type or getattr(self.config, "market_type", None)
        if isinstance(market_type, str):
            return market_type.lower() == "spot"
        return False

    def _resolve_spot_market_id(self) -> str:
        override = getattr(self.config, "spot_market_id", None)
        if not override:
            override = os.getenv("LIGHTER_SPOT_MARKET_ID")

        if override is None:
            return DEFAULT_SPOT_MARKET_ID

        if isinstance(override, (int, float)):
            try:
                return str(int(override))
            except (TypeError, ValueError):
                pass

        override_text = str(override).strip()
        return override_text or DEFAULT_SPOT_MARKET_ID

    def _extract_spot_balance(self, account: Any) -> Decimal:
        assets = getattr(account, "assets", None)
        if assets is None:
            assets = getattr(account, "spot_assets", None)

        if not assets:
            return Decimal("0")

        base_id = self.base_asset_id
        base_symbol = (self.base_asset_symbol or "").upper()

        for asset in assets:
            asset_id = self._extract_mapping_value(asset, "asset_id")
            if base_id is not None:
                try:
                    if int(asset_id) != int(base_id):
                        continue
                except (TypeError, ValueError):
                    continue
            elif base_symbol:
                symbol_value = self._extract_mapping_value(asset, "symbol")
                if not isinstance(symbol_value, str) or symbol_value.upper() != base_symbol:
                    continue
            elif asset_id is None:
                continue

            return self._decimal_or_zero(self._extract_mapping_value(asset, "balance"))

        return Decimal("0")

    def _spot_size_step(self) -> Optional[Decimal]:
        multiplier = self.base_amount_multiplier
        try:
            if multiplier:
                multiplier_dec = Decimal(multiplier)
                if multiplier_dec > 0:
                    return Decimal("1") / multiplier_dec
        except (InvalidOperation, ValueError, TypeError):
            return None
        return None

    @staticmethod
    def _round_to_step(value: Decimal, step: Decimal, *, rounding=ROUND_HALF_UP) -> Decimal:
        if step <= 0:
            return value
        try:
            units = (value / step).to_integral_value(rounding=rounding)
        except (InvalidOperation, ValueError):
            return value
        return units * step

    def _apply_spot_trade_constraints(self, quantity: Decimal, price: Decimal) -> Decimal:
        if not self._is_spot_market():
            return quantity

        detail = self.market_detail
        if detail is None or quantity <= 0:
            return quantity

        result = quantity
        size_step = self._spot_size_step()
        if size_step is not None and size_step > 0:
            result = self._round_to_step(result, size_step)

        min_base = self.spot_min_base_amount
        min_quote = self.spot_min_quote_amount

        if min_base is not None and min_base > 0 and result < min_base:
            result = min_base
            if size_step is not None and size_step > 0:
                result = self._round_to_step(result, size_step, rounding=ROUND_UP)

        if (
            min_quote is not None
            and min_quote > 0
            and price is not None
            and price > 0
        ):
            min_qty_for_quote = min_quote / price
            if min_qty_for_quote > result:
                result = min_qty_for_quote
                if size_step is not None and size_step > 0:
                    result = self._round_to_step(result, size_step, rounding=ROUND_UP)

        return result

    def _resolve_order_price(self, order_data: Dict[str, Any], filled_size: Decimal) -> Decimal:
        average_price = self._first_available_decimal(
            order_data,
            [
                "average_price",
                "average_filled_price",
                "avg_price",
                "avg_filled_price",
                "avgFilledPrice",
                "executed_price",
                "executedPrice",
                "fill_price",
                "filled_price",
                "filledPrice",
            ],
        )
        if average_price is not None and average_price > 0:
            return average_price

        filled_quote = self._first_available_decimal(
            order_data,
            [
                "filled_quote_amount",
                "filledQuoteAmount",
                "filled_quote",
                "filledQuote",
                "filled_value",
                "filledValue",
                "filled_quote_volume",
                "filledQuoteVolume",
            ],
        )

        if filled_quote is not None and filled_size not in (None, Decimal("0")):
            try:
                return filled_quote / filled_size
            except (InvalidOperation, ZeroDivisionError):
                pass

        fallback_price = self._parse_decimal(order_data.get("price"))
        if fallback_price is not None:
            return fallback_price

        return Decimal("0")

    @staticmethod
    def _parse_api_private_key_spec(raw_value: Optional[str]) -> Dict[int, str]:
        parsed: Dict[int, str] = {}
        if not raw_value:
            return parsed

        text = raw_value.strip()
        if not text:
            return parsed

        try:
            candidate = json.loads(text)
        except json.JSONDecodeError:
            candidate = None

        if isinstance(candidate, dict):
            for key, value in candidate.items():
                try:
                    index = int(key)
                except (TypeError, ValueError):
                    continue
                key_text = str(value).strip() if value is not None else ""
                if key_text:
                    parsed[index] = key_text
            if parsed:
                return parsed

        if isinstance(candidate, list):
            for entry in candidate:
                if not isinstance(entry, dict):
                    continue
                idx_value = entry.get("index") or entry.get("apiKeyIndex") or entry.get("api_key_index")
                if idx_value is None:
                    continue
                try:
                    index = int(idx_value)
                except (TypeError, ValueError):
                    continue
                key_text = entry.get("privateKey") or entry.get("private_key") or entry.get("key")
                if isinstance(key_text, str) and key_text.strip():
                    parsed[index] = key_text.strip()
            if parsed:
                return parsed

        segments: List[str]
        if any(sep in text for sep in (";", "|")):
            for delimiter in (";", "|"):
                if delimiter in text:
                    segments = [part for part in text.split(delimiter) if part]
                    break
        else:
            segments = [part for part in text.split(",") if part]

        for segment in segments:
            item = segment.strip()
            if not item or ":" not in item:
                continue
            idx_part, key_part = item.split(":", 1)
            try:
                index = int(idx_part.strip())
            except (TypeError, ValueError):
                continue
            key_text = key_part.strip()
            if key_text:
                parsed[index] = key_text

        return parsed

    def _load_api_private_keys(self) -> Dict[int, str]:
        env_payload = os.getenv('LIGHTER_API_PRIVATE_KEYS') or os.getenv('API_KEY_PRIVATE_KEYS')
        key_map = self._parse_api_private_key_spec(env_payload)

        if not key_map:
            legacy_key = (os.getenv('API_KEY_PRIVATE_KEY') or '').strip()
            legacy_index = (os.getenv('LIGHTER_API_KEY_INDEX') or '').strip()
            if legacy_key and legacy_index:
                try:
                    key_map[int(legacy_index)] = legacy_key
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        f"Invalid LIGHTER_API_KEY_INDEX '{legacy_index}'"
                    ) from exc

        if not key_map:
            raise ValueError(
                "Missing Lighter API credentials. Set LIGHTER_API_PRIVATE_KEYS (JSON or 'index:key' pairs) "
                "or API_KEY_PRIVATE_KEY + LIGHTER_API_KEY_INDEX."
            )

        return key_map

    def _validate_config(self) -> None:
        """Validate Lighter configuration."""
        if not os.getenv('LIGHTER_ACCOUNT_INDEX'):
            raise ValueError("LIGHTER_ACCOUNT_INDEX must be set in environment variables")

        has_multi = bool((os.getenv('LIGHTER_API_PRIVATE_KEYS') or os.getenv('API_KEY_PRIVATE_KEYS')))
        has_legacy = bool(os.getenv('API_KEY_PRIVATE_KEY') and os.getenv('LIGHTER_API_KEY_INDEX'))

        if not (has_multi or has_legacy):
            raise ValueError(
                "Missing Lighter API credentials. Provide LIGHTER_API_PRIVATE_KEYS or API_KEY_PRIVATE_KEY + "
                "LIGHTER_API_KEY_INDEX."
            )

    async def _get_market_config(self, ticker: str) -> Tuple[int, int, int]:
        """Get market configuration for a ticker using official SDK."""
        try:
            # Use shared API client
            order_api = OrderApi(self.api_client)

            # Get order books to find market info
            order_books = await order_api.order_books()

            for market in order_books.order_books:
                if market.symbol == ticker:
                    market_id = market.market_id
                    base_multiplier = pow(10, market.supported_size_decimals)
                    price_multiplier = pow(10, market.supported_price_decimals)

                    # Store market info and asset metadata for later use
                    self.config.market_info = market

                    market_type_value = getattr(market, "market_type", None)
                    if isinstance(market_type_value, str):
                        self.market_type = market_type_value.lower()

                    self.base_asset_id = getattr(market, "base_asset_id", None)
                    self.quote_asset_id = getattr(market, "quote_asset_id", None)

                    symbol_value = getattr(market, "symbol", None)
                    if isinstance(symbol_value, str) and symbol_value:
                        symbol_text = symbol_value.strip()
                        base_symbol = symbol_text
                        if "-" in symbol_text:
                            base_symbol = symbol_text.split("-", 1)[0]
                        elif symbol_text.endswith("PERP"):
                            base_symbol = symbol_text[:-4]
                        elif symbol_text.endswith("-PERP"):
                            base_symbol = symbol_text[:-5]
                        normalized_symbol = base_symbol.strip().upper()
                        if normalized_symbol:
                            self.base_asset_symbol = normalized_symbol

                    if self.base_asset_symbol:
                        setattr(self.config, "base_asset_symbol", self.base_asset_symbol)
                    if self.base_asset_id is not None:
                        setattr(self.config, "base_asset_id", self.base_asset_id)
                    if self.quote_asset_id is not None:
                        setattr(self.config, "quote_asset_id", self.quote_asset_id)
                    if self.market_type:
                        setattr(self.config, "market_type", self.market_type)

                    self.logger.log(
                        f"Market config for {ticker}: ID={market_id}, "
                        f"Base multiplier={base_multiplier}, Price multiplier={price_multiplier}",
                        "INFO"
                    )
                    return market_id, base_multiplier, price_multiplier

            raise Exception(f"Ticker {ticker} not found in available markets")

        except Exception as e:
            self.logger.log(f"Error getting market config: {e}", "ERROR")
            raise

    async def _initialize_lighter_client(self):
        """Initialize the Lighter client using official SDK."""
        if self.lighter_client is None:
            try:
                self.lighter_client = SignerClient(
                    url=self.base_url,
                    account_index=self.account_index,
                    api_private_keys=self.api_private_keys,
                )

                # Check client
                err = self.lighter_client.check_client()
                if err is not None:
                    raise Exception(f"CheckClient error: {err}")

                self.logger.log("Lighter client initialized successfully", "INFO")
            except Exception as e:
                self.logger.log(f"Failed to initialize Lighter client: {e}", "ERROR")
                raise
        return self.lighter_client

    async def connect(self) -> None:
        """Connect to Lighter."""
        try:
            # Initialize shared API client
            self.api_client = ApiClient(configuration=Configuration(host=self.base_url))

            # Initialize Lighter client
            await self._initialize_lighter_client()

        except Exception as e:
            self.logger.log(f"Error connecting to Lighter: {e}", "ERROR")
            raise

    async def ensure_account_tier(
        self,
        target_tier: str = "premium",
        *,
        target_tier_id: Optional[int] = None,
    ) -> bool:
        """Ensure the configured Lighter account is upgraded to the desired tier."""

        def _emit(level: str, message: str) -> None:
            self.logger.log(message, level)

        normalized_tier = (target_tier or "").strip()
        canonical_target = normalized_tier.casefold()
        if not normalized_tier:
            _emit(
                "WARNING",
                "Skipped Lighter tier enforcement because no target tier was provided",
            )
            return False

        cached_tier = getattr(self, "_last_confirmed_tier_name", None)
        if cached_tier and cached_tier.casefold() == canonical_target:
            _emit(
                "INFO",
                f"Skipping Lighter tier enforcement; cached tier '{cached_tier}' already matches target '{normalized_tier}'",
            )
            return True

        if self.api_client is None:
            _emit(
                "WARNING",
                "Cannot enforce Lighter account tier: API client not initialized",
            )
            return False

        if self.lighter_client is None:
            await self._initialize_lighter_client()

        auth_token: Optional[str] = None
        token_error: Optional[str] = None
        signer_client = getattr(self, "lighter_client", None)
        if signer_client is not None and hasattr(signer_client, "create_auth_token_with_expiry"):
            try:
                auth_token, token_error = signer_client.create_auth_token_with_expiry()
            except Exception as exc:  # pragma: no cover - defensive
                token_error = str(exc)
        else:
            token_error = "Signer client missing auth token generator"

        if token_error:
            _emit(
                "WARNING",
                f"Lighter auth token generation warning: {token_error}",
            )

        auth_token = (auth_token or "").strip() or None
        if not auth_token:
            _emit(
                "ERROR",
                f"Unable to generate Lighter auth token for tier change: {token_error or 'unknown error'}",
            )
            return False

        try:
            account_api = AccountApi(self.api_client)
        except Exception as exc:  # pragma: no cover - SDK construction failure
            _emit(
                "ERROR",
                f"Failed to construct Lighter AccountApi for tier enforcement: {exc}",
            )
            return False

        current_account_type: Optional[int] = None
        current_account_tier_name: Optional[str] = None
        try:
            account_snapshot = await account_api.account(by="index", value=str(self.account_index))
        except Exception as exc:
            _emit(
                "WARNING",
                f"Unable to query Lighter account details before tier change: {exc}",
            )
            account_snapshot = None
        else:
            accounts = getattr(account_snapshot, "accounts", None)
            if isinstance(accounts, list):
                for entry in accounts:
                    account_idx = getattr(entry, "account_index", getattr(entry, "index", None))
                    if account_idx == self.account_index:
                        current_account_type = getattr(entry, "account_type", None)
                        extras = getattr(entry, "additional_properties", None)
                        if isinstance(extras, dict):
                            for key, value in extras.items():
                                if isinstance(key, str) and "tier" in key.lower() and isinstance(value, str):
                                    stripped = value.strip()
                                    if stripped:
                                        current_account_tier_name = stripped
                                        break
                        if current_account_tier_name:
                            break
                        break

        if current_account_tier_name is None:
            try:
                limits_snapshot = await account_api.account_limits(
                    self.account_index,
                    authorization=auth_token,
                    auth=auth_token,
                )
            except Exception as exc:
                _emit(
                    "WARNING",
                    f"Unable to fetch Lighter account limits for tier lookup: {exc}",
                )
            else:
                tier_candidate = getattr(limits_snapshot, "user_tier", None)
                if isinstance(tier_candidate, str) and tier_candidate.strip():
                    current_account_tier_name = tier_candidate.strip()

        if current_account_tier_name and current_account_tier_name.casefold() == canonical_target:
            self._last_confirmed_tier_name = current_account_tier_name
            _emit(
                "INFO",
                f"Lighter account {self.account_index} already at tier '{current_account_tier_name}'; skipping change request",
            )
            return True

        if target_tier_id is not None and current_account_type == target_tier_id:
            _emit(
                "INFO",
                f"Lighter account {self.account_index} already at tier id {target_tier_id}",
            )
            self._last_confirmed_tier_name = normalized_tier
            return True

        _emit(
            "INFO",
            f"Requesting tier '{normalized_tier}' for Lighter account {self.account_index}"
            + (f" (expected id {target_tier_id})" if target_tier_id is not None else ""),
        )

        try:
            response = await account_api.change_account_tier(
                self.account_index,
                normalized_tier,
                authorization=auth_token,
                auth=auth_token,
            )
        except Exception as exc:
            _emit(
                "ERROR",
                f"Failed to change Lighter account tier to {normalized_tier}: {exc}",
            )
            return False

        response_code = getattr(response, "code", None)
        response_message = getattr(response, "message", "")
        success_codes = {0, 1, 200, 62003, None}
        success = response_code in success_codes
        message_suffix = f" ({response_message})" if response_message else ""
        if response_code == 62003:
            _emit(
                "INFO",
                f"Tier change request skipped: account already in '{normalized_tier}'{message_suffix}",
            )
            self._last_confirmed_tier_name = normalized_tier
            return True
        if success:
            _emit(
                "INFO",
                f"Tier change request for account {self.account_index} acknowledged with code {response_code}{message_suffix}",
            )
            self._last_confirmed_tier_name = normalized_tier
        else:
            _emit(
                "WARNING",
                f"Tier change request for account {self.account_index} returned code {response_code}{message_suffix}",
            )

        new_account_type: Optional[int] = None
        try:
            refreshed_snapshot = await account_api.account(by="index", value=str(self.account_index))
        except Exception as exc:
            _emit(
                "WARNING",
                f"Unable to confirm Lighter tier change after request: {exc}",
            )
            refreshed_snapshot = None
        else:
            refreshed_accounts = getattr(refreshed_snapshot, "accounts", None)
            if isinstance(refreshed_accounts, list):
                for entry in refreshed_accounts:
                    account_idx = getattr(entry, "account_index", getattr(entry, "index", None))
                    if account_idx == self.account_index:
                        new_account_type = getattr(entry, "account_type", None)
                        break

        if new_account_type is not None:
            _emit(
                "INFO",
                f"Lighter account {self.account_index} tier status: before={current_account_type} -> after={new_account_type}",
            )
            if target_tier_id is None:
                self._last_confirmed_tier_name = normalized_tier

        if target_tier_id is not None and new_account_type is not None:
            if new_account_type != target_tier_id:
                _emit(
                    "WARNING",
                    f"Expected tier id {target_tier_id} but observed {new_account_type} after change request",
                )
                return False
            return success

        if new_account_type is not None and current_account_type is not None and new_account_type == current_account_type:
            _emit(
                "WARNING",
                "Lighter account tier unchanged after request; verify permissions or target tier",
            )
            return success

        if success:
            self._last_confirmed_tier_name = normalized_tier

        return success

    async def wait_for_market_data(self, timeout: float = 5.0) -> bool:
        """Block until the websocket provides bid/ask data or timeout."""
        if hasattr(self, 'ws_manager') and self.ws_manager:
            return await self.ws_manager.wait_until_ready(timeout)
        return False

    async def _ensure_websocket_manager(self) -> None:
        """Start or update the websocket manager after contract metadata is known."""
        created = False
        market_index = None
        try:
            market_index = self._resolve_market_index()
        except ValueError:
            market_index = None

        if self.ws_manager is None:
            if market_index is not None:
                setattr(self.config, "market_index", market_index)
            else:
                setattr(self.config, "market_index", getattr(self.config, "contract_id", None))
            setattr(self.config, "account_index", self.account_index)
            setattr(self.config, "lighter_client", self.lighter_client)

            self.ws_manager = LighterCustomWebSocketManager(
                config=self.config,
                order_update_callback=self._handle_websocket_order_update,
            )
            self.ws_manager.set_logger(self.logger)
            asyncio.create_task(self.ws_manager.connect())
            created = True
        else:
            if market_index is not None:
                self.ws_manager.market_index = market_index
            self.ws_manager.account_index = self.account_index
            self.ws_manager.lighter_client = self.lighter_client

        if created:
            if not await self.wait_for_market_data(timeout=10):
                self.logger.log(
                    "Timed out waiting for Lighter order book data to become available",
                    "WARNING",
                )

    async def disconnect(self) -> None:
        """Disconnect from Lighter."""
        try:
            if hasattr(self, 'ws_manager') and self.ws_manager:
                await self.ws_manager.disconnect()
                self.ws_manager = None

            # Close shared API client
            if self.api_client:
                await self.api_client.close()
                self.api_client = None
        except Exception as e:
            self.logger.log(f"Error during Lighter disconnect: {e}", "ERROR")

    def get_exchange_name(self) -> str:
        """Get the exchange name."""
        return "lighter"

    def setup_order_update_handler(self, handler) -> None:
        """Setup order update handler for WebSocket."""
        self._order_update_handler = handler

    def _handle_websocket_order_update(self, order_data_list: List[Dict[str, Any]]):
        """Handle order updates from WebSocket."""
        for order_data in order_data_list:
            if str(order_data.get('market_index')) != str(getattr(self.config, "contract_id", None)):
                continue

            side = 'sell' if order_data['is_ask'] else 'buy'
            if side == getattr(self.config, "close_order_side", side):
                order_type = "CLOSE"
            else:
                order_type = "OPEN"

            order_id = order_data['order_index']
            status = order_data['status'].upper()
            filled_size = self._decimal_or_zero(order_data.get('filled_base_amount'))
            size = self._decimal_or_zero(order_data.get('initial_base_amount'))
            remaining_size = self._decimal_or_zero(order_data.get('remaining_base_amount'))
            price = self._resolve_order_price(order_data, filled_size)

            if order_id in self.orders_cache.keys():
                if (self.orders_cache[order_id]['status'] == 'OPEN' and
                        status == 'OPEN' and
                        filled_size == self.orders_cache[order_id]['filled_size']):
                    continue
                elif status in ['FILLED', 'CANCELED']:
                    del self.orders_cache[order_id]
                else:
                    self.orders_cache[order_id]['status'] = status
                    self.orders_cache[order_id]['filled_size'] = filled_size
            elif status == 'OPEN':
                self.orders_cache[order_id] = {'status': status, 'filled_size': filled_size}

            if status == 'OPEN' and filled_size > 0:
                status = 'PARTIALLY_FILLED'

            if status == 'OPEN':
                self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                f"{size} @ {price}", "INFO")
            else:
                self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                f"{filled_size} @ {price}", "INFO")

            if self._order_update_handler is not None:
                update_payload = {
                    "contract_id": getattr(self.config, "contract_id", None),
                    "ticker": getattr(self.config, "ticker", None),
                    "order_id": str(order_id),
                    "status": status,
                    "side": side,
                    "order_type": order_type,
                    "price": str(price),
                    "size": str(size),
                    "filled_size": str(filled_size),
                    "remaining_size": str(remaining_size),
                    "timestamp": str(time.time()),
                    "client_order_index": str(order_data.get('client_order_index')),
                }
                try:
                    self._order_update_handler(update_payload)
                except Exception as exc:  # pragma: no cover - logging safeguard
                    self.logger.log(f"Error invoking external order handler: {exc}", "ERROR")

            if order_data['client_order_index'] == self.current_order_client_id or order_type == 'OPEN':
                current_order = OrderInfo(
                    order_id=order_id,
                    side=side,
                    size=size,
                    price=price,
                    status=status,
                    filled_size=filled_size,
                    remaining_size=remaining_size,
                    cancel_reason=''
                )
                self.current_order = current_order

            if status in ['FILLED', 'CANCELED']:
                self.logger.log_transaction(order_id, side, filled_size, price, status)

    @query_retry(default_return=(0, 0))
    async def fetch_bbo_prices(self, contract_id: str) -> Tuple[Decimal, Decimal]:
        """Get orderbook using official SDK."""
        # Use WebSocket data if available
        if not hasattr(self, 'ws_manager'):
            raise ValueError("WebSocket not running. No bid/ask prices available")

        for _ in range(20):
            best_bid_raw = getattr(self.ws_manager, 'best_bid', None)
            best_ask_raw = getattr(self.ws_manager, 'best_ask', None)
            if best_bid_raw and best_ask_raw:
                best_bid = Decimal(str(best_bid_raw))
                best_ask = Decimal(str(best_ask_raw))

                if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
                    self.logger.log("Invalid bid/ask prices", "ERROR")
                    raise ValueError("Invalid bid/ask prices")
                return best_bid, best_ask
            await asyncio.sleep(0.25)

        self.logger.log(
            "Unable to get bid/ask prices from WebSocket within the expected time window.",
            "ERROR"
        )
        raise ValueError("WebSocket not running. No bid/ask prices available")

    async def _submit_order_with_retry(self, order_params: Dict[str, Any]) -> OrderResult:
        """Submit an order with Lighter using official SDK."""
        # Ensure client is initialized
        if self.lighter_client is None:
            # This is a sync method, so we need to handle this differently
            # For now, raise an error if client is not initialized
            raise ValueError("Lighter client not initialized. Call connect() first.")

        # Create order using official SDK
        lighter_client = self._require_lighter_client()
        if self._debug_orders:
            self.logger.log(
                f"Lighter order params: {order_params}",
                "INFO",
            )
        create_order, tx_hash, error = await lighter_client.create_order(**order_params)
        if error is not None:
            if self._debug_orders:
                self.logger.log(
                    f"Lighter order error -> tx_hash={tx_hash}, error={error}",
                    "ERROR",
                )
            return OrderResult(
                success=False, order_id=str(order_params['client_order_index']),
                error_message=f"Order creation error: {error}")

        else:
            return OrderResult(success=True, order_id=str(order_params['client_order_index']))

    async def place_limit_order(self, contract_id: str, quantity: Decimal, price: Decimal,
                                side: str) -> OrderResult:
        """Place a post only order with Lighter using official SDK."""
        # Ensure client is initialized
        if self.lighter_client is None:
            await self._initialize_lighter_client()
        lighter_client = self._require_lighter_client()

        market_index = self._resolve_market_index()

        if self.base_amount_multiplier is None or self.price_multiplier is None:
            raise ValueError("Lighter market metadata not initialized; call get_contract_attributes() before placing orders")

        # Determine order side and price
        if side.lower() == 'buy':
            is_ask = False
        elif side.lower() == 'sell':
            is_ask = True
        else:
            raise Exception(f"Invalid side: {side}")

        # Generate unique client order index
        client_order_index = int(time.time() * 1000) % 1000000  # Simple unique ID
        self.current_order_client_id = client_order_index
        self.current_order = None

        adjusted_quantity = self._apply_spot_trade_constraints(quantity, price)
        if adjusted_quantity != quantity:
            self.logger.log(
                (
                    f"Spot order quantity adjusted for minimums: "
                    f"{quantity} -> {adjusted_quantity}"
                ),
                "INFO",
            )
            quantity = adjusted_quantity

        # Create order parameters
        order_params = {
            'market_index': market_index,
            'client_order_index': client_order_index,
            'base_amount': int(quantity * self.base_amount_multiplier),
            'price': int(price * self.price_multiplier),
            'is_ask': is_ask,
            'order_type': lighter_client.ORDER_TYPE_LIMIT,
            'time_in_force': lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
            'reduce_only': False,
            'trigger_price': 0,
        }

        order_result = await self._submit_order_with_retry(order_params)
        return order_result

    async def place_open_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        """Place an open order with Lighter using official SDK."""

        self.current_order = None
        self.current_order_client_id = None
        order_price = await self.get_order_price(direction)

        order_price = self.round_to_tick(order_price)
        order_result = await self.place_limit_order(contract_id, quantity, order_price, direction)
        if not order_result.success:
            raise Exception(f"[OPEN] Error placing order: {order_result.error_message}")

        start_time = time.time()
        order_status = 'OPEN'

        # While waiting for order to be filled
        while time.time() - start_time < 10 and order_status != 'FILLED':
            await asyncio.sleep(0.1)
            if self.current_order is not None:
                order_status = self.current_order.status

        final_order = self.current_order
        final_order_id = final_order.order_id if final_order is not None else order_result.order_id
        final_status = final_order.status if final_order is not None else order_status

        return OrderResult(
            success=True,
            order_id=final_order_id,
            side=direction,
            size=quantity,
            price=order_price,
            status=final_status
        )

    async def _get_active_close_orders(self, contract_id: str) -> int:
        """Get active close orders for a contract using official SDK."""
        active_orders = await self.get_active_orders(contract_id)
        active_close_orders = 0
        for order in active_orders:
            if order.side == self.config.close_order_side:
                active_close_orders += 1
        return active_close_orders

    async def place_close_order(self, contract_id: str, quantity: Decimal, price: Decimal, side: str) -> OrderResult:
        """Place a close order with Lighter using official SDK."""
        self.current_order = None
        self.current_order_client_id = None
        order_result = await self.place_limit_order(contract_id, quantity, price, side)

        # wait for 5 seconds to ensure order is placed
        await asyncio.sleep(5)
        if order_result.success:
            return OrderResult(
                success=True,
                order_id=order_result.order_id,
                side=side,
                size=quantity,
                price=price,
                status='OPEN'
            )
        else:
            raise Exception(f"[CLOSE] Error placing order: {order_result.error_message}")
    
    async def get_order_price(self, side: str = '') -> Decimal:
        """Get the price of an order with Lighter using official SDK."""
        # Get current market prices
        best_bid, best_ask = await self.fetch_bbo_prices(self.config.contract_id)
        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            self.logger.log("Invalid bid/ask prices", "ERROR")
            raise ValueError("Invalid bid/ask prices")

        order_price = (best_bid + best_ask) / 2

        active_orders = await self.get_active_orders(self.config.contract_id)
        close_orders = [order for order in active_orders if order.side == self.config.close_order_side]
        for order in close_orders:
            if side == 'buy':
                order_price = min(order_price, order.price - self.config.tick_size)
            else:
                order_price = max(order_price, order.price + self.config.tick_size)

        return order_price

    async def cancel_order(self, order_id: str) -> OrderResult:
        """Cancel an order with Lighter."""
        # Ensure client is initialized
        if self.lighter_client is None:
            await self._initialize_lighter_client()
        lighter_client = self._require_lighter_client()

        # Cancel order using official SDK
        cancel_order, tx_hash, error = await lighter_client.cancel_order(
            market_index=self._resolve_market_index(),
            order_index=int(order_id)  # Assuming order_id is the order index
        )

        if error is not None:
            return OrderResult(success=False, error_message=f"Cancel order error: {error}")

        if tx_hash:
            return OrderResult(success=True)
        else:
            return OrderResult(success=False, error_message='Failed to send cancellation transaction')

    async def get_order_info(self, order_id: str) -> Optional[OrderInfo]:
        """Get order information from Lighter using official SDK."""
        try:
            # Use shared API client to get account info
            account_api = AccountApi(self.api_client)

            # Get account orders
            account_data = await account_api.account(by="index", value=str(self.account_index))

            # Look for the specific order in account positions
            for position in getattr(account_data, "positions", []) or []:
                symbol_value = getattr(position, "symbol", None)
                if symbol_value == self.config.ticker:
                    position_amt = abs(float(getattr(position, "position", 0)))
                    if position_amt > 0.001:  # Only include significant positions
                        return OrderInfo(
                            order_id=order_id,
                            side="buy" if float(getattr(position, "position", 0)) > 0 else "sell",
                            size=Decimal(str(position_amt)),
                            price=Decimal(str(getattr(position, "avg_price", 0))),
                            status="FILLED",  # Positions are filled orders
                            filled_size=Decimal(str(position_amt)),
                            remaining_size=Decimal('0')
                        )

            return None

        except Exception as e:
            self.logger.log(f"Error getting order info: {e}", "ERROR")
            return None

    @query_retry(reraise=True)
    async def _fetch_orders_with_retry(self) -> List[Any]:
        """Get orders using official SDK."""
        # Ensure client is initialized
        if self.lighter_client is None:
            await self._initialize_lighter_client()
        lighter_client = self._require_lighter_client()

        # Generate auth token for API call
        auth_token, error = lighter_client.create_auth_token_with_expiry()
        if error is not None:
            self.logger.log(f"Error creating auth token: {error}", "ERROR")
            raise ValueError(f"Error creating auth token: {error}")

        # Use OrderApi to get active orders
        order_api = OrderApi(self.api_client)

        # Get active orders for the specific market
        orders_response = await order_api.account_active_orders(
            account_index=self.account_index,
            market_id=self._resolve_market_index(),
            auth=auth_token
        )

        if not orders_response:
            self.logger.log("Failed to get orders", "ERROR")
            raise ValueError("Failed to get orders")

        orders = getattr(orders_response, "orders", None)
        if not orders:
            return []

        return list(orders)

    async def get_active_orders(self, contract_id: str) -> List[OrderInfo]:
        """Get active orders for a contract using official SDK."""
        order_list: List[Any] = await self._fetch_orders_with_retry()

        if not order_list:
            return []
        if not isinstance(order_list, (list, tuple)):
            order_list = list(order_list)

        # Filter orders for the specific market
        contract_orders: List[OrderInfo] = []
        for order in order_list:
            is_ask = bool(getattr(order, "is_ask", False))
            side = "sell" if is_ask else "buy"
            size = self._decimal_or_zero(getattr(order, "initial_base_amount", None))
            price = self._decimal_or_zero(getattr(order, "price", None))
            remaining_size = self._decimal_or_zero(getattr(order, "remaining_base_amount", None))
            filled_size = self._decimal_or_zero(getattr(order, "filled_base_amount", None))
            order_index = getattr(order, "order_index", getattr(order, "id", None))
            status_value = getattr(order, "status", "")
            status_text = status_value.upper() if isinstance(status_value, str) else str(status_value)

            # Only include orders with remaining size > 0
            if size > 0:
                contract_orders.append(OrderInfo(
                    order_id=str(order_index) if order_index is not None else "",
                    side=side,
                    size=size,
                    price=price,
                    status=status_text,
                    filled_size=filled_size,
                    remaining_size=remaining_size
                ))

        return contract_orders

    @query_retry(reraise=True)
    async def _fetch_account_with_retry(self):
        """Get primary account details using official SDK."""
        account_api = AccountApi(self.api_client)

        account_data = await account_api.account(by="index", value=str(self.account_index))

        if not account_data or not account_data.accounts:
            self.logger.log("Failed to get account data", "ERROR")
            raise ValueError("Failed to get account data")

        return account_data.accounts[0]

    @query_retry(reraise=True)
    async def _fetch_positions_with_retry(self) -> List[Any]:
        """Get positions using official SDK."""
        account = await self._fetch_account_with_retry()
        return getattr(account, "positions", [])

    def _signed_position_quantity(self, position: Any) -> Decimal:
        raw_quantity = self._decimal_or_zero(self._extract_mapping_value(position, "position"))
        sign_value = self._extract_mapping_value(position, "sign")
        sign_multiplier: Optional[int] = None

        if sign_value is not None:
            try:
                sign_multiplier = int(sign_value)
            except (TypeError, ValueError):
                sign_multiplier = None

        if sign_multiplier is None:
            side_value = self._extract_mapping_value(position, "side")
            if isinstance(side_value, str):
                side_normalized = side_value.strip().lower()
                if side_normalized in {"sell", "short", "ask"}:
                    sign_multiplier = -1
                elif side_normalized in {"buy", "long", "bid"}:
                    sign_multiplier = 1

        if sign_multiplier is None:
            sign_multiplier = 1 if raw_quantity >= 0 else -1

        if sign_multiplier == 0:
            return Decimal("0")

        return raw_quantity if sign_multiplier > 0 else -raw_quantity

    async def get_account_positions(self) -> Decimal:
        """Get account positions using official SDK."""
        if self._is_spot_market():
            try:
                account = await self._fetch_account_with_retry()
            except Exception:
                return Decimal("0")
            return self._extract_spot_balance(account)

        # Get account info which includes positions
        positions = await self._fetch_positions_with_retry()

        # Find position for current market
        for position in positions:
            market_identifier = getattr(position, "market_id", None)
            if market_identifier is None and hasattr(position, "to_dict"):
                try:
                    market_identifier = position.to_dict().get("market_id")
                except Exception:
                    market_identifier = None

            if market_identifier is None:
                continue

            try:
                market_identifier_str = str(market_identifier)
            except Exception:
                continue
            contract_identifier = getattr(self.config, "contract_id", None)
            if contract_identifier is None:
                continue
            if market_identifier_str != str(contract_identifier):
                continue
            return self._signed_position_quantity(position)

        return Decimal("0")

    async def get_available_balance(self) -> Decimal:
        """Fetch the available balance from the primary account."""
        account = await self._fetch_account_with_retry()

        balance_value = getattr(account, "available_balance", None)
        if balance_value is None:
            balance_value = getattr(account, "availableBalance", None)
        if balance_value is None and hasattr(account, "to_dict"):
            account_dict = account.to_dict() or {}
            balance_value = account_dict.get("available_balance") or account_dict.get("availableBalance")

        return self._decimal_or_zero(balance_value)

    async def get_account_metrics(self) -> Dict[str, Decimal]:
        """Aggregate key metrics for the configured market and account."""
        account = await self._fetch_account_with_retry()

        metrics: Dict[str, Decimal] = {
            "available_balance": self._decimal_or_zero(self._extract_mapping_value(account, "available_balance")),
            "collateral": self._decimal_or_zero(self._extract_mapping_value(account, "collateral")),
            "total_asset_value": self._decimal_or_zero(self._extract_mapping_value(account, "total_asset_value")),
            "daily_volume": Decimal("0"),
            "weekly_volume": Decimal("0"),
            "monthly_volume": Decimal("0"),
            "total_volume": Decimal("0"),
            "position_size": Decimal("0"),
            "position_value": Decimal("0"),
            "unrealized_pnl": Decimal("0"),
            "realized_pnl": Decimal("0"),
        }

        trade_stats = self._extract_mapping_value(account, "trade_stats")
        for key in ("daily_volume", "weekly_volume", "monthly_volume", "total_volume"):
            value = self._extract_mapping_value(trade_stats, key)
            if value is not None:
                metrics[key] = self._decimal_or_zero(value)

        target_position = None
        contract_identifier = getattr(self.config, "contract_id", None)
        ticker_symbol = getattr(self.config, "ticker", None)
        for position in getattr(account, "positions", []) or []:
            symbol = self._extract_mapping_value(position, "symbol")
            market_identifier = self._extract_mapping_value(position, "market_id")
            if contract_identifier is not None:
                try:
                    if str(market_identifier) == str(contract_identifier):
                        target_position = position
                        break
                except Exception:
                    pass
            if ticker_symbol and symbol == ticker_symbol:
                target_position = position
                break

        if target_position is not None:
            metrics["position_size"] = self._signed_position_quantity(target_position)
            metrics["position_value"] = self._decimal_or_zero(self._extract_mapping_value(target_position, "position_value"))
            metrics["unrealized_pnl"] = self._decimal_or_zero(self._extract_mapping_value(target_position, "unrealized_pnl"))
            metrics["realized_pnl"] = self._decimal_or_zero(self._extract_mapping_value(target_position, "realized_pnl"))
        elif self._is_spot_market():
            spot_balance = self._extract_spot_balance(account)
            metrics["position_size"] = spot_balance
            contract_identifier_value = getattr(self.config, "contract_id", None)
            if contract_identifier_value:
                try:
                    best_bid, best_ask = await self.fetch_bbo_prices(contract_identifier_value)
                except Exception:
                    pass
                else:
                    mid_price = (best_bid + best_ask) / Decimal("2")
                    metrics["position_value"] = spot_balance * mid_price

        return metrics

    async def get_contract_attributes(self) -> Tuple[str, Decimal]:
        """Get contract ID for a ticker."""
        ticker = (getattr(self.config, "ticker", "") or "").strip()

        order_api = OrderApi(self.api_client)
        order_books = await order_api.order_books()
        order_book_entries = getattr(order_books, "order_books", None) or []

        market_info = None
        spot_override: Optional[str] = None

        if self._is_spot_market():
            spot_override = self._resolve_spot_market_id()
            for market in order_book_entries:
                try:
                    if str(market.market_id) == spot_override:
                        market_info = market
                        break
                except Exception:
                    continue

            self.logger.log(
                f"Spot route enabled; forcing Lighter market id {spot_override}",
                "INFO",
            )
            if market_info is None:
                self.logger.log(
                    f"Spot market id {spot_override} not found in order books; falling back to order-book details only",
                    "WARNING",
                )
        else:
            if not ticker:
                self.logger.log("Ticker is empty", "ERROR")
                raise ValueError("Ticker is empty")

            for market in order_book_entries:
                if market.symbol == ticker:
                    market_info = market
                    break

            if market_info is None:
                self.logger.log("Failed to get markets", "ERROR")
                raise ValueError("Failed to get markets")

        market_identifier_text: Optional[str] = spot_override
        if market_identifier_text is None:
            market_identifier_text = str(getattr(market_info, "market_id", "")).strip()

        if not market_identifier_text:
            self.logger.log("Failed to resolve Lighter market id", "ERROR")
            raise ValueError("Failed to get markets")

        try:
            market_index_value = int(market_identifier_text)
        except (TypeError, ValueError) as exc:
            self.logger.log(f"Failed to parse market id '{market_identifier_text}'", "ERROR")
            raise ValueError("Invalid market id received from Lighter") from exc

        market_summary = await order_api.order_book_details(market_id=market_index_value)

        details_list = market_summary.order_book_details or []
        if self._is_spot_market():
            spot_details = getattr(market_summary, "spot_order_book_details", None)
            if spot_details:
                details_list = spot_details

        if not details_list:
            self.logger.log("Failed to load detailed market info", "ERROR")
            raise ValueError("Failed to load market details")

        order_book_details = details_list[0]
        # Set contract_id to market name (Lighter uses market IDs as identifiers)
        contract_identifier = str(market_index_value)
        setattr(self.config, "contract_id", contract_identifier)
        setattr(self.config, "market_index", market_index_value)
        self.market_index = market_index_value
        if market_info is not None:
            self.base_amount_multiplier = pow(10, market_info.supported_size_decimals)
            self.price_multiplier = pow(10, market_info.supported_price_decimals)
        else:
            size_decimals = getattr(order_book_details, "supported_size_decimals", None)
            if not isinstance(size_decimals, int):
                size_decimals = getattr(order_book_details, "size_decimals", None)

            price_decimals = getattr(order_book_details, "supported_price_decimals", None)
            if not isinstance(price_decimals, int):
                price_decimals = getattr(order_book_details, "price_decimals", None)

            if isinstance(size_decimals, int):
                self.base_amount_multiplier = pow(10, size_decimals)
            if isinstance(price_decimals, int):
                self.price_multiplier = pow(10, price_decimals)

        tick_decimals = getattr(order_book_details, "supported_price_decimals", None)
        if not isinstance(tick_decimals, int):
            tick_decimals = getattr(order_book_details, "price_decimals", None)
            if not isinstance(tick_decimals, int) and tick_decimals is not None:
                try:
                    tick_decimals = int(tick_decimals)
                except (TypeError, ValueError):
                    tick_decimals = None

        if not isinstance(tick_decimals, int):
            self.logger.log("Tick size decimals missing in market details", "ERROR")
            raise ValueError("Failed to get tick size")

        try:
            tick_size_value = Decimal("1") / (Decimal("10") ** tick_decimals)
        except Exception:
            self.logger.log("Failed to get tick size", "ERROR")
            raise ValueError("Failed to get tick size")

        setattr(self.config, "tick_size", tick_size_value)

        self.market_detail = order_book_details
        if self._is_spot_market():
            self.spot_min_base_amount = self._parse_decimal(getattr(order_book_details, "min_base_amount", None))
            self.spot_min_quote_amount = self._parse_decimal(getattr(order_book_details, "min_quote_amount", None))
            size_step = self._spot_size_step()
            self.logger.log(
                (
                    "Spot constraints -> min_base="
                    f"{self.spot_min_base_amount or 'n/a'}, min_quote="
                    f"{self.spot_min_quote_amount or 'n/a'}, step="
                    f"{size_step or 'n/a'}"
                ),
                "INFO",
            )
        else:
            self.spot_min_base_amount = None
            self.spot_min_quote_amount = None

        self.default_initial_margin_fraction = getattr(order_book_details, "default_initial_margin_fraction", None)
        self.min_initial_margin_fraction = getattr(order_book_details, "min_initial_margin_fraction", None)
        self.default_leverage = self._fraction_to_leverage(self.default_initial_margin_fraction)
        self.max_leverage = self._fraction_to_leverage(self.min_initial_margin_fraction)

        if self.default_leverage or self.max_leverage:
            default_display = str(self.default_leverage) if self.default_leverage is not None else "?"
            max_display = str(self.max_leverage) if self.max_leverage is not None else "?"
            self.logger.log(
                f"Market leverage limits -> default={default_display}x, max={max_display}x",
                "INFO",
            )

        await self._ensure_websocket_manager()

        return contract_identifier, tick_size_value
