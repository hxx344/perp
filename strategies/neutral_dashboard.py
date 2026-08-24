"""Authenticated operator dashboard for the two-account neutral manager.

The neutral manager owns exchange clients and risk decisions.  This module is
only an HTTP boundary: it validates operator input, serializes snapshots, and
passes an explicit action object to the manager callback.  In particular, the
browser never supplies an order side.  The manager must derive a reduce-only
side from the live position immediately before submitting an order.

The service is loopback-only by default.  A public bind is an explicit opt-in
and requires Basic Auth credentials; TLS should still be terminated by an
authenticated HTTPS reverse proxy in production.
"""

from __future__ import annotations

import base64
import binascii
import asyncio
import hmac
import inspect
import json
import logging
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Mapping, Optional, Sequence, Tuple

from aiohttp import web


LOGGER = logging.getLogger("strategies.neutral_dashboard")

SnapshotFactory = Callable[[], Mapping[str, Any] | Awaitable[Mapping[str, Any]]]
ActionHandler = Callable[["NeutralAction"], Mapping[str, Any] | Awaitable[Mapping[str, Any]]]

_MAX_BODY_BYTES = 64 * 1024
_ACTION_HEADER = "dashboard"
_DEFAULT_ACCOUNTS = ("main", "sub")
_DEFAULT_SYMBOLS = ("SPY", "QQQ")
_ACCOUNT_ALIASES = {
    "main": "main",
    "parent": "main",
    "sub": "sub",
    "subaccount": "sub",
}

_SECURITY_HEADERS = {
    "Cache-Control": "no-store, max-age=0",
    "Pragma": "no-cache",
    "Content-Security-Policy": (
        "default-src 'none'; base-uri 'none'; object-src 'none'; "
        "frame-ancestors 'none'; form-action 'none'; "
        "script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; "
        "connect-src 'self'; img-src 'self' data:; font-src 'self'"
    ),
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "Permissions-Policy": "camera=(), geolocation=(), microphone=(), payment=(), usb=()",
    "Cross-Origin-Resource-Policy": "same-origin",
}


@dataclass(frozen=True)
class NeutralAction:
    """Validated action handed to the neutral manager.

    ``quantity`` is an absolute base quantity.  ``fraction`` is an optional
    fraction of the currently observed position and is mutually exclusive with
    ``quantity``.  The manager must re-read the position before sending a
    reduce-only order; this object is not an authorization to open risk.
    """

    action: str
    request_id: str
    account: Optional[str] = None
    symbol: Optional[str] = None
    quantity: Optional[Decimal] = None
    fraction: Optional[Decimal] = None
    reason: str = "operator dashboard"


def _json_default(value: Any) -> str:
    if isinstance(value, Decimal):
        return format(value, "f")
    raise TypeError(f"unsupported dashboard value: {type(value).__name__}")


def _action_result_status(value: Any) -> Tuple[Optional[str], bool]:
    """Find an unresolved or failed write result in a nested action payload.

    Manager actions deliberately return per-account/per-leg mappings.  Looking
    only at the top-level mapping would let a partial close appear successful
    when one child result contains an error or an exchange-status timeout.
    """

    failure_statuses = {"partial_failure", "failed", "error", "rejected"}
    uncertain_statuses = {
        "unknown_pending",
        "unknown_journal",
        "accepted_pending_confirmation",
        "completed_after_timeout",
        "failed_after_timeout",
        "cancelled_after_timeout",
    }
    if isinstance(value, Mapping):
        uncertain: Optional[str] = None
        raw_status = value.get("status")
        status = str(raw_status).strip().casefold() if raw_status is not None else ""
        if status in failure_statuses:
            return status, True
        if status in uncertain_statuses:
            uncertain = status
        error = value.get("error")
        if error not in (None, "", False, []):
            return "error", True
        for key, child in value.items():
            if str(key).casefold().endswith("error") and child not in (None, "", False, []):
                return "error", True
        for child in value.values():
            found, failed = _action_result_status(child)
            if failed:
                if found in failure_statuses or found == "error":
                    return found, True
                uncertain = uncertain or found
        return (uncertain, uncertain is not None)
    if isinstance(value, (list, tuple)):
        uncertain: Optional[str] = None
        for child in value:
            found, failed = _action_result_status(child)
            if failed:
                if found in failure_statuses or found == "error":
                    return found, True
                uncertain = uncertain or found
        return (uncertain, uncertain is not None)
    return None, False


def _parse_decimal(value: Any, *, field: str, positive: bool = False) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"{field} is required")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a decimal number") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field} must be finite")
    if positive and parsed <= 0:
        raise ValueError(f"{field} must be greater than zero")
    return parsed


def _normalise_choice(value: Any, *, field: str, choices: Sequence[str]) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} is required")
    normalised = value.strip().casefold()
    lookup = {choice.casefold(): choice for choice in choices}
    if normalised not in lookup:
        expected = ", ".join(choices)
        raise ValueError(f"{field} must be one of: {expected}")
    return lookup[normalised]


def _request_id(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("request_id is required")
    text = value.strip()
    if len(text) > 128 or not text:
        raise ValueError("request_id is invalid")
    # UUIDs are preferred, but accepting a short operator-generated id keeps
    # the API usable from curl while still bounding the replay-cache key.
    try:
        return str(uuid.UUID(text))
    except (ValueError, AttributeError):
        if any(ord(char) < 0x21 or ord(char) > 0x7E for char in text):
            raise ValueError("request_id is invalid")
        return text


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _security_headers(_request: web.Request, response: web.StreamResponse) -> None:
    for name, value in _SECURITY_HEADERS.items():
        response.headers.setdefault(name, value)


class NeutralDashboard:
    """Small authenticated aiohttp dashboard for a neutral account manager."""

    def __init__(
        self,
        snapshot_factory: SnapshotFactory,
        action_handler: ActionHandler,
        *,
        host: str = "127.0.0.1",
        port: int = 8790,
        username: Optional[str] = None,
        password: Optional[str] = None,
        allow_public_bind: bool = False,
        allowed_accounts: Sequence[str] = _DEFAULT_ACCOUNTS,
        allowed_symbols: Sequence[str] = _DEFAULT_SYMBOLS,
        replay_ttl_seconds: float = 900.0,
    ) -> None:
        self._snapshot_factory = snapshot_factory
        self._action_handler = action_handler
        self.host = str(host)
        self.port = int(port)
        self.username = username or ""
        self.password = password or ""
        self.allow_public_bind = bool(allow_public_bind)
        self.allowed_accounts = tuple(str(item) for item in allowed_accounts)
        self.allowed_symbols = tuple(str(item).upper() for item in allowed_symbols)
        self.replay_ttl_seconds = max(1.0, float(replay_ttl_seconds))
        self.bound_port: Optional[int] = None
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None
        self._action_lock = asyncio.Lock()
        self._seen_request_ids: Dict[str, float] = {}
        self._page_path = Path(__file__).with_name("neutral_dashboard.html")

    @property
    def running(self) -> bool:
        return self._runner is not None

    @property
    def credentials_configured(self) -> bool:
        return bool(self.username and self.password)

    async def start(self) -> None:
        if self.running:
            return
        host = self.host.strip()
        is_loopback = host in {"127.0.0.1", "::1"}
        if not is_loopback:
            if not self.allow_public_bind:
                raise RuntimeError(
                    "Neutral dashboard only serves loopback by default; "
                    "use an authenticated HTTPS reverse proxy or explicitly opt in"
                )
            if not self.credentials_configured:
                raise RuntimeError("public neutral dashboard bind requires credentials (username and password)")
        if (self.username and not self.password) or (self.password and not self.username):
            raise RuntimeError("neutral dashboard username and password must be configured together")

        app = web.Application(client_max_size=_MAX_BODY_BYTES)
        app.on_response_prepare.append(_security_headers)
        app.router.add_get("/", self._handle_index)
        app.router.add_get("/dashboard", self._handle_index)
        app.router.add_get("/api/healthz", self._handle_health)
        app.router.add_get("/api/snapshot", self._handle_snapshot)
        app.router.add_post("/api/actions/close-position", self._handle_close_position)
        app.router.add_post("/api/actions/close-pair", self._handle_close_pair)
        app.router.add_post("/api/actions/rebalance", self._handle_rebalance)
        app.router.add_post("/api/actions/flatten-all", self._handle_flatten_all)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, host, self.port)
        try:
            await site.start()
        except Exception:
            await runner.cleanup()
            raise
        self._runner = runner
        self._site = site
        sockets = getattr(site, "_server", None)
        socket_list = getattr(sockets, "sockets", None) if sockets is not None else None
        self.bound_port = int(socket_list[0].getsockname()[1]) if socket_list else self.port

    async def stop(self) -> None:
        runner = self._runner
        self._runner = None
        self._site = None
        self.bound_port = None
        if runner is not None:
            await runner.cleanup()

    def _require_auth(self, request: web.Request) -> None:
        # Loopback read-only development mode remains convenient, but once
        # credentials are configured every route is authenticated.
        if not self.credentials_configured:
            if request.path.startswith("/api/actions/"):
                raise web.HTTPServiceUnavailable(
                    text=json.dumps({
                        "ok": False,
                        "status": "actions_disabled",
                        "error": "dashboard actions are disabled; configure RH_NEUTRAL_DASHBOARD_TOKEN",
                    }, ensure_ascii=False),
                    content_type="application/json",
                )
            return
        header = request.headers.get("Authorization", "")
        if not header.startswith("Basic "):
            raise web.HTTPUnauthorized(headers={"WWW-Authenticate": 'Basic realm="Neutral Dashboard"'})
        try:
            decoded = base64.b64decode(header[6:], validate=True).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError, ValueError):
            raise web.HTTPUnauthorized(headers={"WWW-Authenticate": 'Basic realm="Neutral Dashboard"'})
        provided_user, separator, provided_password = decoded.partition(":")
        if not separator or not (
            hmac.compare_digest(provided_user, self.username)
            and hmac.compare_digest(provided_password, self.password)
        ):
            raise web.HTTPUnauthorized(headers={"WWW-Authenticate": 'Basic realm="Neutral Dashboard"'})

    @staticmethod
    def _require_action_header(request: web.Request) -> None:
        if request.headers.get("X-Neutral-Action") != _ACTION_HEADER:
            raise web.HTTPForbidden(text="missing action header")

    async def _read_json(self, request: web.Request) -> Dict[str, Any]:
        if request.content_length is not None and request.content_length > _MAX_BODY_BYTES:
            raise web.HTTPRequestEntityTooLarge(max_size=_MAX_BODY_BYTES, actual_size=request.content_length)
        try:
            payload = await request.json()
        except Exception as exc:
            raise web.HTTPBadRequest(text="request body must be JSON") from exc
        if not isinstance(payload, dict):
            raise web.HTTPBadRequest(text="request body must be an object")
        return payload

    def _check_replay(self, request_id: str) -> None:
        now = time.monotonic()
        expired = [key for key, seen_at in self._seen_request_ids.items() if now - seen_at > self.replay_ttl_seconds]
        for key in expired:
            self._seen_request_ids.pop(key, None)
        if request_id in self._seen_request_ids:
            raise web.HTTPConflict(text="request_id has already been used")
        self._seen_request_ids[request_id] = now

    def _parse_common(
        self,
        payload: Mapping[str, Any],
        action: str,
        *,
        require_size: bool = True,
    ) -> NeutralAction:
        try:
            request_id = _request_id(payload.get("request_id"))
            raw_account = payload.get("account")
            if isinstance(raw_account, str):
                raw_account_normalized = raw_account.strip().casefold()
                allowed_lookup = {item.casefold(): item for item in self.allowed_accounts}
                alias = allowed_lookup.get(raw_account_normalized)
                if alias is None:
                    alias = _ACCOUNT_ALIASES.get(raw_account_normalized)
                account = alias if alias in self.allowed_accounts else None
            else:
                account = None
            if account is None:
                expected = ", ".join(self.allowed_accounts)
                raise ValueError(f"account must be one of: {expected}")
            symbol = _normalise_choice(payload.get("symbol"), field="symbol", choices=self.allowed_symbols)
            quantity_value = payload.get("quantity")
            fraction_value = payload.get("fraction")
            if quantity_value is not None and fraction_value is not None:
                raise ValueError("quantity and fraction are mutually exclusive")
            quantity = (
                _parse_decimal(quantity_value, field="quantity", positive=True)
                if quantity_value is not None
                else None
            )
            fraction = (
                _parse_decimal(fraction_value, field="fraction", positive=True)
                if fraction_value is not None
                else None
            )
            if require_size and quantity is None and fraction is None:
                raise ValueError("quantity or fraction is required")
            if fraction is not None and fraction > 1:
                raise ValueError("fraction must be between 0 and 1")
            reason = payload.get("reason", "operator dashboard")
            if not isinstance(reason, str) or len(reason) > 256:
                raise ValueError("reason is invalid")
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc
        self._check_replay(request_id)
        return NeutralAction(
            action=action,
            request_id=request_id,
            account=account,
            symbol=symbol,
            quantity=quantity,
            fraction=fraction,
            reason=reason,
        )

    async def _dispatch(self, action: NeutralAction) -> web.Response:
        async with self._action_lock:
            try:
                result = await _maybe_await(self._action_handler(action))
            except Exception as exc:
                LOGGER.exception("neutral dashboard action failed: %s", action.action)
                # Return the operator-useful validation/exchange reason. Do
                # not collapse all failures into an opaque generic message.
                detail = str(exc).strip() or type(exc).__name__
                return web.json_response(
                    {"ok": False, "action": action.action, "status": "error", "error": detail},
                    status=502,
                )
        if not isinstance(result, Mapping):
            result = {"result": result}
        status, failed = _action_result_status(result)
        body = {
            "ok": not failed,
            "action": action.action,
            "status": status,
            "result": dict(result),
        }
        if not failed:
            return web.json_response(body)
        # A timeout means the exchange may have accepted the write even though
        # the HTTP request did not complete.  202 keeps that distinction from
        # a normal validation/server failure and tells callers not to retry.
        response_status = 202 if status in {
            "unknown_pending",
            "unknown_journal",
            "accepted_pending_confirmation",
            "completed_after_timeout",
            "failed_after_timeout",
            "cancelled_after_timeout",
        } else 502
        if response_status == 202:
            body["error"] = "exchange status requires reconciliation; do not retry this request"
        return web.json_response(body, status=response_status)

    async def _handle_index(self, request: web.Request) -> web.Response:
        self._require_auth(request)
        try:
            html = self._page_path.read_text(encoding="utf-8")
        except OSError:
            return web.Response(status=500, text="dashboard page unavailable")
        return web.Response(text=html, content_type="text/html")

    async def _handle_health(self, request: web.Request) -> web.Response:
        self._require_auth(request)
        try:
            snapshot = await _maybe_await(self._snapshot_factory())
            healthy = bool(snapshot.get("ok", True)) if isinstance(snapshot, Mapping) else True
        except Exception:
            healthy = False
        return web.json_response(
            {"ok": healthy, "dashboard": "running"},
            status=200 if healthy else 503,
        )

    async def _handle_snapshot(self, request: web.Request) -> web.Response:
        self._require_auth(request)
        try:
            snapshot = await _maybe_await(self._snapshot_factory())
            body = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"), default=_json_default)
        except Exception:
            LOGGER.exception("neutral dashboard snapshot failed")
            return web.json_response({"ok": False, "error": "snapshot unavailable"}, status=503)
        return web.Response(text=body, content_type="application/json")

    async def _handle_close_position(self, request: web.Request) -> web.Response:
        self._require_auth(request)
        self._require_action_header(request)
        action = self._parse_common(await self._read_json(request), "close_position", require_size=False)
        return await self._dispatch(action)

    async def _handle_close_pair(self, request: web.Request) -> web.Response:
        self._require_auth(request)
        self._require_action_header(request)
        payload = await self._read_json(request)
        # A pair action intentionally takes only a symbol.  The manager knows
        # which account holds the long/short legs and closes both reduce-only.
        payload = dict(payload)
        payload["account"] = self.allowed_accounts[0]
        action = self._parse_common(payload, "close_pair", require_size=False)
        action = NeutralAction(
            action=action.action,
            request_id=action.request_id,
            symbol=action.symbol,
            quantity=action.quantity,
            fraction=action.fraction,
            reason=action.reason,
        )
        return await self._dispatch(action)

    async def _handle_rebalance(self, request: web.Request) -> web.Response:
        """Ask the manager to recompute and, when enabled, execute one plan.

        The request deliberately carries no transfer amount or direction.  A
        balance transfer must be calculated from fresh account snapshots by
        the manager, with its own reserve, hysteresis, cooldown, and live-mode
        gates applied.
        """

        self._require_auth(request)
        self._require_action_header(request)
        payload = await self._read_json(request)
        try:
            request_id = _request_id(payload.get("request_id"))
            reason = payload.get("reason", "operator dashboard")
            if not isinstance(reason, str) or len(reason) > 256:
                raise ValueError("reason is invalid")
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc
        self._check_replay(request_id)
        return await self._dispatch(NeutralAction(action="rebalance", request_id=request_id, reason=reason))

    async def _handle_flatten_all(self, request: web.Request) -> web.Response:
        self._require_auth(request)
        self._require_action_header(request)
        payload = await self._read_json(request)
        if payload.get("confirm") != "FLATTEN_ALL":
            raise web.HTTPBadRequest(text="confirm must equal FLATTEN_ALL")
        try:
            request_id = _request_id(payload.get("request_id"))
            reason = payload.get("reason", "operator dashboard")
            if not isinstance(reason, str) or len(reason) > 256:
                raise ValueError("reason is invalid")
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc
        self._check_replay(request_id)
        return await self._dispatch(NeutralAction(action="flatten_all", request_id=request_id, reason=reason))


__all__ = ["NeutralAction", "NeutralDashboard"]
