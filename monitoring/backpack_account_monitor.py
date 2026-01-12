#!/usr/bin/env python3
"""Backpack account monitor.

This helper mirrors the control/ack pattern used by `para_account_monitor.py`:
- Periodically collects Backpack account / position / margin snapshots.
- Pushes the snapshot to the hedge coordinator via POST /update so the dashboard
  can display per-agent health.
- Polls GET /control?agent_id=... to pick up pending adjustments.
- Executes adjustments locally (monitor side), then ACKs them back.

Important: Backpack "native TWAP" integration is intentionally stubbed.
The repository currently doesn't expose an SDK wrapper for Backpack algo/TWAP
endpoints, so this monitor will:
- support MARKET adjustments immediately
- accept TWAP adjustments but will mark them as failed with a clear message

Once we confirm Backpack TWAP API shape (SDK method or REST endpoint), we can
wire `_place_twap_order()` and `_poll_twap_progress()` similar to Paradex.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, cast

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException

import base64
from cryptography.hazmat.primitives.asymmetric import ed25519

# Ensure repo root on sys.path when running standalone.
_PDT_ROOT = Path(__file__).resolve().parents[1]
if str(_PDT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PDT_ROOT))

try:
    from exchanges.backpack import BackpackClient
except Exception as exc:  # pragma: no cover
    BackpackClient = None  # type: ignore[assignment]
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None

LOGGER = logging.getLogger(__name__)


def _decimal_from(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        text = str(value).strip()
    except Exception:
        return None
    if not text:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _decimal_to_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value, "f")
    try:
        return format(Decimal(str(value)), "f")
    except Exception:
        try:
            return str(value)
        except Exception:
            return None


@dataclass
class BackpackMonitorConfig:
    label: str
    coordinator_url: str
    agent_id: str
    poll_interval: float = 5.0
    request_timeout: float = 10.0
    coordinator_username: Optional[str] = None
    coordinator_password: Optional[str] = None


class BackpackAccountMonitor:
    def __init__(self, *, cfg: BackpackMonitorConfig, client: Any) -> None:
        self._cfg = cfg
        self._client = client

        self._http = requests.Session()
        username = (cfg.coordinator_username or "").strip()
        password = (cfg.coordinator_password or "").strip()
        self._auth: Optional[HTTPBasicAuth] = None
        if username or password:
            self._auth = HTTPBasicAuth(username, password)
            self._http.auth = self._auth

        self._update_endpoint = f"{cfg.coordinator_url}/update"
        self._control_endpoint = f"{cfg.coordinator_url}/control"
        # Dedicated ACK endpoint for Backpack adjustments.
        self._ack_endpoint = f"{cfg.coordinator_url}/backpack/adjust/ack"

        self._processed_adjustments: Dict[str, Dict[str, Any]] = {}
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    def _push(self, payload: Dict[str, Any]) -> None:
        try:
            response = self._http.post(
                self._update_endpoint,
                json=payload,
                timeout=self._cfg.request_timeout,
                auth=self._auth,
            )
        except RequestException as exc:
            raise RuntimeError(f"Failed to push monitor payload: {exc}") from exc
        if response.status_code >= 400:
            raise RuntimeError(f"Coordinator rejected payload: HTTP {response.status_code} {response.text}")

    def _fetch_agent_control(self) -> Optional[Dict[str, Any]]:
        try:
            response = self._http.get(
                self._control_endpoint,
                params={"agent_id": self._cfg.agent_id},
                timeout=self._cfg.request_timeout,
                auth=self._auth,
            )
        except RequestException as exc:
            LOGGER.debug("Failed to query coordinator control endpoint: %s", exc)
            return None
        if response.status_code >= 400:
            LOGGER.debug("Coordinator control query failed: HTTP %s %s", response.status_code, response.text)
            return None
        try:
            payload = response.json()
        except ValueError:
            return None
        if not isinstance(payload, dict):
            return None
        return payload

    def _acknowledge_adjustment(
        self,
        request_id: str,
        status: str,
        note: Optional[str],
        extra: Optional[Dict[str, Any]] = None,
    ) -> bool:
        payload: Dict[str, Any] = {
            "request_id": request_id,
            "agent_id": self._cfg.agent_id,
            "status": status,
        }
        if note is not None:
            payload["note"] = note
        if isinstance(extra, dict) and extra:
            payload.update(extra)
        try:
            response = self._http.post(
                self._ack_endpoint,
                json=payload,
                timeout=self._cfg.request_timeout,
                auth=self._auth,
            )
        except RequestException as exc:
            LOGGER.warning("Adjustment ACK request failed for %s: %s", request_id, exc)
            return False
        if response.status_code >= 400:
            LOGGER.warning(
                "Coordinator rejected ACK for %s: HTTP %s %s",
                request_id,
                response.status_code,
                response.text,
            )
            return False
        return True

    def _collect(self) -> Optional[Dict[str, Any]]:
        ts = time.time()

        # Best-effort: these calls depend on `BackpackClient` implementation.
        summary: Dict[str, Any] = {}
        positions: List[Dict[str, Any]] = []

        try:
            get_positions = getattr(self._client, "get_positions", None)
            if callable(get_positions):
                raw = get_positions()  # type: ignore[misc]
                if isinstance(raw, list):
                    for entry in raw:
                        if isinstance(entry, dict):
                            positions.append(cast(Dict[str, Any], entry))
        except Exception as exc:
            summary["positions_error"] = str(exc)

        payload = {
            "agent_id": self._cfg.agent_id,
            "instrument": f"BACKPACK {self._cfg.label}",
            "backpack_accounts": {
                "updated_at": ts,
                "summary": summary,
                "accounts": [
                    {
                        "name": self._cfg.label,
                    }
                ],
                "positions": positions,
            },
        }
        return payload

    def _execute_adjustment(self, entry: Dict[str, Any]) -> Tuple[str, str, Dict[str, Any]]:
        request_id = str(entry.get("request_id") or "")
        action = str(entry.get("action") or "").strip().lower()
        if action not in {"add", "reduce"}:
            raise ValueError(f"Unsupported adjustment action '{action}'")
        magnitude = _decimal_from(entry.get("magnitude"))
        if magnitude is None or magnitude <= 0:
            raise ValueError(f"Invalid adjustment magnitude '{entry.get('magnitude')}'")

        payload_cfg: Dict[str, Any] = cast(Dict[str, Any], entry.get("payload")) if isinstance(entry.get("payload"), dict) else {}
        order_mode = str(payload_cfg.get("order_mode") or "").strip().lower()
        algo_type = str(payload_cfg.get("algo_type") or "").strip().upper()

        # Default adjustments to TWAP unless overridden (matches para monitor pattern).
        if not order_mode and not algo_type:
            order_mode = "twap"
            algo_type = "TWAP"

        symbols = entry.get("symbols") or entry.get("target_symbols")
        symbol = None
        if isinstance(symbols, list) and symbols:
            symbol = str(symbols[0])
        elif isinstance(symbols, str) and symbols:
            symbol = symbols
        if not symbol:
            raise ValueError("Unable to resolve symbol for adjustment request")

        # Side inference: keep it super simple here.
        if action == "add":
            side = "buy"
        else:
            side = "sell"

        if order_mode in {"market", "mkt"} and not algo_type:
            order_mode = "market"

        if order_mode == "market":
            order = self._place_market_order(symbol, side, magnitude)
            order_id = None
            if isinstance(order, dict):
                order_id = order.get("id") or order.get("order_id")
            extra: Dict[str, Any] = {"order_type": "MARKET"}
            if order_id is not None:
                extra["order_id"] = str(order_id)
            note = f"{action} {symbol} {side} qty={_decimal_to_str(magnitude) or magnitude}; order_type=MARKET"
            return "succeeded", note, extra

        # Native TWAP via Strategy API (Scheduled strategy).
        if order_mode == "twap" or algo_type == "TWAP":
            duration_ms = int(payload_cfg.get("duration") or payload_cfg.get("duration_ms") or 0)
            interval_ms = int(payload_cfg.get("interval") or payload_cfg.get("interval_ms") or 0)
            if duration_ms <= 0:
                duration_ms = 60_000
            if interval_ms <= 0:
                interval_ms = 5_000

            reduce_only = bool(payload_cfg.get("reduce_only") or payload_cfg.get("reduceOnly") or False)
            randomized = bool(
                payload_cfg.get("randomized_interval_quantity")
                or payload_cfg.get("randomizedIntervalQuantity")
                or False
            )
            client_strategy_id = payload_cfg.get("clientStrategyId")
            broker_id = payload_cfg.get("brokerId")
            window_ms = int(payload_cfg.get("window_ms") or payload_cfg.get("window") or 5000)

            created = self._place_twap_strategy(
                symbol=symbol,
                side=side,
                quantity=magnitude,
                duration_ms=duration_ms,
                interval_ms=interval_ms,
                reduce_only=reduce_only,
                randomized_interval_quantity=randomized,
                client_strategy_id=int(client_strategy_id) if client_strategy_id is not None else None,
                window_ms=window_ms,
                broker_id=int(broker_id) if broker_id is not None else None,
            )

            # Strategy id can be nested depending on discriminator; keep it defensive.
            strategy_id = created.get("id")
            if strategy_id is None and isinstance(created.get("Scheduled"), dict):
                strategy_id = created["Scheduled"].get("id")
            if strategy_id is None and isinstance(created.get("data"), dict):
                strategy_id = created["data"].get("id")

            extra: Dict[str, Any] = {
                "order_type": "TWAP",
                "strategy_id": str(strategy_id) if strategy_id is not None else None,
                "duration_ms": duration_ms,
                "interval_ms": interval_ms,
            }

            # Progress polling + incremental ACK (best-effort).
            start = time.time()
            deadline = start + max(duration_ms / 1000.0 + 15.0, 30.0)
            last_progress_sent_at = 0.0
            last_executed_qty: Optional[str] = None
            final_status: Optional[str] = None

            while time.time() < deadline:
                try:
                    current = self._get_strategy(symbol=symbol, strategy_id=str(strategy_id) if strategy_id is not None else None, window_ms=window_ms)
                except Exception as exc:
                    # If the strategy is no longer open, BP returns 404. Treat as done-ish.
                    current = {"status": "Unknown", "error": str(exc)}

                status_raw = str(current.get("status") or "")
                executed_qty = current.get("executedQuantity")
                executed_quote_qty = current.get("executedQuoteQuantity")

                now = time.time()
                if executed_qty is not None:
                    executed_qty_str = str(executed_qty)
                else:
                    executed_qty_str = None

                if now - last_progress_sent_at >= max(self._cfg.poll_interval, 2.0):
                    if executed_qty_str != last_executed_qty or status_raw:
                        progress_extra = {
                            "progress": {
                                "strategy_id": str(strategy_id) if strategy_id is not None else None,
                                "status": status_raw,
                                "executedQuantity": executed_qty,
                                "executedQuoteQuantity": executed_quote_qty,
                                "elapsed_s": now - start,
                            }
                        }
                        # Best-effort ACK; don't fail execution if coordinator rejects progress.
                        self._acknowledge_adjustment(
                            request_id,
                            "running",
                            None,
                            progress_extra,
                        )
                        last_progress_sent_at = now
                        last_executed_qty = executed_qty_str

                if status_raw in {"Completed", "Cancelled", "Terminated"}:
                    final_status = status_raw
                    break

                time.sleep(max(1.0, min(self._cfg.poll_interval, 5.0)))

            if final_status is None:
                # Timed out; attempt to cancel so we don't leave it running forever.
                try:
                    self._cancel_strategy(symbol=symbol, strategy_id=str(strategy_id) if strategy_id is not None else None, window_ms=window_ms)
                except Exception:
                    pass
                final_status = "TimedOut"

            note = (
                f"{action} {symbol} {side} qty={_decimal_to_str(magnitude) or magnitude}; "
                f"order_type=TWAP strategy_id={strategy_id} status={final_status}"
            )
            extra["final_status"] = final_status
            return ("succeeded" if final_status == "Completed" else "failed"), note, extra

        raise ValueError(f"Unsupported order_mode '{order_mode}' algo_type='{algo_type}'")

    def _place_market_order(self, symbol: str, side: str, quantity: Decimal) -> Any:
        method = getattr(self._client, "place_market_order", None)
        if not callable(method):
            raise RuntimeError("Backpack client is missing place_market_order")
        return method(symbol=symbol, side=side, quantity=quantity)

    def _bp_private_key(self) -> ed25519.Ed25519PrivateKey:
        secret = os.getenv("BACKPACK_SECRET_KEY")
        if not secret:
            raise RuntimeError("BACKPACK_SECRET_KEY is required for signing")
        try:
            secret_bytes = base64.b64decode(secret)
        except Exception as exc:
            raise RuntimeError(f"BACKPACK_SECRET_KEY must be base64 encoded: {exc}") from exc
        try:
            return ed25519.Ed25519PrivateKey.from_private_bytes(secret_bytes)
        except Exception as exc:
            raise RuntimeError(f"Invalid BACKPACK_SECRET_KEY (ed25519 seed bytes): {exc}") from exc

    @staticmethod
    def _bp_build_query_string(values: Dict[str, Any]) -> str:
        parts: List[str] = []
        for key in sorted(values.keys()):
            val = values.get(key)
            if val is None:
                continue
            if isinstance(val, bool):
                encoded = "true" if val else "false"
            else:
                encoded = str(val)
            parts.append(f"{key}={encoded}")
        return "&".join(parts)

    def _bp_sign(self, instruction: str, *, params_or_body: Dict[str, Any], timestamp_ms: int, window_ms: int) -> str:
        base = self._bp_build_query_string(params_or_body)
        signing = f"instruction={instruction}"
        if base:
            signing += f"&{base}"
        signing += f"&timestamp={timestamp_ms}&window={window_ms}"
        signature = self._bp_private_key().sign(signing.encode())
        return base64.b64encode(signature).decode()

    def _bp_request(
        self,
        instruction: str,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        broker_id: Optional[int] = None,
        window_ms: int = 5000,
    ) -> Any:
        public_key = os.getenv("BACKPACK_PUBLIC_KEY")
        if not public_key:
            raise RuntimeError("BACKPACK_PUBLIC_KEY is required for signing")

        timestamp_ms = int(time.time() * 1000)
        window_ms = int(window_ms or 5000)
        if window_ms <= 0:
            window_ms = 5000
        if window_ms > 60000:
            window_ms = 60000

        signing_payload = body if body is not None else (params or {})
        signature = self._bp_sign(
            instruction,
            params_or_body=signing_payload,
            timestamp_ms=timestamp_ms,
            window_ms=window_ms,
        )

        url = "https://api.backpack.exchange" + path
        headers: Dict[str, str] = {
            "X-API-KEY": public_key,
            "X-SIGNATURE": signature,
            "X-TIMESTAMP": str(timestamp_ms),
            "X-WINDOW": str(window_ms),
            "Content-Type": "application/json; charset=utf-8",
        }
        if broker_id is not None:
            headers["X-Broker-Id"] = str(int(broker_id))

        try:
            response = self._http.request(
                method.upper(),
                url,
                params=params,
                json=body,
                headers=headers,
                timeout=self._cfg.request_timeout,
            )
        except RequestException as exc:
            raise RuntimeError(f"Backpack REST request failed: {exc}") from exc

        if response.status_code >= 400:
            raise RuntimeError(f"Backpack REST error: HTTP {response.status_code} {response.text}")

        if not response.text:
            return None
        try:
            return response.json()
        except ValueError:
            return response.text

    def _place_twap_strategy(
        self,
        *,
        symbol: str,
        side: str,
        quantity: Decimal,
        duration_ms: int,
        interval_ms: int,
        reduce_only: bool,
        randomized_interval_quantity: bool = False,
        client_strategy_id: Optional[int] = None,
        window_ms: int = 5000,
        broker_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "strategyType": "Scheduled",
            "side": "Bid" if side.lower() == "buy" else "Ask",
            "symbol": symbol,
            "quantity": _decimal_to_str(quantity) or str(quantity),
            "duration": int(duration_ms),
            "interval": int(interval_ms),
            "reduceOnly": bool(reduce_only),
            "randomizedIntervalQuantity": bool(randomized_interval_quantity),
        }
        if client_strategy_id is not None:
            payload["clientStrategyId"] = int(client_strategy_id)
        if broker_id is not None:
            payload["brokerId"] = int(broker_id)

        result = self._bp_request(
            "strategyCreate",
            "POST",
            "/api/v1/strategy",
            body=payload,
            broker_id=broker_id,
            window_ms=window_ms,
        )
        if not isinstance(result, dict):
            raise RuntimeError(f"Unexpected strategyCreate response: {result}")
        return cast(Dict[str, Any], result)

    def _get_strategy(
        self,
        *,
        symbol: str,
        strategy_id: Optional[str] = None,
        client_strategy_id: Optional[int] = None,
        window_ms: int = 5000,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"symbol": symbol}
        if strategy_id is not None:
            params["strategyId"] = strategy_id
        if client_strategy_id is not None:
            params["clientStrategyId"] = int(client_strategy_id)
        if "strategyId" not in params and "clientStrategyId" not in params:
            raise ValueError("strategy_id or client_strategy_id is required")

        result = self._bp_request(
            "strategyQuery",
            "GET",
            "/api/v1/strategy",
            params=params,
            window_ms=window_ms,
        )
        if not isinstance(result, dict):
            raise RuntimeError(f"Unexpected strategyQuery response: {result}")
        return cast(Dict[str, Any], result)

    def _cancel_strategy(
        self,
        *,
        symbol: str,
        strategy_id: Optional[str] = None,
        client_strategy_id: Optional[int] = None,
        window_ms: int = 5000,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"symbol": symbol}
        if strategy_id is not None:
            payload["strategyId"] = strategy_id
        if client_strategy_id is not None:
            payload["clientStrategyId"] = int(client_strategy_id)
        if "strategyId" not in payload and "clientStrategyId" not in payload:
            raise ValueError("strategy_id or client_strategy_id is required")

        result = self._bp_request(
            "strategyCancel",
            "DELETE",
            "/api/v1/strategy",
            body=payload,
            window_ms=window_ms,
        )
        if not isinstance(result, dict):
            raise RuntimeError(f"Unexpected strategyCancel response: {result}")
        return cast(Dict[str, Any], result)

    def _process_adjustments(self) -> None:
        snapshot = self._fetch_agent_control()
        if not snapshot:
            return
        agent_block = snapshot.get("agent")
        if not isinstance(agent_block, dict):
            return
        pending = agent_block.get("pending_adjustments")
        if not isinstance(pending, list) or not pending:
            self._prune_processed_adjustments()
            return

        for entry in pending:
            if not isinstance(entry, dict):
                continue
            request_id = entry.get("request_id")
            if not request_id:
                continue

            # Ignore adjustments not meant for Backpack monitor.
            provider = str(entry.get("provider") or entry.get("exchange") or "").strip().lower()
            if provider and provider not in {"backpack", "bp"}:
                continue

            cached = self._processed_adjustments.get(request_id)
            if cached and cached.get("acked"):
                continue
            if cached and cached.get("inflight"):
                continue

            def _worker(entry_copy: Dict[str, Any], req_id: str) -> None:
                status = "failed"
                note: Optional[str] = None
                extra: Optional[Dict[str, Any]] = None
                try:
                    status, note, extra = self._execute_adjustment(entry_copy)
                except Exception as exc:
                    status = "failed"
                    note = f"execution error: {exc}"
                    LOGGER.error("Adjustment %s execution failed: %s", req_id, exc)

                acked = self._acknowledge_adjustment(req_id, status, note, extra)
                self._processed_adjustments[req_id] = {
                    "status": status,
                    "note": note,
                    "extra": extra,
                    "acked": acked,
                    "timestamp": time.time(),
                    "inflight": False,
                }

            self._processed_adjustments[request_id] = {
                "status": "pending",
                "note": None,
                "acked": False,
                "timestamp": time.time(),
                "inflight": True,
            }
            try:
                self._executor.submit(_worker, dict(entry), str(request_id))
            except Exception as exc:
                LOGGER.error("Failed to submit adjustment %s: %s", request_id, exc)
                self._processed_adjustments[request_id] = {
                    "status": "failed",
                    "note": f"submit failed: {exc}",
                    "acked": False,
                    "timestamp": time.time(),
                    "inflight": False,
                }

        self._prune_processed_adjustments()

    def _prune_processed_adjustments(self, ttl: float = 3600.0) -> None:
        if not self._processed_adjustments:
            return
        cutoff = time.time() - max(ttl, 60.0)
        for request_id, record in list(self._processed_adjustments.items()):
            if record.get("acked") and record.get("timestamp", 0) < cutoff:
                self._processed_adjustments.pop(request_id, None)

    def run_once(self) -> None:
        payload = self._collect()
        if payload is None:
            LOGGER.warning("Skipping coordinator update; unable to collect Backpack account data")
        else:
            self._push(payload)
            LOGGER.info("Pushed Backpack monitor snapshot for %s", self._cfg.label)
        self._process_adjustments()

    def run_forever(self) -> None:
        while True:
            started = time.time()
            try:
                self.run_once()
            except Exception as exc:
                LOGGER.warning("Monitor cycle failed: %s", exc)
            elapsed = time.time() - started
            sleep_for = max(0.0, self._cfg.poll_interval - elapsed)
            time.sleep(sleep_for)


def _build_backpack_client() -> Any:
    if BackpackClient is None:
        raise RuntimeError(f"BackpackClient import failed: {_IMPORT_ERROR}")

    public_key = os.getenv("BACKPACK_PUBLIC_KEY")
    secret_key = os.getenv("BACKPACK_SECRET_KEY")
    if not public_key or not secret_key:
        raise RuntimeError("BACKPACK_PUBLIC_KEY/BACKPACK_SECRET_KEY are required")

    # In this repo, exchanges.backpack.BackpackClient expects a config dict.
    # The client itself reads BACKPACK_PUBLIC_KEY/BACKPACK_SECRET_KEY from env,
    # so we only need to provide the minimal instrument metadata.
    try:
        return BackpackClient({"exchange": "backpack", "ticker": "BACKPACK", "contract_id": ""})
    except TypeError as exc:
        raise RuntimeError(f"Unable to construct BackpackClient(config): {exc}") from exc


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Backpack account monitor")
    parser.add_argument("--label", default=os.getenv("BACKPACK_LABEL", "main"))
    parser.add_argument("--coordinator-url", default=os.getenv("COORDINATOR_URL", "http://127.0.0.1:8000"))
    parser.add_argument("--agent-id", default=os.getenv("AGENT_ID", "backpack"))
    parser.add_argument("--poll-interval", type=float, default=float(os.getenv("POLL_INTERVAL", "5")))
    parser.add_argument("--timeout", type=float, default=float(os.getenv("REQUEST_TIMEOUT", "10")))
    parser.add_argument("--coordinator-username", default=os.getenv("COORDINATOR_USERNAME"))
    parser.add_argument("--coordinator-password", default=os.getenv("COORDINATOR_PASSWORD"))
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))

    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO), format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    cfg = BackpackMonitorConfig(
        label=str(args.label),
        coordinator_url=str(args.coordinator_url).rstrip("/"),
        agent_id=str(args.agent_id),
        poll_interval=float(args.poll_interval),
        request_timeout=float(args.timeout),
        coordinator_username=args.coordinator_username,
        coordinator_password=args.coordinator_password,
    )

    client = _build_backpack_client()
    monitor = BackpackAccountMonitor(cfg=cfg, client=client)
    monitor.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
