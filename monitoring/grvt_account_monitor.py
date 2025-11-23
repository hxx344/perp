#!/usr/bin/env python3
"""GRVT multi-account position monitor.

This utility polls the GRVT REST API for multiple trading accounts, aggregates
PnL/position information, and forwards a compact summary to the hedge
coordinator so it can be displayed on the dashboard.
"""

from __future__ import annotations

import argparse
import importlib
import logging
import os
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests

getcontext().prec = 28

LOGGER = logging.getLogger("monitor.grvt_accounts")
DEFAULT_POLL_SECONDS = 15.0
DEFAULT_TIMEOUT_SECONDS = 10.0
MAX_ACCOUNT_POSITIONS = 12
def load_env_files(paths: Sequence[str]) -> None:
    if not paths:
        return
    for raw_path in paths:
        if not raw_path:
            continue
        env_path = Path(raw_path).expanduser()
        if not env_path.exists():
            LOGGER.debug("Env file %s not found; skipping", env_path)
            continue
        loaded_count = 0
        try:
            with env_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    stripped = line.strip()
                    if not stripped or stripped.startswith("#"):
                        continue
                    if "=" not in stripped:
                        continue
                    key, value = stripped.split("=", 1)
                    key = key.strip()
                    if not key:
                        continue
                    value = value.strip()
                    if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
                        value = value[1:-1]
                    if key not in os.environ:
                        os.environ[key] = value
                        loaded_count += 1
        except Exception as exc:
            LOGGER.warning("Failed to load env file %s: %s", env_path, exc)
            continue
        LOGGER.info("Loaded %s variables from %s", loaded_count, env_path)



@dataclass(frozen=True)
class AccountCredentials:
    label: str
    trading_account_id: str
    private_key: str
    api_key: str
    environment: str


@dataclass
class AccountSession:
    label: str
    client: Any


def import_grvt_sdk():  # pragma: no cover - thin import wrapper
    try:
        grvt_ccxt = importlib.import_module("pysdk.grvt_ccxt")
        grvt_env_module = importlib.import_module("pysdk.grvt_ccxt_env")
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise ImportError(
            "grvt_account_monitor requires grvt-pysdk. Install it via 'pip install grvt-pysdk'."
        ) from exc
    return grvt_ccxt.GrvtCcxt, grvt_env_module.GrvtEnv


def decimal_from(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return Decimal(text)
        except InvalidOperation:
            return None
    return None


def decimal_to_str(value: Optional[Decimal]) -> Optional[str]:
    if value is None:
        return None
    try:
        return format(value, "f")
    except Exception:
        return str(value)


def extract_from_paths(source: Dict[str, Any], *paths: Sequence[str]) -> Optional[Any]:
    for path in paths:
        cursor: Any = source
        valid = True
        for key in path:
            if isinstance(cursor, dict) and key in cursor:
                cursor = cursor[key]
            else:
                valid = False
                break
        if valid and cursor is not None:
            return cursor
    return None


def guess_symbol(entry: Dict[str, Any]) -> str:
    fields = [
        entry.get("symbol"),
        entry.get("instrument"),
        entry.get("asset"),
        extract_from_paths(entry, ("info", "instrument")),
        extract_from_paths(entry, ("info", "symbol")),
    ]
    for candidate in fields:
        if candidate:
            return str(candidate)
    return ""


def base_asset(symbol: str) -> Optional[str]:
    if not symbol:
        return None
    text = symbol.replace(":", "/")
    parts = text.split("/")
    if not parts:
        return None
    return parts[0].upper().strip() or None


def normalize_side(entry: Dict[str, Any], size: Optional[Decimal]) -> Optional[str]:
    side_fields = [
        entry.get("side"),
        entry.get("position_side"),
        entry.get("positionSide"),
        entry.get("direction"),
        extract_from_paths(entry, ("info", "side")),
        extract_from_paths(entry, ("info", "direction")),
    ]
    for candidate in side_fields:
        if candidate:
            text = str(candidate).strip().lower()
            if text in {"long", "buy", "bid"}:
                return "LONG"
            if text in {"short", "sell", "ask"}:
                return "SHORT"
    if size is not None and size < 0:
        return "SHORT"
    if size is not None and size > 0:
        return "LONG"
    return None


def determine_signed_size(size: Optional[Decimal], side: Optional[str]) -> Optional[Decimal]:
    if size is None:
        return None
    if side == "SHORT" and size > 0:
        return -size
    if side == "LONG" and size < 0:
        return size
    return size


def compute_position_pnl(entry: Dict[str, Any]) -> Tuple[Decimal, Dict[str, Any]]:
    size_candidates = [
        ("contracts",),
        ("size",),
        ("amount",),
        ("positionAmt",),
        ("net_size",),
        ("position_size",),
        ("quantity",),
        ("info", "position_size"),
        ("info", "contracts"),
    ]
    entry_price_candidates = [
        ("entry_price",),
        ("entryPrice",),
        ("average_price",),
        ("avg_entry_price",),
        ("info", "entry_price"),
        ("info", "average_entry_price"),
    ]
    mark_price_candidates = [
        ("mark_price",),
        ("markPrice",),
        ("last_price",),
        ("info", "mark_price"),
        ("info", "markPrice"),
        ("info", "last_price"),
    ]
    pnl_candidates = [
        ("unrealizedPnl",),
        ("unrealized_pnl",),
        ("pnl",),
        ("info", "unrealizedPnl"),
        ("info", "unrealized_pnl"),
        ("info", "pnl"),
    ]

    raw_size = extract_from_paths(entry, *size_candidates)
    size_value = decimal_from(raw_size)
    side = normalize_side(entry, size_value)
    signed_size = determine_signed_size(size_value, side)

    raw_entry = extract_from_paths(entry, *entry_price_candidates)
    entry_price = decimal_from(raw_entry)
    raw_mark = extract_from_paths(entry, *mark_price_candidates)
    mark_price = decimal_from(raw_mark)
    raw_pnl = extract_from_paths(entry, *pnl_candidates)
    pnl_value = decimal_from(raw_pnl)

    if pnl_value is None and None not in (signed_size, entry_price, mark_price):
        try:
            pnl_value = (mark_price - entry_price) * signed_size  # type: ignore[arg-type]
        except Exception:
            pnl_value = Decimal("0")

    if pnl_value is None:
        pnl_value = Decimal("0")

    payload = {
        "symbol": guess_symbol(entry) or "--",
        "side": side,
        "net_size": decimal_to_str(signed_size),
        "entry_price": decimal_to_str(entry_price),
        "mark_price": decimal_to_str(mark_price),
        "pnl": decimal_to_str(pnl_value),
    }
    return pnl_value, payload


def load_account(label: str) -> AccountCredentials:
    label_clean = label.strip()
    env_prefix = "GRVT" if not label_clean or label_clean.lower() == "default" else f"GRVT_{label_clean.upper()}"
    def _env(key: str) -> Optional[str]:
        return os.getenv(f"{env_prefix}_{key}")

    trading_account_id = _env("TRADING_ACCOUNT_ID")
    private_key = _env("PRIVATE_KEY")
    api_key = _env("API_KEY")
    if trading_account_id is None or private_key is None or api_key is None:
        raise ValueError(
            f"Account '{label}' missing required env vars. Expected {env_prefix}_TRADING_ACCOUNT_ID / PRIVATE_KEY / API_KEY."
        )

    env_name = (_env("ENVIRONMENT") or _env("ENV") or "prod").strip().lower()

    credentials = AccountCredentials(
        label=label_clean or "default",
        trading_account_id=trading_account_id.strip(),
        private_key=private_key.strip(),
        api_key=api_key.strip(),
        environment=env_name,
    )
    return credentials


def build_session(creds: AccountCredentials) -> AccountSession:
    GrvtCcxtCls, GrvtEnvCls = import_grvt_sdk()
    env_map = {
        "prod": GrvtEnvCls.PROD,
        "production": GrvtEnvCls.PROD,
        "testnet": GrvtEnvCls.TESTNET,
        "staging": GrvtEnvCls.STAGING,
        "dev": GrvtEnvCls.DEV,
    }
    resolved_env = env_map.get(creds.environment.lower(), GrvtEnvCls.PROD)
    client = GrvtCcxtCls(
        env=resolved_env,
        parameters={
            "trading_account_id": creds.trading_account_id,
            "private_key": creds.private_key,
            "api_key": creds.api_key,
        },
    )
    return AccountSession(label=creds.label, client=client)


class GrvtAccountMonitor:
    def __init__(
        self,
        *,
        sessions: Sequence[AccountSession],
        coordinator_url: str,
        agent_id: str,
        poll_interval: float,
        request_timeout: float,
        max_positions: int,
    ) -> None:
        self._sessions = list(sessions)
        self._coordinator_url = coordinator_url.rstrip("/")
        self._agent_id = agent_id
        self._poll_interval = max(poll_interval, 2.0)
        self._timeout = max(request_timeout, 1.0)
        self._max_positions = max(1, max_positions)
        self._http = requests.Session()

    def _collect(self) -> Dict[str, Any]:
        accounts_payload: List[Dict[str, Any]] = []
        total_pnl = Decimal("0")
        total_eth = Decimal("0")
        total_btc = Decimal("0")
        timestamp = time.time()

        for session in self._sessions:
            try:
                positions = session.client.fetch_positions() or []
            except Exception as exc:  # pragma: no cover - network path
                LOGGER.exception("Failed to fetch positions for %s: %s", session.label, exc)
                continue

            account_total = Decimal("0")
            account_eth = Decimal("0")
            account_btc = Decimal("0")
            position_rows: List[Dict[str, Any]] = []

            for raw_position in positions:
                pnl_value, position_payload = compute_position_pnl(raw_position)
                account_total += pnl_value
                base = base_asset(position_payload.get("symbol", ""))
                if base == "ETH":
                    account_eth += pnl_value
                elif base == "BTC":
                    account_btc += pnl_value
                position_rows.append(position_payload)

            position_rows = position_rows[: self._max_positions]
            total_pnl += account_total
            total_eth += account_eth
            total_btc += account_btc

            accounts_payload.append(
                {
                    "name": session.label,
                    "total_pnl": decimal_to_str(account_total),
                    "eth_pnl": decimal_to_str(account_eth),
                    "btc_pnl": decimal_to_str(account_btc),
                    "positions": position_rows,
                    "updated_at": timestamp,
                }
            )

        summary = {
            "account_count": len(accounts_payload),
            "total_pnl": decimal_to_str(total_pnl),
            "eth_pnl": decimal_to_str(total_eth),
            "btc_pnl": decimal_to_str(total_btc),
            "updated_at": timestamp,
        }

        payload = {
            "agent_id": self._agent_id,
            "instrument": "GRVT multi-account",
            "grvt_accounts": {
                "updated_at": timestamp,
                "summary": summary,
                "accounts": accounts_payload,
            },
        }
        return payload

    def _push(self, payload: Dict[str, Any]) -> None:
        endpoint = f"{self._coordinator_url}/update"
        response = self._http.post(endpoint, json=payload, timeout=self._timeout)
        if response.status_code >= 400:
            raise RuntimeError(f"Coordinator rejected payload: HTTP {response.status_code} {response.text}")

    def run_once(self) -> None:
        payload = self._collect()
        if not payload["grvt_accounts"]["accounts"]:
            LOGGER.warning("No GRVT account data collected; skipping coordinator update")
            return
        self._push(payload)
        LOGGER.info(
            "Pushed GRVT monitor snapshot (%s accounts, total PnL %s)",
            payload["grvt_accounts"]["summary"].get("account_count"),
            payload["grvt_accounts"]["summary"].get("total_pnl"),
        )

    def run_forever(self) -> None:
        LOGGER.info("Starting GRVT monitor for %s accounts", len(self._sessions))
        while True:
            start = time.time()
            try:
                self.run_once()
            except Exception as exc:  # pragma: no cover - continuous loop
                LOGGER.exception("Monitor iteration failed: %s", exc)
            elapsed = time.time() - start
            sleep_for = max(0.5, self._poll_interval - elapsed)
            time.sleep(sleep_for)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor multiple GRVT accounts and forward PnL data to the dashboard")
    parser.add_argument(
        "--coordinator-url",
        required=True,
        help="Hedge coordinator base URL, e.g. http://localhost:8899",
    )
    parser.add_argument(
        "--account",
        action="append",
        required=True,
        help=(
            "Account label / env prefix. '--account alpha' expects GRVT_ALPHA_TRADING_ACCOUNT_ID / PRIVATE_KEY / API_KEY. "
            "Use '--account default' to reuse the base GRVT_* variables."
        ),
    )
    parser.add_argument("--agent-id", default="grvt-monitor", help="Agent identifier reported to the coordinator")
    parser.add_argument("--poll-interval", type=float, default=DEFAULT_POLL_SECONDS, help="Seconds between refreshes")
    parser.add_argument("--request-timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout for coordinator updates")
    parser.add_argument("--max-positions", type=int, default=MAX_ACCOUNT_POSITIONS, help="Maximum positions to include per account in the payload")
    parser.add_argument("--once", action="store_true", help="Collect and push a single snapshot, then exit")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    parser.add_argument(
        "--env-file",
        action="append",
        help="Env file to preload (defaults to .env if present). Repeat to load multiple files.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    env_files = args.env_file if args.env_file is not None else [".env"]
    load_env_files(env_files)

    try:
        credentials = [load_account(label) for label in args.account]
    except ValueError as exc:
        LOGGER.error(str(exc))
        sys.exit(1)

    sessions = [build_session(creds) for creds in credentials]
    monitor = GrvtAccountMonitor(
        sessions=sessions,
        coordinator_url=args.coordinator_url,
        agent_id=args.agent_id,
        poll_interval=args.poll_interval,
        request_timeout=args.request_timeout,
        max_positions=args.max_positions,
    )

    if args.once:
        monitor.run_once()
    else:
        monitor.run_forever()


if __name__ == "__main__":  # pragma: no cover - manual execution
    main()
