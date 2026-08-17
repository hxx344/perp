#!/usr/bin/env python3
"""Read-only Linux deployment checks for the Robinhood Lighter cycle.

This script never creates a signer, authenticates to an exchange, or submits an
order. It only validates local configuration and public network dependencies.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import shutil
import socket
import ssl
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.request
from decimal import Decimal, InvalidOperation, ROUND_UP
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Mapping


EXPECTED_ENDPOINTS = {
    "LIGHTER_ENVIRONMENT": "robinhood",
    "LIGHTER_BASE_URL": "https://api.rh.lighter.xyz",
    "LIGHTER_WS_URL": "wss://api.rh.lighter.xyz/stream",
    "LIGHTER_CHAIN_ID": "466324",
}

# The live quote check uses Binance as the strategy's reference feed. Apply a
# conservative haircut so a small venue divergence cannot pass min_quote at
# the exact boundary and then be rejected by Lighter.
REFERENCE_PRICE_HAIRCUT = Decimal("0.99")

NETWORK_HOSTS = (
    "api.rh.lighter.xyz",
    "fapi.binance.com",
    "fapi.asterdex.com",
    "fstream.asterdex.com",
)

HTTPS_PROBES = (
    ("Robinhood Lighter REST", "https://api.rh.lighter.xyz/api/v1/orderBooks"),
    ("Binance futures public API", "https://fapi.binance.com/fapi/v1/ping"),
    ("Aster futures public API", "https://fapi.asterdex.com/fapi/v1/exchangeInfo"),
)

SENSITIVE_ENV_NAMES = (
    "LIGHTER_API_PRIVATE_KEYS",
    "API_KEY_PRIVATE_KEYS",
    "API_KEY_PRIVATE_KEY",
    "L1_WALLET_PRIVATE_KEY",
    "LIGHTER_L1_PRIVATE_KEY",
)


class Report:
    def __init__(self) -> None:
        self.errors = 0
        self.warnings = 0

    def ok(self, message: str) -> None:
        print(f"[ OK ] {message}")

    def warn(self, message: str) -> None:
        self.warnings += 1
        print(f"[WARN] {message}")

    def error(self, message: str) -> None:
        self.errors += 1
        print(f"[FAIL] {message}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run read-only Robinhood Lighter Linux deployment checks"
    )
    parser.add_argument("--env-file", required=True, help="Strategy credential env file")
    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[2]),
        help="Repository checkout root",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python interpreter used to run the strategy",
    )
    parser.add_argument(
        "--skip-network",
        action="store_true",
        help="Skip public DNS/TLS/HTTPS probes (local checks still run)",
    )
    parser.add_argument("--ticker", help="Perpetual symbol to validate, for example BTC")
    parser.add_argument("--quantity", help="Order quantity to validate against live market limits")
    return parser.parse_args()


def check_linux(report: Report) -> None:
    if sys.platform != "linux":
        report.error(f"deployment target must be Linux, got {sys.platform}")
        return
    os_release = Path("/etc/os-release")
    if not os_release.is_file():
        report.error("/etc/os-release is missing; Ubuntu or Debian is required")
        return
    content = os_release.read_text(encoding="utf-8", errors="replace").casefold()
    if "ubuntu" not in content and "debian" not in content:
        report.error("unsupported Linux distribution; use Ubuntu or Debian")
        return
    report.ok("Ubuntu/Debian deployment host detected")


def check_python(report: Report, python: Path) -> None:
    if not python.is_file() or not os.access(python, os.X_OK):
        report.error(f"Python interpreter is not executable: {python}")
        return
    completed = subprocess.run(
        [str(python), "-c", "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if completed.returncode != 0:
        report.error("unable to execute the configured Python interpreter")
        return
    version = completed.stdout.strip()
    try:
        major, minor = (int(part) for part in version.split(".", 1))
    except ValueError:
        report.error(f"unable to parse Python version: {version!r}")
        return
    if (major, minor) < (3, 11):
        report.error(f"Python >= 3.11 is required, got {version}")
    else:
        report.ok(f"Python {version} meets the deployment requirement")


def check_env_permissions(report: Report, env_file: Path) -> bool:
    if env_file.is_symlink():
        report.error("credential env file must not be a symbolic link")
        return False
    if not env_file.is_file():
        report.error(f"credential env file does not exist: {env_file}")
        return False
    if not os.access(env_file, os.R_OK):
        report.error("credential env file is not readable by the service user")
        return False

    metadata = env_file.stat()
    permissions = stat.S_IMODE(metadata.st_mode)
    if permissions & 0o077:
        report.error(
            f"credential env file mode must not grant group/other access (current {permissions:04o})"
        )
    else:
        report.ok(f"credential env file permissions are restricted ({permissions:04o})")

    current_uid = getattr(os, "geteuid", lambda: metadata.st_uid)()
    if sys.platform != "linux":
        report.warn("credential ownership validation is only authoritative on Linux")
    elif current_uid == 0:
        report.warn("preflight is running as root; rerun it as the dedicated service user")
    elif metadata.st_uid != current_uid:
        report.error("credential env file is not owned by the current service user")
    else:
        report.ok("credential env file is owned by the service user")
    return True


def load_dotenv_values(report: Report, env_file: Path) -> Mapping[str, str | None]:
    try:
        dotenv = importlib.import_module("dotenv")
    except ImportError:
        report.error("python-dotenv is not installed in the strategy environment")
        return {}
    try:
        return dotenv.dotenv_values(env_file, interpolate=False)
    except Exception as exc:  # no secret-bearing exception text is emitted
        report.error(f"unable to parse credential env file ({type(exc).__name__})")
        return {}


def check_env_values(report: Report, values: Mapping[str, str | None]) -> None:
    for name, expected in EXPECTED_ENDPOINTS.items():
        actual = str(values.get(name) or "").strip().rstrip("/")
        normalized_expected = expected.rstrip("/")
        if actual != normalized_expected:
            report.error(f"{name} must be exactly {expected}")
        else:
            report.ok(f"{name} matches the Robinhood profile")

    account_index = str(values.get("LIGHTER_ACCOUNT_INDEX") or "").strip()
    try:
        parsed_account_index = int(account_index)
        if parsed_account_index < 0:
            raise ValueError
    except ValueError:
        report.error("LIGHTER_ACCOUNT_INDEX must be a non-negative integer")
    else:
        report.ok("LIGHTER_ACCOUNT_INDEX is configured")

    raw_keys = str(values.get("LIGHTER_API_PRIVATE_KEYS") or "").strip()
    if not raw_keys:
        report.error("LIGHTER_API_PRIVATE_KEYS is required")
    else:
        try:
            key_map = json.loads(raw_keys)
        except (TypeError, ValueError):
            report.error("LIGHTER_API_PRIVATE_KEYS must be a valid JSON object")
        else:
            valid = isinstance(key_map, dict) and bool(key_map)
            if valid:
                for raw_index, raw_key in key_map.items():
                    try:
                        index = int(raw_index)
                    except (TypeError, ValueError):
                        valid = False
                        break
                    key = str(raw_key).strip()
                    if not (4 <= index <= 254):
                        valid = False
                        break
                    if not (
                        len(key) == 66
                        and key.startswith("0x")
                        and all(character in "0123456789abcdefABCDEF" for character in key[2:])
                    ):
                        valid = False
                        break
            if not valid:
                report.error(
                    "LIGHTER_API_PRIVATE_KEYS must contain key indexes 4..254 and 32-byte hex private keys"
                )
            else:
                report.ok(f"Lighter API key map contains {len(key_map)} structurally valid key(s)")

    forbidden = ("L1_WALLET_PRIVATE_KEY", "LIGHTER_L1_PRIVATE_KEY")
    present_forbidden = [name for name in forbidden if str(values.get(name) or "").strip()]
    if present_forbidden:
        report.error("Core L1 wallet credentials must be absent in Robinhood mode")
    else:
        report.ok("Core L1 auto-provisioning credentials are absent")

    legacy = ("API_KEY_PRIVATE_KEYS", "API_KEY_PRIVATE_KEY", "LIGHTER_API_KEY_INDEX")
    if any(str(values.get(name) or "").strip() for name in legacy):
        report.warn("legacy Lighter credential variables are set; remove them to keep one credential source")

    if any(str(values.get(name) or "").strip() for name in ("ASTER_API_KEY", "ASTER_SECRET_KEY")):
        report.warn("Aster credentials are set but are unnecessary for virtual Aster maker mode")


def check_imports_and_help(report: Report, python: Path, project_root: Path) -> None:
    modules = ("aiohttp", "dotenv", "lighter", "web3", "websockets")
    code = "\n".join(f"import {name}" for name in modules)
    completed = subprocess.run(
        [str(python), "-c", code],
        cwd=project_root,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if completed.returncode:
        report.error("one or more required Python packages cannot be imported")
    else:
        report.ok("required Python packages import successfully")

    clean_env = os.environ.copy()
    for name in SENSITIVE_ENV_NAMES + tuple(EXPECTED_ENDPOINTS):
        clean_env.pop(name, None)
    completed = subprocess.run(
        [str(python), "-m", "strategies.aster_lighter_cycle", "--help"],
        cwd=project_root,
        env=clean_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
        check=False,
    )
    if completed.returncode:
        report.error("strategy CLI cannot be imported and parsed")
    else:
        report.ok("strategy CLI help completed without starting the strategy")


def check_tls_host(report: Report, host: str, timeout: float = 7.0) -> None:
    try:
        addresses = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
        if not addresses:
            raise OSError("no address records")
        context = ssl.create_default_context()
        with socket.create_connection((host, 443), timeout=timeout) as raw_socket:
            with context.wrap_socket(raw_socket, server_hostname=host):
                pass
    except (OSError, ssl.SSLError, socket.gaierror):
        report.error(f"DNS/TCP/TLS connection failed for {host}:443")
    else:
        report.ok(f"DNS/TCP/TLS connection succeeded for {host}:443")


def check_https(report: Report, label: str, url: str, timeout: float = 10.0) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": "perp-robinhood-preflight/1"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = response.status
            server_date = response.headers.get("Date")
            response.read(4096)
    except (urllib.error.URLError, OSError, TimeoutError):
        report.error(f"HTTPS probe failed for {label}")
        return
    if not 200 <= status < 400:
        report.error(f"HTTPS probe returned status {status} for {label}")
        return
    report.ok(f"HTTPS probe succeeded for {label} (HTTP {status})")

    if server_date:
        try:
            remote_time = parsedate_to_datetime(server_date).timestamp()
            skew = abs(time.time() - remote_time)
        except (TypeError, ValueError, OverflowError):
            report.warn(f"could not parse server clock from {label}")
        else:
            if skew > 60:
                report.error(f"local clock differs from {label} by more than 60 seconds")
            else:
                report.ok(f"local clock is within 60 seconds of {label}")


def validate_market_quantity(
    report: Report,
    *,
    ticker: str,
    quantity_raw: str,
    order_books: object,
    reference_price_raw: object,
) -> None:
    try:
        quantity = Decimal(str(quantity_raw))
        reference_price = Decimal(str(reference_price_raw))
    except (InvalidOperation, TypeError, ValueError):
        report.error("market quantity or reference price is not a valid decimal")
        return
    if not quantity.is_finite() or quantity <= 0:
        report.error("market quantity must be a positive finite decimal")
        return
    if not reference_price.is_finite() or reference_price <= 0:
        report.error("market reference price must be a positive finite decimal")
        return

    normalized_ticker = ticker.strip().upper()
    entries = order_books if isinstance(order_books, list) else []
    market = next(
        (
            entry
            for entry in entries
            if isinstance(entry, dict)
            and str(entry.get("symbol") or "").strip().upper() == normalized_ticker
            and str(entry.get("market_type") or "").strip().casefold() == "perp"
        ),
        None,
    )
    if market is None:
        report.error(f"Robinhood Lighter perpetual market was not found for {normalized_ticker}")
        return
    if str(market.get("status") or "").strip().casefold() != "active":
        report.error(f"Robinhood Lighter {normalized_ticker} perpetual market is not active")
        return

    try:
        size_decimals = int(market["supported_size_decimals"])
        min_base = Decimal(str(market["min_base_amount"]))
        min_quote = Decimal(str(market["min_quote_amount"]))
        if size_decimals < 0 or size_decimals > 18 or min_base < 0 or min_quote < 0:
            raise ValueError
    except (InvalidOperation, KeyError, TypeError, ValueError):
        report.error(f"Robinhood Lighter {normalized_ticker} market limits are malformed")
        return

    size_step = Decimal(1).scaleb(-size_decimals)
    if quantity % size_step != 0:
        report.error(
            f"quantity {quantity} does not align to the {normalized_ticker} size step {size_step}"
        )
        return

    conservative_price = reference_price * REFERENCE_PRICE_HAIRCUT
    required = max(min_base, min_quote / conservative_price)
    required_steps = (required / size_step).to_integral_value(rounding=ROUND_UP)
    required = required_steps * size_step
    if quantity < required:
        report.error(
            f"quantity {quantity} is below the current executable minimum {required} {normalized_ticker}"
        )
        return
    report.ok(
        f"quantity {quantity} meets the live {normalized_ticker} base/quote minimum and size step"
    )


def check_live_market_quantity(
    report: Report,
    *,
    ticker: str,
    quantity: str,
    timeout: float = 10.0,
) -> None:
    normalized_ticker = ticker.strip().upper()
    if not normalized_ticker or not normalized_ticker.replace("_", "").replace("-", "").isalnum():
        report.error("ticker contains unsupported characters")
        return

    urls = (
        "https://api.rh.lighter.xyz/api/v1/orderBooks",
        f"https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol={normalized_ticker}USDT",
    )
    payloads = []
    try:
        for url in urls:
            request = urllib.request.Request(
                url,
                headers={"User-Agent": "perp-robinhood-preflight/1"},
            )
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read(1_000_001)
                if len(raw) > 1_000_000:
                    raise ValueError("oversized response")
                payloads.append(json.loads(raw))
    except (json.JSONDecodeError, OSError, TimeoutError, urllib.error.URLError, ValueError):
        report.error("unable to load live market limits for quantity validation")
        return

    lighter_payload, binance_payload = payloads
    order_books = lighter_payload.get("order_books") if isinstance(lighter_payload, dict) else None
    reference_price = (
        binance_payload.get("bidPrice") if isinstance(binance_payload, dict) else None
    )
    validate_market_quantity(
        report,
        ticker=normalized_ticker,
        quantity_raw=quantity,
        order_books=order_books,
        reference_price_raw=reference_price,
    )


def check_ntp(report: Report) -> None:
    timedatectl = shutil.which("timedatectl")
    if timedatectl:
        completed = subprocess.run(
            [timedatectl, "show", "--property=NTPSynchronized", "--value"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if completed.returncode == 0:
            synchronized = completed.stdout.strip().casefold()
            if synchronized == "yes":
                report.ok("systemd reports the clock is NTP synchronized")
            else:
                report.error("systemd reports the clock is not NTP synchronized")
            return

    chronyc = shutil.which("chronyc")
    if chronyc:
        completed = subprocess.run(
            [chronyc, "tracking"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if completed.returncode == 0 and "leap status" in completed.stdout.casefold():
            if "not synchronised" in completed.stdout.casefold():
                report.error("chrony reports that the clock is not synchronized")
            else:
                report.ok("chrony is running and reports clock tracking")
            return

    report.warn("could not query an NTP daemon; HTTPS clock comparison is the fallback")


def main() -> int:
    args = parse_args()
    report = Report()
    # Keep the final path component unresolved so symlink rejection remains
    # effective and a venv/bin/python symlink continues to select that venv.
    env_file = Path(os.path.abspath(os.path.expanduser(args.env_file)))
    project_root = Path(args.project_root).expanduser().resolve()
    python = Path(os.path.abspath(os.path.expanduser(args.python)))

    print("Robinhood Lighter read-only deployment preflight")
    print("No authenticated exchange requests or orders are sent.\n")

    check_linux(report)
    if not project_root.is_dir() or not (project_root / "strategies").is_dir():
        report.error(f"project root is invalid: {project_root}")
    else:
        report.ok("project checkout contains the strategy package")
    check_python(report, python)

    if check_env_permissions(report, env_file):
        values = load_dotenv_values(report, env_file)
        if values:
            check_env_values(report, values)

    check_imports_and_help(report, python, project_root)

    if args.skip_network:
        report.warn("network probes were skipped by operator request")
    else:
        check_ntp(report)
        for host in NETWORK_HOSTS:
            check_tls_host(report, host)
        for label, url in HTTPS_PROBES:
            check_https(report, label, url)
        if args.ticker and args.quantity:
            check_live_market_quantity(
                report,
                ticker=args.ticker,
                quantity=args.quantity,
            )

    if bool(args.ticker) != bool(args.quantity):
        report.error("--ticker and --quantity must be provided together")

    print(f"\nPreflight result: {report.errors} failure(s), {report.warnings} warning(s)")
    if report.errors:
        print("Trading start is blocked until every failure is resolved.")
        return 1
    print("Read-only checks passed. This result does not authorize or start live trading.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
