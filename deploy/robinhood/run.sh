#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
PROJECT_ROOT="${PERP_PROJECT_ROOT:-$(cd -- "${SCRIPT_DIR}/../.." && pwd -P)}"
VENV="${PERP_VENV:-${PROJECT_ROOT}/.venv}"
PYTHON="${PERP_PYTHON:-${VENV}/bin/python}"
ENV_FILE="${PERP_ENV_FILE:-/etc/perp/robinhood.env}"

TICKER="${PERP_TICKER:-BTC}"
QUANTITY="${PERP_QUANTITY:-}"
DIRECTION="${PERP_DIRECTION:-buy}"
LEVERAGE="${PERP_LIGHTER_LEVERAGE:-2}"
SLIPPAGE="${PERP_SLIPPAGE:-0.02}"
LIGHTER_MAX_WAIT="${PERP_LIGHTER_MAX_WAIT:-10}"
ASTER_MAKER_DEPTH="${PERP_ASTER_MAKER_DEPTH:-10}"
RUN_MODE="${PERP_RUN_MODE:-canary}"
CONTINUOUS_ACK="${PERP_CONTINUOUS_ACK:-}"
CONFIRM_LIVE=0

if [[ "${PYTHON}" != */* ]]; then
  PYTHON="$(command -v "${PYTHON}" 2>/dev/null || true)"
fi

usage() {
  cat <<'EOF'
Usage: deploy/robinhood/run.sh [options] --confirm-live

This command sends real IOC orders to Robinhood Lighter. Aster maker legs are
always virtual and use Binance public prices.

Options:
  --env-file PATH          Credential env file (default: /etc/perp/robinhood.env)
  --quantity DECIMAL       Lighter quantity; required
  --ticker SYMBOL          Market symbol (default: BTC)
  --direction buy|sell     Initial virtual maker direction (default: buy)
  --leverage INTEGER       Lighter leverage, 1..125 (default: 2)
  --slippage PERCENT       Lighter IOC slippage percentage (default: 0.02)
  --lighter-max-wait SEC   Lighter fill timeout (default: 10)
  --aster-maker-depth N    Virtual maker depth, 1..500 (default: 10)
  --mode canary|continuous Canary forces exactly one cycle (default: canary)
  --confirm-live           Required acknowledgement that real orders are sent
  -h, --help               Show this help

Continuous mode also requires:
  PERP_CONTINUOUS_ACK=I_ACKNOWLEDGE_CONTINUOUS_LIVE_TRADING
EOF
}

while (($#)); do
  case "$1" in
    --env-file)
      [[ $# -ge 2 ]] || { printf '%s\n' 'run: --env-file requires a value' >&2; exit 2; }
      ENV_FILE="$2"
      shift 2
      ;;
    --quantity)
      [[ $# -ge 2 ]] || { printf '%s\n' 'run: --quantity requires a value' >&2; exit 2; }
      QUANTITY="$2"
      shift 2
      ;;
    --ticker)
      [[ $# -ge 2 ]] || { printf '%s\n' 'run: --ticker requires a value' >&2; exit 2; }
      TICKER="$2"
      shift 2
      ;;
    --direction)
      [[ $# -ge 2 ]] || { printf '%s\n' 'run: --direction requires a value' >&2; exit 2; }
      DIRECTION="$2"
      shift 2
      ;;
    --leverage)
      [[ $# -ge 2 ]] || { printf '%s\n' 'run: --leverage requires a value' >&2; exit 2; }
      LEVERAGE="$2"
      shift 2
      ;;
    --slippage)
      [[ $# -ge 2 ]] || { printf '%s\n' 'run: --slippage requires a value' >&2; exit 2; }
      SLIPPAGE="$2"
      shift 2
      ;;
    --lighter-max-wait)
      [[ $# -ge 2 ]] || { printf '%s\n' 'run: --lighter-max-wait requires a value' >&2; exit 2; }
      LIGHTER_MAX_WAIT="$2"
      shift 2
      ;;
    --aster-maker-depth)
      [[ $# -ge 2 ]] || { printf '%s\n' 'run: --aster-maker-depth requires a value' >&2; exit 2; }
      ASTER_MAKER_DEPTH="$2"
      shift 2
      ;;
    --mode)
      [[ $# -ge 2 ]] || { printf '%s\n' 'run: --mode requires a value' >&2; exit 2; }
      RUN_MODE="$2"
      shift 2
      ;;
    --confirm-live)
      CONFIRM_LIVE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'run: unknown option: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -x "${PYTHON}" ]]; then
  printf 'run: Python interpreter is not executable: %s\n' "${PYTHON}" >&2
  exit 1
fi
if [[ ! -d "${PROJECT_ROOT}/strategies" ]]; then
  printf 'run: invalid project root: %s\n' "${PROJECT_ROOT}" >&2
  exit 1
fi
if [[ ! -d "${PROJECT_ROOT}/logs" ]] || [[ ! -w "${PROJECT_ROOT}/logs" ]]; then
  printf 'run: log directory must exist and be writable: %s/logs\n' "${PROJECT_ROOT}" >&2
  exit 1
fi
if [[ -z "${QUANTITY}" ]]; then
  printf '%s\n' 'run: quantity is required; set PERP_QUANTITY or use --quantity' >&2
  exit 2
fi
if [[ ! "${TICKER}" =~ ^[A-Za-z0-9_-]+$ ]]; then
  printf '%s\n' 'run: ticker contains unsupported characters' >&2
  exit 2
fi
TICKER="${TICKER^^}"
if [[ "${DIRECTION}" != "buy" && "${DIRECTION}" != "sell" ]]; then
  printf '%s\n' 'run: direction must be buy or sell' >&2
  exit 2
fi
if [[ ! "${LEVERAGE}" =~ ^[0-9]+$ ]] || ((LEVERAGE < 1 || LEVERAGE > 125)); then
  printf '%s\n' 'run: leverage must be an integer from 1 through 125' >&2
  exit 2
fi
if [[ ! "${ASTER_MAKER_DEPTH}" =~ ^[0-9]+$ ]] || ((ASTER_MAKER_DEPTH < 1 || ASTER_MAKER_DEPTH > 500)); then
  printf '%s\n' 'run: Aster maker depth must be an integer from 1 through 500' >&2
  exit 2
fi

"${PYTHON}" - "${QUANTITY}" "${SLIPPAGE}" "${LIGHTER_MAX_WAIT}" <<'PY'
from decimal import Decimal, InvalidOperation
import sys

names = ("quantity", "slippage", "lighter max wait")
try:
    values = [Decimal(item) for item in sys.argv[1:]]
except InvalidOperation:
    raise SystemExit("run: quantity, slippage, and wait must be decimal numbers")
if not values[0].is_finite() or values[0] <= 0:
    raise SystemExit("run: quantity must be a positive finite decimal")
if not values[1].is_finite() or values[1] < 0 or values[1] > 1:
    raise SystemExit("run: slippage must be between 0 and 1 percent")
if not values[2].is_finite() or values[2] <= 0:
    raise SystemExit("run: Lighter max wait must be a positive finite number")
PY

case "${RUN_MODE}" in
  canary)
    CYCLES=1
    ;;
  continuous)
    CYCLES=0
    if [[ "${CONTINUOUS_ACK}" != "I_ACKNOWLEDGE_CONTINUOUS_LIVE_TRADING" ]]; then
      printf '%s\n' 'run: continuous mode requires PERP_CONTINUOUS_ACK=I_ACKNOWLEDGE_CONTINUOUS_LIVE_TRADING' >&2
      exit 2
    fi
    ;;
  *)
    printf '%s\n' 'run: mode must be canary or continuous' >&2
    exit 2
    ;;
esac

if ((CONFIRM_LIVE != 1)); then
  printf '%s\n' 'run: live trading was not acknowledged; rerun with --confirm-live' >&2
  exit 2
fi

PREFLIGHT_ARGS=(
  --env-file "${ENV_FILE}"
  --project-root "${PROJECT_ROOT}"
  --python "${PYTHON}"
  --ticker "${TICKER}"
  --quantity "${QUANTITY}"
)
"${PYTHON}" "${SCRIPT_DIR}/preflight.py" "${PREFLIGHT_ARGS[@]}"

printf 'Starting Robinhood Lighter %s run: ticker=%s quantity=%s direction=%s cycles=%s leverage=%s\n' \
  "${RUN_MODE}" "${TICKER}" "${QUANTITY}" "${DIRECTION}" "${CYCLES}" "${LEVERAGE}"
printf '%s\n' 'Aster legs are virtual; Lighter IOC legs are real.'

cd -- "${PROJECT_ROOT}"
exec env \
  -u LIGHTER_ENVIRONMENT \
  -u LIGHTER_ENDPOINT_PROFILE \
  -u LIGHTER_BASE_URL \
  -u LIGHTER_WS_URL \
  -u LIGHTER_CHAIN_ID \
  -u LIGHTER_ACCOUNT_INDEX \
  -u LIGHTER_API_PRIVATE_KEYS \
  -u API_KEY_PRIVATE_KEYS \
  -u API_KEY_PRIVATE_KEY \
  -u LIGHTER_API_KEY_INDEX \
  -u L1_WALLET_PRIVATE_KEY \
  -u LIGHTER_L1_PRIVATE_KEY \
  "${PYTHON}" -m strategies.aster_lighter_cycle \
    --env-file "${ENV_FILE}" \
    --lighter-environment robinhood \
    --aster-ticker "${TICKER}" \
    --lighter-ticker "${TICKER}" \
    --quantity "${QUANTITY}" \
    --lighter-leverage "${LEVERAGE}" \
    --direction "${DIRECTION}" \
    --virtual-aster-maker \
    --virtual-maker-price-source bn \
    --aster-maker-depth "${ASTER_MAKER_DEPTH}" \
    --slippage "${SLIPPAGE}" \
    --lighter-max-wait "${LIGHTER_MAX_WAIT}" \
    --preserve-initial-position \
    --cycles "${CYCLES}"
