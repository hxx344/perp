#!/usr/bin/env bash
set -Eeuo pipefail

# One-command setup for an existing, clean Robinhood Lighter checkout.
# Installation and preflight are read-only with respect to the exchange. A
# live canary requires both --run-canary and --confirm-live.

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
PROJECT_ROOT="${PERP_PROJECT_ROOT:-$(cd -- "${SCRIPT_DIR}/../.." && pwd -P)}"
PYTHON_REQUEST="${PYTHON_BIN:-python3}"
ENV_FILE="${PERP_ENV_FILE:-/etc/perp/robinhood.env}"
SERVICE_ENV_FILE="${PERP_SERVICE_ENV_FILE:-/etc/perp/robinhood-service.env}"
SERVICE_USER="${PERP_SERVICE_USER:-perp}"
SERVICE_GROUP="${PERP_SERVICE_GROUP:-perp}"
TICKER="${PERP_TICKER:-BTC}"
QUANTITY="${PERP_QUANTITY:-}"
DIRECTION="${PERP_DIRECTION:-buy}"
LEVERAGE="${PERP_LIGHTER_LEVERAGE:-2}"
SLIPPAGE="${PERP_SLIPPAGE:-0.02}"
LIGHTER_MAX_WAIT="${PERP_LIGHTER_MAX_WAIT:-10}"
ASTER_MAKER_DEPTH="${PERP_ASTER_MAKER_DEPTH:-10}"
INSTALL_OS_PACKAGES=1
INSTALL_DEPENDENCIES=1
INSTALL_SYSTEMD=1
RUN_CANARY=0
CONFIRM_LIVE=0

usage() {
  cat <<'EOF'
Usage: sudo bash deploy/robinhood/quickstart.sh [options]

Installs the Robinhood Lighter runtime for an existing clean checkout, creates
the credential template, renders systemd/logrotate, and runs read-only preflight.
It never pulls code, starts systemd, or sends an order unless both
--run-canary and --confirm-live are supplied.

Options:
  --project-root PATH       Checkout root (default: inferred from this script)
  --python COMMAND          Python >= 3.11 (default: python3)
  --env-file PATH           Secret credential file (default: /etc/perp/robinhood.env)
  --service-env PATH        Non-secret service env (default: /etc/perp/robinhood-service.env)
  --ticker SYMBOL           Market symbol (default: BTC)
  --quantity DECIMAL        Quantity for live market preflight
  --direction buy|sell      Canary direction (default: buy)
  --leverage INTEGER        Lighter leverage (default: 2)
  --slippage PERCENT        IOC slippage percent (default: 0.02)
  --lighter-max-wait SEC    IOC fill timeout (default: 10)
  --aster-maker-depth N     Virtual Aster depth (default: 10)
  --skip-os-packages        Do not run apt-get (for pre-provisioned hosts)
  --skip-dependencies       Do not install/update the Python virtualenv
  --no-systemd              Do not render the systemd/logrotate configuration
  --run-canary              Run exactly one real canary after preflight
  --confirm-live            Required together with --run-canary
  -h, --help                Show this help

If the secret credential file does not exist, this command creates it from the
tracked template and exits. Fill it in with an editor, then rerun the command.
EOF
}

while (($#)); do
  case "$1" in
    --project-root)
      [[ $# -ge 2 ]] || { printf '%s\n' 'quickstart: --project-root requires a value' >&2; exit 2; }
      PROJECT_ROOT="$2"
      shift 2
      ;;
    --python)
      [[ $# -ge 2 ]] || { printf '%s\n' 'quickstart: --python requires a value' >&2; exit 2; }
      PYTHON_REQUEST="$2"
      shift 2
      ;;
    --env-file)
      [[ $# -ge 2 ]] || { printf '%s\n' 'quickstart: --env-file requires a value' >&2; exit 2; }
      ENV_FILE="$2"
      shift 2
      ;;
    --service-env)
      [[ $# -ge 2 ]] || { printf '%s\n' 'quickstart: --service-env requires a value' >&2; exit 2; }
      SERVICE_ENV_FILE="$2"
      shift 2
      ;;
    --ticker)
      [[ $# -ge 2 ]] || { printf '%s\n' 'quickstart: --ticker requires a value' >&2; exit 2; }
      TICKER="$2"
      shift 2
      ;;
    --quantity)
      [[ $# -ge 2 ]] || { printf '%s\n' 'quickstart: --quantity requires a value' >&2; exit 2; }
      QUANTITY="$2"
      shift 2
      ;;
    --direction)
      [[ $# -ge 2 ]] || { printf '%s\n' 'quickstart: --direction requires a value' >&2; exit 2; }
      DIRECTION="$2"
      shift 2
      ;;
    --leverage)
      [[ $# -ge 2 ]] || { printf '%s\n' 'quickstart: --leverage requires a value' >&2; exit 2; }
      LEVERAGE="$2"
      shift 2
      ;;
    --slippage)
      [[ $# -ge 2 ]] || { printf '%s\n' 'quickstart: --slippage requires a value' >&2; exit 2; }
      SLIPPAGE="$2"
      shift 2
      ;;
    --lighter-max-wait)
      [[ $# -ge 2 ]] || { printf '%s\n' 'quickstart: --lighter-max-wait requires a value' >&2; exit 2; }
      LIGHTER_MAX_WAIT="$2"
      shift 2
      ;;
    --aster-maker-depth)
      [[ $# -ge 2 ]] || { printf '%s\n' 'quickstart: --aster-maker-depth requires a value' >&2; exit 2; }
      ASTER_MAKER_DEPTH="$2"
      shift 2
      ;;
    --skip-os-packages)
      INSTALL_OS_PACKAGES=0
      shift
      ;;
    --skip-dependencies)
      INSTALL_DEPENDENCIES=0
      shift
      ;;
    --no-systemd)
      INSTALL_SYSTEMD=0
      shift
      ;;
    --run-canary)
      RUN_CANARY=1
      shift
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
      printf 'quickstart: unknown option: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ((EUID != 0)); then
  printf '%s\n' 'quickstart: run as root via sudo; the strategy itself runs as the dedicated user' >&2
  exit 1
fi
if [[ "$(uname -s)" != "Linux" ]]; then
  printf '%s\n' 'quickstart: Linux is required' >&2
  exit 1
fi
if [[ ! -r /etc/os-release ]]; then
  printf '%s\n' 'quickstart: /etc/os-release is missing' >&2
  exit 1
fi
# shellcheck disable=SC1091
source /etc/os-release
DISTRO_TEXT="${ID:-} ${ID_LIKE:-}"
if [[ "${DISTRO_TEXT,,}" != *ubuntu* && "${DISTRO_TEXT,,}" != *debian* ]]; then
  printf 'quickstart: unsupported distribution (%s); use Ubuntu or Debian\n' "${DISTRO_TEXT}" >&2
  exit 1
fi

PROJECT_ROOT="$(cd -- "${PROJECT_ROOT}" 2>/dev/null && pwd -P)" || {
  printf '%s\n' 'quickstart: project root does not exist' >&2
  exit 1
}
if [[ "${ENV_FILE}" != /* ]]; then
  ENV_FILE="${PROJECT_ROOT}/${ENV_FILE}"
fi
if [[ "${SERVICE_ENV_FILE}" != /* ]]; then
  SERVICE_ENV_FILE="${PROJECT_ROOT}/${SERVICE_ENV_FILE}"
fi
if [[ ! "${ENV_FILE}" =~ ^/etc/perp/[A-Za-z0-9][A-Za-z0-9._-]*\.env$ ]]; then
  printf '%s\n' 'quickstart: credential env path must match /etc/perp/<basename>.env' >&2
  exit 1
fi
if [[ ! "${SERVICE_ENV_FILE}" =~ ^/etc/perp/[A-Za-z0-9][A-Za-z0-9._-]*\.env$ ]]; then
  printf '%s\n' 'quickstart: service env path must match /etc/perp/<basename>.env' >&2
  exit 1
fi
for required_path in .git requirements-robinhood.txt requirements-robinhood-offline.txt strategies; do
  if [[ ! -e "${PROJECT_ROOT}/${required_path}" ]]; then
    printf 'quickstart: incomplete checkout; missing %s\n' "${PROJECT_ROOT}/${required_path}" >&2
    exit 1
  fi
done

if ((INSTALL_OS_PACKAGES)); then
  command -v apt-get >/dev/null 2>&1 || {
    printf '%s\n' 'quickstart: apt-get is required unless --skip-os-packages is used' >&2
    exit 1
  }
  apt-get update
  DEBIAN_FRONTEND=noninteractive apt-get install -y \
    ca-certificates git python3 python3-venv python3-dev \
    build-essential libffi-dev libssl-dev util-linux logrotate chrony
  systemctl enable --now chrony || true
fi

resolve_python() {
  local candidate=""
  if [[ "${PYTHON_REQUEST}" == */* ]]; then
    candidate="${PYTHON_REQUEST}"
  else
    candidate="$(command -v "${PYTHON_REQUEST}" 2>/dev/null || true)"
  fi
  [[ -n "${candidate}" && -x "${candidate}" ]] || return 1
  if ! "${candidate}" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)'; then
    return 1
  fi
  PYTHON="$(readlink -f "${candidate}")"
}
resolve_python || {
  printf 'quickstart: Python >= 3.11 not found (requested %s)\n' "${PYTHON_REQUEST}" >&2
  exit 1
}
printf 'quickstart: using %s\n' "${PYTHON}"

if ! getent group "${SERVICE_GROUP}" >/dev/null 2>&1; then
  groupadd --system "${SERVICE_GROUP}"
fi
if ! id "${SERVICE_USER}" >/dev/null 2>&1; then
  useradd --system --create-home --home-dir /var/lib/perp \
    --shell /usr/sbin/nologin --gid "${SERVICE_GROUP}" "${SERVICE_USER}"
fi
install -d -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" -m 0750 \
  "${PROJECT_ROOT}/logs"
install -d -o root -g "${SERVICE_GROUP}" -m 0750 /etc/perp

if [[ ! -e "${ENV_FILE}" ]]; then
  install -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" -m 0600 \
    "${PROJECT_ROOT}/env_robinhood_example.txt" "${ENV_FILE}"
  printf '\nCreated credential template: %s\n' "${ENV_FILE}"
  printf '%s\n' 'Fill LIGHTER_ACCOUNT_INDEX and LIGHTER_API_PRIVATE_KEYS, then rerun quickstart.'
  exit 10
fi
if [[ -L "${ENV_FILE}" || ! -f "${ENV_FILE}" ]]; then
  printf '%s\n' 'quickstart: credential env file must be a regular file, not a symlink' >&2
  exit 1
fi
chown "${SERVICE_USER}:${SERVICE_GROUP}" "${ENV_FILE}"
chmod 0600 "${ENV_FILE}"

if ((INSTALL_DEPENDENCIES)); then
  runuser -u "${SERVICE_USER}" -- env HOME="/var/lib/perp" \
    bash "${PROJECT_ROOT}/deploy/robinhood/install.sh" \
    --project-root "${PROJECT_ROOT}" --python "${PYTHON}"
fi

if ((INSTALL_SYSTEMD)); then
  bash "${PROJECT_ROOT}/deploy/robinhood/install.sh" \
    --project-root "${PROJECT_ROOT}" \
    --venv "${PROJECT_ROOT}/.venv" \
    --skip-dependencies \
    --install-systemd \
    --service-user "${SERVICE_USER}" \
    --service-group "${SERVICE_GROUP}" \
    --service-env "${SERVICE_ENV_FILE}"
fi

PREFLIGHT_ARGS=(
  --env-file "${ENV_FILE}"
  --project-root "${PROJECT_ROOT}"
  --python "${PROJECT_ROOT}/.venv/bin/python"
)
if [[ -n "${QUANTITY}" ]]; then
  PREFLIGHT_ARGS+=(--ticker "${TICKER}" --quantity "${QUANTITY}")
fi
runuser -u "${SERVICE_USER}" -- env HOME="/var/lib/perp" \
  bash "${PROJECT_ROOT}/deploy/robinhood/preflight.sh" "${PREFLIGHT_ARGS[@]}"

if ((RUN_CANARY)); then
  [[ -n "${QUANTITY}" ]] || {
    printf '%s\n' 'quickstart: --run-canary requires --quantity' >&2
    exit 2
  }
  ((CONFIRM_LIVE)) || {
    printf '%s\n' 'quickstart: --run-canary requires --confirm-live' >&2
    exit 2
  }
  [[ "${DIRECTION}" == "buy" || "${DIRECTION}" == "sell" ]] || {
    printf '%s\n' 'quickstart: --direction must be buy or sell' >&2
    exit 2
  }
  runuser -u "${SERVICE_USER}" -- env HOME="/var/lib/perp" \
    /usr/bin/flock --no-fork --exclusive --nonblock \
    "${PROJECT_ROOT}/logs/robinhood-strategy.lock" \
    /usr/bin/bash "${PROJECT_ROOT}/deploy/robinhood/run.sh" \
    --env-file "${ENV_FILE}" \
    --ticker "${TICKER}" \
    --quantity "${QUANTITY}" \
    --direction "${DIRECTION}" \
    --leverage "${LEVERAGE}" \
    --slippage "${SLIPPAGE}" \
    --lighter-max-wait "${LIGHTER_MAX_WAIT}" \
    --aster-maker-depth "${ASTER_MAKER_DEPTH}" \
    --mode canary \
    --confirm-live
else
  printf '\nInstallation and read-only preflight completed.\n'
  printf '%s\n' 'No order was sent. Run a canary only after reviewing the credential, margin, and quantity checks:'
  printf '  sudo bash %s --project-root %s --quantity %s --run-canary --confirm-live\n' \
    "${SCRIPT_DIR}/quickstart.sh" "${PROJECT_ROOT}" "${QUANTITY:-0.00020}"
fi
