#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
DEFAULT_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd -P)"
PROJECT_ROOT="${DEFAULT_ROOT}"
VENV=""
PYTHON_BIN="${PYTHON_BIN:-python3.11}"
SERVICE_USER="perp"
SERVICE_GROUP="perp"
SERVICE_ENV_FILE="/etc/perp/robinhood-service.env"
INSTALL_DEPENDENCIES=1
INSTALL_SYSTEMD=0
ALLOW_DIRTY=0
WHEELHOUSE=""

usage() {
  cat <<'EOF'
Usage: bash deploy/robinhood/install.sh [options]

Installs Python dependencies from the current checkout. It never clones, pulls,
reads a private key from argv, enables a service, or starts live trading.

Options:
  --project-root PATH       Existing repository checkout (default: current checkout)
  --venv PATH               Virtual environment path (default: PROJECT_ROOT/.venv)
  --python COMMAND          Python >= 3.11 executable (default: python3.11)
  --wheelhouse PATH         Install only from a verified local wheelhouse
  --skip-dependencies       Do not create/update the virtual environment
  --allow-dirty             Allow an uncommitted checkout (diagnostics only)
  --install-systemd        Render unit/logrotate files; requires root
  --service-user USER       Dedicated existing account (default: perp)
  --service-group GROUP     Dedicated existing group (default: perp)
  --service-env PATH        Non-secret systemd EnvironmentFile
  -h, --help                Show this help

The --install-systemd action only installs configuration and runs daemon-reload.
It deliberately does not enable or start the service.
EOF
}

while (($#)); do
  case "$1" in
    --project-root)
      [[ $# -ge 2 ]] || { printf '%s\n' 'install: --project-root requires a value' >&2; exit 2; }
      PROJECT_ROOT="$2"
      shift 2
      ;;
    --venv)
      [[ $# -ge 2 ]] || { printf '%s\n' 'install: --venv requires a value' >&2; exit 2; }
      VENV="$2"
      shift 2
      ;;
    --python)
      [[ $# -ge 2 ]] || { printf '%s\n' 'install: --python requires a value' >&2; exit 2; }
      PYTHON_BIN="$2"
      shift 2
      ;;
    --wheelhouse)
      [[ $# -ge 2 ]] || { printf '%s\n' 'install: --wheelhouse requires a value' >&2; exit 2; }
      WHEELHOUSE="$2"
      shift 2
      ;;
    --skip-dependencies)
      INSTALL_DEPENDENCIES=0
      shift
      ;;
    --allow-dirty)
      ALLOW_DIRTY=1
      shift
      ;;
    --install-systemd)
      INSTALL_SYSTEMD=1
      shift
      ;;
    --service-user)
      [[ $# -ge 2 ]] || { printf '%s\n' 'install: --service-user requires a value' >&2; exit 2; }
      SERVICE_USER="$2"
      shift 2
      ;;
    --service-group)
      [[ $# -ge 2 ]] || { printf '%s\n' 'install: --service-group requires a value' >&2; exit 2; }
      SERVICE_GROUP="$2"
      shift 2
      ;;
    --service-env)
      [[ $# -ge 2 ]] || { printf '%s\n' 'install: --service-env requires a value' >&2; exit 2; }
      SERVICE_ENV_FILE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'install: unknown option: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$(uname -s)" != "Linux" ]]; then
  printf '%s\n' 'install: this deployment is supported only on Linux' >&2
  exit 1
fi
if [[ ! -r /etc/os-release ]]; then
  printf '%s\n' 'install: /etc/os-release is missing' >&2
  exit 1
fi
# shellcheck disable=SC1091
source /etc/os-release
DISTRO_TEXT="${ID:-} ${ID_LIKE:-}"
if [[ "${DISTRO_TEXT,,}" != *ubuntu* && "${DISTRO_TEXT,,}" != *debian* ]]; then
  printf 'install: unsupported distribution (%s); use Ubuntu or Debian\n' "${DISTRO_TEXT}" >&2
  exit 1
fi

for command_name in git "${PYTHON_BIN}"; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    printf 'install: required command is missing: %s\n' "${command_name}" >&2
    exit 1
  fi
done

PROJECT_ROOT="$(cd -- "${PROJECT_ROOT}" 2>/dev/null && pwd -P)" || {
  printf '%s\n' 'install: project root does not exist' >&2
  exit 1
}
if [[ ! -e "${PROJECT_ROOT}/.git" || ! -f "${PROJECT_ROOT}/requirements-robinhood.txt" || ! -f "${PROJECT_ROOT}/requirements-robinhood-offline.txt" || ! -d "${PROJECT_ROOT}/strategies" ]]; then
  printf 'install: path is not a complete repository checkout: %s\n' "${PROJECT_ROOT}" >&2
  exit 1
fi
DEPLOY_COMMIT="$(git -C "${PROJECT_ROOT}" rev-parse --verify HEAD 2>/dev/null)" || {
  printf '%s\n' 'install: unable to resolve the deployment commit' >&2
  exit 1
}
if ((ALLOW_DIRTY == 0)) && [[ -n "$(git -C "${PROJECT_ROOT}" status --porcelain --untracked-files=normal)" ]]; then
  printf '%s\n' 'install: checkout has uncommitted files; commit/tag the release or use --allow-dirty for diagnostics' >&2
  exit 1
fi
printf 'Deployment commit: %s\n' "${DEPLOY_COMMIT}"
if [[ -z "${VENV}" ]]; then
  VENV="${PROJECT_ROOT}/.venv"
fi
if [[ "${VENV}" != /* ]]; then
  VENV="${PROJECT_ROOT}/${VENV}"
fi

PYTHON_VERSION="$("${PYTHON_BIN}" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
if ! "${PYTHON_BIN}" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)'; then
  printf 'install: Python >= 3.11 is required, got %s\n' "${PYTHON_VERSION}" >&2
  exit 1
fi
if ! "${PYTHON_BIN}" - <<'PY'
import pathlib
import ssl
paths = ssl.get_default_verify_paths()
valid = any(
    value and pathlib.Path(value).exists()
    for value in (paths.cafile, paths.capath, paths.openssl_cafile, paths.openssl_capath)
)
raise SystemExit(0 if valid else 1)
PY
then
  printf '%s\n' 'install: no usable system CA certificate bundle was found' >&2
  exit 1
fi

printf 'Validated %s with Python %s and system CA certificates.\n' "${PROJECT_ROOT}" "${PYTHON_VERSION}"

if ((INSTALL_DEPENDENCIES)); then
  if [[ ! -d "${VENV}" ]]; then
    "${PYTHON_BIN}" -m venv "${VENV}" || {
      printf '%s\n' 'install: venv creation failed; install python3-venv for the selected Python version' >&2
      exit 1
    }
  fi
  if [[ ! -x "${VENV}/bin/python" ]]; then
    printf 'install: virtual environment is invalid: %s\n' "${VENV}" >&2
    exit 1
  fi
  if [[ -n "${WHEELHOUSE}" ]]; then
    if ! command -v sha256sum >/dev/null 2>&1; then
      printf '%s\n' 'install: sha256sum is required for offline wheel verification' >&2
      exit 1
    fi
    WHEELHOUSE="$(cd -- "${WHEELHOUSE}" 2>/dev/null && pwd -P)" || {
      printf '%s\n' 'install: wheelhouse directory does not exist' >&2
      exit 1
    }
    if [[ ! -f "${WHEELHOUSE}/SHA256SUMS" ]]; then
      printf '%s\n' 'install: wheelhouse SHA256SUMS is missing' >&2
      exit 1
    fi
    (
      cd -- "${WHEELHOUSE}"
      sha256sum --check --strict SHA256SUMS
    )
    "${VENV}/bin/python" -m pip install \
      --disable-pip-version-check \
      --no-index \
      --find-links "${WHEELHOUSE}" \
      -r "${PROJECT_ROOT}/requirements-robinhood-offline.txt"
  else
    "${VENV}/bin/python" -m pip install --disable-pip-version-check -r "${PROJECT_ROOT}/requirements-robinhood.txt"
  fi
  "${VENV}/bin/python" -c 'import aiohttp, dotenv, lighter, web3, websockets'
  install -d -m 0750 "${PROJECT_ROOT}/logs"
  chmod 0755 \
    "${PROJECT_ROOT}/deploy/robinhood/install.sh" \
    "${PROJECT_ROOT}/deploy/robinhood/build-wheelhouse.sh" \
    "${PROJECT_ROOT}/deploy/robinhood/preflight.sh" \
    "${PROJECT_ROOT}/deploy/robinhood/run.sh"
  printf 'Python environment installed at %s. No trading process was started.\n' "${VENV}"
fi

if ((INSTALL_SYSTEMD)); then
  if ((EUID != 0)); then
    printf '%s\n' 'install: --install-systemd must be run as root' >&2
    exit 1
  fi
  if [[ ! "${PROJECT_ROOT}" =~ ^/[A-Za-z0-9._/-]+$ ]]; then
    printf '%s\n' 'install: systemd project path may contain only letters, digits, dot, underscore, slash, and hyphen' >&2
    exit 1
  fi
  if [[ ! "${SERVICE_ENV_FILE}" =~ ^/[A-Za-z0-9._/-]+$ ]]; then
    printf '%s\n' 'install: service environment path must be an absolute simple path' >&2
    exit 1
  fi
  if [[ "${SERVICE_ENV_FILE}" != /etc/perp/*.env ]]; then
    printf '%s\n' 'install: service environment path must be /etc/perp/<name>.env' >&2
    exit 1
  fi
  if [[ ! "${SERVICE_USER}" =~ ^[a-z_][a-z0-9_-]*[$]?$ ]] || [[ ! "${SERVICE_GROUP}" =~ ^[a-z_][a-z0-9_-]*[$]?$ ]]; then
    printf '%s\n' 'install: invalid service user or group name' >&2
    exit 1
  fi
  if ! id "${SERVICE_USER}" >/dev/null 2>&1; then
    printf 'install: dedicated service user does not exist: %s\n' "${SERVICE_USER}" >&2
    exit 1
  fi
  if ! getent group "${SERVICE_GROUP}" >/dev/null 2>&1; then
    printf 'install: dedicated service group does not exist: %s\n' "${SERVICE_GROUP}" >&2
    exit 1
  fi
  for command_name in flock grep install sed systemctl; do
    if ! command -v "${command_name}" >/dev/null 2>&1; then
      printf 'install: required systemd deployment command is missing: %s\n' "${command_name}" >&2
      exit 1
    fi
  done

  SERVICE_ENV_DIR="$(dirname -- "${SERVICE_ENV_FILE}")"
  install -d -o root -g "${SERVICE_GROUP}" -m 0750 "${SERVICE_ENV_DIR}"
  if [[ ! -e "${SERVICE_ENV_FILE}" ]]; then
    install -o root -g "${SERVICE_GROUP}" -m 0640 \
      "${SCRIPT_DIR}/robinhood-service.env.example" "${SERVICE_ENV_FILE}"
    sed -i "s|^PERP_VENV=.*|PERP_VENV=${VENV}|" "${SERVICE_ENV_FILE}"
    printf 'Installed non-secret service configuration at %s; review it before starting.\n' "${SERVICE_ENV_FILE}"
  else
    if [[ -L "${SERVICE_ENV_FILE}" || ! -f "${SERVICE_ENV_FILE}" ]]; then
      printf '%s\n' 'install: service environment file must be a regular file, not a symlink' >&2
      exit 1
    fi
    chown root:"${SERVICE_GROUP}" "${SERVICE_ENV_FILE}"
    chmod 0640 "${SERVICE_ENV_FILE}"
    printf 'Preserved existing service configuration at %s.\n' "${SERVICE_ENV_FILE}"
  fi

  if grep -Eq '^[[:space:]]*(LIGHTER_API_PRIVATE_KEYS|API_KEY_PRIVATE_KEYS|API_KEY_PRIVATE_KEY|L1_WALLET_PRIVATE_KEY|LIGHTER_L1_PRIVATE_KEY|ASTER_API_KEY|ASTER_SECRET_KEY)=' "${SERVICE_ENV_FILE}"; then
    printf '%s\n' 'install: private exchange credentials are forbidden in the non-secret service environment file' >&2
    exit 1
  fi

  UNIT_TMP="$(mktemp /tmp/perp-robinhood-unit.XXXXXX)"
  LOGROTATE_TMP="$(mktemp /tmp/perp-robinhood-logrotate.XXXXXX)"
  cleanup() {
    rm -f -- "${UNIT_TMP}" "${LOGROTATE_TMP}"
  }
  trap cleanup EXIT

  sed \
    -e "s|@@PROJECT_ROOT@@|${PROJECT_ROOT}|g" \
    -e "s|@@SERVICE_USER@@|${SERVICE_USER}|g" \
    -e "s|@@SERVICE_GROUP@@|${SERVICE_GROUP}|g" \
    -e "s|@@SERVICE_ENV_FILE@@|${SERVICE_ENV_FILE}|g" \
    "${SCRIPT_DIR}/perp-robinhood.service.in" >"${UNIT_TMP}"
  sed \
    -e "s|@@PROJECT_ROOT@@|${PROJECT_ROOT}|g" \
    -e "s|@@SERVICE_USER@@|${SERVICE_USER}|g" \
    -e "s|@@SERVICE_GROUP@@|${SERVICE_GROUP}|g" \
    "${SCRIPT_DIR}/perp-robinhood.logrotate.in" >"${LOGROTATE_TMP}"

  install -o root -g root -m 0644 "${UNIT_TMP}" /etc/systemd/system/perp-robinhood.service
  install -o root -g root -m 0644 "${LOGROTATE_TMP}" /etc/logrotate.d/perp-robinhood
  systemctl daemon-reload
  printf '%s\n' 'Installed systemd and logrotate configuration.'
  printf '%s\n' 'The service was not enabled or started. Run preflight and a manual canary first.'
fi

printf '%s\n' 'Installation finished without sending an order.'
