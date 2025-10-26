#!/usr/bin/env bash
set -euo pipefail

PRIVATE_KEY_INPUT=${1-""}

update_env_var() {
  local key="$1"
  local value="$2"
  local env_file=".env"

  if [ ! -f "$env_file" ]; then
    printf '%s=%s\n' "$key" "$value" > "$env_file"
    return
  fi

  local tmp_file
  if command -v mktemp >/dev/null 2>&1; then
    tmp_file=$(mktemp "${env_file}.XXXXXX")
  else
    tmp_file="${env_file}.tmp.$$"
  fi

  if grep -q "^${key}=" "$env_file"; then
    awk -v key="$key" -v value="$value" 'index($0, key "=")==1 { print key "=" value; next } { print }' "$env_file" > "$tmp_file"
    mv "$tmp_file" "$env_file"
  else
    rm -f "$tmp_file"
    printf '%s=%s\n' "$key" "$value" >> "$env_file"
  fi
}

# Determine working directory (default: current directory, override with PERP_SETUP_DIR)
WORKDIR="${PERP_SETUP_DIR:-$PWD}"

mkdir -p "${WORKDIR}/code"
cd "${WORKDIR}/code"

if [ ! -d perp ]; then
  git clone https://github.com/hxx344/perp.git
fi
cd perp

# Ensure python venv support is available
if ! python3 -m ensurepip --version >/dev/null 2>&1; then
  echo "[setup_perp] python3-venv not detected; attempting to install..."
  if command -v apt-get >/dev/null 2>&1; then
    PY_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null || echo "")
    CANDIDATES=("python3-venv")
    if [ -n "$PY_VERSION" ]; then
      CANDIDATES+=("python${PY_VERSION}-venv")
    fi

    SUDO_CMD=""
    if [ "$(id -u)" -ne 0 ]; then
      if command -v sudo >/dev/null 2>&1; then
        SUDO_CMD="sudo"
      else
        echo "[setup_perp] Please run as root or install sudo to allow python3-venv installation." >&2
        exit 1
      fi
    fi

    APT_CMD=(apt-get)
    if [ -n "$SUDO_CMD" ]; then
      APT_CMD=($SUDO_CMD apt-get)
    fi

    "${APT_CMD[@]}" update

    VENV_INSTALLED=0
    for pkg in "${CANDIDATES[@]}"; do
      if "${APT_CMD[@]}" install -y "$pkg"; then
        VENV_INSTALLED=1
        break
      else
        echo "[setup_perp] Package $pkg not available; trying next option..."
      fi
    done

    if [ "$VENV_INSTALLED" -ne 1 ]; then
      echo "[setup_perp] Unable to install python venv packages automatically. Please install python3-venv manually." >&2
      exit 1
    fi
  else
    echo "[setup_perp] Unable to install python3-venv automatically. Please install it manually and rerun." >&2
    exit 1
  fi

  if ! python3 -m ensurepip --version >/dev/null 2>&1; then
    echo "[setup_perp] python3-venv installation appears incomplete. Please install the appropriate pythonX.Y-venv package manually." >&2
    exit 1
  fi
fi

python3 -m venv env
source env/bin/activate

pip install -r requirements.txt

if [ ! -f .env ]; then
  cp env_example.txt .env
fi

# Populate API_KEY_PRIVATE_KEY if provided via argument (final step)
if [ -n "$PRIVATE_KEY_INPUT" ]; then
  update_env_var "API_KEY_PRIVATE_KEY" "$PRIVATE_KEY_INPUT"
  echo "[setup_perp] Injected provided private key into .env (API_KEY_PRIVATE_KEY)." >&2
fi

