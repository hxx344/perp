#!/usr/bin/env bash
set -euo pipefail

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

