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
    if [ "$(id -u)" -ne 0 ]; then
      sudo apt-get update && sudo apt-get install -y python3-venv
    else
      apt-get update && apt-get install -y python3-venv
    fi
  else
    echo "[setup_perp] Unable to install python3-venv automatically. Please install it manually and rerun." >&2
    exit 1
  fi
fi

python3 -m venv env
source env/bin/activate

pip install -r requirements.txt

if [ ! -f .env ]; then
  cp env_example.txt .env
fi
