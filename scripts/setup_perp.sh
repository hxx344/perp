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

python3 -m venv env
source env/bin/activate

pip install -r requirements.txt

if [ ! -f .env ]; then
  cp env_example.txt .env
fi
