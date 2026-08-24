#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${RH_NEUTRAL_ENV_FILE:-/etc/perp/rh-neutral.env}"
PYTHON_BIN="${RH_NEUTRAL_PYTHON:-${PROJECT_ROOT}/.venv/bin/python}"

if [[ ! -r "${ENV_FILE}" ]]; then
  printf 'Environment file not readable: %s\n' "${ENV_FILE}" >&2
  exit 64
fi
if [[ ! -x "${PYTHON_BIN}" ]]; then
  printf 'Python executable not found: %s\n' "${PYTHON_BIN}" >&2
  exit 69
fi

cd "${PROJECT_ROOT}"
exec "${PYTHON_BIN}" -m strategies.rh_neutral_manager --env-file "${ENV_FILE}" "$@"
