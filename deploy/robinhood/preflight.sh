#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
PROJECT_ROOT="${PERP_PROJECT_ROOT:-$(cd -- "${SCRIPT_DIR}/../.." && pwd -P)}"
VENV="${PERP_VENV:-${PROJECT_ROOT}/.venv}"
PYTHON="${PERP_PYTHON:-${VENV}/bin/python}"
ENV_FILE="${PERP_ENV_FILE:-/etc/perp/robinhood.env}"

if [[ "${PYTHON}" != */* ]]; then
  PYTHON="$(command -v "${PYTHON}" 2>/dev/null || true)"
fi
if [[ ! -x "${PYTHON}" ]]; then
  printf 'preflight: Python interpreter is not executable: %s\n' "${PYTHON}" >&2
  exit 1
fi

exec "${PYTHON}" "${SCRIPT_DIR}/preflight.py" \
  --env-file "${ENV_FILE}" \
  --project-root "${PROJECT_ROOT}" \
  --python "${PYTHON}" \
  "$@"
