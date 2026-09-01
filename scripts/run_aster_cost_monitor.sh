#!/usr/bin/env bash
set -euo pipefail
PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ASTER_COST_ENV_FILE:-${PROJECT_ROOT}/env_aster_cost_monitor.env}"
PYTHON_BIN="${ASTER_COST_PYTHON:-${PROJECT_ROOT}/.venv/bin/python}"
[[ -r "${ENV_FILE}" ]] || { printf 'Environment file not readable: %s\n' "${ENV_FILE}" >&2; exit 64; }
[[ -x "${PYTHON_BIN}" ]] || { printf 'Python executable not found: %s\n' "${PYTHON_BIN}" >&2; exit 69; }
cd "${PROJECT_ROOT}"
exec "${PYTHON_BIN}" -m strategies.aster_cost_monitor --env-file "${ENV_FILE}" "$@"
