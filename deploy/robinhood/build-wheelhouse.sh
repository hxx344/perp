#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
PROJECT_ROOT="${PERP_PROJECT_ROOT:-$(cd -- "${SCRIPT_DIR}/../.." && pwd -P)}"
PYTHON_BIN="${PYTHON_BIN:-python3.11}"
OUTPUT=""

usage() {
  cat <<'EOF'
Usage: bash deploy/robinhood/build-wheelhouse.sh --output PATH [--python COMMAND]

Builds a Linux/Python-specific wheelhouse for offline Robinhood deployment.
Run it on the same Linux architecture and Python minor version as the target.
No exchange credentials are read and no trading process is started.
EOF
}

while (($#)); do
  case "$1" in
    --output)
      [[ $# -ge 2 ]] || { printf '%s\n' 'wheelhouse: --output requires a value' >&2; exit 2; }
      OUTPUT="$2"
      shift 2
      ;;
    --python)
      [[ $# -ge 2 ]] || { printf '%s\n' 'wheelhouse: --python requires a value' >&2; exit 2; }
      PYTHON_BIN="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'wheelhouse: unknown option: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$(uname -s)" != "Linux" ]]; then
  printf '%s\n' 'wheelhouse: build on the target Linux family and architecture' >&2
  exit 1
fi
if [[ -z "${OUTPUT}" ]]; then
  printf '%s\n' 'wheelhouse: --output is required' >&2
  exit 2
fi
for command_name in "${PYTHON_BIN}" find grep sha256sum wc; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    printf 'wheelhouse: required command not found: %s\n' "${command_name}" >&2
    exit 1
  fi
done
if ! "${PYTHON_BIN}" -c 'import sys; raise SystemExit(0 if sys.version_info[:2] == (3, 11) else 1)'; then
  printf '%s\n' 'wheelhouse: Python 3.11 is required to match the deployment runtime' >&2
  exit 1
fi

OUTPUT="$(mkdir -p -- "${OUTPUT}" && cd -- "${OUTPUT}" && pwd -P)"
if [[ "${OUTPUT}" == "/" || "${OUTPUT}" == "${PROJECT_ROOT}" ]]; then
  printf '%s\n' 'wheelhouse: refusing a broad output directory' >&2
  exit 1
fi
if find "${OUTPUT}" -mindepth 1 -maxdepth 1 -print -quit | grep -q .; then
  printf '%s\n' 'wheelhouse: output directory must be empty to avoid stale packages' >&2
  exit 1
fi

"${PYTHON_BIN}" -m pip wheel \
  --disable-pip-version-check \
  --wheel-dir "${OUTPUT}" \
  -r "${PROJECT_ROOT}/requirements-robinhood.txt"

if ! compgen -G "${OUTPUT}/lighter_sdk-1.1.2-*.whl" >/dev/null; then
  printf '%s\n' 'wheelhouse: lighter-sdk 1.1.2 wheel was not produced' >&2
  exit 1
fi

(
  cd -- "${OUTPUT}"
  sha256sum -- *.whl > SHA256SUMS
)
printf 'Wheelhouse ready at %s (%s wheels).\n' \
  "${OUTPUT}" "$(find "${OUTPUT}" -maxdepth 1 -type f -name '*.whl' | wc -l)"
