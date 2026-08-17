from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from deploy.robinhood.verify_wheelhouse import verify_wheelhouse


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEPLOY_ROOT = PROJECT_ROOT / "deploy" / "robinhood"


def _write_wheelhouse(root: Path, names: list[str]) -> None:
    entries = []
    for name in names:
        payload = f"wheel payload: {name}\n".encode()
        (root / name).write_bytes(payload)
        entries.append(f"{hashlib.sha256(payload).hexdigest()}  {name}")
    (root / "SHA256SUMS").write_text("\n".join(entries) + "\n", encoding="utf-8")


def test_wheelhouse_manifest_and_hashes_are_verified(tmp_path: Path) -> None:
    _write_wheelhouse(tmp_path, ["aiohttp-3.13.3-py3-none-any.whl"])

    verify_wheelhouse(tmp_path)


def test_wheelhouse_rejects_unlisted_files(tmp_path: Path) -> None:
    _write_wheelhouse(tmp_path, ["aiohttp-3.13.3-py3-none-any.whl"])
    (tmp_path / "requests-2.32.5-py3-none-any.whl").write_bytes(b"unlisted")

    with pytest.raises(ValueError, match="unlisted"):
        verify_wheelhouse(tmp_path)


def test_wheelhouse_rejects_non_wheel_artifacts(tmp_path: Path) -> None:
    _write_wheelhouse(tmp_path, ["aiohttp-3.13.3-py3-none-any.whl"])
    (tmp_path / "README.txt").write_text("unexpected", encoding="utf-8")

    with pytest.raises(ValueError, match="do not exactly match"):
        verify_wheelhouse(tmp_path)


def test_install_script_uses_safe_git_and_does_not_mutate_checkout_mode() -> None:
    source = (DEPLOY_ROOT / "install.sh").read_text(encoding="utf-8")

    assert 'git -c "safe.directory=${PROJECT_ROOT}"' in source
    assert "git_safe -C \"${PROJECT_ROOT}\" status" in source
    assert "chmod 0755" not in source


def test_runner_normalizes_paths_before_changing_directory() -> None:
    source = (DEPLOY_ROOT / "run.sh").read_text(encoding="utf-8")

    assert "absolute_path_from_project" in source
    assert 'ENV_FILE="$(absolute_path_from_project "${ENV_FILE}")"' in source
    assert 'cd -- "${PROJECT_ROOT}"' in source
    assert source.index('ENV_FILE="$(absolute_path_from_project') < source.rindex(
        'cd -- "${PROJECT_ROOT}"'
    )


def test_service_env_path_is_single_basename_under_etc_perp() -> None:
    source = (DEPLOY_ROOT / "install.sh").read_text(encoding="utf-8")

    assert "^/etc/perp/[A-Za-z0-9][A-Za-z0-9._-]*\\.env$" in source
    assert "/etc/perp/*.env" not in source


def test_python_policy_matches_ubuntu_and_debian_documentation() -> None:
    installer = (DEPLOY_ROOT / "install.sh").read_text(encoding="utf-8")
    wheelhouse = (DEPLOY_ROOT / "build-wheelhouse.sh").read_text(encoding="utf-8")
    docs = (PROJECT_ROOT / "docs" / "robinhood_linux_deployment.md").read_text(encoding="utf-8")

    assert 'PYTHON_BIN="${PYTHON_BIN:-python3}"' in installer
    assert 'PYTHON_BIN="${PYTHON_BIN:-python3}"' in wheelhouse
    assert "sys.version_info >= (3, 11)" in installer
    assert "sys.version_info >= (3, 11)" in wheelhouse
    assert "Ubuntu 24.04" in docs and "Debian 12+" in docs
