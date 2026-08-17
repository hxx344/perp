import json
from pathlib import Path

import pytest

from deploy.robinhood import preflight


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _valid_env_values():
    return {
        **preflight.EXPECTED_ENDPOINTS,
        "LIGHTER_ACCOUNT_INDEX": "7",
        "LIGHTER_API_PRIVATE_KEYS": json.dumps({"4": "0x" + "a" * 64}),
    }


def test_preflight_accepts_strict_robinhood_credentials():
    report = preflight.Report()

    preflight.check_env_values(report, _valid_env_values())

    assert report.errors == 0


def test_preflight_rejects_core_key_and_maker_reserved_index():
    values = _valid_env_values()
    values["LIGHTER_API_PRIVATE_KEYS"] = json.dumps({"2": "0x" + "a" * 64})
    values["L1_WALLET_PRIVATE_KEY"] = "0x" + "b" * 64
    report = preflight.Report()

    preflight.check_env_values(report, values)

    assert report.errors == 2


def test_preflight_rejects_incomplete_or_cleartext_remote_coordinator_auth():
    missing_password = _valid_env_values()
    missing_password.update(
        {
            "HEDGE_COORDINATOR_URL": "https://coordinator.example",
            "HEDGE_COORDINATOR_USERNAME": "agent",
        }
    )
    missing_report = preflight.Report()
    preflight.check_env_values(missing_report, missing_password)

    cleartext = _valid_env_values()
    cleartext.update(
        {
            "HEDGE_COORDINATOR_URL": "http://coordinator.example",
            "HEDGE_COORDINATOR_USERNAME": "agent",
            "HEDGE_COORDINATOR_PASSWORD": "secret",
        }
    )
    cleartext_report = preflight.Report()
    preflight.check_env_values(cleartext_report, cleartext)

    embedded = _valid_env_values()
    embedded["HEDGE_COORDINATOR_URL"] = "https://agent:secret@coordinator.example"
    embedded_report = preflight.Report()
    preflight.check_env_values(embedded_report, embedded)

    assert missing_report.errors == 1
    assert cleartext_report.errors == 1
    assert embedded_report.errors == 1


def test_preflight_rejects_credential_symlink(tmp_path):
    target = tmp_path / "credentials.env"
    target.write_text("LIGHTER_ENVIRONMENT=robinhood\n", encoding="utf-8")
    link = tmp_path / "credentials-link.env"
    try:
        link.symlink_to(target)
    except OSError as exc:
        pytest.skip(f"symlink creation is unavailable: {exc}")
    report = preflight.Report()

    assert preflight.check_env_permissions(report, link) is False
    assert report.errors == 1


def test_canary_service_and_runtime_dependencies_fail_closed():
    unit = (PROJECT_ROOT / "deploy/robinhood/perp-robinhood.service.in").read_text(
        encoding="utf-8"
    )
    restart_drop_in = (
        PROJECT_ROOT / "deploy/robinhood/network-restart.conf.example"
    ).read_text(encoding="utf-8")
    requirements = (PROJECT_ROOT / "requirements-robinhood.txt").read_text(
        encoding="utf-8"
    )
    runner = (PROJECT_ROOT / "deploy/robinhood/run.sh").read_text(encoding="utf-8")

    assert "Restart=no" in unit
    assert "RestartForceExitStatus" not in unit
    assert "RestartPreventExitStatus=73 78" in unit
    assert "RestartForceExitStatus=75" in restart_drop_in
    assert "edgex" not in requirements.casefold()
    assert "git+" not in requirements.casefold()
    assert "lighter-sdk @ https://codeload.github.com/" in requirements
    assert "skip-network" not in runner


def test_quickstart_keeps_install_read_only_until_explicit_live_flags():
    quickstart = (PROJECT_ROOT / "deploy/robinhood/quickstart.sh").read_text(
        encoding="utf-8"
    )

    assert "--run-canary" in quickstart
    assert "--confirm-live" in quickstart
    assert "git pull" not in quickstart
    assert "--run-canary requires --confirm-live" in quickstart
    assert "install -d -o root" in quickstart
    assert "credential env path must match /etc/perp/<basename>.env" in quickstart


def test_coordinator_password_stays_in_protected_env_and_off_command_line():
    runner = (PROJECT_ROOT / "deploy/robinhood/run.sh").read_text(encoding="utf-8")
    installer = (PROJECT_ROOT / "deploy/robinhood/install.sh").read_text(
        encoding="utf-8"
    )
    protected_env = (PROJECT_ROOT / "env_robinhood_example.txt").read_text(
        encoding="utf-8"
    )

    assert "HEDGE_COORDINATOR_PASSWORD=" in protected_env
    assert "--coordinator-password" not in runner
    assert "-u HEDGE_COORDINATOR_PASSWORD" in runner
    assert "HEDGE_COORDINATOR_[A-Z_]+" in installer


def test_install_supports_root_git_checks_and_exact_wheelhouse_manifest():
    installer = (PROJECT_ROOT / "deploy/robinhood/install.sh").read_text(
        encoding="utf-8"
    )
    wheelhouse_verifier = (
        PROJECT_ROOT / "deploy/robinhood/verify_wheelhouse.py"
    ).read_text(encoding="utf-8")

    assert 'safe.directory=${PROJECT_ROOT}' in installer
    assert "chmod 0755" not in installer
    assert "verify_wheelhouse.py" in installer
    assert "actual != expected" in wheelhouse_verifier


@pytest.mark.parametrize(
    ("quantity", "reference_price", "expected_errors"),
    [
        ("0.00020", "60000", 0),
        ("0.00020", "50000", 1),
        ("0.000201", "100000", 1),
    ],
)
def test_live_market_quantity_enforces_base_quote_and_step(
    quantity,
    reference_price,
    expected_errors,
):
    report = preflight.Report()
    order_books = [
        {
            "symbol": "BTC",
            "market_type": "perp",
            "status": "active",
            "supported_size_decimals": 5,
            "min_base_amount": "0.00020",
            "min_quote_amount": "10.000000",
        }
    ]

    preflight.validate_market_quantity(
        report,
        ticker="BTC",
        quantity_raw=quantity,
        order_books=order_books,
        reference_price_raw=reference_price,
    )

    assert report.errors == expected_errors
