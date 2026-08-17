from __future__ import annotations

import os
import sys

from strategies import robinhood_lighter_cycle


def test_profile_selects_robinhood_virtual_binance_and_conservative_defaults():
    args = robinhood_lighter_cycle.ROBINHOOD_DEFAULT_ARGS
    required = robinhood_lighter_cycle.ROBINHOOD_REQUIRED_ARGS

    assert required == ("--lighter-environment", "robinhood")
    assert args[args.index("--lighter-leverage") + 1] == "2"
    assert args[args.index("--virtual-maker-price-source") + 1] == "bn"
    assert "--virtual-aster-maker" in args
    assert "--preserve-initial-position" in args
    assert "--quantity" not in args
    assert "--cycles" not in args


def test_env_file_prefers_existing_system_file_then_local_fallback(
    monkeypatch, tmp_path
):
    system_env = tmp_path / "etc" / "perp" / "robinhood.env"
    local_rh_env = tmp_path / ".env.robinhood"
    local_env = tmp_path / ".env"
    monkeypatch.setattr(
        robinhood_lighter_cycle,
        "DEFAULT_ENV_CANDIDATES",
        (str(system_env), str(local_rh_env), str(local_env)),
    )

    local_env.write_text("LIGHTER_ENVIRONMENT=robinhood\n", encoding="utf-8")
    assert robinhood_lighter_cycle.resolve_default_env_file() == str(local_env)

    local_rh_env.write_text("LIGHTER_ENVIRONMENT=robinhood\n", encoding="utf-8")
    assert robinhood_lighter_cycle.resolve_default_env_file() == str(local_rh_env)

    system_env.parent.mkdir(parents=True)
    system_env.write_text("LIGHTER_ENVIRONMENT=robinhood\n", encoding="utf-8")
    assert robinhood_lighter_cycle.resolve_default_env_file() == str(system_env)


def test_missing_env_defaults_to_the_previously_documented_system_path(
    monkeypatch, tmp_path
):
    missing_local_rh = tmp_path / ".env.robinhood"
    missing_local = tmp_path / ".env"
    monkeypatch.setattr(
        robinhood_lighter_cycle,
        "DEFAULT_ENV_CANDIDATES",
        (
            "/etc/perp/robinhood.env",
            str(missing_local_rh),
            str(missing_local),
        ),
    )

    assert (
        robinhood_lighter_cycle.resolve_default_env_file()
        == "/etc/perp/robinhood.env"
    )


def test_user_options_are_appended_and_can_override_profile_values(monkeypatch):
    original_argv = [
        "robinhood_lighter_cycle.py",
        "--quantity",
        "0.00020",
        "--lighter-leverage",
        "3",
    ]
    captured = []

    monkeypatch.setattr(sys, "argv", original_argv)
    monkeypatch.setattr(
        robinhood_lighter_cycle.aster_lighter_cycle,
        "main",
        lambda: captured.extend(sys.argv),
    )

    robinhood_lighter_cycle.main()

    assert captured[-6:-2] == original_argv[-4:]
    assert captured[-2:] == ["--lighter-environment", "robinhood"]
    assert sys.argv is original_argv

    monkeypatch.setattr(sys, "argv", captured)
    parsed = robinhood_lighter_cycle.aster_lighter_cycle._parse_args()
    assert parsed.lighter_leverage == 3
    assert parsed.lighter_environment == "robinhood"


def test_minimal_direct_command_parses_without_repeating_profile_flags(monkeypatch):
    argv = robinhood_lighter_cycle.build_cycle_argv(
        [
            "--quantity",
            "0.00020",
            "--randomize-direction",
            "--cycles",
            "1",
            "--lighter-environment",
            "core",
            "--env-file",
            "/tmp/custom-robinhood.env",
        ]
    )
    monkeypatch.setattr(sys, "argv", argv)

    args = robinhood_lighter_cycle.aster_lighter_cycle._parse_args()

    assert args.aster_ticker == "BTC"
    assert args.lighter_ticker == "BTC"
    assert str(args.quantity) == "0.00020"
    assert args.lighter_environment == "robinhood"
    assert args.env_file == "/tmp/custom-robinhood.env"
    assert args.lighter_leverage == 2
    assert args.virtual_aster_maker is True
    assert args.virtual_maker_price_source == "bn"
    assert args.randomize_direction is True
    assert args.cycles == 1


def test_original_cycle_options_remain_compatible(monkeypatch):
    user_args = [
        "--aster-ticker",
        "BTC",
        "--lighter-ticker",
        "BTC",
        "--quantity",
        "0.001",
        "--aster-quantity",
        "0.01",
        "--lighter-quantity-min",
        "0.01",
        "--lighter-quantity-max",
        "0.02",
        "--randomize-direction",
        "--take-profit",
        "0.02",
        "--slippage",
        "0.3",
        "--max-wait",
        "3",
        "--virtual-aster-maker",
    ]
    monkeypatch.setattr(
        sys, "argv", robinhood_lighter_cycle.build_cycle_argv(user_args)
    )

    args = robinhood_lighter_cycle.aster_lighter_cycle._parse_args()

    assert str(args.aster_quantity) == "0.01"
    assert str(args.lighter_quantity_min) == "0.01"
    assert str(args.lighter_quantity_max) == "0.02"
    assert str(args.take_profit) == "0.02"
    assert str(args.slippage) == "0.3"
    assert args.max_wait == 3


def test_entrypoint_ignores_stale_core_lighter_environment(monkeypatch):
    original_argv = ["robinhood_lighter_cycle.py", "--quantity", "0.00020"]
    observed = {}
    stale_values = {
        "LIGHTER_ENVIRONMENT": "core",
        "LIGHTER_BASE_URL": "https://mainnet.zklighter.elliot.ai",
        "LIGHTER_API_PRIVATE_KEYS": '{"2":"stale"}',
    }
    monkeypatch.setattr(sys, "argv", original_argv)
    for key, value in stale_values.items():
        monkeypatch.setenv(key, value)

    def capture_environment():
        observed.update({key: os.getenv(key) for key in stale_values})

    monkeypatch.setattr(
        robinhood_lighter_cycle.aster_lighter_cycle, "main", capture_environment
    )

    robinhood_lighter_cycle.main()

    assert observed == {key: None for key in stale_values}
    assert {key: os.getenv(key) for key in stale_values} == stale_values
    assert sys.argv is original_argv
