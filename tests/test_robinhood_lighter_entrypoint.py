from __future__ import annotations

import os
import sys

from strategies import robinhood_lighter_cycle


def test_profile_selects_robinhood_virtual_binance_and_conservative_defaults():
    args = robinhood_lighter_cycle.ROBINHOOD_DEFAULT_ARGS
    required = robinhood_lighter_cycle.ROBINHOOD_REQUIRED_ARGS

    assert args[args.index("--env-file") + 1] == ".env.robinhood"
    assert required == ("--lighter-environment", "robinhood")
    assert args[args.index("--lighter-leverage") + 1] == "2"
    assert args[args.index("--virtual-maker-price-source") + 1] == "bn"
    assert "--virtual-aster-maker" in args
    assert "--preserve-initial-position" in args
    assert "--quantity" not in args
    assert "--cycles" not in args


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
        ]
    )
    monkeypatch.setattr(sys, "argv", argv)

    args = robinhood_lighter_cycle.aster_lighter_cycle._parse_args()

    assert args.aster_ticker == "BTC"
    assert args.lighter_ticker == "BTC"
    assert str(args.quantity) == "0.00020"
    assert args.lighter_environment == "robinhood"
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
