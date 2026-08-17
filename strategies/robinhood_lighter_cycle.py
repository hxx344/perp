"""Direct Robinhood Lighter profile for the existing Aster-Lighter cycle."""

from __future__ import annotations

import os
import sys
from typing import Sequence

from strategies import aster_lighter_cycle


ROBINHOOD_DEFAULT_ARGS: tuple[str, ...] = (
    "--env-file",
    ".env.robinhood",
    "--aster-ticker",
    "BTC",
    "--lighter-ticker",
    "BTC",
    "--lighter-leverage",
    "2",
    "--virtual-aster-maker",
    "--virtual-maker-price-source",
    "bn",
    "--preserve-initial-position",
    "--aster-maker-depth",
    "10",
    "--lighter-max-wait",
    "10",
)
ROBINHOOD_REQUIRED_ARGS: tuple[str, ...] = (
    "--lighter-environment",
    "robinhood",
)
LIGHTER_ENV_KEYS: tuple[str, ...] = (
    "LIGHTER_ENVIRONMENT",
    "LIGHTER_ENDPOINT_PROFILE",
    "LIGHTER_BASE_URL",
    "LIGHTER_WS_URL",
    "LIGHTER_CHAIN_ID",
    "LIGHTER_ACCOUNT_INDEX",
    "LIGHTER_API_PRIVATE_KEYS",
    "API_KEY_PRIVATE_KEYS",
    "API_KEY_PRIVATE_KEY",
    "LIGHTER_API_KEY_INDEX",
    "L1_WALLET_PRIVATE_KEY",
    "LIGHTER_L1_PRIVATE_KEY",
)


def build_cycle_argv(user_args: Sequence[str]) -> list[str]:
    """Apply RH defaults, user overrides, then the mandatory endpoint profile."""
    return [
        sys.argv[0],
        *ROBINHOOD_DEFAULT_ARGS,
        *user_args,
        *ROBINHOOD_REQUIRED_ARGS,
    ]


def main() -> None:
    original_argv = sys.argv
    original_env = {key: os.environ[key] for key in LIGHTER_ENV_KEYS if key in os.environ}
    sys.argv = build_cycle_argv(original_argv[1:])
    for key in LIGHTER_ENV_KEYS:
        os.environ.pop(key, None)
    try:
        aster_lighter_cycle.main()
    finally:
        sys.argv = original_argv
        for key in LIGHTER_ENV_KEYS:
            os.environ.pop(key, None)
        os.environ.update(original_env)


if __name__ == "__main__":
    main()
