"""One-command Robinhood Chain Lighter market-maker entrypoint.

The implementation lives in :mod:`strategies.lighter_simple_market_maker` so
the original loop remains reusable for tests and operational integrations. This
module is intentionally a thin profile wrapper: it supplies the Robinhood
defaults and leaves normal strategy flags available to the operator.

Run from the repository root with::

    python -m strategies.robinhood_lighter_market_maker

The process places real post-only orders. Use ``--help`` before the first run
and start with ``--allowed-side buy`` or ``--allowed-side sell`` if you want a
single-sided canary.
"""
from __future__ import annotations

import sys
from typing import Iterable, Optional

from strategies import lighter_simple_market_maker as _maker


def build_argv(argv: Iterable[str]) -> list[str]:
    """Add the selected RH env file while preserving user arguments.

    ``lighter_simple_market_maker`` already owns argument validation and RH
    defaults. Keeping this wrapper as argv composition avoids two parsers that
    could drift apart.
    """

    user_args = list(argv)
    if "--env-file" in user_args:
        return user_args
    return ["--env-file", _maker.resolve_default_env_file(), *user_args]


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = list(sys.argv[1:] if argv is None else argv)
    _maker.main(build_argv(args))


if __name__ == "__main__":
    main()
