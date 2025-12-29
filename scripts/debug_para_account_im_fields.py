#!/usr/bin/env python3
"""Debug helper: print monitor-extracted IM fields.

This script doesn't require coordinator access; it only checks how the monitor code
extracts IM requirement fields from a Paradex-like account object + REST payload.

Usage:
  python scripts/debug_para_account_im_fields.py

It will run a few synthetic cases and print the extracted fields.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys

# Make `perp-dex-tools/` importable when running from repo root.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from monitoring.para_account_monitor import ParadexAccountMonitor  # type: ignore


@dataclass
class DummyAccount:
    initial_margin_requirement: str | None = None
    initial_margin_requirement: str | None = None
    maintenance_margin_requirement: str | None = None
    initial_margin: str | None = None


def main() -> None:
    cases = [
        ("sdk has initial_margin_requirement", DummyAccount(initial_margin_requirement="123.45"), None),
        (
            "sdk missing, rest has initial_margin_requirement",
            DummyAccount(),
            {"initial_margin_requirement": "999.0", "maintenance_margin_requirement": "111.0"},
        ),
        (
            "sdk has only legacy initial_margin",
            DummyAccount(initial_margin="5"),
            {"initial_margin_requirement": "10"},
        ),
    ]

    for title, acct, rest in cases:
        out = ParadexAccountMonitor._extract_account_im_fields(acct, rest)  # type: ignore[arg-type]
        print("\n==", title, "==")
        for k in ("initial_margin_requirement", "maintenance_margin_requirement", "initial_margin", "im_req_source"):
            print(f"{k}: {out.get(k)}")


if __name__ == "__main__":
    main()
