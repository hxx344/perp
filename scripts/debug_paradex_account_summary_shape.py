"""Offline sanity checks for Paradex account summary parsing.

This script doesn't require API keys. It only exercises the parsing logic in
`monitoring/para_account_monitor.py`.

It simulates the *authoritative* paradex-py response shape:
- api_client.fetch_account_summary() -> AccountSummary dataclass (or dict)

It also simulates the non-authoritative /account/info shape that often looks like:
- {"results": [ ... ]}

Usage (PowerShell):
    python scripts\\debug_paradex_account_summary_shape.py
"""

from __future__ import annotations

import sys
import traceback
from dataclasses import dataclass
from pathlib import Path


# Keep this dataclass aligned with paradex-py/paradex_py/api/models.py::AccountSummary
@dataclass
class AccountSummary:
    account: str = "0xabc"
    initial_margin_requirement: str = "123.45"
    maintenance_margin_requirement: str = "67.89"
    account_value: str = "10000"
    total_collateral: str = "5000"
    free_collateral: str = "4000"
    margin_cushion: str = "0.5"
    settlement_asset: str = "USDC"
    updated_at: int = 0
    status: str = "ACTIVE"
    seq_no: int = 1


def main() -> None:
    # Make import stable regardless of how the script is launched.
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root))
    try:
        from monitoring.para_account_monitor import ParadexAccountMonitor
    except Exception as exc:
        traceback.print_exc()
        raise SystemExit(
            "Failed to import monitoring.para_account_monitor. "
            "Run this from the perp-dex-tools folder with the same venv as the monitor. "
            f"Import error: {exc}"
        )

    dummy_account_obj = object()

    # Case 1: paradex-py AccountSummary dataclass
    payload_dc = AccountSummary()
    extracted1 = ParadexAccountMonitor._extract_account_im_fields(dummy_account_obj, payload_dc.__dict__)
    print("case1(dataclass.__dict__):", extracted1)

    # Case 2: flat dict (as if already normalised)
    payload_dict = payload_dc.__dict__.copy()
    extracted2 = ParadexAccountMonitor._extract_account_im_fields(dummy_account_obj, payload_dict)
    print("case2(flat dict):", extracted2)

    # Case 3: /account/info-ish wrapper
    payload_results = {"results": [payload_dict]}
    extracted3 = ParadexAccountMonitor._extract_account_im_fields(dummy_account_obj, payload_results)
    print("case3(results wrapper):", extracted3)


if __name__ == "__main__":
    main()
