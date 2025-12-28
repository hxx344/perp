"""Quick self-test: global risk alert uses global_risk, not per-account.

This script doesn't hit real exchanges. It only exercises the coordinator's
risk aggregation + alert candidate computation.

Run:
  python -m perp-dex-tools.scripts.risk_alert_global_selftest
(or just `python scripts/risk_alert_global_selftest.py` from perp-dex-tools)
"""

from __future__ import annotations

from dataclasses import asdict
from decimal import Decimal


def main() -> None:
    # Import locally to avoid import side effects when this file isn't used.
    from perp_dex_tools.strategies.hedge_coordinator import (  # type: ignore
        HedgeCoordinator,
        RiskAlertSettings,
    )

    # Enable alerts with a low threshold so we can trigger with small numbers.
    settings = RiskAlertSettings(
        threshold=0.20,  # 20%
        cooldown=0.0,
        bark_url="https://example.invalid/bark",  # won't be called in this self-test
    ).normalized()

    coord = HedgeCoordinator(alert_settings=settings)

    # Two agents; agent-a has a BIGGER *loss*, but agent-b has the smaller coordinates for *capacity*.
    # Global risk uses: worst_loss / (sum_equity - sum_initial_margin)
    payload_a = {
        "agent_id": "agent-a",
        "grvt_accounts": {
            "updated_at": 1,
            "summary": {
                "accounts": [],
            },
            "accounts": [
                {
                    "name": "A",
                    "equity": "1000",
                    "initial_margin_requirement": "100",
                    "total_pnl": "-250",  # worst loss 250
                }
            ],
        },
    }
    payload_b = {
        "agent_id": "agent-b",
        "grvt_accounts": {
            "updated_at": 1,
            "accounts": [
                {
                    "name": "B",
                    "equity": "500",
                    "initial_margin_requirement": "450",
                    "total_pnl": "-10",
                }
            ],
        },
    }

    # Push updates (sync wrapper: call async update via loop-less helper)
    # HedgeCoordinator.update is async; simplest is to use asyncio.run.
    import asyncio

    asyncio.run(coord.update(payload_a))
    asyncio.run(coord.update(payload_b))

    # Now compute global snapshot.
    snap = asyncio.run(coord.snapshot())
    global_risk = snap.get("global_risk") or {}

    # The key property: ratio should be worst_loss / (sum_equity - sum_im)
    # sum_equity=1500, sum_im=550 => capacity=950, worst_loss=250 => ratio≈0.26316
    expected_ratio = float(Decimal("250") / Decimal("950"))
    ratio = global_risk.get("ratio")

    print("global_risk:", global_risk)
    print("expected_ratio:", expected_ratio)
    if ratio is None:
        raise SystemExit("FAIL: global_risk.ratio missing")
    if abs(float(ratio) - expected_ratio) > 1e-6:
        raise SystemExit(f"FAIL: ratio mismatch: got {ratio}, expected {expected_ratio}")

    print("PASS: global risk ratio matches expected aggregation")


if __name__ == "__main__":
    main()
