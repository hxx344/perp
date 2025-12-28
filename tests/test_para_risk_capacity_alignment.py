import asyncio
import unittest
from decimal import Decimal

from strategies.hedge_coordinator import HedgeCoordinator, HedgeState


class TestParaRiskCapacityAlignment(unittest.TestCase):
    def test_para_risk_capacity_prefers_initial_margin_field(self):
        # Python 3.9: HedgeCoordinator builds asyncio.Lock() in __init__
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            coordinator = HedgeCoordinator(alert_settings=None)
            state = HedgeState(agent_id="agent-1")
            state.paradex_accounts = {
                "accounts": [
                    {"name": "A", "equity": "100", "total_pnl": "-5", "initial_margin": "10"},
                    {"name": "B", "equity": "200", "total_pnl": "-12", "initial_margin": "60"},
                ]
            }
            coordinator._states = {"agent-1": state}

            snap = coordinator._calculate_para_risk()
            self.assertIsNotNone(snap)
            assert snap is not None

            # (100 + 200) - 1.5 * max(10, 60) = 210
            self.assertEqual(snap.risk_capacity, Decimal("210"))
        finally:
            loop.close()


if __name__ == "__main__":
    unittest.main()
