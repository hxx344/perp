"""Regression tests: PARA alert history should align with dashboard effective risk capacity.

Dashboard uses a buffered (smoothed) risk capacity value as the authoritative denominator.
We mirror that rule in the backend alert history so that the history table matches the cards.
"""

import asyncio
import unittest
from decimal import Decimal

from strategies.hedge_coordinator import HedgeCoordinator, ParaRiskSnapshot


class TestParaRiskAlertHistoryAlignment(unittest.TestCase):
    def setUp(self) -> None:
        # HedgeCoordinator creates asyncio.Lock() at init; ensure an event loop exists.
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

    def tearDown(self) -> None:
        try:
            self._loop.close()
        finally:
            asyncio.set_event_loop(None)

    def test_para_history_base_value_uses_buffered_capacity(self) -> None:
        coordinator = HedgeCoordinator()

        class _DummyNotifier:
            async def send(self, title: str = "", body: str = "") -> None:
                return

        coordinator._bark_notifier = _DummyNotifier()  # type: ignore[attr-defined]
        coordinator._para_bark_notifier = _DummyNotifier()  # type: ignore[attr-defined]
        coordinator._para_risk_alert_threshold = 0.2  # type: ignore[attr-defined]
        coordinator._para_risk_alert_cooldown = 0.0  # type: ignore[attr-defined]

        # First cycle accepts 1000 as the buffered baseline.
        coordinator._para_risk_stats = ParaRiskSnapshot(
            ratio=0.25,
            risk_capacity=Decimal("1000"),
            worst_loss_value=Decimal("250"),
            worst_agent_id="agentA",
            worst_account_label="AccountA",
        )
        self._loop.run_until_complete(coordinator.trigger_test_alert({"kind": "para"}))
        history1 = self._loop.run_until_complete(coordinator.alert_history_snapshot(limit=1))
        self.assertEqual(len(history1), 1)
        self.assertEqual(history1[0].get("kind"), "para_risk")
        self.assertEqual(history1[0].get("base_label"), "risk_capacity(frontend_authority)")
        base1 = history1[0].get("base_value_raw")
        self.assertIsInstance(base1, (int, float))
        self.assertAlmostEqual(float(base1 or 0.0), 1000.0, places=6)

        # Second cycle: raw capacity jumps to 2000, but buffer should keep using 1000
        # until confirmation cycles are met.
        coordinator._para_risk_stats = ParaRiskSnapshot(
            ratio=0.25,
            risk_capacity=Decimal("2000"),
            worst_loss_value=Decimal("500"),
            worst_agent_id="agentA",
            worst_account_label="AccountA",
        )
        self._loop.run_until_complete(coordinator.trigger_test_alert({"kind": "para"}))
        history2 = self._loop.run_until_complete(coordinator.alert_history_snapshot(limit=1))
        self.assertEqual(len(history2), 1)
        self.assertEqual(history2[0].get("kind"), "para_risk")
        self.assertEqual(history2[0].get("base_label"), "risk_capacity(frontend_authority)")
        base2 = history2[0].get("base_value_raw")
        self.assertIsInstance(base2, (int, float))
        self.assertAlmostEqual(float(base2 or 0.0), 2000.0, places=6)


if __name__ == "__main__":
    unittest.main()
