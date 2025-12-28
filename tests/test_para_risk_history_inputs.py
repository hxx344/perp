import asyncio
import time
import unittest
from decimal import Decimal

from strategies.hedge_coordinator import HedgeCoordinator, ParaRiskSnapshot, RiskAlertInfo, PARA_RISK_ALERT_KEY


class TestParaRiskHistoryInputs(unittest.TestCase):
    def setUp(self) -> None:
        # HedgeCoordinator creates asyncio.Lock() at init; ensure an event loop exists.
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

    def tearDown(self) -> None:
        try:
            self._loop.close()
        finally:
            asyncio.set_event_loop(None)

    def test_para_history_contains_input_fields(self):
        coordinator = HedgeCoordinator()

        # Force a para risk stats snapshot with known inputs.
        now = time.time()
        coordinator._para_risk_stats = ParaRiskSnapshot(
            ratio=0.5,
            risk_capacity=Decimal("1000"),
            worst_loss_value=Decimal("500"),
            worst_agent_id="agent-1",
            worst_account_label="PARA",
            equity_sum=Decimal("2500"),
            max_initial_margin=Decimal("1000"),
            account_count=2,
            computed_at=now,
        )

        alert = RiskAlertInfo(
            key=PARA_RISK_ALERT_KEY,
            agent_id="agent-1",
            account_label="PARA",
            ratio=0.5,
            loss_value=Decimal("500"),
            base_value=Decimal("900"),
            base_label="risk_capacity(buffered)",
        )

        async def _run() -> None:
            await coordinator._record_alert_history(
                alert,
                source="test",
                status="ok",
                title="t",
                body="b",
            )
            history = await coordinator.alert_history_snapshot(limit=1)
            self.assertEqual(len(history), 1)
            entry = history[0]
            self.assertEqual(entry.get("kind"), "para_risk")
            # Raw fields already existed.
            self.assertIn("raw_base_value", entry)
            # New input fields.
            self.assertIn("para_risk_computed_at", entry)
            self.assertIn("para_equity_sum", entry)
            self.assertIn("para_max_initial_margin", entry)
            self.assertIn("para_account_count", entry)

        self._loop.run_until_complete(_run())


if __name__ == "__main__":
    unittest.main()
