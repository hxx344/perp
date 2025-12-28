import asyncio
import unittest


class RiskAlertTestHistoryTests(unittest.TestCase):
    def test_global_test_alert_writes_risk_kind(self):
        from strategies.hedge_coordinator import HedgeCoordinator, RiskAlertSettings

        async def _run() -> None:
            settings = RiskAlertSettings(
                threshold=0.3,
                reset_ratio=0.2,
                cooldown=0.0,
                bark_url="http://example.com/{title}/{body}",
                bark_timeout=1.0,
                bark_append_payload=False,
                title_template="",
                body_template="",
            )
            coordinator = HedgeCoordinator(alert_settings=settings)

            before = await coordinator.alert_history_snapshot()
            self.assertEqual(len(before), 0)

            await coordinator.trigger_test_alert({"ratio": 0.31})
            after = await coordinator.alert_history_snapshot()
            self.assertEqual(len(after), 1)
            self.assertEqual(after[0].get("source"), "test")
            self.assertEqual(after[0].get("kind"), "risk")

        asyncio.run(_run())

    def test_para_test_alert_writes_para_risk_kind(self):
        from strategies.hedge_coordinator import HedgeCoordinator, RiskAlertSettings

        async def _run() -> None:
            # trigger_test_alert(use_para=True) checks PARA notifier/threshold, not the global ones.
            settings = RiskAlertSettings(
                threshold=0.3,
                reset_ratio=0.2,
                cooldown=0.0,
                bark_url="http://example.com/{title}/{body}",
                bark_timeout=1.0,
                bark_append_payload=False,
                title_template="",
                body_template="",
            )
            coordinator = HedgeCoordinator(alert_settings=settings)
            # Enable PARA test path.
            coordinator._para_risk_alert_threshold = 0.3
            coordinator._para_bark_notifier = coordinator._bark_notifier

            await coordinator.trigger_test_alert({"kind": "para_risk", "ratio": 0.31})
            after = await coordinator.alert_history_snapshot()
            self.assertEqual(len(after), 1)
            self.assertEqual(after[0].get("source"), "test")
            self.assertEqual(after[0].get("kind"), "para_risk")

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
