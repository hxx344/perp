import asyncio
import unittest
from decimal import Decimal


class FeishuParaPushColorTests(unittest.TestCase):
    def test_risk_level_color_green_yellow_red(self):
        from strategies.hedge_coordinator import HedgeCoordinator, ParaRiskSnapshot

        async def _run() -> None:
            coordinator = HedgeCoordinator()
            coordinator._para_risk_stats = ParaRiskSnapshot(
                ratio=0.0,
                risk_capacity=Decimal("100"),
                worst_loss_value=Decimal("1"),
                worst_agent_id="agent-x",
                worst_account_label="acc-x",
            )

            def _authority(ratio):
                return {
                    "risk_capacity_buffered": Decimal("100"),
                    "worst_loss": Decimal("10"),
                    "ratio": ratio,
                    "buffer_note": "",
                    "buffer_status": "fresh",
                }

            coordinator._compute_para_authority_values = lambda: _authority(0.10)  # type: ignore[assignment]
            payload = coordinator._build_para_risk_push_card(now=0.0)
            assert payload is not None
            header = payload["card"]["header"]
            self.assertEqual(header["template"], "green")
            self.assertIn("RISK LEVEL", header["title"]["content"])
            self.assertIn("10.00%", header["title"]["content"])

            coordinator._compute_para_authority_values = lambda: _authority(0.25)  # type: ignore[assignment]
            payload = coordinator._build_para_risk_push_card(now=0.0)
            assert payload is not None
            header = payload["card"]["header"]
            self.assertEqual(header["template"], "orange")
            self.assertIn("25.00%", header["title"]["content"])

            coordinator._compute_para_authority_values = lambda: _authority(0.30)  # type: ignore[assignment]
            payload = coordinator._build_para_risk_push_card(now=0.0)
            assert payload is not None
            header = payload["card"]["header"]
            self.assertEqual(header["template"], "red")
            self.assertIn("30.00%", header["title"]["content"])

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
