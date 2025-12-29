import asyncio
import unittest
from decimal import Decimal


class FeishuParaPushAlignmentTests(unittest.TestCase):
    def test_feishu_push_uses_authority_capacity_and_loss(self):
        from strategies.hedge_coordinator import HedgeCoordinator, ParaRiskSnapshot

        async def _run() -> None:
            coordinator = HedgeCoordinator()

            # Stats exist so push path is active, but we will override the authority computation.
            coordinator._para_risk_stats = ParaRiskSnapshot(
                ratio=0.99,
                risk_capacity=Decimal("999"),
                worst_loss_value=Decimal("888"),
                worst_agent_id="agent-x",
                worst_account_label="acc-x",
            )

            def _fake_authority():
                return {
                    "inputs": {
                        "worst_account_label": "AUTH_LABEL",
                        "worst_agent_id": "AUTH_AGENT",
                    },
                    "risk_capacity_buffered": Decimal("123"),
                    "worst_loss": Decimal("456"),
                    "ratio": 0.25,
                    "buffer_note": "note",
                    "buffer_status": "fresh",
                }

            coordinator._compute_para_authority_values = _fake_authority  # type: ignore[assignment]

            text = coordinator._build_para_risk_push_text(now=0.0)
            self.assertIsInstance(text, str)
            assert text is not None
            self.assertIn("RISK LEVEL:", text)
            self.assertIn("25.00%", text)
            self.assertIn("风险裕量(risk_capacity): 123.00", text)
            self.assertIn("最坏亏损(worst_loss): 456.00", text)

            # Ensure it does NOT leak the non-authority numbers from ParaRiskSnapshot.
            self.assertNotIn("999.00", text)
            self.assertNotIn("888.00", text)

            # Worst account should also come from authority inputs, not ParaRiskSnapshot.
            assert "最坏账户" in text
            assert "AUTH_LABEL" in text
            assert "(AUTH_AGENT)" in text

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
