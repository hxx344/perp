import unittest
from decimal import Decimal


class TestFeishuParaPush(unittest.TestCase):
    def test_risk_capacity_buffer_pending_keeps_previous(self):
        # Import inside test to avoid side effects during collection
        from strategies.hedge_coordinator import (
            RiskCapacityBufferState,
            _evaluate_risk_capacity_buffer,
            RISK_CAPACITY_CONFIRMATION_CYCLES,
        )

        state = RiskCapacityBufferState()
        # establish accepted value
        value1 = Decimal("10000")
        display, note, status = _evaluate_risk_capacity_buffer(
            has_value=True,
            value=value1,
            timestamp=1000.0,
            base_note="",
            state=state,
        )
        self.assertEqual(display, value1)
        self.assertEqual(status, "fresh")

        # large jump -> pending, should keep previous display temporarily
        value2 = Decimal("15000")
        display2, note2, status2 = _evaluate_risk_capacity_buffer(
            has_value=True,
            value=value2,
            timestamp=1010.0,
            base_note="",
            state=state,
        )
        self.assertEqual(display2, value1)
        self.assertEqual(status2, "pending")
        self.assertIn("数据确认中", note2)

        # repeat stable pending value enough cycles -> accept
        for i in range(2, RISK_CAPACITY_CONFIRMATION_CYCLES + 1):
            display_n, _, status_n = _evaluate_risk_capacity_buffer(
                has_value=True,
                value=value2,
                timestamp=1010.0 + i,
                base_note="",
                state=state,
            )
        self.assertEqual(display_n, value2)
        self.assertEqual(status_n, "fresh")

    def test_format_percent(self):
        from strategies.hedge_coordinator import _format_percent

        self.assertEqual(_format_percent(12.3456, 2), "12.35%")


if __name__ == "__main__":
    unittest.main()
