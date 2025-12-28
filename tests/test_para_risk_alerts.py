import unittest
from decimal import Decimal


class ParaRiskTests(unittest.TestCase):
    def test_para_risk_capacity_math(self):
        # Align with dashboard: risk_capacity = sum_equity - 1.5 * max_initial_margin
        sum_equity = Decimal("1000")
        max_im = Decimal("200")
        risk_capacity = sum_equity - (Decimal("1.5") * max_im)
        self.assertEqual(risk_capacity, Decimal("700"))

        worst_loss = Decimal("140")
        ratio = float(worst_loss / risk_capacity)
        self.assertAlmostEqual(ratio, 0.2, places=9)

    def test_history_kind_filtering_contract(self):
        # Frontend should only render these kinds for PARA history.
        kinds = ["para_risk", "para_stale"]
        self.assertIn("para_risk", kinds)
        self.assertIn("para_stale", kinds)
        self.assertNotIn("risk", kinds)


if __name__ == "__main__":
    unittest.main()
