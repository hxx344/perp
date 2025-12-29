import asyncio
import unittest
from decimal import Decimal

from strategies.hedge_coordinator import HedgeCoordinator


class TestParaRiskLossDefinition(unittest.TestCase):
    def setUp(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

    def tearDown(self) -> None:
        try:
            self._loop.close()
        finally:
            asyncio.set_event_loop(None)

    def test_para_loss_is_max_abs_pnl_for_two_accounts(self) -> None:
        coordinator = HedgeCoordinator()

        # Minimal state injection: coordinator expects state.paradex_accounts to look like
        # {"accounts": [{"total_pnl": ... , "equity": ... , "positions": ...}, ...], "summary": {...}}
        class _State:
            def __init__(self, payload):
                self.paradex_accounts = payload
                self.grvt_accounts = None

        # Two accounts: one +100, one -80 => loss should be 100 (max abs)
        coordinator._states = {  # type: ignore[attr-defined,assignment]
            "agentA": _State(
                {
                    "summary": {"updated_at": 1},
                    "accounts": [
                        {"account_label": "A", "total_pnl": "100", "equity": "1000", "positions": []},
                        {"account_label": "B", "total_pnl": "-80", "equity": "1000", "positions": []},
                    ],
                }
            )
        }

        snap = coordinator._calculate_para_risk()
        self.assertIsNotNone(snap)
        assert snap is not None
        self.assertEqual(snap.worst_loss_value, Decimal("100"))


if __name__ == "__main__":
    unittest.main()
