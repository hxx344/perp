import unittest
from decimal import Decimal
from typing import Any, cast


class ParaBarkAlertScopeGatingTests(unittest.TestCase):
    def test_para_alert_not_blocked_by_missing_global_bark(self):
        from strategies.hedge_coordinator import BarkNotifier, HedgeCoordinator, ParaRiskSnapshot

        coord = HedgeCoordinator()

        # Simulate: PARA bark is configured, GLOBAL bark is not.
        cast(Any, coord)._bark_notifier = None
        cast(Any, coord)._para_bark_notifier = BarkNotifier("http://example.invalid", append_payload=False)
        cast(Any, coord)._para_risk_alert_threshold = 0.2
        cast(Any, coord)._para_risk_alert_reset = 0.1
        cast(Any, coord)._para_risk_alert_cooldown = 0.0

        # Provide minimal para stats so _prepare_para_risk_alerts can build RiskAlertInfo.
        cast(Any, coord)._para_risk_stats = ParaRiskSnapshot(
            ratio=0.25,
            risk_capacity=Decimal("1000"),
            worst_loss_value=Decimal("250"),
            worst_agent_id="agent-1",
            worst_account_label="acc-1",
        )

        # Make authority values meet threshold/positive checks.
        cast(Any, coord)._compute_para_authority_values = lambda: {
            "ratio": 0.25,
            "worst_loss": Decimal("250"),
            "risk_capacity_buffered": Decimal("1000"),
        }

        alerts = coord._prepare_para_risk_alerts("agent-1")
        self.assertTrue(alerts, "Expected PARA alert even when global bark is not configured")

    def test_para_stale_not_blocked_by_missing_global_bark(self):
        from strategies.hedge_coordinator import BarkNotifier, HedgeCoordinator, HedgeState

        coord = HedgeCoordinator()
        cast(Any, coord)._bark_notifier = None
        cast(Any, coord)._para_bark_notifier = BarkNotifier("http://example.invalid", append_payload=False)
        cast(Any, coord)._para_stale_critical_seconds = 1.0
        cast(Any, coord)._risk_alert_cooldown = 0.0

        # Minimal state with old timestamp.
        state = HedgeState(agent_id="agent-1")
        state.paradex_accounts = {"summary": {"updated_at": 0}}
        state.last_update_ts = 0.0
        cast(Any, coord)._states = {"agent-1": state}

        alerts = coord._prepare_para_stale_alerts(now=10.0)
        self.assertTrue(alerts, "Expected PARA stale alert even when global bark is not configured")


if __name__ == "__main__":
    unittest.main()
