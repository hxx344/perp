import json
import unittest
from pathlib import Path
import asyncio


class TestParaAutoBalancePersistence(unittest.TestCase):
    def test_persist_and_reload_para_auto_balance(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            from strategies.hedge_coordinator import (
                CoordinatorApp,
                PERSISTED_PARA_AUTO_BALANCE_FILE,
            )

            path = Path(PERSISTED_PARA_AUTO_BALANCE_FILE)
            if path.exists():
                path.unlink()

            app = CoordinatorApp(enable_volatility_monitor=False)

            cfg_body = {
                "agent_a": "agentA",
                "agent_b": "agentB",
                "threshold_ratio": 0.1,
                "min_transfer": "5",
                "max_transfer": "10",
                "cooldown_seconds": 60,
                "currency": "USDC",
                "use_available_equity": True,
            }
            cfg = app._parse_auto_balance_config(cfg_body, default_currency="USDC")
            app._update_para_auto_balance_config(cfg)
            app._persist_para_auto_balance_config()

            self.assertTrue(path.exists())
            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertTrue(payload["enabled"])
            self.assertIsInstance(payload["config"], dict)
            self.assertEqual(payload["config"]["agent_a"], "agentA")

            # new instance should reload
            app2 = CoordinatorApp(enable_volatility_monitor=False)
            loaded = app2._para_auto_balance_config_as_payload()
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded["agent_a"], "agentA")
            self.assertEqual(loaded["agent_b"], "agentB")
            self.assertAlmostEqual(float(loaded["threshold_ratio"]), 0.1)
            self.assertEqual(loaded["currency"], "USDC")

            # disable should persist disabled and next instance should not load
            app2._update_para_auto_balance_config(None)
            app2._persist_para_auto_balance_config()
            app3 = CoordinatorApp(enable_volatility_monitor=False)
            self.assertIsNone(app3._para_auto_balance_config_as_payload())
        finally:
            loop.close()


if __name__ == "__main__":
    unittest.main()
