import unittest
from decimal import Decimal
import sys
import types
from typing import Any, cast


class ParadexTwapPayloadValidationTests(unittest.TestCase):
    def test_twap_payload_algo_type_and_duration_clamp(self):
        from monitoring import para_account_monitor as pam

        # Provide a tiny stub for `paradex_py.common.order` so the test suite
        # can run without installing paradex-py.
        stub_paradex = types.ModuleType("paradex_py")
        stub_common = types.ModuleType("paradex_py.common")
        stub_order = types.ModuleType("paradex_py.common.order")

        class _OrderType:
            Market = "MARKET"

        class _OrderSide:
            Buy = types.SimpleNamespace(name="BUY")
            Sell = types.SimpleNamespace(name="SELL")

        class _Order:
            def __init__(self, market: str, order_type: Any, order_side: Any, size: Any, signature_timestamp: int):
                self.market = market
                self.order_type = order_type
                self.order_side = order_side
                self.size = size
                self.signature_timestamp = signature_timestamp

        setattr(stub_order, "Order", _Order)
        setattr(stub_order, "OrderType", _OrderType)
        setattr(stub_order, "OrderSide", _OrderSide)

        original_modules = dict(sys.modules)
        sys.modules.setdefault("paradex_py", stub_paradex)  # type: ignore[arg-type]
        sys.modules.setdefault("paradex_py.common", stub_common)  # type: ignore[arg-type]
        sys.modules.setdefault("paradex_py.common.order", stub_order)  # type: ignore[arg-type]

        # Patch module-level helpers used by _place_twap_order.
        orig_flatten = pam.flatten_signature
        orig_build = pam.build_order_message
        try:
            pam.flatten_signature = lambda sig: "FLAT"  # type: ignore[assignment]
            pam.build_order_message = lambda chain_id, order: {"chain_id": chain_id, "market": order.market}  # type: ignore[assignment]

            monitor = pam.ParadexAccountMonitor.__new__(pam.ParadexAccountMonitor)

            class _ApiClient:
                def __init__(self):
                    self.last_payload = None

                def _post_authorized(self, *, path: str, payload: dict):
                    self.last_payload = {"path": path, "payload": payload}
                    return {"ok": True}

            class _Account:
                l2_chain_id = 123

                class _Starknet:
                    def sign_message(self, msg):
                        return ("r", "s")

                starknet = _Starknet()

            monitor._client = type(
                "C",
                (),
                {
                    "account": _Account(),
                    "api_client": _ApiClient(),
                },
            )()

            # duration_seconds < 30 should be clamped to 30.
            monitor._place_twap_order("BTC-USD-PERP", "buy", Decimal("1"), 10)

            client = cast(Any, monitor._client)
            posted = client.api_client.last_payload
            self.assertIsNotNone(posted)
            self.assertEqual(posted["path"], "algo/orders")
            payload = posted["payload"]

            self.assertEqual(payload.get("algo_type"), "TWAP")
            self.assertEqual(payload.get("freq"), 30)
            self.assertEqual(payload.get("duration_seconds"), 30)
            self.assertTrue(payload.get("signature"))
        finally:
            pam.flatten_signature = orig_flatten
            pam.build_order_message = orig_build
            # Restore sys.modules to avoid polluting other tests.
            sys.modules.clear()
            sys.modules.update(original_modules)


if __name__ == "__main__":
    unittest.main()
