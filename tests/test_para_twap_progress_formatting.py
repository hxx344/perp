from __future__ import annotations

from typing import Any, Dict


def test_twap_progress_formats_avg_price_and_filled_qty(monkeypatch):
    from monitoring.para_account_monitor import ParadexAccountMonitor

    class FakeApiClient:
        api_url = "https://api.prod.paradex.trade/v1"

        def __init__(self):
            self.client = None
            self._manual_token = "test.jwt.token"

    class FakeParadex:
        def __init__(self):
            self.api_client = FakeApiClient()
            self.account = None

    mon = ParadexAccountMonitor.__new__(ParadexAccountMonitor)
    mon._client = FakeParadex()
    mon._timeout = 0.001
    mon._agent_id = "agent"

    started_at = 1700000000000
    mon._processed_adjustments = {
        "req": {
            "extra": {
                "order_type": "TWAP",
                "algo_id": "algo-1",
                "algo_market": "ETH-USD-PERP",
                "algo_side": "BUY",
                "algo_expected_size": "2.0",
                "algo_started_at_ms": started_at,
            }
        }
    }

    monkeypatch.setenv("PARA_TWAP_PROGRESS_HISTORY_ONLY", "1")
    monkeypatch.setenv("PARA_TWAP_PROGRESS_DEBUG", "")
    monkeypatch.setenv("PARA_TWAP_HISTORY_MAX_PAGES", "1")
    monkeypatch.setenv("PARA_TWAP_HISTORY_PAGE_LIMIT", "50")

    import requests

    def fake_get(url: str, *args: Any, **kwargs: Any):
        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "next": None,
                    "results": [
                        {
                            "id": "algo-1",
                            "algo_type": "TWAP",
                            "market": "ETH-USD-PERP",
                            "side": "BUY",
                            "status": "OPEN",
                            "size": "2.0",
                            "remaining_size": "0.7654321",
                            "avg_fill_price": "3097.85991",
                            "created_at": started_at + 1000,
                            "last_updated_at": started_at + 2000,
                        }
                    ],
                }

        return Resp()

    monkeypatch.setattr(requests, "get", fake_get)

    captured: Dict[str, Any] = {}

    def fake_ack(request_id: str, status: str, note: Any, extra: Any = None) -> bool:
        captured["extra"] = extra
        return True

    mon._acknowledge_adjustment = fake_ack  # type: ignore[assignment]

    mon._poll_twap_progress("req")
    payload = captured["extra"]
    assert payload["avg_price"] == "3097.8599"
    # filled = 2.0 - 0.7654321 = 1.2345679 -> 1.2346
    assert payload["filled_qty"] == "1.2346"


def test_formatting_strips_trailing_zeros():
    from monitoring.para_account_monitor import _format_decimal_places

    assert _format_decimal_places("3000", 2) == "3000"
    assert _format_decimal_places("3000.0", 2) == "3000"
    assert _format_decimal_places("3000.10", 2) == "3000.1"
    assert _format_decimal_places("0.0000", 4) == "0"
    assert _format_decimal_places("1.2300", 4) == "1.23"
