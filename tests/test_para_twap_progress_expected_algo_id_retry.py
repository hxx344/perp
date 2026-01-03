from __future__ import annotations

from typing import Any, Dict, List


def test_history_missing_expected_algo_id_retries(monkeypatch):
    """If expected algo_id isn't in history yet, the monitor should wait and refetch.

    This prevents accidentally picking an older TWAP from the candidate list.
    """

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
                "algo_id": "algo-new",
                "algo_market": "ETH-USD-PERP",
                "algo_side": "BUY",
                "algo_expected_size": "2.0",
                "algo_started_at_ms": started_at,
            }
        }
    }

    # Keep history to first page.
    monkeypatch.setenv("PARA_TWAP_PROGRESS_DEBUG", "1")
    monkeypatch.setenv("PARA_TWAP_HISTORY_MAX_PAGES", "1")
    monkeypatch.setenv("PARA_TWAP_HISTORY_PAGE_LIMIT", "50")

    import requests

    calls: List[Dict[str, Any]] = []

    def fake_get(url: str, *args: Any, **kwargs: Any):
        # First call returns only an older algo (wrong id). Second call includes expected id.
        calls.append({"url": url, "headers": kwargs.get("headers")})
        idx = len(calls)

        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                if idx == 1:
                    return {
                        "next": None,
                        "results": [
                            {
                                "id": "algo-old",
                                "algo_type": "TWAP",
                                "market": "ETH-USD-PERP",
                                "side": "BUY",
                                "status": "OPEN",
                                "size": "2.0",
                                "remaining_size": "1.0",
                                "avg_fill_price": "3000.0",
                                "created_at": started_at - 60_000,
                                "last_updated_at": started_at - 30_000,
                            }
                        ],
                    }
                return {
                    "next": None,
                    "results": [
                        {
                            "id": "algo-new",
                            "algo_type": "TWAP",
                            "market": "ETH-USD-PERP",
                            "side": "BUY",
                            "status": "OPEN",
                            "size": "2.0",
                            "remaining_size": "0.5",
                            "avg_fill_price": "3100.0",
                            "created_at": started_at + 1_000,
                            "last_updated_at": started_at + 2_000,
                        }
                    ],
                }

        return Resp()

    monkeypatch.setattr(requests, "get", fake_get)

    # Don't actually sleep in tests; record the requested sleep.
    slept: List[float] = []

    def fake_sleep(seconds: float):
        slept.append(seconds)
        return None

    monkeypatch.setattr("monitoring.para_account_monitor.time.sleep", fake_sleep)

    captured: Dict[str, Any] = {}

    def fake_ack(request_id: str, status: str, note: Any, extra: Any = None) -> bool:
        captured["extra"] = extra
        return True

    mon._acknowledge_adjustment = fake_ack  # type: ignore[assignment]

    mon._poll_twap_progress("req")

    # Ensure we retried once with a ~5s delay.
    assert slept and slept[0] == 5.0
    # Ensure we fetched twice.
    assert len(calls) >= 2
    # Ensure final progress uses the expected algo id.
    payload = captured["extra"]
    assert payload["algo_id"] == "algo-new"
