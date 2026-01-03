from __future__ import annotations

from typing import Any


def test_twap_progress_strips_trailing_v1_from_api_url(monkeypatch):
    """Regression: production api_client.api_url may already end with /v1.

    Our helper clients append /v1 internally, so we must strip it or requests become /v1/v1/... (404).
    """

    from monitoring.para_account_monitor import ParadexAccountMonitor

    class FakeApiClient:
        api_url = "https://api.prod.paradex.trade/v1"  # already includes /v1

        def __init__(self):
            # Make token extraction use _manual_token path.
            self.client = None
            self._manual_token = "test.jwt.token"

    class FakeParadex:
        def __init__(self):
            self.api_client = FakeApiClient()
            self.account = None

    mon = ParadexAccountMonitor.__new__(ParadexAccountMonitor)
    mon._client = FakeParadex()
    mon._timeout = 1.0
    mon._agent_id = "test-agent"

    # Minimal wiring for internals.
    mon._processed_adjustments = {
        "req": {
            "extra": {
                "algo_market": "ETH-USD-PERP",
                "algo_side": "SELL",
                "algo_expected_size": "0.1",
                "algo_started_at_ms": 1700000000000,
                "twap_duration_seconds": 30,
            }
        }
    }

    # Capture the URL used by requests.get inside ParadexPrivateClient.
    import requests

    captured: dict[str, Any] = {}

    def fake_get(url: str, *args: Any, **kwargs: Any):
        captured["url"] = url
        # Return a minimal response-like object.
        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return {"results": []}

        return Resp()

    monkeypatch.setattr(requests, "get", fake_get)

    # Enable history-only and debug so it takes the history path.
    monkeypatch.setenv("PARA_TWAP_PROGRESS_HISTORY_ONLY", "1")
    monkeypatch.setenv("PARA_TWAP_PROGRESS_DEBUG", "1")
    # Make it return quickly.
    mon._timeout = 0.001

    try:
        mon._poll_twap_progress("req")
    except Exception:
        pass

    assert captured.get("url")
    assert "/v1/v1/" not in str(captured["url"])
