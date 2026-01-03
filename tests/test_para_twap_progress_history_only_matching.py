from __future__ import annotations

from typing import Any, Dict


def test_twap_progress_history_only_picks_closed_and_sends_progress(monkeypatch):
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

    started_at = 1767428508123
    mon._processed_adjustments = {
        "req": {
            "extra": {
                "algo_market": "ETH-USD-PERP",
                "algo_side": "BUY",
                "algo_expected_size": "0.1",
                "algo_started_at_ms": started_at,
                "twap_duration_seconds": 30,
            }
        }
    }

    # Force history-only.
    monkeypatch.setenv("PARA_TWAP_PROGRESS_HISTORY_ONLY", "1")
    monkeypatch.setenv("PARA_TWAP_PROGRESS_DEBUG", "1")

    # Mock requests.get used by ParadexPrivateClient.
    import requests

    captured: Dict[str, Any] = {"acks": []}

    def fake_get(url: str, *args: Any, **kwargs: Any):
        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "results": [
                        {
                            "id": "x1",
                            "algo_type": "TWAP",
                            "market": "ETH-USD-PERP",
                            "side": "BUY",
                            "status": "CLOSED",
                            "size": "0.1",
                            "remaining_size": "0",
                            "avg_fill_price": "3000.1",
                            "created_at": started_at + 1000,
                            "last_updated_at": started_at + 2000,
                        }
                    ]
                }

        return Resp()

    monkeypatch.setattr(requests, "get", fake_get)

    # Capture ACK payload instead of performing HTTP.
    def fake_ack(request_id: str, status: str, note: Any, extra: Any = None) -> bool:
        captured["acks"].append({"request_id": request_id, "status": status, "extra": extra})
        return True

    mon._acknowledge_adjustment = fake_ack  # type: ignore[assignment]

    mon._poll_twap_progress("req")
    assert captured["acks"], "expected at least one progress ack"
    payload = captured["acks"][0]["extra"]
    assert payload["progress"] is True
    assert payload["avg_price"] == "3000.1"
    assert "filled_qty" in payload
