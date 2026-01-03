from __future__ import annotations

from typing import Any, Dict, List


def test_history_only_follows_next_cursor_to_find_algo_id(monkeypatch):
    # NOTE: pagination defaults are read at module import time.
    monkeypatch.setenv("PARA_TWAP_PROGRESS_HISTORY_ONLY", "1")
    monkeypatch.setenv("PARA_TWAP_PROGRESS_DEBUG", "1")
    monkeypatch.setenv("PARA_TWAP_HISTORY_MAX_PAGES", "3")
    monkeypatch.setenv("PARA_TWAP_HISTORY_PAGE_LIMIT", "2")

    import importlib
    import monitoring.para_account_monitor as pam
    importlib.reload(pam)
    ParadexAccountMonitor = pam.ParadexAccountMonitor

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

    target_id = "target-algo"
    started_at = 1700000000000

    mon._processed_adjustments = {
        "req": {
            "extra": {
                "order_type": "TWAP",
                "algo_id": target_id,
                "algo_market": "ETH-USD-PERP",
                "algo_side": "BUY",
                "algo_expected_size": "0.1",
                "algo_started_at_ms": started_at,
                "twap_duration_seconds": 30,
            }
        }
    }

    import requests

    calls: List[Dict[str, Any]] = []

    def fake_get(url: str, *args: Any, **kwargs: Any):
        params = kwargs.get("params") or {}
        calls.append({"url": url, "params": dict(params)})
        cursor = params.get("cursor")

        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                if not cursor:
                    return {
                        "next": "CURSOR_1",
                        "results": [
                            {
                                "id": "old",
                                "algo_type": "TWAP",
                                "market": "ETH-USD-PERP",
                                "side": "BUY",
                                "status": "CLOSED",
                                "size": "0.1",
                                "remaining_size": "0.1",
                                "avg_fill_price": None,
                                "created_at": started_at - 1000,
                                "last_updated_at": started_at - 500,
                            }
                        ],
                    }
                return {
                    "next": None,
                    "results": [
                        {
                            "id": target_id,
                            "algo_type": "TWAP",
                            "market": "ETH-USD-PERP",
                            "side": "BUY",
                            "status": "OPEN",
                            "size": "0.1",
                            "remaining_size": "0.0",
                            "avg_fill_price": "3100.0",
                            "created_at": started_at + 1000,
                            "last_updated_at": started_at + 1500,
                        }
                    ],
                }

        return Resp()

    monkeypatch.setattr(requests, "get", fake_get)

    captured: List[Dict[str, Any]] = []

    def fake_ack(request_id: str, status: str, note: Any, extra: Any = None) -> bool:
        captured.append({"request_id": request_id, "status": status, "extra": extra})
        return True

    mon._acknowledge_adjustment = fake_ack  # type: ignore[assignment]

    mon._poll_twap_progress("req")

    # Ensure we paged at least twice (cursor used).
    assert any(call["params"].get("cursor") == "CURSOR_1" for call in calls)
    # Ensure we sent a progress ACK based on the second page.
    assert captured
    payload = captured[0]["extra"]
    assert payload["progress"] is True
    assert payload["algo_id"] == target_id


def test_history_only_max_pages_one_does_not_page(monkeypatch):
    """When max_pages=1, we should only request the first page even if next cursor exists."""

    # Keep debug off so the "empty rows" envelope dump doesn't trigger a second request.
    monkeypatch.setenv("PARA_TWAP_PROGRESS_HISTORY_ONLY", "1")
    monkeypatch.setenv("PARA_TWAP_PROGRESS_DEBUG", "")
    monkeypatch.setenv("PARA_TWAP_HISTORY_MAX_PAGES", "1")
    monkeypatch.setenv("PARA_TWAP_HISTORY_PAGE_LIMIT", "2")

    import importlib
    import monitoring.para_account_monitor as pam
    importlib.reload(pam)
    ParadexAccountMonitor = pam.ParadexAccountMonitor

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

    target_id = "target-algo"
    started_at = 1700000000000
    mon._processed_adjustments = {
        "req": {
            "extra": {
                "order_type": "TWAP",
                "algo_id": target_id,
                "algo_market": "ETH-USD-PERP",
                "algo_side": "BUY",
                "algo_expected_size": "0.1",
                "algo_started_at_ms": started_at,
            }
        }
    }

    import requests

    calls = []

    def fake_get(url: str, *args: Any, **kwargs: Any):
        params = kwargs.get("params") or {}
        calls.append(dict(params))

        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                # next exists but we must not follow it.
                return {"next": "CURSOR_1", "results": []}

        return Resp()

    monkeypatch.setattr(requests, "get", fake_get)

    # Capture ACK (shouldn't send because we never find the algo).
    captured = []

    def fake_ack(request_id: str, status: str, note: Any, extra: Any = None) -> bool:
        captured.append(extra)
        return True

    mon._acknowledge_adjustment = fake_ack  # type: ignore[assignment]

    mon._poll_twap_progress("req")
    # No cursor paging should happen when max_pages=1.
    assert all(call.get("cursor") is None for call in calls)
    assert not captured
