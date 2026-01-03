import time
from typing import Any, Dict, List, Optional


def test_twap_progress_history_matches_by_id_even_if_size_format_differs(monkeypatch):
    """If expected algo_id is present, matching must succeed even when size formatting differs.

    Regression for cases like expected_size="1.0" while history row size is "1".
    """

    # Import module to ensure we use the actual class name defined in the repo
    # (avoids hard-coding it in this test).
    from monitoring import para_account_monitor as mod

    monitor_cls = mod.ParadexAccountMonitor
    monitor = monitor_cls.__new__(monitor_cls)

    # Minimal fields used by _poll_twap_progress
    monitor._agent_id = "agent-1"
    monitor._timeout = 0.001  # keep loop short in unit tests
    monitor._ack_endpoint = "http://localhost/ack"
    monitor._auth = None
    monitor._http = None

    started_at = int(time.time() * 1000)

    request_id = "req-1"
    monitor._processed_adjustments = {
        request_id: {
            "extra": {
                "algo_market": "BTC-USD-PERP",
                "algo_side": "BUY",
                # expected_size uses a different string format than what history returns
                "algo_expected_size": "1.0",
                "algo_started_at_ms": started_at,
                "algo_id": "algo-123",
            }
        }
    }

    class _ApiClient:
        api_url = "https://example.com/v1"

        class _HttpxClient:
            headers = {"Authorization": "Bearer test"}

        client = _HttpxClient()

    class _Client:
        api_client = _ApiClient()
        account = type("A", (), {"jwt_token": "test"})()

    monitor._client = _Client()

    # Capture ACK payloads
    acks: List[Dict[str, Any]] = []

    def _ack(request_id: str, status: str, note: Any, extra: Optional[Dict[str, Any]] = None) -> bool:
        payload = {"request_id": request_id, "status": status}
        if extra:
            payload.update(extra)
        acks.append(payload)
        return True

    monitor._acknowledge_adjustment = _ack  # type: ignore

    # Stub private client history fetch to return size="1" but correct id.

    class _PrivateClient:
        def fetch_algo_orders_history(self, **kwargs):
            return {
                "results": [
                    {
                        "id": "algo-123",
                        "algo_type": "TWAP",
                        "market": "BTC-USD-PERP",
                        "side": "BUY",
                        "size": "1",
                        "remaining_size": "0.5",
                        "avg_fill_price": "42000",
                        "status": "OPEN",
                        "created_at": started_at + 1000,
                        "last_updated_at": started_at + 2000,
                    }
                ]
            }

    monkeypatch.setattr(mod, "ParadexPrivateClient", lambda *a, **k: _PrivateClient())

    # Run poller once (it will exit quickly due to tiny timeout)
    monitor._poll_twap_progress(request_id)

    # We must have emitted at least one progress ACK and it should include algo_id.
    progress_acks = [a for a in acks if a.get("progress") is True]
    assert progress_acks, "Expected at least one progress ACK"
    assert progress_acks[0].get("algo_id") == "algo-123"
