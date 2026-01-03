import time


class _HttpError(Exception):
    pass


def test_twap_progress_unauthorized_sends_failed_ack(monkeypatch):
    from monitoring import para_account_monitor as mod

    class _DummyMonitor(mod.ParadexAccountMonitor):
        def __init__(self):
            self._timeout = 60
            self._processed_adjustments = {}
            self.acks = []

            class _DummyHttpClient:
                headers = {"Authorization": "Bearer test-token"}

            class _DummyApiClient:
                api_url = "https://example.invalid/v1"
                client = _DummyHttpClient()

            class _DummyClient:
                api_client = _DummyApiClient()

            self._client = _DummyClient()

        def _acknowledge_adjustment(self, request_id, status, note=None, extra=None, progress=False):
            self.acks.append(
                {
                    "request_id": request_id,
                    "status": status,
                    "note": note,
                    "extra": extra,
                    "progress": progress,
                }
            )
            return True

    class _UnauthorizedPrivateClient:
        def fetch_algo_orders_history(self, **kwargs):
            raise _HttpError("401 Client Error: Unauthorized")

    # Avoid real sleeps.
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    # Keep time moving enough that we can loop several times.
    ticks = {"t": 0.0}

    def _fake_time():
        ticks["t"] += 1.0
        return ticks["t"]

    monkeypatch.setattr(mod.time, "time", _fake_time)

    # Force our private client to be used.
    monkeypatch.setattr(mod, "ParadexPrivateClient", lambda *args, **kwargs: _UnauthorizedPrivateClient())

    mon = _DummyMonitor()
    req_id = "req-unauth"
    mon._processed_adjustments[req_id] = {
        "extra": {
            "algo_market": "ETH-USD-PERP",
            "algo_side": "BUY",
            "algo_expected_size": "1",
            "algo_id": "algo-expected",
            "algo_started_at_ms": 1700000000000,
            "twap_duration_seconds": 60,
        }
    }

    mod.ParadexAccountMonitor._poll_twap_progress(mon, req_id)

    assert mon.acks, "Should send a final failed ack when unauthorized persists"
    final = mon.acks[-1]
    assert final["status"] == "failed"
    assert "unauthorized" in (final["note"] or "").lower()
