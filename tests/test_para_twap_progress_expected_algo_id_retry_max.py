def test_expected_algo_id_retry_max_gives_up(monkeypatch):
    """If expected_algo_id never shows up in history, we should retry at most 5 times and stop.

    This prevents indefinite waiting and also prevents heuristic picking of the wrong algo.
    """

    from monitoring import para_account_monitor as mod

    class _DummyMonitor(mod.ParadexAccountMonitor):
        def __init__(self):
            # Do not call super().__init__ (would require real env/network setup).
            self.acks = []
            self._timeout = 60
            self._processed_adjustments = {}
            # Minimal shape for token/base_url discovery inside _poll_twap_progress.
            class _DummyHttpClient:
                headers = {"Authorization": "Bearer test-token"}

            class _DummyApiClient:
                api_url = "https://example.invalid/v1"
                client = _DummyHttpClient()

            class _DummyClient:
                api_client = _DummyApiClient()

            self._client = _DummyClient()

        def _acknowledge_adjustment(self, request_id, payload, progress=False):
            self.acks.append({"request_id": request_id, "payload": payload, "progress": progress})
            return True

    monitor = _DummyMonitor()

    # Two stable candidates, but NOT the expected one.
    history_resp = {
        "results": [
            {
                "id": "algo-old-1",
                "algo_type": "TWAP",
                "market": "BTC-USD-PERP",
                "side": "BUY",
                "size": "1",
                "status": "CLOSED",
                "remaining_size": "0",
                "avg_fill_price": "30000",
                "created_at": 1700000000000,
                "last_updated_at": 1700000001000,
            },
            {
                "id": "algo-old-2",
                "algo_type": "TWAP",
                "market": "BTC-USD-PERP",
                "side": "BUY",
                "size": "1",
                "status": "CLOSED",
                "remaining_size": "0",
                "avg_fill_price": "30010",
                "created_at": 1700000002000,
                "last_updated_at": 1700000003000,
            },
        ]
    }

    class _DummyPrivateClient:
        def fetch_algo_orders_history(self, **kwargs):
            return history_resp

    # Avoid real sleeping; count how many times we would sleep.
    sleeps = {"count": 0, "seconds": []}

    def _fake_sleep(seconds):
        sleeps["count"] += 1
        sleeps["seconds"].append(seconds)

    monkeypatch.setattr(mod.time, "sleep", _fake_sleep)

    # Make sure we have enough deadline to actually attempt all retries.
    monkeypatch.setattr(mod.time, "time", lambda: 0.0)

    req_id = "req-1"

    # Patch the private client inside the monitor instance by monkeypatching
    # the factory used inside `_poll_twap_progress`.
    monkeypatch.setattr(mod, "ParadexPrivateClient", lambda *args, **kwargs: _DummyPrivateClient())

    # Seed match context in processed adjustments (this is how the monitor gets inputs).
    monitor._processed_adjustments[req_id] = {
        "extra": {
            "algo_market": "BTC-USD-PERP",
            "algo_side": "BUY",
            "algo_expected_size": "1",
            "algo_id": "algo-expected",
            "algo_started_at_ms": 1700000000000,
            "twap_duration_seconds": 60,
        }
    }

    # Call the real implementation as an unbound method with our dummy instance.
    mod.ParadexAccountMonitor._poll_twap_progress(monitor, req_id)

    assert sleeps["count"] == 20
    assert sleeps["seconds"] == [5.0] * 20
    # Since we never matched, we should have sent no progress acks.
    assert monitor.acks == []
