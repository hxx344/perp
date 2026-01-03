import time


def test_process_adjustments_starts_progress_for_many(monkeypatch):
    """Ensure we schedule progress pollers for multiple TWAP adjustments in the same tick.

    Historically, low executor concurrency could delay poller start so much that it
    looked like later requests never started polling.
    """

    from monitoring import para_account_monitor as mod

    class _DummyExecutor:
        def __init__(self):
            self.submitted = []

        def submit(self, fn, *args, **kwargs):
            self.submitted.append((fn, args, kwargs))
            # Execute worker immediately for determinism.
            try:
                fn(*args, **kwargs)
            except TypeError:
                # Some submits may pass bound methods; still fine.
                fn(*args)
            return None

    class _Monitor(mod.ParadexAccountMonitor):
        def __init__(self):
            # Don't call parent init.
            self._processed_adjustments = {}
            self._adjust_executor = _DummyExecutor()

        def _fetch_agent_control(self):
            # Build 5 pending TWAP adjustments.
            pending = []
            for i in range(5):
                pending.append(
                    {
                        "request_id": f"req-{i}",
                        "action": "add",
                        "magnitude": 1,
                        "symbols": ["ETH-USD-PERP"],
                        "payload": {"order_type": "TWAP"},
                    }
                )
            return {"agent": {"pending_adjustments": pending}}

        def _execute_adjustment(self, entry):
            # Always produce a TWAP extra.
            rid = entry.get("request_id")
            extra = {
                "order_type": "TWAP",
                "algo_market": "ETH-USD-PERP",
                "algo_side": "BUY",
                "algo_expected_size": "1",
                "algo_id": f"algo-{rid}",
                "algo_started_at_ms": int(time.time() * 1000),
                "twap_duration_seconds": 60,
            }
            return "acknowledged", None, extra

        def _acknowledge_adjustment(self, request_id, status, note=None, extra=None, progress=False):
            return True

        def _poll_twap_progress(self, request_id):
            # No-op; just needs to be schedulable.
            return None

    mon = _Monitor()
    mon._process_adjustments()

    # We expect one _poll_twap_progress submit per request id.
    poll_calls = [
        (fn, args)
        for (fn, args, _kwargs) in mon._adjust_executor.submitted
        if getattr(fn, "__name__", "") == "_poll_twap_progress"
    ]
    assert len(poll_calls) == 5
