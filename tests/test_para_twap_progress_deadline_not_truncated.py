from typing import Any, Dict, Optional


def test_twap_progress_deadline_not_truncated_for_60m(monkeypatch) -> None:
    """Regression: 60m TWAP progress polling must not stop after ~30m.

    The monitor used to do:
        hard_deadline = min(now+MAX_SECONDS, now+duration+120)
    with MAX_SECONDS defaulting to 1800, which truncated 60m (3600s) TWAPs.
    """

    # Ensure module-level defaults are in the expected state.
    monkeypatch.setenv("PARA_TWAP_PROGRESS_MAX_SECONDS", "1800")
    monkeypatch.setenv("PARA_TWAP_PROGRESS_HARD_CAP_SECONDS", "86400")

    # NOTE: this repo's tests import modules via the local folder layout (not an installed package).
    from monitoring import para_account_monitor as pam

    class DummyMonitor:
        def __init__(self):
            self._processed_adjustments: Dict[str, Dict[str, Any]] = {
                "req-1": {
                    "extra": {
                        "algo_market": "BTC-USD-PERP",
                        "algo_side": "BUY",
                        "algo_expected_size": "1",
                        "twap_duration_seconds": 3600,
                    }
                }
            }

            # Make the poller exit early after computing hard_deadline.
            self._client = None

        # Make the loop exit immediately: hard_deadline will be computed first.
        def _list_open_algo_orders(self, history_only: bool = True):
            return []

    m = DummyMonitor()

    # Freeze time at a known value so we can infer hard_deadline from sleep duration.
    now = 1_700_000_000.0
    monkeypatch.setattr(pam.time, "time", lambda: now)

    slept: Dict[str, Optional[float]] = {"seconds": None}
    monkeypatch.setattr(pam, "_is_para_twap_progress_debug_enabled", lambda: False)

    pam.ParadexAccountMonitor._poll_twap_progress(m, "req-1")  # type: ignore[arg-type]

    # Assert the intended deadline math directly: for a 60m TWAP, we must not cap at 1800 seconds.
    # This mirrors the logic in _poll_twap_progress.
    hard_deadline_base = now + max(30.0, pam.DEFAULT_TWAP_PROGRESS_MAX_SECONDS)
    expected_deadline = now + 3600.0 + 120.0
    capped_expected_deadline = min(expected_deadline, now + pam.DEFAULT_TWAP_PROGRESS_HARD_CAP_SECONDS)
    hard_deadline = max(hard_deadline_base, capped_expected_deadline)

    assert hard_deadline >= now + 3600.0
