import json
from pathlib import Path
import asyncio


def test_bp_volume_history_clear_persists(monkeypatch, tmp_path):
    """Clearing a symbol should be persisted to disk and reflected by subsequent reads."""

    # Import inside test so monkeypatching module globals works reliably.
    import strategies.hedge_coordinator as hc

    history_path = tmp_path / "bp_volume_history.json"
    monkeypatch.setattr(hc, "BP_VOLUME_HISTORY_FILE", history_path)

    # Windows + Python 3.9: asyncio.Lock() requires an event loop to be set.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # No auto-load on purpose; we want deterministic snapshot.
    app = hc.CoordinatorApp(enable_volatility_monitor=False)

    # Persist a snapshot.
    app._persist_bp_volume_snapshot(
        "ETH-PERP",
        summary={"foo": 1},
        recent=[{"cycle_id": "1"}],
    )

    assert history_path.exists()

    # New app instance should load it.
    app2 = hc.CoordinatorApp(enable_volatility_monitor=False)
    snap = app2._get_bp_volume_history_snapshot("ETH-PERP")
    assert snap
    assert (snap.get("summary") or {}).get("foo") == 1

    # Clear and ensure file is updated.
    app2._bp_volume_history.pop("ETH-PERP", None)
    app2._save_bp_volume_history()

    app3 = hc.CoordinatorApp(enable_volatility_monitor=False)
    assert app3._get_bp_volume_history_snapshot("ETH-PERP") is None

    loop.close()

    # File should be valid JSON dict.
    payload = json.loads(history_path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    assert "ETH-PERP" not in payload
