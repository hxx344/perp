from __future__ import annotations

from decimal import Decimal


def _make_monitor():
    from monitoring.para_account_monitor import ParadexAccountMonitor

    mon = ParadexAccountMonitor.__new__(ParadexAccountMonitor)
    # Minimal stubs for methods used by _execute_adjustment.
    mon._resolve_symbol = lambda symbols: (symbols[0] if isinstance(symbols, list) else symbols) or "BTC-USD-PERP"  # type: ignore[attr-defined]
    mon._lookup_net_position = lambda symbol: Decimal("0")  # type: ignore[attr-defined]
    mon._update_cached_position = lambda symbol, new_net: None  # type: ignore[attr-defined]
    mon._place_market_order = lambda symbol, side, qty: {"id": "order-1"}  # type: ignore[attr-defined]
    mon._place_twap_order = lambda symbol, side, qty, dur: {"id": "algo-1"}  # type: ignore[attr-defined]
    return mon


def test_reduce_does_not_require_position_and_does_not_clamp() -> None:
    mon = _make_monitor()

    entry = {
        "action": "reduce",
        "magnitude": "5",
        "symbols": ["BTC-USD-PERP"],
        "payload": {"order_mode": "market"},
    }

    # Should not raise even if net position is flat.
    status, note, extra = mon._execute_adjustment(entry)
    assert status == "succeeded"
    # Default reduce side when flat is SELL.
    assert "SELL 5 BTC-USD-PERP" in note
    # Reduce should not clamp magnitude based on exposure.
    assert "clamped" not in note


def test_reduce_side_override_is_respected() -> None:
    mon = _make_monitor()

    entry = {
        "action": "reduce",
        "magnitude": "1",
        "symbols": ["BTC-USD-PERP"],
        "reduce_side": "buy",
        "payload": {"order_mode": "market"},
    }

    status, note, _extra = mon._execute_adjustment(entry)
    assert status == "succeeded"
    assert "BUY 1 BTC-USD-PERP" in note
