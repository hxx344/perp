import asyncio
import time
from decimal import Decimal
from typing import Any, cast


def test_bp_bbo_preview_ws_and_http_sources(monkeypatch):
    """Ensure handler returns `source` and `age_ms`, preferring WS when available.

    This test calls the async handler directly (no aiohttp server spin-up).
    """

    from strategies.hedge_coordinator import CoordinatorApp
    import strategies.hedge_coordinator as hc

    # The handler guards on these imports; for unit tests we just stub them.
    monkeypatch.setattr(hc, "BackpackClient", object(), raising=False)
    monkeypatch.setattr(hc, "TradingConfig", object(), raising=False)

    app = CoordinatorApp()

    # Capture aiohttp json response payload.
    captured = {}

    def fake_json_response(payload, status=200):
        captured["payload"] = payload
        captured["status"] = status
        return payload

    monkeypatch.setattr("aiohttp.web.json_response", fake_json_response)

    class DummyRelUrl:
        def __init__(self, query):
            self.query = query

    class DummyReq:
        def __init__(self, query):
            self.rel_url = DummyRelUrl(query)

    # ---- WS hit path
    now = time.time()

    class DummyWS:
        async def get_quote(self, symbol):
            assert symbol == "ETH-PERP"
            return (Decimal("100"), Decimal("2"), Decimal("101"), Decimal("3"))

        def last_update_ts(self, symbol):
            assert symbol == "ETH-PERP"
            return now - 0.25

    cast(Any, app)._bp_bbo_ws = DummyWS()

    asyncio.run(app.handle_bp_bbo_preview(cast(Any, DummyReq({"symbol": "ETH-PERP", "qty": "1"}))))

    payload = captured["payload"]
    assert payload["ok"] is True
    assert payload["source"] == "ws"
    # age_ms should be a number when WS timestamp available
    assert payload["age_ms"] is None or payload["age_ms"] >= 0
    assert payload["bid1"] == "100"
    assert payload["ask1"] == "101"

    # ---- HTTP fallback path
    class DummyClient:
        async def fetch_bbo_with_qty(self, symbol):
            assert symbol == "ETH-PERP"
            return (Decimal("200"), Decimal("4"), Decimal("201"), Decimal("5"))

    def fake_make_backpack_client(symbol_hint="ETH-PERP"):
        return DummyClient()

    # WS returns no quote -> should fallback to HTTP
    class DummyWSNoHit:
        async def get_quote(self, symbol):
            return None

        def last_update_ts(self, symbol):
            return None

    cast(Any, app)._bp_bbo_ws = DummyWSNoHit()
    monkeypatch.setattr("strategies.hedge_coordinator._make_backpack_client", fake_make_backpack_client)

    asyncio.run(app.handle_bp_bbo_preview(cast(Any, DummyReq({"symbol": "ETH-PERP", "qty": "1"}))))

    payload = captured["payload"]
    assert payload["ok"] is True
    assert payload["source"] == "http"
    assert payload["age_ms"] is None
    assert payload["bid1"] == "200"
    assert payload["ask1"] == "201"
