import json
from decimal import Decimal

import aiohttp
import pytest

from strategies.lighter_simple_market_maker import SimpleMarketMaker, SimpleMakerSettings, _parse_args
from strategies.maker_dashboard import MarketMakerDashboard


def _settings(**overrides):
    values = {
        "lighter_ticker": "BTC",
        "binance_symbol": "BTCUSDT",
        "order_quantity": Decimal("0.0002"),
        "base_spread_bps": Decimal("2"),
        "hedge_threshold": Decimal("0.001"),
        "dashboard_port": 0,
    }
    values.update(overrides)
    return SimpleMakerSettings(**values)


def test_dashboard_snapshot_is_json_safe_and_contains_operational_metrics():
    maker = SimpleMarketMaker(_settings())
    maker._running = True
    maker._inventory_state_known = True
    maker._last_best_bid = maker._to_decimal("64000.0")
    maker._last_best_ask = maker._to_decimal("64000.1")
    maker._last_lighter_mid = maker._to_decimal("64000.05")
    maker._last_target_prices = {"buy": maker._to_decimal("64000.0"), "sell": maker._to_decimal("64000.1")}
    snapshot = maker.export_dashboard_snapshot()

    encoded = json.dumps(snapshot, ensure_ascii=False)
    assert json.loads(encoded)["market"]["best_bid"] == "64000.000000"
    assert set(("orders", "inventory", "signals", "performance")).issubset(snapshot)
    assert snapshot["orders"]["buy"] == []
    assert snapshot["quote_mode"] == "post_only"
    assert "LIGHTER_API_PRIVATE_KEYS" not in encoded


def test_dashboard_cli_defaults_and_disable_flag():
    settings = _parse_args(["--no-dashboard", "--dashboard-host", "0.0.0.0", "--dashboard-port", "9001"])
    assert settings.dashboard_enabled is False
    assert settings.dashboard_host == "0.0.0.0"
    assert settings.dashboard_port == 9001


def test_dashboard_rejects_public_bind_when_enabled():
    with pytest.raises(SystemExit):
        _parse_args(["--dashboard-host", "0.0.0.0"])
    with pytest.raises(SystemExit):
        _parse_args(["--dashboard-host", "localhost"])


@pytest.mark.asyncio
async def test_dashboard_server_rejects_non_loopback_bind():
    dashboard = MarketMakerDashboard(lambda: {"ok": True}, host="0.0.0.0", port=0)
    with pytest.raises(RuntimeError, match="authenticated HTTPS reverse proxy"):
        await dashboard.start()


@pytest.mark.asyncio
async def test_dashboard_serves_snapshot_and_health():
    dashboard = MarketMakerDashboard(lambda: {"ok": True, "value": "42"}, port=0)
    await dashboard.start()
    try:
        assert dashboard.bound_port is not None
        expected_headers = {
            "Cache-Control": "no-store, max-age=0",
            "Pragma": "no-cache",
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "Referrer-Policy": "no-referrer",
            "Cross-Origin-Resource-Policy": "same-origin",
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{dashboard.bound_port}/api/snapshot") as response:
                assert response.status == 200
                assert await response.json() == {"ok": True, "value": "42"}
                for name, value in expected_headers.items():
                    assert response.headers[name] == value
                assert "default-src 'none'" in response.headers["Content-Security-Policy"]
            async with session.get(f"http://127.0.0.1:{dashboard.bound_port}/api/healthz") as response:
                assert response.status == 200
                assert (await response.json())["ok"] is True
            async with session.get(f"http://127.0.0.1:{dashboard.bound_port}/") as response:
                assert response.status == 200
                assert "Robinhood Lighter" in await response.text()
            async with session.get(f"http://127.0.0.1:{dashboard.bound_port}/not-found") as response:
                assert response.status == 404
                for name, value in expected_headers.items():
                    assert response.headers[name] == value
    finally:
        await dashboard.stop()
    assert dashboard.running is False


@pytest.mark.asyncio
async def test_dashboard_error_response_is_not_cached():
    dashboard = MarketMakerDashboard(lambda: {"value": object()}, port=0)
    await dashboard.start()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{dashboard.bound_port}/api/snapshot") as response:
                assert response.status == 503
                payload = await response.json()
                assert payload == {"ok": False, "error": "dashboard snapshot unavailable"}
                assert response.headers["Cache-Control"] == "no-store, max-age=0"
                assert response.headers["X-Content-Type-Options"] == "nosniff"
    finally:
        await dashboard.stop()
