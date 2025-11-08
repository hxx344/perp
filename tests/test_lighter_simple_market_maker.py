from decimal import Decimal

from strategies.lighter_simple_market_maker import (
    SimpleMarketMaker,
    SimpleMakerSettings,
    compute_target_prices,
    required_hedge_quantity,
    should_enable_side,
)


def test_compute_target_prices_respects_tick_size():
    prices = compute_target_prices(Decimal("100"), Decimal("10"), Decimal("0.5"))
    assert prices["buy"] == Decimal("99.5")
    assert prices["sell"] == Decimal("100.5")


def test_should_enable_side_applies_inventory_limit():
    limit = Decimal("5")
    assert should_enable_side(Decimal("3"), limit, "buy")
    assert not should_enable_side(Decimal("6"), limit, "buy")
    assert should_enable_side(Decimal("-4"), limit, "sell")
    assert not should_enable_side(Decimal("-8"), limit, "sell")


def test_required_hedge_quantity_respects_buffer():
    threshold = Decimal("5")
    buffer = Decimal("1")
    assert required_hedge_quantity(Decimal("4"), threshold, buffer) == Decimal("0")
    assert required_hedge_quantity(Decimal("6"), threshold, buffer) == Decimal("5")
    assert required_hedge_quantity(Decimal("-8"), threshold, buffer) == Decimal("7")


def test_resolve_spread_scale_uses_depth_multiplier(tmp_path):
    settings = SimpleMakerSettings(
        lighter_ticker="TEST",
        binance_symbol="TESTUSDT",
        order_quantity=Decimal("1"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("10"),
        config_path=str(tmp_path / "hot_update.json"),
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)
    result = maker._resolve_spread_scale({"aster_maker_depth_level": 20})
    assert result == Decimal("10.0000")
    result = maker._resolve_spread_scale({})
    assert result == Decimal("5.0000")
