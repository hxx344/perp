import asyncio
import aiohttp
import time
from decimal import Decimal
from types import SimpleNamespace
from typing import cast

from helpers.logger import TradingLogger
from trading_bot import TradingConfig
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


def test_format_decimal_rounds_half_up():
    result = SimpleMarketMaker._format_decimal(Decimal("1.23456"), precision=3)
    assert result == "1.235"
    result = SimpleMarketMaker._format_decimal(Decimal("1.23456"), precision=2)
    assert result == "1.23"
    result = SimpleMarketMaker._format_decimal(Decimal("1.235"), precision=2)
    assert result == "1.24"


def test_maybe_report_metrics_tracks_session_volume(tmp_path):
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
    maker._hedger = None  # skip Binance metrics
    maker._lighter_config = TradingConfig(
        ticker="TEST",
        contract_id="MARKET",
        quantity=Decimal("1"),
        take_profit=Decimal("0"),
        tick_size=Decimal("0.01"),
        direction="buy",
        max_orders=1,
        wait_time=1,
        exchange="lighter",
        grid_step=Decimal("0"),
        stop_price=Decimal("0"),
        pause_price=Decimal("0"),
        boost_mode=False,
    )

    logs = []
    maker.logger = cast(
        TradingLogger,
        SimpleNamespace(log=lambda message, level="INFO": logs.append((level, message))),
    )

    maker._last_metrics_time = time.time() - maker.settings.metrics_interval_seconds - 1

    base_metrics = {
        "position_size": Decimal("0"),
        "position_value": Decimal("0"),
        "unrealized_pnl": Decimal("2"),
        "realized_pnl": Decimal("1"),
        "available_balance": Decimal("50"),
        "daily_volume": Decimal("4"),
        "weekly_volume": Decimal("0"),
        "monthly_volume": Decimal("0"),
    }

    maker._lighter_last_mark_price = Decimal("100")
    asyncio.run(maker._maybe_report_metrics(base_metrics))
    assert logs, "Expected monitoring log output"
    assert "sessionVol=0.000000" in logs[0][1]
    assert "sessionReal=0.00" in logs[0][1]
    assert "sessionPnl=0.00" in logs[0][1]

    maker._last_metrics_time = time.time() - maker.settings.metrics_interval_seconds - 1
    logs.clear()
    maker._handle_lighter_order_update(
        {
            "contract_id": "MARKET",
            "order_id": "1",
            "status": "PARTIALLY_FILLED",
            "filled_size": "0.02",
            "price": "100",
            "side": "buy",
        }
    )
    maker._handle_lighter_order_update(
        {
            "contract_id": "MARKET",
            "order_id": "1",
            "status": "FILLED",
            "filled_size": "0.05",
            "price": "100",
            "side": "buy",
        }
    )
    maker._lighter_last_mark_price = Decimal("100")
    base_metrics["position_size"] = Decimal("0.05")
    base_metrics["position_value"] = Decimal("5")
    asyncio.run(maker._maybe_report_metrics(base_metrics))
    assert logs
    assert "sessionVol=5.000000" in logs[0][1]
    assert "sessionReal=0.00" in logs[0][1]
    assert "sessionUnreal=0.00" in logs[0][1]

    maker._last_metrics_time = time.time() - maker.settings.metrics_interval_seconds - 1
    logs.clear()
    maker._handle_lighter_order_update(
        {
            "contract_id": "MARKET",
            "order_id": "2",
            "status": "FILLED",
            "filled_size": "0.05",
            "price": "101",
            "side": "sell",
        }
    )
    maker._lighter_last_mark_price = Decimal("101")
    base_metrics["position_size"] = Decimal("0")
    base_metrics["position_value"] = Decimal("0")
    asyncio.run(maker._maybe_report_metrics(base_metrics))
    assert logs
    assert "sessionReal=0.05" in logs[0][1]
    assert "sessionUnreal=0.00" in logs[0][1]
    assert "sessionPnl=0.05" in logs[0][1]


def test_apply_fill_to_session_pnl_tracks_realized_and_inventory(tmp_path):
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

    maker._apply_fill_to_session_pnl(Decimal("0.5"), Decimal("100"))
    assert maker._lighter_inventory_base == Decimal("0.5")
    assert maker._lighter_avg_entry_price == Decimal("100")
    assert maker._lighter_session_realized_pnl == Decimal("0")

    maker._apply_fill_to_session_pnl(Decimal("-0.2"), Decimal("101"))
    assert maker._lighter_inventory_base == Decimal("0.3")
    assert maker._lighter_avg_entry_price == Decimal("100")
    assert maker._lighter_session_realized_pnl == Decimal("0.2")

    maker._apply_fill_to_session_pnl(Decimal("-0.6"), Decimal("99"))
    # Remaining 0.3 closes, new short 0.3 opens at 99
    assert maker._lighter_inventory_base == Decimal("-0.3")
    assert maker._lighter_avg_entry_price == Decimal("99")
    expected_realized = Decimal("0.2") + (Decimal("99") - Decimal("100")) * Decimal("0.3")
    assert maker._lighter_session_realized_pnl == expected_realized


def test_maybe_report_metrics_combines_binance_pnl(tmp_path):
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
    maker._lighter_config = TradingConfig(
        ticker="TEST",
        contract_id="MARKET",
        quantity=Decimal("1"),
        take_profit=Decimal("0"),
        tick_size=Decimal("0.01"),
        direction="buy",
        max_orders=1,
        wait_time=1,
        exchange="lighter",
        grid_step=Decimal("0"),
        stop_price=Decimal("0"),
        pause_price=Decimal("0"),
        boost_mode=False,
    )

    logs = []
    maker.logger = cast(
        TradingLogger,
        SimpleNamespace(log=lambda message, level="INFO": logs.append((level, message))),
    )

    maker._last_metrics_time = time.time() - maker.settings.metrics_interval_seconds - 1
    maker._lighter_session_realized_pnl = Decimal("1")
    maker._lighter_inventory_base = Decimal("0")
    maker._lighter_last_mark_price = Decimal("0")
    maker._lighter_avg_entry_price = Decimal("0")

    hedger_metrics = {
        "wallet_balance": Decimal("102"),
        "available_balance": Decimal("80"),
        "position_unrealized_pnl": Decimal("0.5"),
        "position_size": Decimal("0"),
        "position_notional": Decimal("0"),
    }

    class HedgerStub:
        async def get_account_metrics(self) -> dict:
            return dict(hedger_metrics)

    maker._hedger = HedgerStub()  # type: ignore[assignment]
    maker._binance_initial_wallet_balance = Decimal("100")

    base_metrics = {
        "position_size": Decimal("0"),
        "position_value": Decimal("0"),
        "unrealized_pnl": Decimal("0"),
        "realized_pnl": Decimal("1"),
        "available_balance": Decimal("50"),
        "daily_volume": Decimal("4"),
        "weekly_volume": Decimal("0"),
        "monthly_volume": Decimal("0"),
    }

    asyncio.run(maker._maybe_report_metrics(base_metrics))
    combined_logs = [msg for _lvl, msg in logs if msg.startswith("Combined")]
    assert combined_logs, "Expected combined metrics log entry"
    assert "sessionReal=3.00" in combined_logs[-1]
    assert "sessionUnreal=0.50" in combined_logs[-1]
    assert "sessionPnl=3.50" in combined_logs[-1]


class StubHedger:
    def __init__(self) -> None:
        self.position = Decimal("0")
        self.orders = []

    async def place_market_order(self, side: str, quantity: Decimal) -> dict:
        qty = Decimal(str(quantity))
        if side.upper() == "BUY":
            self.position += qty
        else:
            self.position -= qty
        self.orders.append((side.upper(), qty))
        return {"executedQty": str(qty)}

    async def get_account_metrics(self) -> dict:
        return {"position_size": self.position}


def test_maybe_execute_hedge_respects_existing_binance_position():
    settings = SimpleMakerSettings(
        lighter_ticker="TEST",
        binance_symbol="TESTUSDT",
        order_quantity=Decimal("1"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("0.01"),
        hedge_buffer=Decimal("0"),
        config_path="configs/hot_update.json",
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)
    maker.logger = cast(TradingLogger, SimpleNamespace(log=lambda *args, **kwargs: None))
    maker._hedger = StubHedger()  # type: ignore[assignment]
    maker._binance_position_estimate = Decimal("0")

    asyncio.run(maker._maybe_execute_hedge(Decimal("-0.012")))
    assert maker._binance_position_estimate == Decimal("0.012")

    stub_hedger = cast(StubHedger, maker._hedger)
    assert stub_hedger.orders == [("BUY", Decimal("0.012"))]
    stub_hedger.orders.clear()

    asyncio.run(maker._maybe_execute_hedge(Decimal("-0.012")))
    assert stub_hedger.orders == []

    asyncio.run(maker._maybe_execute_hedge(Decimal("0")))
    assert stub_hedger.orders == [("SELL", Decimal("0.012"))]
    assert maker._binance_position_estimate == Decimal("0")


class StubRateLimitError(Exception):
    def __init__(self, status: int = 429, message: str = "Too Many Requests"):
        super().__init__(message)
        self.status = status


def test_handle_iteration_failure_rate_limit_backoff(tmp_path):
    settings = SimpleMakerSettings(
        lighter_ticker="TEST",
        binance_symbol="TESTUSDT",
        order_quantity=Decimal("1"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("10"),
        config_path=str(tmp_path / "hot_update.json"),
        loop_sleep_seconds=1.5,
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)
    maker.logger = cast(TradingLogger, SimpleNamespace(log=lambda *args, **kwargs: None))

    initial_backoff = maker._rate_limit_backoff_seconds
    assert initial_backoff == max(settings.loop_sleep_seconds, 1.0)

    delay = maker._handle_iteration_failure(StubRateLimitError())
    assert delay == initial_backoff
    assert maker._rate_limit_backoff_seconds == min(initial_backoff * 2, maker._max_rate_limit_backoff_seconds)

    maker._reset_rate_limit_backoff()
    assert maker._rate_limit_backoff_seconds == maker._base_rate_limit_backoff_seconds

    delay = maker._handle_iteration_failure(aiohttp.ClientError("network"))
    assert delay is not None
