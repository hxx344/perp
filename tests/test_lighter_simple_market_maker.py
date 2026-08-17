import asyncio
import aiohttp
import json
import pytest
import time
from decimal import Decimal, ROUND_DOWN
from types import SimpleNamespace
from typing import Dict, cast

from helpers.logger import TradingLogger
from exchanges.lighter import LighterClient
from trading_bot import TradingConfig
from strategies.lighter_simple_market_maker import (
    ActiveOrder,
    _parse_args,
    apply_orderbook_imbalance,
    apply_inventory_skew,
    SimpleMarketMaker,
    SimpleMakerSettings,
    clamp_maker_targets,
    compute_orderbook_imbalance,
    compute_target_prices,
    required_hedge_quantity,
    side_has_inventory_capacity,
    should_enable_side,
)


def test_compute_target_prices_respects_tick_size():
    prices = compute_target_prices(Decimal("100"), Decimal("10"), Decimal("0.5"))
    assert prices["buy"] == Decimal("99.5")
    assert prices["sell"] == Decimal("100.5")


def test_orderbook_imbalance_is_relative_and_directional():
    balanced = compute_orderbook_imbalance(
        [["100", "2"], ["99", "1"]],
        [["101", "2"], ["102", "1"]],
        depth_levels=2,
    )
    bid_heavy = compute_orderbook_imbalance(
        [["100", "8"], ["99", "4"]],
        [["101", "2"], ["102", "1"]],
        depth_levels=2,
    )
    ask_heavy = compute_orderbook_imbalance(
        [["100", "2"], ["99", "1"]],
        [["101", "8"], ["102", "4"]],
        depth_levels=2,
    )

    assert balanced == Decimal("0")
    assert bid_heavy > 0
    assert ask_heavy < 0
    assert bid_heavy == -ask_heavy


def test_orderbook_signal_shifts_local_price_by_bps_not_absolute_binance_price():
    assert apply_orderbook_imbalance(
        Decimal("100"), Decimal("0.5"), Decimal("3")
    ) == Decimal("100.015")
    assert apply_orderbook_imbalance(
        Decimal("10000"), Decimal("0.5"), Decimal("3")
    ) == Decimal("10001.500")


def test_orderbook_imbalance_rejects_empty_depth():
    with pytest.raises(ValueError, match="no usable"):
        compute_orderbook_imbalance([], [], depth_levels=10)
    with pytest.raises(ValueError, match="no usable"):
        compute_orderbook_imbalance([["100", "1"]], [], depth_levels=10)


class _DepthResponse:
    status = 200

    def __init__(self, payload):
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self):
        return self.payload


class _DepthSession:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, *, params):
        self.calls.append((url, params))
        return _DepthResponse(self.payload)


def test_binance_public_reference_reads_depth_endpoint():
    from strategies.lighter_simple_market_maker import BinancePublicReference

    session = _DepthSession(
        {
            "bids": [["100", "5"], ["99", "1"]],
            "asks": [["101", "1"], ["102", "1"]],
        }
    )
    reference = BinancePublicReference("OTHERUSDT", session)  # type: ignore[arg-type]

    signal = asyncio.run(reference.fetch_orderbook_imbalance(depth_levels=5))

    assert signal > 0
    assert session.calls == [
        (
            "https://fapi.binance.com/fapi/v1/depth",
            {"symbol": "OTHERUSDT", "limit": 5},
        )
    ]


def test_maker_applies_binance_pressure_to_lighter_midpoint_only():
    settings = SimpleMakerSettings(
        lighter_ticker="LIGHTER-PAIR",
        binance_symbol="OTHERUSDT",
        order_quantity=Decimal("0.1"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("1"),
        binance_imbalance_max_bps=Decimal("3"),
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)

    class SignalReference:
        async def fetch_orderbook_imbalance(self, depth_levels: int) -> Decimal:
            assert depth_levels == 10
            return Decimal("0.5")

    maker._binance_reference = SignalReference()  # type: ignore[assignment]
    shifted = asyncio.run(maker._resolve_reference_mid(Decimal("100")))

    assert shifted == Decimal("100.015")


def test_maker_uses_neutral_signal_when_binance_depth_is_unavailable():
    settings = SimpleMakerSettings(
        lighter_ticker="LIGHTER-PAIR",
        binance_symbol="OTHERUSDT",
        order_quantity=Decimal("0.1"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("1"),
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)

    class FailedReference:
        async def fetch_orderbook_imbalance(self, depth_levels: int) -> Decimal:
            raise RuntimeError("depth unavailable")

    maker._binance_reference = FailedReference()  # type: ignore[assignment]
    shifted = asyncio.run(maker._resolve_reference_mid(Decimal("100")))

    assert shifted == Decimal("100")


def test_should_enable_side_applies_inventory_limit():
    limit = Decimal("5")
    assert should_enable_side(Decimal("3"), limit, "buy")
    assert not should_enable_side(Decimal("6"), limit, "buy")
    assert should_enable_side(Decimal("-4"), limit, "sell")
    assert not should_enable_side(Decimal("-8"), limit, "sell")


def test_inventory_gate_reserves_full_next_quote_and_is_strict_at_cap():
    limit = Decimal("0.001")
    assert side_has_inventory_capacity(Decimal("0"), limit, "buy", Decimal("0.0002"))
    assert not side_has_inventory_capacity(Decimal("0.0008"), limit, "buy", Decimal("0.0002"))
    assert side_has_inventory_capacity(Decimal("0"), limit, "sell", Decimal("0.0002"))
    assert not side_has_inventory_capacity(Decimal("-0.0008"), limit, "sell", Decimal("0.0002"))
    assert not should_enable_side(limit, limit, "buy")
    assert not should_enable_side(-limit, limit, "sell")


def test_inventory_skew_moves_quotes_toward_flattening_inventory():
    mid = Decimal("100")
    limit = Decimal("1")
    assert apply_inventory_skew(mid, Decimal("0.5"), limit, Decimal("4")) == Decimal("99.98")
    assert apply_inventory_skew(mid, Decimal("-0.5"), limit, Decimal("4")) == Decimal("100.02")
    assert apply_inventory_skew(mid, Decimal("2"), limit, Decimal("4")) == Decimal("99.96")


def test_reference_targets_never_cross_local_lighter_book():
    targets = {"buy": Decimal("101.00"), "sell": Decimal("102.00")}
    clamped = clamp_maker_targets(targets, Decimal("99.50"), Decimal("100.50"), Decimal("0.01"))
    assert clamped == {"buy": Decimal("100.49"), "sell": Decimal("102.00")}

    crossed = clamp_maker_targets(
        {"buy": Decimal("101.00"), "sell": Decimal("101.10")},
        Decimal("99.50"),
        Decimal("100.50"),
        Decimal("0.01"),
    )
    assert crossed["buy"] < crossed["sell"]
    assert crossed["buy"] <= Decimal("100.49")
    assert crossed["sell"] >= Decimal("99.51")


def test_market_maker_defaults_are_robinhood_and_no_binance_trading():
    settings = _parse_args([])
    assert settings.lighter_ticker == "BTC"
    assert settings.binance_symbol == "BTCUSDT"
    assert settings.lighter_environment == "robinhood"
    assert settings.enable_binance_hedge is False
    assert settings.use_binance_reference is True
    assert settings.order_quantity == Decimal("0.00020")
    assert settings.lighter_leverage == 2
    assert settings.binance_depth_levels == 10
    assert settings.binance_imbalance_max_bps == Decimal("3")
    assert SimpleMakerSettings(
        lighter_ticker="BTC",
        binance_symbol="BTCUSDT",
        order_quantity=Decimal("0.00020"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("0.001"),
    ).lighter_leverage == 2


@pytest.mark.parametrize(
    "argv",
    [
        ["--cycles", "-1"],
        ["--max-hedge-quantity", "0"],
        ["--hedge-buffer", "-0.1"],
        ["--binance-imbalance-max-bps", "-1"],
        ["--order-ack-timeout-seconds", "0"],
    ],
)
def test_dangerous_numeric_options_are_rejected(argv):
    with pytest.raises(SystemExit):
        _parse_args(argv)


def test_robinhood_credential_validation_rejects_reserved_key_index():
    client = SimpleNamespace(
        account_index=7,
        api_private_keys={2: "0x" + ("1" * 64)},
    )
    with pytest.raises(ValueError, match="4..254"):
        SimpleMarketMaker._validate_robinhood_credentials(client)  # type: ignore[arg-type]


def test_robinhood_credential_validation_accepts_key_four():
    client = SimpleNamespace(
        account_index=7,
        api_private_keys={4: "0x" + ("a" * 64)},
    )
    SimpleMarketMaker._validate_robinhood_credentials(client)  # type: ignore[arg-type]


def test_robinhood_credential_validation_accepts_raw_key_without_0x():
    client = SimpleNamespace(
        account_index=7,
        api_private_keys={4: "b" * 64},
    )
    SimpleMarketMaker._validate_robinhood_credentials(client)  # type: ignore[arg-type]


def test_sync_side_uses_post_only_and_accepts_shared_order_snapshot():
    settings = SimpleMakerSettings(
        lighter_ticker="TEST",
        binance_symbol="TESTUSDT",
        order_quantity=Decimal("0.1"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("1"),
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)
    maker._lighter_config = TradingConfig(
        ticker="TEST",
        contract_id="7",
        quantity=Decimal("0.1"),
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

    class StubLighter:
        def __init__(self) -> None:
            self.calls = []

        async def get_active_orders(self, contract_id: str):
            self.calls.append(("active", contract_id))
            return []

        async def place_limit_order(self, contract_id, quantity, price, side, **kwargs):
            self.calls.append(("place", contract_id, quantity, price, side, kwargs))
            return SimpleNamespace(success=True, order_id="101")

    client = StubLighter()
    maker._lighter_client = client  # type: ignore[assignment]
    asyncio.run(
        maker._sync_side(
            "buy",
            Decimal("99.50"),
            True,
            active_orders=[],
        )
    )
    assert [call[0] for call in client.calls] == ["place"]
    assert client.calls[0][-1] == {"time_in_force": "post_only"}


def test_sync_side_waits_for_pending_quote_ack_before_replacing():
    settings = SimpleMakerSettings(
        lighter_ticker="TEST",
        binance_symbol="TESTUSDT",
        order_quantity=Decimal("0.1"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("1"),
        order_ack_timeout_seconds=5,
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)
    maker._lighter_config = SimpleNamespace(
        contract_id="7",
        tick_size=Decimal("0.01"),
    )  # type: ignore[assignment]
    maker._tracked_orders["buy"] = ActiveOrder(
        order_id="101",
        client_order_index="101",
        price=Decimal("99.50"),
        side="buy",
        created_at=time.monotonic(),
        confirmed=False,
    )

    class StubLighter:
        async def place_limit_order(self, *args, **kwargs):
            raise AssertionError("pending quote must not be duplicated")

    maker._lighter_client = StubLighter()  # type: ignore[assignment]
    asyncio.run(
        maker._sync_side(
            "buy",
            Decimal("99.50"),
            True,
            active_orders=[],
        )
    )


def test_confirmed_quote_missing_from_rest_never_triggers_replacement():
    settings = SimpleMakerSettings(
        lighter_ticker="TEST",
        binance_symbol="TESTUSDT",
        order_quantity=Decimal("0.1"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("1"),
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)
    maker._lighter_config = SimpleNamespace(
        contract_id="7",
        tick_size=Decimal("0.01"),
    )  # type: ignore[assignment]
    maker._tracked_orders["buy"] = ActiveOrder(
        order_id="9001",
        client_order_index="101",
        price=Decimal("99.50"),
        side="buy",
        created_at=time.monotonic(),
        confirmed=True,
    )

    class StubLighter:
        async def place_limit_order(self, *args, **kwargs):
            raise AssertionError("missing confirmed quote must not be replaced")

    maker._lighter_client = StubLighter()  # type: ignore[assignment]
    for _ in range(2):
        asyncio.run(
            maker._sync_side(
                "buy",
                Decimal("99.50"),
                True,
                active_orders=[],
            )
        )
    with pytest.raises(RuntimeError, match="disappeared"):
        asyncio.run(
            maker._sync_side(
                "buy",
                Decimal("99.50"),
                True,
                active_orders=[],
            )
        )


def test_private_open_ack_confirms_pending_quote():
    settings = SimpleMakerSettings(
        lighter_ticker="TEST",
        binance_symbol="TESTUSDT",
        order_quantity=Decimal("0.1"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("1"),
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)
    maker._lighter_config = SimpleNamespace(contract_id="7")  # type: ignore[assignment]
    maker._own_client_order_indices.add("101")
    maker._tracked_orders["buy"] = ActiveOrder(
        order_id="101",
        client_order_index="101",
        price=Decimal("99.50"),
        side="buy",
        created_at=time.monotonic(),
        confirmed=False,
    )

    maker._handle_lighter_order_update(
        {
            "contract_id": "7",
            "order_id": "9001",
            "client_order_index": "101",
            "status": "OPEN",
            "side": "buy",
        }
    )

    assert maker._tracked_orders["buy"].confirmed is True
    assert maker._tracked_orders["buy"].order_id == "9001"


def test_terminal_ws_update_before_send_response_is_not_resurrected():
    settings = SimpleMakerSettings(
        lighter_ticker="TEST",
        binance_symbol="TESTUSDT",
        order_quantity=Decimal("0.1"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("1"),
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)
    maker._lighter_config = SimpleNamespace(
        contract_id="7",
        tick_size=Decimal("0.01"),
    )  # type: ignore[assignment]

    class StubLighter:
        current_order_client_id = 101

        def reserve_client_order_index(self):
            return 101

        async def place_limit_order(self, *args, **kwargs):
            maker._handle_lighter_order_update(
                {
                    "contract_id": "7",
                    "order_id": "9001",
                    "client_order_index": "101",
                    "status": "REJECTED_BAD_PRICE",
                    "side": "buy",
                }
            )
            return SimpleNamespace(success=True, order_id="101")

    maker._lighter_client = StubLighter()  # type: ignore[assignment]
    asyncio.run(
        maker._sync_side(
            "buy",
            Decimal("99.50"),
            True,
            active_orders=[],
        )
    )

    assert maker._own_client_order_indices == set()
    assert "buy" not in maker._tracked_orders


def test_sync_side_keeps_recent_owned_quote_inside_emergency_threshold():
    settings = SimpleMakerSettings(
        lighter_ticker="TEST",
        binance_symbol="TESTUSDT",
        order_quantity=Decimal("0.1"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("1"),
        order_refresh_ticks=1,
        order_refresh_bps=Decimal("0"),
        min_quote_lifetime_seconds=10,
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)
    maker._lighter_config = TradingConfig(
        ticker="TEST",
        contract_id="7",
        quantity=Decimal("0.1"),
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
    client = SimpleNamespace(
        cancel_order=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("recent quote must not be cancelled")
        ),
        place_limit_order=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("recent quote must not be replaced")
        ),
    )
    maker._lighter_client = client  # type: ignore[assignment]
    maker._own_client_order_indices.add("123")
    maker._tracked_orders["buy"] = SimpleNamespace(
        order_id="99",
        client_order_index="123",
        created_at=time.monotonic(),
    )
    active = SimpleNamespace(
        order_id="99",
        client_order_index="123",
        side="buy",
        price=Decimal("100.00"),
    )

    asyncio.run(
        maker._sync_side(
            "buy",
            Decimal("100.02"),
            True,
            active_orders=[active],
        )
    )

    assert maker._tracked_orders["buy"].order_id == "99"


def test_unmanaged_active_order_blocks_duplicate_and_is_not_cancelled():
    settings = SimpleMakerSettings(
        lighter_ticker="TEST",
        binance_symbol="TESTUSDT",
        order_quantity=Decimal("0.1"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("1"),
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)
    maker._lighter_config = TradingConfig(
        ticker="TEST",
        contract_id="7",
        quantity=Decimal("0.1"),
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

    class StubLighter:
        def __init__(self) -> None:
            self.placed = []
            self.cancelled = []

        async def place_limit_order(self, *args, **kwargs):
            self.placed.append((args, kwargs))
            return SimpleNamespace(success=True, order_id="new")

        async def cancel_order(self, order_id):
            self.cancelled.append(order_id)
            return SimpleNamespace(success=True)

    client = StubLighter()
    maker._lighter_client = client  # type: ignore[assignment]
    unmanaged = SimpleNamespace(
        side="buy",
        order_id="manual-order",
        client_order_index="manual-client",
        price=Decimal("99.50"),
    )
    asyncio.run(
        maker._sync_side(
            "buy",
            Decimal("99.50"),
            True,
            active_orders=[unmanaged],
        )
    )
    assert client.placed == []
    assert client.cancelled == []


def test_cancel_all_orders_only_cancels_owned_client_indexes():
    settings = SimpleMakerSettings(
        lighter_ticker="TEST",
        binance_symbol="TESTUSDT",
        order_quantity=Decimal("0.1"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("1"),
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)
    maker._lighter_config = SimpleNamespace(contract_id="7")  # type: ignore[assignment]
    maker._own_client_order_indices.add("own-client")
    orders = [
        SimpleNamespace(side="buy", order_id="own-order", client_order_index="own-client"),
        SimpleNamespace(side="sell", order_id="manual-order", client_order_index="manual-client"),
    ]

    class StubLighter:
        def __init__(self) -> None:
            self.cancelled = []

        async def get_active_orders(self, contract_id):
            return [order for order in orders if order.order_id not in self.cancelled]

        async def cancel_order(self, order_id):
            self.cancelled.append(order_id)
            return SimpleNamespace(success=True)

    client = StubLighter()
    maker._lighter_client = client  # type: ignore[assignment]
    asyncio.run(maker._cancel_all_orders())
    assert client.cancelled == ["own-order"]


def test_cancel_all_orders_raises_when_cancellation_never_succeeds():
    settings = SimpleMakerSettings(
        lighter_ticker="TEST",
        binance_symbol="TESTUSDT",
        order_quantity=Decimal("0.1"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("1"),
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)
    maker._lighter_config = SimpleNamespace(contract_id="7")  # type: ignore[assignment]
    maker._own_client_order_indices.add("own-client")
    maker._tracked_orders["buy"] = SimpleNamespace(
        order_id="own-order",
        client_order_index="own-client",
    )
    own_order = SimpleNamespace(
        side="buy",
        order_id="own-order",
        client_order_index="own-client",
    )

    class StubLighter:
        def __init__(self) -> None:
            self.cancel_attempts = 0

        async def get_active_orders(self, contract_id):
            return [own_order]

        async def cancel_order(self, order_id):
            self.cancel_attempts += 1
            return SimpleNamespace(success=False, error_message="sequencer rejected")

    client = StubLighter()
    maker._lighter_client = client  # type: ignore[assignment]

    with pytest.raises(RuntimeError, match="Could not confirm cancellation"):
        asyncio.run(maker._cancel_all_orders(reconciliation_attempts=2))

    assert client.cancel_attempts == 2


def test_owned_client_indexes_are_persisted_for_crash_recovery(tmp_path):
    state_path = tmp_path / "maker-state.json"
    settings = SimpleMakerSettings(
        lighter_ticker="TEST",
        binance_symbol="TESTUSDT",
        order_quantity=Decimal("0.1"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("1"),
        ownership_state_path=str(state_path),
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)
    maker._lighter_client = SimpleNamespace(account_index=7)  # type: ignore[assignment]
    maker._initialize_runtime_state(7)
    maker._remember_owned_client_index(123)

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["client_order_indices"] == ["123"]

    restarted = SimpleMarketMaker(settings)
    restarted._lighter_client = SimpleNamespace(account_index=7)  # type: ignore[assignment]
    restarted._initialize_runtime_state(7)
    restarted._load_ownership_state()
    assert restarted._own_client_order_indices == {"123"}


def test_startup_empty_snapshot_does_not_forget_persisted_order():
    settings = SimpleMakerSettings(
        lighter_ticker="TEST",
        binance_symbol="TESTUSDT",
        order_quantity=Decimal("0.1"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("1"),
        order_ack_timeout_seconds=0.1,
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)
    maker._lighter_config = SimpleNamespace(contract_id="7")  # type: ignore[assignment]
    maker._own_client_order_indices.add("123")

    class StubLighter:
        async def get_active_orders(self, contract_id):
            return []

    maker._lighter_client = StubLighter()  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="not visible"):
        asyncio.run(maker._reconcile_startup_orders())
    assert maker._own_client_order_indices == {"123"}


def test_instance_lock_rejects_second_maker_for_same_account(tmp_path):
    settings = SimpleMakerSettings(
        lighter_ticker="TEST",
        binance_symbol="TESTUSDT",
        order_quantity=Decimal("0.1"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("1"),
        ownership_state_path=str(tmp_path / "maker-state.json"),
        log_to_console=False,
    )
    first = SimpleMarketMaker(settings)
    second = SimpleMarketMaker(settings)
    first._initialize_runtime_state(7)
    second._initialize_runtime_state(7)
    first._acquire_instance_lock()
    try:
        with pytest.raises(RuntimeError, match="already running"):
            second._acquire_instance_lock()
    finally:
        first._release_instance_lock()


@pytest.mark.parametrize(
    ("position", "expected_side", "expected_price"),
    [
        (Decimal("0.00020"), "sell", Decimal("99.0")),
        (Decimal("-0.00020"), "buy", Decimal("101.0")),
    ],
)
def test_emergency_flatten_uses_marketable_ioc_and_small_tolerance(
    position,
    expected_side,
    expected_price,
):
    settings = SimpleMakerSettings(
        lighter_ticker="TEST",
        binance_symbol="TESTUSDT",
        order_quantity=Decimal("0.00020"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("0.001"),
        loop_sleep_seconds=0,
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)
    maker._lighter_config = SimpleNamespace(
        contract_id="7",
        tick_size=Decimal("0.1"),
    )  # type: ignore[assignment]
    maker._lighter_inventory_base = position

    class StubLighter:
        base_amount_multiplier = 100_000
        min_base_amount = Decimal("0.00020")
        min_quote_amount = Decimal("10")

        def __init__(self):
            self.orders = []

        def _spot_size_step(self):
            return Decimal("0.00001")

        async def get_active_orders(self, contract_id):
            return []

        async def cancel_order(self, order_id):
            return SimpleNamespace(success=True)

        async def fetch_bbo_prices(self, contract_id):
            return Decimal("99.0"), Decimal("101.0")

        async def place_limit_order(self, contract_id, quantity, price, side, **kwargs):
            self.orders.append((side, quantity, price, kwargs))
            maker._lighter_inventory_base = Decimal("0")
            return SimpleNamespace(success=True, order_id="123", error_message=None)

    client = StubLighter()
    maker._lighter_client = client  # type: ignore[assignment]

    async def no_state_update(*, force=False):
        return None

    maker._update_state_guarded = no_state_update  # type: ignore[method-assign]
    asyncio.run(
        maker.emergency_flatten(
            tolerance=Decimal("0.01"),
            max_iterations=1,
            sleep_interval=0,
        )
    )

    assert client.orders == [
        (
            expected_side,
            Decimal("0.00020"),
            expected_price,
            {"time_in_force": "ioc", "reduce_only": True},
        )
    ]


def test_emergency_flatten_counts_failed_book_attempts():
    settings = SimpleMakerSettings(
        lighter_ticker="TEST",
        binance_symbol="TESTUSDT",
        order_quantity=Decimal("0.00020"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("0.001"),
        loop_sleep_seconds=0,
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)
    maker._lighter_config = SimpleNamespace(
        contract_id="7",
        tick_size=Decimal("0.1"),
    )  # type: ignore[assignment]
    maker._lighter_inventory_base = Decimal("0.00020")

    class StubLighter:
        base_amount_multiplier = 100_000

        def __init__(self):
            self.book_attempts = 0

        def _spot_size_step(self):
            return Decimal("0.00001")

        async def get_active_orders(self, contract_id):
            return []

        async def fetch_bbo_prices(self, contract_id):
            self.book_attempts += 1
            raise aiohttp.ClientConnectionError("offline")

    client = StubLighter()
    maker._lighter_client = client  # type: ignore[assignment]

    async def no_state_update(*, force=False):
        return None

    maker._update_state_guarded = no_state_update  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="max attempts"):
        asyncio.run(
            maker.emergency_flatten(
                max_iterations=2,
                sleep_interval=0,
            )
        )
    assert client.book_attempts == 2


def test_required_hedge_quantity_respects_buffer():
    threshold = Decimal("5")
    buffer = Decimal("1")
    assert required_hedge_quantity(Decimal("4"), threshold, buffer) == Decimal("0")
    assert required_hedge_quantity(Decimal("6"), threshold, buffer) == Decimal("5")
    assert required_hedge_quantity(Decimal("-8"), threshold, buffer) == Decimal("7")


def test_fraction_to_leverage_conversion():
    assert LighterClient._fraction_to_leverage(200) == 50
    assert LighterClient._fraction_to_leverage(625) == 16
    assert LighterClient._fraction_to_leverage(None) is None


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
    assert any(msg.startswith("Positions") for _lvl, msg in logs)
    summary_logs = [msg for _lvl, msg in logs if msg.startswith("PnL Summary")]
    assert summary_logs, "Expected PnL summary log entry"
    summary = summary_logs[-1]
    assert "Lighter=0.00" in summary
    assert "Binance=0.00" in summary
    assert "Combined=0.00" in summary
    volume_logs = [msg for _lvl, msg in logs if msg.startswith("Volume Summary")]
    assert volume_logs, "Expected volume summary log entry"
    volume = volume_logs[-1]
    assert "Lighter=0.00" in volume
    assert "Binance=0.00" in volume
    assert "Combined=0.00" in volume

    maker._last_metrics_time = time.time() - maker.settings.metrics_interval_seconds - 1
    logs.clear()
    maker._own_client_order_indices.add("101")
    maker._handle_lighter_order_update(
        {
            "contract_id": "MARKET",
            "order_id": "1",
            "status": "PARTIALLY_FILLED",
            "filled_size": "0.02",
            "price": "100",
            "side": "buy",
            "client_order_index": "101",
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
            "client_order_index": "101",
        }
    )
    maker._lighter_last_mark_price = Decimal("100")
    base_metrics["position_size"] = Decimal("0.05")
    base_metrics["position_value"] = Decimal("5")
    asyncio.run(maker._maybe_report_metrics(base_metrics))
    assert logs
    summary_logs = [msg for _lvl, msg in logs if msg.startswith("PnL Summary")]
    assert summary_logs, "Expected PnL summary log entry"
    summary = summary_logs[-1]
    assert "Lighter=0.00" in summary
    assert "Binance=0.00" in summary
    assert "Combined=0.00" in summary
    volume_logs = [msg for _lvl, msg in logs if msg.startswith("Volume Summary")]
    assert volume_logs
    volume = volume_logs[-1]
    assert "Lighter=5.00" in volume
    assert "Binance=0.00" in volume
    assert "Combined=5.00" in volume

    maker._last_metrics_time = time.time() - maker.settings.metrics_interval_seconds - 1
    logs.clear()
    maker._own_client_order_indices.add("102")
    maker._handle_lighter_order_update(
        {
            "contract_id": "MARKET",
            "order_id": "2",
            "status": "FILLED",
            "filled_size": "0.05",
            "price": "101",
            "side": "sell",
            "client_order_index": "102",
        }
    )
    maker._lighter_last_mark_price = Decimal("101")
    base_metrics["position_size"] = Decimal("0")
    base_metrics["position_value"] = Decimal("0")
    asyncio.run(maker._maybe_report_metrics(base_metrics))
    assert logs
    summary_logs = [msg for _lvl, msg in logs if msg.startswith("PnL Summary")]
    assert summary_logs, "Expected PnL summary log entry"
    summary = summary_logs[-1]
    assert "Lighter=0.05" in summary
    assert "Binance=0.00" in summary
    assert "Combined=0.05" in summary
    volume_logs = [msg for _lvl, msg in logs if msg.startswith("Volume Summary")]
    assert volume_logs
    volume = volume_logs[-1]
    assert "Lighter=10.05" in volume
    assert "Binance=0.00" in volume
    assert "Combined=10.05" in volume


def test_external_lighter_order_update_is_ignored():
    settings = SimpleMakerSettings(
        lighter_ticker="TEST",
        binance_symbol="TESTUSDT",
        order_quantity=Decimal("1"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("10"),
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)
    maker._lighter_config = SimpleNamespace(contract_id="MARKET")  # type: ignore[assignment]

    maker._handle_lighter_order_update(
        {
            "contract_id": "MARKET",
            "order_id": "manual-1",
            "client_order_index": "unmanaged-client",
            "status": "FILLED",
            "filled_size": "1",
            "price": "100",
            "side": "buy",
        }
    )

    assert maker._lighter_session_volume_base == Decimal("0")
    assert maker._lighter_session_volume_quote == Decimal("0")


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
    maker._binance_session_realized_pnl = Decimal("2")
    maker._binance_inventory_base = Decimal("0.1")
    maker._binance_avg_entry_price = Decimal("100")
    maker._lighter_session_volume_quote = Decimal("5")
    maker._binance_session_volume_quote = Decimal("8")

    hedger_metrics = {
        "wallet_balance": Decimal("102"),
        "available_balance": Decimal("80"),
        "position_unrealized_pnl": Decimal("0.5"),
        "position_size": Decimal("0.1"),
        "position_notional": Decimal("10.5"),
        "position_entry_price": Decimal("100"),
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
    summaries = [msg for _lvl, msg in logs if msg.startswith("PnL Summary")]
    assert summaries, "Expected PnL summary log entry"
    summary = summaries[-1]
    assert "Lighter=1.00" in summary
    assert "Binance=2.50" in summary
    assert "Combined=3.50" in summary
    volume_logs = [msg for _lvl, msg in logs if msg.startswith("Volume Summary")]
    assert volume_logs, "Expected volume summary log entry"
    volume = volume_logs[-1]
    assert "Lighter=5.00" in volume
    assert "Binance=8.00" in volume
    assert "Combined=13.00" in volume


class StubHedger:
    def __init__(self, step: Decimal = Decimal("0.001"), min_qty: Decimal = Decimal("0.001")) -> None:
        self.position = Decimal("0")
        self.orders = []
        self.step = step
        self.min_qty = min_qty

    async def prepare_market_quantity(self, quantity: Decimal) -> Decimal:
        if quantity <= 0:
            return Decimal("0")
        if self.step <= 0:
            return quantity
        scaled = (quantity / self.step).to_integral_value(rounding=ROUND_DOWN)
        normalized = (scaled * self.step).quantize(self.step, rounding=ROUND_DOWN)
        if normalized < self.min_qty:
            return Decimal("0")
        return normalized

    async def place_market_order(
        self,
        side: str,
        quantity: Decimal,
        *,
        reduce_only: bool = False,
    ) -> dict:
        qty = await self.prepare_market_quantity(quantity)
        if qty <= 0:
            raise ValueError("quantity below minimum lot size")
        if side.upper() == "BUY":
            self.position += qty
        else:
            self.position -= qty
        self.orders.append((side.upper(), qty))
        return {"executedQty": str(qty)}

    async def get_account_metrics(self) -> dict:
        return {"position_size": self.position}

    def lot_size_constraints(self) -> dict:
        return {"step_size": self.step, "min_quantity": self.min_qty}


def test_emergency_flatten_closes_enabled_binance_hedge_leg():
    settings = SimpleMakerSettings(
        lighter_ticker="TEST",
        binance_symbol="TESTUSDT",
        order_quantity=Decimal("0.001"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("0.01"),
        enable_binance_hedge=True,
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)

    class EmergencyHedger(StubHedger):
        def __init__(self):
            super().__init__()
            self.position = Decimal("0.002")
            self.reduce_flags = []

        async def place_market_order(
            self,
            side: str,
            quantity: Decimal,
            *,
            reduce_only: bool = False,
        ) -> dict:
            self.reduce_flags.append(reduce_only)
            return await super().place_market_order(
                side,
                quantity,
                reduce_only=reduce_only,
            )

    hedger = EmergencyHedger()
    maker._hedger = hedger  # type: ignore[assignment]
    asyncio.run(
        maker._flatten_binance_hedge(
            tolerance=Decimal("0.0005"),
            max_attempts=2,
        )
    )

    assert hedger.position == Decimal("0")
    assert hedger.orders == [("SELL", Decimal("0.002"))]
    assert hedger.reduce_flags == [True]


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


def test_maybe_execute_hedge_skips_when_quantity_below_lot_size():
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
    maker._hedger = StubHedger(step=Decimal("0.001"), min_qty=Decimal("0.02"))  # type: ignore[assignment]
    maker._binance_position_estimate = Decimal("0")

    asyncio.run(maker._maybe_execute_hedge(Decimal("0.015")))
    stub_hedger = cast(StubHedger, maker._hedger)
    assert stub_hedger.orders == []
    assert maker._binance_position_estimate == Decimal("0")


def test_maybe_execute_hedge_does_not_assume_request_quantity_filled():
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

    class UncertainHedger(StubHedger):
        async def place_market_order(self, side, quantity, *, reduce_only=False):
            return {}

    maker._hedger = UncertainHedger()  # type: ignore[assignment]
    maker._binance_position_estimate = Decimal("0")
    maker._binance_state_known = True

    asyncio.run(maker._maybe_execute_hedge(Decimal("0.02")))

    assert maker._binance_position_estimate == Decimal("0")
    assert maker._binance_state_known is False


def test_configure_lighter_leverage_targets_max(tmp_path):
    settings = SimpleMakerSettings(
        lighter_ticker="TEST",
        binance_symbol="TESTUSDT",
        order_quantity=Decimal("1"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("10"),
        lighter_leverage=None,
        config_path=str(tmp_path / "hot_update.json"),
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)

    captured: Dict[str, int] = {}

    async def record(leverage: int) -> None:
        captured["value"] = leverage

    maker._ensure_lighter_leverage = record  # type: ignore[assignment]
    maker._lighter_client = SimpleNamespace(get_leverage_limits=lambda: {"max": 30, "default": 20})  # type: ignore[assignment]

    logs = []
    maker.logger = cast(
        TradingLogger,
        SimpleNamespace(log=lambda message, level="INFO": logs.append((level, message))),
    )

    asyncio.run(maker._configure_lighter_leverage())
    assert captured.get("value") == 30
    assert any("Targeting Lighter max leverage 30x" in msg for _level, msg in logs)


def test_configure_lighter_leverage_handles_missing_limit(tmp_path):
    settings = SimpleMakerSettings(
        lighter_ticker="TEST",
        binance_symbol="TESTUSDT",
        order_quantity=Decimal("1"),
        base_spread_bps=Decimal("5"),
        hedge_threshold=Decimal("10"),
        lighter_leverage=None,
        config_path=str(tmp_path / "hot_update.json"),
        log_to_console=False,
    )
    maker = SimpleMarketMaker(settings)

    maker._lighter_client = SimpleNamespace(get_leverage_limits=lambda: {"default": 10})  # type: ignore[assignment]

    called = {}

    async def record(leverage: int) -> None:
        called["value"] = leverage

    maker._ensure_lighter_leverage = record  # type: ignore[assignment]

    logs = []
    maker.logger = cast(
        TradingLogger,
        SimpleNamespace(log=lambda message, level="INFO": logs.append((level, message))),
    )

    asyncio.run(maker._configure_lighter_leverage())
    assert "value" not in called
    assert any("Unable to determine Lighter max leverage" in msg for _level, msg in logs)


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
