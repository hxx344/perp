import asyncio
from typing import Any, Dict

import pytest

from exchanges.lighter_custom_websocket import LighterCustomWebSocketManager


ROBINHOOD_WS_URL = "wss://api.rh.lighter.xyz/stream"


@pytest.fixture(scope="module", autouse=True)
def _module_event_loop():
    try:
        previous_loop = asyncio.get_event_loop()
    except RuntimeError:
        previous_loop = None

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()
    asyncio.set_event_loop(previous_loop)


class _Config(dict):
    def __getattr__(self, item: str) -> Any:  # pragma: no cover - helper
        return self.get(item)


def _make_manager(market_index=None, contract_id=None, lighter_ws_url=None):
    config: Dict[str, Any] = _Config(
        market_index=market_index,
        contract_id=contract_id,
        lighter_client=None,
        lighter_ws_url=lighter_ws_url,
    )
    return LighterCustomWebSocketManager(config=config, order_update_callback=None)


def test_extract_orders_matches_integer_key():
    manager = _make_manager(market_index="2048")
    orders = {2048: [{"client_order_index": 1}]}

    result = manager._extract_orders_for_market(orders)

    assert result == [{"client_order_index": 1}]


def test_extract_orders_matches_string_key():
    manager = _make_manager(market_index=2049)
    orders = {"2049": [{"client_order_index": 2}]}

    result = manager._extract_orders_for_market(orders)

    assert result == [{"client_order_index": 2}]


def test_extract_orders_falls_back_to_contract_id():
    manager = _make_manager(market_index=None, contract_id="2050")
    orders = {2050: [{"client_order_index": 3}]}

    result = manager._extract_orders_for_market(orders)

    assert result == [{"client_order_index": 3}]


def test_extract_orders_handles_list_payload():
    manager = _make_manager(market_index="2051")
    orders = [
        {"market_index": "2050", "client_order_index": 9},
        {"market_index": 2051, "client_order_index": 4},
    ]

    result = manager._extract_orders_for_market(orders)

    assert result == [{"market_index": 2051, "client_order_index": 4}]


def test_custom_websocket_manager_uses_the_resolved_url_from_config():
    manager = _make_manager(market_index=1, lighter_ws_url=ROBINHOOD_WS_URL)

    assert manager.ws_url == ROBINHOOD_WS_URL


def test_order_book_offset_may_jump_when_nonce_is_continuous():
    manager = _make_manager(market_index=1)
    manager.order_book_offset = 100
    manager.order_book_nonce = 500

    accepted = manager.validate_order_book_offset(
        108,
        begin_nonce=500,
        nonce=507,
    )

    assert accepted is True
    assert manager.order_book_offset == 108
    assert manager.order_book_nonce == 507
    assert manager.order_book_sequence_gap is False


def test_order_book_nonce_gap_is_rejected_even_when_offset_increases():
    manager = _make_manager(market_index=1)
    manager.order_book_offset = 100
    manager.order_book_nonce = 500

    accepted = manager.validate_order_book_offset(
        101,
        begin_nonce=499,
        nonce=501,
    )

    assert accepted is False
    assert manager.order_book_offset == 100
    assert manager.order_book_nonce == 500
    assert manager.order_book_sequence_gap is True


def test_order_book_offset_jump_is_not_treated_as_a_gap_without_nonce_fields():
    manager = _make_manager(market_index=1)
    manager.order_book_offset = 10

    assert manager.validate_order_book_offset(25) is True
    assert manager.order_book_offset == 25
    assert manager.order_book_sequence_gap is False


def test_reset_order_book_clears_nonce_and_offset_state():
    manager = _make_manager(market_index=1)
    manager.order_book_offset = 100
    manager.order_book_nonce = 500
    manager.order_book_sequence_gap = True
    manager.order_book["bids"][100.0] = 1.0
    manager.order_book["asks"][101.0] = 1.0
    manager.ready_event.set()

    asyncio.run(manager.reset_order_book())

    assert manager.order_book_offset is None
    assert manager.order_book_nonce is None
    assert manager.order_book_sequence_gap is False
    assert manager.order_book == {"bids": {}, "asks": {}}
    assert manager.ready_event.is_set() is False
