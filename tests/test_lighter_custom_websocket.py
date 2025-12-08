from typing import Any, Dict

from exchanges.lighter_custom_websocket import LighterCustomWebSocketManager


class _Config(dict):
    def __getattr__(self, item: str) -> Any:  # pragma: no cover - helper
        return self.get(item)


def _make_manager(market_index=None, contract_id=None):
    config: Dict[str, Any] = _Config(
        market_index=market_index,
        contract_id=contract_id,
        lighter_client=None,
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
