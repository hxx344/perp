import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest

import exchanges.lighter as lighter_module
from exchanges.lighter import (
    MAX_CLIENT_ORDER_INDEX,
    LighterClient,
    LighterOrderSubmissionUncertainError,
)
from lighter.exceptions import ServiceException


class _Config(dict):
    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError as exc:
            raise AttributeError(item) from exc

    def __setattr__(self, key, value):
        self[key] = value


class _FakeSignerClient:
    DEFAULT_28_DAY_ORDER_EXPIRY = -1
    DEFAULT_IOC_EXPIRY = 0
    ORDER_TYPE_LIMIT = 0
    ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
    ORDER_TIME_IN_FORCE_GOOD_TILL_TIME = 1
    ORDER_TIME_IN_FORCE_POST_ONLY = 2
    SELF_TRADE_BEHAVIOR_EXPIRE_BOTH = 17
    SELF_TRADE_EQUALITY_MASTER_ACCOUNT_INDEX = 23

    def __init__(self):
        self.calls = []

    async def create_order(self, **kwargs):
        self.calls.append(kwargs)
        return object(), {"code": 200}, None


class _FailingSignerClient(_FakeSignerClient):
    async def create_order(self, **kwargs):
        self.calls.append(kwargs)
        raise ServiceException(status=502, reason="Bad Gateway", body="Bad Gateway")


class _ClosableClient:
    def __init__(self, *, fail: bool = False):
        self.close_calls = 0
        self.fail = fail

    async def close(self):
        self.close_calls += 1
        if self.fail:
            raise RuntimeError("close failed")


class _ClosableWebSocket:
    def __init__(self, *, fail: bool = False):
        self.disconnect_calls = 0
        self.fail = fail

    async def disconnect(self):
        self.disconnect_calls += 1
        if self.fail:
            raise RuntimeError("disconnect failed")


class _RejectingSignerClient(_ClosableClient):
    instances = []

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.__class__.instances.append(self)

    def check_client(self):
        return "invalid signer"


class _SharedApiClient(_ClosableClient):
    instances = []

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.__class__.instances.append(self)


def _make_client(monkeypatch) -> LighterClient:
    monkeypatch.setenv("LIGHTER_ACCOUNT_INDEX", "7")
    monkeypatch.setenv("LIGHTER_API_PRIVATE_KEYS", '{"2":"0xtest"}')
    monkeypatch.delenv("LIGHTER_BASE_URL", raising=False)
    monkeypatch.delenv("LIGHTER_WS_URL", raising=False)
    monkeypatch.delenv("LIGHTER_CHAIN_ID", raising=False)
    monkeypatch.delenv("LIGHTER_ENVIRONMENT", raising=False)
    monkeypatch.delenv("LIGHTER_ENDPOINT_PROFILE", raising=False)
    return LighterClient(_Config(ticker="BTC", contract_id="1", market_type="perp"))


def test_client_order_index_equals_accepts_string_input():
    assert LighterClient._client_order_index_equals("123", 123)


def test_client_order_index_equals_handles_whitespace_numbers():
    assert LighterClient._client_order_index_equals(" 0042 ", "42")


def test_client_order_index_equals_rejects_non_numeric():
    assert not LighterClient._client_order_index_equals("abc", 123)


def test_client_order_indexes_are_monotonic_uint48_and_orders_use_stp(monkeypatch):
    client = _make_client(monkeypatch)
    signer = _FakeSignerClient()
    client.lighter_client = signer
    client.base_amount_multiplier = 100_000
    client.price_multiplier = 10
    monkeypatch.setattr(lighter_module.time, "time_ns", lambda: 1_700_000_000_000_000_000)

    first = asyncio.run(
        client.place_limit_order("1", Decimal("0.001"), Decimal("50000.1"), "buy")
    )
    second = asyncio.run(
        client.place_limit_order("1", Decimal("0.001"), Decimal("50000.1"), "sell")
    )

    first_id = int(first.order_id)
    second_id = int(second.order_id)
    assert 0 <= first_id < second_id <= MAX_CLIENT_ORDER_INDEX
    assert second_id == first_id + 1
    assert [call["self_trade_behavior_mode"] for call in signer.calls] == [17, 17]
    assert [call["self_trade_equality_mode"] for call in signer.calls] == [23, 23]


def test_reserved_client_order_index_can_be_persisted_before_submit(monkeypatch):
    client = _make_client(monkeypatch)
    signer = _FakeSignerClient()
    client.lighter_client = signer
    client.base_amount_multiplier = 100_000
    client.price_multiplier = 10

    reserved = client.reserve_client_order_index()
    result = asyncio.run(
        client.place_limit_order(
            "1",
            Decimal("0.001"),
            Decimal("50000.1"),
            "buy",
            client_order_index=reserved,
        )
    )

    assert result.order_id == str(reserved)
    assert signer.calls[-1]["client_order_index"] == reserved


def test_perp_order_constraints_enforce_runtime_minimums_and_size_step(monkeypatch):
    client = _make_client(monkeypatch)
    client.market_detail = SimpleNamespace()
    client.base_amount_multiplier = 100_000
    client.min_base_amount = Decimal("0.00020")
    client.min_quote_amount = Decimal("10")

    assert client._apply_trade_constraints(
        Decimal("0.00020"),
        Decimal("50000"),
    ) == Decimal("0.00020")

    with pytest.raises(ValueError, match="below the runtime market minimum"):
        client._apply_trade_constraints(Decimal("0.00019"), Decimal("50000"))

    with pytest.raises(ValueError, match="not aligned to market size step"):
        client._apply_trade_constraints(Decimal("0.000201"), Decimal("50000"))

    with pytest.raises(ValueError, match="below the runtime market minimum"):
        client._apply_trade_constraints(Decimal("0.00020"), Decimal("40000"))


def test_lighter_ioc_order_is_non_resting_reduce_only_and_tick_aligned(monkeypatch):
    client = _make_client(monkeypatch)
    signer = _FakeSignerClient()
    client.lighter_client = signer
    client.base_amount_multiplier = 100_000
    client.price_multiplier = 10
    client.config.tick_size = Decimal("0.1")

    asyncio.run(
        client.place_limit_order(
            "1",
            Decimal("0.00020"),
            Decimal("50000.1"),
            "sell",
            time_in_force="ioc",
            reduce_only=True,
        )
    )

    assert signer.calls[-1]["time_in_force"] == signer.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL
    assert signer.calls[-1]["order_expiry"] == signer.DEFAULT_IOC_EXPIRY
    assert signer.calls[-1]["reduce_only"] is True

    with pytest.raises(ValueError, match="not aligned to market tick size"):
        asyncio.run(
            client.place_limit_order(
                "1",
                Decimal("0.00020"),
                Decimal("50000.11"),
                "buy",
                time_in_force="ioc",
            )
        )


def test_lighter_502_is_uncertain_and_never_retried(monkeypatch):
    client = _make_client(monkeypatch)
    signer = _FailingSignerClient()
    client.lighter_client = signer
    client.base_amount_multiplier = 100_000
    client.price_multiplier = 10
    client.config.tick_size = Decimal("0.1")

    with pytest.raises(LighterOrderSubmissionUncertainError) as exc_info:
        asyncio.run(
            client.place_limit_order(
                "1",
                Decimal("0.00020"),
                Decimal("50000.1"),
                "buy",
                time_in_force="ioc",
            )
        )

    assert len(signer.calls) == 1
    assert isinstance(exc_info.value, ConnectionError)
    assert exc_info.value.status == 502
    assert str(exc_info.value.client_order_index) in str(exc_info.value)
    assert "refusing to resubmit" in str(exc_info.value)


def test_disconnect_closes_signer_and_api_even_when_websocket_close_fails(monkeypatch):
    client = _make_client(monkeypatch)
    signer = _ClosableClient()
    api_client = _ClosableClient()
    ws_manager = _ClosableWebSocket(fail=True)
    client.lighter_client = signer
    client.api_client = api_client
    client.ws_manager = ws_manager

    asyncio.run(client.disconnect())

    assert ws_manager.disconnect_calls == 1
    assert signer.close_calls == 1
    assert api_client.close_calls == 1
    assert client.ws_manager is None
    assert client.lighter_client is None
    assert client.api_client is None


def test_connect_failure_closes_both_created_api_sessions(monkeypatch):
    client = _make_client(monkeypatch)
    _RejectingSignerClient.instances.clear()
    _SharedApiClient.instances.clear()
    monkeypatch.setattr(lighter_module, "SignerClient", _RejectingSignerClient)
    monkeypatch.setattr(lighter_module, "ApiClient", _SharedApiClient)

    with pytest.raises(Exception, match="CheckClient error: invalid signer"):
        asyncio.run(client.connect())

    assert len(_RejectingSignerClient.instances) == 1
    assert _RejectingSignerClient.instances[0].close_calls == 1
    assert len(_SharedApiClient.instances) == 1
    assert _SharedApiClient.instances[0].close_calls == 1
    assert client.lighter_client is None
    assert client.api_client is None
