import asyncio
import os
from decimal import Decimal

import pytest

from exchanges.lighter import LighterClient


class DummyPosition:
    def __init__(self, market_id, position, sign=None, side=None):
        self.market_id = market_id
        self.position = position
        self.sign = sign
        self.side = side


class DummyConfig(dict):
    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError as exc:
            raise AttributeError(item) from exc

    def __setattr__(self, key, value):
        self[key] = value


@pytest.fixture(autouse=True)
def _set_envvars(monkeypatch):
    monkeypatch.setenv("API_KEY_PRIVATE_KEY", "dummy")
    monkeypatch.setenv("LIGHTER_ACCOUNT_INDEX", "0")
    monkeypatch.setenv("LIGHTER_API_KEY_INDEX", "0")


def _make_client(contract_id):
    config = DummyConfig(ticker="TEST-USD", contract_id=contract_id)
    return LighterClient(config)


def test_get_account_positions_uses_sign_field(monkeypatch):
    client = _make_client(contract_id=7)

    async def fake_fetch():
        return [DummyPosition(market_id=7, position="1.5", sign=-1)]

    monkeypatch.setattr(client, "_fetch_positions_with_retry", fake_fetch)

    quantity = asyncio.run(client.get_account_positions())

    assert quantity == Decimal("-1.5")


def test_get_account_positions_falls_back_to_side(monkeypatch):
    client = _make_client(contract_id=9)

    async def fake_fetch():
        return [DummyPosition(market_id=9, position="3.0", sign=None, side="sell")]

    monkeypatch.setattr(client, "_fetch_positions_with_retry", fake_fetch)

    quantity = asyncio.run(client.get_account_positions())

    assert quantity == Decimal("-3.0")