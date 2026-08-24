from __future__ import annotations

from typing import Any, Dict

import pytest

from strategies.rh_neutral_manager import AccountSpec, LighterAccountGateway, NeutralSettings


def _settings() -> NeutralSettings:
    return NeutralSettings(
        main=AccountSpec("main", 10, 4, {4: "a" * 80}),
        sub=AccountSpec("sub", 11, 4, {4: "b" * 80}),
        spy_market_id=26,
        qqq_market_id=25,
        live=True,
    )


def _account_payload(**overrides: Any) -> Dict[str, Any]:
    account: Dict[str, Any] = {
        "account_index": 10,
        "l1_address": "0xmaster",
        "status": 1,
        "account_type": 0,
        "cross_asset_value": "100",
        "collateral": "100",
        "available_balance": "80",
        "cross_initial_margin_requirement": "10",
        "cross_maintenance_margin_requirement": "20",
        "positions": [],
    }
    account.update(overrides)
    return {"accounts": [account]}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("cross_asset_value", "not-a-number", "invalid equity"),
        ("available_balance", "-1", "negative available balance"),
        ("cross_maintenance_margin_requirement", "NaN", "invalid maintenance margin requirement"),
    ],
)
async def test_live_account_reader_rejects_invalid_risk_balances(field, value, message):
    gateway = LighterAccountGateway(_settings().main, _settings(), session=None)  # type: ignore[arg-type]

    async def fake_get(path, params=None, **kwargs):
        return _account_payload(**{field: value})

    gateway._get_json = fake_get  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match=message):
        await gateway.fetch_account()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("position", "NaN", "invalid position size"),
        ("position_value", "Infinity", "invalid position value"),
    ],
)
async def test_live_account_reader_rejects_invalid_position_numbers(field, value, message):
    gateway = LighterAccountGateway(_settings().main, _settings(), session=None)  # type: ignore[arg-type]

    position = {
        "market_id": 26,
        "symbol": "SPY",
        "position": "1",
        "sign": 1,
        "position_value": "100",
    }
    position[field] = value

    async def fake_get(path, params=None, **kwargs):
        return _account_payload(positions=[position])

    gateway._get_json = fake_get  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match=message):
        await gateway.fetch_account()


@pytest.mark.asyncio
async def test_live_account_reader_rejects_duplicate_position_market_ids():
    gateway = LighterAccountGateway(_settings().main, _settings(), session=None)  # type: ignore[arg-type]
    positions = [
        {"market_id": 26, "symbol": "SPY", "position": "1", "sign": 1, "position_value": "100"},
        {"market_id": 26, "symbol": "SPY", "position": "0.5", "sign": 1, "position_value": "50"},
    ]

    async def fake_get(path, params=None, **kwargs):
        return _account_payload(positions=positions)

    gateway._get_json = fake_get  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="duplicate position market id 26"):
        await gateway.fetch_account()
