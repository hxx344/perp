from decimal import Decimal

import pytest

from strategies.aster_cost_monitor import BBO
from strategies.aster_market_stability_canary import CanarySettings, MarketRules, AsterMarketStabilityCanary
from strategies.aster_neutral_manager import AsterAccountSpec


def _settings(**kwargs):
    defaults = dict(
        account=AsterAccountSpec(
            "canary",
            user_address="0x" + "1" * 40,
            signer_address="0x" + "2" * 40,
            signer_private_key="3" * 64,
        )
    )
    defaults.update(kwargs)
    return CanarySettings(**defaults)


def test_live_canary_requires_explicit_confirmation():
    with pytest.raises(ValueError, match="confirm_live"):
        _settings(live=True, confirm_live=False).validate()


def test_canary_default_quantity_clears_current_xau_minimum_notional_near_4000():
    settings = _settings()
    assert settings.quantity * Decimal("4000") >= Decimal("5")


def test_canary_cycles_and_quantity_are_bounded():
    with pytest.raises(ValueError, match="cycles"):
        _settings(cycles=11).validate()
    with pytest.raises(ValueError, match="quantity"):
        _settings(quantity=Decimal("1"), max_quantity=Decimal("0.01")).validate()


def test_wear_includes_spread_and_two_sided_fee():
    canary = AsterMarketStabilityCanary(_settings())
    bbo = BBO(1, Decimal("100"), Decimal("101"))
    wear = canary.estimate_wear(bbo, Decimal("1"))
    expected = Decimal("1") + Decimal("100.5") * Decimal("0.00009") * 2
    assert wear == expected


def test_market_rules_floor_quantity_to_step():
    rules = MarketRules(Decimal("0.001"), Decimal("0.001"), Decimal("5"))
    assert rules.normalize(Decimal("0.0019")) == Decimal("0.001")


@pytest.mark.asyncio
async def test_emergency_flatten_uses_authoritative_position_and_reduce_only():
    canary = AsterMarketStabilityCanary(_settings())
    positions = iter((Decimal("0.003"), Decimal("0")))
    calls = []

    async def fetch_position():
        return next(positions)

    async def submit_market(side, quantity, *, reduce_only):
        calls.append((side, quantity, reduce_only))
        return {"status": "FILLED", "executed_quantity": quantity, "average_price": Decimal("100")}

    canary.fetch_position = fetch_position  # type: ignore[method-assign]
    canary.submit_market = submit_market  # type: ignore[method-assign]

    result = await canary.emergency_flatten()

    assert result["status"] == "flat"
    assert calls == [("SELL", Decimal("0.003"), True)]
