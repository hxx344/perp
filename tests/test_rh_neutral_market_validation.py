from __future__ import annotations

from typing import Any, Dict

import pytest

from strategies.rh_neutral_manager import AccountSpec, LighterAccountGateway, NeutralSettings


def _settings(*, live: bool) -> NeutralSettings:
    return NeutralSettings(
        main=AccountSpec("main", 10, 4, {4: "a" * 80} if live else {}),
        sub=AccountSpec("sub", 11, 4, {4: "b" * 80} if live else {}),
        spy_market_id=26,
        qqq_market_id=25,
        live=live,
    )


def _market_details(**overrides: Any) -> Dict[str, Any]:
    details: Dict[str, Any] = {
        "market_id": 26,
        "symbol": "SPY",
        "market_type": "perp",
        "status": "active",
        "size_decimals": 5,
        "price_decimals": 1,
        "min_base_amount": "0.00020",
        "min_quote_amount": "1",
    }
    details.update(overrides)
    return {"order_book_details": [details]}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("size_decimals", "2.5", "non-integer supported size precision"),
        ("price_decimals", "19", "unsupported price precision"),
        ("min_base_amount", "NaN", "invalid minimum base amount"),
        ("min_quote_amount", "-1", "invalid minimum quote amount"),
    ],
)
async def test_live_market_details_reject_invalid_numeric_metadata(field, value, message):
    settings = _settings(live=True)
    gateway = LighterAccountGateway(settings.main, settings, session=None)  # type: ignore[arg-type]

    async def fake_get(path, params=None, **kwargs):
        return _market_details(**{field: value})

    gateway._get_json = fake_get  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match=message):
        await gateway.fetch_market(26)


@pytest.mark.asyncio
async def test_live_market_details_reject_conflicting_precision_aliases():
    settings = _settings(live=True)
    gateway = LighterAccountGateway(settings.main, settings, session=None)  # type: ignore[arg-type]

    async def fake_get(path, params=None, **kwargs):
        return _market_details(size_decimals=5, supported_size_decimals=4)

    gateway._get_json = fake_get  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="conflicting supported size precision"):
        await gateway.fetch_market(26)
