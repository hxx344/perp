import asyncio

import pytest


def test_parse_para_twap_scheduler_defaults():
    # Import is intentionally inside the test to match other tests' patterns.
    from strategies.hedge_coordinator import CoordinatorApp

    app = CoordinatorApp()
    cfg = app._parse_para_twap_scheduler_config({
        "action": "add",
        "magnitude": 1,
        "interval_seconds": 900,
        "twap_duration_seconds": 900,
        "symbols": ["BTC"],
    })

    assert cfg["action"] == "add"
    assert cfg["magnitude"] == 1.0
    assert cfg["interval_seconds"] == 900
    assert cfg["twap_duration_seconds"] == 900
    assert cfg["symbols"] == ["BTC"]


def test_parse_para_twap_scheduler_clamps_and_symbol_normalizes():
    from strategies.hedge_coordinator import CoordinatorApp

    app = CoordinatorApp()
    cfg = app._parse_para_twap_scheduler_config({
        "action": "reduce",
        "magnitude": "2",
        "interval_seconds": 5,  # should clamp to >=30
        "twap_duration_seconds": 31,  # should round to 30-step
        "symbols": [" btc ", "BTC", ""],
    })

    assert cfg["action"] == "reduce"
    assert cfg["magnitude"] == 2.0
    assert cfg["interval_seconds"] == 30
    assert cfg["twap_duration_seconds"] == 30
    assert cfg["symbols"] == ["BTC"]


def test_scheduler_fire_requires_agents(monkeypatch):
    from strategies.hedge_coordinator import CoordinatorApp

    app = CoordinatorApp()

    async def fake_list_agent_ids():
        return []

    monkeypatch.setattr(app._coordinator, "list_agent_ids", fake_list_agent_ids)

    with pytest.raises(RuntimeError):
        asyncio.run(
            app._fire_para_twap_scheduler_once({
                "action": "add",
                "magnitude": 1,
                "interval_seconds": 900,
                "twap_duration_seconds": 900,
                "symbols": ["BTC"],
            })
        )
