from __future__ import annotations

import time
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from strategies.aster_lighter_cycle import (
    HedgingCycleExecutor,
    _resolve_hedge_coordinator_settings,
)
from strategies.hedge_coordinator import HedgeState


def test_hedge_state_preserves_position_metadata_and_aggregates_direction() -> None:
    long_state = HedgeState(agent_id="long")
    long_state.update_from_payload(
        {
            "position": "0.20",
            "position_symbol": "BTC",
            "position_value": "13000",
            "position_direction": "long",
            "active_close_amount": "0.05",
            "manual_balance_preview": {"residual": "0.15"},
        }
    )
    short_state = HedgeState(agent_id="short")
    short_state.update_from_payload(
        {
            "position": "-0.10",
            "position_symbol": "BTC",
            "position_value": "6500",
            "active_close_amount": "0.02",
        }
    )

    serialized = long_state.serialize()
    aggregate = HedgeState.aggregate({"long": long_state, "short": short_state}).serialize()

    assert serialized["position_direction"] == "long"
    assert serialized["manual_balance_preview"] == {"residual": "0.15"}
    assert aggregate["position"] == "0.10"
    assert aggregate["position_direction"] == "long"
    assert aggregate["position_symbol"] == "BTC"
    assert aggregate["position_value"] == "19500"
    assert aggregate["active_close_amount"] == "0.07"


def test_robinhood_coordinator_settings_use_protected_environment(monkeypatch) -> None:
    monkeypatch.setenv("HEDGE_COORDINATOR_URL", "https://coordinator.example")
    monkeypatch.setenv("HEDGE_COORDINATOR_AGENT", "rh-btc-01")
    monkeypatch.setenv("HEDGE_COORDINATOR_USERNAME", "agent")
    monkeypatch.setenv("HEDGE_COORDINATOR_PASSWORD", "secret")

    settings = _resolve_hedge_coordinator_settings(SimpleNamespace())

    assert settings == (
        "https://coordinator.example",
        "rh-btc-01",
        "agent",
        "secret",
    )


def test_robinhood_coordinator_credentials_must_be_paired(monkeypatch) -> None:
    monkeypatch.setenv("HEDGE_COORDINATOR_USERNAME", "agent")
    monkeypatch.delenv("HEDGE_COORDINATOR_PASSWORD", raising=False)

    with pytest.raises(ValueError, match="must be configured together"):
        _resolve_hedge_coordinator_settings(SimpleNamespace())


@pytest.mark.parametrize(
    ("url", "message"),
    [
        ("http://agent:secret@coordinator.example", "embedded credentials"),
        ("http://coordinator.example", "must use HTTPS"),
    ],
)
def test_robinhood_coordinator_rejects_unsafe_remote_auth(
    monkeypatch,
    url,
    message,
) -> None:
    monkeypatch.setenv("HEDGE_COORDINATOR_URL", url)
    monkeypatch.setenv("HEDGE_COORDINATOR_USERNAME", "agent")
    monkeypatch.setenv("HEDGE_COORDINATOR_PASSWORD", "secret")

    with pytest.raises(ValueError, match=message):
        _resolve_hedge_coordinator_settings(SimpleNamespace())


@pytest.mark.asyncio
async def test_robinhood_cycle_reports_inventory_recovery_preview() -> None:
    executor = object.__new__(HedgingCycleExecutor)
    executor.config = SimpleNamespace(aster_ticker="BTC", lighter_ticker="BTC")
    executor.lighter_config = SimpleNamespace(contract_id="1")
    executor._metrics_reporter = SimpleNamespace(report=AsyncMock())
    executor._refresh_pause_state = AsyncMock(return_value=False)
    executor._coordinator_paused = False
    executor._last_reported_position = Decimal("0")
    executor._baseline_lighter_position = Decimal("0.10")
    executor._aster_maker_depth_level = 10
    executor._aster_leg1_depth_level = 10
    executor._aster_leg3_depth_level = 10
    executor._run_started_at = time.time() - 5
    executor._coordinator_agent_id = "rh-btc"
    executor.logger = SimpleNamespace(log=lambda *_args, **_kwargs: None)
    executor.lighter_client = SimpleNamespace(
        get_position_snapshot=AsyncMock(
            return_value={
                "symbol": "BTC",
                "size": Decimal("0.13"),
                "value": Decimal("8450"),
            }
        ),
        get_account_metrics=AsyncMock(
            return_value={
                "available_balance": Decimal("500"),
                "total_asset_value": Decimal("1000"),
            }
        ),
    )

    await executor.report_metrics(
        total_cycles=2,
        cumulative_pnl=Decimal("1.2"),
        cumulative_volume=Decimal("2000"),
    )

    kwargs = executor._metrics_reporter.report.await_args.kwargs
    assert kwargs["position"] == Decimal("0.13")
    assert kwargs["position_symbol"] == "BTC"
    assert kwargs["position_value"] == Decimal("8450")
    assert kwargs["position_direction"] == "long"
    assert kwargs["active_close_amount"] == Decimal("0")
    assert kwargs["manual_balance_preview"] == {
        "current_position": "0.13",
        "target_position": "0.10",
        "residual": "0.03",
        "suggested_action": "sell",
    }
