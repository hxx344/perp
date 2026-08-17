from __future__ import annotations

import asyncio
import base64
from decimal import Decimal
from types import SimpleNamespace

import pytest
from aiohttp import web

from services.coordinator import CoordinatorApp, CoordinatorState, _validate_bind_security


def test_coordinator_tracks_position_metadata_and_targeted_actions() -> None:
    async def scenario() -> None:
        state = CoordinatorState()
        await state.register(vps_id="node-a", display_name="A")
        await state.register(vps_id="node-b", display_name="B")
        await state.update_metrics(
            vps_id="node-a",
            position=Decimal("0.2"),
            position_symbol="BTC",
            position_value=Decimal("13000"),
            position_direction="long",
            active_close_amount=Decimal("0.15"),
            trading_volume=Decimal("5000"),
            balance=Decimal("100"),
            total_value=Decimal("200"),
            timestamp=123.0,
            manual_balance_preview={"difference": "0.05"},
        )
        await state.enqueue_action(
            action_type="MANUAL_BALANCE",
            reason="operator",
            target_vps_ids=["node-a"],
        )

        command_a = await state.next_command(vps_id="node-a")
        command_b = await state.next_command(vps_id="node-b")
        status = await state.status()

        assert command_a["actions"][0]["type"] == "MANUAL_BALANCE"
        assert command_b["actions"] == []
        agent = next(item for item in status["agents"] if item["vps_id"] == "node-a")
        assert agent["position_direction"] == "long"
        assert agent["position_value"] == "13000"
        assert agent["active_close_amount"] == "0.15"
        assert agent["manual_balance_preview"] == {"difference": "0.05"}

    asyncio.run(scenario())


@pytest.mark.asyncio
async def test_coordinator_agent_endpoints_require_auth_when_configured() -> None:
    app = CoordinatorApp(
        state=CoordinatorState(),
        dashboard_user="operator",
        dashboard_password="secret",
    )

    with pytest.raises(web.HTTPUnauthorized):
        app._require_dashboard_auth(SimpleNamespace(headers={}))

    token = base64.b64encode(b"operator:secret").decode("ascii")
    app._require_dashboard_auth(
        SimpleNamespace(headers={"Authorization": f"Basic {token}"})
    )


def test_coordinator_rejects_unauthenticated_public_bind() -> None:
    with pytest.raises(ValueError, match="require authentication"):
        _validate_bind_security("0.0.0.0", user=None, password=None)

    with pytest.raises(ValueError, match="configured together"):
        _validate_bind_security("127.0.0.1", user="operator", password=None)

    _validate_bind_security("127.0.0.1", user=None, password=None)
    _validate_bind_security("0.0.0.0", user="operator", password="secret")
