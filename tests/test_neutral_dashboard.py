import base64
import json
from decimal import Decimal

import aiohttp
import pytest

from strategies.neutral_dashboard import NeutralAction, NeutralDashboard


def _auth(username="operator", password="test-password"):
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


@pytest.mark.asyncio
async def test_loopback_dashboard_snapshot_serializes_decimal_and_dispatches_position_action():
    actions = []

    async def handler(action):
        actions.append(action)
        return {"accepted": True}

    dashboard = NeutralDashboard(
        lambda: {"accounts": {"parent": {"available_balance": Decimal("12.3")}}},
        handler,
        port=0,
        username="operator",
        password="test-password",
    )
    await dashboard.start()
    try:
        async with aiohttp.ClientSession() as session:
            headers = _auth()
            async with session.get(f"http://127.0.0.1:{dashboard.bound_port}/api/snapshot", headers=headers) as response:
                assert response.status == 200
                assert (await response.json())["accounts"]["parent"]["available_balance"] == "12.3"

            payload = {
                "request_id": "position-1",
                "account": "parent",
                "symbol": "spy",
                "quantity": "0.25",
            }
            headers.update({"Content-Type": "application/json", "X-Neutral-Action": "dashboard"})
            async with session.post(
                f"http://127.0.0.1:{dashboard.bound_port}/api/actions/close-position",
                headers=headers,
                data=json.dumps(payload),
            ) as response:
                assert response.status == 200
                assert (await response.json())["ok"] is True
        assert actions == [
            NeutralAction(
                action="close_position",
                request_id="position-1",
                account="main",
                symbol="SPY",
                quantity=Decimal("0.25"),
                fraction=None,
                reason="operator dashboard",
            )
        ]
    finally:
        await dashboard.stop()


@pytest.mark.asyncio
async def test_actions_require_auth_header_and_are_idempotent():
    actions = []

    async def handler(action):
        actions.append(action)
        return {"ok": "handled"}

    dashboard = NeutralDashboard(lambda: {}, handler, port=0, username="u", password="p")
    await dashboard.start()
    try:
        async with aiohttp.ClientSession() as session:
            body = {"request_id": "same", "account": "parent", "symbol": "QQQ", "quantity": "1"}
            headers = {**_auth("u", "p"), "Content-Type": "application/json"}
            async with session.post(
                f"http://127.0.0.1:{dashboard.bound_port}/api/actions/close-position",
                headers=headers,
                data=json.dumps(body),
            ) as response:
                assert response.status == 403
            headers["X-Neutral-Action"] = "dashboard"
            async with session.post(
                f"http://127.0.0.1:{dashboard.bound_port}/api/actions/close-position",
                headers=headers,
                data=json.dumps(body),
            ) as response:
                assert response.status == 200
            async with session.post(
                f"http://127.0.0.1:{dashboard.bound_port}/api/actions/close-position",
                headers=headers,
                data=json.dumps(body),
            ) as response:
                assert response.status == 409
        assert len(actions) == 1
    finally:
        await dashboard.stop()


@pytest.mark.asyncio
async def test_pair_action_does_not_expose_an_account_and_flatten_requires_confirmation():
    actions = []

    async def handler(action):
        actions.append(action)
        return {}

    dashboard = NeutralDashboard(lambda: {}, handler, port=0, username="u", password="p")
    await dashboard.start()
    try:
        async with aiohttp.ClientSession() as session:
            headers = {
                **_auth("u", "p"),
                "Content-Type": "application/json",
                "X-Neutral-Action": "dashboard",
            }
            async with session.post(
                f"http://127.0.0.1:{dashboard.bound_port}/api/actions/close-pair",
                headers=headers,
                json={"request_id": "pair-1", "symbol": "SPY", "quantity": "0.1"},
            ) as response:
                assert response.status == 200
            async with session.post(
                f"http://127.0.0.1:{dashboard.bound_port}/api/actions/close-pair",
                headers=headers,
                json={"request_id": "pair-full", "symbol": "QQQ"},
            ) as response:
                assert response.status == 200
            async with session.post(
                f"http://127.0.0.1:{dashboard.bound_port}/api/actions/flatten-all",
                headers=headers,
                json={"request_id": "flat-1"},
            ) as response:
                assert response.status == 400
            async with session.post(
                f"http://127.0.0.1:{dashboard.bound_port}/api/actions/flatten-all",
                headers=headers,
                json={"request_id": "flat-1", "confirm": "FLATTEN_ALL"},
            ) as response:
                assert response.status == 200
        assert actions[0].action == "close_pair"
        assert actions[0].account is None
        assert actions[0].symbol == "SPY"
        assert actions[1].action == "close_pair"
        assert actions[1].quantity is None
        assert actions[2].action == "flatten_all"
    finally:
        await dashboard.stop()


@pytest.mark.asyncio
async def test_rebalance_action_carries_no_client_supplied_transfer_amount():
    actions = []

    async def handler(action):
        actions.append(action)
        return {"status": "dry_run"}

    dashboard = NeutralDashboard(lambda: {}, handler, port=0, username="u", password="p")
    await dashboard.start()
    try:
        async with aiohttp.ClientSession() as session:
            headers = {
                **_auth("u", "p"),
                "Content-Type": "application/json",
                "X-Neutral-Action": "dashboard",
            }
            async with session.post(
                f"http://127.0.0.1:{dashboard.bound_port}/api/actions/rebalance",
                headers=headers,
                json={"request_id": "rebalance-1", "amount": "999999"},
            ) as response:
                assert response.status == 200
        assert actions[0].action == "rebalance"
        assert actions[0].quantity is None
        assert actions[0].account is None
    finally:
        await dashboard.stop()


@pytest.mark.asyncio
async def test_action_with_unknown_exchange_status_is_not_reported_as_success():
    async def handler(_action):
        return {"status": "unknown_pending", "pending_id": "p-1"}

    dashboard = NeutralDashboard(lambda: {}, handler, port=0, username="u", password="p")
    await dashboard.start()
    try:
        async with aiohttp.ClientSession() as session:
            headers = {
                **_auth("u", "p"),
                "Content-Type": "application/json",
                "X-Neutral-Action": "dashboard",
            }
            async with session.post(
                f"http://127.0.0.1:{dashboard.bound_port}/api/actions/rebalance",
                headers=headers,
                json={"request_id": "unknown-1"},
            ) as response:
                assert response.status == 202
                body = await response.json()
                assert body["ok"] is False
                assert body["status"] == "unknown_pending"
    finally:
        await dashboard.stop()


@pytest.mark.asyncio
async def test_nested_partial_leg_error_is_visible_to_dashboard_client():
    async def handler(_action):
        return {"results": {"main:SPY": {"status": "accepted_pending_confirmation"}, "sub:SPY": {"error": "rejected"}}}

    dashboard = NeutralDashboard(lambda: {}, handler, port=0, username="u", password="p")
    await dashboard.start()
    try:
        async with aiohttp.ClientSession() as session:
            headers = {
                **_auth("u", "p"),
                "Content-Type": "application/json",
                "X-Neutral-Action": "dashboard",
            }
            async with session.post(
                f"http://127.0.0.1:{dashboard.bound_port}/api/actions/close-pair",
                headers=headers,
                json={"request_id": "partial-1", "symbol": "SPY", "quantity": "0.1"},
            ) as response:
                assert response.status == 502
                body = await response.json()
                assert body["ok"] is False
                assert body["status"] == "error"
    finally:
        await dashboard.stop()


@pytest.mark.asyncio
async def test_public_bind_requires_explicit_opt_in_and_credentials():
    dashboard = NeutralDashboard(lambda: {}, lambda _action: {}, host="0.0.0.0", port=0)
    with pytest.raises(RuntimeError, match="loopback"):
        await dashboard.start()

    dashboard = NeutralDashboard(
        lambda: {},
        lambda _action: {},
        host="0.0.0.0",
        port=0,
        allow_public_bind=True,
    )
    with pytest.raises(RuntimeError, match="credentials"):
        await dashboard.start()


@pytest.mark.asyncio
async def test_unconfigured_loopback_dashboard_is_read_only():
    dashboard = NeutralDashboard(lambda: {"ok": True}, lambda _action: {}, port=0)
    await dashboard.start()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"http://127.0.0.1:{dashboard.bound_port}/api/actions/close-position",
                headers={"X-Neutral-Action": "dashboard"},
                json={"request_id": "x", "account": "parent", "symbol": "SPY", "quantity": "1"},
            ) as response:
                assert response.status == 503
    finally:
        await dashboard.stop()
