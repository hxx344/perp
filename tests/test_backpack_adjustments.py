import pytest

from strategies.backpack_adjustments import BackpackAdjustmentManager


@pytest.mark.asyncio
async def test_broadcast_pending_visible_to_any_agent_and_multi_ack() -> None:
    mgr = BackpackAdjustmentManager()

    adj = mgr.create(
        agent_id="all",
        action="add",
        magnitude=1,
        symbols=[],
        payload={"order_mode": "twap", "twap_duration_seconds": 60},
    )

    pending_a = await mgr.pending_for_agent("agent-a")
    pending_b = await mgr.pending_for_agent("agent-b")

    assert any(item["request_id"] == adj.request_id for item in pending_a)
    assert any(item["request_id"] == adj.request_id for item in pending_b)

    ok_a = await mgr.acknowledge(request_id=adj.request_id, agent_id="agent-a", status="succeeded", note=None, extra={"avg_price": 1})
    ok_b = await mgr.acknowledge(request_id=adj.request_id, agent_id="agent-b", status="failed", note="no liquidity", extra=None)
    assert ok_a is True
    assert ok_b is True

    all_items = await mgr.list_all()
    item = next(x for x in all_items if x["request_id"] == adj.request_id)

    assert item["agent_id"] == "all"
    assert "agents" in item
    agent_ids = {a["agent_id"] for a in item["agents"]}
    assert agent_ids == {"agent-a", "agent-b"}
    assert item.get("overall_status") in {"failed", "in_progress"}


@pytest.mark.asyncio
async def test_targeted_ack_enforces_agent_id() -> None:
    mgr = BackpackAdjustmentManager()

    adj = mgr.create(
        agent_id="agent-x",
        action="reduce",
        magnitude=2,
        symbols=["ETH-PERP"],
        payload={},
    )

    assert await mgr.acknowledge(request_id=adj.request_id, agent_id="agent-y", status="succeeded", note=None, extra=None) is False
    assert await mgr.acknowledge(request_id=adj.request_id, agent_id="agent-x", status="succeeded", note="ok", extra={"filled_qty": 1}) is True

    all_items = await mgr.list_all()
    item = next(x for x in all_items if x["request_id"] == adj.request_id)
    assert item["status"] == "succeeded"
