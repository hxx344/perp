import asyncio
from dataclasses import dataclass


def test_adjustment_manager_acknowledge_extra_fields() -> None:
    # Local import to match production wiring
    from strategies.adjustments import GrvtAdjustmentManager

    manager = GrvtAdjustmentManager(history_limit=5, expiry_seconds=900)
    req = asyncio.run(
        manager.create_request(
            action="add",
            magnitude=1.0,
            agent_ids=["agent-a"],
            symbols=["BTC-USD-PERP"],
            created_by="test",
            payload={"order_mode": "twap"},
        )
    )

    extra = {
        "order_type": "TWAP",
        "avg_price": "123.45",
        "filled_qty": "0.7",
        "order_id": "abc",
    }

    updated = asyncio.run(
        manager.acknowledge(
            request_id=req["request_id"],
            agent_id="agent-a",
            status="acknowledged",
            note="ok",
            extra=extra,
        )
    )

    agents = updated.get("agents")
    assert isinstance(agents, list) and agents
    agent_state = agents[0]
    assert agent_state.get("extra") == extra


def test_para_adjust_ack_handler_accepts_extra_fields() -> None:
    # This test directly exercises the handler method with a fake request object,
    # ensuring we plumb extra fields into the adjustment manager.
    from strategies.hedge_coordinator import CoordinatorApp

    @dataclass
    class _FakeRequest:
        body: dict

        async def json(self):
            return self.body

    # CoordinatorApp creates asyncio primitives in __init__ on Python 3.9,
    # so the main thread must have a current event loop.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        app = CoordinatorApp(
            dashboard_username=None,
            dashboard_password=None,
            dashboard_session_ttl=3600,
            alert_settings=None,
            enable_volatility_monitor=False,
            volatility_symbols=None,
            volatility_poll_interval=60.0,
            volatility_history_limit=1800,
        )
    finally:
        # Keep loop available for subsequent asyncio.run calls in this test.
        pass

    req_payload = asyncio.run(
        app._para_adjustments.create_request(
            action="add",
            magnitude=1.0,
            agent_ids=["agent-a"],
            symbols=["BTC-USD-PERP"],
            created_by="test",
            payload={"order_mode": "twap"},
        )
    )

    fake = _FakeRequest(
        {
            "request_id": req_payload["request_id"],
            "agent_id": "agent-a",
            "status": "acknowledged",
            "note": "ok",
            "order_type": "TWAP",
            "avg_price": "123.45",
            "filled_qty": "0.7",
            "order_id": "abc",
            "algo_size": "1.0",
            "algo_expected_size": "1.0",
            # raw history timestamps
            "created_at": 1700000000000,
            "last_updated_at": 1700000005000,
            "end_at": 1700000009000,
        }
    )

    # handler returns aiohttp Response; we just need it not to raise and to update manager state.
    asyncio.run(app.handle_para_adjust_ack(fake))  # type: ignore[arg-type]

    summary = asyncio.run(app._para_adjustments.summary())
    latest = summary["requests"][0]
    agent_state = latest["agents"][0]
    assert agent_state["extra"]["order_type"] == "TWAP"
    assert agent_state["extra"]["avg_price"] == "123.45"
    assert agent_state["extra"]["filled_qty"] == "0.7"
    assert agent_state["extra"]["order_id"] == "abc"
    assert agent_state["extra"]["algo_size"] == "1.0"
    assert agent_state["extra"]["algo_expected_size"] == "1.0"
    assert agent_state["extra"]["created_at"] == 1700000000000
    assert agent_state["extra"]["last_updated_at"] == 1700000005000
    assert agent_state["extra"]["end_at"] == 1700000009000

    loop.close()
    asyncio.set_event_loop(None)
