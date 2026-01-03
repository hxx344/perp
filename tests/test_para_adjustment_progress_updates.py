import asyncio

from strategies.adjustments import GrvtAdjustmentManager


def test_progress_ack_updates_extra_without_finalizing_status():
    # Ensure an event loop exists for asyncio.Lock() on older Python.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    mgr = GrvtAdjustmentManager(history_limit=10, expiry_seconds=60)

    request = loop.run_until_complete(
        mgr.create_request(action="add", magnitude=1.0, agent_ids=["vps-a", "vps-b"], symbols=["BTC-USD-PERP"])
    )
    request_id = request["request_id"]

    # Progress update should not mark agent as acknowledged.
    loop.run_until_complete(
        mgr.acknowledge(
            request_id=request_id,
            agent_id="vps-a",
            status="acknowledged",
            progress=True,
            extra={"avg_price": "25000", "filled_qty": 0.1, "algo_status": "OPEN"},
        )
    )

    snapshot = loop.run_until_complete(mgr.list_requests(limit=1))[0]
    agent_a = next(a for a in snapshot["agents"] if a["agent_id"] == "vps-a")
    assert agent_a["status"] == "pending"
    assert agent_a["extra"]["avg_price"] == "25000"
    assert agent_a["extra"]["filled_qty"] == 0.1

    # Final acknowledgement should finalize.
    loop.run_until_complete(
        mgr.acknowledge(
            request_id=request_id,
            agent_id="vps-a",
            status="acknowledged",
            note="done",
            extra={"filled_qty": 1.0},
        )
    )

    snapshot2 = loop.run_until_complete(mgr.list_requests(limit=1))[0]
    agent_a2 = next(a for a in snapshot2["agents"] if a["agent_id"] == "vps-a")
    assert agent_a2["status"] == "acknowledged"
    assert agent_a2["extra"]["filled_qty"] == 1.0
