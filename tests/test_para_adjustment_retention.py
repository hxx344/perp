import asyncio


def test_adjustment_manager_retention_seconds_keeps_recent() -> None:
    from strategies.adjustments import GrvtAdjustmentManager

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        manager = GrvtAdjustmentManager(history_limit=50, expiry_seconds=900, retention_seconds=3 * 24 * 3600)

        req = loop.run_until_complete(
            manager.create_request(
                action="add",
                magnitude=1.0,
                agent_ids=["agent-a"],
                symbols=["BTC-USD-PERP"],
                created_by="test",
            )
        )

        # Artificially age the request close to (but not exceeding) retention window.
        created_at = float(req["created_at"])
        manager._requests[req["request_id"]].created_at = created_at - (3 * 24 * 3600) + 10  # type: ignore[attr-defined]

        summary = loop.run_until_complete(manager.summary())
        assert summary.get("requests"), "request should still be retained within 3d window"
    finally:
        loop.close()
        asyncio.set_event_loop(None)


def test_adjustment_manager_retention_seconds_prunes_old() -> None:
    from strategies.adjustments import GrvtAdjustmentManager

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        manager = GrvtAdjustmentManager(history_limit=50, expiry_seconds=900, retention_seconds=3 * 24 * 3600)

        req = loop.run_until_complete(
            manager.create_request(
                action="add",
                magnitude=1.0,
                agent_ids=["agent-a"],
                symbols=["BTC-USD-PERP"],
                created_by="test",
            )
        )

        created_at = float(req["created_at"])
        manager._requests[req["request_id"]].created_at = created_at - (3 * 24 * 3600) - 10  # type: ignore[attr-defined]

        summary = loop.run_until_complete(manager.summary())
        assert not summary.get("requests"), "request should be pruned after 3d retention window"
    finally:
        loop.close()
        asyncio.set_event_loop(None)
