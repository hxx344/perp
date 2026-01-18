import asyncio


def test_para_twap_scheduler_rollup_accumulates_requests(monkeypatch):
    from strategies.hedge_coordinator import CoordinatorApp

    # Windows + Py3.9: pytest 环境下默认可能没有 current event loop，
    # 但 CoordinatorApp 初始化会创建 asyncio.Lock()。
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    app = CoordinatorApp()

    async def fake_list_agent_ids():
        return ["a1"]

    monkeypatch.setattr(app._coordinator, "list_agent_ids", fake_list_agent_ids)

    # Create two fake TWAP requests by reusing the scheduler fire
    cfg = {
        "action": "add",
        "magnitude": 1,
        "interval_seconds": 90,
        "twap_duration_seconds": 90,
        "symbols": ["BTC"],
    }

    # Fire twice
    asyncio.run(app._fire_para_twap_scheduler_once(cfg))
    asyncio.run(app._fire_para_twap_scheduler_once(cfg))

    history = app._para_twap_scheduler_status.get("history")
    assert isinstance(history, list) and history
    entry = history[0]
    assert isinstance(entry, dict)
    req_ids = entry.get("request_ids")
    assert isinstance(req_ids, list)
    assert len(req_ids) == 2

    # Now rollup should compute request_count == 2
    asyncio.run(app._para_twap_scheduler_rollup_history())
    roll = entry.get("rollup")
    assert isinstance(roll, dict)
    assert roll.get("request_count") == 2
