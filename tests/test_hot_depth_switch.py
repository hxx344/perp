import asyncio
from typing import cast

from strategies.aster_lighter_cycle import _wait_for_hot_update_enabled
from helpers.logger import TradingLogger


class _StubLogger(TradingLogger):
    def __init__(self):
        super().__init__(exchange="test", ticker="TEST", log_to_console=False)
        self.messages = []

    def log(self, message: str, level: str) -> None:  # type: ignore[override]
        self.messages.append((message, level))
        super().log(message, level)


def test_wait_for_hot_update_enabled_returns_payload(monkeypatch):
    logger = _StubLogger()
    expected_payload = {"cycle_enabled": True, "aster_maker_depth_level": 12}

    def _fake_fetch(url, _logger):
        return expected_payload

    monkeypatch.setattr(
        "strategies.aster_lighter_cycle._fetch_hot_update_payload",
        _fake_fetch,
    )

    result = asyncio.run(_wait_for_hot_update_enabled("http://example.com", logger))

    assert result == expected_payload
    assert logger.messages == []


def test_wait_for_hot_update_enabled_waits_when_disabled(monkeypatch):
    logger = _StubLogger()
    payloads = iter([
        {"cycle_enabled": False},
        {"cycle_enabled": True, "aster_leg1_depth_level": 8},
    ])
    sleep_calls = []

    def _fake_fetch(url, _logger):
        try:
            return next(payloads)
        except StopIteration:
            return None

    async def _fake_sleep(duration: float):
        sleep_calls.append(duration)

    monkeypatch.setattr(
        "strategies.aster_lighter_cycle._fetch_hot_update_payload",
        _fake_fetch,
    )
    monkeypatch.setattr("strategies.aster_lighter_cycle.asyncio.sleep", _fake_sleep)

    result = asyncio.run(_wait_for_hot_update_enabled("http://example.com", logger))

    assert result == {"cycle_enabled": True, "aster_leg1_depth_level": 8}
    assert sleep_calls == [60.0]
    assert any("disabled" in msg.lower() for msg, _ in logger.messages)
