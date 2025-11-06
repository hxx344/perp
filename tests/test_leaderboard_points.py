import asyncio
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

from strategies.aster_lighter_cycle import CycleConfig, HedgingCycleExecutor
from strategies.aster_lighter_cycle import _extract_leaderboard_points


def test_extract_leaderboard_points_prefers_entry_id_11():
    entries = [
        {"l1_address": "0xTopOne", "points": 123, "entryId": 1},
        {"l1_address": "0xMyAddress", "points": "456.78", "entryId": 11},
    ]

    result = _extract_leaderboard_points(entries, "0xmyaddress")

    assert result == Decimal("456.78")


def test_extract_leaderboard_points_falls_back_to_address_match():
    entries = [
        {"l1_address": "0xMYAddress", "points": 99.5, "entryId": 42},
    ]

    result = _extract_leaderboard_points(entries, "0xmyaddress")

    assert result == Decimal("99.5")


def test_extract_leaderboard_points_handles_invalid_payload():
    entries = [
        {"l1_address": "0xmyaddress", "points": None, "entryId": 11},
    ]

    result = _extract_leaderboard_points(entries, "0xmyaddress")

    assert result is None


def _make_executor() -> HedgingCycleExecutor:
    config = CycleConfig(
        aster_ticker="ETH",
        lighter_ticker="ETH-PERP",
        quantity=Decimal("1"),
        aster_quantity=Decimal("1"),
        lighter_quantity=Decimal("1"),
        direction="buy",
        take_profit_pct=Decimal("0"),
        slippage_pct=Decimal("0.1"),
        max_wait_seconds=0.0,
        lighter_max_wait_seconds=0.0,
        poll_interval=0.0,
        max_retries=1,
        retry_delay_seconds=0.0,
        max_cycles=1,
        delay_between_cycles=0.0,
        virtual_aster_maker=False,
    )
    config.log_to_console = False
    return HedgingCycleExecutor(config)


def test_log_leaderboard_points_refreshes_every_100_cycles(monkeypatch):
    executor = _make_executor()

    cached_address = "0x1234567890abcdef1234567890abcdef12345678"
    executor._resolve_lighter_l1_address = AsyncMock(return_value=cached_address)

    tokens = iter(["token-1", "token-2"])

    def _create_auth_token_with_expiry():
        try:
            token_value = next(tokens)
        except StopIteration:
            return None, "no-token"
        return token_value, None

    executor.lighter_client = cast(
        Any,
        SimpleNamespace(
        base_url="https://example.zklighter.elliot.ai",
        lighter_client=SimpleNamespace(create_auth_token_with_expiry=_create_auth_token_with_expiry),
        ),
    )

    fetch_calls = []

    async def fake_fetch(address, *, base_url=None, timeout_seconds=10.0, auth_token=None):
        fetch_calls.append((address, len(fetch_calls), base_url, auth_token))
        index = len(fetch_calls)
        return Decimal(str(index)), Decimal(str(index * 10))

    print_calls = []

    def fake_print(cycle_number, weekly_points, total_points, **kwargs):
        print_calls.append((cycle_number, weekly_points, total_points))

    monkeypatch.setattr("strategies.aster_lighter_cycle._fetch_lighter_leaderboard_points", fake_fetch)
    monkeypatch.setattr("strategies.aster_lighter_cycle._print_leaderboard_points", fake_print)

    async def _run():
        await executor._log_leaderboard_points(1)
        await executor._log_leaderboard_points(5)
        await executor._log_leaderboard_points(101)

    asyncio.run(_run())

    # Fetch should only occur on first call and when refresh threshold reached
    assert len(fetch_calls) == 2
    assert all(call[0] == cached_address for call in fetch_calls)
    assert [call[2] for call in fetch_calls] == ["https://example.zklighter.elliot.ai", "https://example.zklighter.elliot.ai"]
    assert [call[3] for call in fetch_calls] == ["token-1", "token-2"]

    assert print_calls[0][0] == 1
    assert print_calls[0][1:] == (Decimal("1"), Decimal("10"))

    # Cycle 5 uses cached values from first fetch
    assert print_calls[1][0] == 5
    assert print_calls[1][1:] == print_calls[0][1:]

    # Cycle 101 uses refreshed values
    assert print_calls[2][0] == 101
    assert print_calls[2][1:] == (Decimal("2"), Decimal("20"))
