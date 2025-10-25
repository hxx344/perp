import asyncio
import logging
from decimal import Decimal

from strategies import aster_lighter_cycle as cycle


def test_extract_available_balance_handles_dict():
    account = {"available_balance": "5.100000"}
    amount = cycle._extract_available_balance(account)
    assert isinstance(amount, Decimal)
    assert amount == Decimal("5.100000")


def test_wait_for_account_ready_reaches_expected_balance(monkeypatch):
    snapshots = [
        None,
        {"index": 101, "available_balance": "0"},
        {"account_index": 101, "available_balance": "5.100000"},
    ]

    async def fake_fetch(_base_url: str, _l1_address: str, session=None):
        return snapshots.pop(0) if snapshots else None

    async def fake_sleep(_delay: float):
        return None

    monkeypatch.setattr(cycle, "_fetch_lighter_account_overview", fake_fetch)
    monkeypatch.setattr(cycle.asyncio, "sleep", fake_sleep)

    logger = logging.getLogger("test.wait_for_account")

    account_index, balance = asyncio.run(
        cycle._wait_for_lighter_account_ready(
            base_url="https://example",
            l1_address="0xabc",
            expected_balance=Decimal("5.1"),
            logger=logger,
            timeout_seconds=5,
            poll_interval=0,
        )
    )

    assert account_index == 101
    assert balance == Decimal("5.100000")