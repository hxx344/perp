from decimal import Decimal

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
