from decimal import Decimal

from strategies.aster_cost_monitor import BBO, AsterCostMonitor, CostSettings, SymbolStats


def test_symbol_stats_calculates_average_spread_and_round_trip_wear():
    stats = SymbolStats("XAUUSD1")
    stats.add(BBO(100, Decimal("100"), Decimal("101")), 0)
    stats.add(BBO(101, Decimal("100"), Decimal("102")), 0)

    payload = stats.as_payload(Decimal("10000"), Decimal("0.00009"))

    assert payload["sample_count"] == 2
    assert Decimal(payload["average_spread_bps"]) == Decimal("148.7611447711935372641741786")
    assert Decimal(payload["round_trip_fees"]) == Decimal("1.8")
    assert Decimal(payload["total_wear"]) > Decimal("1.8")


def test_cost_settings_default_symbols_include_requested_pairs():
    settings = CostSettings()
    settings.validate()
    assert settings.target_symbol == "XAUUSD1"
    assert {"SKHYNIXUSD1", "SPCXUSD1", "CLUSD1", "SNDKUSD1", "MUUSD1"}.issubset(settings.symbols)


def test_cost_monitor_snapshot_is_read_only():
    monitor = AsterCostMonitor(CostSettings())
    payload = monitor.snapshot()
    assert payload["mode"] == "read_only_cost_monitor"
    assert payload["condition_met"] is False
    assert not hasattr(monitor, "place_order")
