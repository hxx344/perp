from decimal import Decimal

import pytest

from strategies.aster_cost_monitor import BBO
from strategies.aster_volume_paper_strategy import PaperSettings, PaperStrategy


def test_paper_cycle_accounts_for_two_sided_fee_and_spread_wear():
    strategy = PaperStrategy(PaperSettings(cycle_notional=Decimal("100"), max_spread_bps=Decimal("200")))
    result = strategy.paper_cycle(BBO(1, Decimal("100"), Decimal("101")))

    assert result["status"] == "paper_completed"
    assert result["direction"] == "BUY"
    assert result["position_after"] == Decimal("0")
    assert result["fees"] > 0
    assert result["wear"] > result["fees"]
    assert strategy.state.volume > 0


def test_paper_cycle_skips_wide_spread_without_changing_position():
    strategy = PaperStrategy(PaperSettings(max_spread_bps=Decimal("1")))
    result = strategy.paper_cycle(BBO(1, Decimal("100"), Decimal("101")))

    assert result["status"] == "skipped"
    assert strategy.state.cycles == 0
    assert strategy.state.position == 0


def test_paper_adapter_cannot_submit_live_orders():
    strategy = PaperStrategy(PaperSettings())
    with pytest.raises(RuntimeError, match="paper-only"):
        import asyncio
        asyncio.run(strategy.adapter.market_order("BUY", Decimal("1")))
