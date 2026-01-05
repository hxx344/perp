from decimal import Decimal

from strategies.hedge_coordinator import CoordinatorApp


def test_backpack_bbo_wear_estimate_basic() -> None:
    # Using L1 approximation: buy at ask, sell at bid
    bid = Decimal("99")
    ask = Decimal("101")
    qty = Decimal("2")

    fee_rate = 0.00026
    net_fee_rate = fee_rate * 0.55

    wear = CoordinatorApp._estimate_backpack_wear(
        bid1=bid,
        ask1=ask,
        qty=qty,
        fee_rate=fee_rate,
        net_fee_rate=net_fee_rate,
    )

    assert Decimal(wear["buy_avg_est"]) == ask
    assert Decimal(wear["sell_avg_est"]) == bid

    # volume_quote_est = (ask + bid) * qty
    assert Decimal(wear["volume_quote_est"]) == (ask + bid) * qty

    # spread wear = (ask - bid) * qty
    assert Decimal(wear["wear_spread_est"]) == (ask - bid) * qty

    # fee_net = volume_quote * net_fee_rate
    assert Decimal(wear["fee_net_est"]) == Decimal(wear["volume_quote_est"]) * Decimal(str(net_fee_rate))

    # total wear = spread wear + fee_net
    assert Decimal(wear["wear_total_net_est"]) == Decimal(wear["wear_spread_est"]) + Decimal(wear["fee_net_est"]) 
