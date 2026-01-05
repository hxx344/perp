from decimal import Decimal


def test_backpack_fee_rebate_math():
    fee_rate = Decimal("0.00026")
    rebate_rate = Decimal("0.45")
    net_fee_rate = fee_rate * (Decimal("1") - rebate_rate)
    assert net_fee_rate == Decimal("0.000143")


def test_backpack_wear_math():
    buy_avg = Decimal("2501.2")
    sell_avg = Decimal("2501.1")
    qty = Decimal("0.2")
    wear_spread = (buy_avg - sell_avg) * qty
    assert wear_spread == Decimal("0.02")

    volume_quote = buy_avg * qty + sell_avg * qty
    fee_rate = Decimal("0.00026")
    rebate_rate = Decimal("0.45")
    fee_gross = volume_quote * fee_rate
    fee_net = volume_quote * (fee_rate * (Decimal("1") - rebate_rate))

    assert fee_gross > fee_net
    wear_total_net = wear_spread + fee_net
    assert wear_total_net > wear_spread
