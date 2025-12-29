import math


def _compute_reduce_suggestion_worst_account(
    *,
    accounts,
    ff_symbol: str = "FF-USD",
):
    """Reference implementation of the dashboard reduce-suggestion math.

    This is intentionally a pure function so we can regression-test the agreed
    contract without needing a JS runtime.

    Contract (1.B 2.A):
    - Exactly 2 accounts.
    - worst account by nonFF total (total - ff_pnl)
    - L = abs(nonFFTotalWorst) if worst is losing else 0
    - allocate L across symbols by negative pnl abs
    - reduceSize = absPosSize * (allocation / abs(symbolNegPnl))
    - headline: choose the symbol with max suggested size
    """
    assert len(accounts) == 2

    def pack(acct):
        total_raw = float(acct.get("total", 0.0))
        ff_pnl = float(acct.get("ff_pnl", 0.0))
        non_ff_total = total_raw - ff_pnl
        symbol_pnl = dict(acct.get("symbol_pnl", {}))
        symbol_cap = dict(acct.get("symbol_cap", {}))
        name = acct.get("name", "Account")
        return {
            "name": name,
            "total_raw": total_raw,
            "ff_pnl": ff_pnl,
            "non_ff_total": non_ff_total,
            "symbol_pnl": symbol_pnl,
            "symbol_cap": symbol_cap,
        }

    a = pack(accounts[0])
    b = pack(accounts[1])
    worst = a if a["non_ff_total"] <= b["non_ff_total"] else b

    if worst["non_ff_total"] >= 0:
        return {"ok": True, "message": "无需减仓"}

    L = abs(worst["non_ff_total"])

    allowed = {"BTC", "ETH"}
    rows = []
    for sym, pnl in worst["symbol_pnl"].items():
        if sym == ff_symbol:
            continue
        if sym not in allowed:
            continue
        pnl = float(pnl)
        if pnl >= 0:
            continue
        cap = float(worst["symbol_cap"].get(sym, 0.0))
        if cap <= 0:
            continue
        rows.append({"symbol": sym, "neg_abs": abs(pnl), "cap": cap})

    if not rows:
        return {"ok": False, "message": "no candidates"}

    total_neg_abs = sum(r["neg_abs"] for r in rows)
    assert total_neg_abs > 0

    computed = []
    for r in rows:
        allocation = L * r["neg_abs"] / total_neg_abs
        ratio = allocation / r["neg_abs"]
        size = r["cap"] * ratio
        size = min(max(size, 0.0), r["cap"])
        computed.append({**r, "allocation": allocation, "size": size})

    computed.sort(key=lambda x: x["size"], reverse=True)
    best = computed[0]
    return {"ok": True, "symbol": best["symbol"], "size": best["size"]}


def test_para_reduce_suggestion_example_btc_6():
    # User authoritative example:
    # worst total=-5000, FF=+1000 => nonFF=-6000
    # BTC pnl=-10000 cap=10, ETH pnl=+4000 (not candidate)
    # L=6000, totalNegAbs=10000
    # size=10*(6000/10000)=6
    res = _compute_reduce_suggestion_worst_account(
        accounts=[
            {
                "name": "worst",
                "total": -5000,
                "ff_pnl": 1000,
                "symbol_pnl": {"BTC": -10000, "ETH": 4000},
                "symbol_cap": {"BTC": 10, "ETH": 10},
            },
            {
                "name": "other",
                "total": 0,
                "ff_pnl": 0,
                "symbol_pnl": {"BTC": 0, "ETH": 0},
                "symbol_cap": {"BTC": 0, "ETH": 0},
            },
        ]
    )
    assert res["ok"] is True
    assert res["symbol"] == "BTC"
    assert math.isclose(res["size"], 6.0, rel_tol=0, abs_tol=1e-9)


def test_para_reduce_suggestion_no_loss_means_no_reduce():
    res = _compute_reduce_suggestion_worst_account(
        accounts=[
            {
                "name": "a",
                "total": 100,
                "ff_pnl": 0,
                "symbol_pnl": {"BTC": -50},
                "symbol_cap": {"BTC": 10},
            },
            {"name": "b", "total": 0, "ff_pnl": 0, "symbol_pnl": {}, "symbol_cap": {}},
        ]
    )
    assert res["ok"] is True
    assert res["message"] == "无需减仓"


def test_para_reduce_suggestion_ignores_non_btc_eth_candidates():
    # When only other symbols are losing, dashboard should not propose a reduce.
    # (We only consider BTC/ETH.)
    res = _compute_reduce_suggestion_worst_account(
        accounts=[
            {
                "name": "worst",
                "total": -5000,
                "ff_pnl": 0,
                "symbol_pnl": {"SOL": -10000},
                "symbol_cap": {"SOL": 10},
            },
            {"name": "other", "total": 0, "ff_pnl": 0, "symbol_pnl": {}, "symbol_cap": {}},
        ]
    )
    # Reference impl returns no candidates; UI should show cannot compute.
    assert res["ok"] is False
