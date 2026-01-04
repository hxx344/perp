from __future__ import annotations


def test_dashboard_contains_wear_total_row() -> None:
    """Guardrail: ensure the dashboard renders wear summary text for PARA VPS detail."""

    path = r"d:\project8\perp-dex-tools\strategies\hedge_dashboard.html"
    with open(path, "r", encoding="utf-8") as f:
        html = f.read()

    # We don't assert exact layout, only that the new label exists.
    assert "磨损" in html
    # Frontend-only wear computation should be based on avg_price/filled_qty.
    assert "avg_price" in html
    assert "filled_qty" in html

    # Guardrail: should NOT depend on backend wear_* fields.
    assert "wear_value_usd" not in html
    assert "wear_notional_usd" not in html
