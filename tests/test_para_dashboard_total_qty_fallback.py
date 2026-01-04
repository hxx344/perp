def test_dashboard_does_not_fallback_to_request_magnitude_for_total_qty() -> None:
    """Guardrail: VPS detail should not show request.magnitude as total qty anymore.

    We can’t execute the JS here, but we can enforce the implementation does not
    pass `request.magnitude` into total-resolution helper.
    """

    path = "strategies/hedge_dashboard.html"
    with open(path, "r", encoding="utf-8") as handle:
        html = handle.read()

    assert "resolveReportedTotalQty(leftAgent, request.magnitude)" not in html
    assert "resolveReportedTotalQty(rightAgent, request.magnitude)" not in html
    assert "resolveReportedTotalQty(leftAgent)" in html
    assert "resolveReportedTotalQty(rightAgent)" in html
