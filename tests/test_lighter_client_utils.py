from exchanges.lighter import LighterClient


def test_client_order_index_equals_accepts_string_input():
    assert LighterClient._client_order_index_equals("123", 123)


def test_client_order_index_equals_handles_whitespace_numbers():
    assert LighterClient._client_order_index_equals(" 0042 ", "42")


def test_client_order_index_equals_rejects_non_numeric():
    assert not LighterClient._client_order_index_equals("abc", 123)
