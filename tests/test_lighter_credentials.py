import pytest

from exchanges.lighter import LighterClient


class DummyConfig(dict):
    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError as exc:
            raise AttributeError(item) from exc

    def __setattr__(self, key, value):
        self[key] = value


def _make_client():
    return LighterClient(DummyConfig(ticker="TEST", contract_id="1"))


def test_parse_api_private_key_spec_handles_json_object():
    payload = '{"0":"0xaaa","5":"0xbbb"}'

    result = LighterClient._parse_api_private_key_spec(payload)

    assert result == {0: "0xaaa", 5: "0xbbb"}


def test_parse_api_private_key_spec_handles_list_of_dicts():
    payload = (
        '[{"index":1,"privateKey":"0x111"},'
        '{"apiKeyIndex":3,"key":"0x333"}]'
    )

    result = LighterClient._parse_api_private_key_spec(payload)

    assert result == {1: "0x111", 3: "0x333"}


def test_parse_api_private_key_spec_handles_delimited_pairs():
    payload = "0:0xaaa;2:0xbbb;4:0xccc"

    result = LighterClient._parse_api_private_key_spec(payload)

    assert result == {0: "0xaaa", 2: "0xbbb", 4: "0xccc"}


def test_load_api_private_keys_prefers_multi_key_env(monkeypatch):
    monkeypatch.setenv("LIGHTER_API_PRIVATE_KEYS", '{"0":"0xabc"}')
    monkeypatch.delenv("API_KEY_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("LIGHTER_API_KEY_INDEX", raising=False)
    monkeypatch.setenv("LIGHTER_ACCOUNT_INDEX", "0")

    client = _make_client()

    assert client.api_private_keys == {0: "0xabc"}


def test_load_api_private_keys_falls_back_to_legacy_fields(monkeypatch):
    monkeypatch.delenv("LIGHTER_API_PRIVATE_KEYS", raising=False)
    monkeypatch.setenv("API_KEY_PRIVATE_KEY", "0xlegacy")
    monkeypatch.setenv("LIGHTER_API_KEY_INDEX", "4")
    monkeypatch.setenv("LIGHTER_ACCOUNT_INDEX", "0")

    client = _make_client()

    assert client.api_private_keys == {4: "0xlegacy"}
