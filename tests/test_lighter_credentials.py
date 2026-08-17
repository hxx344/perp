import asyncio

import pytest

import exchanges.lighter as lighter_module
from exchanges.lighter import LighterClient
from exchanges.lighter_endpoints import resolve_lighter_endpoint_profile


CORE_REST_URL = "https://mainnet.zklighter.elliot.ai"
CORE_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
ROBINHOOD_REST_URL = "https://api.rh.lighter.xyz"
ROBINHOOD_WS_URL = "wss://api.rh.lighter.xyz/stream"


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


def _run_async(coro):
    try:
        previous_loop = asyncio.get_event_loop()
    except RuntimeError:
        previous_loop = None

    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        loop.close()
        asyncio.set_event_loop(previous_loop)


@pytest.fixture
def lighter_credentials(monkeypatch):
    monkeypatch.setenv("LIGHTER_ACCOUNT_INDEX", "7")
    monkeypatch.setenv("LIGHTER_API_PRIVATE_KEYS", '{"2":"0xtest"}')
    monkeypatch.delenv("API_KEY_PRIVATE_KEYS", raising=False)
    monkeypatch.delenv("API_KEY_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("LIGHTER_API_KEY_INDEX", raising=False)
    monkeypatch.delenv("LIGHTER_BASE_URL", raising=False)
    monkeypatch.delenv("LIGHTER_WS_URL", raising=False)
    monkeypatch.delenv("LIGHTER_CHAIN_ID", raising=False)
    monkeypatch.delenv("LIGHTER_ENVIRONMENT", raising=False)
    monkeypatch.delenv("LIGHTER_ENDPOINT_PROFILE", raising=False)


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


@pytest.mark.parametrize("alias", ["robinhood-mainnet", "robinhood", "rh"])
def test_robinhood_profile_aliases_resolve_as_one_atomic_endpoint_set(alias):
    profile = resolve_lighter_endpoint_profile(alias)

    assert profile.name == "robinhood"
    assert profile.rest_url == ROBINHOOD_REST_URL
    assert profile.ws_url == ROBINHOOD_WS_URL
    assert profile.chain_id == 466324
    assert profile.supports_l1_auto_provision is False


@pytest.mark.parametrize("alias", [None, "core"])
def test_core_profile_remains_the_default_and_backwards_compatible(alias):
    profile = resolve_lighter_endpoint_profile(alias)

    assert profile.name == "core"
    assert profile.rest_url == CORE_REST_URL
    assert profile.ws_url == CORE_WS_URL
    assert profile.chain_id == 304
    assert profile.supports_l1_auto_provision is True


def test_matching_robinhood_overrides_are_accepted_and_normalized():
    profile = resolve_lighter_endpoint_profile(
        "robinhood",
        rest_url=f"{ROBINHOOD_REST_URL}/",
        ws_url=ROBINHOOD_WS_URL,
        chain_id="466324",
    )

    assert profile.name == "robinhood"
    assert profile.rest_url == ROBINHOOD_REST_URL
    assert profile.ws_url == ROBINHOOD_WS_URL
    assert profile.chain_id == 466324


@pytest.mark.parametrize(
    "override",
    [
        {"rest_url": CORE_REST_URL},
        {"ws_url": CORE_WS_URL},
        {"chain_id": 304},
    ],
)
def test_robinhood_profile_rejects_cross_network_overrides(override):
    with pytest.raises(ValueError) as exc_info:
        resolve_lighter_endpoint_profile("robinhood", **override)

    assert "robinhood" in str(exc_info.value).lower()


def test_known_robinhood_rest_url_can_select_the_profile_without_a_name():
    profile = resolve_lighter_endpoint_profile(rest_url=f"{ROBINHOOD_REST_URL}/")

    assert profile.name == "robinhood"
    assert profile.rest_url == ROBINHOOD_REST_URL
    assert profile.ws_url == ROBINHOOD_WS_URL
    assert profile.chain_id == 466324


@pytest.mark.parametrize(
    ("rest_url", "ws_url"),
    [
        ("http://api.rh.lighter.xyz", None),
        (f"{ROBINHOOD_REST_URL}/v1", None),
        (None, "ws://api.rh.lighter.xyz/stream"),
        (None, f"{ROBINHOOD_WS_URL}?source=test"),
    ],
)
def test_robinhood_profile_rejects_noncanonical_or_insecure_urls(
    rest_url,
    ws_url,
):
    with pytest.raises(ValueError):
        resolve_lighter_endpoint_profile(
            "robinhood",
            rest_url=rest_url,
            ws_url=ws_url,
        )


def test_lighter_client_exposes_the_resolved_robinhood_endpoint_set(
    lighter_credentials,
):
    client = LighterClient(
        DummyConfig(
            ticker="BTC",
            contract_id="1",
            lighter_environment="robinhood",
        )
    )

    assert client.endpoint_profile.name == "robinhood"
    assert client.base_url == ROBINHOOD_REST_URL
    assert client.ws_url == ROBINHOOD_WS_URL
    assert client.chain_id == 466324


def test_lighter_client_rejects_mixed_profile_before_connecting(
    lighter_credentials,
):
    with pytest.raises(ValueError):
        LighterClient(
            DummyConfig(
                ticker="BTC",
                contract_id="1",
                lighter_environment="robinhood",
                lighter_ws_url=CORE_WS_URL,
            )
        )


def test_lighter_client_keeps_core_mainnet_as_its_default(lighter_credentials):
    client = LighterClient(DummyConfig(ticker="BTC", contract_id="1"))

    assert client.endpoint_profile.name == "core"
    assert client.base_url == CORE_REST_URL
    assert client.ws_url == CORE_WS_URL
    assert client.chain_id == 304


def test_lighter_signer_receives_robinhood_chain_id_explicitly(
    monkeypatch,
    lighter_credentials,
):
    captured = {}

    class _SignerClient:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        @staticmethod
        def check_client():
            return None

    monkeypatch.setattr(lighter_module, "SignerClient", _SignerClient)
    client = LighterClient(
        DummyConfig(
            ticker="BTC",
            contract_id="1",
            lighter_environment="robinhood",
        )
    )

    _run_async(client._initialize_lighter_client())

    assert captured == {
        "url": ROBINHOOD_REST_URL,
        "account_index": 7,
        "api_private_keys": {2: "0xtest"},
        "chain_id": 466324,
    }


def test_lighter_signer_receives_core_chain_id_explicitly(
    monkeypatch,
    lighter_credentials,
):
    captured = {}

    class _SignerClient:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        @staticmethod
        def check_client():
            return None

    monkeypatch.setattr(lighter_module, "SignerClient", _SignerClient)
    client = LighterClient(DummyConfig(ticker="BTC", contract_id="1"))

    _run_async(client._initialize_lighter_client())

    assert captured["url"] == CORE_REST_URL
    assert captured["chain_id"] == 304
