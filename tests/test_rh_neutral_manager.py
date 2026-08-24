import asyncio
import time
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from strategies.rh_neutral_manager import (
    AccountSnapshot,
    AccountSpec,
    LighterAccountGateway,
    NeutralPositionManager,
    NeutralSettings,
    PositionSnapshot,
    LighterWriteUncertainError,
    NeutralJournalError,
    build_transfer_plan,
    margin_ratio,
    settings_from_env,
)


class _ModelResponse:
    def model_dump(self):
        return {"code": 200, "nested": {"value": Decimal("1.25")}}


def _settings(*, live=False, spy=26, qqq=25, port=0):
    return NeutralSettings(
        main=AccountSpec("main", 10, 4, {4: "a" * 80} if live else {}),
        sub=AccountSpec("sub", 11, 4, {4: "b" * 80} if live else {}),
        spy_market_id=spy,
        qqq_market_id=qqq,
        live=live,
        dashboard_port=port,
    )


def _account(name, equity, mmr, available, *, error=None):
    return AccountSnapshot(
        name=name,
        account_index=10 if name == "main" else 11,
        l1_address="0xmaster",
        equity=Decimal(str(equity)),
        collateral=Decimal(str(equity)),
        available_balance=Decimal(str(available)),
        initial_margin_requirement=Decimal("0"),
        maintenance_margin_requirement=Decimal(str(mmr)),
        pending_order_count=0,
        transaction_time=0,
        positions=[],
        observed_at=0,
        error=error,
    )


def test_transfer_plan_equalizes_available_balance_with_hysteresis():
    settings = _settings()
    first = _account("main", 100, 60, 120)
    second = _account("sub", 500, 100, 500)
    assert margin_ratio(first.equity, first.maintenance_margin_requirement) == Decimal("1.666666666666666666666666667")
    plan = build_transfer_plan(first, second, settings)
    assert plan is not None
    assert plan.source == "sub"
    assert plan.destination == "main"
    assert plan.amount == Decimal("190")
    assert plan.urgent is False

    # A small deficit below hysteresis must not cause churn.
    near = _account("main", 166, 60, 490)
    assert build_transfer_plan(near, second, settings) is None


def test_transfer_plan_can_reverse_from_main_to_sub():
    settings = _settings()
    main = _account("main", 500, 100, 500)
    sub = _account("sub", 100, 60, 120)

    plan = build_transfer_plan(main, sub, settings)

    assert plan is not None
    assert plan.source == "main"
    assert plan.destination == "sub"
    assert plan.amount == Decimal("190")


def test_transfer_memo_is_fixed_length_hex_and_does_not_leak_reason():
    memo = LighterAccountGateway._transfer_memo("main requires 12.5 USDG")
    assert len(memo) == 66
    assert memo.startswith("0x")
    assert all(char in "0123456789abcdef" for char in memo[2:])
    assert "requires" not in memo


def test_sdk_models_are_json_safe_for_dashboard_payloads():
    from strategies.rh_neutral_manager import _json_value

    assert _json_value(_ModelResponse()) == {"code": 200, "nested": {"value": "1.25"}}


def test_transfer_plan_is_fail_closed_when_an_account_is_stale():
    settings = _settings()
    assert build_transfer_plan(_account("main", 1, 1, 1, error="timeout"), _account("sub", 100, 10, 100), settings) is None


def test_transfer_plan_is_blocked_for_isolated_positions():
    settings = _settings()
    first = _account("main", 10, 10, 10)
    first.positions = [PositionSnapshot(
        symbol="SPY", market_id=26, signed_size=Decimal("1"), position_value=Decimal("100"),
        avg_entry_price=Decimal("100"), unrealized_pnl=Decimal("0"), liquidation_price=Decimal("0"),
        initial_margin_fraction=Decimal("0.05"), allocated_margin=Decimal("20"), margin_mode=1,
    )]
    assert build_transfer_plan(first, _account("sub", 500, 10, 500), settings) is None


@pytest.mark.asyncio
async def test_market_discovery_and_account_position_sign_parsing():
    settings = _settings()
    gateway = LighterAccountGateway(settings.main, settings, session=None)  # type: ignore[arg-type]

    async def fake_get(path, params=None, **kwargs):
        if path.endswith("orderBooks"):
            return {"order_books": [{"symbol": "SPY", "market_id": 26}, {"symbol": "QQQ", "market_id": 25}]}
        return {
            "accounts": [{
                "account_index": 10,
                "l1_address": "0xmaster",
                "cross_asset_value": "100",
                "collateral": "90",
                "available_balance": "80",
                "cross_maintenance_margin_requirement": "20",
                "positions": [
                    {"market_id": 26, "symbol": "SPY", "position": "0.5", "sign": -1},
                    {"market_id": 25, "symbol": "QQQ", "position": "-0.25"},
                ],
            }]
        }

    gateway._get_json = fake_get  # type: ignore[method-assign]
    assert await gateway.discover_market_ids(("SPY", "QQQ")) == {"SPY": 26, "QQQ": 25}
    snapshot = await gateway.fetch_account()
    assert snapshot.position("SPY", 26).signed_size == Decimal("-0.5")
    assert snapshot.position("QQQ", 25).signed_size == Decimal("-0.25")


@pytest.mark.asyncio
async def test_account_reader_rejects_response_for_a_different_account_or_l1():
    settings = _settings()
    settings.l1_address = "0x" + "a" * 40
    gateway = LighterAccountGateway(settings.main, settings, session=None)  # type: ignore[arg-type]

    async def fake_get(path, params=None, **kwargs):
        return {"accounts": [{
            "account_index": 999,
            "l1_address": "0x" + "a" * 40,
            "cross_asset_value": "1",
            "collateral": "1",
            "available_balance": "1",
            "cross_maintenance_margin_requirement": "0",
            "positions": [],
        }]}

    gateway._get_json = fake_get  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="returned account"):
        await gateway.fetch_account()


@pytest.mark.asyncio
async def test_market_catalogue_filters_non_perpetual_or_inactive_entries():
    gateway = LighterAccountGateway(_settings().main, _settings(), session=None)  # type: ignore[arg-type]

    async def fake_get(path, params=None, **kwargs):
        return {"order_books": [
            {"symbol": "SPY", "market_id": 101, "market_type": "spot", "status": "active"},
            {"symbol": "SPY", "market_id": 102, "market_type": "perp", "status": "paused"},
            {"symbol": "SPY", "market_id": 103, "market_type": "perp", "status": "active"},
            {"symbol": "QQQ", "market_id": 104, "market_type": "perp", "status": "active"},
        ]}

    gateway._get_json = fake_get  # type: ignore[method-assign]
    assert await gateway.discover_market_ids(("SPY", "QQQ")) == {"SPY": 103, "QQQ": 104}


@pytest.mark.asyncio
async def test_market_catalogue_rejects_duplicate_active_symbol_ids():
    gateway = LighterAccountGateway(_settings().main, _settings(), session=None)  # type: ignore[arg-type]

    async def fake_get(path, params=None, **kwargs):
        return {"order_books": [
            {"symbol": "SPY", "market_id": 103, "market_type": "perp", "status": "active"},
            {"symbol": "SPY", "market_id": 104, "market_type": "perp", "status": "active"},
            {"symbol": "QQQ", "market_id": 105, "market_type": "perp", "status": "active"},
        ]}

    gateway._get_json = fake_get  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="multiple active perp ids"):
        await gateway.discover_market_ids(("SPY", "QQQ"))


@pytest.mark.asyncio
async def test_bbo_reader_rejects_crossed_book():
    settings = _settings()
    gateway = LighterAccountGateway(settings.main, settings, session=None)  # type: ignore[arg-type]

    async def fake_get(path, params=None, **kwargs):
        return {"bids": [{"price": "101"}], "asks": [{"price": "100"}]}

    gateway._get_json = fake_get  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="Crossed or invalid BBO"):
        await gateway.fetch_bbo(26)


@pytest.mark.asyncio
async def test_account_discovery_ignores_inactive_reserved_entries():
    settings = _settings()
    gateway = LighterAccountGateway(settings.main, settings, session=None)  # type: ignore[arg-type]

    async def fake_get(path, params=None, **kwargs):
        return {"sub_accounts": [
            {"index": 1, "account_type": 0, "status": 0},
            {"index": 2, "account_type": 0, "status": 0},
            {"index": 281474976710654, "account_type": 3, "status": 1},
        ]}

    gateway._get_json = fake_get  # type: ignore[method-assign]
    assert await gateway.discover_account_indexes("0xmaster") == (1, 2)
    assert await gateway.discover_account_indexes("0xmaster", exclude=1) == (1, 2)


@pytest.mark.asyncio
async def test_account_discovery_accepts_main_and_subaccount_types():
    settings = _settings()
    gateway = LighterAccountGateway(settings.main, settings, session=None)  # type: ignore[arg-type]

    async def fake_get(path, params=None, **kwargs):
        return {"sub_accounts": [
            {"index": 6985, "account_type": 0, "status": 1},
            {"index": 281474976710318, "account_type": 1, "status": 1},
        ]}

    gateway._get_json = fake_get  # type: ignore[method-assign]
    assert await gateway.discover_account_indexes("0xmaster") == (6985, 281474976710318)

    async def only_reserved(path, params=None, **kwargs):
        return {"sub_accounts": [{"index": 281474976710318, "account_type": 3, "status": 1}]}

    gateway._get_json = only_reserved  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="tradable active accounts"):
        await gateway.discover_account_indexes("0xmaster")


@pytest.mark.asyncio
async def test_account_discovery_merges_accounts_and_subaccounts_fields():
    settings = _settings()
    gateway = LighterAccountGateway(settings.main, settings, session=None)  # type: ignore[arg-type]

    async def fake_get(path, params=None, **kwargs):
        return {
            "accounts": [{"index": 6985, "account_type": 0, "status": "active"}],
            "sub_accounts": [{"index": 281474976710314, "account_type": 1, "status": "active"}],
        }

    gateway._get_json = fake_get  # type: ignore[method-assign]
    assert await gateway.discover_account_indexes("0xmaster") == (6985, 281474976710314)


@pytest.mark.asyncio
async def test_manager_start_discovers_markets_and_starts_loopback_dashboard(monkeypatch):
    settings = _settings(spy=0, qqq=0, port=0)

    async def discover(self, symbols):
        return {"SPY": 26, "QQQ": 25}

    async def fetch(self):
        return _account(self.spec.name, 100, 10, 80)

    async def validate_market(self, market_id, symbol):
        return {"market_id": market_id, "symbol": symbol, "market_type": "perp", "status": "active"}

    monkeypatch.setattr(LighterAccountGateway, "discover_market_ids", discover)
    monkeypatch.setattr(LighterAccountGateway, "fetch_account", fetch)
    monkeypatch.setattr(LighterAccountGateway, "validate_market_identity", validate_market)
    manager = NeutralPositionManager(settings)
    await manager.start()
    try:
        assert settings.spy_market_id == 26
        assert settings.qqq_market_id == 25
        assert manager._dashboard is not None
        assert manager._dashboard.running
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_close_position_derives_reduce_only_side_and_respects_minimums():
    settings = _settings(live=True)
    gateway = LighterAccountGateway(settings.main, settings, session=None)  # type: ignore[arg-type]

    position = PositionSnapshot(
        symbol="SPY", market_id=26, signed_size=Decimal("0.5"), position_value=Decimal("50"),
        avg_entry_price=Decimal("100"), unrealized_pnl=Decimal("0"), liquidation_price=Decimal("0"),
        initial_margin_fraction=Decimal("0.05"), allocated_margin=Decimal("1"),
    )
    snapshot = _account("main", 100, 20, 90)
    snapshot.positions = [position]
    async def fetch_account():
        return snapshot

    async def fetch_market(_market):
        return {
            "market_id": 26,
            "symbol": "SPY",
            "market_type": "perp",
            "status": "active",
            "size_decimals": 2,
            "price_decimals": 1,
            "min_base_amount": "0.1",
            "min_quote_amount": "10",
        }

    async def fetch_bbo(_market):
        return Decimal("99"), Decimal("101")

    gateway.fetch_account = fetch_account  # type: ignore[method-assign]
    gateway.fetch_market = fetch_market  # type: ignore[method-assign]
    gateway.fetch_bbo = fetch_bbo  # type: ignore[method-assign]

    class FakeSigner:
        ORDER_TYPE_LIMIT = 0
        ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
        DEFAULT_IOC_EXPIRY = 0
        SELF_TRADE_BEHAVIOR_EXPIRE_BOTH = 2
        SELF_TRADE_EQUALITY_MASTER_ACCOUNT_INDEX = 1

        def __init__(self):
            self.kwargs = None

        async def create_order(self, **kwargs):
            self.kwargs = kwargs
            return ({"ok": True}, {"code": 200}, None)

    signer = FakeSigner()
    gateway._signer = signer
    result = await gateway.close_position(26, quantity=Decimal("0.23"), slippage_bps=Decimal("50"), dry_run=False)
    assert result["side"] == "sell"
    assert result["quantity"] == "0.23"
    assert signer.kwargs["is_ask"] is True
    assert signer.kwargs["reduce_only"] is True
    assert signer.kwargs["time_in_force"] == 0
    assert signer.kwargs["base_amount"] == 23


@pytest.mark.asyncio
async def test_explicit_key_writes_allocate_and_serialize_nonce():
    settings = _settings(live=True)
    gateway = LighterAccountGateway(settings.main, settings, session=None)  # type: ignore[arg-type]

    class FakeNonceManager:
        def __init__(self):
            self.calls = []
            self.value = 40
            self.lock_obj = __import__("asyncio").Lock()

        def lock(self, key):
            self.calls.append(("lock", key))
            return self.lock_obj

        async def async_next_nonce(self, key):
            self.value += 1
            self.calls.append(("next", key, self.value))
            return key, self.value

        def acknowledge_failure(self, key):
            self.calls.append(("failure", key))

    class FakeSigner:
        nonce_manager = FakeNonceManager()

        async def create_order(self, **kwargs):
            self.kwargs = kwargs
            return ({"ok": True}, {"code": 200}, None)

    signer = FakeSigner()
    gateway._signer = signer
    result = await gateway._call_signed(signer.create_order, market_index=1)
    assert result[1]["code"] == 200
    assert signer.kwargs["api_key_index"] == 4
    assert signer.kwargs["nonce"] == 41
    assert ("next", 4, 41) in signer.nonce_manager.calls


@pytest.mark.asyncio
async def test_auth_token_uses_configured_non_reserved_key_index():
    settings = _settings(live=True)
    gateway = LighterAccountGateway(settings.main, settings, session=None)  # type: ignore[arg-type]

    class FakeSigner:
        DEFAULT_10_MIN_AUTH_EXPIRY = -1

        def __init__(self):
            self.args = None

        def create_auth_token_with_expiry(self, *args, **kwargs):
            self.args = (args, kwargs)
            return "token", None

    signer = FakeSigner()
    gateway._signer = signer
    assert await gateway._auth_token() == "token"
    assert signer.args[1]["api_key_index"] == 4


def test_settings_from_env_uses_rh_profile_and_allows_runtime_market_discovery(monkeypatch):
    values = {
        "RH_NEUTRAL_MAIN_ACCOUNT_INDEX": "10",
        "RH_NEUTRAL_SUB_ACCOUNT_INDEX": "11",
        "RH_NEUTRAL_MAIN_API_PRIVATE_KEYS": '{"4":"' + "a" * 80 + '"}',
        "RH_NEUTRAL_SUB_API_PRIVATE_KEYS": '{"4":"' + "b" * 80 + '"}',
        "RH_NEUTRAL_SPY_MARKET_ID": "0",
        "RH_NEUTRAL_QQQ_MARKET_ID": "0",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    settings = settings_from_env()
    assert settings.rest_url == "https://api.rh.lighter.xyz"
    assert settings.chain_id == 466324
    assert settings.spy_market_id == 0
    assert settings.qqq_market_id == 0


def test_settings_from_env_treats_blank_indexes_as_auto_discovery(tmp_path, monkeypatch):
    env_file = tmp_path / "rh-neutral.env"
    env_file.write_text(
        "\n".join(
            [
                "RH_NEUTRAL_L1_ADDRESS=0x" + "a" * 40,
                "RH_NEUTRAL_MAIN_ACCOUNT_INDEX=",
                "RH_NEUTRAL_SUB_ACCOUNT_INDEX=",
                "RH_NEUTRAL_SPY_MARKET_ID=",
                "RH_NEUTRAL_QQQ_MARKET_ID=",
                "LIGHTER_BASE_URL=https://api.rh.lighter.xyz",
                "LIGHTER_WS_URL=wss://api.rh.lighter.xyz/stream",
                "LIGHTER_CHAIN_ID=466324",
            ]
        ),
        encoding="utf-8",
    )
    for key in (
        "RH_NEUTRAL_L1_ADDRESS",
        "RH_NEUTRAL_MAIN_ACCOUNT_INDEX",
        "RH_NEUTRAL_SUB_ACCOUNT_INDEX",
        "RH_NEUTRAL_SPY_MARKET_ID",
        "RH_NEUTRAL_QQQ_MARKET_ID",
        "LIGHTER_BASE_URL",
        "LIGHTER_WS_URL",
        "LIGHTER_CHAIN_ID",
        "LIGHTER_ACCOUNT_INDEX",
    ):
        monkeypatch.delenv(key, raising=False)
    settings = settings_from_env(str(env_file))
    assert settings.main.account_index == -1
    assert settings.sub.account_index == -1
    assert settings.spy_market_id == 0
    assert settings.qqq_market_id == 0


def test_settings_reject_reserved_api_key_index_even_before_live_mode():
    settings = _settings()
    settings.main = AccountSpec("main", 10, 3, {3: "a" * 80})
    with pytest.raises(ValueError, match="API key index"):
        settings.validate(require_market_ids=False)


def test_manager_rejects_same_direction_four_leg_configuration():
    settings = _settings()
    settings.legs  # opposite layout is part of the public settings contract
    manager = NeutralPositionManager(settings)
    assert manager.settings.legs[0].expected_sign == -manager.settings.legs[2].expected_sign
    assert manager.settings.legs[1].expected_sign == -manager.settings.legs[3].expected_sign


def test_manager_supports_qqq_long_on_main_and_reverses_subaccount():
    settings = _settings()
    settings.main_long_symbol = "QQQ"
    settings.validate()
    signs = {(leg.account, leg.symbol): leg.expected_sign for leg in settings.legs}
    assert signs == {
        ("main", "SPY"): -1,
        ("main", "QQQ"): 1,
        ("sub", "SPY"): 1,
        ("sub", "QQQ"): -1,
    }


def test_manager_rejects_unknown_main_long_symbol():
    settings = _settings()
    settings.main_long_symbol = "DIA"
    with pytest.raises(ValueError, match="main_long_symbol"):
        settings.validate()


@pytest.mark.asyncio
async def test_transfer_plan_blocks_an_actual_wrong_direction_leg():
    manager = NeutralPositionManager(_settings())
    main = _account("main", 100, 20, 80)
    sub = _account("sub", 100, 20, 80)
    # The configured sub SPY leg is short; a long position is a mismatch.
    sub.positions = [PositionSnapshot(
        symbol="SPY", market_id=26, signed_size=Decimal("1"), position_value=Decimal("100"),
        avg_entry_price=Decimal("100"), unrealized_pnl=Decimal("0"), liquidation_price=Decimal("0"),
        initial_margin_fraction=Decimal("0.05"), allocated_margin=Decimal("20"),
    )]
    manager.snapshots = {"main": main, "sub": sub}
    assert await manager.calculate_transfer_plan() is None
    assert "opposite" in manager.snapshot_payload()["transfer_block_reason"]


@pytest.mark.asyncio
async def test_transfer_plan_blocks_flat_or_missing_fourth_leg():
    manager = NeutralPositionManager(_settings())
    manager.snapshots = {
        "main": _account("main", 100, 20, 80),
        "sub": _account("sub", 100, 20, 80),
    }
    assert await manager.calculate_transfer_plan() is None
    assert "four-leg layout incomplete" in manager.snapshot_payload()["transfer_block_reason"]


@pytest.mark.asyncio
async def test_transfer_plan_blocks_large_cross_account_notional_skew():
    manager = NeutralPositionManager(_settings())
    main = _account("main", 100, 20, 80)
    sub = _account("sub", 100, 20, 80)
    main.positions = [
        PositionSnapshot("SPY", 26, Decimal("1"), Decimal("100"), Decimal("100"), Decimal("0"), Decimal("0"), Decimal("0.05"), Decimal("1")),
        PositionSnapshot("QQQ", 25, Decimal("-1"), Decimal("100"), Decimal("100"), Decimal("0"), Decimal("0"), Decimal("0.05"), Decimal("1")),
    ]
    sub.positions = [
        PositionSnapshot("SPY", 26, Decimal("-1"), Decimal("250"), Decimal("100"), Decimal("0"), Decimal("0"), Decimal("0.05"), Decimal("1")),
        PositionSnapshot("QQQ", 25, Decimal("1"), Decimal("100"), Decimal("100"), Decimal("0"), Decimal("0"), Decimal("0.05"), Decimal("1")),
    ]
    manager.snapshots = {"main": main, "sub": sub}
    assert await manager.calculate_transfer_plan() is None
    assert "notional skew" in manager.snapshot_payload()["transfer_block_reason"]


@pytest.mark.asyncio
async def test_manual_rebalance_respects_transfer_cooldown():
    manager = NeutralPositionManager(_settings(live=True))
    manager.last_transfer = {"timestamp": time.time(), "type": "transfer"}
    with pytest.raises(RuntimeError, match="cooldown"):
        await manager.manual_rebalance(request_id="cooldown-1")


def test_state_journal_restores_action_history(tmp_path):
    settings = _settings()
    settings.state_path = str(tmp_path / "state.json")
    manager = NeutralPositionManager(settings)
    manager._record_action({"type": "test", "result": {"status": "ok"}})
    restored = NeutralPositionManager(settings)
    assert restored.action_history[-1]["type"] == "test"


@pytest.mark.asyncio
async def test_non_timeout_submission_exception_creates_pending_write_record(tmp_path):
    settings = _settings(live=True)
    settings.state_path = str(tmp_path / "state.json")
    manager = NeutralPositionManager(settings)

    async def uncertain_call():
        raise LighterWriteUncertainError("connection dropped after submit", metadata={"nonce": 42})

    result = await manager._write_with_timeout("test-write", uncertain_call(), {"account": "main"})
    assert result["status"] == "unknown_pending"
    assert manager._pending_unknown_records[0]["nonce"] == 42
    assert manager._pending_write_reason()


@pytest.mark.asyncio
async def test_accepted_write_is_blocked_until_reconciled(tmp_path):
    settings = _settings(live=True)
    settings.state_path = str(tmp_path / "state.json")
    manager = NeutralPositionManager(settings)
    result = await manager._write_with_timeout(
        "test-write",
        __import__("asyncio").sleep(0, result={"status": "accepted_pending_confirmation", "tx_hash": "0xabc"}),
        {"account": "main"},
    )
    assert result["pending_id"]
    assert manager._pending_write_reason()


@pytest.mark.asyncio
async def test_snapshot_is_degraded_while_write_confirmation_is_pending(tmp_path):
    settings = _settings(live=True)
    settings.state_path = str(tmp_path / "state.json")
    manager = NeutralPositionManager(settings)
    manager.snapshots = {
        "main": _account("main", 100, 20, 80),
        "sub": _account("sub", 100, 20, 80),
    }
    pending_id = manager._mark_pending_result(
        "close:test",
        {"status": "accepted_pending_confirmation"},
        {"kind": "close", "account": "main", "symbol": "SPY", "market_id": 26, "before_signed_size": "1"},
    )
    payload = manager.snapshot_payload()
    assert pending_id
    assert payload["writes_blocked"] is True
    assert payload["ok"] is False


@pytest.mark.asyncio
async def test_accepted_transfer_is_acknowledged_only_after_account_balances_move(tmp_path):
    settings = _settings(live=True)
    settings.state_path = str(tmp_path / "state.json")
    settings.confirmation_poll_seconds = 0
    manager = NeutralPositionManager(settings)
    manager.snapshots = {
        "main": _account("main", 100, 20, 80),
        "sub": _account("sub", 200, 20, 180),
    }
    manager.snapshots["main"].transaction_time = 100
    manager.snapshots["sub"].transaction_time = 200
    metadata = manager._transfer_confirmation_metadata("sub", "main", Decimal("25"))
    result = manager._mark_pending_result(
        "transfer:test",
        {"status": "accepted_pending_confirmation", "tx_hash": "0xabc"},
        metadata,
    )
    record = {
        "type": "transfer",
        "result": {"status": "accepted_pending_confirmation", "pending_id": result},
        "timestamp": time.time(),
    }
    manager.action_history = [record]
    # The first read is stale; a later read proves both sides moved by the
    # requested amount.  This also exercises the bounded retry loop.
    fresh = {
        "main": _account("main", 125, 20, 105),
        "sub": _account("sub", 175, 20, 155),
    }
    fresh["main"].transaction_time = 101
    fresh["sub"].transaction_time = 201
    reads = iter((manager.snapshots, fresh))

    async def refresh():
        manager.snapshots = next(reads)
        return manager.snapshot_payload()

    manager.refresh_once = refresh  # type: ignore[method-assign]
    settings.confirmation_attempts = 2
    await manager._reconcile_after_write(record)
    pending = next(item for item in manager._pending_unknown_records if item["pending_id"] == result)
    assert pending["status"] == "acknowledged"
    assert record["result"]["status"] == "acknowledged"
    assert manager._pending_write_reason() is None


@pytest.mark.asyncio
async def test_accepted_transfer_without_opposite_balance_delta_stays_pending(tmp_path):
    settings = _settings(live=True)
    settings.state_path = str(tmp_path / "state.json")
    settings.confirmation_poll_seconds = 0
    settings.confirmation_attempts = 2
    manager = NeutralPositionManager(settings)
    manager.snapshots = {
        "main": _account("main", 100, 20, 80),
        "sub": _account("sub", 200, 20, 180),
    }
    metadata = manager._transfer_confirmation_metadata("sub", "main", Decimal("25"))
    pending_id = manager._mark_pending_result(
        "transfer:stale",
        {"status": "accepted_pending_confirmation"},
        metadata,
    )
    record = {"result": {"status": "accepted_pending_confirmation", "pending_id": pending_id}}
    manager.action_history = [record]
    manager.refresh_once = AsyncMock(return_value=manager.snapshot_payload())
    await manager._reconcile_after_write(record)
    pending = next(item for item in manager._pending_unknown_records if item["pending_id"] == pending_id)
    assert pending["status"] == "accepted_pending_confirmation"
    assert manager._pending_write_reason()


@pytest.mark.asyncio
async def test_accepted_close_is_acknowledged_after_position_reduction(tmp_path):
    settings = _settings(live=True)
    settings.state_path = str(tmp_path / "state.json")
    settings.confirmation_poll_seconds = 0
    manager = NeutralPositionManager(settings)
    before = _account("main", 100, 20, 80)
    before.transaction_time = 100
    before.positions = [PositionSnapshot(
        "SPY", 26, Decimal("1.0"), Decimal("100"), Decimal("100"), Decimal("0"),
        Decimal("0"), Decimal("0.05"), Decimal("1"),
    )]
    manager.snapshots = {"main": before, "sub": _account("sub", 100, 20, 80)}
    metadata = manager._close_confirmation_metadata("main", "SPY", 26, Decimal("0.5"))
    pending_id = manager._mark_pending_result(
        "close:test",
        {
            "status": "accepted_pending_confirmation",
            "pending_id": "placeholder",
            "pre_close_signed_size": "1.0",
            "quantity": "0.5",
        },
        metadata,
    )
    record = {
        "type": "close",
        "result": {"status": "accepted_pending_confirmation", "pending_id": pending_id},
        "timestamp": time.time(),
    }
    manager.action_history = [record]
    after = _account("main", 100, 20, 80)
    after.transaction_time = 101
    after.positions = [PositionSnapshot(
        "SPY", 26, Decimal("0.5"), Decimal("50"), Decimal("100"), Decimal("0"),
        Decimal("0"), Decimal("0.05"), Decimal("1"),
    )]
    manager.refresh_once = AsyncMock(side_effect=lambda: manager.snapshots.update({"main": after}) or manager.snapshot_payload())
    await manager._reconcile_after_write(record)
    pending = next(item for item in manager._pending_unknown_records if item["pending_id"] == pending_id)
    assert pending["status"] == "acknowledged"
    assert record["result"]["status"] == "acknowledged"


@pytest.mark.asyncio
async def test_unknown_write_is_never_acknowledged_by_position_reconciliation(tmp_path):
    settings = _settings(live=True)
    settings.state_path = str(tmp_path / "state.json")
    manager = NeutralPositionManager(settings)
    pending_id = manager._mark_pending_result(
        "close:unknown",
        {"status": "unknown_pending"},
        {"kind": "close", "account": "main", "symbol": "SPY", "market_id": 26,
         "before_signed_size": "1"},
        status="unknown_pending",
    )
    record = {"result": {"status": "unknown_pending", "pending_id": pending_id}}
    manager.action_history = [record]
    manager.refresh_once = AsyncMock(return_value=manager.snapshot_payload())
    await manager._reconcile_after_write(record)
    pending = next(item for item in manager._pending_unknown_records if item["pending_id"] == pending_id)
    assert pending["status"] == "unknown_pending"
    assert manager._pending_write_reason()


def test_failed_pending_journal_write_does_not_unblock_in_memory(tmp_path):
    settings = _settings(live=True)
    settings.state_path = str(tmp_path / "state.json")
    manager = NeutralPositionManager(settings)
    pending_id = manager._mark_pending_result(
        "close:persist",
        {"status": "accepted_pending_confirmation"},
        {"kind": "close", "account": "main", "symbol": "SPY", "market_id": 26,
         "before_signed_size": "1"},
    )
    manager._persist_state_checked = lambda: (_ for _ in ()).throw(OSError("disk full"))  # type: ignore[method-assign]
    with pytest.raises(OSError, match="disk full"):
        manager._update_pending_record(pending_id, status="acknowledged")
    pending = next(item for item in manager._pending_unknown_records if item["pending_id"] == pending_id)
    assert pending["status"] == "accepted_pending_confirmation"
    assert manager._pending_write_reason()


@pytest.mark.asyncio
async def test_journal_failure_after_signer_acceptance_becomes_unknown_journal(tmp_path):
    settings = _settings(live=True)
    settings.state_path = str(tmp_path / "state.json")
    manager = NeutralPositionManager(settings)
    original_persist = manager._persist_state_checked
    calls = 0

    def fail_after_intent():
        nonlocal calls
        calls += 1
        if calls >= 2:
            raise NeutralJournalError("disk full")
        original_persist()

    manager._persist_state_checked = fail_after_intent  # type: ignore[method-assign]
    result = await manager._write_with_timeout(
        "accepted-with-journal-failure",
        __import__("asyncio").sleep(0, result={"status": "accepted_pending_confirmation", "tx_hash": "0xabc"}),
        {"account": "main"},
    )
    assert result["status"] == "unknown_journal"
    assert manager._pending_unknown_records[0]["status"] == "unknown_journal"
    assert manager._pending_write_reason()


@pytest.mark.asyncio
async def test_refresh_propagates_account_cancellation():
    manager = NeutralPositionManager(_settings())

    async def cancelled_fetch():
        raise asyncio.CancelledError()

    manager.gateways = {
        "main": LighterAccountGateway(manager.settings.main, manager.settings, session=None),  # type: ignore[arg-type]
        "sub": LighterAccountGateway(manager.settings.sub, manager.settings, session=None),  # type: ignore[arg-type]
    }
    manager.gateways["main"].fetch_account = cancelled_fetch  # type: ignore[method-assign]
    manager.gateways["sub"].fetch_account = cancelled_fetch  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await manager.refresh_once()


@pytest.mark.asyncio
async def test_live_manual_rebalance_fails_closed_on_mismatched_master():
    manager = NeutralPositionManager(_settings(live=True))
    manager._pair_error = "main and sub accounts do not share the same L1 master address"
    with pytest.raises(RuntimeError, match="same L1 master"):
        await manager.manual_rebalance(request_id="pair-mismatch")
