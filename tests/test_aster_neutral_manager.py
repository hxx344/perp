import time
from decimal import Decimal

import pytest

from strategies.aster_neutral_manager import (
    AsterAccountClient,
    AsterAccountSnapshot,
    AsterAccountSpec,
    AsterNeutralManager,
    AsterNeutralSettings,
    AsterPositionSnapshot,
    AsterTransferRejectedError,
    build_transfer_plan,
    settings_from_env,
)
from strategies.aster_neutral_manager import _AsterInstanceLock


def _settings(*, live=False):
    return AsterNeutralSettings(
        main=AsterAccountSpec("main", "main-key", "main-secret", "0x" + "1" * 40),
        sub=AsterAccountSpec("sub", "sub-key", "sub-secret", "0x" + "2" * 40),
        live=live,
    )


def _snapshot(name, available, withdrawable):
    return AsterAccountSnapshot(
        name=name,
        account_alias=name,
        available_balance=Decimal(str(available)),
        max_withdraw_amount=Decimal(str(withdrawable)),
        wallet_balance=Decimal("100"),
        equity=Decimal("100"),
        unrealized_pnl=Decimal("0"),
        initial_margin=Decimal("10"),
        maintenance_margin=Decimal("5"),
        can_trade=True,
        can_withdraw=True,
        position=AsterPositionSnapshot(
            symbol="XAUUSD1",
            signed_size=Decimal("1") if name == "main" else Decimal("-1"),
            position_value=Decimal("100"),
            entry_price=Decimal("100"),
            mark_price=Decimal("100"),
            unrealized_pnl=Decimal("0"),
            liquidation_price=Decimal("50"),
            leverage=Decimal("10"),
            isolated=False,
        ),
        observed_at=1.0,
    )


def test_aster_transfer_plan_uses_available_balance_and_withdrawable_cap():
    plan = build_transfer_plan(_snapshot("main", 200, 15), _snapshot("sub", 100, 100), _settings())

    assert plan is not None
    assert plan.source == "main"
    assert plan.destination == "sub"
    # Half of the 100 USD difference is 50, capped by Aster's official
    # maxWithdrawAmount of the source account.
    assert plan.amount == Decimal("15")


def test_aster_snapshot_returns_transfer_history_newest_first():
    settings = _settings()
    manager = AsterNeutralManager(settings)
    manager.transfer_history = [
        {"timestamp": 100, "plan": {"amount": "10"}},
        {"timestamp": 200, "plan": {"amount": "20"}},
        {"timestamp": 300, "plan": {"amount": "30"}},
    ]

    history = manager.snapshot_payload()["transfer_history"]

    assert [item["timestamp"] for item in history] == [300, 200, 100]


@pytest.mark.asyncio
async def test_explicit_transfer_rejection_clears_pending_lock(tmp_path):
    settings = _settings(live=True)
    settings.transfers_enabled = True
    settings.wallet_private_key = "4" * 64
    settings.state_path = str(tmp_path / "aster-state.json")
    manager = AsterNeutralManager(settings)
    manager.snapshots = {"main": _snapshot("main", 200, 100), "sub": _snapshot("sub", 100, 100)}
    for snapshot in manager.snapshots.values():
        snapshot.observed_at = time.time()
    manager._recovery_successes = settings.recovery_successes_required

    async def rejected(_plan):
        raise AsterTransferRejectedError("permission denied before submit")

    manager._submit_transfer = rejected  # type: ignore[method-assign]
    result = await manager.execute_transfer(request_id="reject-1")

    assert result["result"]["status"] == "rejected_before_submit"
    assert manager._pending_transfer is None
    assert manager._health()["state"] == "ready"


@pytest.mark.asyncio
async def test_pending_transfer_reconciles_matching_income_records():
    settings = _settings()
    manager = AsterNeutralManager(settings)
    manager._pending_transfer = {
        "timestamp": time.time(),
        "plan": {"source": "main", "destination": "sub", "amount": "25"},
    }

    class FakeClient:
        def __init__(self, rows):
            self.rows = rows

        async def request(self, _method, _path, _params):
            return self.rows

    manager._clients = {
        "main": FakeClient([{"tranId": "tx-1", "income": "-25"}]),
        "sub": FakeClient([{"tranId": "tx-1", "income": "25"}]),
    }

    assert await manager._reconcile_pending_transfer() is True
    assert manager._pending_transfer is None
    assert manager.last_transfer["result"]["tran_id"] == "tx-1"


@pytest.mark.asyncio
async def test_pending_transfer_reconciles_matching_balance_deltas():
    settings = _settings()
    manager = AsterNeutralManager(settings)
    manager._pending_transfer = {
        "timestamp": time.time(),
        "plan": {"source": "main", "destination": "sub", "amount": "25"},
        "before_available": {"main": "200", "sub": "100"},
    }
    manager.snapshots = {
        "main": _snapshot("main", 175, 100),
        "sub": _snapshot("sub", 125, 100),
    }

    class EmptyIncomeClient:
        async def request(self, _method, _path, _params):
            return []

    manager._clients = {"main": EmptyIncomeClient(), "sub": EmptyIncomeClient()}

    assert await manager._reconcile_pending_transfer() is True
    assert manager._pending_transfer is None
    assert manager.last_transfer["result"]["confirmation"] == "matching_balance_deltas"


def test_aster_snapshot_exposes_transfer_delta_and_threshold():
    settings = _settings()
    settings.transfer_hysteresis = Decimal("25")
    manager = AsterNeutralManager(settings)
    manager.snapshots = {
        "main": _snapshot("main", 140, 100),
        "sub": _snapshot("sub", 100, 100),
    }

    aggregate = manager.snapshot_payload()["aggregate"]

    assert aggregate["available_balance_delta"] == "40"
    assert aggregate["available_balance_delta_abs"] == "40"
    assert aggregate["transfer_hysteresis"] == "25"
    assert aggregate["transfer_trigger_threshold"] == "25"


def test_aster_transfer_trigger_threshold_includes_minimum_transfer():
    settings = _settings()
    settings.transfer_hysteresis = Decimal("1")
    settings.min_transfer = Decimal("10")
    manager = AsterNeutralManager(settings)
    manager.snapshots = {
        "main": _snapshot("main", 140, 100),
        "sub": _snapshot("sub", 100, 100),
    }

    aggregate = manager.snapshot_payload()["aggregate"]

    assert aggregate["transfer_trigger_threshold"] == "20"


def test_aster_transfer_plan_supports_reverse_direction():
    plan = build_transfer_plan(_snapshot("main", 100, 100), _snapshot("sub", 200, 30), _settings())

    assert plan is not None
    assert (plan.source, plan.destination) == ("sub", "main")
    assert plan.amount == Decimal("30")


def test_aster_transfer_plan_requires_opposite_xau_position_signs():
    main = _snapshot("main", 200, 100)
    sub = _snapshot("sub", 100, 100)
    sub.position.signed_size = Decimal("1")

    assert build_transfer_plan(main, sub, _settings()) is None


def test_aster_settings_reject_live_transfers_without_wallet_signer():
    settings = _settings(live=True)
    settings.wallet_address = ""
    settings.main = AsterAccountSpec("main", settings.main.api_key, settings.main.api_secret)
    with pytest.raises(ValueError, match="master wallet address"):
        settings.validate()


def test_aster_settings_rejects_malformed_live_wallet_credentials():
    settings = _settings(live=True)
    settings.wallet_address = "0xnot-an-address"
    settings.wallet_private_key = "deadbeef"
    with pytest.raises(ValueError, match="wallet address"):
        settings.validate()


def test_aster_settings_accepts_approved_agent_signer_without_master_key():
    settings = _settings(live=True)
    settings.wallet_address = "0x" + "1" * 40
    settings.transfer_signer_address = "0x" + "3" * 40
    settings.transfer_signer_private_key = "4" * 64

    settings.validate()


def test_aster_main_pro_signer_is_valid_transfer_signer_by_default():
    settings = AsterNeutralSettings(
        main=AsterAccountSpec(
            "main", user_address="0x" + "1" * 40,
            signer_address="0x" + "3" * 40, signer_private_key="4" * 64,
        ),
        sub=AsterAccountSpec(
            "sub", user_address="0x" + "2" * 40,
            signer_address="0x" + "5" * 40, signer_private_key="6" * 64,
        ),
        live=True,
        transfers_enabled=True,
        wallet_address="0x" + "1" * 40,
    )

    settings.validate()
    assert settings.effective_transfer_signer_address == settings.main.signer_address
    assert settings.effective_transfer_signer_private_key == settings.main.signer_private_key


def test_aster_settings_rejects_insecure_feishu_webhook():
    settings = _settings()
    settings.feishu_webhook_url = "http://example.test/hook"
    with pytest.raises(ValueError, match="HTTPS"):
        settings.validate()


@pytest.mark.asyncio
async def test_aster_feishu_alert_is_gated_by_balance_delta_threshold():
    settings = _settings()
    settings.feishu_webhook_url = "https://open.feishu.cn/open-apis/bot/v2/hook/test"
    settings.alert_threshold = Decimal("50")
    manager = AsterNeutralManager(settings)
    manager.snapshots = {"main": _snapshot("main", 120, 100), "sub": _snapshot("sub", 100, 100)}

    class FakeSession:
        def __init__(self):
            self.calls = 0

        def post(self, *args, **kwargs):
            self.calls += 1
            raise AssertionError("Feishu must not be called below threshold")

    session = FakeSession()
    manager._session = session
    await manager._maybe_feishu()
    assert session.calls == 0


def test_aster_hmac_params_have_signature_without_leaking_secret():
    settings = _settings()
    client = AsterAccountClient(settings.main, settings, session=None)  # type: ignore[arg-type]
    params = client._signed_params({"symbol": "XAUUSD1"})

    assert params["symbol"] == "XAUUSD1"
    assert len(params["signature"]) == 64
    assert settings.main.api_secret not in str(params)


def test_aster_pro_api_settings_accept_user_signer_credentials():
    settings = AsterNeutralSettings(
        main=AsterAccountSpec(
            "main", user_address="0x" + "1" * 40,
            signer_address="0x" + "3" * 40, signer_private_key="4" * 64,
        ),
        sub=AsterAccountSpec(
            "sub", user_address="0x" + "2" * 40,
            signer_address="0x" + "5" * 40, signer_private_key="6" * 64,
        ),
    )

    settings.validate()
    assert settings.main.uses_pro_api is True
    assert settings.sub.uses_pro_api is True


def test_aster_pro_api_signature_includes_exact_auth_fields():
    settings = AsterNeutralSettings(
        main=AsterAccountSpec(
            "main", user_address="0x" + "1" * 40,
            signer_address="0x" + "3" * 40, signer_private_key="4" * 64,
        ),
        sub=AsterAccountSpec(
            "sub", user_address="0x" + "2" * 40,
            signer_address="0x" + "5" * 40, signer_private_key="6" * 64,
        ),
    )
    client = AsterAccountClient(settings.main, settings, session=None)  # type: ignore[arg-type]

    params = client._pro_signed_params({"symbol": "XAUUSD1"})
    names = [name for name, _value in params]

    assert names[:1] == ["symbol"]
    assert "nonce" in names and "user" in names and "signer" in names
    assert names[-1] == "signature"
    assert len(params[-1][1]) == 130


def test_aster_env_requires_explicit_three_switches_for_auto_transfer(monkeypatch):
    values = {
        "ASTER_NEUTRAL_MAIN_USER_ADDRESS": "0x" + "1" * 40,
        "ASTER_NEUTRAL_MAIN_SIGNER_ADDRESS": "0x" + "3" * 40,
        "ASTER_NEUTRAL_MAIN_SIGNER_PRIVATE_KEY": "4" * 64,
        "ASTER_NEUTRAL_SUB_USER_ADDRESS": "0x" + "2" * 40,
        "ASTER_NEUTRAL_SUB_SIGNER_ADDRESS": "0x" + "5" * 40,
        "ASTER_NEUTRAL_SUB_SIGNER_PRIVATE_KEY": "6" * 64,
        "ASTER_NEUTRAL_MASTER_WALLET_ADDRESS": "0x" + "1" * 40,
        "ASTER_NEUTRAL_SUB_WALLET_ADDRESS": "0x" + "2" * 40,
        "ASTER_NEUTRAL_TRANSFER_SIGNER_ADDRESS": "0x" + "3" * 40,
        "ASTER_NEUTRAL_TRANSFER_SIGNER_PRIVATE_KEY": "4" * 64,
        "ASTER_NEUTRAL_LIVE": "true",
        "ASTER_NEUTRAL_ENABLE_TRANSFERS": "true",
        "ASTER_NEUTRAL_AUTO_TRANSFER": "true",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)

    settings = settings_from_env()

    assert settings.live is True
    assert settings.auto_transfer is True
    assert settings.transfers_enabled is True


def test_aster_instance_lock_blocks_duplicate_manager(tmp_path):
    path = tmp_path / "aster-neutral.lock"
    first = _AsterInstanceLock(path)
    second = _AsterInstanceLock(path)
    first.acquire()
    try:
        with pytest.raises(RuntimeError, match="another Aster neutral manager"):
            second.acquire()
    finally:
        first.release()
    second.acquire()
    second.release()


@pytest.mark.asyncio
async def test_aster_account_snapshot_parses_v4_balance_and_position():
    settings = _settings()
    client = AsterAccountClient(settings.main, settings, session=None)  # type: ignore[arg-type]

    async def request(_method, _path, _params=None):
        return {
            "accountAlias": "main",
            "availableBalance": "80",
            "maxWithdrawAmount": "70",
            "totalWalletBalance": "100",
            "totalUnrealizedProfit": "5",
            "totalMarginBalance": "105",
            "totalInitialMargin": "20",
            "totalMaintMargin": "12",
            "canTrade": "true",
            "canWithdraw": True,
            "assets": [{
                "asset": "USD1",
                "availableBalance": "80",
                "maxWithdrawAmount": "70",
                "walletBalance": "100",
                "unrealizedProfit": "5",
                "marginBalance": "105",
                "initialMargin": "20",
                "maintMargin": "12",
            }],
            "positions": [{
                "symbol": "XAUUSD1",
                "positionAmt": "3",
                "entryPrice": "100",
                "markPrice": "101",
                "unRealizedProfit": "3",
                "liquidationPrice": "80",
                "leverage": "10",
                "marginType": "crossed",
            }],
        }

    client.request = request  # type: ignore[method-assign]
    snapshot = await client.fetch_snapshot()

    assert snapshot.available_balance == Decimal("80")
    assert snapshot.max_withdraw_amount == Decimal("70")
    assert snapshot.equity == Decimal("105")
    assert snapshot.position is not None
    assert snapshot.position.signed_size == Decimal("3")
    assert snapshot.position.position_value == Decimal("303")


@pytest.mark.asyncio
async def test_aster_live_snapshot_rejects_missing_account_capability_fields():
    settings = _settings(live=True)
    client = AsterAccountClient(settings.main, settings, session=None)  # type: ignore[arg-type]

    async def request(_method, _path, _params=None):
        return {
            "accountAlias": "main",
            "availableBalance": "80",
            "maxWithdrawAmount": "70",
            "totalWalletBalance": "100",
            "totalUnrealizedProfit": "0",
            "totalMarginBalance": "100",
            "totalInitialMargin": "20",
            "totalMaintMargin": "12",
            "assets": [{
                "asset": "USD1",
                "availableBalance": "80",
                "maxWithdrawAmount": "70",
                "walletBalance": "100",
                "unrealizedProfit": "0",
                "marginBalance": "100",
                "initialMargin": "20",
                "maintMargin": "12",
            }],
            "positions": [],
        }

    client.request = request  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="canTrade/canWithdraw"):
        await client.fetch_snapshot()


@pytest.mark.asyncio
async def test_aster_position_risk_fallback_fills_missing_notional_and_liquidation_price():
    settings = _settings()
    client = AsterAccountClient(settings.main, settings, session=None)  # type: ignore[arg-type]

    async def request(_method, path, _params=None):
        if path.endswith("positionRisk"):
            return [{
                "symbol": "XAUUSD1",
                "positionAmt": "-2",
                "markPrice": "110",
                "entryPrice": "100",
                "liquidationPrice": "150",
                "unRealizedProfit": "-20",
                "leverage": "10",
            }]
        return {
            "accountAlias": "sub",
            "availableBalance": "80",
            "maxWithdrawAmount": "70",
            "totalWalletBalance": "100",
            "totalUnrealizedProfit": "-20",
            "totalMarginBalance": "80",
            "totalInitialMargin": "20",
            "totalMaintMargin": "12",
            "positions": [{"symbol": "XAUUSD1", "positionAmt": "-2"}],
        }

    client.request = request  # type: ignore[method-assign]
    snapshot = await client.fetch_snapshot()

    assert snapshot.position is not None
    assert snapshot.position.position_value == Decimal("220")
    assert snapshot.position.liquidation_price == Decimal("150")
