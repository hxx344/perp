import asyncio
import builtins
import signal
import subprocess
import sys
import textwrap
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest

import strategies.aster_lighter_cycle as strategy_module
from exchanges.lighter_endpoints import resolve_lighter_endpoint_profile
from strategies.aster_lighter_cycle import (
    CycleConfig,
    HedgingCycleExecutor,
    LighterProvisioningError,
    _auto_provision_lighter_credentials,
    _lighter_credentials_present,
)


class _FakeSignerClient:
    CROSS_MARGIN_MODE = 7

    def __init__(self, *, error=None):
        self.calls = []
        self._error = error

    async def update_leverage(self, market_index: int, margin_mode: int, leverage: int):
        self.calls.append((market_index, margin_mode, leverage))
        return ("0xtx", None, self._error)


class _FakeLighterClient(SimpleNamespace):
    lighter_client: _FakeSignerClient
    config: SimpleNamespace


def _make_executor(
    signer: _FakeSignerClient,
    *,
    lighter_environment: str = "core",
    supports_l1_auto_provision: bool = True,
) -> HedgingCycleExecutor:
    config = CycleConfig(
        aster_ticker="ETH",
        lighter_ticker="ETH-PERP",
        quantity=Decimal("1"),
        aster_quantity=Decimal("1"),
        lighter_quantity=Decimal("1"),
        direction="buy",
        take_profit_pct=Decimal("0"),
        slippage_pct=Decimal("0.1"),
        max_wait_seconds=0.0,
        lighter_max_wait_seconds=0.0,
        poll_interval=0.0,
        max_retries=1,
        retry_delay_seconds=0.0,
        max_cycles=1,
        delay_between_cycles=0.0,
        virtual_aster_maker=False,
        lighter_environment=lighter_environment,
        lighter_supports_l1_auto_provision=supports_l1_auto_provision,
    )
    executor = HedgingCycleExecutor(config)
    executor.lighter_client = cast(
        Any,
        _FakeLighterClient(
            lighter_client=signer,
            config=SimpleNamespace(contract_id="123"),
        ),
    )
    return executor


def test_ensure_lighter_leverage_invokes_signer_update():
    signer = _FakeSignerClient()
    executor = _make_executor(signer)

    asyncio.run(executor._ensure_lighter_leverage(50))

    assert signer.calls == [(123, signer.CROSS_MARGIN_MODE, 50)]


def test_ensure_lighter_leverage_swallows_already_set_error():
    signer = _FakeSignerClient(error=RuntimeError("leverage already the same"))
    executor = _make_executor(signer)

    asyncio.run(executor._ensure_lighter_leverage(50))

    assert signer.calls == [(123, signer.CROSS_MARGIN_MODE, 50)]


def test_ensure_lighter_leverage_raises_on_other_error():
    signer = _FakeSignerClient(error=RuntimeError("something else failed"))
    executor = _make_executor(signer)

    with pytest.raises(RuntimeError):
        asyncio.run(executor._ensure_lighter_leverage(25))

    assert signer.calls == [(123, signer.CROSS_MARGIN_MODE, 25)]


def test_ensure_lighter_leverage_rejects_runtime_market_maximum():
    signer = _FakeSignerClient()
    executor = _make_executor(signer)
    executor.lighter_client.get_leverage_limits = lambda: {"default": 10, "max": 50}

    with pytest.raises(ValueError, match="exceeds market maximum 50x"):
        asyncio.run(executor._ensure_lighter_leverage(51))

    assert signer.calls == []


def test_robinhood_missing_credentials_fail_without_auto_provisioning(
    monkeypatch,
):
    for key in (
        "LIGHTER_ACCOUNT_INDEX",
        "LIGHTER_API_PRIVATE_KEYS",
        "API_KEY_PRIVATE_KEY",
        "LIGHTER_API_KEY_INDEX",
    ):
        monkeypatch.delenv(key, raising=False)

    request_intent = AsyncMock(side_effect=AssertionError("must not provision on Robinhood"))
    transfer_funds = AsyncMock(side_effect=AssertionError("must not transfer on Robinhood"))
    monkeypatch.setattr(strategy_module, "_request_lighter_intent_address", request_intent)
    monkeypatch.setattr(strategy_module, "_transfer_full_usdc_balance", transfer_funds)

    with pytest.raises(LighterProvisioningError, match="pre-created API credentials"):
        asyncio.run(
            _auto_provision_lighter_credentials(
                Path(".env-unused-by-test"),
                resolve_lighter_endpoint_profile("robinhood"),
            )
        )

    request_intent.assert_not_awaited()
    transfer_funds.assert_not_awaited()


def test_robinhood_multi_key_credentials_count_as_present(monkeypatch):
    monkeypatch.setenv("LIGHTER_ACCOUNT_INDEX", "7")
    monkeypatch.setenv(
        "LIGHTER_API_PRIVATE_KEYS",
        '{"2":"0xfirst","9":"0xsecond"}',
    )
    monkeypatch.delenv("API_KEY_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("LIGHTER_API_KEY_INDEX", raising=False)

    assert _lighter_credentials_present() is True
    asyncio.run(
        _auto_provision_lighter_credentials(
            Path(".env-unused-by-test"),
            resolve_lighter_endpoint_profile("robinhood"),
        )
    )


def test_legacy_multi_key_alias_counts_as_present(monkeypatch):
    monkeypatch.setenv("LIGHTER_ACCOUNT_INDEX", "7")
    monkeypatch.delenv("LIGHTER_API_PRIVATE_KEYS", raising=False)
    monkeypatch.setenv("API_KEY_PRIVATE_KEYS", '{"4":"0xsecret"}')
    monkeypatch.delenv("API_KEY_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("LIGHTER_API_KEY_INDEX", raising=False)

    assert _lighter_credentials_present() is True


def test_robinhood_l1_top_up_path_is_a_no_op(monkeypatch):
    signer = _FakeSignerClient()
    executor = _make_executor(
        signer,
        lighter_environment="robinhood",
        supports_l1_auto_provision=False,
    )
    resolve_address = AsyncMock(side_effect=AssertionError("must not inspect L1 on Robinhood"))
    executor._resolve_lighter_l1_address = resolve_address
    monkeypatch.setenv("ARBITRUM_RPC_URL", "https://arbitrum.invalid")
    monkeypatch.setenv("L1_WALLET_PRIVATE_KEY", "0xnot-used")

    asyncio.run(executor.ensure_l1_top_up_if_needed())

    resolve_address.assert_not_awaited()


def test_robinhood_l1_private_key_flag_fails_before_credentials_change(monkeypatch, tmp_path):
    monkeypatch.delenv("LIGHTER_ENVIRONMENT", raising=False)
    monkeypatch.delenv("LIGHTER_ENDPOINT_PROFILE", raising=False)
    env_path = tmp_path / ".env"
    original = (
        "LIGHTER_ENVIRONMENT=robinhood\n"
        "LIGHTER_ACCOUNT_INDEX=7\n"
        'LIGHTER_API_PRIVATE_KEYS={"4":"0xsecret"}\n'
    )
    env_path.write_text(original, encoding="utf-8")
    clear_credentials = Mock(side_effect=AssertionError("credentials must not be changed"))
    monkeypatch.setattr(strategy_module, "_clear_env_credentials", clear_credentials)
    monkeypatch.setattr(strategy_module, "_configure_logging", lambda _level: None)
    monkeypatch.setattr(
        strategy_module,
        "_parse_args",
        lambda: SimpleNamespace(
            log_level="INFO",
            env_file=str(env_path),
            reset_credentials=False,
            l1_private_key="0xdeadbeef",
            lighter_environment=None,
        ),
    )

    with pytest.raises(SystemExit) as exc_info:
        strategy_module.main()

    assert exc_info.value.code == 2
    clear_credentials.assert_not_called()
    assert env_path.read_text(encoding="utf-8") == original


def _lifecycle_args(monkeypatch, tmp_path):
    env_path = tmp_path / ".env.robinhood"
    env_path.write_text("LIGHTER_ENVIRONMENT=robinhood\n", encoding="utf-8")
    monkeypatch.setattr(strategy_module.dotenv, "load_dotenv", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        strategy_module,
        "_auto_provision_lighter_credentials",
        AsyncMock(return_value=None),
    )
    monkeypatch.setenv("LIGHTER_ENVIRONMENT", "robinhood")
    monkeypatch.setenv("LIGHTER_BASE_URL", "https://api.rh.lighter.xyz")
    monkeypatch.setenv("LIGHTER_WS_URL", "wss://api.rh.lighter.xyz/stream")
    monkeypatch.setenv("LIGHTER_CHAIN_ID", "466324")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "aster_lighter_cycle",
            "--aster-ticker",
            "BTC",
            "--lighter-ticker",
            "BTC",
            "--quantity",
            "0.0002",
            "--env-file",
            str(env_path),
            "--lighter-environment",
            "robinhood",
            "--virtual-aster-maker",
            "--virtual-maker-price-source",
            "bn",
            "--disable-min-cycle-interval",
        ],
    )
    return strategy_module._parse_args()


class _LifecycleLogger:
    def log(self, _message, _level="INFO"):
        return None


def test_sigterm_runs_async_main_inventory_recovery_and_shutdown(monkeypatch, tmp_path):
    args = _lifecycle_args(monkeypatch, tmp_path)
    relay = {}
    instances = []

    class FakeExecutor:
        def __init__(self, config):
            self.config = config
            self.logger = _LifecycleLogger()
            self.lighter_client = None
            self._lighter_recovery_blocked = False
            self._lighter_recovery_block_reason = None
            self.flat_calls = 0
            self.shutdown_called = False
            instances.append(self)

        async def setup(self):
            return None

        async def report_metrics(self, **_kwargs):
            return None

        async def wait_for_resume(self, _context):
            return None

        async def ensure_l1_top_up_if_needed(self):
            return None

        async def execute_cycle(self):
            relay["handler"]._request_shutdown(int(signal.SIGTERM))
            await asyncio.sleep(60)

        async def ensure_lighter_flat(self):
            self.flat_calls += 1

        async def shutdown(self):
            self.shutdown_called = True

    def capture_signal_relay(self):
        relay["handler"] = self

    monkeypatch.setattr(strategy_module, "HedgingCycleExecutor", FakeExecutor)
    monkeypatch.setattr(strategy_module._GracefulSignalRelay, "install", capture_signal_relay)

    exit_code = asyncio.run(strategy_module._run_with_graceful_signals(args))

    assert exit_code == strategy_module.EXIT_OK
    assert len(instances) == 1
    assert instances[0].flat_calls == 1
    assert instances[0].shutdown_called is True


def test_three_network_failures_return_restartable_exit_code(monkeypatch, tmp_path):
    args = _lifecycle_args(monkeypatch, tmp_path)
    instances = []

    class FakeExecutor:
        def __init__(self, config):
            self.config = config
            self.logger = _LifecycleLogger()
            self.lighter_client = None
            self._lighter_recovery_blocked = False
            self._lighter_recovery_block_reason = None
            self.execute_calls = 0
            instances.append(self)

        async def setup(self):
            return None

        async def report_metrics(self, **_kwargs):
            return None

        async def wait_for_resume(self, _context):
            return None

        async def ensure_l1_top_up_if_needed(self):
            return None

        async def execute_cycle(self):
            self.execute_calls += 1
            raise ConnectionError("temporary test outage")

        async def ensure_lighter_flat(self):
            return None

        async def shutdown(self):
            return None

    async def no_delay(_seconds):
        return None

    monkeypatch.setattr(strategy_module, "HedgingCycleExecutor", FakeExecutor)
    monkeypatch.setattr(strategy_module.asyncio, "sleep", no_delay)

    exit_code = asyncio.run(strategy_module._async_main(args))

    assert exit_code == strategy_module.EXIT_TEMPORARY_NETWORK_FAILURE
    assert instances[0].execute_calls == 3


@pytest.mark.parametrize(
    ("failure", "expected_exit"),
    [
        (strategy_module.SkipCycleError("IOC did not complete"), strategy_module.EXIT_RUNTIME_ERROR),
        (ConnectionError("temporary test outage"), strategy_module.EXIT_TEMPORARY_NETWORK_FAILURE),
    ],
)
def test_finite_canary_stops_after_one_failed_attempt(
    monkeypatch,
    tmp_path,
    failure,
    expected_exit,
):
    args = _lifecycle_args(monkeypatch, tmp_path)
    args.cycles = 1
    instances = []

    class FakeExecutor:
        def __init__(self, config):
            self.config = config
            self.logger = _LifecycleLogger()
            self.lighter_client = None
            self._lighter_recovery_blocked = False
            self._lighter_recovery_block_reason = None
            self.execute_calls = 0
            instances.append(self)

        async def setup(self):
            return None

        async def report_metrics(self, **_kwargs):
            return None

        async def wait_for_resume(self, _context):
            return None

        async def ensure_l1_top_up_if_needed(self):
            return None

        async def execute_cycle(self):
            self.execute_calls += 1
            raise failure

        async def ensure_lighter_flat(self):
            return None

        async def shutdown(self):
            return None

    monkeypatch.setattr(strategy_module, "HedgingCycleExecutor", FakeExecutor)

    exit_code = asyncio.run(strategy_module._async_main(args))

    assert exit_code == expected_exit
    assert instances[0].execute_calls == 1


def test_inventory_recovery_block_uses_non_restartable_exit(monkeypatch, tmp_path):
    args = _lifecycle_args(monkeypatch, tmp_path)

    class FakeExecutor:
        def __init__(self, config):
            self.config = config
            self.logger = _LifecycleLogger()
            self.lighter_client = None
            self._lighter_recovery_blocked = True
            self._lighter_recovery_block_reason = "residual below minimum"

        async def setup(self):
            return None

        async def report_metrics(self, **_kwargs):
            return None

        async def ensure_lighter_flat(self):
            return None

        async def shutdown(self):
            return None

    monkeypatch.setattr(strategy_module, "HedgingCycleExecutor", FakeExecutor)

    with pytest.raises(strategy_module.InventoryRecoveryBlockedError, match="residual below minimum"):
        asyncio.run(strategy_module._async_main(args))


def test_shutdown_recovery_exception_uses_non_restartable_exit(monkeypatch, tmp_path):
    args = _lifecycle_args(monkeypatch, tmp_path)
    args.cycles = 1

    class FakeExecutor:
        def __init__(self, config):
            self.config = config
            self.logger = _LifecycleLogger()
            self.lighter_client = None
            self._lighter_recovery_blocked = False
            self._lighter_recovery_block_reason = None

        async def setup(self):
            return None

        async def report_metrics(self, **_kwargs):
            return None

        async def wait_for_resume(self, _context):
            return None

        async def ensure_l1_top_up_if_needed(self):
            return None

        async def execute_cycle(self):
            return []

        async def _log_leaderboard_points(self, _cycle_number):
            return None

        async def ensure_lighter_flat(self):
            raise RuntimeError("position API unavailable")

        async def shutdown(self):
            return None

    monkeypatch.setattr(strategy_module, "HedgingCycleExecutor", FakeExecutor)
    monkeypatch.setattr(strategy_module, "_print_summary", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(strategy_module, "_calculate_cycle_pnl", lambda _results: Decimal("0"))
    monkeypatch.setattr(strategy_module, "_calculate_cycle_volume", lambda _results: Decimal("0"))
    monkeypatch.setattr(strategy_module, "_print_pnl_progress", lambda *_args, **_kwargs: None)

    with pytest.raises(
        strategy_module.InventoryRecoveryBlockedError,
        match="Shutdown inventory recovery raised RuntimeError",
    ):
        asyncio.run(strategy_module._async_main(args))


def test_single_instance_lock_rejects_duplicate_and_releases(tmp_path):
    lock_path = tmp_path / "strategy.lock"
    first = strategy_module._SingleInstanceLock(lock_path)
    second = strategy_module._SingleInstanceLock(lock_path)

    first.acquire()
    try:
        with pytest.raises(strategy_module.SingleInstanceError):
            second.acquire()
    finally:
        first.release()

    second.acquire()
    second.release()


def test_duplicate_instance_fails_before_async_strategy_start(monkeypatch, tmp_path):
    env_path = tmp_path / ".env.robinhood"
    env_path.write_text(
        "LIGHTER_ENVIRONMENT=robinhood\nLIGHTER_ACCOUNT_INDEX=7\n",
        encoding="utf-8",
    )
    args = SimpleNamespace(
        log_level="INFO",
        env_file=str(env_path),
        reset_credentials=False,
        l1_private_key=None,
        lighter_environment="robinhood",
        lock_file=str(tmp_path / "strategy.lock"),
    )
    async_run = Mock(side_effect=AssertionError("strategy must not start"))
    monkeypatch.setattr(strategy_module, "_parse_args", lambda: args)
    monkeypatch.setattr(strategy_module, "_configure_logging", lambda _level: None)
    monkeypatch.setattr(strategy_module.asyncio, "run", async_run)
    monkeypatch.setattr(
        strategy_module._SingleInstanceLock,
        "acquire",
        Mock(side_effect=strategy_module.SingleInstanceError("already held")),
    )

    with pytest.raises(SystemExit) as exc_info:
        strategy_module.main()

    assert exc_info.value.code == strategy_module.EXIT_INSTANCE_ALREADY_RUNNING
    async_run.assert_not_called()


def test_edgex_sdk_is_optional_until_private_mode(monkeypatch):
    original_import = builtins.__import__

    def reject_edgex(name, *args, **kwargs):
        if name == "edgex_sdk" or name.startswith("edgex_sdk."):
            raise ImportError("edgex_sdk intentionally unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", reject_edgex)
    monkeypatch.delenv("EDGEX_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("EDGEX_STARK_PRIVATE_KEY", raising=False)
    public_source = strategy_module._EdgeXPriceSource("BTC", _LifecycleLogger())
    public_source._initialize_public = AsyncMock(return_value=("10000001", Decimal("0.1")))

    assert asyncio.run(public_source.initialize()) == ("10000001", Decimal("0.1"))

    monkeypatch.setenv("EDGEX_ACCOUNT_ID", "123")
    monkeypatch.setenv("EDGEX_STARK_PRIVATE_KEY", "0x456")
    private_source = strategy_module._EdgeXPriceSource("BTC", _LifecycleLogger())
    with pytest.raises(RuntimeError, match="Private EdgeX pricing requires the optional"):
        asyncio.run(private_source.initialize())


def test_strategy_module_import_does_not_require_edgex_sdk():
    script = textwrap.dedent(
        """
        import builtins
        original_import = builtins.__import__

        def reject_edgex(name, *args, **kwargs):
            if name == "edgex_sdk" or name.startswith("edgex_sdk."):
                raise ImportError("edgex_sdk intentionally unavailable")
            return original_import(name, *args, **kwargs)

        builtins.__import__ = reject_edgex
        import strategies.aster_lighter_cycle
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert result.returncode == 0, result.stderr
