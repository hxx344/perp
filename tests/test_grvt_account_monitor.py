import os
import sys
from enum import Enum
from pathlib import Path
from types import SimpleNamespace
import unittest

sys.path.append(str(Path(__file__).parent.parent))

from monitoring import grvt_account_monitor as monitor_module


class FakeTransferType(Enum):
    UNSPECIFIED = "UNSPECIFIED"
    STANDARD = "STANDARD"
    FAST_ARB_DEPOSIT = "FAST_ARB_DEPOSIT"


def _build_session() -> monitor_module.AccountSession:
    return monitor_module.AccountSession(
        label="test",
        client=SimpleNamespace(),
        main_account_id="1",
        sub_account_id="2",
        main_sub_account_id="0",
    )


class TransferTypeAliasTests(unittest.TestCase):
    def setUp(self) -> None:
        self._original_transfer_type = monitor_module.TransferType
        monitor_module.TransferType = FakeTransferType
        self._env_backup = {
            "GRVT_DEFAULT_TRANSFER_TYPE": os.environ.pop("GRVT_DEFAULT_TRANSFER_TYPE", None),
            "GRVT_TRANSFER_TYPE": os.environ.pop("GRVT_TRANSFER_TYPE", None),
        }

    def tearDown(self) -> None:
        monitor_module.TransferType = self._original_transfer_type
        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def _build_monitor(self, **overrides):
        session = overrides.pop("session", None) or _build_session()
        return monitor_module.GrvtAccountMonitor(
            session=session,
            coordinator_url="https://example.com",
            agent_id="agent-1",
            poll_interval=5,
            request_timeout=5,
            max_positions=1,
            **overrides,
        )

    def test_internal_alias_applied_to_default(self):
        monitor = self._build_monitor(default_transfer_type="INTERNAL")
        self.assertEqual("STANDARD", monitor._default_transfer_type)
        self.assertIs(FakeTransferType.STANDARD, monitor._normalize_transfer_type(None))

    def test_spot_alias_from_payload(self):
        monitor = self._build_monitor()
        self.assertIs(FakeTransferType.STANDARD, monitor._normalize_transfer_type("spot"))

    def test_existing_member_name_is_resolved(self):
        monitor = self._build_monitor()
        self.assertIs(
            FakeTransferType.FAST_ARB_DEPOSIT,
            monitor._normalize_transfer_type("fast_arb_deposit"),
        )

    def test_unknown_transfer_type_raises_value_error(self):
        monitor = self._build_monitor()
        with self.assertRaises(ValueError):
            monitor._normalize_transfer_type("totally_unknown")


if __name__ == "__main__":
    unittest.main()
