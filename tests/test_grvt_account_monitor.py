import json
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


class TransferMetadataTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env_backup = {
            "GRVT_TRANSFER_METADATA_PROVIDER": os.environ.pop("GRVT_TRANSFER_METADATA_PROVIDER", None),
            "GRVT_TRANSFER_METADATA_CHAIN_ID": os.environ.pop("GRVT_TRANSFER_METADATA_CHAIN_ID", None),
            "GRVT_TRANSFER_METADATA_ENDPOINT": os.environ.pop("GRVT_TRANSFER_METADATA_ENDPOINT", None),
            "GRVT_LITE_INCLUDE_METADATA": os.environ.pop("GRVT_LITE_INCLUDE_METADATA", None),
        }

    def tearDown(self) -> None:
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
            agent_id="agent-2",
            poll_interval=5,
            request_timeout=5,
            max_positions=1,
            **overrides,
        )

    def test_prepare_transfer_payload_populates_metadata_defaults(self):
        os.environ["GRVT_TRANSFER_METADATA_PROVIDER"] = "TESTPROV"
        os.environ["GRVT_TRANSFER_METADATA_CHAIN_ID"] = "42161"
        monitor = self._build_monitor()
        entry = {
            "request_id": "req-1",
            "transfer": {
                "from_account_id": "0xaaa",
                "from_sub_account_id": "1",
                "to_account_id": "0xbbb",
                "to_sub_account_id": "2",
                "currency": "USDT",
                "num_tokens": "1",
            },
        }
        payload = monitor._prepare_transfer_payload(entry)
        metadata = payload.get("transfer_metadata")
        self.assertIsInstance(metadata, dict)
        metadata_dict = dict(metadata or {})
        for key in ("provider", "direction", "provider_tx_id", "chainid", "endpoint"):
            self.assertIn(key, metadata_dict)
            self.assertTrue(metadata_dict[key])

    def test_lite_payload_includes_metadata_by_default(self):
        monitor = self._build_monitor()
        transfer_dict = {
            "from_account_id": "0xabc",
            "from_sub_account_id": "1",
            "to_account_id": "0xdef",
            "to_sub_account_id": "2",
            "currency": "USDT",
            "num_tokens": "1",
            "signature": {"signer": "0xabc", "r": "0x0", "s": "0x0", "v": 27, "expiration": "0", "nonce": 1},
            "transfer_type": "STANDARD",
        }
        metadata = {
            "provider": "XY",
            "direction": "WITHDRAWAL",
            "provider_tx_id": "txn1",
            "chainid": "42161",
            "endpoint": "0xdef",
        }
        metadata_text = monitor._serialize_transfer_metadata(metadata)
        lite_payload = monitor._convert_transfer_to_lite_payload(transfer_dict, metadata, metadata_text)
        self.assertEqual(lite_payload["tt"], "STANDARD")
        self.assertIn("tm", lite_payload)
        self.assertIsInstance(lite_payload["tm"], str)
        decoded = json.loads(lite_payload["tm"])
        self.assertEqual(decoded.get("provider"), "XY")

    def test_lite_metadata_flag_can_disable(self):
        os.environ["GRVT_LITE_INCLUDE_METADATA"] = "false"
        monitor = self._build_monitor()
        transfer_dict = {
            "from_account_id": "0xabc",
            "from_sub_account_id": "1",
            "to_account_id": "0xdef",
            "to_sub_account_id": "2",
            "currency": "USDT",
            "num_tokens": "1",
            "signature": {"signer": "0xabc", "r": "0x0", "s": "0x0", "v": 27, "expiration": "0", "nonce": 1},
            "transfer_type": "STANDARD",
        }
        metadata = {
            "provider": "XY",
            "direction": "WITHDRAWAL",
            "provider_tx_id": "txn1",
            "chainid": "42161",
            "endpoint": "0xdef",
        }
        metadata_text = monitor._serialize_transfer_metadata(metadata)
        lite_payload = monitor._convert_transfer_to_lite_payload(transfer_dict, metadata, metadata_text)
        self.assertNotIn("tm", lite_payload)

if __name__ == "__main__":
    unittest.main()
