import unittest


class ParadexSigningAdapterTests(unittest.TestCase):
    def test_sign_message_preferred(self):
        from monitoring.para_account_monitor import ParadexAccountMonitor

        monitor = ParadexAccountMonitor.__new__(ParadexAccountMonitor)

        class _Acct:
            def sign_message(self, msg):
                return ("signed_message", msg)

        monitor._client = type("C", (), {"account": _Acct()})()
        out = monitor._sign_paradex_message("hello")
        self.assertEqual(out, ("signed_message", "hello"))

    def test_sign_fallback(self):
        from monitoring.para_account_monitor import ParadexAccountMonitor

        monitor = ParadexAccountMonitor.__new__(ParadexAccountMonitor)

        class _Acct:
            def sign(self, msg):
                return ("signed", msg)

        monitor._client = type("C", (), {"account": _Acct()})()
        out = monitor._sign_paradex_message("world")
        self.assertEqual(out, ("signed", "world"))

    def test_no_signing_methods(self):
        from monitoring.para_account_monitor import ParadexAccountMonitor

        monitor = ParadexAccountMonitor.__new__(ParadexAccountMonitor)

        class _Acct:
            pass

        monitor._client = type("C", (), {"account": _Acct()})()
        with self.assertRaises(AttributeError):
            monitor._sign_paradex_message("x")


if __name__ == "__main__":
    unittest.main()
