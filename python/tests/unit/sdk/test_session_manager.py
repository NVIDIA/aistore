"""
Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
"""

import pickle
import unittest
from multiprocessing import get_all_start_methods
from unittest.mock import patch, call, Mock

from requests.adapters import HTTPAdapter

import urllib3

from aistore.sdk.const import AIS_CLIENT_CA, AIS_CLIENT_CRT, AIS_CLIENT_KEY
from aistore.sdk.session_manager import SessionManager

from tests.utils import cases, run_forked_test


class TestSessionManager(unittest.TestCase):  # pylint: disable=unused-variable
    """Cover session configuration, ownership, serialization, and closure."""

    def setUp(self) -> None:
        """Prepare the endpoint and session mock used by the tests."""
        self.endpoint = "https://aistore-endpoint"
        self.mock_session = Mock()

    def test_init_default(self):
        """Default configuration enables certificate verification without a custom CA."""
        session_manager = SessionManager()
        self.assertIsNone(session_manager.ca_cert)
        self.assertFalse(session_manager.skip_verify)

    def test_init_args(self):
        """Preserve explicit retry, CA certificate, and verification settings."""
        custom_retry = urllib3.Retry(total=9)
        ca_cert_path = "/any/path"
        session_manager = SessionManager(
            retry=custom_retry, ca_cert=ca_cert_path, skip_verify=True
        )
        self.assertEqual(custom_retry, session_manager.retry)
        self.assertEqual(ca_cert_path, session_manager.ca_cert)
        self.assertTrue(session_manager.skip_verify)

    def test_session_exists(self):
        """Repeated access in the same process returns the existing session."""
        session_manager = SessionManager()
        first_session = session_manager.session
        self.assertEqual(first_session, session_manager.session)

    @cases(False, True)
    def test_session_processes(self, acquire):
        """Never close an inherited session, with or without acquiring a new one.

        Close a newly acquired session only in its owning process.
        """
        session_manager = SessionManager()
        first_session = session_manager.session
        self.addCleanup(first_session.close)

        mock_process = Mock()
        mock_process.pid = "MOCKPID"

        with (
            patch(
                "aistore.sdk.session_manager.current_process", return_value=mock_process
            ),
            patch.object(first_session, "close") as inherited_close,
        ):
            if acquire:
                self.assertIsNot(first_session, session_manager.session)
                self.addCleanup(session_manager.session.close)
                with patch.object(session_manager.session, "close") as close:
                    session_manager.close()
                    close.assert_called_once_with()
            else:
                session_manager.close()
            inherited_close.assert_not_called()

    @unittest.skipUnless("fork" in get_all_start_methods(), "requires fork")
    def test_session_fork(self):
        """Forking creates a child session without replacing the parent's session."""
        manager = SessionManager()
        self.addCleanup(manager.close)
        original = manager.session

        def child():
            """Compare session identities inside the child process."""
            try:
                self.assertIsNot(manager.session, original)
            finally:
                manager.close()

        run_forked_test(child)
        self.assertIs(manager.session, original)

    def test_close(self):
        """Close and detach once; later acquisition creates a fresh session."""
        manager = SessionManager()
        self.addCleanup(manager.close)
        session = manager.session
        with patch.object(session, "close", wraps=session.close) as close:
            manager.close()
            manager.close()
            close.assert_called_once_with()
        self.assertIsNot(manager.session, session)

    def test_pickle(self):
        """Preserve configuration while omitting live sessions."""
        manager = SessionManager(retry=urllib3.Retry(total=3), max_pool_size=7)
        self.addCleanup(manager.close)
        session = manager.session
        # Live session state must not be serialized.
        session.headers["X-Test-Session"] = "original"
        restored = pickle.loads(pickle.dumps(manager))
        self.addCleanup(restored.close)
        self.assertEqual(restored.retry.total, 3)
        self.assertIs(manager.session, session)
        self.assertIsNot(restored.session, session)
        self.assertNotIn("X-Test-Session", restored.session.headers)
        adapter = restored.session.get_adapter(self.endpoint)
        self.assertEqual(adapter.poolmanager.connection_pool_kw["maxsize"], 7)

    def test_create_custom_retry(self):
        """Apply the configured retry policy to the session HTTP adapter."""
        custom_retry = urllib3.util.Retry(total=40, connect=2)
        session_manager = SessionManager(retry=custom_retry)
        adapter = session_manager.session.get_adapter(self.endpoint)
        self.assertIsInstance(adapter, HTTPAdapter)
        self.assertEqual(custom_retry, adapter.max_retries)

    @cases(
        (("env-cert", "arg-cert", False), "arg-cert"),
        (("env-cert", "arg-cert", True), False),
        (("env-cert", None, False), "env-cert"),
        ((True, None, False), True),
        ((None, None, True), False),
    )
    def test_create_tls(self, test_case):
        """Resolve TLS verification from explicit arguments and environment settings."""
        env_cert, arg_cert, skip_verify = test_case[0]
        with patch(
            "aistore.sdk.session_manager.os.getenv", return_value=env_cert
        ) as mock_getenv:
            session_manager = SessionManager(skip_verify=skip_verify, ca_cert=arg_cert)
            session = session_manager.session
            if not skip_verify and not arg_cert:
                mock_getenv.assert_called_with(AIS_CLIENT_CA)
            self.assertEqual(test_case[1], session.verify)

    @cases(
        ((None, None, None), None),
        (("env-cert", "env-key", None), ("env-cert", "env-key")),
        ((None, None, ("client.crt", "client.key")), ("client.crt", "client.key")),
        (
            ("env-cert", "env-key", ("client.crt", "client.key")),
            ("client.crt", "client.key"),
        ),
    )
    def test_create_mtls(self, test_case):
        """Prefer explicit client certificates over environment certificate paths."""
        env_cert, env_key, arg_cert = test_case[0]
        with patch("aistore.sdk.session_manager.os.getenv") as mock_getenv:
            mock_getenv.side_effect = lambda x: (
                env_cert if x == AIS_CLIENT_CRT else env_key
            )
            session_manager = SessionManager(client_cert=arg_cert, skip_verify=True)
            if arg_cert:
                mock_getenv.assert_not_called()
            else:
                mock_getenv.assert_has_calls(
                    [call(AIS_CLIENT_CRT), call(AIS_CLIENT_KEY)], any_order=True
                )
            self.assertEqual(test_case[1], session_manager.session.cert)

    @patch("urllib3.disable_warnings")
    def test_skip_verify_suppresses_warnings(self, mock_disable_warnings):
        """Test that SSL warnings are suppressed when skip_verify=True."""
        SessionManager(skip_verify=True)
        mock_disable_warnings.assert_called_once_with(
            urllib3.exceptions.InsecureRequestWarning
        )

    @patch("urllib3.disable_warnings")
    def test_verify_enabled_no_warning_suppression(self, mock_disable_warnings):
        """Test that SSL warnings are NOT suppressed when skip_verify=False."""
        SessionManager(skip_verify=False)
        mock_disable_warnings.assert_not_called()
