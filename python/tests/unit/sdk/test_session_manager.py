import unittest
from unittest.mock import patch, Mock

from requests.adapters import HTTPAdapter

import urllib3

from aistore.sdk.const import AIS_CLIENT_CA
from aistore.sdk.session_manager import SessionManager, DEFAULT_RETRY
from tests.utils import cases


class TestSessionManager(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.endpoint = "https://aistore-endpoint"
        self.mock_session = Mock()

    def test_init_default(self):
        session_manager = SessionManager()
        self.assertEqual(DEFAULT_RETRY, session_manager.retry)
        self.assertIsNone(session_manager.ca_cert)
        self.assertFalse(session_manager.skip_verify)

    def test_init_args(self):
        custom_retry = urllib3.Retry(total=9)
        ca_cert_path = "/any/path"
        session_manager = SessionManager(
            retry=custom_retry, ca_cert=ca_cert_path, skip_verify=True
        )
        self.assertEqual(custom_retry, session_manager.retry)
        self.assertEqual(ca_cert_path, session_manager.ca_cert)
        self.assertTrue(session_manager.skip_verify)

    def test_session_exists(self):
        session_manager = SessionManager()
        first_session = session_manager.session
        self.assertEqual(first_session, session_manager.session)

    def test_session_processes(self):
        session_manager = SessionManager()
        first_session = session_manager.session

        mock_process = Mock()
        mock_process.pid = "MOCKPID"

        with patch(
            "aistore.sdk.session_manager.current_process", return_value=mock_process
        ):
            self.assertNotEqual(first_session, session_manager.session)

    def test_create_custom_retry(self):
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
        env_cert, arg_cert, skip_verify = test_case[0]
        with patch(
            "aistore.sdk.session_manager.os.getenv", return_value=env_cert
        ) as mock_getenv:
            session_manager = SessionManager(skip_verify=skip_verify, ca_cert=arg_cert)
            session = session_manager.session
            if not skip_verify and not arg_cert:
                mock_getenv.assert_called_with(AIS_CLIENT_CA)
            self.assertEqual(test_case[1], session.verify)
