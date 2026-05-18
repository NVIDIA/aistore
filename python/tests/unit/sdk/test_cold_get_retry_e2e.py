#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#
"""
End-to-end coverage of the cold-get retry path within the Python SDK.
Uses a mocked session and patched retries to avoid actual delayed retry or any external calls.
"""

import unittest
from unittest.mock import MagicMock, Mock, patch

from requests import PreparedRequest
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.structures import CaseInsensitiveDict
from tenacity import (
    Retrying,
    retry_if_exception_type,
    retry_if_result,
    stop_after_attempt,
    stop_after_delay,
    wait_exponential,
    wait_fixed,
)
from urllib3.exceptions import (
    HTTPError as Urllib3HTTPError,
    MaxRetryError,
    ReadTimeoutError,
)

from aistore.sdk import Client
from aistore.sdk.const import (
    ACT_CHECK_LOCK,
    HEADER_CONTENT_LENGTH,
    HTTP_METHOD_GET,
    HTTP_METHOD_HEAD,
    HTTP_METHOD_POST,
    QPARAM_PROVIDER,
    STATUS_LOCKED,
    STATUS_OK,
    URL_PATH_OBJECTS,
    QPARAM_FLT_PRESENCE,
)
from aistore.sdk.enums import FLTPresence
from aistore.sdk.provider import Provider
from aistore.sdk.retry_config import NETWORK_RETRY_EXCEPTIONS, RetryConfig
from aistore.sdk.types import ActionMsg
from tests.const import GB

PROXY_ENDPOINT = "http://aistore-proxy:8080"
BUCKET = "remote-bucket"
OBJ = "my-obj"
PROVIDER = Provider.AMAZON
# Object endpoint as seen by `requests.Session.request`
EXPECTED_OBJ_URL = f"{PROXY_ENDPOINT}/v1/{URL_PATH_OBJECTS}/{BUCKET}/{OBJ}"
EXPECTED_HEAD_PARAMS = {
    QPARAM_FLT_PRESENCE: str(FLTPresence.FLT_EXISTS.value),
    QPARAM_PROVIDER: PROVIDER.value,
}
EXPECTED_PROVIDER_PARAMS = {QPARAM_PROVIDER: PROVIDER.value}
EXPECTED_LOCK_BODY = ActionMsg(action=ACT_CHECK_LOCK).model_dump_json()
# Values for retry polling are calculated based on this size and validated below
HEAD_SIZE_BYTES = GB
EXPECTED_WAIT_MIN = 2.0
EXPECTED_WAIT_MAX = 3.0
EXPECTED_WAIT_MULTIPLIER = 2


def _ok_resp(headers=None) -> MagicMock:
    """Mocked 200 OK Response with optional headers."""
    # `spec=Response` would block access to instance-level attributes like
    # `raw`, which `ObjectReader.raw()` accesses on the success response —
    # use a plain MagicMock and stub the surface the SDK actually touches.
    resp = MagicMock()
    resp.status_code = STATUS_OK
    resp.headers = CaseInsensitiveDict(headers or {})
    resp.raise_for_status = Mock()
    return resp


def _locked_resp() -> MagicMock:
    """Mocked 423 Locked Response (signals an in-flight cold-GET write-lock)."""
    resp = MagicMock()
    resp.status_code = STATUS_LOCKED
    resp.headers = CaseInsensitiveDict()
    resp.raise_for_status = Mock()
    return resp


def _conn_error(read_timeout: bool) -> RequestsConnectionError:
    """
    Generate a requests.ConnectionError, optionally wrapping a read timeout error.
    This is the error raised AFTER any initial urllib3.Retry

    Args:
        read_timeout (bool): Whether ConnectionError wraps urllib3 ReadTimeoutError

    Returns:
        Requests library ConnectionError,
    """
    req = Mock(spec=PreparedRequest)
    req.method = HTTP_METHOD_GET.upper()
    req.url = f"{EXPECTED_OBJ_URL}?{QPARAM_PROVIDER}={PROVIDER.value}"
    req.headers = CaseInsensitiveDict()
    max_retry_err = Mock(spec=MaxRetryError)
    if read_timeout:
        max_retry_err.reason = Mock(spec=ReadTimeoutError)
    else:
        max_retry_err.reason = Mock(spec=Urllib3HTTPError)
    return RequestsConnectionError(max_retry_err, request=req)


class TestColdGetRetryE2E(unittest.TestCase):  # pylint: disable=unused-variable
    INNER_RETRY_ATTEMPTS = 3
    OUTER_RETRY_ATTEMPTS = 2

    def setUp(self) -> None:
        # 1) Replace the `requests.Session` created by `SessionManager`
        #    with a MagicMock so every HTTP call from the SDK is intercepted.
        session_patcher = patch("aistore.sdk.session_manager.Session")
        mock_session_cls = session_patcher.start()
        self.addCleanup(session_patcher.stop)
        self.mock_session = MagicMock()
        mock_session_cls.return_value = self.mock_session

        # 2) Wrap the poller's tenacity.Retrying so we can (a) record the
        #    construction kwargs for validation and (b) neuter both the
        #    per-attempt `time.sleep` and the wall-clock `stop_after_delay`
        #    so tests run instantly.
        self.cold_get_retryer_kwargs = []
        real_retrying = Retrying
        capture = self.cold_get_retryer_kwargs
        inner_cap = self.INNER_RETRY_ATTEMPTS

        def _capturing_retrying(*args, **kwargs):
            capture.append(kwargs)
            inner = real_retrying(*args, **kwargs)
            inner.sleep = lambda _s: None
            # Swap the wall-clock-based stop with an attempt cap so the
            # "remains locked" test doesn't have to wait for `max_cold_wait`
            # of real time; the originally-passed `stop` is still captured
            # in `kwargs` for validation.
            inner.stop = stop_after_attempt(inner_cap)
            return inner

        retrying_patcher = patch(
            "aistore.sdk.lock_poller.Retrying", side_effect=_capturing_retrying
        )
        retrying_patcher.start()
        self.addCleanup(retrying_patcher.stop)

        # 3) Outer `network_retry`: small attempt cap, no real sleep.
        retry_config = RetryConfig.default()
        retry_config.network_retry = Retrying(
            wait=wait_fixed(0),
            stop=stop_after_attempt(self.OUTER_RETRY_ATTEMPTS),
            retry=retry_if_exception_type(NETWORK_RETRY_EXCEPTIONS),
            reraise=True,
        )
        retry_config.network_retry.sleep = lambda _s: None
        self.cold_get_conf = retry_config.cold_get_conf

        # 4) Build the SDK object graph against the mocked session.
        self.client = Client(PROXY_ENDPOINT, retry_config=retry_config)
        self.bucket = self.client.bucket(BUCKET, provider=PROVIDER)
        self.obj = self.bucket.object(OBJ)

    # ------- helpers -------

    def _assert_obj_call(self, mock_call, method: str) -> dict:
        """Every SDK call to the session should target the proxy object URL
        with the provider qparam. Returns the kwargs for further inspection."""
        args, kwargs = mock_call
        self.assertEqual(args[0], method)
        self.assertEqual(args[1], EXPECTED_OBJ_URL)
        if method == HTTP_METHOD_HEAD:
            self.assertEqual(kwargs.get("params"), EXPECTED_HEAD_PARAMS)
        else:
            self.assertEqual(kwargs.get("params"), EXPECTED_PROVIDER_PARAMS)
        return kwargs

    def _calls_for_method(self, method: str) -> list:
        return [
            c
            for c in self.mock_session.request.call_args_list
            if c.args and c.args[0] == method
        ]

    def _assert_retries_routed_to_proxy(self) -> None:
        """All GETs (initial + outer-retry) must go to the proxy endpoint."""
        gets = self._calls_for_method(HTTP_METHOD_GET)
        self.assertGreaterEqual(len(gets), 1, "expected at least one GET")
        for c in gets:
            self.assertEqual(c.args[1], EXPECTED_OBJ_URL)
            self.assertEqual(c.kwargs.get("params"), EXPECTED_PROVIDER_PARAMS)

    def _assert_inner_retryer_creation(self) -> None:
        """`LockPoller._create_cold_get_retryer` should have been
        invoked exactly once per cold-get poll cycle with the expected
        Tenacity components."""
        self.assertEqual(
            len(self.cold_get_retryer_kwargs),
            1,
            "expected exactly one cold-get poll cycle",
        )
        kwargs = self.cold_get_retryer_kwargs[0]

        # Validate corresponding expected wait values for a given object size
        wait = kwargs["wait"]
        self.assertIsInstance(wait, wait_exponential)
        self.assertEqual(wait.multiplier, EXPECTED_WAIT_MULTIPLIER)
        self.assertEqual(wait.min, EXPECTED_WAIT_MIN)
        self.assertEqual(wait.max, EXPECTED_WAIT_MAX)

        # Production code uses wall-clock `stop_after_delay(max_cold_wait)`
        # — capture the kwarg as-passed, separate from the attempt-cap we
        # substituted post-construction.
        self.assertEqual(
            kwargs["stop"].__dict__,
            stop_after_delay(self.cold_get_conf.max_cold_wait).__dict__,
        )
        self.assertIsInstance(kwargs["retry"], retry_if_result)

    def _assert_no_poller_invoked(self) -> None:
        """LockPoller never built an inner retryer and never issued
        check-lock POSTs or HEADs."""
        self.assertEqual(len(self.cold_get_retryer_kwargs), 0)
        self.assertEqual(self._calls_for_method(HTTP_METHOD_POST), [])
        self.assertEqual(self._calls_for_method(HTTP_METHOD_HEAD), [])

    # ------- tests -------

    def test_object_unlocks_after_polls(self) -> None:
        """Read-timeout on the initial cold GET, write-lock released after
        two polls, retried GET succeeds."""
        self.mock_session.request.side_effect = [
            _conn_error(read_timeout=True),  # 1) initial GET fails
            _locked_resp(),  # 2) initial gate POST: locked
            _ok_resp({HEADER_CONTENT_LENGTH: str(HEAD_SIZE_BYTES)}),  # 3) HEAD
            _locked_resp(),  # 4) poll #1: still locked
            _locked_resp(),  # 5) poll #2: still locked
            _ok_resp(),  # 6) poll #3: unlocked -> poller exits
            _ok_resp(),  # 7) retried GET succeeds
        ]

        self.obj.get_reader().raw()

        calls = self.mock_session.request.call_args_list
        self.assertEqual(len(calls), 7)

        # Initial proxy GET
        self._assert_obj_call(calls[0], HTTP_METHOD_GET)
        # Initial check-lock gate POST carries the ActionMsg body
        gate_kwargs = self._assert_obj_call(calls[1], HTTP_METHOD_POST)
        self.assertEqual(gate_kwargs.get("data"), EXPECTED_LOCK_BODY)
        # HEAD for sizing the poll backoff
        self._assert_obj_call(calls[2], HTTP_METHOD_HEAD)
        # 3 poll POSTs
        for c in calls[3:6]:
            kwargs = self._assert_obj_call(c, HTTP_METHOD_POST)
            self.assertEqual(kwargs.get("data"), EXPECTED_LOCK_BODY)
        # Retried GET goes to the proxy (not to any redirected target URL)
        self._assert_obj_call(calls[6], HTTP_METHOD_GET)

        self._assert_retries_routed_to_proxy()
        self._assert_inner_retryer_creation()

    def test_object_remains_locked(self) -> None:
        """Object never unlocks → outer retry exhausts and reraises the
        original `ConnectionError`."""

        def _dispatch(method, url, **_kwargs):
            if method == HTTP_METHOD_GET:
                raise _conn_error(read_timeout=True)
            if method == HTTP_METHOD_HEAD:
                return _ok_resp({HEADER_CONTENT_LENGTH: str(HEAD_SIZE_BYTES)})
            if method == HTTP_METHOD_POST:
                return _locked_resp()
            raise AssertionError(f"unexpected: {method} {url}")

        self.mock_session.request.side_effect = _dispatch

        with self.assertRaises(RequestsConnectionError):
            self.obj.get_reader().raw()

        # Outer retry should have produced exactly `OUTER_RETRY_ATTEMPTS`
        # GETs, all targeting the proxy endpoint.
        gets = self._calls_for_method(HTTP_METHOD_GET)
        self.assertEqual(len(gets), self.OUTER_RETRY_ATTEMPTS)
        self._assert_retries_routed_to_proxy()

        # Exactly one polling cycle (only fires from `before_sleep` between
        # attempts 1 and 2; after attempt 2 `stop` fires and we reraise):
        # 1 initial-gate POST + 1 HEAD + INNER_RETRY_ATTEMPTS poll POSTs.
        heads = self._calls_for_method(HTTP_METHOD_HEAD)
        posts = self._calls_for_method(HTTP_METHOD_POST)
        self.assertEqual(len(heads), 1)
        self.assertEqual(len(posts), 1 + self.INNER_RETRY_ATTEMPTS)
        self._assert_inner_retryer_creation()

    def test_non_retryable_exception_short_circuits(self) -> None:
        """`ValueError` (not in `NETWORK_RETRY_EXCEPTIONS`) propagates on
        the first attempt with no outer retry and no poll."""
        self.mock_session.request.side_effect = ValueError("boom")

        with self.assertRaises(ValueError):
            self.obj.get_reader().raw()

        # Single GET attempt; no polling artifacts.
        self.assertEqual(len(self._calls_for_method(HTTP_METHOD_GET)), 1)
        self._assert_retries_routed_to_proxy()
        self._assert_no_poller_invoked()

    def test_connection_error_without_read_timeout_skips_poller(self) -> None:
        """`ConnectionError` with a non-`ReadTimeoutError` root cause is
        retried (it's in `NETWORK_RETRY_EXCEPTIONS`) but must NOT trigger
        the write-lock poll — `_should_delay_retry` filters it out."""
        self.mock_session.request.side_effect = [
            _conn_error(read_timeout=False),
            _conn_error(read_timeout=False),
        ]

        with self.assertRaises(RequestsConnectionError):
            self.obj.get_reader().raw()

        # Both outer attempts ran, both as GETs to the proxy URL.
        self.assertEqual(
            len(self._calls_for_method(HTTP_METHOD_GET)),
            self.OUTER_RETRY_ATTEMPTS,
        )
        self._assert_retries_routed_to_proxy()
        # No LockPoller invocation at all.
        self._assert_no_poller_invoked()

    def test_connection_error_without_prepared_request_skips_poller(self) -> None:
        """A `ConnectionError` without a `PreparedRequest` attached cannot
        be routed to the poll — `_before_sleep` must early-return, the
        outer retry continues without polling."""
        # Raw exception with no `.request` attribute (None by default).
        self.mock_session.request.side_effect = [
            RequestsConnectionError("bare"),
            RequestsConnectionError("bare"),
        ]

        with self.assertRaises(RequestsConnectionError):
            self.obj.get_reader().raw()

        self.assertEqual(
            len(self._calls_for_method(HTTP_METHOD_GET)),
            self.OUTER_RETRY_ATTEMPTS,
        )
        self._assert_no_poller_invoked()
