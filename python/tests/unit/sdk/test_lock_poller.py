import unittest
from typing import Optional
from unittest.mock import ANY, Mock, call, patch

from requests import PreparedRequest, RequestException, Response
from requests.structures import CaseInsensitiveDict
from tenacity import stop_after_delay

from aistore.sdk.const import (
    ACT_CHECK_LOCK,
    HEADER_CONTENT_LENGTH,
    HTTP_METHOD_HEAD,
    HTTP_METHOD_POST,
    QPARAM_FLT_PRESENCE,
    QPARAM_PROVIDER,
    STATUS_LOCKED,
    STATUS_OK,
)
from aistore.sdk.enums import FLTPresence
from aistore.sdk.lock_poller import LockPoller
from aistore.sdk.provider import Provider
from aistore.sdk.request_executor import RequestExecutor
from aistore.sdk.retry_config import ColdGetConf, RetryConfig
from aistore.sdk.types import ActionMsg

from tests.const import GB, MB, MIB, MBIT
from tests.utils import cases

SLOW_GET_CONF = ColdGetConf(est_bandwidth_bps=10 * MBIT, max_cold_wait=300)
BAD_GET_CONF = ColdGetConf(est_bandwidth_bps=0)
NO_BACKEND_HEAD_CONF = ColdGetConf(enable_remote_head=False)

TEST_BUCKET = "test-bucket"
TEST_OBJ = "obj"
# Failed request URL: target leg of a redirected GET carrying the GS alias
# and the proxy-signed CSK qparams. The poller must derive the bucket+object
# path and ONLY the provider qparam
TEST_REQ_URL = (
    f"https://target:8081/v1/objects/{TEST_BUCKET}/{TEST_OBJ}"
    f"?{QPARAM_PROVIDER}=gcp&latest=true"
    f"&pid=p123&hsig=sig123&hnonce=42&hsmap=7"
)
EXPECTED_PATH = f"objects/{TEST_BUCKET}/{TEST_OBJ}"
EXPECTED_PARAMS = {QPARAM_PROVIDER: Provider.GOOGLE.value}
EXPECTED_LOCK_BODY = ActionMsg(action=ACT_CHECK_LOCK).model_dump_json()
FLT_EXISTS = str(FLTPresence.FLT_EXISTS.value)
FLT_PRESENT = str(FLTPresence.FLT_PRESENT.value)


def _resp(status: int, headers: Optional[dict] = None) -> Mock:
    return Mock(
        spec=Response, status_code=status, headers=CaseInsensitiveDict(headers or {})
    )


def _build_req() -> PreparedRequest:
    req = PreparedRequest()
    req.method = "GET"
    req.url = TEST_REQ_URL
    req.headers = CaseInsensitiveDict()
    return req


def _head_call(presence: str):
    return call(
        HTTP_METHOD_HEAD,
        EXPECTED_PATH,
        params={**EXPECTED_PARAMS, QPARAM_FLT_PRESENCE: presence},
    )


CHECK_LOCK_CALL = call(
    HTTP_METHOD_POST,
    EXPECTED_PATH,
    params=EXPECTED_PARAMS,
    data=EXPECTED_LOCK_BODY,
)


class TestLockPoller(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.mock_executor = Mock(spec=RequestExecutor)
        self.lock_poller = LockPoller(
            self.mock_executor, cold_get_conf=RetryConfig.default().cold_get_conf
        )

    def test_wait_returns_immediately_when_not_write_locked(self):
        """Initial check-lock returns unlocked -> no HEAD, no polling."""
        self.mock_executor.request.return_value = _resp(STATUS_OK)

        self.lock_poller.wait_for_unlock(_build_req())

        self.assertEqual(self.mock_executor.request.call_args_list, [CHECK_LOCK_CALL])

    def test_wait_polls_until_unlocked(self):
        """
        Initial check-lock -> locked (423), HEAD -> size (with default
        enable_remote_head=True -> presence=FLT_EXISTS), polling check-lock
        returns 423 then 200; poller exits cleanly. The polling POSTs must
        NOT inherit the FLT_PRESENCE qparam scoped to the HEAD call.
        """
        size_header = {HEADER_CONTENT_LENGTH: str(10 * MIB)}
        self.mock_executor.request.side_effect = [
            _resp(STATUS_LOCKED),  # initial gate
            _resp(STATUS_OK, size_header),  # HEAD for size
            _resp(STATUS_LOCKED),  # poll #1
            _resp(STATUS_OK),  # poll #2 -> unlocked, exit
        ]

        self.lock_poller.wait_for_unlock(_build_req())

        self.assertEqual(
            self.mock_executor.request.call_args_list,
            [
                CHECK_LOCK_CALL,
                _head_call(FLT_EXISTS),
                CHECK_LOCK_CALL,
                CHECK_LOCK_CALL,
            ],
        )

    def test_wait_fails_open_when_check_lock_raises(self):
        """Initial check-lock raising a transport error -> fail open, no delay."""
        self.mock_executor.request.side_effect = RequestException("boom")

        self.lock_poller.wait_for_unlock(_build_req())

        self.mock_executor.request.assert_called_once()

    @cases(
        # (enable_remote_head, expected presence qparam string sent on HEAD)
        (True, FLT_EXISTS),
        (False, FLT_PRESENT),
    )
    def test_remote_head_qparams(self, test_case):
        """
        enable_remote_head toggles which presence filter we send on the HEAD:
            - True  -> FLT_EXISTS  (target will HEAD remote backend on local miss)
            - False -> FLT_PRESENT (target returns 404 on local miss; no remote HEAD)
        """
        remote_head, expected_presence = test_case
        # `@cases` reuses the same `self` across sub-tests, so the mock state
        # (call_args_list, side_effect) persists across iterations -- reset it.
        self.mock_executor.reset_mock(side_effect=True)
        cold_get_conf = ColdGetConf(enable_remote_head=remote_head)
        self.lock_poller = LockPoller(self.mock_executor, cold_get_conf=cold_get_conf)
        self.mock_executor.request.side_effect = [
            _resp(STATUS_LOCKED),  # initial gate
            _resp(STATUS_OK, {HEADER_CONTENT_LENGTH: str(10 * MIB)}),  # HEAD
            _resp(STATUS_OK),  # poll -> unlocked
        ]

        self.lock_poller.wait_for_unlock(_build_req())

        self.assertEqual(
            self.mock_executor.request.call_args_list[1],
            _head_call(expected_presence),
        )

    @cases(
        _resp(404),
        RequestException("ANY HEAD request failure"),
    )
    def test_head_size_falls_back_to_min_floor(self, head_outcome):
        """
        With enable_remote_head=False, a local miss (404) or a transport error
        on HEAD both cause `_head_size` to return 0. The retryer is still
        built (poller proceeds to wait/poll loop) but with bounds collapsed
        to MIN_WAIT_FLOOR. Polling uses the original params (no FLT_PRESENCE).
        """
        self.mock_executor.reset_mock(side_effect=True)
        self.lock_poller = LockPoller(
            self.mock_executor, cold_get_conf=NO_BACKEND_HEAD_CONF
        )
        self.mock_executor.request.side_effect = [
            _resp(STATUS_LOCKED),  # initial gate
            head_outcome,  # HEAD: 404 or RequestException -> size=0
        ]

        # Patch the polling retryer so the test doesn't actually loop.
        with patch("aistore.sdk.lock_poller.Retrying") as mock_retrying:
            self.lock_poller.wait_for_unlock(_build_req())

            _, kwargs = mock_retrying.call_args
            wait_config = kwargs["wait"]
            self.assertEqual(wait_config.min, 2.0)
            self.assertEqual(wait_config.max, 2.0)
            # Retryer was invoked exactly once with the original path/params
            # (no FLT_PRESENCE bleed-through from the HEAD call). The callable
            # is whatever `wait_for_unlock` wires up internally
            mock_retrying.return_value.assert_called_once_with(
                ANY, EXPECTED_PATH, EXPECTED_PARAMS
            )

        self.assertEqual(
            self.mock_executor.request.call_args_list,
            [CHECK_LOCK_CALL, _head_call(FLT_PRESENT)],
        )

    @cases(
        (0, ColdGetConf, 2.0, 2.0, 180.0),
        (100 * MB, ColdGetConf, 2.0, 2.0, 180.0),
        (10 * GB, ColdGetConf, 2.0, 32.0, 180.0),
        (100 * GB, ColdGetConf, 10.0, 90.0, 180.0),
        (10 * MB, SLOW_GET_CONF, 2.0, 32.0, 300.0),
        (20 * MB, SLOW_GET_CONF, 4.0, 64.0, 300.0),
        (50 * MB, SLOW_GET_CONF, 10.0, 150.0, 300.0),
        (10 * GB, SLOW_GET_CONF, 10.0, 150.0, 300.0),
        (10 * GB, BAD_GET_CONF, 2, 32, 180.0),
    )
    def test_create_cold_get_retryer(self, test_case):
        obj_size, cold_get_conf, min_wait, max_wait, timeout = test_case
        self.lock_poller = LockPoller(self.mock_executor, cold_get_conf=cold_get_conf)

        self.mock_executor.request.side_effect = [
            _resp(STATUS_LOCKED),  # initial gate
            _resp(STATUS_OK, {HEADER_CONTENT_LENGTH: str(obj_size)}),  # HEAD
        ]
        with patch("aistore.sdk.lock_poller.Retrying") as mock_retrying:
            self.lock_poller.wait_for_unlock(_build_req())

            # Verify Retrying was initialized with expected parameters
            mock_retrying.assert_called_once()
            _, kwargs = mock_retrying.call_args

            wait_config = kwargs["wait"]
            self.assertEqual(wait_config.min, min_wait)
            self.assertEqual(wait_config.max, max_wait)
            self.assertEqual(wait_config.multiplier, 2)
            self.assertEqual(
                stop_after_delay(timeout).__dict__, kwargs["stop"].__dict__
            )
