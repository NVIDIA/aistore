import unittest
from typing import Optional
from unittest.mock import Mock, call, patch

from requests import PreparedRequest, RequestException, Response
from requests.structures import CaseInsensitiveDict
from tenacity import stop_after_delay

from aistore.sdk.const import (
    ACT_CHECK_LOCK,
    HEADER_CONTENT_LENGTH,
    HTTP_METHOD_HEAD,
    HTTP_METHOD_POST,
    QPARAM_PROVIDER,
    STATUS_LOCKED,
    STATUS_OK,
)
from aistore.sdk.lock_poller import LockPoller
from aistore.sdk.provider import Provider
from aistore.sdk.request_executor import RequestExecutor
from aistore.sdk.retry_config import ColdGetConf, RetryConfig
from aistore.sdk.types import ActionMsg

from tests.const import GB, MB, MIB, MBIT
from tests.utils import cases

SLOW_GET_CONF = ColdGetConf(est_bandwidth_bps=10 * MBIT, max_cold_wait=300)
BAD_GET_CONF = ColdGetConf(est_bandwidth_bps=0, max_cold_wait=300)

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

        self.mock_executor.request.assert_called_once_with(
            HTTP_METHOD_POST,
            EXPECTED_PATH,
            params=EXPECTED_PARAMS,
            data=EXPECTED_LOCK_BODY,
        )

    def test_wait_polls_until_unlocked(self):
        """
        Initial check-lock -> locked (423), HEAD -> size,
        polling check-lock returns 423 then 200; poller exits cleanly.
        """
        size_header = {HEADER_CONTENT_LENGTH: str(10 * MIB)}
        self.mock_executor.request.side_effect = [
            _resp(STATUS_LOCKED),  # initial gate
            _resp(STATUS_OK, size_header),  # HEAD for size
            _resp(STATUS_LOCKED),  # poll #1
            _resp(STATUS_OK),  # poll #2 -> unlocked, exit
        ]

        self.lock_poller.wait_for_unlock(_build_req())

        check_lock_call = call(
            HTTP_METHOD_POST,
            EXPECTED_PATH,
            params=EXPECTED_PARAMS,
            data=EXPECTED_LOCK_BODY,
        )
        head_call = call(HTTP_METHOD_HEAD, EXPECTED_PATH, params=EXPECTED_PARAMS)
        self.mock_executor.request.assert_has_calls(
            [check_lock_call, head_call, check_lock_call, check_lock_call]
        )

    def test_wait_fails_open_when_check_lock_raises(self):
        """Initial check-lock raising a transport error -> fail open, no delay."""
        self.mock_executor.request.side_effect = RequestException("boom")

        self.lock_poller.wait_for_unlock(_build_req())

        self.mock_executor.request.assert_called_once()

    @cases(
        (0, ColdGetConf.default(), 2.0, 2.0, 180.0),
        (100 * MB, ColdGetConf.default(), 2.0, 2.0, 180.0),
        (10 * GB, ColdGetConf.default(), 2.0, 32.0, 180.0),
        (100 * GB, ColdGetConf.default(), 10.0, 90.0, 180.0),
        (10 * MB, SLOW_GET_CONF, 2.0, 32.0, 300.0),
        (20 * MB, SLOW_GET_CONF, 4.0, 64.0, 300.0),
        (50 * MB, SLOW_GET_CONF, 10.0, 150.0, 300.0),
        (10 * GB, SLOW_GET_CONF, 10.0, 150.0, 300.0),
        (10 * GB, BAD_GET_CONF, 2, 32, 300.0),
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
