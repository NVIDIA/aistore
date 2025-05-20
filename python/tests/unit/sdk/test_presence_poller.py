import unittest
from unittest.mock import Mock, call, patch

from requests import Response, PreparedRequest
from requests.structures import CaseInsensitiveDict
from tenacity import stop_after_delay

from aistore.sdk.const import (
    JSON_CONTENT_TYPE,
    HEADER_USER_AGENT,
    USER_AGENT_BASE,
    HEADER_CONTENT_TYPE,
    HTTP_METHOD_HEAD,
    AIS_PRESENT,
    HEADER_CONTENT_LENGTH,
)
from aistore.sdk.presence_poller import PresencePoller
from aistore.sdk.retry_config import RetryConfig, ColdGetConf

from aistore.version import __version__ as sdk_version
from tests.const import MIB, GB, MB
from tests.utils import cases

SLOW_GET_CONF = ColdGetConf(est_bandwidth_bps=10 * MB / 8, max_cold_wait=300)


class TestRequestClient(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.mock_session_request = Mock()
        self.presence_poller = PresencePoller(
            self.mock_session_request, cold_get_conf=RetryConfig.default().cold_get_conf
        )

    def test_wait_for_presence(self):
        """Test that the function delays retries until the object is present in cluster."""
        req = PreparedRequest()
        req.method = "GET"
        req.url = "https://aistore-endpoint/object-url"
        req.headers = CaseInsensitiveDict(
            {
                HEADER_CONTENT_TYPE: JSON_CONTENT_TYPE,
                HEADER_USER_AGENT: f"{USER_AGENT_BASE}/{sdk_version}",
            }
        )

        non_present_header = CaseInsensitiveDict(
            {AIS_PRESENT: "false", HEADER_CONTENT_LENGTH: 10 * MIB}
        )
        # First attempt should succeed, the object props shows it is not present, but gets the size
        # Second attempt is in the poller, we'll let it fail to be sure we retry
        # Third HEAD call shows object props is present, so we return
        self.mock_session_request.side_effect = [
            Mock(spec=Response, headers=non_present_header),
            Mock(spec=Response, headers=non_present_header),
            Mock(spec=Response, headers={AIS_PRESENT: "true"}),
        ]
        self.presence_poller.wait_for_presence(req)
        self.mock_session_request.assert_has_calls(
            [call(HTTP_METHOD_HEAD, req.url, req.headers)] * 3
        )

    @cases(
        (100 * MB, ColdGetConf.default(), 2.0, 3.0, 180.0),
        (1 * GB, ColdGetConf.default(), 2.0, 32.0, 180.0),
        (10 * GB, ColdGetConf.default(), 10.0, 90.0, 180.0),
        (1 * MB, SLOW_GET_CONF, 2.0, 3.0, 300.0),
        (5 * MB, SLOW_GET_CONF, 2.0, 16.0, 300.0),
        (1 * GB, SLOW_GET_CONF, 10.0, 150.0, 300.0),
    )
    def test_create_cold_get_retryer(self, test_case):
        obj_size, cold_get_conf, min_wait, max_wait, timeout = test_case
        self.presence_poller = PresencePoller(
            self.mock_session_request, cold_get_conf=cold_get_conf
        )

        non_present_header = CaseInsensitiveDict(
            {AIS_PRESENT: "false", HEADER_CONTENT_LENGTH: obj_size}
        )
        req = PreparedRequest()
        req.method = "GET"
        self.mock_session_request.side_effect = [
            Mock(spec=Response, headers=non_present_header),
            Mock(spec=Response, headers={AIS_PRESENT: "true"}),
        ]
        with patch("aistore.sdk.presence_poller.Retrying") as mock_retrying:
            self.presence_poller.wait_for_presence(req)

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
