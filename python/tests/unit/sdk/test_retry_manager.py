import unittest
from unittest.mock import patch, Mock
from urllib.parse import urlencode

import urllib3
from requests import PreparedRequest
from requests.exceptions import (
    ConnectTimeout,
    ConnectionError as RequestsConnectionError,
)
from tenacity import (
    Retrying,
    stop_after_attempt,
    retry_if_exception_type,
)
from aistore.sdk.const import QPARAM_PROVIDER, HTTP_METHOD_GET
from aistore.sdk.presence_poller import PresencePoller
from aistore.sdk.provider import Provider

from aistore.sdk.retry_manager import RetryManager
from aistore.sdk.errors import AISRetryableError
from aistore.sdk.retry_config import NETWORK_RETRY_EXCEPTIONS, RetryConfig
from tests.utils import cases


class TestRetryManager(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        # Function passed to be retried
        self.primary_func = Mock()
        self.mock_response = Mock()
        # Function used for the retry manager to use internally, e.g. for polling
        self.mock_req_func = Mock()
        self.mock_retry_config = Mock()
        self.retry_manager = RetryManager(self.mock_req_func)

    def test_config(self):
        retry_config = RetryConfig.default()
        retry_config.max_retries = 45
        self.retry_manager = RetryManager(self.mock_req_func, retry_config)
        self.assertEqual(self.retry_manager.retry_config, retry_config)

    def test_retry_on_connect_timeout(self):
        """Test that the function retries without delay on ConnectTimeout."""
        self.mock_response.status_code = 200
        self.primary_func.side_effect = [
            ConnectTimeout,
            self.mock_response,
        ]  # Fails once, then succeeds
        arg = "http://test-url"

        response = self.retry_manager.with_retry(
            self.primary_func, arg, extra_key="val"
        )

        # Test args are passed through to primary func and response is returned
        self.assertEqual(response.status_code, 200)
        self.primary_func.assert_called_with(arg, extra_key="val")
        # Retries once before success
        self.assertEqual(self.primary_func.call_count, 2)
        # Raw ConnectTimeout does not qualify for delayed retry
        self.assertEqual(self.mock_req_func.call_count, 0)

    def test_retry_on_connection_error(self):
        """Test that the function retries on ConnectionError (e.g., refused connection)."""
        self.mock_response.status_code = 200
        self.primary_func.side_effect = [
            RequestsConnectionError,
            RequestsConnectionError,
            self.mock_response,
        ]
        response = self.retry_manager.with_retry(self.primary_func)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            self.primary_func.call_count, 3
        )  # Retries twice before success
        # Raw ConnectionError does not qualify for delayed retry
        self.assertEqual(self.mock_req_func.call_count, 0)

    @staticmethod
    def get_req_and_err(source_exception):
        query = urlencode({QPARAM_PROVIDER: Provider.AMAZON.value})
        initial_request = Mock(
            spec=PreparedRequest,
            method=HTTP_METHOD_GET,
            url=f"http://localhost:8080/v1/objects/bck/my-obj?{query}",
        )
        # Any failed request with urllib3-style retries will be wrapped in this
        mock_retry_err = Mock(spec=urllib3.exceptions.MaxRetryError)
        # Delayed retry will trigger iff this source exception is ReadTimeoutError
        mock_retry_err.reason = Mock(source_exception)
        mock_err = RequestsConnectionError(mock_retry_err, request=initial_request)
        return initial_request, mock_err

    @cases((urllib3.exceptions.ReadTimeoutError, 1), (urllib3.exceptions.HTTPError, 0))
    @patch("aistore.sdk.retry_manager.PresencePoller")
    def test_delayed_retry_on_error_source(self, case, mock_presence_poller):
        """Test that the function uses the presence retryer only when a Connection error is caused by a
        ReadTimeoutError."""
        source_exc, poller_calls = case
        poller_instance = Mock(spec=PresencePoller)
        mock_presence_poller.return_value = poller_instance
        # Re-create to pick up the mock in init
        self.retry_manager = RetryManager(self.mock_req_func)
        self.mock_response.status_code = 200

        initial_req, err = self.get_req_and_err(source_exc)
        self.primary_func.side_effect = [
            err,
            self.mock_response,
        ]

        response = self.retry_manager.with_retry(self.primary_func)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(poller_instance.wait_for_presence.call_count, poller_calls)
        # Assert original request was passed to poller to wait, if needed
        for call_args in poller_instance.wait_for_presence.call_args_list:
            self.assertEqual(call_args[0][0], initial_req)
        self.assertEqual(self.primary_func.call_count, 2)
        # reset for cases
        self.primary_func.reset_mock()

    @patch("aistore.sdk.retry_manager.PresencePoller")
    def test_failed_presence_poller(self, mock_presence_poller):
        """Test that the function raises properly if the presence poller also raises an error on a ReadTimeoutError."""
        # Same setup as the success-flow, but this time make the poller error
        initial_req, err = self.get_req_and_err(urllib3.exceptions.ReadTimeoutError)
        inner_exc = RuntimeError("An error inside the presence poller")
        poller_instance = Mock(spec=PresencePoller)
        poller_instance.wait_for_presence.side_effect = [inner_exc, None]
        mock_presence_poller.return_value = poller_instance
        # Re-create to pick up the mock in init
        self.retry_manager = RetryManager(self.mock_req_func)
        self.primary_func.side_effect = [
            err,
            self.mock_response,
        ]
        # Fail the session request with a ConnectionError wrapping MaxRetryError wrapping ReadTimeout
        # Then fail polling with a RuntimeError, which will be wrapped in the original ConnectionError and retried
        self.retry_manager.with_retry(self.primary_func)
        poller_instance.wait_for_presence.assert_called_once_with(initial_req)
        self.assertEqual(self.primary_func.call_count, 2)

    def test_max_retries_exceeded(self):
        """Test that the function raises an error after max retries are exceeded."""
        self.primary_func.side_effect = RequestsConnectionError  # Always fails

        retry_conf = RetryConfig.default()
        # change retry logic for this request
        retry_conf.network_retry = Retrying(
            stop=stop_after_attempt(5),
            retry=retry_if_exception_type(NETWORK_RETRY_EXCEPTIONS),
            reraise=True,
        )
        self.retry_manager = RetryManager(self.mock_req_func, retry_config=retry_conf)
        with self.assertRaises(RequestsConnectionError):
            self.retry_manager.with_retry(self.primary_func)

        self.assertEqual(self.primary_func.call_count, 5)  # Stops at max retry limit

    def test_unexpected_exception(self):
        """Test that an unexpected exception is raised correctly."""
        self.primary_func.side_effect = ValueError(
            "Unexpected error"
        )  # Simulate unexpected failure

        with self.assertRaises(ValueError):
            self.retry_manager.with_retry(self.primary_func)

        self.primary_func.assert_called_once()  # Should fail immediately, no retries

    def test_ais_retryable_errors(self):
        """Test that the function is retried if it raises AISRetryableError."""
        self.mock_response.status_code = 200
        mock_request = Mock()
        self.primary_func.side_effect = [
            AISRetryableError(409, "Conflict", "http://test-url", mock_request),
            self.mock_response,
        ]

        response = self.retry_manager.with_retry(self.primary_func)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.primary_func.call_count, 2)  # Retries once before success
