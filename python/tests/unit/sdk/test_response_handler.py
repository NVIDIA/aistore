import unittest
from unittest.mock import Mock
from typing import Type

import requests

from aistore.sdk.errors import APIRequestError
from aistore.sdk.response_handler import ResponseHandler
from tests.utils import cases, create_api_error_response


class MockAPIException(APIRequestError):
    """Custom exception for testing"""


class ResponseHandlerImpl(ResponseHandler):
    """Custom implementation of response handler for testing"""

    @property
    def exc_class(self) -> Type[APIRequestError]:
        return MockAPIException

    def parse_error(self, r: requests.Response):
        return self.exc_class(r.status_code, r.text, r.request.url, r.request)


# pylint: disable=unused-variable
class TestResponseHandler(unittest.TestCase):

    def setUp(self):
        self.resp_handler = ResponseHandlerImpl()

    @cases(200, 299, 300)
    def test_handle_response_no_err_code(self, status):
        mock_response = Mock(status_code=status)
        self.assertEqual(
            mock_response, self.resp_handler.handle_response(mock_response)
        )
        mock_response.raise_for_status.assert_not_called()

    @cases(("msg", 199, False), ("msg", 400, True), ("msg", 500, True))
    def test_handle_response_err_code(self, test_case):
        err_msg, status, http_err = test_case
        test_url = "http://test-url"
        response = create_api_error_response(test_url, status, err_msg)
        with self.assertRaises(MockAPIException) as context:
            self.resp_handler.handle_response(response)
        # Check if raised from HTTPError (includes error context)
        self.assertEqual(
            http_err,
            isinstance(context.exception.__cause__, requests.exceptions.HTTPError),
        )
        self.assertEqual(status, context.exception.status_code)
        self.assertEqual(err_msg, context.exception.message)
        self.assertEqual(test_url, context.exception.req_url)
        self.assertEqual(response.request, context.exception.req)
