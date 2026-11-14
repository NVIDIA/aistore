#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#
import pickle
import unittest

import httpx
import requests
import urllib3

from aistore.sdk.authn.errors import ErrUserNotFound
from aistore.sdk.errors import (
    AISError,
    ETLDirectPutTransientError,
    ErrBckNotFound,
    ErrGETConflict,
    ErrObjNotFound,
    InvalidBckProvider,
    InvalidObjectRangeIndex,
    InvalidURLException,
    JobInfoNotFound,
    NoTargetError,
    Timeout,
    UnexpectedHTTPStatusCode,
)
from aistore.sdk.obj.obj_file.errors import (
    ObjectFileReaderMaxResumeError,
    ObjectFileReaderStreamError,
    ObjectFileReaderUnexpectedEOF,
)
from tests.utils import cases


class NarrowAPIError(AISError):
    def __init__(self, message):
        super().__init__(404, message, "http://ais.example/obj", None)


# pylint: disable=unused-variable
class TestExceptionPickle(unittest.TestCase):
    def _assert_round_trip(self, error):
        for protocol in (0, 2, pickle.HIGHEST_PROTOCOL):
            restored = error
            for _ in range(2):
                restored = pickle.loads(pickle.dumps(restored, protocol=protocol))
                self.assertIs(type(restored), type(error))
                self.assertEqual(restored.args, error.args)
                self.assertEqual(str(restored), str(error))
        return restored

    @cases(
        (ErrObjNotFound, 404, "object not found"),
        (ErrGETConflict, 409, "write lock held"),
        (ErrBckNotFound, 404, "bucket missing"),
        (AISError, 500, "internal error"),
        (ErrUserNotFound, 404, "user missing"),
    )
    def test_api_request_error(self, case):
        error_type, status, message = case
        request = requests.Request(
            "GET", "http://ais.example/obj", data=b"body"
        ).prepare()
        error = error_type(status, message, request.url, request)
        if hasattr(error, "add_note"):
            error.add_note("seen by worker")
        restored = self._assert_round_trip(error)
        self.assertEqual(restored.status_code, status)
        self.assertEqual(restored.message, message)
        self.assertEqual(restored.req_url, request.url)
        self.assertEqual(restored.req.method, request.method)
        self.assertEqual(restored.req.url, request.url)
        self.assertEqual(restored.req.body, request.body)
        if hasattr(error, "add_note"):
            self.assertEqual(restored.__notes__, error.__notes__)

    @cases(
        NarrowAPIError("missing"),
        InvalidBckProvider("nope"),
        Timeout("bucket ready", "after 5s"),
        Timeout("list objects"),
        InvalidObjectRangeIndex("invalid range"),
        JobInfoNotFound("job-1"),
        InvalidURLException("not a url"),
        NoTargetError(4),
        UnexpectedHTTPStatusCode([200, 202], 404),
        ObjectFileReaderUnexpectedEOF(10, 40),
    )
    def test_constructor_state(self, error):
        restored = self._assert_round_trip(error)
        self.assertEqual(vars(restored), vars(error))

    @cases(
        (
            ETLDirectPutTransientError(
                "http://target/obj", requests.ConnectionError("reset")
            ),
            "cause",
        ),
        (
            ETLDirectPutTransientError(
                "http://target/obj", requests.ConnectionError("reset"), True
            ),
            "cause",
        ),
        (
            ETLDirectPutTransientError(
                "http://target/obj", httpx.ConnectError("failed")
            ),
            "cause",
        ),
        (
            ObjectFileReaderMaxResumeError(ValueError("stream broke"), 3),
            "original_error",
        ),
        (ObjectFileReaderStreamError(ValueError("stream broke")), "original_error"),
    )
    def test_nested_error(self, case):
        error, cause_attribute = case
        restored = self._assert_round_trip(error)
        expected_state, actual_state = vars(error).copy(), vars(restored).copy()
        expected_cause = expected_state.pop(cause_attribute)
        actual_cause = actual_state.pop(cause_attribute)
        self.assertIs(type(actual_cause), type(expected_cause))
        self.assertEqual(actual_cause.args, expected_cause.args)
        self.assertEqual(str(actual_cause), str(expected_cause))
        self.assertEqual(actual_state, expected_state)

    def test_saved_message_survives_attribute_changes(self):
        error = Timeout("job", "first")
        error.message = "second"
        restored = self._assert_round_trip(error)
        self.assertEqual(restored.message, "second")

    def test_saved_message_survives_cause_reconstruction(self):
        # urllib3 drops the pool from a timeout when it is unpickled.
        cause = requests.ReadTimeout(
            urllib3.exceptions.ReadTimeoutError(
                urllib3.HTTPConnectionPool("ais.example"), "/obj", "read timed out"
            )
        )
        error = ETLDirectPutTransientError("http://target/obj", cause)
        restored = self._assert_round_trip(error)
        self.assertIsInstance(restored.cause, requests.ReadTimeout)
