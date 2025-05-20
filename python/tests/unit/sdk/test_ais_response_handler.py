import unittest

from aistore.sdk.errors import (
    AISError,
    ErrRemoteBckNotFound,
    ErrBckNotFound,
    ErrBckAlreadyExists,
    ErrObjNotFound,
    ErrETLAlreadyExists,
    ErrETLNotFound,
)
from aistore.sdk.response_handler import AISResponseHandler
from tests.utils import cases, handler_parse_and_assert


# pylint: disable=unused-variable
class TestAISResponseHandler(unittest.TestCase):
    @cases(
        ("", AISError, None),
        ("generic error message", AISError, 500),
        ("generic error message", AISError, 399),
        ('aws bucket "s3://test-bck" does not exist', ErrRemoteBckNotFound, 404),
        ('remote bucket "ais://@test-bck" does not exist', ErrRemoteBckNotFound, 404),
        ('bucket "ais://test-bck" does not exist', ErrBckNotFound, 404),
        ('bucket "ais://test-bck" already exists', ErrBckAlreadyExists, 409),
        ("ais://test-bck/test-obj does not exist", ErrObjNotFound, 404),
        ("ais://test-bck/ test-obj does not exist", ErrObjNotFound, 404),
        ("ais://test-bck/こんにちは世界 does not exist", ErrObjNotFound, 404),
        (
            "ais://test-bck/!@$%^&*()-_=+[{]}\\|;:'\",<.>/`~?# does not exist",
            ErrObjNotFound,
            404,
        ),
        ("etl job test-etl-job already exists", ErrETLAlreadyExists, 409),
        ("etl job test-etl-job does not exist", ErrETLNotFound, 404),
    )
    def test_parse_ais_error(self, test_case):
        handler_parse_and_assert(self, AISResponseHandler(), AISError, test_case)
