#
# Copyright (c) 2022-2025, NVIDIA CORPORATION. All rights reserved.
#

import json
import unittest
from unittest.mock import Mock, patch, mock_open

from msgspec import msgpack
from requests import Response

from aistore.sdk.authn.utils import parse_authn_error
from aistore.sdk.const import MSGPACK_CONTENT_TYPE, HEADER_CONTENT_TYPE, XX_HASH_SEED
from aistore.sdk.errors import (
    AISError,
    ErrRemoteBckNotFound,
    ErrBckNotFound,
    ErrBckAlreadyExists,
    ErrObjNotFound,
    ErrETLAlreadyExists,
    ErrETLNotFound,
)
from aistore.sdk.utils import (
    decode_response,
    expand_braces,
    get_file_size,
    handle_errors,
    parse_ais_error,
    probing_frequency,
    read_file_bytes,
    validate_directory,
    validate_file,
    xoshiro256_hash,
    get_digest,
)
from aistore.sdk.authn.errors import (
    AuthNError,
    ErrUserNotFound,
    ErrUserAlreadyExists,
    ErrRoleNotFound,
    ErrRoleAlreadyExists,
    ErrClusterNotFound,
    ErrClusterAlreadyRegistered,
    ErrUserInvalidCredentials,
)
from tests.const import PREFIX_NAME
from tests.utils import cases, case_matrix


# pylint: disable=unused-variable
class TestUtils(unittest.TestCase):
    def _get_err_parse_fn(self, err_type):
        if issubclass(err_type, AuthNError):
            return parse_authn_error
        return parse_ais_error

    @cases(AISError, AuthNError)
    def test_handle_error_no_text(self, err_type):
        mock_response = Mock(text="")
        with self.assertRaises(err_type):
            handle_errors(mock_response, self._get_err_parse_fn(err_type))
        mock_response.raise_for_status.assert_called()

    @cases(AISError, AuthNError)
    def test_handle_error_decode_err(self, err_type):
        err_status = 300
        err_msg = "error message iso-8859-1"
        expected_text = json.dumps({"status": err_status, "message": err_msg})
        # Fail initial decoding, then return the decoded text
        decode_err = UnicodeDecodeError("1", b"2", 3, 4, "5")
        mock_iso_text = Mock(spec=bytes)
        mock_iso_text.decode.side_effect = [decode_err, expected_text]
        self.handle_err_exec_assert(err_type, err_status, err_msg, mock_iso_text)

    @case_matrix([399, 500], [AISError, AuthNError])
    def test_handle_error_base_errors(self, err_status, err_type):
        err_msg = "error message"
        expected_text = json.dumps({"status": err_status, "message": err_msg})
        mock_text = Mock(spec=bytes)
        mock_text.decode.return_value = expected_text
        self.handle_err_exec_assert(err_type, err_status, err_msg, mock_text)

    @cases(
        ('aws bucket "s3://test-bck" does not exist', ErrRemoteBckNotFound, 404),
        ('remote bucket "ais://@test-bck" does not exist', ErrRemoteBckNotFound, 404),
        ('bucket "ais://test-bck" does not exist', ErrBckNotFound, 404),
        ('bucket "ais://test-bck" already exists', ErrBckAlreadyExists, 409),
        ("ais://test-bck/test-obj does not exist", ErrObjNotFound, 404),
        ("etl job test-etl-job already exists", ErrETLAlreadyExists, 409),
        ("etl job test-etl-job does not exist", ErrETLNotFound, 404),
        ('user "test-user" does not exist', ErrUserNotFound, 404),
        ('user "test-user" already exists', ErrUserAlreadyExists, 409),
        ('role "test-role" does not exist', ErrRoleNotFound, 404),
        ('role "test-role" already exists', ErrRoleAlreadyExists, 409),
        ("cluster test-cluster does not exist", ErrClusterNotFound, 404),
        (
            "cluster OnBejJEpe[OnBejJEpe] already registered",
            ErrClusterAlreadyRegistered,
            409,
        ),
        ("invalid credentials", ErrUserInvalidCredentials, 401),
    )
    def test_handle_errors(self, test_case):
        err_msg, expected_err, err_status = test_case
        expected_text = json.dumps({"status": err_status, "message": err_msg})
        mock_text = Mock(spec=bytes)
        mock_text.decode.return_value = expected_text
        self.handle_err_exec_assert(expected_err, err_status, err_msg, mock_text)

    def handle_err_exec_assert(self, err_type, err_status, err_msg, mock_err_text):
        err_parse_fn = self._get_err_parse_fn(err_type)
        mock_response = Mock(text=mock_err_text)
        with self.assertRaises(err_type) as context:
            handle_errors(mock_response, err_parse_fn)
        self.assertEqual(err_msg, context.exception.message)
        self.assertEqual(err_status, context.exception.status_code)

    @cases((0, 0.1), (-1, 0.1), (64, 1), (128, 2), (100000, 1562.5))
    def test_probing_frequency(self, test_case):
        self.assertEqual(test_case[1], probing_frequency(test_case[0]))

    @patch("pathlib.Path.is_file")
    @patch("pathlib.Path.exists")
    def test_validate_file(self, mock_exists, mock_is_file):
        mock_exists.return_value = False
        with self.assertRaises(ValueError):
            validate_file("any path")
        mock_exists.return_value = True
        mock_is_file.return_value = False
        with self.assertRaises(ValueError):
            validate_file("any path")
        mock_is_file.return_value = True
        validate_file("any path")

    @patch("pathlib.Path.is_dir")
    @patch("pathlib.Path.exists")
    def test_validate_dir(self, mock_exists, mock_is_dir):
        mock_exists.return_value = False
        with self.assertRaises(ValueError):
            validate_directory("any path")
        mock_exists.return_value = True
        mock_is_dir.return_value = False
        with self.assertRaises(ValueError):
            validate_directory("any path")
        mock_is_dir.return_value = True
        validate_directory("any path")

    def test_read_file_bytes(self):
        data = b"Test data"
        with patch("builtins.open", mock_open(read_data=data)):
            res = read_file_bytes("any path")
        self.assertEqual(data, res)

    @cases((123, "123 Bytes"), (None, "unknown"))
    def test_get_file_size(self, test_case):
        mock_file = Mock()
        mock_file.stat.return_value = Mock(st_size=test_case[0])
        self.assertEqual(test_case[1], get_file_size(mock_file))

    @cases(
        (PREFIX_NAME, [PREFIX_NAME], None),
        ("prefix-{}", ["prefix-{}"], None),
        ("prefix-{0..1..2..3}", ["prefix-{0..1..2..3}"], None),
        ("prefix-{0..1..2}}", [], ValueError),
        (
            "prefix-{1..6..2}-gap-{12..14..1}-suffix",
            [
                "prefix-1-gap-12-suffix",
                "prefix-1-gap-13-suffix",
                "prefix-1-gap-14-suffix",
                "prefix-3-gap-12-suffix",
                "prefix-3-gap-13-suffix",
                "prefix-3-gap-14-suffix",
                "prefix-5-gap-12-suffix",
                "prefix-5-gap-13-suffix",
                "prefix-5-gap-14-suffix",
            ],
            None,
        ),
    )
    def test_expand_braces(self, test_case):
        input_str, output, expected_error = test_case
        if not expected_error:
            self.assertEqual(output, list(expand_braces(input_str)))
        else:
            with self.assertRaises(expected_error):
                expand_braces(input_str)

    @patch("aistore.sdk.utils.parse_raw_as")
    def test_decode_response_json(self, mock_parse):
        response_content = "text content"
        parsed_content = "parsed content"
        mock_response = Mock(Response)
        mock_response.headers = {}
        mock_response.text = response_content
        mock_parse.return_value = parsed_content

        res = decode_response(str, mock_response)

        self.assertEqual(parsed_content, res)
        mock_parse.assert_called_with(str, response_content)

    def test_decode_response_msgpack(self):
        unpacked_content = {"content key": "content value"}
        packed_content = msgpack.encode(unpacked_content)
        mock_response = Mock(Response)
        mock_response.headers = {HEADER_CONTENT_TYPE: MSGPACK_CONTENT_TYPE}
        mock_response.content = packed_content

        res = decode_response(dict, mock_response)

        self.assertEqual(unpacked_content, res)

    @cases(
        (123456789, 5288836854215336256),
        (0, 1905207664160064169),
        (2**64 - 1, 10227601306713020730),
    )
    def test_xoshiro256_hash(self, test_case):
        seed, expected_result = test_case
        result = xoshiro256_hash(seed)
        self.assertIsInstance(result, int)
        self.assertGreaterEqual(result, 0)
        self.assertLess(result, 2**64)  # Ensure 64-bit overflow behavior
        self.assertEqual(expected_result, result)

    @patch("aistore.sdk.utils.xxhash.xxh64")
    def test_get_digest(self, mock_xxhash):
        mock_xxhash.return_value.intdigest.return_value = 987654321
        name = "test_object"
        result = get_digest(name)
        mock_xxhash.assert_called_once_with(
            seed=XX_HASH_SEED, input=name.encode("utf-8")
        )
        self.assertEqual(result, 987654321)
