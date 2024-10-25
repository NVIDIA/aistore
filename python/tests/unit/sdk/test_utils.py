import json
import unittest
from unittest.mock import Mock, patch, mock_open

from msgspec import msgpack
from requests import Response

from aistore.sdk import utils
from aistore.sdk.const import MSGPACK_CONTENT_TYPE, HEADER_CONTENT_TYPE
from aistore.sdk.errors import (
    AISError,
    ErrRemoteBckNotFound,
    ErrBckNotFound,
    ErrBckAlreadyExists,
    ErrETLAlreadyExists,
)
from tests.const import PREFIX_NAME
from tests.utils import cases


# pylint: disable=unused-variable
class TestUtils(unittest.TestCase):
    def test_handle_error_no_text(self):
        mock_response = Mock(text="")

        utils.handle_errors(mock_response)
        mock_response.raise_for_status.assert_called()

    def test_handle_error_decode_err(self):
        err_status = 300
        err_msg = "error message iso-8859-1"
        expected_text = json.dumps({"status": err_status, "message": err_msg})
        # Fail initial decoding, then return the decoded text
        decode_err = UnicodeDecodeError("1", b"2", 3, 4, "5")
        mock_iso_text = Mock(spec=bytes)
        mock_iso_text.decode.side_effect = [decode_err, expected_text]
        self.handle_err_exec_assert(AISError, err_status, err_msg, mock_iso_text)

    @cases(399, 500)
    def test_handle_error_ais_err(self, err_status):
        err_msg = "error message"
        expected_text = json.dumps({"status": err_status, "message": err_msg})
        mock_text = Mock(spec=bytes)
        mock_text.decode.return_value = expected_text
        self.handle_err_exec_assert(AISError, err_status, err_msg, mock_text)

    @cases(
        ("cloud bucket does not exist", ErrRemoteBckNotFound),
        ("remote bucket does not exist", ErrRemoteBckNotFound),
        ("bucket does not exist", ErrBckNotFound),
        ("bucket already exists", ErrBckAlreadyExists),
        ("etl already exists", ErrETLAlreadyExists),
    )
    def test_handle_error_no_remote_bucket(self, test_case):
        err_msg, expected_err = test_case
        err_status = 400
        expected_text = json.dumps({"status": err_status, "message": err_msg})
        mock_text = Mock(spec=bytes)
        mock_text.decode.return_value = expected_text
        self.handle_err_exec_assert(expected_err, err_status, err_msg, mock_text)

    def handle_err_exec_assert(self, err_type, err_status, err_msg, mock_err_text):
        mock_response = Mock(text=mock_err_text)
        with self.assertRaises(err_type) as context:
            utils.handle_errors(mock_response)
        self.assertEqual(err_msg, context.exception.message)
        self.assertEqual(err_status, context.exception.status_code)

    @cases((0, 0.1), (-1, 0.1), (64, 1), (128, 2), (100000, 1562.5))
    def test_probing_frequency(self, test_case):
        self.assertEqual(test_case[1], utils.probing_frequency(test_case[0]))

    @patch("pathlib.Path.is_file")
    @patch("pathlib.Path.exists")
    def test_validate_file(self, mock_exists, mock_is_file):
        mock_exists.return_value = False
        with self.assertRaises(ValueError):
            utils.validate_file("any path")
        mock_exists.return_value = True
        mock_is_file.return_value = False
        with self.assertRaises(ValueError):
            utils.validate_file("any path")
        mock_is_file.return_value = True
        utils.validate_file("any path")

    @patch("pathlib.Path.is_dir")
    @patch("pathlib.Path.exists")
    def test_validate_dir(self, mock_exists, mock_is_dir):
        mock_exists.return_value = False
        with self.assertRaises(ValueError):
            utils.validate_directory("any path")
        mock_exists.return_value = True
        mock_is_dir.return_value = False
        with self.assertRaises(ValueError):
            utils.validate_directory("any path")
        mock_is_dir.return_value = True
        utils.validate_directory("any path")

    def test_read_file_bytes(self):
        data = b"Test data"
        with patch("builtins.open", mock_open(read_data=data)):
            res = utils.read_file_bytes("any path")
        self.assertEqual(data, res)

    @cases((123, "123 Bytes"), (None, "unknown"))
    def test_get_file_size(self, test_case):
        mock_file = Mock()
        mock_file.stat.return_value = Mock(st_size=test_case[0])
        self.assertEqual(test_case[1], utils.get_file_size(mock_file))

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
            self.assertEqual(output, list(utils.expand_braces(input_str)))
        else:
            with self.assertRaises(expected_error):
                utils.expand_braces(input_str)

    @patch("aistore.sdk.utils.parse_raw_as")
    def test_decode_response_json(self, mock_parse):
        response_content = "text content"
        parsed_content = "parsed content"
        mock_response = Mock(Response)
        mock_response.headers = {}
        mock_response.text = response_content
        mock_parse.return_value = parsed_content

        res = utils.decode_response(str, mock_response)

        self.assertEqual(parsed_content, res)
        mock_parse.assert_called_with(str, response_content)

    def test_decode_response_msgpack(self):
        unpacked_content = {"content key": "content value"}
        packed_content = msgpack.encode(unpacked_content)
        mock_response = Mock(Response)
        mock_response.headers = {HEADER_CONTENT_TYPE: MSGPACK_CONTENT_TYPE}
        mock_response.content = packed_content

        res = utils.decode_response(dict, mock_response)

        self.assertEqual(unpacked_content, res)
