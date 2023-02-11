import json
import unittest
from unittest.mock import Mock, patch, mock_open

from aistore.sdk import utils
from aistore.sdk.errors import AISError, ErrRemoteBckNotFound, ErrBckNotFound


def test_cases(*args):
    def decorator(func):
        def wrapper(self, *inner_args, **kwargs):
            for arg in args:
                with self.subTest(arg=arg):
                    func(self, arg, *inner_args, **kwargs)

        return wrapper

    return decorator


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

    @test_cases(399, 500)
    def test_handle_error_ais_err(self, err_status):
        err_msg = "error message"
        expected_text = json.dumps({"status": err_status, "message": err_msg})
        mock_text = Mock(spec=bytes)
        mock_text.decode.return_value = expected_text
        self.handle_err_exec_assert(AISError, err_status, err_msg, mock_text)

    @test_cases("cloud bucket does not exist", "remote bucket does not exist")
    def test_handle_error_no_remote_bucket(self, err_msg):
        err_status = 400
        expected_text = json.dumps({"status": err_status, "message": err_msg})
        mock_text = Mock(spec=bytes)
        mock_text.decode.return_value = expected_text
        self.handle_err_exec_assert(
            ErrRemoteBckNotFound, err_status, err_msg, mock_text
        )

    def test_handle_error_no_bucket(self):
        err_msg = "bucket does not exist"
        err_status = 400
        expected_text = json.dumps({"status": err_status, "message": err_msg})
        mock_text = Mock(spec=bytes)
        mock_text.decode.return_value = expected_text
        self.handle_err_exec_assert(ErrBckNotFound, 400, err_msg, mock_text)

    def handle_err_exec_assert(self, err_type, err_status, err_msg, mock_err_text):
        mock_response = Mock(text=mock_err_text)
        with self.assertRaises(err_type) as context:
            utils.handle_errors(mock_response)
        self.assertEqual(err_msg, context.exception.message)
        self.assertEqual(err_status, context.exception.status_code)

    @test_cases((0, 0.1), (-1, 0.1), (64, 1), (128, 2), (100000, 1562.5))
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
