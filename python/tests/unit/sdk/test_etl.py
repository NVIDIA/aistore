import base64
import unittest
from unittest.mock import Mock
from unittest.mock import patch

import cloudpickle

import aistore
from aistore.sdk.const import (
    HTTP_METHOD_PUT,
    HTTP_METHOD_GET,
    HTTP_METHOD_POST,
    HTTP_METHOD_DELETE,
    URL_PATH_ETL,
    UTF_ENCODING,
)
from aistore.sdk.etl.etl_const import (
    CODE_TEMPLATE,
    ETL_COMM_HPUSH,
    ETL_COMM_HPULL,
    ETL_COMM_IO,
    DEFAULT_ETL_TIMEOUT,
    DEFAULT_ETL_OBJ_TIMEOUT,
)

from aistore.sdk.etl.etl import Etl, _get_default_runtime
from aistore.sdk.types import ETLDetails
from aistore.sdk.utils import convert_to_seconds
from tests.const import ETL_NAME


class TestEtl(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.mock_client = Mock()
        self.etl_name = ETL_NAME
        self.etl = Etl(self.mock_client, self.etl_name)

    def test_init_spec_default_params(self):
        expected_action = {
            "communication": "hpush://",
            "init_timeout": DEFAULT_ETL_TIMEOUT,
            "obj_timeout": DEFAULT_ETL_OBJ_TIMEOUT,
            "argument": "",
        }
        self.init_spec_exec_assert(expected_action)

    def test_init_spec_invalid_comm(self):
        with self.assertRaises(ValueError):
            self.etl.init_spec("template", communication_type="invalid")

    def test_init_spec(self):
        communication_type = ETL_COMM_HPUSH
        init_timeout = "6m"
        obj_timeout = "20s"
        expected_action = {
            "communication": f"{communication_type}://",
            "init_timeout": init_timeout,
            "obj_timeout": obj_timeout,
            "argument": "",
        }
        self.init_spec_exec_assert(
            expected_action, init_timeout=init_timeout, obj_timeout=obj_timeout
        )

    def init_spec_exec_assert(self, expected_action, **kwargs):
        template = "pod spec template"
        expected_action["spec"] = base64.b64encode(
            template.encode(UTF_ENCODING)
        ).decode(UTF_ENCODING)
        expected_action["id"] = self.etl_name

        expected_response_text = self.etl_name
        mock_response = Mock()
        mock_response.text = expected_response_text
        self.mock_client.request.return_value = mock_response

        response = self.etl.init_spec(template, **kwargs)

        self.assertEqual(expected_response_text, response)

        # All ETL messages are called with timeout
        if "init_timeout" not in kwargs:
            kwargs["init_timeout"] = DEFAULT_ETL_TIMEOUT

        req_timeout = convert_to_seconds(kwargs.pop("init_timeout"))

        self.mock_client.request.assert_called_with(
            HTTP_METHOD_PUT,
            path=URL_PATH_ETL,
            timeout=req_timeout,
            json=expected_action,
        )

    def test_init_code_default_runtime(self):
        version_to_runtime = {
            (3, 7): "python3.13v2",
            (3, 1234): "python3.13v2",
            (3, 8): "python3.13v2",
            (3, 10): "python3.10v2",
            (3, 11): "python3.11v2",
            (3, 12): "python3.12v2",
            (3, 13): "python3.13v2",
        }
        for version, runtime in version_to_runtime.items():
            with patch.object(aistore.sdk.etl.etl.sys, "version_info") as version_info:
                version_info.major = version[0]
                version_info.minor = version[1]
                self.assertEqual(runtime, _get_default_runtime())

    def test_init_code_default_params(self):
        communication_type = ETL_COMM_HPUSH

        expected_action = {
            "runtime": _get_default_runtime(),
            "communication": f"{communication_type}://",
            "init_timeout": DEFAULT_ETL_TIMEOUT,
            "obj_timeout": DEFAULT_ETL_OBJ_TIMEOUT,
            "funcs": {"transform": "transform"},
            "code": self.encode_fn([], self.transform_fn, communication_type),
            "dependencies": base64.b64encode(b"cloudpickle>=3.0.0").decode(
                UTF_ENCODING
            ),
            "argument": "",
        }
        self.init_code_exec_assert(expected_action)

    def test_init_code_invalid_comm(self):
        with self.assertRaises(ValueError):
            self.etl.init_code(Mock(), communication_type="invalid")

    def test_init_code(self):
        runtime = "python-non-default"
        communication_type = ETL_COMM_HPULL
        init_timeout = "6m"
        obj_timeout = "6m"
        preimported = ["pytorch"]
        user_dependencies = ["pytorch"]
        chunk_size = 123
        arg_type = "url"

        expected_dependencies = user_dependencies.copy()
        expected_dependencies.append("cloudpickle>=3.0.0")
        expected_dep_str = base64.b64encode(
            "\n".join(expected_dependencies).encode(UTF_ENCODING)
        ).decode(UTF_ENCODING)

        expected_action = {
            "runtime": runtime,
            "communication": f"{communication_type}://",
            "init_timeout": init_timeout,
            "obj_timeout": obj_timeout,
            "funcs": {"transform": "transform"},
            "code": self.encode_fn(preimported, self.transform_fn, communication_type),
            "dependencies": expected_dep_str,
            "chunk_size": chunk_size,
            "argument": arg_type,
        }
        self.init_code_exec_assert(
            expected_action,
            preimported_modules=preimported,
            dependencies=user_dependencies,
            runtime=runtime,
            communication_type=communication_type,
            init_timeout=init_timeout,
            obj_timeout=obj_timeout,
            chunk_size=chunk_size,
            arg_type=arg_type,
        )

    @staticmethod
    def transform_fn():
        print("example action")

    @staticmethod
    def encode_fn(preimported_modules, func, comm_type):
        transform = base64.b64encode(cloudpickle.dumps(func)).decode(UTF_ENCODING)
        io_comm_context = "transform()" if comm_type == ETL_COMM_IO else ""
        template = CODE_TEMPLATE.format(
            preimported_modules, transform, io_comm_context
        ).encode(UTF_ENCODING)
        return base64.b64encode(template).decode(UTF_ENCODING)

    def init_code_exec_assert(self, expected_action, **kwargs):
        expected_action["id"] = self.etl_name

        expected_response_text = "response text"
        mock_response = Mock()
        mock_response.text = expected_response_text
        self.mock_client.request.return_value = mock_response

        response = self.etl.init_code(transform=self.transform_fn, **kwargs)

        self.assertEqual(expected_response_text, response)

        if "init_timeout" not in kwargs:
            kwargs["init_timeout"] = DEFAULT_ETL_TIMEOUT
        req_timeout = convert_to_seconds(kwargs.pop("init_timeout"))

        self.mock_client.request.assert_called_with(
            HTTP_METHOD_PUT,
            path=URL_PATH_ETL,
            timeout=req_timeout,
            json=expected_action,
        )

    def test_view(self):
        mock_response = Mock()
        self.mock_client.request_deserialize.return_value = mock_response
        response = self.etl.view()
        self.assertEqual(mock_response, response)
        self.mock_client.request_deserialize.assert_called_with(
            HTTP_METHOD_GET, path=f"etl/{ self.etl_name }", res_model=ETLDetails
        )

    def test_start(self):
        self.etl.start()
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST,
            path=f"etl/{ self.etl_name }/start",
            timeout=convert_to_seconds(DEFAULT_ETL_TIMEOUT),
        )

    def test_stop(self):
        self.etl.stop()
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST,
            path=f"etl/{ self.etl_name }/stop",
            timeout=convert_to_seconds(DEFAULT_ETL_TIMEOUT),
        )

    def test_delete(self):
        self.etl.delete()
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_DELETE,
            path=f"etl/{ self.etl_name }",
            timeout=convert_to_seconds(DEFAULT_ETL_TIMEOUT),
        )

    def test_valid_names(self):
        valid_names = [
            "validname",
            "valid-name",
            "name123",
            "123name",
            "name-123",
            "validname123",
            "a" * 6,  # exactly 6 characters
            "a" * 32,  # exactly 32 characters
        ]
        for name in valid_names:
            try:
                Etl.validate_etl_name(name)
            except ValueError:
                self.fail(f"Valid name '{name}' raised ValueError unexpectedly!")

    def test_invalid_names(self):
        invalid_names = [
            "",
            "a" * 5,  # 5 characters, too short
            "a" * 33,  # 33 characters, too long
            "InvalidName",
            "invalid_name",
            "invalid name",
            "invalid-name-",
            "-invalid-name",
            "-invalid-name-",
            "------------",
            "invalid$name",
            "invalid@name",
        ]

        for name in invalid_names:
            with self.assertRaises(ValueError):
                Etl.validate_etl_name(name)
