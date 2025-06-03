import sys
import base64
import unittest
from unittest.mock import Mock
from unittest.mock import patch

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
    ETL_COMM_HPUSH,
    ETL_COMM_HPULL,
    DEFAULT_ETL_TIMEOUT,
    DEFAULT_ETL_OBJ_TIMEOUT,
    PYTHON_RUNTIME_CMD,
)

from aistore.sdk.types import ETLDetails
from aistore.sdk.utils import convert_to_seconds
from aistore.sdk.etl.etl import Etl, _get_runtime
from aistore.sdk.etl.webserver.http_multi_threaded_server import HTTPMultiThreadedServer
from aistore.sdk.etl.webserver import serialize_class
from aistore.sdk.errors import AISError

from tests.const import ETL_NAME


class TestEtl(
    unittest.TestCase
):  # pylint: disable=unused-variable, too-many-public-methods
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
        expected_action["name"] = self.etl_name

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

    def test_default_runtime(self):
        version_to_runtime = {
            (3, 9): "3.9",
            (3, 10): "3.10",
            (3, 11): "3.11",
            (3, 12): "3.12",
            (3, 13): "3.13",
        }

        failed_versions = [
            (3, 8),  # Too old, not supported
            (3, 14),  # TODO: Not supported yet
            (4, 0),  # Future version
        ]
        for version, runtime in version_to_runtime.items():
            with patch.object(aistore.sdk.etl.etl.sys, "version_info") as version_info:
                version_info.major = version[0]
                version_info.minor = version[1]
                self.assertEqual(runtime, _get_runtime())

        for version in failed_versions:
            with patch.object(aistore.sdk.etl.etl.sys, "version_info") as version_info:
                version_info.major = version[0]
                version_info.minor = version[1]
                with self.assertRaises(ValueError):
                    _get_runtime()

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    def test_init_etl_class_with_defaults(self):
        # Define a minimal ETLServer subclass
        class MyETL(HTTPMultiThreadedServer):
            def transform(self, data: bytes, *_args) -> bytes:
                return data.upper()

        # 1) Replace init with a mock before we apply the decorator
        self.etl.init = Mock(return_value="job-xyz")

        # 2) Get the decorator and apply it
        decorator = self.etl.init_class()
        returned = decorator(MyETL)
        self.assertIs(returned, MyETL, "Decorator should return the original class")

        # 3) Now we can inspect the call_args on the mock
        _, init_kwargs = self.etl.init.call_args

        # Required fields:
        self.assertEqual(
            init_kwargs["image"], f"aistorage/runtime_python:{_get_runtime()}"
        )
        self.assertEqual(init_kwargs["command"], PYTHON_RUNTIME_CMD)
        self.assertEqual(init_kwargs["comm_type"], ETL_COMM_HPUSH)
        self.assertEqual(init_kwargs["init_timeout"], DEFAULT_ETL_TIMEOUT)
        self.assertEqual(init_kwargs["obj_timeout"], DEFAULT_ETL_OBJ_TIMEOUT)
        self.assertEqual(init_kwargs["arg_type"], "")
        self.assertTrue(init_kwargs["direct_put"])

        # The serialized class must match what our util would produce
        expected_payload = serialize_class(MyETL)
        self.assertEqual(init_kwargs["ETL_CLASS_PAYLOAD"], expected_payload)

        # No dependencies given => empty PACKAGES
        self.assertEqual(init_kwargs["PACKAGES"], "")

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    def test_init_etl_class_with_args(self):
        class AnotherETL(HTTPMultiThreadedServer):
            def transform(self, data: bytes, *_args) -> bytes:
                return data

        # Replace init with a mock before we apply the decorator
        self.etl.init = Mock(return_value="job-xyz")

        decorator = self.etl.init_class(
            dependencies=["foo", "bar"],
            comm_type=ETL_COMM_HPULL,
            init_timeout="2m",
            obj_timeout="10s",
            arg_type="fqn",
            direct_put=True,
            CUSTOM="value",
        )
        returned = decorator(AnotherETL)
        self.assertIs(returned, AnotherETL)

        self.etl.init.assert_called_once()
        _, init_kwargs = self.etl.init.call_args

        # our customizations:
        self.assertEqual(init_kwargs["comm_type"], ETL_COMM_HPULL)
        self.assertEqual(init_kwargs["init_timeout"], "2m")
        self.assertEqual(init_kwargs["obj_timeout"], "10s")
        self.assertEqual(init_kwargs["arg_type"], "fqn")
        self.assertTrue(init_kwargs["direct_put"])
        self.assertEqual(init_kwargs["CUSTOM"], "value")

        # dependencies should be comma-joined
        self.assertEqual(init_kwargs["PACKAGES"], "foo,bar")

        expected_payload = serialize_class(AnotherETL)
        self.assertEqual(init_kwargs["ETL_CLASS_PAYLOAD"], expected_payload)

    def test_decorator_non_subclass_raises(self):
        decorator = self.etl.init_class()
        with self.assertRaises(TypeError):
            decorator(str)  # str is not an ETLServer subclass

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

    def test_init_default_params(self):
        image = "my-image"
        command = ["run", "me"]
        expected_action = {
            "name": self.etl_name,
            "communication": f"{ETL_COMM_HPUSH}://",
            "init_timeout": DEFAULT_ETL_TIMEOUT,
            "obj_timeout": DEFAULT_ETL_OBJ_TIMEOUT,
            "argument": "",
            "support_direct_put": False,
            "runtime": {
                "image": image,
                "command": command,
                "env": [],
            },
        }

        mock_resp = Mock()
        mock_resp.text = "job-123"
        self.mock_client.request.return_value = mock_resp

        result = self.etl.init(image, command)
        self.assertEqual("job-123", result)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_PUT,
            path=URL_PATH_ETL,
            timeout=convert_to_seconds(DEFAULT_ETL_TIMEOUT),
            json=expected_action,
        )

    def test_init_with_string_command_and_env(self):
        image = "img2"
        cmd_str = "echo hello world"
        expected_action = {
            "name": self.etl_name,
            "communication": f"{ETL_COMM_HPUSH}://",
            "init_timeout": DEFAULT_ETL_TIMEOUT,
            "obj_timeout": DEFAULT_ETL_OBJ_TIMEOUT,
            "argument": "",
            "support_direct_put": False,
            "runtime": {
                "image": image,
                "command": cmd_str.split(),
                "env": [{"name": "FOO", "value": "BAR"}],
            },
        }

        mock_resp = Mock()
        mock_resp.text = "job-456"
        self.mock_client.request.return_value = mock_resp

        result = self.etl.init(image, cmd_str, FOO="BAR")
        self.assertEqual("job-456", result)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_PUT,
            path=URL_PATH_ETL,
            timeout=convert_to_seconds(DEFAULT_ETL_TIMEOUT),
            json=expected_action,
        )

    def test_init_custom_params(self):
        image = "img3"
        command = ["run3"]
        comm_type = ETL_COMM_HPULL
        init_t = "10m"
        obj_t = "30s"
        arg_t = "url"
        direct = True

        expected_action = {
            "name": self.etl_name,
            "communication": f"{comm_type}://",
            "init_timeout": init_t,
            "obj_timeout": obj_t,
            "argument": arg_t,
            "support_direct_put": direct,
            "runtime": {
                "image": image,
                "command": command,
                "env": [{"name": "BAZ", "value": "QUX"}],
            },
        }

        mock_resp = Mock()
        mock_resp.text = "job-789"
        self.mock_client.request.return_value = mock_resp

        result = self.etl.init(
            image,
            command,
            comm_type=comm_type,
            init_timeout=init_t,
            obj_timeout=obj_t,
            arg_type=arg_t,
            direct_put=direct,
            BAZ="QUX",
        )
        self.assertEqual("job-789", result)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_PUT,
            path=URL_PATH_ETL,
            timeout=convert_to_seconds(init_t),
            json=expected_action,
        )

    def test_init_invalid_comm(self):
        with self.assertRaises(ValueError):
            self.etl.init("img", ["cmd"], comm_type="not-a-valid-type")

    def test_context_manager_calls_stop_and_delete(self):
        self.etl.stop = Mock()
        self.etl.delete = Mock()

        with self.etl:
            pass

        self.etl.stop.assert_called_once()
        self.etl.delete.assert_called_once()

    def test_context_manager_handles_exception(self):
        self.etl.stop = Mock()
        self.etl.delete = Mock(
            side_effect=AISError(
                status_code=500, message="Delete failed", req=None, req_url=""
            )
        )
        try:
            with self.etl:
                pass
        except Exception:
            self.fail("Exception should be suppressed by __exit__")

        self.etl.stop.assert_called_once()
        self.etl.delete.assert_called_once()
