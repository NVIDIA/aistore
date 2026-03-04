import os
import sys
import base64
import unittest
from unittest.mock import Mock
from unittest.mock import patch

from starlette.testclient import TestClient

import aistore
from aistore.sdk.const import (
    HTTP_METHOD_PUT,
    HTTP_METHOD_GET,
    HTTP_METHOD_POST,
    HTTP_METHOD_DELETE,
    URL_PATH_ETL,
    UTF_ENCODING,
    QPARAM_UUID,
    QPARAM_ETL_FQN,
)
from aistore.sdk.errors import AISError
from aistore.sdk.etl.etl import Etl, _get_runtime
from aistore.sdk.etl.etl_config import ETLConfig
from aistore.sdk.etl.etl_const import (
    ETL_COMM_HPUSH,
    ETL_COMM_HPULL,
    DEFAULT_ETL_TIMEOUT,
    DEFAULT_ETL_OBJ_TIMEOUT,
)
from aistore.sdk.etl.webserver import serialize_class
from aistore.sdk.etl.webserver.fastapi_server import FastAPIServer
from aistore.sdk.etl.webserver.http_multi_threaded_server import HTTPMultiThreadedServer
from aistore.sdk.types import ETLDetails
from aistore.sdk.utils import convert_to_seconds

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

    @unittest.skipIf(
        sys.version_info < (3, 9) or sys.version_info >= (3, 14),
        "requires Python 3.9 to 3.13 inclusive",
    )
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
        self.assertEqual(init_kwargs["comm_type"], ETL_COMM_HPUSH)
        self.assertEqual(init_kwargs["init_timeout"], DEFAULT_ETL_TIMEOUT)
        self.assertEqual(init_kwargs["obj_timeout"], DEFAULT_ETL_OBJ_TIMEOUT)
        self.assertTrue(init_kwargs["direct_put"])

        # The serialized class must match what our util would produce
        expected_payload = serialize_class(MyETL)
        self.assertEqual(init_kwargs["ETL_CLASS_PAYLOAD"], expected_payload)

        # No dependencies given => empty PACKAGES
        self.assertNotIn("PACKAGES", init_kwargs)

    @unittest.skipIf(
        sys.version_info < (3, 9) or sys.version_info >= (3, 14),
        "requires Python 3.9 to 3.13 inclusive",
    )
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
        self.assertTrue(init_kwargs["direct_put"])
        self.assertEqual(init_kwargs["CUSTOM"], "value")

        # dependencies should be comma-joined
        self.assertEqual(init_kwargs["PACKAGES"], "foo,bar")

        expected_payload = serialize_class(AnotherETL)
        self.assertEqual(init_kwargs["ETL_CLASS_PAYLOAD"], expected_payload)

    @unittest.skipIf(
        sys.version_info < (3, 9) or sys.version_info >= (3, 14),
        "requires Python 3.9 to 3.13 inclusive",
    )
    def test_init_class_direct_file_access(self):
        class DFAServer(HTTPMultiThreadedServer):
            def transform(self, data, *_args) -> bytes:
                return data if isinstance(data, bytes) else open(data, "rb").read()

        self.etl.init = Mock(return_value="job-dfa")
        decorator = self.etl.init_class(direct_file_access=True)
        decorator(DFAServer)

        _, init_kwargs = self.etl.init.call_args
        self.assertTrue(init_kwargs["direct_file_access"])

    def test_decorator_non_subclass_raises(self):
        decorator = self.etl.init_class()
        with self.assertRaises(TypeError):
            decorator(str)  # str is not an ETLServer subclass

    def test_view(self):
        mock_response = Mock()
        self.mock_client.request_deserialize.return_value = mock_response
        job_id = "mock-job-id"
        response = self.etl.view(job_id=job_id)
        self.assertEqual(mock_response, response)
        self.mock_client.request_deserialize.assert_called_with(
            HTTP_METHOD_GET,
            path=f"etl/{ self.etl_name }",
            res_model=ETLDetails,
            params={QPARAM_UUID: job_id},
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
        expected_action = {
            "name": self.etl_name,
            "communication": f"{ETL_COMM_HPUSH}://",
            "init_timeout": DEFAULT_ETL_TIMEOUT,
            "obj_timeout": DEFAULT_ETL_OBJ_TIMEOUT,
            "support_direct_put": False,
            "runtime": {
                "image": image,
            },
        }

        mock_resp = Mock()
        mock_resp.text = "job-123"
        self.mock_client.request.return_value = mock_resp

        result = self.etl.init(image)
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
        direct = True

        expected_action = {
            "name": self.etl_name,
            "communication": f"{comm_type}://",
            "init_timeout": init_t,
            "obj_timeout": obj_t,
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

    def test_init_direct_file_access(self):
        image = "img-dfa"
        mock_resp = Mock()
        mock_resp.text = "job-dfa"
        self.mock_client.request.return_value = mock_resp

        self.etl.init(image, direct_file_access=True)

        _, call_kwargs = self.mock_client.request.call_args
        env = call_kwargs["json"]["runtime"]["env"]
        self.assertIn({"name": "ETL_DIRECT_FQN", "value": "true"}, env)

    def test_init_direct_file_access_false_by_default(self):
        image = "img-no-dfa"
        mock_resp = Mock()
        mock_resp.text = "job-no-dfa"
        self.mock_client.request.return_value = mock_resp

        self.etl.init(image)

        _, call_kwargs = self.mock_client.request.call_args
        self.assertIsNone(call_kwargs["json"]["runtime"].get("env"))

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


class TestDirectFileAccessEndToEnd(unittest.TestCase):
    """
    Verify that direct_file_access=True in Etl.init() actually causes transform()
    to receive a str filepath rather than bytes when the server handles a request
    with an FQN.

    This chains the full path:
      Etl.init(direct_file_access=True)
        → ETL_DIRECT_FQN=true injected into container env vars
          → ETLServer reads os.getenv("ETL_DIRECT_FQN")
            → request with FQN query param
              → transform() receives str (filepath), not bytes
    """

    def setUp(self):
        self.mock_client = Mock()
        self.mock_client.request.return_value = Mock(text="job-e2e")
        self.etl = Etl(self.mock_client, ETL_NAME)

    def tearDown(self):
        os.environ.pop("ETL_DIRECT_FQN", None)
        os.environ.pop("AIS_TARGET_URL", None)

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    def test_direct_file_access_arg_causes_transform_to_receive_str(self):
        """
        Calling init(direct_file_access=True) injects ETL_DIRECT_FQN=true into the
        container env. Simulate the container booting with that env and verify that
        a request carrying an FQN results in transform() receiving a str path.
        """
        # 1. Call init() and capture what env vars would be injected into the pod.
        self.etl.init("some-image", direct_file_access=True)
        _, call_kwargs = self.mock_client.request.call_args
        env_list = call_kwargs["json"]["runtime"]["env"]
        injected = {e["name"]: e["value"] for e in env_list}
        self.assertIn("ETL_DIRECT_FQN", injected, "ETL_DIRECT_FQN must be injected")
        self.assertEqual(injected["ETL_DIRECT_FQN"], "true")

        # 2. Simulate the container environment receiving those injected env vars.
        for name, value in injected.items():
            os.environ[name] = value
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"

        # 3. Boot a server as the container would — it reads ETL_DIRECT_FQN from env.
        received = []

        class ProbeServer(FastAPIServer):
            def transform(self, data, _path, _etl_args):
                received.append(data)
                return data.encode() if isinstance(data, str) else data

        server = ProbeServer()
        client = TestClient(server.app)

        # 4. Send a request with an FQN (as AIS target would in hpush mode).
        fqn = "/mnt/ais/local/bucket/object.bin"
        client.put("/bucket/object.bin", content=b"", params={QPARAM_ETL_FQN: fqn})

        # 5. transform() must have received the file path as str, not bytes.
        self.assertEqual(len(received), 1)
        self.assertIsInstance(
            received[0],
            str,
            "transform() should receive str filepath when direct_file_access=True",
        )
        self.assertEqual(received[0], os.path.normpath(fqn))

    @unittest.skipIf(sys.version_info < (3, 9), "requires Python 3.9 or higher")
    def test_without_direct_file_access_transform_receives_bytes(self):
        """
        Without direct_file_access, ETL_DIRECT_FQN is never set, so transform()
        must always receive bytes even when an FQN query param is present.
        """
        # init() without direct_file_access — no ETL_DIRECT_FQN in the payload.
        self.etl.init("some-image")
        _, call_kwargs = self.mock_client.request.call_args
        runtime = call_kwargs["json"]["runtime"]
        env_list = runtime.get("env") or []
        injected_names = {e["name"] for e in env_list}
        self.assertNotIn("ETL_DIRECT_FQN", injected_names)

        # Boot the server without ETL_DIRECT_FQN set.
        os.environ["AIS_TARGET_URL"] = "http://localhost:8080"

        received = []

        class ProbeServer(FastAPIServer):
            def transform(self, data, _path, _etl_args):
                received.append(data)
                return data if isinstance(data, bytes) else data.encode()

        server = ProbeServer()
        client = TestClient(server.app)

        # No FQN — plain hpush with body bytes (e.g. pipeline intermediate stage).
        body = b"some object bytes"
        client.put("/bucket/object.bin", content=body)

        self.assertEqual(len(received), 1)
        self.assertIsInstance(
            received[0],
            bytes,
            "transform() should receive bytes when direct_file_access is not set",
        )
        self.assertEqual(received[0], body)


class TestETLConfig(unittest.TestCase):
    """Test ETLConfig with positional arguments."""

    def test_no_arguments(self):
        """Test creating ETLConfig with no arguments."""
        config = ETLConfig()
        self.assertIsNone(config.name)
        self.assertIsNone(config.args)

    def test_positional_name_only(self):
        """Test creating ETLConfig with name as positional argument."""
        config = ETLConfig("test-etl")
        self.assertEqual(config.name, "test-etl")
        self.assertIsNone(config.args)

    def test_positional_name_and_args(self):
        """Test creating ETLConfig with both name and args as positional arguments."""
        config = ETLConfig("test-etl", {"key": "value"})
        self.assertEqual(config.name, "test-etl")
        self.assertEqual(config.args, {"key": "value"})

    def test_mixed_positional_keyword(self):
        """Test creating ETLConfig with name positional and args as keyword."""
        config = ETLConfig("test-etl", args="string-args")
        self.assertEqual(config.name, "test-etl")
        self.assertEqual(config.args, "string-args")
