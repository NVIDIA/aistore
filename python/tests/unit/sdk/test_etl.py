import base64
import unittest
from typing import List
from unittest.mock import Mock
from unittest.mock import patch

import cloudpickle

import aistore
from aistore.sdk.const import (
    HTTP_METHOD_PUT,
    CODE_TEMPLATE,
    HTTP_METHOD_GET,
    HTTP_METHOD_POST,
    HTTP_METHOD_DELETE,
)
from aistore.sdk.etl import Etl, get_default_runtime
from aistore.sdk.types import ETL, ETLDetails


class TestEtl(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.mock_client = Mock()
        self.etl = Etl(self.mock_client)
        self.etl_name = "etl id"

    def test_properties(self):
        self.assertEqual(self.mock_client, self.etl.client)

    def test_init_spec_default_params(self):
        expected_action = {
            "communication": "hpush://",
            "timeout": "5m",
        }
        self.init_spec_exec_assert(expected_action)

    def test_init_spec(self):
        communication_type = "io"
        timeout = "6m"
        expected_action = {
            "communication": f"{communication_type}://",
            "timeout": timeout,
        }
        self.init_spec_exec_assert(
            expected_action, communication_type=communication_type, timeout=timeout
        )

    def init_spec_exec_assert(self, expected_action, **kwargs):
        template = "pod spec template"
        expected_action["spec"] = base64.b64encode(template.encode("utf-8")).decode(
            "utf-8"
        )
        expected_action["id"] = self.etl_name
        expected_response_text = "response text"
        mock_response = Mock()
        mock_response.text = expected_response_text
        self.mock_client.request.return_value = mock_response

        response = self.etl.init_spec(template, self.etl_name, **kwargs)

        self.assertEqual(expected_response_text, response)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_PUT, path="etl", json=expected_action
        )

    def test_init_code_default_runtime(self):
        version_to_runtime = {
            (3, 7): "python3.8v2",
            (3, 1234): "python3.8v2",
            (3, 8): "python3.8v2",
            (3, 10): "python3.10v2",
            (3, 11): "python3.11v2",
        }
        for version, runtime in version_to_runtime.items():
            with patch.object(aistore.sdk.etl.sys, "version_info") as version_info:
                version_info.major = version[0]
                version_info.minor = version[1]
                self.assertEqual(runtime, get_default_runtime())

    def test_init_code_default_params(self):
        communication_type = "hpush"

        expected_action = {
            "runtime": get_default_runtime(),
            "communication": f"{communication_type}://",
            "timeout": "5m",
            "funcs": {"transform": "transform"},
            "code": self.encode_fn(self.transform_fn, communication_type),
            "dependencies": base64.b64encode(b"cloudpickle==2.2.0").decode("utf-8"),
        }
        self.init_code_exec_assert(expected_action)

    def test_init_code(self):
        runtime = "python-non-default"
        communication_type = "hpull"
        timeout = "6m"
        user_dependencies = ["pytorch"]
        chunk_size = "123"

        expected_dependencies = user_dependencies.copy()
        expected_dependencies.append("cloudpickle==2.2.0")
        expected_dep_str = base64.b64encode(
            "\n".join(expected_dependencies).encode("utf-8")
        ).decode("utf-8")

        expected_action = {
            "runtime": runtime,
            "communication": f"{communication_type}://",
            "timeout": timeout,
            "funcs": {"transform": "transform"},
            "code": self.encode_fn(self.transform_fn, communication_type),
            "dependencies": expected_dep_str,
            "chunk_size": chunk_size,
        }
        self.init_code_exec_assert(
            expected_action,
            dependencies=user_dependencies,
            runtime=runtime,
            communication_type=communication_type,
            timeout=timeout,
            chunk_size=chunk_size,
        )

    @staticmethod
    def transform_fn():
        print("example action")

    @staticmethod
    def encode_fn(fn, comm_type):
        transform = base64.b64encode(cloudpickle.dumps(fn)).decode("utf-8")
        io_comm_context = "transform()" if comm_type == "io" else ""
        template = CODE_TEMPLATE.format(transform, io_comm_context).encode("utf-8")
        return base64.b64encode(template).decode("utf-8")

    def init_code_exec_assert(self, expected_action, **kwargs):
        expected_action["id"] = self.etl_name

        expected_response_text = "response text"
        mock_response = Mock()
        mock_response.text = expected_response_text
        self.mock_client.request.return_value = mock_response

        response = self.etl.init_code(self.transform_fn, self.etl_name, **kwargs)

        self.assertEqual(expected_response_text, response)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_PUT, path="etl", json=expected_action
        )

    def test_list(self):
        mock_response = Mock()
        self.mock_client.request_deserialize.return_value = mock_response
        response = self.etl.list()
        self.assertEqual(mock_response, response)
        self.mock_client.request_deserialize.assert_called_with(
            HTTP_METHOD_GET, path="etl", res_model=List[ETL]
        )

    def test_view(self):
        mock_response = Mock()
        self.mock_client.request_deserialize.return_value = mock_response
        response = self.etl.view(self.etl_name)
        self.assertEqual(mock_response, response)
        self.mock_client.request_deserialize.assert_called_with(
            HTTP_METHOD_GET, path=f"etl/{ self.etl_name }", res_model=ETLDetails
        )

    def test_start(self):
        self.etl.start(self.etl_name)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST, path=f"etl/{ self.etl_name }/start"
        )

    def test_stop(self):
        self.etl.stop(self.etl_name)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST, path=f"etl/{ self.etl_name }/stop"
        )

    def test_delete(self):
        self.etl.delete(self.etl_name)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_DELETE, path=f"etl/{ self.etl_name }"
        )
