#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#
import sys

import base64
from typing import Callable, List
import cloudpickle
from aistore.sdk.const import (
    CODE_TEMPLATE,
    HTTP_METHOD_DELETE,
    HTTP_METHOD_GET,
    HTTP_METHOD_POST,
    HTTP_METHOD_PUT,
)
from aistore.sdk.types import ETL, ETLDetails


def get_default_runtime():
    """
    Determines etl runtime to use if not specified
    Returns:
        String of runtime
    """
    version = f"{sys.version_info.major}.{sys.version_info.minor}"
    if version in ["3.10", "3.11"]:
        return f"python{version}v2"
    return "python3.8v2"


# pylint: disable=unused-variable
class Etl:
    """
    A class containing ETL-related functions.

    Args:
        None
    """

    def __init__(self, client):
        self._client = client

    @property
    def client(self):
        """The client bound to this ETL object."""
        return self._client

    def init_spec(
        self,
        template: str,
        etl_name: str,
        communication_type: str = "hpush",
        timeout: str = "5m",
    ):
        """
        Initializes ETL based on POD spec template. Returns etl_name.
        Existing templates can be found at `sdk.etl_templates`
        For more information visit: https://github.com/NVIDIA/ais-etl/tree/master/transformers

        Args:
            docker_image (str): docker image name looks like: <hub-user>/<repo-name>:<tag>
            etl_name (str): name of new ETL
            communication_type (str): Communication type of the ETL (options: hpull, hrev, hpush)
            timeout (str): timeout of the ETL (eg. 5m for 5 minutes)
        Returns:
            etl_name (str): ETL name
        """

        # spec
        spec_encoded = base64.b64encode(template.encode("utf-8")).decode("utf-8")

        action = {
            "spec": spec_encoded,
            "id": etl_name,
            "communication": f"{communication_type}://",
            "timeout": timeout,
        }

        resp = self.client.request(HTTP_METHOD_PUT, path="etl", json=action)
        return resp.text

    def init_code(
        self,
        transform: Callable,
        etl_name: str,
        dependencies: List[str] = None,
        runtime: str = get_default_runtime(),
        communication_type: str = "hpush",
        timeout: str = "5m",
        chunk_size: int = None,
    ):
        """
        Initializes ETL based on the provided source code. Returns etl_name.

        Args:
            transform (Callable): Transform function of the ETL
            etl_name (str): Name of new ETL
            dependencies (list[str]): Python dependencies to install
            runtime (str): [optional, default= V2 implementation of the current python version if supported, else
                python3.8v2] Runtime environment of the ETL [choose from: python3.8v2, python3.10v2, python3.11v2]
                (see ext/etl/runtime/all.go)
            communication_type (str): [optional, default="hpush"] Communication type of the ETL (options: hpull, hrev, hpush, io)
            timeout (str): [optional, default="5m"] Timeout of the ETL (e.g. 5m for 5 minutes)
            chunk_size (int): Chunk size in bytes if transform function in streaming data. (whole object is read by default)
        Returns:
            etl_name (str): ETL name
        """
        if communication_type not in ["io", "hpush", "hrev", "hpull"]:
            raise ValueError("communication_type should be in: hpull, hrev, hpush, io")

        functions = {
            "transform": "transform",
        }

        action = {
            "id": etl_name,
            "runtime": runtime,
            "communication": f"{communication_type}://",
            "timeout": timeout,
            "funcs": functions,
        }

        if chunk_size:
            action["chunk_size"] = chunk_size

        # code
        transform = base64.b64encode(cloudpickle.dumps(transform)).decode("utf-8")

        io_comm_context = "transform()" if communication_type == "io" else ""
        template = CODE_TEMPLATE.format(transform, io_comm_context).encode("utf-8")
        action["code"] = base64.b64encode(template).decode("utf-8")

        # dependencies
        if dependencies is None:
            dependencies = []
        dependencies.append("cloudpickle==2.2.0")
        deps = "\n".join(dependencies).encode("utf-8")
        action["dependencies"] = base64.b64encode(deps).decode("utf-8")

        resp = self.client.request(
            HTTP_METHOD_PUT,
            path="etl",
            json=action,
        )
        return resp.text

    def list(self) -> List[ETLDetails]:
        """
        Lists all running ETLs.

        Note: Does not list ETLs that have been stopped or deleted.

        Args:
            Nothing
        Returns:
            List[ETL]: A list of running ETLs
        """
        resp = self.client.request_deserialize(
            HTTP_METHOD_GET, path="etl", res_model=List[ETL]
        )
        return resp

    def view(self, etl_name: str) -> ETLDetails:
        """
        View ETLs Init spec/code

        Args:
            etl_name (str): name of ETL
        Returns:
            ETLDetails: details of the ETL
        """
        resp = self.client.request_deserialize(
            HTTP_METHOD_GET, path=f"etl/{ etl_name }", res_model=ETLDetails
        )
        return resp

    def start(self, etl_name: str):
        """
        Resumes a stopped ETL with given ETL name.

        Note: Deleted ETLs cannot be started.

        Args:
            etl_name (str): name of ETL
        Returns:
            Nothing
        """
        self.client.request(HTTP_METHOD_POST, path=f"etl/{ etl_name }/start")

    def stop(self, etl_name: str):
        """
        Stops ETL with given ETL name. Stops (but does not delete) all the pods created by Kubernetes for this ETL and terminates any transforms.

        Args:
            etl_name (str): name of ETL
        Returns:
            Nothing
        """
        self.client.request(HTTP_METHOD_POST, path=f"etl/{ etl_name }/stop")

    def delete(self, etl_name: str):
        """
        Delete ETL with given ETL name. Deletes pods created by Kubernetes for this ETL and specifications for this ETL in Kubernetes.

        Note: Running ETLs cannot be deleted.

        Args:
            etl_name (str): name of ETL
        Returns:
            Nothing
        """
        self.client.request(HTTP_METHOD_DELETE, path=f"etl/{ etl_name }")
