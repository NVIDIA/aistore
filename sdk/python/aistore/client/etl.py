#
# Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
#

import base64
from typing import Callable, List
import cloudpickle
from aistore.client.const import (
    CODE_TEMPLATE,
    HTTP_METHOD_DELETE,
    HTTP_METHOD_GET,
    HTTP_METHOD_POST,
    HTTP_METHOD_PUT,
)
from aistore.client.types import ETL, ETLDetails

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
        etl_id: str,
        communication_type: str = "hpush",
        timeout: str = "5m",
    ):
        """
        Initializes ETL based on POD spec template. Returns ETL_ID.
        Existing templates can be found at `aistore.client.etl_templates`
        For more information visit: https://github.com/NVIDIA/ais-etl/tree/master/transformers

        Args:
            docker_image (str): docker image name looks like: <hub-user>/<repo-name>:<tag>
            etl_id (str): id of new ETL
            communication_type (str): Communication type of the ETL (options: hpull, hrev, hpush)
            timeout (str): timeout of the ETL (eg. 5m for 5 minutes)
        Returns:
            etl_id (str): ETL ID
        """

        # spec
        spec_encoded = base64.b64encode(template.encode("utf-8")).decode("utf-8")

        action = {
            "spec": spec_encoded,
            "id": etl_id,
            "communication": f"{communication_type}://",
            "timeout": timeout,
        }

        resp = self.client.request(HTTP_METHOD_PUT, path="etl", json=action)
        return resp.text

    def init_code(
        self,
        transform: Callable,
        etl_id: str,
        dependencies: List[str] = None,
        runtime: str = "python3.8v2",
        communication_type: str = "hpush",
        timeout: str = "5m",
        chunk_size: int = None,
    ):
        """
        Initializes ETL based on the provided source code. Returns ETL_ID.

        Args:
            transform (Callable): Transform function of the ETL
            etl_id (str): Id of new ETL
            runtime (str): [optional, default="python3.8v2"] Runtime environment of the ETL [choose from: python3.8v2, python3.10v2] (see etl/runtime/all.go)
            communication_type (str): [optional, default="hpush"] Communication type of the ETL (options: hpull, hrev, hpush, io)
            timeout (str): [optional, default="5m"] Timeout of the ETL (eg. 5m for 5 minutes)
            chunk_size (int): Chunk size in bytes if transform function in streaming data. (whole object is read by default)
        Returns:
            etl_id (str): ETL ID
        """
        if communication_type not in ["io", "hpush", "hrev", "hpull"]:
            raise ValueError("communication_type should be in: hpull, hrev, hpush, io")

        functions = {
            "transform": "transform",
        }

        action = {
            "id": etl_id,
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
        dependencies.append("cloudpickle==2.0.0")
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

    def view(self, etl_id: str) -> ETLDetails:
        """
        View ETLs Init spec/code

        Args:
            etl_id (str): id of ETL
        Returns:
            ETLDetails: details of the ETL
        """
        resp = self.client.request_deserialize(
            HTTP_METHOD_GET, path=f"etl/{ etl_id }", res_model=ETLDetails
        )
        return resp

    def start(self, etl_id: str):
        """
        Resumes a stopped ETL with given ETL_ID.

        Note: Deleted ETLs cannot be started.

        Args:
            etl_id (str): id of ETL
        Returns:
            Nothing
        """
        self.client.request(HTTP_METHOD_POST, path=f"etl/{ etl_id }/start")

    def stop(self, etl_id: str):
        """
        Stops ETL with given ETL_ID. Stops (but does not delete) all the pods created by Kubernetes for this ETL and terminates any transforms.

        Args:
            etl_id (str): id of ETL
        Returns:
            Nothing
        """
        self.client.request(HTTP_METHOD_POST, path=f"etl/{ etl_id }/stop")

    def delete(self, etl_id: str):
        """
        Delete ETL with given ETL_ID. Deletes pods created by Kubernetes for this ETL and specifications for this ETL in Kubernetes.

        Note: Running ETLs cannot be deleted.

        Args:
            etl_id (str): id of ETL
        Returns:
            Nothing
        """
        self.client.request(HTTP_METHOD_DELETE, path=f"etl/{ etl_id }")
