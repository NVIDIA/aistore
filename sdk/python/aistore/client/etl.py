#
# Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
#

import base64
from typing import List
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
        code: object,
        etl_id: str,
        dependencies: List[str] = None,
        runtime: str = "python3",
        communication_type: str = "hpush",
        timeout: str = "5m",
    ):
        """
        Initializes ETL based on the provided source code. Returns ETL_ID.

        Args:
            code (object): code function of the new ETL
            etl_id (str): id of new ETL
            dependencies (List[str]): list of the necessary dependencies with version (eg. aistore>1.0.0)
            runtime (str): Runtime environment of the ETL [choose from: python2, python3, python3.6, python3.8, python3.10]
            communication_type (str): Communication type of the ETL (options: hpull, hrev, hpush)
            timeout (str): timeout of the ETL (eg. 5m for 5 minutes)
        Returns:
            etl_id (str): ETL ID
        """
        # code
        func = base64.b64encode(cloudpickle.dumps(code)).decode("utf-8")
        template = CODE_TEMPLATE.format(func).encode("utf-8")
        code = base64.b64encode(template).decode("utf-8")

        # dependencies
        if dependencies is None:
            dependencies = []
        dependencies.append("cloudpickle==2.0.0")
        deps = "\n".join(dependencies).encode("utf-8")
        deps_encoded = base64.b64encode(deps).decode("utf-8")

        action = {
            "code": code,
            "dependencies": deps_encoded,
            "id": etl_id,
            "runtime": runtime,
            "communication": f"{communication_type}://",
            "timeout": timeout,
        }

        resp = self.client.request(
            HTTP_METHOD_PUT,
            path="etl",
            json=action,
        )
        return resp.text

    def list(self) -> List[ETLDetails]:
        """
        Lists all running ETLs.

        Note: Does not list ETLs that have been stopped.

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
        Starts a stopped ETL with given ETL_ID.

        Args:
            etl_id (str): id of ETL
        Returns:
            Nothing
        """
        self.client.request(HTTP_METHOD_POST, path=f"etl/{ etl_id }/start")

    def stop(self, etl_id: str):
        """
        Stops ETL with given ETL_ID. Stops all the pods created by kubernetes for this ETL.

        Args:
            etl_id (str): id of ETL
        Returns:
            Nothing
        """
        self.client.request(HTTP_METHOD_POST, path=f"etl/{ etl_id }/stop")

    def delete(self, etl_id: str):
        """
        Delete ETL with given ETL_ID. Deletes all pods created by kubernetes for this ETL. Can only a delete a stopped ETL.

        Args:
            etl_id (str): id of ETL
        Returns:
            Nothing
        """
        self.client.request(HTTP_METHOD_DELETE, path=f"etl/{ etl_id }")
