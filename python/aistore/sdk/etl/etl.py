#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#
import sys
import re

import base64
from typing import Callable, List, Union

import cloudpickle

from aistore.sdk.const import (
    HTTP_METHOD_DELETE,
    HTTP_METHOD_GET,
    HTTP_METHOD_POST,
    HTTP_METHOD_PUT,
    URL_PATH_ETL,
    UTF_ENCODING,
)
from aistore.sdk.etl.etl_const import (
    ETL_SUPPORTED_PYTHON_VERSIONS,
    DEFAULT_ETL_RUNTIME,
    DEFAULT_ETL_COMM,
    DEFAULT_ETL_TIMEOUT,
    DEFAULT_ETL_OBJ_TIMEOUT,
    ETL_COMM_SPEC,
    ETL_COMM_CODE,
    CODE_TEMPLATE,
)

from aistore.sdk.types import (
    ETLDetails,
    InitCodeETLArgs,
    InitSpecETLArgs,
    ETLSpecMsg,
    EnvVar,
    ETLRuntimeSpec,
)
from aistore.sdk.utils import convert_to_seconds


def _get_default_runtime():
    """
    Determines etl runtime to use if not specified
    Returns:
        String of runtime
    """
    version = f"{sys.version_info.major}.{sys.version_info.minor}"
    if version in ETL_SUPPORTED_PYTHON_VERSIONS:
        return f"python{version}v2"
    return DEFAULT_ETL_RUNTIME


# pylint: disable=unused-variable
def _validate_comm_type(given: str, valid: List[str]):
    if given not in valid:
        valid_str = ", ".join(valid)
        raise ValueError(f"communication_type should be one of: {valid_str}")


class Etl:
    """
    A class containing ETL-related functions.
    """

    def __init__(self, client: "Client", name: str):
        self._client = client
        self._name = name
        self.validate_etl_name(name)

    @property
    def name(self) -> str:
        """Name of the ETL"""
        return self._name

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def init_spec(
        self,
        template: str,
        communication_type: str = DEFAULT_ETL_COMM,
        init_timeout: str = DEFAULT_ETL_TIMEOUT,
        obj_timeout: str = DEFAULT_ETL_OBJ_TIMEOUT,
        arg_type: str = "",
    ) -> str:
        """
        Initializes ETL based on Kubernetes pod spec template.

        Args:
            template (str): Kubernetes pod spec template
                Existing templates can be found at `sdk.etl_templates`
                For more information visit: https://github.com/NVIDIA/ais-etl/tree/main/transformers
            communication_type (str): Communication type of the ETL (options: hpull, hpush)
            init_timeout (str): [optional, default="5m"] Timeout of the ETL job (e.g. 5m for 5 minutes)
            obj_timeout (str): [optional, default="45s"] Timeout of transforming a single object
        Returns:
            Job ID string associated with this ETL
        """
        _validate_comm_type(communication_type, ETL_COMM_SPEC)

        # spec
        spec_encoded = base64.b64encode(template.encode(UTF_ENCODING)).decode(
            UTF_ENCODING
        )

        value = InitSpecETLArgs(
            spec=spec_encoded,
            name=self._name,
            comm_type=communication_type,
            init_timeout=init_timeout,
            obj_timeout=obj_timeout,
            arg_type=arg_type,
        ).as_dict()

        return self._client.request(
            HTTP_METHOD_PUT,
            path=URL_PATH_ETL,
            timeout=convert_to_seconds(init_timeout),
            json=value,
        ).text

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def init_code(
        self,
        transform: Callable,
        dependencies: List[str] = None,
        preimported_modules: List[str] = None,
        runtime: str = _get_default_runtime(),
        communication_type: str = DEFAULT_ETL_COMM,
        init_timeout: str = DEFAULT_ETL_TIMEOUT,
        obj_timeout: str = DEFAULT_ETL_OBJ_TIMEOUT,
        chunk_size: int = None,
        arg_type: str = "",
    ) -> str:
        """
        Initializes ETL based on the provided source code.

        Args:
            transform (Callable): Transform function of the ETL
            dependencies (list[str]): Python dependencies to install
            preimported_modules (list[str]): Modules to import before running the transform function. This can
             be necessary in cases where the modules used both attempt to import each other circularly
            runtime (str): [optional, default= V2 implementation of the current python version if supported, else
                python3.13v2] Runtime environment of the ETL [choose from: python3.9v2, python3.10v2, python3.11v2,
                python3.12v2, python3.13v2] (see ext/etl/runtime/all.go)
            communication_type (str): [optional, default="hpush"] Communication type of the ETL (options: hpull,
                hpush, io)
            init_timeout (str): [optional, default="5m"] Timeout of the ETL job (e.g. 5m for 5 minutes)
            obj_timeout (str): [optional, default="45s"] Timeout of transforming a single object
            chunk_size (int): Chunk size in bytes if transform function in streaming data.
                (whole object is read by default)
            arg_type (optional, str): The type of argument the runtime will provide the transform function.
                The default value of "" will provide the raw bytes read from the object.
                When used with hpull communication_type, setting this to "url" will provide the URL of the object.
        Returns:
            Job ID string associated with this ETL
        """
        _validate_comm_type(communication_type, ETL_COMM_CODE)

        # code functions to call
        functions = {
            "transform": "transform",
        }

        value = InitCodeETLArgs(
            name=self._name,
            runtime=runtime,
            comm_type=communication_type,
            init_timeout=init_timeout,
            obj_timeout=obj_timeout,
            dependencies=self._encode_dependencies(dependencies),
            functions=functions,
            code=self._encode_transform(
                transform, preimported_modules, communication_type
            ),
            chunk_size=chunk_size,
            arg_type=arg_type,
        ).as_dict()

        return self._client.request(
            HTTP_METHOD_PUT,
            path=URL_PATH_ETL,
            timeout=convert_to_seconds(init_timeout),
            json=value,
        ).text

    def init(
        self,
        image: str,
        command: Union[List[str], str],
        comm_type: str = DEFAULT_ETL_COMM,
        init_timeout: str = DEFAULT_ETL_TIMEOUT,
        obj_timeout: str = DEFAULT_ETL_OBJ_TIMEOUT,
        arg_type: str = "",
        direct_put: bool = False,
        **kwargs,
    ):
        """
        Initializes ETL based on the provided image and command.
        Args:
            name (str): Name of the ETL
            image (str): Docker image to use for the ETL
            command (Union[List[str], str]): Command to run in the container
            comm_type (str): Communication type of the ETL (options: hpull, hpush, ws)
            init_timeout (str): [optional, default="5m"] Timeout of the ETL job (e.g. 5m for 5 minutes)
            obj_timeout (str): [optional, default="45s"] Timeout of transforming a single object
            arg_type (str): The type of argument the runtime will provide the transform function.
                The default value of "" will provide the raw bytes read from the object.
            direct_put (bool): Whether to support direct put optimization in bck-to-bck operations.
            kwargs (dict): Additional keyword arguments to pass to the ETL, will be passed as
                environment variables to the container
        Returns:
            Job ID string associated with this ETL
        """

        # Validate communication type
        _validate_comm_type(comm_type, ETL_COMM_SPEC)

        # normalize command
        if isinstance(command, str):
            command = command.split()

        # build EnvVar list
        env_vars = [EnvVar(name=k, value=v) for k, v in kwargs.items()]
        # assemble spec
        spec_msg = ETLSpecMsg(
            name=self._name,
            comm_type=comm_type,
            init_timeout=init_timeout,
            obj_timeout=obj_timeout,
            arg_type=arg_type,
            direct_put=direct_put,
            runtime=ETLRuntimeSpec(
                image=image,
                command=command,
                env=env_vars,
            ),
        )

        resp = self._client.request(
            HTTP_METHOD_PUT,
            path=URL_PATH_ETL,
            timeout=convert_to_seconds(init_timeout),
            json=spec_msg.as_dict(),
        )
        return resp.text

    def view(self) -> ETLDetails:
        """
        View ETL details

        Returns:
            ETLDetails: details of the ETL
        """
        resp = self._client.request_deserialize(
            HTTP_METHOD_GET, path=f"{URL_PATH_ETL}/{self._name}", res_model=ETLDetails
        )
        return resp

    def start(self):
        """
        Resumes a stopped ETL with given ETL name.

        Note: Deleted ETLs cannot be started.
        """
        self._client.request(
            HTTP_METHOD_POST,
            path=f"{URL_PATH_ETL}/{self._name}/start",
            timeout=convert_to_seconds(DEFAULT_ETL_TIMEOUT),
        )

    def stop(self):
        """
        Stops ETL. Stops (but does not delete) all the pods created by Kubernetes for this ETL and
        terminates any transforms.
        """
        self._client.request(
            HTTP_METHOD_POST,
            path=f"{URL_PATH_ETL}/{self._name}/stop",
            timeout=convert_to_seconds(DEFAULT_ETL_TIMEOUT),
        )

    def delete(self):
        """
        Delete ETL. Deletes pods created by Kubernetes for this ETL and specifications for this ETL
        in Kubernetes.

        Note: Running ETLs cannot be deleted.
        """
        self._client.request(
            HTTP_METHOD_DELETE,
            path=f"{URL_PATH_ETL}/{self._name}",
            timeout=convert_to_seconds(DEFAULT_ETL_TIMEOUT),
        )

    @staticmethod
    def _encode_transform(
        transform: Callable,
        preimported_modules: List[str] = None,
        comm_type: str = None,
    ):
        transform = base64.b64encode(cloudpickle.dumps(transform)).decode(UTF_ENCODING)

        io_comm_context = "transform()" if comm_type == "io" else ""
        modules = preimported_modules if preimported_modules else []
        template = CODE_TEMPLATE.format(modules, transform, io_comm_context).encode(
            UTF_ENCODING
        )
        return base64.b64encode(template).decode(UTF_ENCODING)

    @staticmethod
    def _encode_dependencies(dependencies: List[str]):
        if dependencies is None:
            dependencies = []
        dependencies.append("cloudpickle>=3.0.0")
        deps = "\n".join(dependencies).encode(UTF_ENCODING)
        return base64.b64encode(deps).decode(UTF_ENCODING)

    @staticmethod
    def validate_etl_name(name: str):
        """
        Validate the ETL name based on specific criteria.

        Args:
            name (str): The name of the ETL to validate.

        Raises:
            ValueError: If the name is too short (less than 6 characters),
                        too long (more than 32 characters),
                        or contains invalid characters (anything other than lowercase letters, digits, or hyphens).
        """
        prefix = f"ETL name '{name}' "
        short_name_etl = 6
        long_name_etl = 32

        length = len(name)
        if length < short_name_etl:
            raise ValueError(f"{prefix}is too short")
        if length > long_name_etl:
            raise ValueError(f"{prefix}is too long")

        if not re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])", name):
            raise ValueError(
                f"{prefix}is invalid: must start/end with a lowercase letter/number, and can only contain [a-z0-9-]"
            )
