#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#
import sys
import re

import base64
from typing import List, Union, Type

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
    DEFAULT_ETL_COMM,
    DEFAULT_ETL_TIMEOUT,
    DEFAULT_ETL_OBJ_TIMEOUT,
    ETL_COMM_OPTIONS,
    PYTHON_RUNTIME_CMD,
)

from aistore.sdk.types import (
    ETLDetails,
    InitSpecETLArgs,
    ETLSpecMsg,
    EnvVar,
    ETLRuntimeSpec,
)
from aistore.sdk.utils import convert_to_seconds
from aistore.sdk.etl.webserver.base_etl_server import ETLServer
from aistore.sdk.etl.webserver import serialize_class


def _get_runtime() -> str:
    """
    Determine the AIStore ETL runtime identifier for the current Python interpreter.

    Returns:
        A string like "3.10" when running under Python 3.10.

    Raises:
        ValueError: If the current Python version isn't in ETL_SUPPORTED_PYTHON_VERSIONS.
    """
    ver = f"{sys.version_info.major}.{sys.version_info.minor}"
    if ver not in ETL_SUPPORTED_PYTHON_VERSIONS:
        supported = ", ".join(ETL_SUPPORTED_PYTHON_VERSIONS)
        raise ValueError(f"Unsupported Python version {ver}; supported: {supported}")
    return ver


# pylint: disable=unused-variable
def _validate_comm_type(given: str, valid: List[str]):
    if given not in valid:
        valid_str = ", ".join(valid)
        raise ValueError(f"communication_type should be one of: {valid_str}")


class Etl:
    """
    A class containing ETL-related functions.

    **Disclaimer:** The `init_code` method has been removed as of version v1.14.0+.
        Please use `init_class` instead when registering ETL server classes.
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
        _validate_comm_type(communication_type, ETL_COMM_OPTIONS)

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
            image (str): Docker image to use for the ETL.
            command (Union[List[str], str]): Command to run in the container.
            comm_type (str): Communication type of the ETL (options: hpull, hpush, ws).
            init_timeout (str): [optional, default="5m"] Timeout of the ETL job (e.g. 5m for 5 minutes).
            obj_timeout (str): [optional, default="45s"] Timeout of transforming a single object.
            arg_type (str): The type of argument the runtime will provide the transform function.
                The default value of "" will provide the raw bytes read from the object.
            direct_put (bool): Whether to support direct put optimization in bck-to-bck operations.
            kwargs (dict): Additional keyword arguments to pass to the ETL, will be passed as
                environment variables to the container.
        Returns:
            Job ID string associated with this ETL
        """

        # Validate communication type
        _validate_comm_type(comm_type, ETL_COMM_OPTIONS)

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

    def init_class(
        self,
        *,
        dependencies: List[str] = None,
        comm_type: str = DEFAULT_ETL_COMM,
        init_timeout: str = DEFAULT_ETL_TIMEOUT,
        obj_timeout: str = DEFAULT_ETL_OBJ_TIMEOUT,
        arg_type: str = "",
        direct_put: bool = True,
        **kwargs,
    ):
        """
        Initialize an ETLServer subclass in AIS.

        `init_class` realizes a special case of ETL initialization that allows to
        register custom Python class on the server side. This class must extend `ETLServer`
        and implement the `transform` method. The class will be serialized and
        passed to the ETL runtime as an environment variable. The runtime will
        deserialize the class and use it to handle incoming requests.

        This method is a decorator that can be used to register an ETL server class.

        Args:
            dependencies (List[str], optional):
                A list of extra PyPI package names to install inside the ETL pod
                before running your server. Defaults to no extra packages.
            comm_type (str, optional):
                How AIS should talk to your ETL pod. Set to `"hpush://"` or `"hpull://"`
                (and is forwarded into the `init(...)` call). Defaults to `"hpush://"`.
            init_timeout (str, optional):
                How long AIS waits for all ETL pods to become ready (e.g. `"5m"` for five minutes).
                Defaults to `"5m"`.
            obj_timeout (str, optional):
                How long each individual object-transform call can run (e.g. `"45s"` for 45 seconds).
                Defaults to `"45s"`.
            arg_type (str, optional):
                What form AIS should pass the object to your `transform` method. If `""`,
                you get the raw bytes. If `"fqn"`, you get a fully-qualified local path.
                Defaults to `""`.
            direct_put (bool, optional):
                When doing a bucket-to-bucket transform, set to `True` to enable “direct put”
                optimization. Defaults to `True`.
            **kwargs:
                Any other keyword arguments become environment-variables inside the ETL pod.
                To configure concurrency, set env-var `NUM_WORKERS` to specify the number of worker
                processes (default: 4).
        """
        dependencies = dependencies or []

        def decorator(cls: Type[ETLServer]) -> Type[ETLServer]:
            # Check if the class is a subclass of ETLServer
            if not isinstance(cls, type) or not issubclass(cls, ETLServer):
                raise TypeError(f"{cls!r} must extend ETLServer")

            # Serialize the class to pass it as an environment variable
            class_payload = serialize_class(cls)

            # Call init(), passing our special env-vars
            self.init(
                image=f"aistorage/runtime_python:{_get_runtime()}",
                command=PYTHON_RUNTIME_CMD,
                comm_type=comm_type,
                init_timeout=init_timeout,
                obj_timeout=obj_timeout,
                arg_type=arg_type,
                direct_put=direct_put,
                # these become env vars inside the ETL pod:
                ETL_CLASS_PAYLOAD=class_payload,
                PACKAGES=",".join(dependencies),
                **kwargs,
            )
            return cls

        return decorator

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
