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
    QPARAM_UUID,
)
from aistore.sdk.etl.etl_const import (
    ETL_SUPPORTED_PYTHON_VERSIONS,
    DEFAULT_ETL_COMM,
    DEFAULT_ETL_TIMEOUT,
    DEFAULT_ETL_OBJ_TIMEOUT,
    ETL_COMM_OPTIONS,
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
from aistore.sdk.errors import AISError


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
        self._pipeline: List[str] = []  # pipeline never includes the current ETL
        self.validate_etl_name(name)

    def __rshift__(self, other: "Etl") -> "Etl":
        """
        Combine multiple ETLs into a pipeline.
        The combined ETL will have the same name as the first ETL,
        with the rest of the ETL stages added to the pipeline property.

        Args:
            other (Etl): The ETL to combine with.

        Returns:
            Etl: The combined ETL.
        """
        combined_etl = Etl(self._client, self._name)
        combined_etl._pipeline = self.pipeline + [other._name] + other.pipeline
        return combined_etl

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        try:
            self.stop()
            self.delete()
        except AISError as e:
            pass

    @property
    def pipeline(self) -> List[str]:
        """List of ETL names in the pipeline"""
        return self._pipeline

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
    ) -> str:
        """
        Initializes ETL based on Kubernetes pod spec template.

        Args:
            template (str): Kubernetes pod spec template
                Existing templates can be found at `sdk.etl_templates`
                For more information visit: https://github.com/NVIDIA/ais-etl/tree/main/transformers
            communication_type (str): Communication type of the ETL (options: hpull, hpush)
            init_timeout (str, optional): Timeout of the ETL job (e.g., "5m" for 5 minutes). Default is "5m".
            obj_timeout (str, optional): Timeout for transforming a single object (e.g., "45s"). Default is "45s".

        Returns:
            str: Job ID string associated with this ETL
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
        command: Union[List[str], str] = None,
        comm_type: str = DEFAULT_ETL_COMM,
        init_timeout: str = DEFAULT_ETL_TIMEOUT,
        obj_timeout: str = DEFAULT_ETL_OBJ_TIMEOUT,
        direct_put: bool = False,
        **kwargs,
    ) -> str:
        """
        Initializes an ETL based on a container image and optional command/env vars.

        Args:
            image (str): Docker image for the ETL.
            command (Union[List[str], str], optional): Command to run in the container.
            comm_type (str, optional): Communication type (hpull, hpush, ws).
            init_timeout (str, optional): ETL job timeout (e.g., "5m").
            obj_timeout (str, optional): Per-object transform timeout (e.g., "45s").
            direct_put (bool, optional): Enable direct-put optimization.
            **kwargs: Additional key-value pairs → env vars in the ETL container.

        Returns:
            str: Job ID for this ETL.
        """
        # 1. Validate communication type
        _validate_comm_type(comm_type, ETL_COMM_OPTIONS)

        # 2. Normalize command
        if isinstance(command, str):
            command = command.split()

        # 3. Build env var list (None if no extras)
        env_vars = [EnvVar(name=k, value=v) for k, v in kwargs.items()] or None

        # 4. Create the runtime spec (command/env omitted if None)
        runtime = ETLRuntimeSpec(
            image=image,
            command=command,
            env=env_vars,
        )

        # 5. Assemble and serialize the init-spec message
        spec_msg = ETLSpecMsg(
            name=self._name,
            comm_type=comm_type,
            init_timeout=init_timeout,
            obj_timeout=obj_timeout,
            direct_put=direct_put,
            runtime=runtime,
        )
        payload = spec_msg.as_dict()

        # 6. Send the request
        resp = self._client.request(
            HTTP_METHOD_PUT,
            path=URL_PATH_ETL,
            timeout=convert_to_seconds(init_timeout),
            json=payload,
        )
        return resp.text

    def init_class(
        self,
        *,
        dependencies: List[str] = None,
        os_packages: List[str] = None,
        comm_type: str = DEFAULT_ETL_COMM,
        init_timeout: str = DEFAULT_ETL_TIMEOUT,
        obj_timeout: str = DEFAULT_ETL_OBJ_TIMEOUT,
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
            os_packages (List[str], optional):
                Names of Linux packages to install inside the ETL container before the server starts.
                These must be available as Debian-based system packages installable via the `apt` package manager.
                (e.g. `libsndfile-dev`, `ffmpeg`). Defaults to no extra system packages.
            comm_type (str, optional):
                How AIS should talk to your ETL pod. Set to `"hpush://"` or `"hpull://"`
                (and is forwarded into the `init(...)` call). Defaults to `"hpush://"`.
            init_timeout (str, optional):
                How long AIS waits for all ETL pods to become ready (e.g. `"5m"` for five minutes).
                Defaults to `"5m"`.
            obj_timeout (str, optional):
                How long each individual object-transform call can run (e.g. `"45s"` for 45 seconds).
                Defaults to `"45s"`.
            direct_put (bool, optional):
                When doing a bucket-to-bucket transform, set to `True` to enable “direct put”
                optimization. Defaults to `True`.
            **kwargs:
                Any other keyword arguments become environment-variables inside the ETL pod.
                To configure concurrency, set env-var `NUM_WORKERS` to specify the number of worker
                processes (default: 4).
        """
        dependencies = dependencies or []
        os_packages = os_packages or []

        def decorator(cls: Type[ETLServer]) -> Type[ETLServer]:
            # Check if the class is a subclass of ETLServer
            if not isinstance(cls, type) or not issubclass(cls, ETLServer):
                raise TypeError(f"{cls!r} must extend ETLServer")

            # Serialize the class to pass it as an environment variable
            class_payload = serialize_class(cls)
            env_kwargs = {
                "ETL_CLASS_PAYLOAD": class_payload,
            }

            # include PACKAGES if any
            if dependencies:
                env_kwargs["PACKAGES"] = ",".join(dependencies)

            # include OS_PACKAGES if any
            if os_packages:
                env_kwargs["OS_PACKAGES"] = ",".join(os_packages)

            env_kwargs.update(kwargs)

            # Call init(), passing our special env-vars
            self.init(
                image=f"aistorage/runtime_python:{_get_runtime()}",
                comm_type=comm_type,
                init_timeout=init_timeout,
                obj_timeout=obj_timeout,
                direct_put=direct_put,
                **env_kwargs,
            )
            return cls

        return decorator

    def view(self, job_id: str = "") -> ETLDetails:
        """
        View ETL details

        Args:
            job_id (str):
                Offline Transform job ID of the ETL to view details for. Default to view inline transform details.

        Returns:
            ETLDetails: details of the ETL
        """
        resp = self._client.request_deserialize(
            HTTP_METHOD_GET,
            path=f"{URL_PATH_ETL}/{self._name}",
            params={QPARAM_UUID: job_id},
            res_model=ETLDetails,
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
