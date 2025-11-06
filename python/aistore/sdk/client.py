#
# Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
#
from typing import List, Optional, Tuple, Union
import os
import warnings
from urllib3 import Retry

from aistore.sdk.bucket import Bucket
from aistore.sdk.provider import Provider
from aistore.sdk.const import (
    AIS_AUTHN_TOKEN,
    AIS_READ_TIMEOUT,
    AIS_CONNECT_TIMEOUT,
    AIS_MAX_CONN_POOL,
    EXT_TAR,
)
from aistore.sdk.cluster import Cluster
from aistore.sdk.dsort import Dsort
from aistore.sdk.request_client import RequestClient
from aistore.sdk.session_manager import SessionManager
from aistore.sdk.types import Namespace
from aistore.sdk.job import Job
from aistore.sdk.etl.etl import Etl
from aistore.sdk.utils import parse_url
from aistore.sdk.obj.object import Object
from aistore.sdk.errors import InvalidURLException
from aistore.sdk.retry_config import RetryConfig
from aistore.sdk.batch.batch import Batch


class Client:
    """
    AIStore client for managing buckets, objects, and ETL jobs.

    Args:
        endpoint (str): AIStore endpoint.
        skip_verify (bool, optional): If True, skip SSL certificate verification. Defaults to False.
        ca_cert (str, optional): Path to a CA certificate file for SSL verification. If not provided,
            the 'AIS_CLIENT_CA' environment variable will be used. Defaults to None.
        client_cert (Union[str, Tuple[str, str], None], optional): Path to a client certificate PEM file
            or a tuple (cert, key) for mTLS. If not provided, 'AIS_CRT' and 'AIS_CRT_KEY' environment
            variables will be used. Defaults to None.
        timeout (Union[float, Tuple[float, float], None], optional): Timeout for HTTP requests.
            - Single float (e.g., `5.0`): Applies to both connection and read timeouts.
            - Tuple (e.g., `(3.0, 20.0)`): First value is the connection timeout, second is the read timeout.
            - Tuple with 0 (e.g., `(0, 20.0)` or `(3.0, 0)`): Use `0` to disable specific timeout.
            - `0` or `0.0` or `(0, 0)`: Disables all timeouts.
            - `None` (default): Check environment variables 'AIS_CONNECT_TIMEOUT' and 'AIS_READ_TIMEOUT'.
              If env var is set to `0`, that specific timeout is disabled. Defaults to `(3, 20)` if not set.
        retry_config (RetryConfig, optional): Defines retry behavior for HTTP and network failures.
            If not provided, the default retry configuration (`RetryConfig.default()`) is used.
        retry (urllib3.Retry, optional): [Deprecated] Retry configuration from urllib3. Use `retry_config` instead.
        token (str, optional): Authorization token. If not provided, the 'AIS_AUTHN_TOKEN' environment variable
            will be used. Defaults to None.
        max_pool_size (int, optional): Maximum number of connections per host in the connection pool.
            If not provided, the 'AIS_MAX_CONN_POOL' environment variable will be used, or defaults to 10.
    """

    # Default timeout values (connect, read)
    _DEFAULT_CONNECT_TIMEOUT = 3
    _DEFAULT_READ_TIMEOUT = 20
    _DEFAULT_MAX_POOL_SIZE = 10

    @staticmethod
    def _parse_timeout_from_env(
        env_var_name: str, default_value: float
    ) -> Optional[float]:
        """
        Parse a timeout value from an environment variable.

        Args:
            env_var_name: Name of the environment variable to read
            default_value: Default value to use if not set or parsing fails

        Returns:
            Parsed timeout value (None if 0, float otherwise), or default if invalid
        """
        env_value = os.environ.get(env_var_name)

        if not env_value:
            return default_value

        try:
            parsed = float(env_value)
            # If env var is 0, return None (no timeout limit)
            if parsed == 0:
                return None
            return parsed
        except (ValueError, TypeError):
            warnings.warn(
                f"Invalid value for {env_var_name}='{env_value}'. "
                f"Using default timeout of {default_value} seconds."
            )
            return default_value

    @staticmethod
    def _resolve_timeout(
        timeout: Optional[Union[float, Tuple[float, float]]],
    ) -> Optional[Union[float, Tuple[float, float]]]:
        """
        Resolve timeout value from parameter or environment variables.

        Priority: explicit parameter > environment variables > defaults

        Special handling:
        - timeout=0 or timeout=(0, 0) -> None (disable all timeouts)
        - timeout=(0, 20) or timeout=(3, 0) -> convert 0 to None for that specific timeout
        - timeout=None (default) -> check env vars, fallback to (3, 20)
        - timeout=<value> -> use as-is (with 0 converted to None)
        - AIS_CONNECT_TIMEOUT=0 -> None for connect timeout (no limit)
        - AIS_READ_TIMEOUT=0 -> None for read timeout (no limit)
        - Both env vars=0 -> None (disable all timeouts)

        Args:
            timeout: Timeout parameter passed to __init__

        Returns:
            Resolved timeout value, tuple (connect, read), or None if disabled
        """
        # Convert 0 to None (disable timeout)
        if timeout in (0, 0.0, (0, 0)):
            return None

        # If timeout was explicitly provided (not None), use it
        if timeout is not None:
            # Handle tuple: convert 0 to None for individual timeouts
            if isinstance(timeout, tuple):
                connect, read = timeout
                connect = None if connect == 0 else connect
                read = None if read == 0 else read
                # If both are None, return None completely
                if connect is None and read is None:
                    return None
                return (connect, read)
            # Single float value - use for both
            return timeout

        # timeout is None - check environment variables or use defaults
        connect_timeout = Client._parse_timeout_from_env(
            AIS_CONNECT_TIMEOUT, Client._DEFAULT_CONNECT_TIMEOUT
        )
        read_timeout = Client._parse_timeout_from_env(
            AIS_READ_TIMEOUT, Client._DEFAULT_READ_TIMEOUT
        )

        # If both timeouts are None, return None (disable timeout completely)
        if connect_timeout is None and read_timeout is None:
            return None

        return (connect_timeout, read_timeout)

    @staticmethod
    def _resolve_max_pool_size(max_pool_size: Optional[int]) -> int:
        """
        Resolve max_pool_size value from parameter or environment variable.

        Priority: explicit parameter > environment variable > default

        Args:
            max_pool_size: max_pool_size parameter passed to __init__

        Returns:
            Resolved max_pool_size value
        """
        # If max_pool_size was explicitly provided, use it
        if max_pool_size is not None:
            return max_pool_size

        # Try to get value from environment variable
        env_max_pool = os.environ.get(AIS_MAX_CONN_POOL)

        if env_max_pool:
            try:
                return int(env_max_pool)
            except (ValueError, TypeError):
                warnings.warn(
                    f"Invalid value for {AIS_MAX_CONN_POOL}='{env_max_pool}'. "
                    f"Using default max_pool_size of {Client._DEFAULT_MAX_POOL_SIZE}."
                )

        return Client._DEFAULT_MAX_POOL_SIZE

    # pylint: disable=too-many-arguments, too-many-positional-arguments, too-many-locals
    def __init__(
        self,
        endpoint: str,
        skip_verify: bool = False,
        ca_cert: Optional[str] = None,
        client_cert: Optional[Union[str, Tuple[str, str]]] = None,
        timeout: Optional[Union[float, Tuple[float, float]]] = None,
        retry_config: Optional[RetryConfig] = None,
        retry: Optional[Retry] = None,  # deprecated
        token: Optional[str] = None,
        max_pool_size: Optional[int] = None,
    ):
        # Use `retry_config` (RetryConfig) if provided; otherwise, fall back to the deprecated `retry` (urllib3.Retry)
        if retry_config is not None:
            self.retry_config = retry_config
        elif retry is not None:
            warnings.warn(
                "'retry' is deprecated and will be removed in a future release. "
                "Use 'retry_config' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            self.retry_config = RetryConfig.default()
            self.retry_config.http_retry = retry
        else:
            self.retry_config = RetryConfig.default()

        if skip_verify:
            warnings.warn(
                "Skipping SSL certificate verification is insecure. "
                "Use a valid SSL certificate instead.",
                UserWarning,
            )

        # Resolve configuration values: params > env_vars > defaults
        timeout = self._resolve_timeout(timeout)
        max_pool_size = self._resolve_max_pool_size(max_pool_size)

        session_manager = SessionManager(
            retry=self.retry_config.http_retry,
            ca_cert=ca_cert,
            client_cert=client_cert,
            skip_verify=skip_verify,
            max_pool_size=max_pool_size,
        )

        # Check for token from arguments or environment variable
        if not token:
            token = os.environ.get(AIS_AUTHN_TOKEN)

        self._request_client = RequestClient(
            endpoint=endpoint,
            session_manager=session_manager,
            timeout=timeout,
            token=token,
            retry_config=self.retry_config,
        )

    def bucket(
        self,
        bck_name: str,
        provider: Union[Provider, str] = Provider.AIS,
        namespace: Namespace = None,
    ):
        """
        Factory constructor for bucket object.
        Does not make any HTTP request, only instantiates a bucket object.

        Args:
            bck_name (str): Name of bucket
            provider (str or Provider): Provider of bucket, one of "ais", "aws", "gcp", ...
                (optional, defaults to ais)
            namespace (Namespace): Namespace of bucket (optional, defaults to None)

        Returns:
            The bucket object created.
        """
        return Bucket(
            client=self._request_client,
            name=bck_name,
            provider=provider,
            namespace=namespace,
        )

    def cluster(self):
        """
        Factory constructor for cluster object.
        Does not make any HTTP request, only instantiates a cluster object.

        Returns:
            The cluster object created.
        """
        return Cluster(client=self._request_client)

    def job(self, job_id: str = "", job_kind: str = ""):
        """
        Factory constructor for job object, which contains job-related functions.
        Does not make any HTTP request, only instantiates a job object.

        Args:
            job_id (str, optional): Optional ID for interacting with a specific job
            job_kind (str, optional): Optional specific type of job empty for all kinds

        Returns:
            The job object created.
        """
        return Job(client=self._request_client, job_id=job_id, job_kind=job_kind)

    def etl(self, etl_name: str):
        """
        Factory constructor for ETL object.
        Contains APIs related to AIStore ETL operations.
        Does not make any HTTP request, only instantiates an ETL object.

        Args:
            etl_name (str): Name of the ETL

        Returns:
            The ETL object created.
        """
        return Etl(client=self._request_client, name=etl_name)

    def dsort(self, dsort_id: str = ""):
        """
        Factory constructor for dSort object.
        Contains APIs related to AIStore dSort operations.
        Does not make any HTTP request, only instantiates a dSort object.

        Args:
            dsort_id: ID of the dSort job

        Returns:
            dSort object created
        """
        return Dsort(client=self._request_client, dsort_id=dsort_id)

    def fetch_object_by_url(self, url: str) -> Object:
        """
        Deprecated: Use `get_object_from_url` instead.

        Creates an Object instance from a URL.

        This method does not make any HTTP requests.

        Args:
            url (str): Full URL of the object (e.g., "ais://bucket1/file.txt")

        Returns:
            Object: The object constructed from the specified URL
        """
        warnings.warn(
            "The 'fetch_object_by_url' method is deprecated and will be removed in a future release. "
            "Please use 'get_object_from_url' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_object_from_url(url)

    def get_object_from_url(self, url: str) -> Object:
        """
        Creates an Object instance from a URL.

        This method does not make any HTTP requests.

        Args:
            url (str): Full URL of the object (e.g., "ais://bucket1/file.txt")

        Returns:
            Object: The object constructed from the specified URL

        Raises:
            InvalidURLException: If the URL is invalid.
        """
        try:
            provider, bck_name, obj_name = parse_url(url)
            if not provider or not bck_name or not obj_name:
                raise InvalidURLException(url)
            return self.bucket(bck_name, provider=provider).object(obj_name)
        except InvalidURLException as err:
            raise err

    def batch(
        self,
        objects: Union[List[Object], Object, str, List[str]] = None,
        bucket: Optional[Bucket] = None,
        output_format: str = EXT_TAR,
        cont_on_err: bool = True,
        only_obj_name: bool = False,
        streaming_get: bool = True,
    ):
        """
        Factory constructor for Get-Batch API (MOSS - Multi-Object Streaming Service).

        Efficiently retrieve multiple objects, archive files, or byte ranges in a single request,
        reducing network overhead and improving throughput for ML training workloads.

        Args:
            objects (Optional[Union[List[Object], Object, str, List[str]]]): Objects to retrieve. Can be:
                - Single object name: "file.txt"
                - List of names: ["file1.txt", "file2.txt"]
                - Single Object instance
                - List of Object instances
                - None (add objects later via batch.add())
                Note: if objects are specified as raw names (str or list of str), bucket must be provided
            bucket (Optional[Bucket]): Default bucket for all objects
            output_format (str): Archive format (tar, tgz, zip). Defaults to ".tar"
            cont_on_err (bool): Continue on errors (missing files under __404__/). Defaults to True
            only_obj_name (bool): Use only obj name in archive path. Defaults to False
            streaming_get (bool): Stream resulting archive prior to finalizing it in memory. Defaults to True

        Returns:
            Batch: Batch object for building and executing Get-Batch requests

        Example:
            # Quick batch with string names
            batch = client.batch(["file1.txt", "file2.txt"], bucket=bucket)
            for obj_info, data in batch.get():
                print(f"Object: {obj_info.obj_name}, Size: {len(data)}")

            # Build batch incrementally with advanced options
            batch = client.batch(bucket=bucket)
            batch.add("simple.txt")
            batch.add("archive.tar", archpath="images/photo.jpg")  # extract from archive
            batch.add("tracked.txt", opaque=b"user-id-123")  # with tracking data
            for obj_info, data in batch.get():
                print(f"Object: {obj_info.obj_name}")
        """
        return Batch(
            self._request_client,
            objects,
            bucket,
            output_format,
            cont_on_err,
            only_obj_name,
            streaming_get,
        )
