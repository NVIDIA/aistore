#
# Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
#
from typing import Optional, Tuple, Union
import os

from urllib3 import Retry

from aistore.sdk.bucket import Bucket
from aistore.sdk.provider import Provider
from aistore.sdk.const import AIS_AUTHN_TOKEN
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


class Client:
    """
    AIStore client for managing buckets, objects, ETL jobs

    Args:
        endpoint (str): AIStore endpoint
        skip_verify (bool, optional): If True, skip SSL certificate verification. Defaults to False.
        ca_cert (str, optional): Path to a CA certificate file for SSL verification. If not provided, the
            'AIS_CLIENT_CA' environment variable will be used. Defaults to None.
        client_cert (Union[str, Tuple[str, str], None], optional): Path to a client certificate PEM file
            or a path pair (cert, key) for mTLS. If not provided, 'AIS_CRT' and 'AIS_CRT_KEY'
            environment variables will be used. Defaults to None.
        timeout (Union[float, Tuple[float, float], None], optional): Request timeout in seconds; a single float
            for both connect/read timeouts (e.g., 5.0), a tuple for separate connect/read timeouts (e.g., (3.0, 10.0)),
            or None to disable timeout.
        retry (urllib3.Retry, optional): Retry configuration object from the urllib3 library.
        token (str, optional): Authorization token. If not provided, the 'AIS_AUTHN_TOKEN' environment variable
            will be used. Defaults to None.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        endpoint: str,
        skip_verify: bool = False,
        ca_cert: Optional[str] = None,
        client_cert: Optional[Union[str, Tuple[str, str]]] = None,
        timeout: Optional[Union[float, Tuple[float, float]]] = None,
        retry: Optional[Retry] = None,
        token: Optional[str] = None,
    ):
        session_manager = SessionManager(
            retry=retry,
            ca_cert=ca_cert,
            client_cert=client_cert,
            skip_verify=skip_verify,
        )

        # Check for token from arguments or environment variable
        if not token:
            token = os.environ.get(AIS_AUTHN_TOKEN, None)
        self._request_client = RequestClient(
            endpoint=endpoint,
            session_manager=session_manager,
            timeout=timeout,
            token=token,
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
        Retrieve an object based on its URL.

        Args:
            url (str): Full URL of the object (e.g., "ais://bucket1/file.txt")

        Returns:
            Object: The object retrieved from the specified URL
        """
        try:
            provider, bck_name, obj_name = parse_url(url)
            if not provider or not bck_name or not obj_name:
                raise InvalidURLException(url)
            return self.bucket(bck_name, provider=provider).object(obj_name)
        except InvalidURLException as err:
            raise err
