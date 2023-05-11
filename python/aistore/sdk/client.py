#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations  # pylint: disable=unused-variable

from aistore.sdk.bucket import Bucket
from aistore.sdk.const import (
    PROVIDER_AIS,
)
from aistore.sdk.cluster import Cluster
from aistore.sdk.dsort import Dsort
from aistore.sdk.request_client import RequestClient
from aistore.sdk.types import Namespace
from aistore.sdk.job import Job
from aistore.sdk.etl import Etl


# pylint: disable=unused-variable
class Client:
    """
    AIStore client for managing buckets, objects, ETL jobs

    Args:
        endpoint (str): AIStore endpoint
    """

    def __init__(self, endpoint: str):
        self._request_client = RequestClient(endpoint)

    def bucket(
        self, bck_name: str, provider: str = PROVIDER_AIS, namespace: Namespace = None
    ):
        """
        Factory constructor for bucket object.
        Does not make any HTTP request, only instantiates a bucket object.

        Args:
            bck_name (str): Name of bucket
            provider (str): Provider of bucket, one of "ais", "aws", "gcp", ... (optional, defaults to ais)
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
