#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

from typing import List, Optional
from aistore.sdk.request_client import RequestClient
from aistore.sdk.client import Client as AISClient
from aistore.sdk.authn.types import ClusterInfo, ClusterList
from aistore.sdk.utils import get_logger
from aistore.sdk.const import (
    HTTP_METHOD_GET,
    HTTP_METHOD_POST,
    HTTP_METHOD_PUT,
    HTTP_METHOD_DELETE,
    URL_PATH_AUTHN_CLUSTERS,
)

logger = get_logger(__name__)


class ClusterManager:
    """
    ClusterManager class for handling operations on clusters within the context of authentication.

    This class provides methods to list, get, register, update, and delete clusters on AuthN server.

    Args:
        client (RequestClient): The request client to make HTTP requests.
    """

    def __init__(self, client: RequestClient):
        self._client = client

    @property
    def client(self) -> RequestClient:
        """RequestClient: The client this cluster manager uses to make requests."""
        return self._client

    def list(self) -> ClusterList:
        """
        Retrieve all clusters.

        Returns:
            ClusterList: A list of all clusters.

        Raises:
            AISError: If an error occurs while listing clusters.
        """
        logger.info("Listing all clusters")
        return self.client.request_deserialize(
            HTTP_METHOD_GET,
            path=URL_PATH_AUTHN_CLUSTERS,
            res_model=ClusterList,
        )

    def get(
        self, cluster_id: Optional[str] = None, cluster_alias: Optional[str] = None
    ) -> ClusterInfo:
        """
        Retrieve a specific cluster by ID or alias.

        Args:
            cluster_id (Optional[str]): The ID of the cluster. Defaults to None.
            cluster_alias (Optional[str]): The alias of the cluster. Defaults to None.

        Returns:
            ClusterInfo: Information about the specified cluster.

        Raises:
            ValueError: If neither cluster_id nor cluster_alias is provided.
            RuntimeError: If no cluster matches the provided ID or alias.
            AISError: If an error occurs while getting the cluster.
        """
        if not cluster_id and not cluster_alias:
            raise ValueError(
                "At least one of cluster_id or cluster_alias must be provided"
            )

        cluster_id_value = cluster_id or cluster_alias

        logger.info("Getting cluster with ID or alias: %s", cluster_id_value)

        cluster_list = self.client.request_deserialize(
            HTTP_METHOD_GET,
            path=f"{URL_PATH_AUTHN_CLUSTERS}/{cluster_id_value}",
            res_model=ClusterList,
        )

        if not cluster_list.clusters:
            raise RuntimeError(
                f"No cluster found with ID or alias '{cluster_id_value}'"
            )

        first_key = next(iter(cluster_list.clusters))
        return cluster_list.clusters[first_key]

    def register(self, cluster_alias: str, urls: List[str]) -> ClusterInfo:
        """
        Register a new cluster.

        Args:
            cluster_alias (str): The alias for the new cluster.
            urls (List[str]): A list of URLs for the new cluster.

        Returns:
            ClusterInfo: Information about the registered cluster.

        Raises:
            ValueError: If no URLs are provided or an invalid URL is provided.
            AISError: If an error occurs while registering the cluster.
        """
        if not urls:
            raise ValueError("At least one URL must be provided")

        logger.info(
            "Registering new cluster with alias: %s and URLs: %s", cluster_alias, urls
        )

        try:
            ais_client = AISClient(
                endpoint=urls[0], token=self.client.token, skip_verify=True
            )
            uuid = ais_client.cluster().get_uuid()
        except Exception as err:
            raise ValueError(
                f"Failed to retrieve UUID for the provided URL: {urls[0]}. "
                f"Ensure the URL is correct and the endpoint is accessible."
            ) from err

        if not uuid:
            raise ValueError("Failed to retrieve UUID for the provided URL")

        self.client.request(
            HTTP_METHOD_POST,
            path=URL_PATH_AUTHN_CLUSTERS,
            json={"id": uuid, "alias": cluster_alias, "urls": urls},
        )

        return self.get(uuid)

    def update(
        self,
        cluster_id: str,
        cluster_alias: Optional[str] = None,
        urls: Optional[List[str]] = None,
    ) -> ClusterInfo:
        """
        Update an existing cluster.

        Args:
            cluster_id (str): The ID of the cluster to update.
            cluster_alias (Optional[str]): The new alias for the cluster. Defaults to None.
            urls (Optional[List[str]]): The new list of URLs for the cluster. Defaults to None.

        Returns:
            ClusterInfo: Information about the updated cluster.

        Raises:
            ValueError: If neither cluster_alias nor urls are provided.
            AISError: If an error occurs while updating the cluster
        """
        if not cluster_id:
            raise ValueError("Cluster ID must be provided")
        if cluster_alias is None and urls is None:
            raise ValueError("At least one of cluster_alias or urls must be provided")

        cluster_info = ClusterInfo(id=cluster_id)
        if cluster_alias:
            cluster_info.alias = cluster_alias
        if urls:
            cluster_info.urls = urls

        logger.info("Updating cluster with ID: %s", cluster_id)

        self.client.request(
            HTTP_METHOD_PUT,
            path=f"{URL_PATH_AUTHN_CLUSTERS}/{cluster_id}",
            json=cluster_info.dict(),
        )
        return self.get(cluster_id=cluster_id)

    def delete(
        self, cluster_id: Optional[str] = None, cluster_alias: Optional[str] = None
    ):
        """
        Delete a specific cluster by ID or alias.

        Args:
            cluster_id (Optional[str]): The ID of the cluster to delete. Defaults to None.
            cluster_alias (Optional[str]): The alias of the cluster to delete. Defaults to None.

        Raises:
            ValueError: If neither cluster_id nor cluster_alias is provided.
            AISError: If an error occurs while deleting the cluster
        """
        if not cluster_id and not cluster_alias:
            raise ValueError(
                "At least one of cluster_id or cluster_alias must be provided"
            )

        cluster_id_value = cluster_id or cluster_alias

        logger.info("Deleting cluster with ID or alias: %s", cluster_id_value)

        self.client.request(
            HTTP_METHOD_DELETE,
            path=f"{URL_PATH_AUTHN_CLUSTERS}/{cluster_id_value}",
        )
