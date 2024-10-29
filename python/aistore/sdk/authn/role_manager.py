#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#
from typing import List

from aistore.sdk.provider import Provider
from aistore.sdk.request_client import RequestClient
from aistore.sdk.authn.access_attr import AccessAttr
from aistore.sdk.authn.cluster_manager import ClusterManager
from aistore.sdk.types import BucketModel
from aistore.sdk.namespace import Namespace
from aistore.sdk.authn.errors import ErrRoleNotFound
from aistore.sdk.utils import get_logger
from aistore.sdk.const import (
    HTTP_METHOD_GET,
    HTTP_METHOD_POST,
    HTTP_METHOD_PUT,
    HTTP_METHOD_DELETE,
    URL_PATH_AUTHN_ROLES,
)
from aistore.sdk.authn.types import (
    RoleInfo,
    RolesList,
    BucketPermission,
    ClusterPermission,
)

logger = get_logger(__name__)


class RoleManager:
    """
    Manages role-related operations.

    This class provides methods to interact with roles, including
    retrieving, creating, updating, and deleting role information.

    Args:
        client (RequestClient): The RequestClient used to make HTTP requests.
    """

    # pylint: disable=too-many-arguments
    def __init__(self, client: RequestClient):
        self._client = client

    @property
    def client(self) -> RequestClient:
        """Returns the RequestClient instance used by this RoleManager."""
        return self._client

    def list(self) -> RolesList:
        """
        Retrieves information about all roles.

        Returns:
            RoleList: A list containing information about all roles.

        Raises:
            aistore.sdk.errors.AISError: All other types of errors with AIStore.
            requests.RequestException: If the HTTP request fails.
        """
        logger.info("Listing all roles")
        return self.client.request_deserialize(
            HTTP_METHOD_GET,
            path=URL_PATH_AUTHN_ROLES,
            res_model=RolesList,
        )

    def get(self, role_name: str) -> RoleInfo:
        """
        Retrieves information about a specific role.

        Args:
            role_name (str): The name of the role to retrieve.

        Returns:
            RoleInfo: Information about the specified role.

        Raises:
            aistore.sdk.errors.AISError: All other types of errors with AIStore.
            requests.RequestException: If the HTTP request fails.
        """
        logger.info("Getting role with name: %s", role_name)
        return self.client.request_deserialize(
            HTTP_METHOD_GET,
            path=f"{URL_PATH_AUTHN_ROLES}/{role_name}",
            res_model=RoleInfo,
        )

    def create(
        self,
        name: str,
        desc: str,
        cluster_alias: str,
        perms: List[AccessAttr],
        bucket_name: str = None,
    ) -> RoleInfo:
        """
        Creates a new role.

        Args:
            name (str): The name of the role.
            desc (str): A description of the role.
            cluster_alias (str): The alias of the cluster this role will have access to.
            perms (List[AccessAttr]): A list of permissions to be granted for this role.
            bucket_name (str, optional): The name of the bucket this role will have access to.

        Returns:
            RoleInfo: Information about the newly created role.

        Raises:
            aistore.sdk.errors.AISError: All other types of errors with AIStore.
            requests.RequestException: If the HTTP request fails.
        """
        # Convert the list of AccessAttr to an integer representing the permissions
        perm_value = sum(perm.value for perm in perms)

        cluster_uuid = ClusterManager(self.client).get(cluster_alias=cluster_alias).id
        role_info = RoleInfo(name=name, desc=desc)
        if bucket_name:
            role_info.buckets = [
                BucketPermission(
                    bck=BucketModel(
                        name=bucket_name,
                        provider=Provider.AIS.value,
                        namespace=Namespace(uuid=cluster_uuid),
                    ),
                    perm=perm_value,
                )
            ]
        else:
            role_info.clusters = [ClusterPermission(id=cluster_uuid, perm=perm_value)]

        logger.info("Creating role with name: %s", name)
        self.client.request(
            HTTP_METHOD_POST,
            path=URL_PATH_AUTHN_ROLES,
            json=role_info.dict(),
        )

        return self.get(role_name=name)

    def update(
        self,
        name: str,
        desc: str = None,
        cluster_alias: str = None,
        perms: List[AccessAttr] = None,
        bucket_name: str = None,
    ) -> RoleInfo:
        """
        Updates an existing role.

        Args:
            name (str): The name of the role.
            desc (str, optional): An updated description of the role.
            cluster_alias (str, optional): The alias of the cluster this role will have access to.
            perms (List[AccessAttr], optional): A list of updated permissions to be granted for this role.
            bucket_name (str, optional): The name of the bucket this role will have access to.

        Raises:
            aistore.sdk.errors.AISError: All other types of errors with AIStore.
            requests.RequestException: If the HTTP request fails.
            ValueError: If the role does not exist or if invalid parameters are provided.
        """

        if not (desc or cluster_alias or perms or bucket_name):
            raise ValueError(
                "You must change either the description or permissions for a bucket or cluster."
            )

        if perms and not cluster_alias:
            raise ValueError(
                "Cluster alias must be provided when permissions are specified."
            )

        if bucket_name and not cluster_alias:
            raise ValueError(
                "Cluster alias must be provided when bucket_name is specified."
            )

        if (cluster_alias or bucket_name) and not perms:
            raise ValueError(
                "Permissions must be provided when cluster alias or bucket name is specified."
            )

        try:
            role_info = self.get(role_name=name)
        except ErrRoleNotFound as error:
            raise ValueError(f"Role {name} does not exist") from error

        if desc:
            role_info.desc = desc

        if cluster_alias:
            cluster_uuid = (
                ClusterManager(self.client).get(cluster_alias=cluster_alias).id
            )
            perm_value = sum(perm.value for perm in perms)

            if bucket_name:
                logger.info(
                    "Preparing bucket-specific permissions for bucket: %s", bucket_name
                )
                role_info.buckets = [
                    BucketPermission(
                        bck=BucketModel(
                            name=bucket_name,
                            provider=Provider.AIS.value,
                            namespace=Namespace(uuid=cluster_uuid),
                        ),
                        perm=perm_value,
                    )
                ]
            else:
                logger.info(
                    "Preparing cluster-wide permissions for cluster alias: %s",
                    cluster_alias,
                )
                role_info.clusters = [
                    ClusterPermission(id=cluster_uuid, perm=perm_value)
                ]

        logger.info("Updating role with name: %s", name)
        self.client.request(
            HTTP_METHOD_PUT,
            path=f"{URL_PATH_AUTHN_ROLES}/{name}",
            json=role_info.dict(),
        )

    def delete(self, name: str, missing_ok: bool = False) -> None:
        """
        Deletes a role.

        Args:
            name (str): The name of the role to delete.
            missing_ok (bool): Ignore error if role does not exist. Defaults to False

        Raises:
            aistore.sdk.errors.AISError: All other types of errors with AIStore.
            requests.RequestException: If the HTTP request fails.
            ValueError: If the role does not exist.
        """
        logger.info("Deleting role with name: %s", name)

        try:
            self.client.request(
                HTTP_METHOD_DELETE,
                path=f"{URL_PATH_AUTHN_ROLES}/{name}",
            )
        except ErrRoleNotFound as err:
            if not missing_ok:
                raise err
