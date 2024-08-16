#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations
from typing import Optional, Union, Dict, List
from pydantic import BaseModel, Field
from aistore.sdk.authn.access_attr import AccessAttr
from aistore.sdk.const import NANOSECONDS_IN_SECOND
from aistore.sdk.types import BucketModel


# pylint: disable=missing-class-docstring, missing-function-docstring


class LoginMsg(BaseModel):
    """
    Represents a login message with a password and optional expiration duration.

    Attributes:
        password (str): The password string.
        expires_in (Optional[Union[int, float]]): The expiration duration in seconds.
    """

    password: str
    expires_in: Optional[Union[int, float]] = Field(
        None, description="Expiration duration in seconds."
    )

    def as_dict(self):
        """
        Converts the instance to a dict, converting the expiration duration to nanoseconds if specified.

        Returns:
            Dict[str, Union[str, int]]: The dict representation of the login message.
        """

        data = self.dict()
        if self.expires_in is not None:
            data["expires_in"] = int(
                self.expires_in * NANOSECONDS_IN_SECOND
            )  # Convert seconds to nanoseconds
        return data


class TokenMsg(BaseModel):
    """
    Represents a message containing a token.

    Attributes:
        token (str): The token string.
    """

    token: str


class ClusterInfo(BaseModel):
    """
    Represents information about a cluster.

    Attributes:
        id (str): The unique identifier of the cluster.
        alias (Optional[str]): The alias name of the cluster. Defaults to None.
        urls (List[str]): A list of URLs associated with the cluster.
    """

    id: str
    alias: Optional[str] = None
    urls: List[str] = []


class ClusterList(BaseModel):
    """
    Represents a list of clusters.

    Attributes:
        clusters (Dict[str, ClusterInfo]): A dictionary of cluster IDs to ClusterInfo objects.
    """

    clusters: Dict[str, ClusterInfo] = {}


class ClusterPermission(BaseModel):
    """
    Represents a cluster with its associated permissions.
    """

    id: str
    perm: str

    def describe(self) -> str:
        return AccessAttr.describe(int(self.perm))

    def __str__(self) -> str:
        return f"ClusterPermission(id={self.id}, perm={self.describe()})"


class BucketPermission(BaseModel):
    """
    Represents a bucket with its associated permissions.
    """

    bck: BucketModel
    perm: str

    def describe(self) -> str:
        return AccessAttr.describe(int(self.perm))

    def __str__(self) -> str:
        return f"BucketPermission(bck={self.bck}, perm={self.describe()})"


class RoleInfo(BaseModel):
    """
    Represents role information including permissions for clusters and buckets.

    Attributes:
        name (str): Name of the role.
        desc (str): Description of the role.
        clusters (Optional[List[ClusterPermission]]): List of cluster permissions.
        buckets (Optional[List[BucketPermission]]): List of bucket permissions.
        admin (bool): Whether the role has admin privileges.
    """

    name: str
    desc: str
    clusters: List[ClusterPermission] = None
    buckets: List[BucketPermission] = None
    admin: bool = False

    def __str__(self) -> str:
        if self.clusters is None:
            clusters_str = "None"
        else:
            clusters_str = ", ".join(str(cluster) for cluster in self.clusters)

        if self.buckets is None:
            buckets_str = "None"
        else:
            buckets_str = ", ".join(str(bucket) for bucket in self.buckets)

        return (
            f"RoleInfo(name={self.name}, desc={self.desc}, "
            f"clusters=[{clusters_str}], buckets=[{buckets_str}], admin={self.admin})"
        )


class RolesList(BaseModel):
    """
    Represents a list of roles.

    Attributes:
        __root__ (List[RoleInfo]): List of roles.
    """

    __root__: List[RoleInfo]

    def __iter__(self):
        return iter(self.__root__)

    def __getitem__(self, item):
        return self.__root__[item]

    def __len__(self):
        return len(self.__root__)

    @property
    def roles(self):
        return self.__root__

    def __str__(self) -> str:
        return "\n".join(str(role) for role in self.__root__)


class UserInfo(BaseModel):
    """
    Represents user information in the AuthN service.

    Attributes:
        id (str): The username or ID of the user.
        password (str, optional): The user's password. Serialized as 'pass' in the request.
        roles (RolesList): The list of roles assigned to the user.
    """

    id: str
    password: Optional[str] = None
    roles: RolesList

    def dict(self, **kwargs):
        """
        Override the dict method to serialize the 'password' field as 'pass'.

        Returns:
            Dict[str, Union[str, RolesList]]: The dict representation of the user information.
        """
        user_dict = super().dict(**kwargs)
        if "password" in user_dict and user_dict["password"] is not None:
            user_dict["pass"] = user_dict.pop("password")
        return user_dict


class UsersList(BaseModel):
    """
    Represents a list of users.

    Attributes:
        __root__ (Dict[str, UserInfo]): Dictionary of user names/IDs to UserInfo objects.
    """

    __root__: Dict[str, UserInfo]

    def __iter__(self):
        return iter(self.__root__.values())

    def __getitem__(self, item):
        return self.__root__[item]

    def __len__(self):
        return len(self.__root__)

    @property
    def users(self):
        return self.__root__

    def __str__(self) -> str:
        return "\n".join(str(user) for user in self.__root__)
