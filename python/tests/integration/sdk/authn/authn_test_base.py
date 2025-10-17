import unittest
from typing import List, Optional, Callable, Any

from aistore import Client
from aistore.sdk import AuthNClient
from aistore.sdk.authn.access_attr import AccessAttr
from aistore.sdk.const import STATUS_UNAUTHORIZED, STATUS_FORBIDDEN
from aistore.sdk.errors import AISError
from tests.integration import (
    AUTHN_ENDPOINT,
    CLUSTER_ENDPOINT,
    AIS_AUTHN_SU_NAME,
    AIS_AUTHN_SU_PASS,
)
from tests.utils import random_string


# pylint: disable=too-many-instance-attributes
class AuthNTestBase(unittest.TestCase):
    """
    Performs common setup and teardown for all auth tests
    """

    def setUp(self) -> None:
        # AuthN Client
        authn_initial_client = AuthNClient(AUTHN_ENDPOINT)
        self.admin_token = authn_initial_client.login(
            AIS_AUTHN_SU_NAME, AIS_AUTHN_SU_PASS
        )
        self.authn_client = AuthNClient(AUTHN_ENDPOINT, token=self.admin_token)
        # For cleanup if authn_client is changed
        self.admin_client = AuthNClient(AUTHN_ENDPOINT, token=self.admin_token)

        # AIStore Client
        self.ais_client = self._create_ais_client(self.admin_token)
        self.uuid = self.ais_client.cluster().get_uuid()
        self.cluster_alias = "authn-test-cluster"
        self.bck = self.ais_client.bucket(random_string())
        self.bck.create()

        self.cluster_info = self._register_if_needed(self.uuid, self.cluster_alias)
        self.roles = []
        self.user_names = []
        self.buckets = [self.bck]

    def _register_if_needed(self, uuid, alias):
        cluster_manager = self.authn_client.cluster_manager()
        # Register the AIS Cluster only if it does not exist
        cluster_list = cluster_manager.list()
        if uuid in cluster_list.clusters.keys():
            return cluster_list.clusters.get(uuid)
        return cluster_manager.register(alias, [CLUSTER_ENDPOINT])

    def tearDown(self) -> None:
        cm = self.admin_client.cluster_manager()
        if self.uuid in cm.list().clusters.keys():
            cm.delete(cluster_id=self.uuid)
        for role in self.roles:
            self.admin_client.role_manager().delete(name=role.name, missing_ok=True)
        for user_name in self.user_names:
            self.admin_client.user_manager().delete(username=user_name, missing_ok=True)
        for bck in self.buckets:
            # Use the admin AIS client to delete
            self.ais_client.bucket(bck.name).delete(missing_ok=True)

    def _create_role(
        self, access_attrs: List[AccessAttr], bucket_name: Optional[str] = None
    ):
        if bucket_name:
            role = self.admin_client.role_manager().create(
                name=f"Test-Role-{random_string()}",
                desc="Test Description",
                cluster_alias=self.cluster_info.alias,
                perms=access_attrs,
                bucket_name=bucket_name,
            )
        else:
            role = self.admin_client.role_manager().create(
                name=f"Test-Role-{random_string()}",
                desc="Test Description",
                cluster_alias=self.cluster_info.alias,
                perms=access_attrs,
            )
        self.roles.append(role)
        return role

    def _create_user(self, roles: List[str], password: str = "12345"):
        user = self.admin_client.user_manager().create(
            username=f"Test-User-{random_string()}",
            password=password,
            roles=roles,
        )
        self.user_names.append(user.id)
        return user

    def _assert_forbidden(self, func: Callable[[], Any]):
        with self.assertRaises(AISError) as e:
            # Return result to use for cleanup if call does succeed
            return func()
        self.assertEqual(STATUS_FORBIDDEN, e.exception.status_code)
        return None

    def _assert_unauthorized(self, func: Callable[[], Any]):
        with self.assertRaises(AISError) as e:
            # Return result to use for cleanup if call does succeed
            return func()
        self.assertEqual(STATUS_UNAUTHORIZED, e.exception.status_code)
        return None

    @staticmethod
    def _create_ais_client(token: str):
        return Client(CLUSTER_ENDPOINT, token=token)
