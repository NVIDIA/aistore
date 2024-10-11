import unittest
from unittest.mock import Mock

from aistore.sdk.provider import Provider
from aistore.sdk.request_client import RequestClient
from aistore.sdk.authn.types import (
    BucketModel,
    BucketPermission,
    ClusterPermission,
    RoleInfo,
    UserInfo,
    UsersList,
)
from aistore.sdk.authn.access_attr import AccessAttr
from aistore.sdk.authn.user_manager import UserManager
from aistore.sdk.authn.role_manager import RoleManager
from aistore.sdk.const import (
    HTTP_METHOD_GET,
    HTTP_METHOD_DELETE,
    HTTP_METHOD_PUT,
    HTTP_METHOD_POST,
    URL_PATH_AUTHN_USERS,
)


class TestAuthNUserManager(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_client = Mock(RequestClient)
        self.mock_role_manager = Mock(RoleManager)
        self.user_manager = UserManager(self.mock_client)
        # pylint: disable=protected-access
        self.user_manager._role_manager = self.mock_role_manager

        self.mock_role_info = RoleInfo(
            name="test-role",
            desc="Test Description",
            clusters=[
                ClusterPermission(id="test-cluster", perm=str(AccessAttr.GET.value))
            ],
            buckets=[
                BucketPermission(
                    bck=BucketModel(name="test-bucket", provider=Provider.AIS.value),
                    perm=str(AccessAttr.GET.value),
                )
            ],
        )
        self.mock_role_manager.get.return_value = self.mock_role_info

    def test_user_delete(self):
        self.user_manager.delete(username="test-user")
        self.mock_client.request.assert_called_once_with(
            HTTP_METHOD_DELETE,
            path=f"{URL_PATH_AUTHN_USERS}/test-user",
        )

    def test_user_get(self):
        mock_user_info = UserInfo(
            id="test-user",
            roles=[self.mock_role_info],
        )

        self.mock_client.request_deserialize.return_value = mock_user_info

        user_info = self.user_manager.get(username="test-user")
        self.assertEqual(user_info, mock_user_info)

        self.mock_client.request_deserialize.assert_called_once_with(
            HTTP_METHOD_GET,
            path=f"{URL_PATH_AUTHN_USERS}/test-user",
            res_model=UserInfo,
        )

    def test_user_create(self):
        mock_user_info = UserInfo(
            id="test-user", roles=[self.mock_role_info], password="test-password"
        )

        self.mock_client.request_deserialize.return_value = mock_user_info

        created_user_info = self.user_manager.create(
            username="test-user",
            password="test-password",
            roles=["test-role"],
        )

        self.mock_role_manager.get.assert_called_once_with("test-role")

        self.assertEqual(created_user_info, mock_user_info)

        self.mock_client.request.assert_called_once_with(
            HTTP_METHOD_POST,
            path=URL_PATH_AUTHN_USERS,
            json=mock_user_info.dict(),
        )
        self.mock_client.request_deserialize.assert_called_once_with(
            HTTP_METHOD_GET,
            path=f"{URL_PATH_AUTHN_USERS}/test-user",
            res_model=UserInfo,
        )

    def test_user_update(self):
        mock_user_info = UserInfo(
            id="test-user", roles=[self.mock_role_info], password="new-password"
        )

        self.mock_client.request_deserialize.return_value = mock_user_info

        updated_user_info = self.user_manager.update(
            username="test-user",
            password="new-password",
            roles=["test-role"],
        )

        self.mock_role_manager.get.assert_called_once_with("test-role")

        self.assertEqual(updated_user_info, mock_user_info)

        self.mock_client.request.assert_called_once_with(
            HTTP_METHOD_PUT,
            path=f"{URL_PATH_AUTHN_USERS}/test-user",
            json=mock_user_info.dict(),
        )
        self.mock_client.request_deserialize.assert_called_once_with(
            HTTP_METHOD_GET,
            path=f"{URL_PATH_AUTHN_USERS}/test-user",
            res_model=UserInfo,
        )

    def test_user_list(self):
        mock_users_list = [
            UserInfo(
                id="test-user-1",
                roles=[self.mock_role_info],
            ),
            UserInfo(
                id="test-user-2",
                roles=[self.mock_role_info],
            ),
        ]

        self.mock_client.request_deserialize.return_value = mock_users_list

        users_list = self.user_manager.list()
        self.assertEqual(users_list, mock_users_list)

        self.mock_client.request_deserialize.assert_called_once_with(
            HTTP_METHOD_GET,
            path=URL_PATH_AUTHN_USERS,
            res_model=UsersList,
        )
