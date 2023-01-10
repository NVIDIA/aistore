import unittest
from unittest import mock
from unittest.mock import Mock

from aistore.sdk import Client
from aistore.sdk.bucket import Bucket, Header

from aistore.sdk.const import (
    ACT_CREATE_BCK,
    HTTP_METHOD_POST,
    ProviderAmazon,
    QParamBucketTo,
    ProviderAIS,
    ACT_MOVE_BCK,
    ACT_DESTROY_BCK,
    HTTP_METHOD_DELETE,
    QParamKeepBckMD,
    ACT_EVICT_REMOTE_BCK,
    HTTP_METHOD_HEAD,
    ACT_COPY_BCK,
    ACT_LIST,
    HTTP_METHOD_GET,
    ACT_ETL_BCK,
)
from aistore.sdk.errors import InvalidBckProvider
from aistore.sdk.types import (
    ActionMsg,
    BucketList,
    BucketLister,
    BucketEntry,
    Bck,
    Namespace,
)

bck_name = "bucket_name"
namespace = "namespace"


# pylint: disable=too-many-public-methods
class TestBucket(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.mock_client = Mock(Client)
        self.amz_bck = Bucket(self.mock_client, bck_name, provider=ProviderAmazon)
        self.amz_bck_params = self.amz_bck.qparam.copy()
        self.ais_bck = Bucket(
            self.mock_client, bck_name, ns=Namespace(uuid="", name=namespace)
        )
        self.ais_bck_params = self.ais_bck.qparam.copy()

    def test_default_props(self):
        self.ais_bck = Bucket(self.mock_client, bck_name)
        self.assertEqual(ProviderAIS, self.ais_bck.provider)
        self.assertIsNone(self.ais_bck.namespace)

    def test_properties(self):
        self.assertEqual(self.mock_client, self.ais_bck.client)
        expected_ns = Namespace(uuid="", name=namespace)
        expected_bck = Bck(name=bck_name, provider=ProviderAIS, ns=expected_ns)
        self.assertEqual(expected_bck, self.ais_bck.bck)
        self.assertEqual(ProviderAIS, self.ais_bck.provider)
        self.assertEqual(bck_name, self.ais_bck.name)
        self.assertEqual(expected_ns, self.ais_bck.namespace)

    def test_create_invalid_provider(self):
        self.assertRaises(InvalidBckProvider, self.amz_bck.create)

    def test_create_success(self):
        self.ais_bck.create()
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST,
            path=f"buckets/{bck_name}",
            json=ActionMsg(action=ACT_CREATE_BCK).dict(),
            params=self.ais_bck.qparam,
        )

    def test_rename_invalid_provider(self):
        self.assertRaises(InvalidBckProvider, self.amz_bck.rename, "new_name")

    def test_rename_success(self):
        new_bck_name = "new_bucket"
        expected_response = "rename_op_123"
        self.ais_bck_params[QParamBucketTo] = f"{ProviderAIS}/@#/{new_bck_name}/"
        mock_response = Mock()
        mock_response.text = expected_response
        self.mock_client.request.return_value = mock_response

        response = self.ais_bck.rename(new_bck_name)

        self.assertEqual(expected_response, response)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST,
            path=f"buckets/{bck_name}",
            json=ActionMsg(action=ACT_MOVE_BCK).dict(),
            params=self.ais_bck_params,
        )
        self.assertEqual(self.ais_bck.name, new_bck_name)

    def test_delete_invalid_provider(self):
        self.assertRaises(InvalidBckProvider, self.amz_bck.delete)

    def test_delete_success(self):
        self.ais_bck.delete()
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_DELETE,
            path=f"buckets/{bck_name}",
            json=ActionMsg(action=ACT_DESTROY_BCK).dict(),
            params=self.ais_bck.qparam,
        )

    def test_evict_invalid_provider(self):
        self.assertRaises(InvalidBckProvider, self.ais_bck.evict)

    def test_evict_success(self):
        for keep_md in [True, False]:
            self.amz_bck_params[QParamKeepBckMD] = keep_md
            self.amz_bck.evict(keep_md=keep_md)
            self.mock_client.request.assert_called_with(
                HTTP_METHOD_DELETE,
                path=f"buckets/{bck_name}",
                json=ActionMsg(action=ACT_EVICT_REMOTE_BCK).dict(),
                params=self.amz_bck_params,
            )

    def test_head(self):
        mock_header = Mock()
        mock_header.headers = Header("value")
        self.mock_client.request.return_value = mock_header
        headers = self.ais_bck.head()
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_HEAD,
            path=f"buckets/{bck_name}",
            params=self.ais_bck.qparam,
        )
        self.assertEqual(headers, mock_header.headers)

    def test_copy_default_params(self):
        action_value = {"prefix": "", "dry_run": False, "force": False}
        self._copy_exec_assert("new_bck", ProviderAIS, action_value)

    def test_copy(self):
        prefix = "prefix-"
        dry_run = True
        force = True
        action_value = {"prefix": prefix, "dry_run": dry_run, "force": force}

        self._copy_exec_assert(
            "new_bck",
            ProviderAmazon,
            action_value,
            prefix=prefix,
            dry_run=dry_run,
            force=force,
        )

    def _copy_exec_assert(self, to_bck_name, to_provider, expected_act_value, **kwargs):
        expected_response = "copy-action-id"
        mock_response = Mock()
        mock_response.text = expected_response
        self.mock_client.request.return_value = mock_response
        self.ais_bck_params[QParamBucketTo] = f"{to_provider}/@#/{to_bck_name}/"
        expected_action = ActionMsg(
            action=ACT_COPY_BCK, value=expected_act_value
        ).dict()

        job_id = self.ais_bck.copy(to_bck_name, to_provider=to_provider, **kwargs)

        self.assertEqual(expected_response, job_id)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST,
            path=f"buckets/{bck_name}",
            json=expected_action,
            params=self.ais_bck_params,
        )

    def test_list_objects(self):
        prefix = "prefix-"
        page_size = 0
        uuid = "1234"
        props = "name"
        continuation_token = "token"
        expected_act_value = {
            "prefix": prefix,
            "pagesize": page_size,
            "uuid": uuid,
            "props": props,
            "continuation_token": continuation_token,
        }
        self._list_objects_exec_assert(
            expected_act_value,
            prefix=prefix,
            page_size=page_size,
            uuid=uuid,
            props=props,
            continuation_token=continuation_token,
        )

    def test_list_objects_default_params(self):
        expected_act_value = {
            "prefix": "",
            "pagesize": 0,
            "uuid": "",
            "props": "",
            "continuation_token": "",
        }
        self._list_objects_exec_assert(expected_act_value)

    def _list_objects_exec_assert(self, expected_act_value, **kwargs):
        action = ActionMsg(action=ACT_LIST, value=expected_act_value).dict()

        return_val = Mock(BucketList)
        self.mock_client.request_deserialize.return_value = return_val
        result = self.ais_bck.list_objects(**kwargs)
        self.mock_client.request_deserialize.assert_called_with(
            HTTP_METHOD_GET,
            path=f"buckets/{bck_name}",
            res_model=BucketList,
            json=action,
            params=self.ais_bck_params,
        )
        self.assertEqual(result, return_val)

    def test_list_objects_iter(self):
        prefix = "prefix-"
        props = "name"
        page_size = 123
        expected_res = BucketLister(
            self.mock_client,
            bck_name=bck_name,
            provider=ProviderAIS,
            prefix=prefix,
            props=props,
            page_size=page_size,
        )
        self.assertEqual(
            expected_res.__dict__,
            self.ais_bck.list_objects_iter(prefix, props, page_size).__dict__,
        )

    def test_list_objects_iter_default_params(self):
        expected_res = BucketLister(
            self.mock_client,
            bck_name=bck_name,
            provider=ProviderAIS,
            prefix="",
            props="",
            page_size=0,
        )
        self.assertEqual(
            expected_res.__dict__,
            self.ais_bck.list_objects_iter().__dict__,
        )

    def test_list_all_objects(self):
        list_1_id = "123"
        list_1_cont = "cont"
        prefix = "prefix-"
        page_size = 5
        props = "name"
        expected_act_value_1 = {
            "prefix": prefix,
            "pagesize": page_size,
            "uuid": "",
            "props": props,
            "continuation_token": "",
        }
        expected_act_value_2 = {
            "prefix": prefix,
            "pagesize": page_size,
            "uuid": list_1_id,
            "props": props,
            "continuation_token": list_1_cont,
        }
        self._list_all_objects_exec_assert(
            list_1_id,
            list_1_cont,
            expected_act_value_1,
            expected_act_value_2,
            prefix=prefix,
            page_size=page_size,
            props=props,
        )

    def test_list_all_objects_default_params(self):
        list_1_id = "123"
        list_1_cont = "cont"
        expected_act_value_1 = {
            "prefix": "",
            "pagesize": 0,
            "uuid": "",
            "props": "",
            "continuation_token": "",
        }
        expected_act_value_2 = {
            "prefix": "",
            "pagesize": 0,
            "uuid": list_1_id,
            "props": "",
            "continuation_token": list_1_cont,
        }
        self._list_all_objects_exec_assert(
            list_1_id, list_1_cont, expected_act_value_1, expected_act_value_2
        )

    def _list_all_objects_exec_assert(
        self,
        list_1_id,
        list_1_cont,
        expected_act_value_1,
        expected_act_value_2,
        **kwargs,
    ):
        entry_1 = BucketEntry(name="entry1")
        entry_2 = BucketEntry(name="entry2")
        entry_3 = BucketEntry(name="entry3")
        list_1 = BucketList(uuid=list_1_id, continuation_token=list_1_cont, flags=0)
        list_1.entries = [entry_1]
        list_2 = BucketList(uuid="456", continuation_token="", flags=0)
        list_2.entries = [entry_2, entry_3]

        self.mock_client.request_deserialize.return_value = BucketList(
            uuid="empty", continuation_token="", flags=0
        )
        self.assertEqual([], self.ais_bck.list_all_objects(**kwargs))

        action_1 = ActionMsg(action=ACT_LIST, value=expected_act_value_1).dict()
        action_2 = ActionMsg(action=ACT_LIST, value=expected_act_value_2).dict()

        self.mock_client.request_deserialize.side_effect = [list_1, list_2]
        result = self.ais_bck.list_all_objects(**kwargs)
        self.assertEqual([entry_1, entry_2, entry_3], result)

        call_1 = mock.call(
            HTTP_METHOD_GET,
            path=f"buckets/{bck_name}",
            res_model=BucketList,
            json=action_1,
            params=self.ais_bck_params,
        )

        call_2 = mock.call(
            HTTP_METHOD_GET,
            path=f"buckets/{bck_name}",
            res_model=BucketList,
            json=action_2,
            params=self.ais_bck_params,
        )

        self.mock_client.request_deserialize.assert_has_calls([call_1, call_2])

    def test_transform(self):
        etl_id = "etl-id"
        prefix = "prefix-"
        ext = {"jpg": "txt"}
        force = True
        dry_run = True
        action_value = {
            "id": etl_id,
            "prefix": prefix,
            "force": force,
            "dry_run": dry_run,
            "ext": ext,
        }

        self._transform_exec_assert(
            etl_id, action_value, prefix=prefix, ext=ext, force=force, dry_run=dry_run
        )

    def test_transform_default_params(self):
        etl_id = "etl-id"
        action_value = {"id": etl_id, "prefix": "", "force": False, "dry_run": False}

        self._transform_exec_assert(etl_id, action_value)

    def _transform_exec_assert(self, etl_id, expected_act_value, **kwargs):
        to_bck = "new-bucket"
        self.ais_bck_params[QParamBucketTo] = f"{ProviderAIS}/@#/{to_bck}/"
        expected_action = ActionMsg(action=ACT_ETL_BCK, value=expected_act_value).dict()
        expected_response = "job-id"
        mock_response = Mock()
        mock_response.text = expected_response
        self.mock_client.request.return_value = mock_response

        result_id = self.ais_bck.transform(etl_id, to_bck, **kwargs)

        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST,
            path=f"buckets/{bck_name}",
            json=expected_action,
            params=self.ais_bck_params,
        )
        self.assertEqual(expected_response, result_id)

    def test_object(self):
        new_obj = self.ais_bck.object(obj_name="name")
        self.assertEqual(self.ais_bck, new_obj.bck)


if __name__ == "__main__":
    unittest.main()
