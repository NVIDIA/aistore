import unittest
from unittest import mock
from unittest.mock import Mock, call, patch

from aistore.sdk.ais_source import AISSource
from aistore.sdk.bucket import Bucket, Header
from aistore.sdk.etl_const import DEFAULT_ETL_TIMEOUT
from aistore.sdk.object_iterator import ObjectIterator
from aistore.sdk import ListObjectFlag

from aistore.sdk.const import (
    ACT_COPY_BCK,
    ACT_CREATE_BCK,
    ACT_DESTROY_BCK,
    ACT_ETL_BCK,
    ACT_EVICT_REMOTE_BCK,
    ACT_LIST,
    ACT_MOVE_BCK,
    PROVIDER_AMAZON,
    PROVIDER_AIS,
    QPARAM_BCK_TO,
    QPARAM_NAMESPACE,
    QPARAM_PROVIDER,
    QPARAM_KEEP_REMOTE,
    HTTP_METHOD_DELETE,
    HTTP_METHOD_GET,
    HTTP_METHOD_HEAD,
    HTTP_METHOD_PUT,
    HTTP_METHOD_POST,
    URL_PATH_BUCKETS,
    HEADER_ACCEPT,
    MSGPACK_CONTENT_TYPE,
)
from aistore.sdk.errors import InvalidBckProvider, ErrBckAlreadyExists, ErrBckNotFound
from aistore.sdk.request_client import RequestClient
from aistore.sdk.types import (
    ActionMsg,
    BucketList,
    BucketEntry,
    Namespace,
    TCBckMsg,
    TransformBckMsg,
    CopyBckMsg,
)

BCK_NAME = "bucket_name"


# pylint: disable=too-many-public-methods,unused-variable
class TestBucket(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_client = Mock(RequestClient)
        self.amz_bck = Bucket(
            name=BCK_NAME, client=self.mock_client, provider=PROVIDER_AMAZON
        )
        self.amz_bck_params = self.amz_bck.qparam.copy()
        self.ais_bck = Bucket(name=BCK_NAME, client=self.mock_client)
        self.ais_bck_params = self.ais_bck.qparam.copy()

    def test_default_props(self):
        bucket = Bucket(name=BCK_NAME, client=self.mock_client)
        self.assertEqual({QPARAM_PROVIDER: PROVIDER_AIS}, bucket.qparam)
        self.assertEqual(PROVIDER_AIS, bucket.provider)
        self.assertIsNone(bucket.namespace)

    def test_properties(self):
        self.assertEqual(self.mock_client, self.ais_bck.client)
        expected_ns = Namespace(uuid="ns-id", name="ns-name")
        client = RequestClient("test client name")
        bck = Bucket(
            client=client,
            name=BCK_NAME,
            provider=PROVIDER_AMAZON,
            namespace=expected_ns,
        )
        self.assertEqual(client, bck.client)
        self.assertEqual(PROVIDER_AMAZON, bck.provider)
        self.assertEqual(
            {
                QPARAM_PROVIDER: PROVIDER_AMAZON,
                QPARAM_NAMESPACE: expected_ns.get_path(),
            },
            bck.qparam,
        )
        self.assertEqual(BCK_NAME, bck.name)
        self.assertEqual(expected_ns, bck.namespace)

    def test_ais_source(self):
        self.assertIsInstance(self.ais_bck, AISSource)

    def test_create_invalid_provider(self):
        self.assertRaises(InvalidBckProvider, self.amz_bck.create)

    def _assert_bucket_created(self, bck):
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST,
            path=f"{URL_PATH_BUCKETS}/{BCK_NAME}",
            json=ActionMsg(action=ACT_CREATE_BCK).dict(),
            params=self.ais_bck.qparam,
        )
        self.assertIsInstance(bck, Bucket)

    def test_create_success(self):
        res = self.ais_bck.create()
        self._assert_bucket_created(res)

    def test_create_already_exists(self):
        already_exists_err = ErrBckAlreadyExists(400, "message")
        self.mock_client.request.side_effect = already_exists_err
        with self.assertRaises(ErrBckAlreadyExists):
            self.ais_bck.create()

        res = self.ais_bck.create(exist_ok=True)
        self._assert_bucket_created(res)

    def test_rename_invalid_provider(self):
        self.assertRaises(InvalidBckProvider, self.amz_bck.rename, "new_name")

    def test_rename_success(self):
        new_bck_name = "new_bucket"
        expected_response = "rename_op_123"
        self.ais_bck_params[QPARAM_BCK_TO] = f"{PROVIDER_AIS}/@#/{new_bck_name}/"
        mock_response = Mock()
        mock_response.text = expected_response
        self.mock_client.request.return_value = mock_response

        response = self.ais_bck.rename(new_bck_name)

        self.assertEqual(expected_response, response)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST,
            path=f"{URL_PATH_BUCKETS}/{BCK_NAME}",
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
            path=f"{URL_PATH_BUCKETS}/{BCK_NAME}",
            json=ActionMsg(action=ACT_DESTROY_BCK).dict(),
            params=self.ais_bck.qparam,
        )

    def test_delete_missing(self):
        self.mock_client.request.side_effect = ErrBckNotFound(400, "not found")
        with self.assertRaises(ErrBckNotFound):
            Bucket(client=self.mock_client, name="missing-bucket").delete()
        self.ais_bck.delete(missing_ok=True)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_DELETE,
            path=f"{URL_PATH_BUCKETS}/{BCK_NAME}",
            json=ActionMsg(action=ACT_DESTROY_BCK).dict(),
            params=self.ais_bck.qparam,
        )

    def test_evict_invalid_provider(self):
        self.assertRaises(InvalidBckProvider, self.ais_bck.evict)

    def test_evict_success(self):
        for keep_md in [True, False]:
            self.amz_bck_params[QPARAM_KEEP_REMOTE] = str(keep_md)
            self.amz_bck.evict(keep_md=keep_md)
            self.mock_client.request.assert_called_with(
                HTTP_METHOD_DELETE,
                path=f"{URL_PATH_BUCKETS}/{BCK_NAME}",
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
            path=f"{URL_PATH_BUCKETS}/{BCK_NAME}",
            params=self.ais_bck.qparam,
        )
        self.assertEqual(headers, mock_header.headers)

    def test_copy_default_params(self):
        dest_bck = Bucket(
            client=self.mock_client,
            name="test-bck",
            namespace=Namespace(uuid="namespace-id", name="ns-name"),
            provider="any-provider",
        )
        action_value = {"prefix": "", "prepend": "", "dry_run": False, "force": False}
        self._copy_exec_assert(dest_bck, action_value)

    def test_copy(self):
        prefix_filter = "existing-"
        prepend_val = "prefix-"
        dry_run = True
        force = True
        action_value = {
            "prefix": prefix_filter,
            "prepend": prepend_val,
            "dry_run": dry_run,
            "force": force,
        }

        self._copy_exec_assert(
            self.ais_bck,
            action_value,
            prefix_filter=prefix_filter,
            prepend=prepend_val,
            dry_run=dry_run,
            force=force,
        )

    def _copy_exec_assert(self, to_bck, expected_act_value, **kwargs):
        expected_response = "copy-action-id"
        mock_response = Mock()
        mock_response.text = expected_response
        self.mock_client.request.return_value = mock_response
        self.ais_bck_params[QPARAM_BCK_TO] = to_bck.get_path()
        expected_action = ActionMsg(
            action=ACT_COPY_BCK, value=expected_act_value
        ).dict()

        job_id = self.ais_bck.copy(to_bck=to_bck, **kwargs)

        self.assertEqual(expected_response, job_id)
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST,
            path=f"{URL_PATH_BUCKETS}/{BCK_NAME}",
            json=expected_action,
            params=self.ais_bck_params,
        )

    def test_list_objects(self):
        prefix = "prefix-"
        page_size = 0
        uuid = "1234"
        props = "name"
        continuation_token = "token"
        flags = [ListObjectFlag.CACHED, ListObjectFlag.DELETED]
        flag_value = "5"
        target_id = "target-node"
        expected_act_value = {
            "prefix": prefix,
            "pagesize": page_size,
            "uuid": uuid,
            "props": props,
            "continuation_token": continuation_token,
            "flags": flag_value,
            "target": target_id,
        }
        self._list_objects_exec_assert(
            expected_act_value,
            prefix=prefix,
            page_size=page_size,
            uuid=uuid,
            props=props,
            continuation_token=continuation_token,
            flags=flags,
            target=target_id,
        )

    def test_list_objects_default_params(self):
        expected_act_value = {
            "prefix": "",
            "pagesize": 0,
            "uuid": "",
            "props": "",
            "continuation_token": "",
            "flags": "0",
            "target": "",
        }
        self._list_objects_exec_assert(expected_act_value)

    def _list_objects_exec_assert(self, expected_act_value, **kwargs):
        action = ActionMsg(action=ACT_LIST, value=expected_act_value).dict()

        object_names = ["obj_name", "obj_name2"]
        bucket_entries = [BucketEntry(n=name) for name in object_names]
        mock_list = Mock(BucketList)
        mock_list.entries = bucket_entries
        self.mock_client.request_deserialize.return_value = mock_list
        result = self.ais_bck.list_objects(**kwargs)
        self.mock_client.request_deserialize.assert_called_with(
            HTTP_METHOD_GET,
            path=f"{URL_PATH_BUCKETS}/{BCK_NAME}",
            headers={HEADER_ACCEPT: MSGPACK_CONTENT_TYPE},
            res_model=BucketList,
            json=action,
            params=self.ais_bck_params,
        )
        self.assertEqual(result, mock_list)
        self.assertEqual(object_names, [entry.object.name for entry in result.entries])

    def test_list_objects_iter(self):
        self.assertIsInstance(
            self.ais_bck.list_objects_iter("prefix-", "obj props", 123), ObjectIterator
        )

    def test_list_all_objects(self):
        list_1_id = "123"
        list_1_cont = "cont"
        prefix = "prefix-"
        page_size = 5
        props = "name"
        flags = [ListObjectFlag.CACHED, ListObjectFlag.DELETED]
        flag_value = "5"
        target_id = "target-node"
        expected_act_value_1 = {
            "prefix": prefix,
            "pagesize": page_size,
            "uuid": "",
            "props": props,
            "continuation_token": "",
            "flags": flag_value,
            "target": target_id,
        }
        expected_act_value_2 = {
            "prefix": prefix,
            "pagesize": page_size,
            "uuid": list_1_id,
            "props": props,
            "continuation_token": list_1_cont,
            "flags": flag_value,
            "target": target_id,
        }
        self._list_all_objects_exec_assert(
            list_1_id,
            list_1_cont,
            expected_act_value_1,
            expected_act_value_2,
            prefix=prefix,
            page_size=page_size,
            props=props,
            flags=flags,
            target=target_id,
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
            "flags": "0",
            "target": "",
        }
        expected_act_value_2 = {
            "prefix": "",
            "pagesize": 0,
            "uuid": list_1_id,
            "props": "",
            "continuation_token": list_1_cont,
            "flags": "0",
            "target": "",
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
        entry_1 = BucketEntry(n="entry1")
        entry_2 = BucketEntry(n="entry2")
        entry_3 = BucketEntry(n="entry3")
        list_1 = BucketList(
            UUID=list_1_id, ContinuationToken=list_1_cont, Flags=0, Entries=[entry_1]
        )
        list_2 = BucketList(
            UUID="456", ContinuationToken="", Flags=0, Entries=[entry_2, entry_3]
        )

        # Test with empty list of entries
        self.mock_client.request_deserialize.return_value = BucketList(
            UUID="empty", ContinuationToken="", Flags=0
        )

        self.assertEqual([], self.ais_bck.list_all_objects(**kwargs))

        # Test with non-empty lists
        self.mock_client.request_deserialize.side_effect = [list_1, list_2]
        self.assertEqual(
            [entry_1, entry_2, entry_3], self.ais_bck.list_all_objects(**kwargs)
        )

        expected_calls = []
        for expected_val in [expected_act_value_1, expected_act_value_2]:
            expected_calls.append(
                mock.call(
                    HTTP_METHOD_GET,
                    path=f"{URL_PATH_BUCKETS}/{BCK_NAME}",
                    headers={HEADER_ACCEPT: MSGPACK_CONTENT_TYPE},
                    res_model=BucketList,
                    json=ActionMsg(action=ACT_LIST, value=expected_val).dict(),
                    params=self.ais_bck_params,
                )
            )

        for expected in expected_calls:
            self.assertIn(expected, self.mock_client.request_deserialize.call_args_list)

    def test_transform(self):
        etl_name = "etl-name"
        prepend_val = "prefix-"
        prefix_filter = "required-prefix-"
        ext = {"jpg": "txt"}
        timeout = "4m"
        force = True
        dry_run = True
        action_value = TCBckMsg(
            ext=ext,
            transform_msg=TransformBckMsg(etl_name=etl_name, timeout=timeout),
            copy_msg=CopyBckMsg(
                prefix=prefix_filter, prepend=prepend_val, force=force, dry_run=dry_run
            ),
        ).as_dict()

        self._transform_exec_assert(
            etl_name,
            action_value,
            prepend=prepend_val,
            prefix_filter=prefix_filter,
            ext=ext,
            force=force,
            dry_run=dry_run,
            timeout=timeout,
        )

    def test_transform_default_params(self):
        etl_name = "etl-name"
        action_value = {
            "id": etl_name,
            "prefix": "",
            "prepend": "",
            "force": False,
            "dry_run": False,
            "request_timeout": DEFAULT_ETL_TIMEOUT,
        }

        self._transform_exec_assert(etl_name, action_value)

    def _transform_exec_assert(self, etl_name, expected_act_value, **kwargs):
        to_bck = Bucket(name="new-bucket")
        self.ais_bck_params[QPARAM_BCK_TO] = to_bck.get_path()
        expected_action = ActionMsg(action=ACT_ETL_BCK, value=expected_act_value).dict()
        expected_response = "job-id"
        mock_response = Mock()
        mock_response.text = expected_response
        self.mock_client.request.return_value = mock_response

        result_id = self.ais_bck.transform(etl_name, to_bck, **kwargs)

        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST,
            path=f"{URL_PATH_BUCKETS}/{BCK_NAME}",
            json=expected_action,
            params=self.ais_bck_params,
        )
        self.assertEqual(expected_response, result_id)

    def test_object(self):
        new_obj = self.ais_bck.object(obj_name="name")
        self.assertEqual(self.ais_bck, new_obj.bucket)

    @patch("aistore.sdk.object.read_file_bytes")
    @patch("aistore.sdk.object.validate_file")
    @patch("aistore.sdk.bucket.validate_directory")
    @patch("pathlib.Path.glob")
    def test_put_files(
        self, mock_glob, mock_validate_dir, mock_validate_file, mock_read
    ):
        path = "directory"
        file_1_name = "file_1_name"
        file_2_name = "file_2_name"
        path_1 = Mock()
        path_1.is_file.return_value = True
        path_1.relative_to.return_value = file_1_name
        path_1.stat.return_value = Mock(st_size=123)
        path_2 = Mock()
        path_2.relative_to.return_value = file_2_name
        path_2.is_file.return_value = True
        path_2.stat.return_value = Mock(st_size=4567)
        file_1_data = b"bytes in the first file"
        file_2_data = b"bytes in the second file"
        mock_glob.return_value = [path_1, path_2]
        expected_obj_names = [file_1_name, file_2_name]
        mock_read.side_effect = [file_1_data, file_2_data]

        res = self.ais_bck.put_files(path)

        mock_validate_dir.assert_called_with(path)
        mock_validate_file.assert_has_calls([call(str(path_1)), call(str(path_2))])
        self.assertEqual(expected_obj_names, res)
        expected_calls = [
            call(
                HTTP_METHOD_PUT,
                path=f"objects/{BCK_NAME}/{file_1_name}",
                params=self.ais_bck_params,
                data=file_1_data,
            ),
            call(
                HTTP_METHOD_PUT,
                path=f"objects/{BCK_NAME}/{file_2_name}",
                params=self.ais_bck_params,
                data=file_2_data,
            ),
        ]
        self.mock_client.request.assert_has_calls(expected_calls)

    def test_get_path(self):
        namespace = Namespace(uuid="ns-id", name="ns-name")
        bucket = Bucket(name=BCK_NAME, namespace=namespace, provider=PROVIDER_AMAZON)
        expected_path = (
            f"{PROVIDER_AMAZON}/@{namespace.uuid}#{namespace.name}/{bucket.name}/"
        )
        self.assertEqual(expected_path, bucket.get_path())
        self.assertEqual(f"{PROVIDER_AIS}/@#/{bucket.name}/", self.ais_bck.get_path())

    @patch("aistore.sdk.bucket.Bucket.object")
    @patch("aistore.sdk.bucket.Bucket.list_objects_iter")
    def test_list_urls(self, mock_list_obj, mock_object):
        prefix = "my-prefix"
        etl_name = "my-etl"
        object_names = ["obj_name", "obj_name2"]
        expected_obj_calls = []
        # Should create an object reference and get url for every object returned by listing
        for name in object_names:
            expected_obj_calls.append(call(name))
            expected_obj_calls.append(call().get_url(etl_name=etl_name))
        mock_list_obj.return_value = [BucketEntry(n=name) for name in object_names]
        list(self.ais_bck.list_urls(prefix=prefix, etl_name=etl_name))
        mock_list_obj.assert_called_with(prefix=prefix, props="name")
        mock_object.assert_has_calls(expected_obj_calls)

    def test_make_request_no_client(self):
        bucket = Bucket(name="name")
        with self.assertRaises(ValueError):
            bucket.make_request("method", "action")

    def test_make_request_default_params(self):
        method = "method"
        action = "action"
        self.ais_bck.make_request(method, action)
        self.mock_client.request.assert_called_with(
            method,
            path=f"{URL_PATH_BUCKETS}/{BCK_NAME}",
            json=ActionMsg(action=action, value=None).dict(),
            params=self.ais_bck.qparam,
        )

    def test_make_request(self):
        method = "method"
        action = "action"
        value = {"request_key": "value"}
        params = {"qparamkey": "qparamval"}
        self.ais_bck.make_request(method, action, value, params)
        self.mock_client.request.assert_called_with(
            method,
            path=f"{URL_PATH_BUCKETS}/{BCK_NAME}",
            json=ActionMsg(action=action, value=value).dict(),
            params=params,
        )


if __name__ == "__main__":
    unittest.main()
