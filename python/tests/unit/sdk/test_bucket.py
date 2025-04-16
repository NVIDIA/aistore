import unittest
from unittest.mock import Mock, call, patch, MagicMock, mock_open
from urllib.parse import quote

from requests.structures import CaseInsensitiveDict

from aistore.sdk.ais_source import AISSource
from aistore.sdk.bucket import Bucket, Header
from aistore.sdk.obj.object import Object
from aistore.sdk.etl.etl_const import DEFAULT_ETL_TIMEOUT
from aistore.sdk.obj.object_iterator import ObjectIterator
from aistore.sdk import ListObjectFlag
from aistore.sdk.etl import ETLConfig

from aistore.sdk.const import (
    ACT_COPY_BCK,
    ACT_CREATE_BCK,
    ACT_DESTROY_BCK,
    ACT_ETL_BCK,
    ACT_EVICT_REMOTE_BCK,
    ACT_LIST,
    ACT_MOVE_BCK,
    ACT_SUMMARY_BCK,
    QPARAM_BCK_TO,
    QPARAM_NAMESPACE,
    QPARAM_PROVIDER,
    QPARAM_KEEP_REMOTE,
    QPARAM_BSUMM_REMOTE,
    QPARAM_FLT_PRESENCE,
    QPARAM_UUID,
    HTTP_METHOD_DELETE,
    HTTP_METHOD_GET,
    HTTP_METHOD_HEAD,
    HTTP_METHOD_PUT,
    HTTP_METHOD_POST,
    URL_PATH_BUCKETS,
    HEADER_ACCEPT,
    HEADER_BUCKET_PROPS,
    HEADER_XACTION_ID,
    HEADER_BUCKET_SUMM,
    MSGPACK_CONTENT_TYPE,
    STATUS_ACCEPTED,
    STATUS_BAD_REQUEST,
    STATUS_OK,
    AIS_BCK_NAME,
    AIS_OBJ_NAME,
    AIS_MIRROR_PATHS,
    AIS_PRESENT,
    AIS_BCK_PROVIDER,
    AIS_LOCATION,
    AIS_MIRROR_COPIES,
)

from aistore.sdk.dataset.dataset_config import DatasetConfig
from aistore.sdk.errors import (
    InvalidBckProvider,
    ErrBckAlreadyExists,
    ErrBckNotFound,
    UnexpectedHTTPStatusCode,
)
from aistore.sdk.obj.object_props import ObjectProps
from aistore.sdk.request_client import RequestClient
from aistore.sdk.types import (
    ActionMsg,
    BucketList,
    BucketEntry,
    BsummCtrlMsg,
    Namespace,
    TCBckMsg,
    TransformBckMsg,
    CopyBckMsg,
)
from aistore.sdk.enums import FLTPresence
from aistore.sdk.provider import Provider
from tests.const import ETL_NAME, PREFIX_NAME
from tests.utils import cases

BCK_NAME = "bucket_name"


# pylint: disable=too-many-public-methods,unused-variable
class TestBucket(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_client = Mock(RequestClient)
        self.amz_bck = Bucket(
            name=BCK_NAME, client=self.mock_client, provider=Provider.AMAZON
        )
        self.amz_bck_params = self.amz_bck.qparam.copy()
        self.ais_bck = Bucket(name=BCK_NAME, client=self.mock_client)
        self.ais_bck_params = self.ais_bck.qparam.copy()
        self.dataset_config = MagicMock(spec=DatasetConfig)

    def test_default_props(self):
        bucket = Bucket(name=BCK_NAME, client=self.mock_client)
        self.assertEqual({QPARAM_PROVIDER: Provider.AIS.value}, bucket.qparam)
        self.assertEqual(Provider.AIS, bucket.provider)
        self.assertIsNone(bucket.namespace)

    def test_properties(self):
        self.assertEqual(self.mock_client, self.ais_bck.client)
        expected_ns = Namespace(uuid="ns-id", name="ns-name")
        client = RequestClient("test client name", session_manager=Mock())
        bck = Bucket(
            client=client,
            name=BCK_NAME,
            provider=Provider.AMAZON,
            namespace=expected_ns,
        )
        self.assertEqual(client, bck.client)
        self.assertEqual(Provider.AMAZON, bck.provider)
        self.assertEqual(
            {
                QPARAM_PROVIDER: Provider.AMAZON.value,
                QPARAM_NAMESPACE: expected_ns.get_path(),
            },
            bck.qparam,
        )
        self.assertEqual(BCK_NAME, bck.name)
        self.assertEqual(expected_ns, bck.namespace)

    @cases(("gs", Provider.GOOGLE), ("s3", Provider.AMAZON))
    def test_init_mapped_provider(self, test_case):
        alias, provider = test_case
        bck = Bucket(
            client=self.mock_client,
            name="test-bck",
            provider=alias,
        )
        self.assertEqual(provider, bck.provider)

    def test_ais_source(self):
        self.assertIsInstance(self.ais_bck, AISSource)

    def test_create_invalid_provider(self):
        self.assertRaises(InvalidBckProvider, self.amz_bck.create)

    def _assert_bucket_created(self, bck):
        # Ensure that the last request was called with create args
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
        already_exists_err = ErrBckAlreadyExists(400, "message", "bck_create_url")
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
        self.ais_bck_params[QPARAM_BCK_TO] = f"{Provider.AIS.value}/@#/{new_bck_name}/"
        mock_response = Mock()
        mock_response.text = expected_response
        self.mock_client.request.return_value = mock_response

        response = self.ais_bck.rename(new_bck_name)

        self.assertEqual(expected_response, response)
        # Ensure that last request was called to with rename args
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
        # Ensure that last request was called with delete args
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_DELETE,
            path=f"{URL_PATH_BUCKETS}/{BCK_NAME}",
            json=ActionMsg(action=ACT_DESTROY_BCK).dict(),
            params=self.ais_bck.qparam,
        )

    def test_delete_missing(self):
        self.mock_client.request.side_effect = ErrBckNotFound(
            400, "not found", "bck_delete_url"
        )
        with self.assertRaises(ErrBckNotFound):
            Bucket(client=self.mock_client, name="missing-bucket").delete()
        self.ais_bck.delete(missing_ok=True)

        # Ensure that last request was called with delete args
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

            # Ensure that last request was a delete and other args
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
        # Ensure that the last request was called with right args
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_HEAD,
            path=f"{URL_PATH_BUCKETS}/{BCK_NAME}",
            params=self.ais_bck.qparam,
        )
        self.assertEqual(headers, mock_header.headers)

    @cases(*Provider)
    def test_copy_default_params(self, provider):
        dest_bck = Bucket(
            client=self.mock_client,
            name="test-bck",
            namespace=Namespace(uuid="namespace-id", name="ns-name"),
            provider=provider,
        )
        action_value = {
            "prefix": "",
            "prepend": "",
            "dry_run": False,
            "force": False,
            "latest-ver": False,
            "synchronize": False,
        }
        self._copy_exec_assert(dest_bck, action_value)

    def test_copy(self):
        prefix_filter = "existing-"
        prepend_val = PREFIX_NAME
        dry_run = True
        force = True
        latest = False
        sync = False
        ext = {"jpg": "txt"}
        num_workers = 0
        action_value = TCBckMsg(
            ext=ext,
            num_workers=num_workers,
            copy_msg=CopyBckMsg(
                prefix=prefix_filter,
                prepend=prepend_val,
                force=force,
                dry_run=dry_run,
                latest=latest,
                sync=sync,
            ),
        ).as_dict()

        self._copy_exec_assert(
            self.ais_bck,
            action_value,
            prefix_filter=prefix_filter,
            prepend=prepend_val,
            dry_run=dry_run,
            force=force,
            ext=ext,
            num_workers=num_workers,
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
        # Ensure that last request was called with right args
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST,
            path=f"{URL_PATH_BUCKETS}/{BCK_NAME}",
            json=expected_action,
            params=self.ais_bck_params,
        )

    def test_list_objects(self):
        prefix = PREFIX_NAME
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

        # Ensure that the objects retrieved are the same as expected
        self.assertEqual(result, mock_list)

        # Ensure that the object names are the same as names for each object entry
        self.assertEqual(object_names, [entry.object.name for entry in result.entries])

    def test_list_objects_iter(self):
        # Ensure that iterator returned is correct type
        self.assertIsInstance(
            self.ais_bck.list_objects_iter(PREFIX_NAME, "obj props", 123),
            ObjectIterator,
        )

    def test_list_all_objects(self):
        list_1_id = "123"
        list_1_cont = "cont"
        prefix = PREFIX_NAME
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

        # Test client get calls match expected calls
        expected_calls = []
        for expected_val in [expected_act_value_1, expected_act_value_2]:
            expected_calls.append(
                call(
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
        prepend_val = PREFIX_NAME
        prefix_filter = "required-prefix-"
        ext = {"jpg": "txt"}
        timeout = "4m"
        force = True
        dry_run = True
        num_workers = 0
        # Ensure that request has been made with specified arguments
        action_value = TCBckMsg(
            ext=ext,
            num_workers=num_workers,
            transform_msg=TransformBckMsg(etl_name=ETL_NAME, timeout=timeout),
            copy_msg=CopyBckMsg(
                prefix=prefix_filter,
                prepend=prepend_val,
                force=force,
                dry_run=dry_run,
                latest=False,
                sync=False,
            ),
        ).as_dict()

        self._transform_exec_assert(
            ETL_NAME,
            action_value,
            prepend=prepend_val,
            prefix_filter=prefix_filter,
            ext=ext,
            num_workers=num_workers,
            force=force,
            dry_run=dry_run,
            timeout=timeout,
        )

    def test_transform_default_params(self):
        action_value = {
            "id": ETL_NAME,
            "prefix": "",
            "prepend": "",
            "force": False,
            "dry_run": False,
            "request_timeout": DEFAULT_ETL_TIMEOUT,
            "latest-ver": False,
            "synchronize": False,
        }

        self._transform_exec_assert(ETL_NAME, action_value)

    def _transform_exec_assert(self, etl_name, expected_act_value, **kwargs):
        to_bck = Bucket(name="new-bucket")
        self.ais_bck_params[QPARAM_BCK_TO] = to_bck.get_path()
        expected_action = ActionMsg(action=ACT_ETL_BCK, value=expected_act_value).dict()
        expected_response = "job-id"
        mock_response = Mock()
        mock_response.text = expected_response
        self.mock_client.request.return_value = mock_response

        result_id = self.ais_bck.transform(etl_name, to_bck, **kwargs)

        # Ensure that request inside transform was given correct args
        self.mock_client.request.assert_called_with(
            HTTP_METHOD_POST,
            path=f"{URL_PATH_BUCKETS}/{BCK_NAME}",
            json=expected_action,
            params=self.ais_bck_params,
        )
        self.assertEqual(expected_response, result_id)

    def test_object(self):
        obj_name = "test object!#?"
        props_dict = CaseInsensitiveDict(
            {
                AIS_BCK_NAME: "test-bck-name",
                AIS_BCK_PROVIDER: "ais",
                AIS_OBJ_NAME: obj_name,
                AIS_LOCATION: "/sda/test-location",
                AIS_MIRROR_PATHS: "path1,path2",
                AIS_MIRROR_COPIES: "2",
                AIS_PRESENT: "true",
            }
        )
        props = ObjectProps(props_dict)

        new_obj = self.ais_bck.object(obj_name=obj_name, props=props)
        self.assertEqual(self.ais_bck.name, new_obj.bucket_name)
        self.assertEqual(self.ais_bck.provider, new_obj.bucket_provider)
        self.assertEqual(self.ais_bck.qparam, new_obj.query_params)
        # pylint: disable=protected-access
        self.assertEqual(f"{new_obj._bck_path}/{quote(obj_name)}", new_obj._object_path)

        self.assertEqual(props, new_obj.props_cached)

        # Mock response with a headers attribute
        mock_response = Mock()
        mock_response.headers = props_dict

        # Set mock return value for HEAD request
        self.mock_client.request.return_value = mock_response
        self.assertEqual(props.present, new_obj.props.present)
        self.assertEqual(props.access_time, new_obj.props.access_time)
        self.assertEqual(props.location, new_obj.props.location)
        self.assertEqual(props.bucket_name, new_obj.props.bucket_name)
        self.assertEqual(props.bucket_provider, new_obj.props.bucket_provider)
        self.assertEqual(props.mirror_copies, new_obj.props.mirror_copies)
        self.assertEqual(props.mirror_paths, new_obj.props.mirror_paths)

    @patch("aistore.sdk.obj.object_writer.validate_file")
    @patch("aistore.sdk.bucket.validate_directory")
    @patch("pathlib.Path.glob")
    def test_put_files(self, mock_glob, mock_validate_dir, mock_validate_file):
        path = "directory"
        file_names = ["file_1_name", "file_2_name"]
        file_sizes = [123, 4567]

        file_readers = [
            mock_open(read_data=b"bytes in the first file").return_value,
            mock_open(read_data=b"bytes in the second file").return_value,
        ]
        mock_file = mock_open()
        mock_file.side_effect = file_readers

        # Set up mock files
        mock_files = [
            Mock(
                is_file=Mock(return_value=True),
                relative_to=Mock(return_value=name),
                stat=Mock(return_value=Mock(st_size=size)),
            )
            for name, size in zip(file_names, file_sizes)
        ]
        mock_glob.return_value = mock_files

        with patch("builtins.open", mock_file):
            res = self.ais_bck.put_files(path)

        # Ensure that put_files is called for files for the directory at path
        mock_validate_dir.assert_called_with(path)
        # Ensure that files have been created with the proper path
        mock_validate_file.assert_has_calls([call(str(f)) for f in mock_files])
        # Ensure that the files put in the bucket have the sane names
        self.assertEqual(file_names, res)
        expected_calls = []

        for file_name, file_reader in zip(file_names, file_readers):
            expected_calls.append(
                call(
                    HTTP_METHOD_PUT,
                    path=f"objects/{BCK_NAME}/{file_name}",
                    params=self.ais_bck_params,
                    data=file_reader,
                )
            )

        # Ensure that the file data and metadata is the same
        self.mock_client.request.assert_has_calls(expected_calls)

    def test_get_path(self):
        namespace = Namespace(uuid="ns-id", name="ns-name")
        bucket = Bucket(name=BCK_NAME, namespace=namespace, provider=Provider.AMAZON)
        expected_path = (
            f"{Provider.AMAZON.value}/@{namespace.uuid}#{namespace.name}/{bucket.name}/"
        )
        self.assertEqual(expected_path, bucket.get_path())
        self.assertEqual(
            f"{Provider.AIS.value}/@#/{bucket.name}/", self.ais_bck.get_path()
        )

    @patch("aistore.sdk.bucket.Bucket.object")
    @patch("aistore.sdk.bucket.Bucket.list_objects_iter")
    def test_list_urls(self, mock_list_obj, mock_object):
        prefix = "my-prefix"
        object_names = ["obj_name", "obj_name2"]
        expected_obj_calls = []
        # Should create an object reference and get url for every object returned by listing
        for name in object_names:
            expected_obj_calls.append(call(name))
            expected_obj_calls.append(call().get_url(etl=ETLConfig(name=ETL_NAME)))
        mock_list_obj.return_value = [BucketEntry(n=name) for name in object_names]
        list(self.ais_bck.list_urls(prefix=prefix, etl=ETLConfig(name=ETL_NAME)))
        mock_list_obj.assert_called_with(prefix=prefix, props="name")
        mock_object.assert_has_calls(expected_obj_calls)

    @patch("aistore.sdk.bucket.Bucket.list_objects_iter")
    def test_list_all_objects_iter(self, mock_list_obj):
        object_names = ["obj_name", "obj_name2"]
        mock_list_obj.return_value = [BucketEntry(n=name) for name in object_names]
        objects_iter = self.ais_bck.list_all_objects_iter()
        # Ensure that every object is in created iterator
        for obj in objects_iter:
            self.assertIsInstance(obj, Object)

    def test_make_request_no_client(self):
        bucket = Bucket(name="name")
        with self.assertRaises(ValueError):
            bucket.make_request("method", "action")

    def test_make_request_default_params(self):
        method = "method"
        action = "action"
        self.ais_bck.make_request(method, action)
        # Ensure that last call has been made with default arguments
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
        # Ensure that last call has been made with specified arguments
        self.mock_client.request.assert_called_with(
            method,
            path=f"{URL_PATH_BUCKETS}/{BCK_NAME}",
            json=ActionMsg(action=action, value=value).dict(),
            params=params,
        )

    def test_summary(self):
        # Mock responses for request calls
        response1 = Mock()
        response1.status_code = STATUS_ACCEPTED
        response1.text = '"job_id"'

        response2 = Mock()
        response2.status_code = STATUS_OK
        response2.content = (
            b'[{"name":"temporary","provider":"ais","namespace":{"uuid":"","name":""},'
            b'"ObjCount":{"obj_count_present":"137160","obj_count_remote":"0"},'
            b'"ObjSize":{"obj_min_size":1024,"obj_avg_size":1024,"obj_max_size":1024},'
            b'"TotalSize":{"size_on_disk":"148832256","size_all_present_objs":"140451840",'
            b'"size_all_remote_objs":"0","total_disks_size":"4955520307200"},'
            b'"used_pct":0,"is_present":false}]\n'
        )

        # Set the side_effect of the request method to return the two responses in sequence
        self.mock_client.request.side_effect = [response1, response2]

        # Call the summary method
        result = self.ais_bck.summary()

        # Ensure that request was called with the correct sequence of calls
        bsumm_ctrl_msg = BsummCtrlMsg(
            uuid="", prefix="", fast=True, cached=True, present=True
        ).dict()
        bsumm_ctrl_msg_with_uuid = BsummCtrlMsg(
            uuid="job_id", prefix="", fast=True, cached=True, present=True
        ).dict()

        calls = []

        for msg in [bsumm_ctrl_msg, bsumm_ctrl_msg_with_uuid]:
            calls.append(
                call(
                    HTTP_METHOD_GET,
                    path="buckets/bucket_name",
                    json={
                        "action": ACT_SUMMARY_BCK,
                        "name": "",
                        "value": msg,
                    },
                    params=self.ais_bck.qparam,
                ),
            )

        self.mock_client.request.assert_has_calls(calls)

        # Assert that the result has the expected structure
        self.assertIsInstance(result, dict)
        self.assertIn("name", result)
        self.assertIn("provider", result)
        self.assertIn("ObjCount", result)
        self.assertIn("ObjSize", result)
        self.assertIn("TotalSize", result)

    def test_summary_error_handling(self):
        # Mock responses for the first and second request call
        first_response = Mock()
        first_response.status_code = STATUS_ACCEPTED

        second_response = Mock()
        second_response.status_code = STATUS_BAD_REQUEST
        second_response.text = '"job_id"'

        # Set the side_effect of the request method to return the correct mock response
        self.mock_client.request.side_effect = [first_response, second_response]

        # Call the summary method and expect an UnexpectedHTTPStatusCode exception
        with self.assertRaises(UnexpectedHTTPStatusCode):
            self.ais_bck.summary()

        # Verify that the request method was called twice
        assert self.mock_client.request.call_count == 2

    def test_info(self):
        # Mock responses for request calls
        response1 = Mock()
        response1.status_code = STATUS_ACCEPTED
        response1.headers = {
            HEADER_BUCKET_PROPS: '{"some": "props"}',
            HEADER_XACTION_ID: "some-id",
        }

        response2 = Mock()
        response2.status_code = STATUS_OK
        response2.headers = {
            HEADER_BUCKET_SUMM: '{"some": "summary"}',
        }

        # Set the side_effect of the request method to return the two responses in sequence
        self.mock_client.request.side_effect = [response1, response2]

        # Call the info method
        bucket_props, bucket_summ = self.ais_bck.info()

        expected_call = call(
            HTTP_METHOD_HEAD,
            path=f"{URL_PATH_BUCKETS}/{self.ais_bck.name}",
            params={
                **self.ais_bck.qparam,
                QPARAM_FLT_PRESENCE: FLTPresence.FLT_EXISTS,
                QPARAM_BSUMM_REMOTE: True,
                QPARAM_UUID: "some-id",
            },
        )

        # Ensure two calls were made matching the expected call
        self.mock_client.request.assert_has_calls([expected_call, expected_call])

        # Check the return values
        self.assertEqual(bucket_props, '{"some": "props"}')
        self.assertEqual(bucket_summ, {"some": "summary"})

        # Test with invalid flt_presence
        with self.assertRaises(ValueError):
            self.ais_bck.info(flt_presence=6)

    def test_write_dataset_skip_missing_true_but_missing_attributes(self):
        self.dataset_config.write_shards = MagicMock()

        self.ais_bck.write_dataset(
            self.dataset_config, log_dir="/fake/log", skip_missing=True
        )

        self.dataset_config.write_shards.assert_called()
        _, kwargs = self.dataset_config.write_shards.call_args
        self.assertTrue(callable(kwargs["post"]))

    def test_write_dataset_successful(self):
        self.dataset_config.write_shards = MagicMock()

        self.ais_bck.write_dataset(self.dataset_config, skip_missing=True)
        self.dataset_config.write_shards.assert_called()
        _, kwargs = self.dataset_config.write_shards.call_args
        self.assertTrue(callable(kwargs["post"]))
