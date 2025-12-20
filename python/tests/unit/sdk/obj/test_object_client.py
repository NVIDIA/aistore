"""Unit tests for ObjectClient with full coverage."""

import unittest
from unittest.mock import Mock, patch

import requests
from requests import PreparedRequest

from aistore.sdk.const import HTTP_METHOD_HEAD, HTTP_METHOD_GET, HEADER_RANGE
from aistore.sdk.request_client import RequestClient
from aistore.sdk.obj.object_client import ObjectClient
from aistore.sdk.errors import ErrObjNotFound

from tests.utils import cases


class TestObjectClient(
    unittest.TestCase
):  # pylint: disable=too-many-instance-attributes
    """Unit tests for ObjectClient."""

    def setUp(self) -> None:
        self.request_client = Mock(spec=RequestClient)
        self.path = "/test/path"
        self.params = {"param1": "val1", "param2": "val2"}
        self.headers = {"header1": "req1", "header2": "req2"}
        self.response_headers = {"attr1": "resp1", "attr2": "resp2"}
        self.direct_url = "https://node-1:1234"
        self.uname = "unique-object"
        self.object_client = ObjectClient(
            request_client=self.request_client,
            path=self.path,
            params=self.params,
            headers=self.headers,
        )

    def test_get(self):
        self.get_exec_assert(
            self.object_client,
            stream=True,
            offset=0,
            expected_headers=self.headers,
        )

    def test_get_no_headers(self):
        object_client = ObjectClient(
            request_client=self.request_client,
            path=self.path,
            params=self.params,
        )
        self.get_exec_assert(object_client, stream=True, offset=0, expected_headers={})

    @cases(
        {"byte_range_tuple": (None, None), "expected_range": "bytes=50-"},
        {"byte_range_tuple": (100, 200), "expected_range": "bytes=150-200"},
        {"byte_range_tuple": (100, None), "expected_range": "bytes=150-"},
        {"byte_range_tuple": (None, 200), "expected_range": "bytes=-150"},
    )
    def test_get_with_options(self, case):
        byte_range_tuple = case["byte_range_tuple"]
        expected_range = case["expected_range"]
        offset = 50

        object_client = ObjectClient(
            request_client=self.request_client,
            path=self.path,
            params=self.params,
            headers=self.headers,
            byte_range=byte_range_tuple,
        )

        expected_headers = self.headers.copy()
        if expected_range:
            expected_headers[HEADER_RANGE] = expected_range

        mock_response = Mock(spec=requests.Response, headers=self.response_headers)
        self.request_client.request.return_value = mock_response

        res = object_client.get(stream=False, offset=offset)

        self.assert_get(expected_stream=False, expected_headers=expected_headers)

        self.assertEqual(res, mock_response)
        mock_response.raise_for_status.assert_called_once()

        self.request_client.request.reset_mock()

    def get_exec_assert(self, object_client, stream, offset, expected_headers):
        mock_response = Mock(spec=requests.Response)
        self.request_client.request.return_value = mock_response

        res = object_client.get(stream=stream, offset=offset)

        self.assert_get(stream, expected_headers)
        self.assertEqual(res, mock_response)
        mock_response.raise_for_status.assert_called_once()

    def assert_get(self, expected_stream, expected_headers):
        self.request_client.request.assert_called_once_with(
            HTTP_METHOD_GET,
            path=self.path,
            params=self.params,
            stream=expected_stream,
            headers=expected_headers,
        )

    @patch("aistore.sdk.obj.object_client.ObjectAttributes", autospec=True)
    def test_head(self, mock_attr):
        mock_response = Mock(spec=requests.Response, headers=self.response_headers)
        self.request_client.request.return_value = mock_response

        res = self.object_client.head()

        self.assertEqual(mock_attr.return_value, res)
        self.request_client.request.assert_called_once_with(
            HTTP_METHOD_HEAD, path=self.path, params=self.params
        )
        mock_attr.assert_called_with(self.response_headers)

    @patch("aistore.sdk.obj.object_client.ObjectClient._retry_with_new_smap")
    def test_obj_not_found_retry(self, mock_retry_with_new_smap):
        """
        Test that the get method retries with a new smap when ErrObjNotFound is raised and caught.
        """
        self.request_client.request.side_effect = ErrObjNotFound(
            404, "Object not found", "object_url", Mock(PreparedRequest)
        )
        # pylint: disable=protected-access
        self.object_client._uname = (
            "test"  # will retry only if ObjectClient._uname is set
        )

        mock_retry_response = Mock(spec=requests.Response)
        mock_retry_with_new_smap.return_value = mock_retry_response

        res = self.object_client.get(stream=False)

        mock_retry_with_new_smap.assert_called_once_with(
            HTTP_METHOD_GET,
            path=self.path,
            params=self.params,
            stream=False,
            headers=self.headers,
        )

        self.assertEqual(res, mock_retry_response)

    def test_raises_err_obj_not_found(self):
        """
        Test that the get method raises ErrObjNotFound when no uname is provided.
        """
        object_client = ObjectClient(
            request_client=self.request_client,
            path=self.path,
            params=self.params,
        )
        self.request_client.request.side_effect = ErrObjNotFound(
            404, "Object not found", "object_url", Mock(PreparedRequest)
        )

        with self.assertRaises(ErrObjNotFound):
            object_client.get(stream=False)

    def _create_client_with_uname(self):
        """Helper that returns an ObjectClient configured with a unique
        object name (`uname`). It also returns the original and cloned
        `RequestClient` mocks, and the target node mock.
        """
        orig_rc = Mock(spec=RequestClient)

        # Arrange smap → target mapping
        target = Mock()
        target.public_net.direct_url = self.direct_url
        smap = Mock()
        smap.get_target_for_object.return_value = target
        orig_rc.get_smap.return_value = smap

        cloned_rc = Mock(spec=RequestClient)
        orig_rc.clone.return_value = cloned_rc

        oc = ObjectClient(
            request_client=orig_rc,
            path=self.path,
            params=self.params,
            headers=self.headers,
            uname=self.uname,
        )
        return oc, orig_rc, cloned_rc, target

    def test_constructor_selects_target_via_clone(self):
        oc, orig_rc, cloned_rc, _ = self._create_client_with_uname()
        orig_rc.get_smap.assert_called_once_with(False)
        orig_rc.clone.assert_called_once_with(base_url=self.direct_url)

        cloned_rc.request.return_value = Mock(
            spec=requests.Response, headers=self.response_headers
        )
        oc.head()
        cloned_rc.request.assert_called_once()
        orig_rc.request.assert_not_called()

    def test_get_retries_after_404(self):
        oc, _, cloned_rc, _ = self._create_client_with_uname()

        good_resp = Mock(spec=requests.Response)
        cloned_rc.request.side_effect = [
            ErrObjNotFound(404, "nf", "url", Mock(spec=PreparedRequest)),
            good_resp,
        ]

        smap2 = Mock()
        refresh_target = Mock()
        refresh_target.public_net.direct_url = "http://target-2:8081"
        smap2.get_target_for_object.return_value = refresh_target
        cloned_rc.get_smap.return_value = smap2
        cloned_rc.clone.return_value = cloned_rc

        result = oc.get(stream=False)
        self.assertEqual(cloned_rc.request.call_count, 2)
        cloned_rc.get_smap.assert_called_with(True)
        self.assertIs(result, good_resp)

    def test_get_raises_err_obj_not_found_no_uname(self):
        obj = ObjectClient(
            request_client=self.request_client,
            path=self.path,
            params=self.params,
        )
        self.request_client.request.side_effect = ErrObjNotFound(
            404,
            "Object not found",
            "object_url",
            Mock(spec=PreparedRequest),
        )
        with self.assertRaises(ErrObjNotFound):
            obj.get(stream=False)

    def test_get_network_error(self):
        self.request_client.request.side_effect = requests.ConnectionError()
        with self.assertRaises(requests.ConnectionError):
            self.object_client.get(stream=False)

    def test_get_timeout_error(self):
        self.request_client.request.side_effect = requests.Timeout()
        with self.assertRaises(requests.Timeout):
            self.object_client.get(stream=False)

    def _build_object_client_with_mocks(self):
        """Utility that sets up mocks so internal helpers can run without network."""
        orig_rc = Mock(name="orig_request_client")
        public_net = Mock(direct_url=self.direct_url)
        target_node = Mock(public_net=public_net)
        smap = Mock()
        smap.get_target_for_object.return_value = target_node
        orig_rc.get_smap.return_value = smap

        cloned_rc = Mock(name="cloned_request_client")
        orig_rc.clone.return_value = cloned_rc

        oc = ObjectClient(
            request_client=orig_rc,
            path=self.path,
            params=self.params,
            headers=self.headers,
            uname=self.uname,
        )
        return oc, orig_rc, cloned_rc, smap, self.direct_url

    def test_initialize_target_client_real(self):
        oc, orig_rc, cloned_rc, smap, _ = self._build_object_client_with_mocks()
        orig_rc.clone.assert_called_once_with(base_url=self.direct_url)
        self.assertIs(oc._request_client, cloned_rc)  # pylint: disable=protected-access
        orig_rc.get_smap.assert_called_once_with(False)
        smap.get_target_for_object.assert_called_once_with(self.uname)

    def test_retry_with_new_smap_real(self):
        oc, _, cloned_rc, _, _ = self._build_object_client_with_mocks()

        smap2 = Mock(name="forced_smap")
        smap2.get_target_for_object.return_value = Mock(
            public_net=Mock(direct_url="https://node-2:9999")
        )
        cloned_rc.get_smap.return_value = smap2
        cloned_rc.clone.return_value = cloned_rc

        expected_response = Mock(name="response")
        cloned_rc.request.return_value = expected_response

        req_kwargs = {
            "path": self.path,
            "params": self.params,
            "stream": False,
            "headers": self.headers,
        }
        result = oc._retry_with_new_smap(  # pylint: disable=protected-access
            HTTP_METHOD_GET, **req_kwargs
        )

        cloned_rc.get_smap.assert_called_with(True)
        cloned_rc.request.assert_called_once_with(HTTP_METHOD_GET, **req_kwargs)
        self.assertIs(result, expected_response)

    def test_path_property(self):
        """Path property should return the request path."""
        self.assertEqual(self.object_client.path, self.path)

    def test_get_chunk(self):
        """Test get_chunk() fetches specific byte range."""
        mock_response = Mock(spec=requests.Response)
        mock_response.content = b"chunk_data"
        self.request_client.request.return_value = mock_response

        result = self.object_client.get_chunk(100, 200)

        self.assertEqual(result, b"chunk_data")
        expected_headers = self.headers.copy()
        expected_headers[HEADER_RANGE] = "bytes=100-199"
        self.request_client.request.assert_called_once_with(
            HTTP_METHOD_GET,
            path=self.path,
            params=self.params,
            stream=False,
            headers=expected_headers,
        )
