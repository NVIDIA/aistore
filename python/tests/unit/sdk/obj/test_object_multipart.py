#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#
import unittest
from unittest.mock import Mock, patch

from requests import Response
from aistore.sdk.const import (
    HTTP_METHOD_POST,
    HTTP_METHOD_DELETE,
    QPARAM_MPT_UPLOAD_ID,
    QPARAM_MPT_PART_NO,
    ACT_MPT_UPLOAD,
    ACT_MPT_COMPLETE,
    ACT_MPT_ABORT,
)
from aistore.sdk.obj.multipart_upload import MultipartUpload
from aistore.sdk.obj.object_writer import ObjectWriter
from aistore.sdk.request_client import RequestClient
from aistore.sdk.types import ActionMsg, MptCompletedPart
from aistore.sdk.obj.object import Object, BucketDetails
from aistore.sdk.provider import Provider

# Test constants
OBJECT_PATH = "objects/test-bucket/test-object"
UPLOAD_ID = "test-upload-id-12345"


class TestMultipartUpload(unittest.TestCase):
    """Comprehensive unit tests for ``aistore.sdk.obj.multipart_upload.MultipartUpload``."""

    def setUp(self) -> None:
        self.mock_client = Mock(spec=RequestClient)
        self.multipart_upload = MultipartUpload(self.mock_client, OBJECT_PATH)

    def test_init(self):
        """Test MultipartUpload initialization."""
        self.assertEqual(self.multipart_upload.client, self.mock_client)
        self.assertEqual(self.multipart_upload.object_path, OBJECT_PATH)
        self.assertIsNone(self.multipart_upload.upload_id)
        self.assertEqual(self.multipart_upload.parts, [])

    def test_create(self):
        """Test creating a multipart upload."""
        # Mock the response
        mock_response = Mock()
        mock_response.text = UPLOAD_ID
        self.mock_client.request.return_value = mock_response

        # Execute create
        result = self.multipart_upload.create()

        # Verify the request was made correctly
        self.mock_client.request.assert_called_once_with(
            HTTP_METHOD_POST,
            path=OBJECT_PATH,
            params=None,
            json=ActionMsg(action=ACT_MPT_UPLOAD).model_dump(),
        )

        # Verify the upload_id was set and self was returned
        self.assertEqual(self.multipart_upload.upload_id, UPLOAD_ID)
        self.assertIs(result, self.multipart_upload)

    @patch("aistore.sdk.obj.multipart_upload.ObjectWriter")
    def test_add_part_success(self, mock_writer_class):
        """Test adding a part to multipart upload when upload is created."""
        # Set up upload_id as if create() was called
        self.multipart_upload.upload_id = UPLOAD_ID
        part_number = 1
        mock_writer_instance = Mock(spec=ObjectWriter)
        mock_writer_class.return_value = mock_writer_instance

        # Execute add_part
        result = self.multipart_upload.add_part(part_number)

        # Verify ObjectWriter was created with correct parameters
        mock_writer_class.assert_called_once_with(
            self.mock_client,
            OBJECT_PATH,
            {QPARAM_MPT_UPLOAD_ID: UPLOAD_ID, QPARAM_MPT_PART_NO: part_number},
        )

        # Verify part number was added to parts list
        self.assertEqual(self.multipart_upload.parts, [part_number])

        # Verify ObjectWriter instance was returned
        self.assertIs(result, mock_writer_instance)

    def test_add_part_no_upload_id(self):
        """Test adding a part when multipart upload hasn't been created."""
        # upload_id is None by default
        part_number = 1

        # Execute add_part and expect ValueError
        with self.assertRaises(ValueError) as context:
            self.multipart_upload.add_part(part_number)

        self.assertEqual(str(context.exception), "Multipart upload not created")

    def test_add_part_invalid_part_number_negative(self):
        """Test adding a part with negative part number."""
        self.multipart_upload.upload_id = UPLOAD_ID

        with self.assertRaises(ValueError) as context:
            self.multipart_upload.add_part(-1)

        self.assertEqual(
            str(context.exception), "Part number must be a positive integer"
        )

        with self.assertRaises(ValueError) as context:
            self.multipart_upload.add_part(0)

        self.assertEqual(
            str(context.exception), "Part number must be a positive integer"
        )

    @patch("aistore.sdk.obj.multipart_upload.ObjectWriter")
    def test_add_multiple_parts(self, mock_writer_class):
        """Test adding multiple parts to multipart upload."""
        # Set up upload_id as if create() was called
        self.multipart_upload.upload_id = UPLOAD_ID
        mock_writer_instance = Mock(spec=ObjectWriter)
        mock_writer_class.return_value = mock_writer_instance

        # Add multiple parts
        part_numbers = [1, 2, 3]
        for part_number in part_numbers:
            result = self.multipart_upload.add_part(part_number)
            self.assertIs(result, mock_writer_instance)

        # Verify all parts were added
        self.assertEqual(self.multipart_upload.parts, part_numbers)

        # Verify ObjectWriter was called for each part
        self.assertEqual(mock_writer_class.call_count, len(part_numbers))

    def test_complete_empty_parts(self):
        """Test completing multipart upload with no parts."""
        self.multipart_upload.upload_id = UPLOAD_ID
        mock_response = Mock(spec=Response)
        self.mock_client.request.return_value = mock_response

        # Execute complete
        result = self.multipart_upload.complete()

        # Verify the request was made correctly
        expected_json = ActionMsg(
            action=ACT_MPT_COMPLETE, value=[]
        ).model_dump()  # No parts

        self.mock_client.request.assert_called_once_with(
            HTTP_METHOD_POST,
            path=OBJECT_PATH,
            params={QPARAM_MPT_UPLOAD_ID: UPLOAD_ID},
            json=expected_json,
        )

        # Verify response was returned
        self.assertIs(result, mock_response)

    def test_complete_with_parts(self):
        """Test completing multipart upload with parts."""
        self.multipart_upload.upload_id = UPLOAD_ID
        self.multipart_upload.parts = [1, 2, 3]
        mock_response = Mock(spec=Response)
        self.mock_client.request.return_value = mock_response

        # Execute complete
        result = self.multipart_upload.complete()

        # Verify the request was made correctly
        expected_parts = [
            MptCompletedPart(part_number=1, etag="").as_dict(),
            MptCompletedPart(part_number=2, etag="").as_dict(),
            MptCompletedPart(part_number=3, etag="").as_dict(),
        ]
        expected_json = ActionMsg(
            action=ACT_MPT_COMPLETE, value=expected_parts
        ).model_dump()

        self.mock_client.request.assert_called_once_with(
            HTTP_METHOD_POST,
            path=OBJECT_PATH,
            params={QPARAM_MPT_UPLOAD_ID: UPLOAD_ID},
            json=expected_json,
        )

        # Verify response was returned
        self.assertIs(result, mock_response)

    def test_complete_serialization_format(self):
        """Test that complete() serializes parts with correct key format."""
        self.multipart_upload.upload_id = UPLOAD_ID
        self.multipart_upload.parts = [1, 2]
        mock_response = Mock(spec=Response)
        self.mock_client.request.return_value = mock_response

        # Execute complete
        self.multipart_upload.complete()

        # Get the actual JSON that was sent
        call_args = self.mock_client.request.call_args
        actual_json = call_args[1]["json"]

        # Verify the structure and key names
        self.assertEqual(actual_json["action"], ACT_MPT_COMPLETE)
        self.assertEqual(len(actual_json["value"]), 2)

        # Verify each part has the correct key format
        for i, part in enumerate(actual_json["value"]):
            self.assertIn(
                "part-number", part
            )  # Should be 'part-number', not 'part_number'
            self.assertEqual(part["part-number"], i + 1)
            self.assertIn("etag", part)
            self.assertEqual(part["etag"], "")

    def test_abort(self):
        """Test aborting multipart upload."""
        self.multipart_upload.upload_id = UPLOAD_ID
        mock_response = Mock(spec=Response)
        self.mock_client.request.return_value = mock_response

        # Execute abort
        result = self.multipart_upload.abort()

        # Verify the request was made correctly
        expected_json = ActionMsg(action=ACT_MPT_ABORT).model_dump()

        self.mock_client.request.assert_called_once_with(
            HTTP_METHOD_DELETE,
            path=OBJECT_PATH,
            params={QPARAM_MPT_UPLOAD_ID: UPLOAD_ID},
            json=expected_json,
        )

        # Verify response was returned
        self.assertIs(result, mock_response)

    @patch("aistore.sdk.obj.multipart_upload.ObjectWriter")
    def test_full_workflow(self, mock_writer_class):
        """Test a complete multipart upload workflow."""
        # Mock responses
        create_response = Mock()
        create_response.text = UPLOAD_ID
        complete_response = Mock(spec=Response)
        self.mock_client.request.side_effect = [create_response, complete_response]

        mock_writer_instance = Mock(spec=ObjectWriter)
        mock_writer_class.return_value = mock_writer_instance

        # 1. Create multipart upload
        create_result = self.multipart_upload.create()
        self.assertIs(create_result, self.multipart_upload)
        self.assertEqual(self.multipart_upload.upload_id, UPLOAD_ID)

        # 2. Add parts
        writer1 = self.multipart_upload.add_part(1)
        writer2 = self.multipart_upload.add_part(2)
        self.assertIs(writer1, mock_writer_instance)
        self.assertIs(writer2, mock_writer_instance)
        self.assertEqual(self.multipart_upload.parts, [1, 2])

        # 3. Complete upload
        complete_result = self.multipart_upload.complete()
        self.assertIs(complete_result, complete_response)

        # Verify all requests were made
        self.assertEqual(self.mock_client.request.call_count, 2)

        # Verify create request
        create_call = self.mock_client.request.call_args_list[0]
        self.assertEqual(create_call[0], (HTTP_METHOD_POST,))
        self.assertEqual(create_call[1]["path"], OBJECT_PATH)
        self.assertEqual(
            create_call[1]["json"], ActionMsg(action=ACT_MPT_UPLOAD).model_dump()
        )

        # Verify complete request
        complete_call = self.mock_client.request.call_args_list[1]
        self.assertEqual(complete_call[0], (HTTP_METHOD_POST,))
        self.assertEqual(complete_call[1]["path"], OBJECT_PATH)
        self.assertEqual(complete_call[1]["params"], {QPARAM_MPT_UPLOAD_ID: UPLOAD_ID})

    def test_abort_workflow(self):
        """Test creating and then aborting a multipart upload."""
        # Mock responses
        create_response = Mock()
        create_response.text = UPLOAD_ID
        abort_response = Mock(spec=Response)
        self.mock_client.request.side_effect = [create_response, abort_response]

        # 1. Create multipart upload
        self.multipart_upload.create()

        # 2. Abort upload
        abort_result = self.multipart_upload.abort()
        self.assertIs(abort_result, abort_response)

        # Verify both requests were made
        self.assertEqual(self.mock_client.request.call_count, 2)

        # Verify abort request
        abort_call = self.mock_client.request.call_args_list[1]
        self.assertEqual(abort_call[0], (HTTP_METHOD_DELETE,))
        self.assertEqual(abort_call[1]["path"], OBJECT_PATH)
        self.assertEqual(abort_call[1]["params"], {QPARAM_MPT_UPLOAD_ID: UPLOAD_ID})
        self.assertEqual(
            abort_call[1]["json"], ActionMsg(action=ACT_MPT_ABORT).model_dump()
        )

    @patch("aistore.sdk.obj.object.MultipartUpload")
    def test_object_integration(self, mock_multipart_class):
        """Test that Object.multipart_upload() creates MultipartUpload correctly."""
        # Set up mock
        mock_multipart_instance = Mock(spec=MultipartUpload)
        mock_multipart_class.return_value = mock_multipart_instance

        # Create a mock object
        mock_client = Mock(spec=RequestClient)
        bucket_details = BucketDetails(
            "test-bucket", Provider.AIS, {}, "ais/@#/test-bucket/"
        )
        obj = Object(mock_client, bucket_details, "test-object")

        # Get multipart upload
        result = obj.multipart_upload()

        # Verify MultipartUpload was created with correct parameters
        mock_multipart_class.assert_called_once_with(
            mock_client, "objects/test-bucket/test-object", {}
        )

        # Verify the instance was returned
        self.assertIs(result, mock_multipart_instance)

    def test_mpt_completed_part_serialization(self):
        """Test that MptCompletedPart serializes correctly."""
        part = MptCompletedPart(part_number=5, etag="test-etag")

        # Test as_dict method
        result = part.as_dict()
        expected = {"part-number": 5, "etag": "test-etag"}
        self.assertEqual(result, expected)

        # Test with empty etag (default)
        part_empty = MptCompletedPart(part_number=1, etag="")
        result_empty = part_empty.as_dict()
        expected_empty = {"part-number": 1, "etag": ""}
        self.assertEqual(result_empty, expected_empty)
