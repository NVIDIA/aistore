from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError

# pylint: disable=unused-import,unused-variable
from aistore.sdk.const import UTF_ENCODING

from tests.integration.boto3.base_test import BaseBotoTest
from tests.utils import random_string


class BotoMultipartUploadTest(BaseBotoTest):
    """Test multipart upload operations on an AIS cluster using the boto3 client"""

    def tearDown(self) -> None:
        """Clean up multipart uploads and call parent tearDown"""
        self._cleanup_multipart_uploads()
        super().tearDown()

    def _cleanup_multipart_uploads(self):
        """Clean up all ongoing multipart uploads"""
        try:
            buckets = self.client.list_buckets().get("Buckets", [])
            for bucket in buckets:
                try:
                    uploads = self.client.list_multipart_uploads(
                        Bucket=bucket["Name"]
                    ).get("Uploads", [])
                    for upload in uploads:
                        self.client.abort_multipart_upload(
                            Bucket=bucket["Name"],
                            Key=upload["Key"],
                            UploadId=upload["UploadId"],
                        )
                except ClientError:
                    pass

        except Exception:
            # Ignore cleanup errors
            pass

    def test_basic_multipart_upload(self):
        """Test basic multipart upload flow"""
        key = "basic-multipart-object"
        data_len = 1000
        num_parts = 4
        chunk_size = int(data_len / num_parts)
        data = random_string(data_len)
        parts = [data[i * chunk_size : (i + 1) * chunk_size] for i in range(num_parts)]

        bucket_name = self.create_bucket()
        response = self.client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = response.get("UploadId")
        self.assertIsNotNone(upload_id)

        # Upload parts sequentially
        uploaded_parts = []
        for part_num, part_data in enumerate(parts, 1):
            response = self.client.upload_part(
                Body=part_data,
                Bucket=bucket_name,
                Key=key,
                PartNumber=part_num,
                UploadId=upload_id,
            )
            uploaded_parts.append({"PartNumber": part_num, "ETag": response["ETag"]})

        # Complete multipart upload
        self.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": uploaded_parts},
        )

        # Verify uploaded data
        response = self.client.get_object(Bucket=bucket_name, Key=key)
        uploaded_data = response["Body"].read().decode(UTF_ENCODING)
        self.assertEqual(data, uploaded_data)

    # pylint: disable=too-many-locals
    def test_concurrent_part_uploads(self):
        """Test uploading parts concurrently"""
        key = "concurrent-multipart-object"
        data_len = 2000
        num_parts = 8
        chunk_size = int(data_len / num_parts)
        data = random_string(data_len)
        parts = [data[i * chunk_size : (i + 1) * chunk_size] for i in range(num_parts)]

        bucket_name = self.create_bucket()
        response = self.client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = response.get("UploadId")

        def upload_part(part_num, part_data):
            """Upload a single part"""
            return self.client.upload_part(
                Body=part_data,
                Bucket=bucket_name,
                Key=key,
                PartNumber=part_num,
                UploadId=upload_id,
            )

        # Upload parts concurrently
        uploaded_parts = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_part = {
                executor.submit(upload_part, i + 1, part_data): i + 1
                for i, part_data in enumerate(parts)
            }

            for future in as_completed(future_to_part):
                part_num = future_to_part[future]
                response = future.result()
                uploaded_parts.append(
                    {"PartNumber": part_num, "ETag": response["ETag"]}
                )

        # Sort parts by part number for completion
        uploaded_parts.sort(key=lambda x: x["PartNumber"])

        # Complete multipart upload
        self.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": uploaded_parts},
        )

        # Verify uploaded data
        response = self.client.get_object(Bucket=bucket_name, Key=key)
        uploaded_data = response["Body"].read().decode(UTF_ENCODING)
        self.assertEqual(data, uploaded_data)

    def test_abort_multipart_upload(self):
        """Test aborting a multipart upload"""
        key = "aborted-multipart-object"
        bucket_name = self.create_bucket()

        # Create multipart upload
        response = self.client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = response.get("UploadId")

        # Upload some parts
        self.client.upload_part(
            Body="part1data",
            Bucket=bucket_name,
            Key=key,
            PartNumber=1,
            UploadId=upload_id,
        )

        self.client.upload_part(
            Body="part2data",
            Bucket=bucket_name,
            Key=key,
            PartNumber=2,
            UploadId=upload_id,
        )

        # Abort the upload
        self.client.abort_multipart_upload(
            Bucket=bucket_name, Key=key, UploadId=upload_id
        )

        # Verify object doesn't exist
        with self.assertRaises(ClientError) as ctx:
            self.client.get_object(Bucket=bucket_name, Key=key)
        # AIS returns "ErrNotFound" instead of standard S3 "NoSuchKey"
        error_code = ctx.exception.response["Error"]["Code"]
        self.assertIn(error_code, ["NoSuchKey", "ErrNotFound"])

    def test_large_number_of_parts(self):
        """Test multipart upload with many parts (100 parts)"""
        key = "many-parts-object"
        num_parts = 100
        part_size = 50  # 50 characters per part
        data = "".join(
            [f"part{i:03d}:" + random_string(part_size - 8) for i in range(num_parts)]
        )
        parts = [data[i * part_size : (i + 1) * part_size] for i in range(num_parts)]

        bucket_name = self.create_bucket()
        response = self.client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = response.get("UploadId")

        # Upload all parts
        uploaded_parts = []
        for part_num, part_data in enumerate(parts, 1):
            response = self.client.upload_part(
                Body=part_data,
                Bucket=bucket_name,
                Key=key,
                PartNumber=part_num,
                UploadId=upload_id,
            )
            uploaded_parts.append({"PartNumber": part_num, "ETag": response["ETag"]})

        # Complete upload
        self.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": uploaded_parts},
        )

        # Verify data integrity
        response = self.client.get_object(Bucket=bucket_name, Key=key)
        uploaded_data = response["Body"].read().decode(UTF_ENCODING)
        self.assertEqual(data, uploaded_data)

    def test_list_multipart_uploads(self):
        """Test listing multipart uploads"""
        bucket_name = self.create_bucket()

        # Create multiple multipart uploads
        uploads = []
        for i in range(3):
            key = f"upload-{i}"
            response = self.client.create_multipart_upload(Bucket=bucket_name, Key=key)
            uploads.append({"Key": key, "UploadId": response["UploadId"]})

        # List multipart uploads
        response = self.client.list_multipart_uploads(Bucket=bucket_name)
        listed_uploads = response.get("Uploads", [])

        # Verify all uploads are listed
        self.assertEqual(len(listed_uploads), 3)
        listed_keys = {upload["Key"] for upload in listed_uploads}
        expected_keys = {upload["Key"] for upload in uploads}
        self.assertEqual(listed_keys, expected_keys)

        # Clean up uploads
        for upload in uploads:
            self.client.abort_multipart_upload(
                Bucket=bucket_name, Key=upload["Key"], UploadId=upload["UploadId"]
            )

    def test_list_parts(self):
        """Test listing parts of a multipart upload"""
        key = "list-parts-object"
        bucket_name = self.create_bucket()

        response = self.client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = response.get("UploadId")

        # Upload some parts
        num_parts = 5
        for part_num in range(1, num_parts + 1):
            self.client.upload_part(
                Body=f"data for part {part_num}",
                Bucket=bucket_name,
                Key=key,
                PartNumber=part_num,
                UploadId=upload_id,
            )

        # List parts
        response = self.client.list_parts(
            Bucket=bucket_name, Key=key, UploadId=upload_id
        )

        parts = response.get("Parts", [])
        self.assertEqual(len(parts), num_parts)

        # Verify part numbers
        part_numbers = {part["PartNumber"] for part in parts}
        expected_numbers = set(range(1, num_parts + 1))
        self.assertEqual(part_numbers, expected_numbers)

        # Clean up
        self.client.abort_multipart_upload(
            Bucket=bucket_name, Key=key, UploadId=upload_id
        )

    def test_out_of_order_parts(self):
        """Test uploading parts out of order"""
        key = "out-of-order-object"
        data = "abcdefghijklmnopqrstuvwxyz"
        # Split into parts: "abcdef", "ghijkl", "mnopqr", "stuvwx", "yz"
        parts = [data[i : i + 6] for i in range(0, len(data), 6)]

        bucket_name = self.create_bucket()
        response = self.client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = response.get("UploadId")

        # Upload parts in reverse order
        uploaded_parts = []
        for i in range(len(parts) - 1, -1, -1):  # 4, 3, 2, 1, 0
            part_num = i + 1  # 5, 4, 3, 2, 1
            response = self.client.upload_part(
                Body=parts[i],
                Bucket=bucket_name,
                Key=key,
                PartNumber=part_num,
                UploadId=upload_id,
            )
            uploaded_parts.append({"PartNumber": part_num, "ETag": response["ETag"]})

        # Sort parts for completion
        uploaded_parts.sort(key=lambda x: x["PartNumber"])

        # Complete upload
        self.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": uploaded_parts},
        )

        # Verify data is reassembled correctly
        response = self.client.get_object(Bucket=bucket_name, Key=key)
        uploaded_data = response["Body"].read().decode(UTF_ENCODING)
        self.assertEqual(data, uploaded_data)

    def test_invalid_upload_id(self):
        """Test operations with invalid upload ID"""
        key = "invalid-upload-object"
        bucket_name = self.create_bucket()
        fake_upload_id = "invalid-upload-id-12345"

        # Try to upload part with invalid ID
        with self.assertRaises(ClientError) as ctx:
            self.client.upload_part(
                Body="test data",
                Bucket=bucket_name,
                Key=key,
                PartNumber=1,
                UploadId=fake_upload_id,
            )
        # Check for AIS-specific error codes
        error_str = str(ctx.exception)
        self.assertTrue(
            "NoSuchUpload" in error_str
            or "ErrNotFound" in error_str
            or "invalid" in error_str.lower()
        )

        # Try to complete with invalid ID
        with self.assertRaises(ClientError) as ctx:
            self.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=fake_upload_id,
                MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": "fake-etag"}]},
            )

    def test_complete_with_missing_parts(self):
        """Test completing upload with missing parts"""
        key = "missing-parts-object"
        bucket_name = self.create_bucket()

        response = self.client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = response.get("UploadId")

        # Upload only part 1
        response = self.client.upload_part(
            Body="part 1 data",
            Bucket=bucket_name,
            Key=key,
            PartNumber=1,
            UploadId=upload_id,
        )

        # Try to complete with parts 1 and 2 (part 2 missing)
        with self.assertRaises(ClientError) as ctx:
            self.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={
                    "Parts": [
                        {"PartNumber": 1, "ETag": response["ETag"]},
                        {"PartNumber": 2, "ETag": "fake-etag"},
                    ]
                },
            )

        # Clean up
        self.client.abort_multipart_upload(
            Bucket=bucket_name, Key=key, UploadId=upload_id
        )

    def test_single_part_multipart_upload(self):
        """Test multipart upload with only one part"""
        key = "single-part-object"
        data = random_string(500)
        bucket_name = self.create_bucket()

        response = self.client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = response.get("UploadId")

        # Upload single part
        response = self.client.upload_part(
            Body=data,
            Bucket=bucket_name,
            Key=key,
            PartNumber=1,
            UploadId=upload_id,
        )

        # Complete upload
        self.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": response["ETag"]}]},
        )

        # Verify data
        response = self.client.get_object(Bucket=bucket_name, Key=key)
        uploaded_data = response["Body"].read().decode(UTF_ENCODING)
        self.assertEqual(data, uploaded_data)

    def test_concurrent_multipart_uploads(self):
        """Test multiple concurrent multipart uploads to same bucket"""
        bucket_name = self.create_bucket()
        num_uploads = 3

        def perform_multipart_upload(upload_index):
            """Perform a complete multipart upload"""
            key = f"concurrent-upload-{upload_index}"
            data = random_string(200)
            parts = [data[i : i + 50] for i in range(0, len(data), 50)]

            response = self.client.create_multipart_upload(Bucket=bucket_name, Key=key)
            upload_id = response.get("UploadId")

            uploaded_parts = []
            for part_num, part_data in enumerate(parts, 1):
                response = self.client.upload_part(
                    Body=part_data,
                    Bucket=bucket_name,
                    Key=key,
                    PartNumber=part_num,
                    UploadId=upload_id,
                )
                uploaded_parts.append(
                    {"PartNumber": part_num, "ETag": response["ETag"]}
                )

            self.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": uploaded_parts},
            )

            return key, data

        # Perform concurrent uploads
        results = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(perform_multipart_upload, i) for i in range(num_uploads)
            ]

            for future in as_completed(futures):
                results.append(future.result())

        # Verify all uploads completed successfully
        self.assertEqual(len(results), num_uploads)

        # Verify data integrity for all uploads
        for key, expected_data in results:
            response = self.client.get_object(Bucket=bucket_name, Key=key)
            uploaded_data = response["Body"].read().decode(UTF_ENCODING)
            self.assertEqual(expected_data, uploaded_data)
