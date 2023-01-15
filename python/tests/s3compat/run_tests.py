#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#

import os
import minio_helpers
import tests as minio_test

# pylint: disable=unused-variable,undefined-variable
run_all = os.getenv("S3_COMPAT_RUN_ALL", "True").lower() in ("true", "t", 1)
strict = os.getenv("S3_COMPAT_STRICT", "False").lower() in ("true", "t", 1)
access_key = os.getenv("ACCESS_KEY", "testing")
secret_key = os.getenv("SECRET_KEY", "testing")
server_endpoint = os.getenv("AIS_ENDPOINT", "localhost:8080")
test_list = os.getenv("S3_COMPAT_TEST_LIST")

minio_helpers.init_tests(server_endpoint, access_key, secret_key)

# Always include verified tests
tests = [
    minio_test.test_make_bucket_default_region,
    minio_test.test_get_object,
    minio_test.test_get_object_version,
    minio_test.test_presigned_post_policy,
    minio_test.test_get_bucket_notification,
    minio_test.test_remove_object,
    minio_test.test_remove_object_version,
    minio_test.test_remove_bucket,
    minio_test.test_make_bucket_with_region,
    minio_test.test_negative_make_bucket_invalid_name,
    minio_test.test_fput_object_small_file,
    minio_test.test_fput_object_large_file,
    minio_test.test_fput_object_with_content_type,
    minio_test.test_copy_object_etag_match,
    minio_test.test_copy_object_negative_etag_match,
    minio_test.test_copy_object_modified_since,
    minio_test.test_copy_object_unmodified_since,
    minio_test.test_negative_put_object_with_path_segment,
    minio_test.test_fget_object,
    minio_test.test_fget_object_version,
    minio_test.test_get_object_with_default_length,
    minio_test.test_get_partial_object,
    minio_test.test_thread_safe,
]

unverified_tests = [
    minio_test.test_list_buckets,
    minio_test.test_copy_object_no_copy_condition,
    minio_test.test_copy_object_with_metadata,
    minio_test.test_put_object,
    minio_test.test_stat_object,
    minio_test.test_stat_object_version,
    minio_test.test_list_objects_v1,
    minio_test.test_list_object_v1_versions,
    minio_test.test_list_objects_with_prefix,
    minio_test.test_list_objects_with_1001_files,
    minio_test.test_list_objects,
    minio_test.test_list_object_versions,
    minio_test.test_presigned_get_object_default_expiry,
    minio_test.test_presigned_get_object_expiry,
    minio_test.test_presigned_get_object_response_headers,
    minio_test.test_presigned_get_object_version,
    minio_test.test_presigned_put_object_default_expiry,
    minio_test.test_presigned_put_object_expiry,
    minio_test.test_get_bucket_policy,
    minio_test.test_set_bucket_policy_readonly,
    minio_test.test_set_bucket_policy_readwrite,
    minio_test.test_select_object_content,
    minio_test.test_remove_objects,
    minio_test.test_remove_object_versions,
]

if run_all:
    tests.extend(unverified_tests)

if test_list:
    test_names = str.split(test_list, ",")
    tests = [getattr(minio_test, test_name) for test_name in test_names]

for test_name in tests:
    minio_helpers.call_test(test_name, strict)
