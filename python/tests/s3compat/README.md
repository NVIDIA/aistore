# Minio s3 tests

This directory contains a modified version of the s3 tests from the [Minio Python SDK](https://github.com/minio/minio-py). 
* `minio_helpers.py` replaces the default minio client initialization and test setup.
* `run_tests` serves as an entry point and specifies which tests to include in a given test run. 
* `tests.py` is imported directly from the minio functional test suite and is unmodified.

The goal of this module is to debug and maintain AIS s3 compatibility

These tests are NOT verified as working on Windows, and many may not work properly because the tests expect to run 
against the Minio client. 

The tests are divided into two sections:
1. Verified as working -- AIS responds as an s3 client should
2. Unverified -- these tests expect s3 behavior AIS does not currently support.

---

## Cluster configuration

First, the AIS cluster to test against must be configured to run as an s3 client and run with AWS-style MD5 checksums.
This can be set with AIS cli. 

    ais config cluster features Provide-S3-API-via-Root
    ais config cluster checksum.type=md5

---

## Environment variables

The default is to run all tests (`S3_COMPAT_RUN_ALL=True`). 
To run only tests we expect to succeed, set the environment variable `S3_COMPAT_RUN_ALL=False` 

To run a specific set of tests, set the environment variable `S3_COMPAT_TEST_LIST` to the test names separated by commas 
e.g.

    export S3_COMPAT_TEST_LIST="test_remove_object,test_remove_object_version"

To exit tests on the first failure, set the environment variable `S3_COMPAT_STRICT=True`

Additional cluster connection details can be defined with the following parameters:
* AIS_ENDPOINT (default localhost:8080)
* ACCESS_KEY
* SECRET_KEY

---

## Running compatibility tests 
See above [Environment variables](#environment-variables) section for selecting tests to run.

Because these tests use the Minio client rather than boto3, running does not require importing the
`aistore.botocore_patch` module. 


From `/python` directory:

    make python_s3_compat_test

Or: 
- Install the prerequisites from the requirements file: `pip3 install -r tests/s3compat/requirements`
- Run `python3 tests/s3compat/run_tests.py`


## References

* [Minio Python SDK](https://github.com/minio/minio-py)
* [Python Tests README](https://github.com/NVIDIA/aistore/tree/main/python/tests/README.md)
