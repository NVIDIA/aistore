# Python Tests

This directory contains unit tests and integration tests for each of the python package interfaces we provide to access AIStore, including the Amazon S3 botocore, SDK, and PyTorch datasets APIs.
It also contains tests for verifying s3 compatibility.

---

## Prerequisites

Before running the tests, install Python and set up your environment (venv, pyenv, conda, etc.). 
The below instructions assume `python` will execute the correct version of Python. 

Next, with your environment active, from the `aistore/python` directory install the dependencies for the tests you want 
to run:

For all packages:

`pip install -r aistore/common_requirements`

For botocore patch tests:

`pip install -r aistore/botocore_patch/botocore_requirements`

For pytorch tests:

`pip install -r aistore/pytorch/dev_requirements`

---

## Integration tests

### Cluster

Integration tests require a running AIS cluster. 
This can be specified with the environment variable `AIS ENDPOINT` or will default to http://localhost:8080. 

### ETL

Note that any tests of etl functionality require the AIS cluster to be running in kubernetes. 
These tests can be skipped using the pytest ignore option, e.g. `--ignore=tests/integration/sdk/test_etl_ops.py`

### Remote buckets

To run tests that expect a remote bucket, e.g. testing eviction of an object stored in s3, the `BUCKET`
environment variable must be set; otherwise these tests will be skipped. 

---

## Running the tests

All tests should support running on all operating systems, given access to a working AIS cluster. 

Because of the way our botocore patch works, simply running pytest on the entire test directory will fail those tests. See the [botocore test README](python/tests/unit/botocore_patch/README.md) for more info

Below are the recommended commands for running the test suites. Run these commands from the aistore/python directory. 

---

### All SDK tests:

`python -m pytest tests/unit/sdk tests/integration/sdk`


#### Integration only, excluding ETL (ETL requires AIS cluster running on k8s)

`python -m pytest tests/integration/sdk -m "not etl"`

#### Integration only, with ETL

`python -m pytest tests/integration/sdk -m etl`


#### Unit only

`python -m pytest tests/unit/sdk`

---

### Botocore patch

#### Unit tests
1.  Set the variable for number of tests
    ##### Windows (Powershell): 
        $BOTO_UNIT_TEST_COUNT = Get-ChildItem -Path "tests/unit/botocore_patch" -Filter "test*.py" | Measure-Object | Select-Object -ExpandProperty Count
    ##### Unix:
        BOTO_UNIT_TEST_COUNT=$(ls -1 tests/unit/botocore_patch/test*py | wc -l)
2.  Run pytest with the xdist plugin to ensure each test runs separately

    `python -m pytest -v -n $BOTO_UNIT_TEST_COUNT --dist loadfile tests/unit/botocore_patch/`

#### Integration tests

    python -m pytest tests/integration/botocore_patch

---

### PyTorch

    python -m pytest tests/integration/pytorch

---

### S3 compatibility

Currently, we have 2 separate tests for AIS S3 compatibility.

#### Custom s3 tests

There is our own suite of tests, located in tests/integration/boto3. It can be run with

    python -m pytest -v tests/integration/boto3 


#### Minio s3 tests

`s3compat` contains a modified version of the s3 tests from the Minio Python SDK, see the [README](s3compat/README.md) for details.

### References

* [Botocore Test README](unit/botocore_patch/README.md)
* [S3 Compatibility Test README](s3compat/README.md)

