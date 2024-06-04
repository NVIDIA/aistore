# Botocore Monkey Patch Testing

These tests verify the behavior of [`aistore.botocore_patch.botocore`](/python/aistore/botocore_patch/README.md), a monkey patch for the Amazon botocore library that allows developers to access AIStore using popular Amazon [Boto3](https://githbu.com/boto/boto3)'s S3 client code.

Specifically, these tests verify the Boto3, monkey patched by `aistore.botocore_patch.botocore`, works correctly as configured to respect the HTTP `location` header when receiving a HTTP 301, 302 or 307.

## Test Sets

The [`botocore_common.py`](python/tests/botocore_common.py) file contains a common test group to verify a basic set of S3 operations.

There are two degrees of freedom to test here:

 - Whether botocore has been monkey-patched ("patched") or not ("unpatched").
 - Whether the upstream service (like AIStore) sends redirects ("redirects") or doesn't ("noredirects").

To fully cover all possibilities, we have four test sets, each inheriting and testing against the same base fixture `botocore_common.py`:

|                   | **No Redirects**                       | **Redirects**                         |
|-------------------|----------------------------------------|---------------------------------------|
| **Unpatched**     | `test_botocore_noredirect_unpatched.py`: A control case for the default botocore behavior. | `test_botocore_redirects_unpatched.py`: Another control test where unpatched botocore should throw errors when redirects occur. |
| **Patched**       | `test_botocore_noredirect_patched.py`: A passthrough test to ensure the monkey patch doesn't break botocore's normal behavior. | `test_botocore_redirects_patched.py`: Positive tests for the monkey patch itself. |

## Unit Testing Mocks

For the purpose of unit testing within the scope of an interface provided by `aistore.botocore_patch.botocore`, here we do not actually set up AIStore as an object storage to test against. Instead, we use [moto](https://github.com/spulec/moto) to mock an S3-like interface, simulating AIStore. Additionally, we implement another monkey patch [`mock_s3_redirect.py`](python/tests/unit/botocore_patch/mock_s3_redirect.py) under moto to send redirects when needed, mimicking the behavior of AIStore.

## Unit Testing Execution

Since both the monkey patch and our moto patches rely on runtime imports, running tests like this:

```
pytest -v tests/unit
```

...won't work. Because it's hard to "unimport" a module in python, previous tests will contaminate each other's state.

To run these tests, you need to start a new forked process each time, scoped per test file.
To achieve this inline we use the pytest-xdist plugin like so:

```
pytest -v -n $(MP_TESTCOUNT) --dist loadfile tests/unit/botocore_patch
```

...which is one of the reasons why this test set is kept separate from those for the aistore SDK proper.

## Testing different boto3 and botocore versions

By default we pull in the latest `boto3` and `botocore` to test against (see `botocore_requirements.dev.txt`).

You can alter this behavior by exporting `BOTO3_VERSION` and / or `BOTOCORE_VERSION` prior to running tests:

```
export BOTO3_VERSION=1.26.24
export BOTOCORE_VERSION=1.29.24
make python_botocore_tests
```
