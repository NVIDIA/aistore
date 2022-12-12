# A note on botocore monkey patch testing

These tests verify the behavior of aistore.botocore_patch.botocore, a monkey patch
for the Amazon botocore library that modifies it to respect the HTTP 
`location` header when receiving a HTTP 301, 302 or 307.

There are two degrees of freedom to test here:

 - We've either monkey patched botocore, or we haven't ("patched" or "unpatched")
 - The upstream service either sends redirects (like aistore), or it doesn't ("redirects" or "noredirects").

We have four test sets which each run the same base fixture:

 - `test_botocore_noredirect_unpatched.py`: A control case for the default botocore behavior
 - `test_botocore_noredirect_patched.py`: A passthrough test to check the monkey patch doesn't break botocores normal behavior
 - `test_botocore_redirects_unpatched.py`: Another control test: unpatched botocore should throw errors when redirects happen
 - `test_botocore_redirects_patched.py`: Positive tests for the monkey patch itself

We use [moto](https://github.com/spulec/moto) to mock an S3 like endpoint, and monkey patch it to send redirects when wanted.

Since both the monkey patch and our moto patches rely on runtime imports, running tests like this:

```
pytest -v tests/unit
```

...won't work. It's hard to "unimport" a module in python, and so previous tests will contaminate each other's state.

To run these tests, you need to start a new forked process each time, scoped per test file.
To do this inline we use the pytest-xdist plugin like so:

```
pytest -v -n $(MP_TESTCOUNT) --dist loadfile tests/unit
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
