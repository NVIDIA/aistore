Testing
-----------------------------------------------------------------

DFC has both unit tests and integration tests, they are all in the form of GO tests. Tests can be run individually or run all. Some tests require DFC cluster, some don't, and some tests require more than one target or proxy. To run all tests, deploy a DFC cluster with at least 3 proxies and 3 targets.

To run all tests, from $GOPATH/src/github.com/NVIDIA/dfcpub:

```
$ BUCKET=<bucket name> go test -v -p 1 -count 1 -timeout 20m ./...
```

- 'bucket name': bucket name in AWS, some tests require access to AWS.
- '-timeout 20m': to make sure the test runs are not cancelled by GO test's default 10 minute cut off time. Running all tests takes about 10 minutes.
- "-p 1": run tests sequentially; since all tests share the same bucket, can't allow tests run in parallel.
- "-count=1": this is to disable GO test cache; without it, when tests fail, go test might show ok if the same test passed before and results are cached.
- "-v": when used, GO test shows result (PASS/FAIL) for each test; so if -v is used, check the results carefully, last line shows PASS doesn't mean the test passed, it only means the last test passed.

Before pushing to master, should run all tests to make sure no regression.

For a quick run during development:
```
$ BUCKET=<bucket name> go test -v -p 1 -count 1 -short ./...
```

This will skip some of the long running tests, it runs all unit tests plus some basic DFC put/get/delete operations.

To run individual tests:
```
BUCKET=<bucket name> go test ./dfc/tests -v -run=Regression
BUCKET=<bucket name> go test ./dfc/tests -run=TestProxy/PrimaryCrash
BUCKET=<bucket name> go test ./dfc/tests -v -run=smoke -numworkers=4
```

When running individual tests, more command line optons are available, for example, duration of runs, number of workers, etc, for a full list of all parameters, use go test --help.
