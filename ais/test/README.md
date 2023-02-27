## AIStore testing

AIStore provides both unit tests and integration tests that can be run individually or in batches. Some tests require AIStore cluster, others do not, and some of the tests require the cluster (under test) to be deployed with more than one storage target and more than one proxy/gateway.

To run all tests, make sure to deploy a AIStore cluster with at least 3 proxies/gateways and at least 3 targets.
Then, cd to $GOPATH/src/github.com/NVIDIA/aistore and execute:

```console
$ BUCKET=<provider>://<bucket_name> go test -v -p 1 -count 1 -timeout 1h ./...
```

- provider: Backend provider AWS (="aws"), GCP (="gcp") or AZURE(="az") (note that some of the tests require access to the Cloud; optional if included in bucket name).
- bucket_name: Cloud-based bucket backed by AWS or GCP (note that some of the tests require access to the Cloud).
- -timeout 1h: to make sure the test runs are not terminated by the Go default 10 minute timeout.
- -p 1: run tests sequentially; since all tests share the same bucket, we sometimes can't allow them to run in parallel.
- -count=1: to disable Go test cache.
- -v: when used, Go test shows result (PASS/FAIL) for each of the named tests.

For a quick run, execute the following from the `$GOPATH/src/github.com/NVIDIA/aistore`:

```console
$ BUCKET=<bucket name> go test -v -p 1 -count 1 -short ./...
```

This will skip some long-running tests and run instead all unit tests, plus some basic PUT/GET/DELETE operations.

To run individual tests:

```console
$ BUCKET=<bucket name> go test ./ais/test -v -p 1 -run=Regression
$ BUCKET=<bucket name> go test ./ais/test -v -p 1 -run=GetAndRe
$ BUCKET=<bucket name> go test ./ais/test -run=TestProxy/PrimaryCrash
```
