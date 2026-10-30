This folder is a collection of scripts used for development and productionxi - primarily, to build images and run tests.

Separately, there's also root of the repository that contains:

* main [Makefile](/Makefile)
* GitLab [CI](/.gitlab-ci.yml) (utilizes [Makefile](/Makefile) and the scripts)
* GitHub [workflows](/.github/workflows) (ditto)

Script names and respective descriptions follow below.

## upgrade-oss.sh

Upgrade the root Go module's dependencies, then remove and restore the Kubernetes
requirements so MVS selects their required `kube-openapi` version. Remove the Go
patch version from `go.mod` and run `go mod tidy`.

```console
$ ./scripts/upgrade-oss.sh
```

## github-release/

Release scripts: root module checksums before tagging (`gosum.sh`) and release assets (`upload-assets.sh`).
See [github-release/README.md](github-release/README.md).

## bench.sh

A tool to run and compare benchmarks between current (the latest) commit and a specified one.

Utilizes `go test` and `benchcmp` tools.

Is used in our [Makefile](/Makefile) for integration into CI pipeline.

### Usage

```console
$ ./bench.sh cmp --dir "<directory to search for tests>" --verbose --post-checkout "<post checkout commands to run>"
```

## clean_deploy.sh

Development-only. Performs several useful commands including shutdown of a locally deployed cluster, `cleanup`, etc.

### Usage

```console
$ ./clean_deploy.sh <directory used for $AIS_DIR>
```

[More details on usage can be found here.](/docs/development.md#clean-deploy)

## check-ktls.sh

Check that a Linux host or container can run AIS targets with kTLS transmit offload ([docs](/docs/https.md#ktls-transmit-offload)).
After a test run, `--armed` fails if no connection armed kTLS. The CI jobs `test:short:https` and `test:long:https` run both checks.

```console
$ ./scripts/check-ktls.sh
$ ./scripts/check-ktls.sh --armed
```

## bootstrap.sh

Used internally by almost all our [Makefile](/Makefile) commands.

## utils.sh

A collection of common `shell` functions.
