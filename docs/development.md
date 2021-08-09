---
layout: post
title: DEVELOPMENT
permalink: /docs/development
redirect_from:
 - /development.md/
 - /docs/development.md/
---

## Table of Contents

- [Debugging](#debugging)
- [Useful scripts](#scripts)
  - [Clean deploy](#clean-deploy)
  - [Performance comparison](#performance-comparison)

## Debugging

By default, the cluster is deployed in `production` mode with verbose logging and asserts disabled.

To turn on the `debug` mode, deploy a cluster with `MODE="debug"` env variable (eg. `MODE="debug" make deploy`).
A cluster deployed in `debug` mode will produce a log like this:

```
... [DEBUG] starting with debug asserts/logs
```

As this only enables general debug asserts and logs it is also possible to enable verbose logging per package.
To do that deploy cluster with eg. `AIS_DEBUG="fs=4,reb=4"` what means that packages `fs` and `reb` will have logging level set to `4` (generally used for verbose logging).

## Scripts

There is a growing number of scripts and useful commands that can be used in development.
To see `make` options and usage examples, do:

```console
$ make help
```

### Clean deploy

```
./clean_deploy.sh [--ntargets TARGET_CNT] [--nproxies PROXY_CNT] [--mountpoints MPOINT_CNT] [--https] [--deploy local|remote|both] [--remote-alias REMOTE_ALIAS] [--PROVIDER ...] [--debug PKG=LOG_LEVEL[,PKG=LOG_LEVEL]]
```

Performs cleanup and then deploys a new instance of an AIS cluster.
To make it even more convenient, consider setting up an alias:

```bash
alias cais="bash ${GOPATH}/src/github.com/NVIDIA/aistore/deploy/scripts/clean-deploy --aws --gcp"
```

#### Example usage

The command below starts a cluster with 5 proxies and 5 targets with GCP cloud enabled.
Remember to set `GOOGLE_APPLICATION_CREDENTIALS` env when using GCP cloud!

```console
$ bash ./deploy/scripts/clean-deploy --gcp
```

The example below deploys:
- a simulated remote cluster with alias "remoteAIS"
- 3 targets
- 3 proxies
- with AWS support

```console
$ bash ./deploy/scripts/clean-deploy --deployment all --remote-alias remoteAIS -ntargets 3 -nproxies 3 --aws
```

#### Options

| Option | Description |
| ------ | ----------- |
| `--ntargets` | Number of targets to start (default: 5) |
| `--nproxies` | Number of proxies to start (default: 5) |
| `--mountpoints` | Number of mountpoints to use (default: 5) |
| `--PROVIDER` | Specifies the backend provider(s). Can be: `--aws`, `--azure`, `--gcp`, `--hdfs` |
| `--loopback` | Provision loopback devices |
| `--deployment` | Choose which AIS cluster to deploy. `local` to deploy only one AIS cluster, `remote` to only start an AIS-behind-AIS cluster, and `all` to deploy both the local and remote clusters. |
| `--remote-alias` | Alias to assign to the remote cluster |
| `--https` | Start cluster with HTTPS enabled (*) |
| `--debug PKG=LOG_LEVEL` | Change logging level of particular package(s) |

(*) To use this option, you must have generated certificates in `$HOME` directory. Here is a script which can help you with that:
```console
$ cd $HOME && openssl req -x509 -out localhost.crt -keyout localhost.key \
    -newkey rsa:2048 -nodes -sha256 \
    -subj '/CN=localhost' -extensions EXT -config <( \
     printf "[dn]\nCN=localhost\n[req]\ndistinguished_name = dn\n[EXT]\nsubjectAltName=DNS:localhost\nkeyUsage=digitalSignature\nextendedKeyUsage=serverAuth")
```

### Performance comparison

```
./bench.sh cmp [OLD_COMMIT] [--dir DIRECTORY] [--bench BENCHMARK_NAME] [--post_checkout SCRIPT_NAME] [--verbose]
```

Compares benchmark between the current commit and old commit provided in argument.

This script is incredibly important because it allows catching regressions.
It also can quickly provide an answer if the change that was made actually improved the performance.

#### Example usage

The command below will compare the benchmark(s) `BenchmarkRandom*` between the current commit and `f9a1536f...`.

```console
$ bash ./deploy/scripts/bootstrap.sh bench cmp f9a1536f4c9af0d1ac84c200e68f2ba73676c487 --dir bench/aisloader --bench BenchmarkRandom
```

#### Options

| Option | Description |
| ------ | ----------- |
| `--dir DIRECTORY` | Directory in which benchmark(s) should be run |
| `--bench BENCHMARK_NAME` | Name or prefix of benchmark(s) to run |
| `--post_checkout SCRIPT_NAME` | Script name which will executed after each `git checkout <commit>` (old and new commit) |
| `--verbose` | Run benchmarks in verbose mode |
