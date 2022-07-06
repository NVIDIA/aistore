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
- [MsgPack](/docs/msgp.md)
- [Useful scripts](#scripts)
  - [Clean deploy](#clean-deploy)
  - [Performance comparison](#performance-comparison)
- [More](#more)

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
./clean_deploy.sh [--target-cnt TARGET_CNT] [--proxy-cnt PROXY_CNT] [--mountpath-cnt MOUNTPATH_CNT] [--https] [--deployment local|remote|all] [--remote-alias REMOTE_ALIAS] [--PROVIDER ...] [--debug PKG=LOG_LEVEL[,PKG=LOG_LEVEL]] [--loopback SIZE]
```

Performs cleanup and then deploys a new instance of an AIS cluster.
To make it even more convenient, consider setting up an alias:

```bash
alias cais="bash ${GOPATH}/src/github.com/NVIDIA/aistore/deploy/scripts/clean-deploy --aws --gcp"
```

#### Example: minimal remote cluster

The command below can be conveniently used to develop with remote AIS clusters and/or test related functionality.

Here we run two minimal - one gateway and one target - clusters: "local" (by default, at http://localhost:8080)
and "remote", at http://127.0.0.1:11080.

> Henceforth, the terms "local cluster" and "remote cluster" are used without quotes.

The script will not only deploy the two clusters - it will also assign the remote one its user-specified alias
and attach one cluster to another, thus forming a [global namespace](providers.md#remote-ais-cluster).

```console
$ deploy/scripts/clean_deploy.sh --target-cnt 1 --proxy-cnt 1 --mountpath-cnt 4 --deployment all --remote-alias remais
```

Here's another example that illustrates multi-node (6 + 6) cluster with storage targets utilizing loopback devices to simulate actual non-shared storage disks (one disk per target mountpath):

```console
$ deploy/scripts/clean_deploy.sh --target-cnt 6 --proxy-cnt 6 --mountpath-cnt 4 --deployment all --loopback 123M --remote-alias remais --gcp --aws
```

> Overall, this line above will create 4 loopbacks of total size 123M * 4 = 0.5GiB. It'll take maybe a few extra seconds but only at the very first run - subsequent cluster restarts will utilize already provisioned devices and other persistent configuration.

From here, you can create and destroy buckets, read and write data, show buckets, objects and their respective properties -
in short, perform all supported operations on the remote cluster - either directly, via `AIS_ENDPOINT` or indirectly,
via the (attached) local cluster. For example:

```console
# create bucket by "pointing" the CLI i(directly) to the remote cluster:
$ AIS_ENDPOINT=http://127.0.0.1:11080 ais bucket create ais://abc

# PUT an object into remote cluster's bucket:
$ AIS_ENDPOINT=http://127.0.0.1:11080 ais put README.md ais://abc

# make sure that the local cluster can "see" remote buckets (and **notice** the usage of the remote alias):
$ ais ls ais://@remais/abc

# show properties of objects stored in the remote cluster's buckets, etc.
$ ais object show ais://@remais/abc/README.md
```

> Notice the bucket naming syntax: by convention, prefix `@` indicated remote cluster's UUIDs, and so
`ais://@remais/abc` translates as "AIS backend provider, where remote cluster has alias `remais`".

#### Example: 5 proxies and 5 targets with GCP backend

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
$ bash ./deploy/scripts/clean-deploy --deployment all --remote-alias remoteAIS --target-cnt 3 --proxy-cnt 3 --aws
```

#### Options

| Option | Description |
| ------ | ----------- |
| `--target-cnt` | Number of targets to start (default: 5) |
| `--proxy-cnt` | Number of proxies to start (default: 5) |
| `--mountpath-cnt` | Number of mountpaths to use (default: 5) |
| `--PROVIDER` | Specifies the backend provider(s). Can be: `--aws`, `--azure`, `--gcp`, `--hdfs` |
| `--loopback` | Loopback device size, e.g. 10G, 100M (default: 0). Zero size means: no loopbacks. The minimum size is 100M. |
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

## More

This [local-playground usage example](/deploy/dev/local/README.md) is a yet another brief introduction into setting up Go environment, provisioniong data drives for AIS deployment, and running a minimal AIS cluster - locally.
