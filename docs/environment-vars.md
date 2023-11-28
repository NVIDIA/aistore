---
layout: post
title: Environment Variables
permalink: /docs/environment-vars
redirect_from:
 - /environment-vars.md/
 - /docs/environment-vars.md/
---

## Introduction

Generally, aistore configuration comprizes several sources, including:

1. cluster (a.k.a. global) and node (or, local) configurations, the latter further "splitting" into local config per se and local overrides of the inherited cluster config;
2. `aisnode` [command line](/docs/command_line.md);
3. environment variables - this document;
4. assorted low-level constants (also referred to as "hardcoded defaults") that almost never have to change.

This enumeration does _not_ include buckets. In aistore, buckets inherit a part of the cluster config that can be further changed on a per-bucket basis - either at creation time or at any later time, etc.

> In effect, cluster configuration contains cluster-wide defaults for all AIS buckets, current and future.

For additional references, please see the [last section](#references) in this document. The rest of it, though, describes only and exclusively **environment variables** - the item number 3 above.

## Rules

But first, let's state two common rules that, in fact, apply across the board:

* all environment settings are **optional**
* if specified, environment variable will **override**:
  - the corresponding default *constant* in the code (if exists), and/or
  - persistent *configuration* (again, if exists)

For example:

* persistent (and replicated) node ID can be overridden via `AIS_DAEMON_ID` (below)
* `AIS_READ_HEADER_TIMEOUT`, if specified, will be used instead of the `apc.ReadHeaderTimeout` [constant](https://github.com/NVIDIA/aistore/blob/master/api/apc/const.go)
* `AIS_USE_HTTPS` takes precedence over `net.http.use_https` value from the [cluster configuration](/docs/configuration.md)

and so on.

### Table of Contents

The remainder of this text groups aistore environment variables by their respective usages, and is structured as follows:

- [Primary](#primary)
- [Network](#network)
- [Node](#node)
- [HTTPS](#https)
- [Local Playground](#local-playground)
- [Kubernetes](#kubernetes)
- [Package: backend](#package-backend)
  - [AIS as S3 storage](#ais-as-s3-storage)
- [Package: stats](#package-stats)
- [Package: memsys](#package-memsys)
- [Package: transport](#package-transport)

separately, there's authenication server config:
- [AuthN](#authn)

and finally:
- [References](#references)

## Primary

Background: in a running aistore cluster, at any point in time there's a single _primary_ gateway that may also be administratively selected, elected, reelected. Hence, two related variables:

| name | comment |
| ---- | ------- |
| `AIS_IS_PRIMARY` | at startup, tells _one_ of the (starting-up) proxies to assume the _primary_ role; e.g. usage: 'export AIS_IS_PRIMARY=true' |
| `AIS_PRIMARY_ID` | at startup, tells _all_ starting-up proxies that the one of them with a given ID _is_, in fact, the _primary_; e.g. usage: 'export AIS_PRIMARY_ID=foo-bar' |

## Network

| name | comment |
| ---- | ------- |
| `AIS_ENDPOINT` | http or https address of an arbitrary AIS gateway (proxy) in a given cluster |
| `AIS_CLUSTER_CIDR` | ais cluster [CIDR](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing); often can be understood/approximated as the cluster's subnet; when specified will be used to differentiate between clients within the same subnet vs outside |
| `AIS_READ_HEADER_TIMEOUT` | maximum time to receive request headers; e.g. usage: 'export AIS_READ_HEADER_TIMEOUT=10s', and note that '0s' (zero) is also permitted |

## Node

| name | comment |
| ---- | ------- |
| `AIS_DAEMON_ID` | ais node ID |
| `AIS_HOST_IP` | node's public IPv4 |
| `AIS_HOST_PORT` | node's public TCP port (and note the corresponding local config: "host_net.port") |

See also:
* [three logical networks](/docs/performance.md#network)
* [K8s deployment: custom resource 'AIStore'](https://github.com/NVIDIA/ais-k8s/blob/master/docs/walkthrough.md#deploy-an-aistore-cluster)

## HTTPS

| name | comment |
| ---- | ------- |
| `AIS_USE_HTTPS` | tells aistore to run HTTPS transport (both public and intra-cluster networks); overrides the corresponding config; e.g. usage: 'export AIS_USE_HTTPS=true' |
| `AIS_CRT` | X509 certificate pathname (this and the rest variables in the table are ignored when aistore is AIS_USE_HTTPS==false |
| `AIS_CRT_KEY` | pathname that contains X509 certificate private key |
| `AIS_CLIENT_CA` | certificate authority that authorized (signed) the certificate |
| `AIS_SKIP_VERIFY_CRT` | when true will skip X509 cert verification (usually enabled to circumvent limitations of self-signed certs) |

## Local Playground

| name | comment |
| ---- | ------- |
| `NUM_TARGET` | usage: development scripts, local playground automation |
| `NUM_PROXY` | (ditto) |

See also:
* [deploy/scripts/clean_deploy.sh](https://github.com/NVIDIA/aistore/blob/master/deploy/scripts/clean_deploy.sh)

Or, just run the script for quick inline help:

```console
$ deploy/scripts/clean_deploy.sh --help
NAME:
  clean_deploy.sh - locally deploy AIS clusters for development

USAGE:
  ./clean_deploy.sh [options...]

OPTIONS:
  --target-cnt        Number of target nodes in the cluster (default: 5)
  --proxy-cnt         Number of proxies/gateways (default: 5)
...
...
```

## Kubernetes

| name | comment |
| ---- | ------- |
| `MY_POD` | POD name |
| `K8S_NODE_NAME` | Kubernetes node name |
| `POD_NAMESPACE` | Kubernetes namespace |

See also:
* [AIS K8s Operator: environment variables](https://github.com/NVIDIA/ais-k8s/blob/master/operator/pkg/resources/cmn/env.go)

## Package: backend

AIS natively supports 3 (three) [Cloud storages](/docs/providers.md).

The corresponding environment "belongs" to the internal [backend](https://github.com/NVIDIA/aistore/tree/master/ais/backend) package and includes:

| name | comment |
| ---- | ------- |
| `S3_ENDPOINT` | global S3 endpoint to be used instead of `s3.amazonaws.com` |
| `AWS_PROFILE` | global AWS profiles with alternative account credentials and/or AWS region |
| `GOOGLE_CLOUD_PROJECT` | GCP account with permissions to access your Google Cloud Storage buckets |
| `GOOGLE_APPLICATION_CREDENTIALS` | (ditto) |
| `AZURE_STORAGE_ACCOUNT` | Azure account |
| `AZURE_STORAGE_KEY` | (ditto) |
| `AIS_AZURE_URL` | Azure endpoint, e.g. `http://<account_name>.blob.core.windows.net` |

Notice in the table above that variables `S3_ENDPOINT` and `AWS_PROFILE` are designated as _global_.

The implication: it is possible to override one or both of them on a per-bucket basis:

* [configuring buckets with alternative S3 andpoint and/or credentials](/docs/cli/aws_profile_endpoint.md)

### AIS as S3 storage

Environment `S3_ENDPOINT` is _important_, and may be also be a source of minor confusion. The reason: aistore itself provides S3 compatible interface.

For instance, on the aistore's client side you could say something like:

```console
export S3_ENDPOINT=https://10.0.4.53:51080/s3
```

and then run existing S3 applications against an aistore cluster at `10.0.4.53` - with no changes (to the application).

Moreover, configure aistore to handle S3 requests at its "/" root:

```console
$ ais config cluster features Provide-S3-API-via-Root
```

and specify `S3_ENDPOINT` environment that looks even better (some would maybe say):

```console
export S3_ENDPOINT=https://10.0.4.53:51080
```

**To recap**:

* use `S3_ENDPOINT` to override the `s3.amazonaws.com` default;
* specify `AWS_PROFILE` to use a non-default (named) AWS profile

and separately:

* you could run existing S3 apps (with no changes) against aistore by using `S3_ENDPOINT` on the client side

See also:
* [AIS buckets](/docs/cli/bucket.md)
* [Bucket configuration: AWS profiles](/docs/cli/aws_profile_endpoint.md)
* [Using aistore as S3 endpoint](/docs/s3compat.md)

## Package: stats

AIStore is a fully compliant [Prometheus exporter](https://prometheus.io/docs/instrumenting/writing_exporters/).

In addition and separately, AIStore supports [StatsD](https://github.com/etsy/statsd), and via StatsD - Graphite (collection) and Grafana (graphics).

The corresponding binary choice between StatsD and Prometheus is a **deployment-time** switch controlled by a single environment variable: **AIS_PROMETHEUS**.

Namely:

| name | comment |
| ---- | ------- |
| `AIS_PROMETHEUS` | e.g. usage: `export AIS_PROMETHEUS=true` |
| `AIS_STATSD_PORT` | use it to override the default `8125` (see https://github.com/etsy/stats) |
| `AIS_STATSD_PROBE` | a startup option that, when true, tells an ais node to _probe_ whether StatsD server exists (and responds); if the probe fails, the node will disable its StatsD functionality completely - i.e., will not be sending any metrics to the StatsD port (above) |

## Package: memsys

| name | comment |
| ---- | ------- |
| `AIS_MINMEM_FREE` | TODO |
| `AIS_MINMEM_PCT_TOTAL` | TODO |
| `AIS_MINMEM_PCT_FREE` | TODO |

## Package: transport

| name | comment |
| ---- | ------- |
| `AIS_STREAM_DRY_RUN` | TODO |
| `AIS_STREAM_BURST_NUM` | TODO |

## AuthN

| name | comment |
| ---- | ------- |
| `AIS_AUTHN_ENABLED` | TODO |
| `AIS_AUTHN_URL` | TODO |
| `AIS_AUTHN_TOKEN_FILE` | TODO |
| `AIS_AUTHN_CONF_DIR` | TODO |
| `AIS_AUTHN_LOG_DIR` | TODO |
| `AIS_AUTHN_LOG_LEVEL` | TODO |
| `AIS_AUTHN_PORT` | TODO |
| `AIS_AUTHN_TTL` | TODO |
| `AIS_AUTHN_USE_HTTPS` | TODO |

## References

* `env` package [README](https://github.com/NVIDIA/aistore/blob/master/api/env/README.md)
* [system filenames](https://github.com/NVIDIA/aistore/blob/master/cmn/fname/fname.go)
* [cluster configuration](/docs/configuration.md)
* [local playground: cluster and node configuration](https://github.com/NVIDIA/aistore/blob/master/deploy/dev/local/aisnode_config.sh)
* [`ais config` command](/docs/cli/config.md)
* [performance tunables](/docs/performance.md)
* [feature flags](/docs/feature_flags.md)
