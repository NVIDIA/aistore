---
layout: post
title: Environment Variables
permalink: /docs/environment-vars
redirect_from:
 - /environment-vars.md/
 - /docs/environment-vars.md/
---

## Introduction

Generally, aistore configuration comprises several sources:

1. cluster (a.k.a. global) and node (or, local) configurations, the latter further "splitting" into local config per se and local overrides of the inherited cluster config;
2. `aisnode` [command line](/docs/command_line.md);
3. environment variables (this document);
4. finally, assorted low-level constants (also referred to as "hardcoded defaults") that almost never have to change.

This enumeration does _not_ include buckets (and their respective configurations). In aistore, buckets inherit a part of the cluster config that can be further changed on a per-bucket basis - either at creation time or at any later time, etc.

> In effect, cluster configuration contains cluster-wide defaults for all AIS buckets, current and future.

For additional references, please see the [last section](#references) in this document. The rest of it, though, describes only and exclusively **environment variables** - item `3` above.

## Rules

First though, two common rules that, in fact, apply across the board:

* in aistore, all environment settings are **optional**
* if specified, environment variable will always **override**:
  - the corresponding default *constant* (if exists), and/or
  - persistent *configuration* (again, if the latter exists).

For example:

* in aistore cluster, each node has an `ID`, which is persistent, replicated and unique; at node startup its `ID` can be overridden via `AIS_DAEMON_ID` environment (see below);
* environment `AIS_READ_HEADER_TIMEOUT`, if specified, will be used instead of the `apc.ReadHeaderTimeout` [constant](https://github.com/NVIDIA/aistore/blob/main/api/apc/const.go) in the code;
* `AIS_USE_HTTPS` takes precedence over `net.http.use_https` value from the [cluster configuration](/docs/configuration.md),

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
| `AIS_PRIMARY_EP` | at startup, tells _one_ of the starting-up proxies to assume the _primary_ role iff `AIS_PRIMARY_EP` specifies one of the proxy's listening endpoints; e.g. usage: |

```bash
$ AIS_PRIMARY_EP=https://ais-proxy-0.svc.cluster.local:51082
# or (same):
$ AIS_PRIMARY_EP=ais-proxy-0.svc.cluster.local:51082
```

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
| `NUM_TARGET` | usage is limited to development scripts and test automation |
| `NUM_PROXY` | (ditto) |

See also:
* [scripts/clean_deploy.sh](https://github.com/NVIDIA/aistore/blob/main/scripts/clean_deploy.sh)
* [wait-for-cluster](https://github.com/NVIDIA/aistore/blob/main/ais/test/main_test.go#L47-L56)

## Kubernetes

| name | comment |
| ---- | ------- |
| `MY_POD` and `HOSTNAME` | Kubernetes POD name. `MY_POD` is used in [production](operator/pkg/resources/cmn/env.go); `HOSTNAME`, on the other hand, is usually considered a Kubernetes default |
| `MY_NODE` | Kubernetes node name |
| `K8S_NS` and `POD_NAMESPACE` | Kubernetes namespace. `K8S_NS` is used in [production](operator/pkg/resources/cmn/env.go), while `POD_NAMESPACE` - development |

Kubernetes POD name is also reported via `ais show cluster` CLI - when it is a Kubernetes deployment, e.g.:

```console
$ ais show cluster t[fXbarEnn]
TARGET         MEM USED(%)   MEM AVAIL     CAP USED(%)   CAP AVAIL     LOAD AVERAGE    REBALANCE   UPTIME        K8s POD         STATUS  VERSION        BUILD TIME
t[fXbarEnn]    3.08%         367.66GiB     51%           8.414TiB      [0.9 1.1 1.3]   -           1852h19m40s   ais-target-26   online  3.20.92bc0c1   2023-09-18T19:12:52+0000
```

See related:
* [AIS K8s Operator: environment variables](https://github.com/NVIDIA/ais-k8s/blob/main/operator/pkg/resources/cmn/env.go)

## AWS S3

**NOTE:** for the most recent updates, please refer to the [source](https://github.com/NVIDIA/aistore/blob/main/api/env/aws.go).

| name | comment |
| ---- | ------- |
| `S3_ENDPOINT` | global S3 endpoint to be used instead of `s3.amazonaws.com` |
| `AWS_REGION` | default bucket region; can be set to override the global default 'us-east-1' location |
| `AWS_PROFILE` | global AWS profile with alternative (as far as the [default]) credentials and/or AWS region |

## Package: backend

AIS natively supports 3 (three) [Cloud storages](/docs/providers.md).

The corresponding environment "belongs" to the internal [backend](https://github.com/NVIDIA/aistore/tree/main/ais/backend) package and includes:

| name | comment |
| ---- | ------- |
| `S3_ENDPOINT`, `AWS_PROFILE`, and `AWS_REGION`| see previous section |
| `GOOGLE_CLOUD_PROJECT`, `GOOGLE_APPLICATION_CREDENTIALS` | GCP account with permissions to access Google Cloud Storage buckets |
| `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY` | Azure account with  permissions to access Blob Storage containers |
| `AIS_AZURE_URL` | Azure endpoint, e.g. `http://<account_name>.blob.core.windows.net` |

Notice in the table above that the variables `S3_ENDPOINT` and `AWS_PROFILE` are designated as _global_: cluster-wide.

The implication: it is possible to override one or both of them on a **per-bucket basis**:

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
$ ais config cluster features S3-API-via-Root
```

and re-specify `S3_ENDPOINT` environment to make it looking slightly more conventional:

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
| `AIS_MINMEM_FREE` | for details, see [Memory Manager, Slab Allocator (MMSA)](https://github.com/NVIDIA/aistore/blob/main/memsys/README.md) |
| `AIS_MINMEM_PCT_TOTAL` | same as above and, specifically, te section "Minimum Available Memory" |
| `AIS_MINMEM_PCT_FREE` | (ditto) |

## Package: transport

| name | comment |
| ---- | ------- |
| `AIS_STREAM_DRY_RUN` | read and immediately discard all read data (can be used to evaluate client-side throughput) |
| `AIS_STREAM_BURST_NUM` | overrides `transport.burst_buffer` knob from the [cluster configuration](/docs/configuration.md) |

See also: [streaming intra-cluster transport](https://github.com/NVIDIA/aistore/blob/main/transport/README.md).

## AuthN

AIStore Authentication Server (**AuthN**) provides OAuth 2.0 compliant [JSON Web Tokens](https://datatracker.ietf.org/doc/html/rfc7519) based secure access to AIStore.

AuthN supports multiple AIS clusters; in fact, there's no limit on the number of clusters a given AuthN instance can provide authentication and access control service for.

| name | comment |
| ---- | ------- |
| `AIS_AUTHN_ENABLED` | aistore cluster itself must "know" whether it is being authenticated; see usage and references below |
| `AIS_AUTHN_CONF_DIR` | AuthN server configuration directory, e.g. `"$HOME/.config/ais/authn` |
| `AIS_AUTHN_LOG_DIR` | usage: deployment scripts and integration tests |
| `AIS_AUTHN_LOG_LEVEL` | ditto |
| `AIS_AUTHN_PORT` | can be used to override `52001` default |
| `AIS_AUTHN_TTL` | authentication token expiration time; 0 (zero) means "never expires" |
| `AIS_AUTHN_USE_HTTPS` | when true, tells a starting-up AuthN to use HTTPS |

Separately, there's also client-side AuthN environment that includes:

| name | comment |
| ---- | ------- |
| `AIS_AUTHN_URL` | used by [CLI](docs/cli/auth.md) to configure and query authenication server (AuthN) |
| `AIS_AUTHN_TOKEN_FILE` | token file pathname; can be used to override the default `$HOME/.config/ais/cli/<fname.Token>`  |

When AuthN is disabled (i.e., not used), `ais config` CLI will show something like:

```console
$ ais config cluster auth
PROPERTY         VALUE
auth.secret      **********
auth.enabled     false
```

Notice: this command is executed on the aistore cluster, not AuthN per se.

See also:

* [AIS AuthN server](/docs/authn.md)
* [AIS AuthN server: CLI management](/docs/cli/authn.md)

## References

* `env` package [README](https://github.com/NVIDIA/aistore/blob/main/api/env/README.md)
* [system filenames](https://github.com/NVIDIA/aistore/blob/main/cmn/fname/fname.go)
* [cluster configuration](/docs/configuration.md)
* [local playground: cluster and node configuration](https://github.com/NVIDIA/aistore/blob/main/deploy/dev/local/aisnode_config.sh)
* [`ais config` command](/docs/cli/config.md)
* [performance tunables](/docs/performance.md)
* [feature flags](/docs/feature_flags.md)
