## Introduction

Generally, AIStore (AIS) configuration comprises several sources:

1. cluster (a.k.a. global) and node (or, local) configurations, the latter further "splitting" into local config per se and local overrides of the inherited cluster config;
2. `aisnode` [command line](/docs/command_line.md);
3. environment variables (this document);
4. finally, assorted low-level constants (also referred to as "hardcoded defaults") that almost never have to change.

This enumeration does _not_ include buckets (and their respective configurations). In AIS, buckets inherit a part of the cluster config that can be further changed on a per-bucket basis - either at creation time or at any later time, etc.

> In effect, cluster configuration contains cluster-wide defaults for all AIS buckets, current and future.

For additional references, please see the [last section](#references) in this document. The rest of it, though, describes only and exclusively **environment variables** - item `3` above.

## Rules

First though, two common rules that, in fact, apply across the board:

* in AIS, all environment settings are **optional**
* if specified, environment variable will always **override**:
  - the corresponding default *constant* (if exists), and/or
  - persistent *configuration* (again, if the latter exists).

For example:

* in [AIS cluster](/docs/overview.md#at-a-glance), each node has an `ID`, which is persistent, replicated and unique; at node startup its `ID` can be overridden via `AIS_DAEMON_ID` environment (see below);
* environment `AIS_READ_HEADER_TIMEOUT`, if specified, will be used instead of the `apc.ReadHeaderTimeout` [constant](https://github.com/NVIDIA/aistore/blob/main/api/apc/const.go) in the code;
* `AIS_USE_HTTPS` takes precedence over `net.http.use_https` value from the [cluster configuration](/docs/configuration.md),

and so on.

### Table of Contents

The remainder of this text groups AIS environment variables by their respective usages, and is structured as follows:

- [Build Tags](#build-tags)
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

separately, there's authentication server config:
- [AuthN](#authn)

and finally:
- [References](#references)


## Build Tags

Different AIS builds may (or may not) require different environment vars. For complete list of supported build tags, please see [conditional linkage](/docs/build_tags.md). Here's a very brief and non-exhaustive intro:

```console
# 1) no build tags, no debug
MODE="" make node

# 2) no build tags, debug
MODE="debug" make node

# 3) cloud backends, no debug
AIS_BACKEND_PROVIDERS="aws azure gcp" MODE="" make node

# 4) cloud backends, debug
AIS_BACKEND_PROVIDERS="aws azure gcp" MODE="debug" make node

# 5) cloud backends, debug
TAGS="aws azure gcp debug" make node

# 6) debug, nethttp (note that fasthttp is used by default)
TAGS="nethttp debug" make node
```

## Primary

Background: in a running AIS cluster, at any point in time there's a single _primary_ gateway that may also be administratively selected, elected, reelected. Hence, two related variables:

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

At first it may sound slightly confusing, but HTTP-wise AIS is both a client and a server.

All nodes in a cluster talk to each other using HTTP (or HTTPS) - the fact that inevitably implies a certain client-side configuration (and configurability).

In particular, AIS server-side HTTPS environment includes:

| name | comment |
| ---- | ------- |
| `AIS_USE_HTTPS`       | tells AIS to run HTTPS transport (both public and intra-cluster networks)                                |
| `AIS_SERVER_CRT`      | TLS certificate (pathname). Required when `AIS_USE_HTTPS` is `true`                                          |
| `AIS_SERVER_KEY`      | private key (pathname) for the certificate above.                                                            |
| `AIS_SKIP_VERIFY_CRT` | when true will skip X.509 cert verification (usually enabled to circumvent limitations of self-signed certs) |

> E.g., for local playground, typical usage starts from running `export AIS_USE_HTTPS=true` followed by one of the usual `make deploy` combinations.

In addition, all embedded (intra-cluster) clients in a cluster utilize the following environment:

| name | comment |
| ---- | ------- |
| `AIS_CRT`             | TLS certificate pathname (this and the rest variables in the table are ignored when AIS is AIS_USE_HTTPS==false |
| `AIS_CRT_KEY`         | pathname that contains X.509 certificate private key |
| `AIS_CLIENT_CA`       | certificate authority that authorized (signed) the certificate |
| `AIS_SKIP_VERIFY_CRT` | when true will skip X.509 cert verification (usually enabled to circumvent limitations of self-signed certs) |

### Further references

- [Generating self-signed certificates](/docs/https.md#generating-self-signed-certificates)
- [Deploying: 4 targets, 1 gateway, 6 mountpaths, AWS backend](/docs/https.md#deploying-4-targets-1-gateway-6-mountpaths-aws-backend)
- [Accessing HTTPS-based cluster](/docs/https.md#accessing-https-based-cluster)
- [Testing with self-signed certificates](/docs/https.md#testing-with-self-signed-certificates)
- [Observability: TLS related alerts]((/docs/https.md#observability-tls-related-alerts)
- [Updating and reloading X.509 certificates](/docs/https.md#updating-and-reloading-x509-certificates)
- [Switching cluster between HTTP and HTTPS](/docs/https.md#switching-cluster-between-http-and-https)

## Local Playground

This group of environment variables is used exclusively by development scripts and integration tests.

| name         | comment |
|--------------|---------|
| `NUM_TARGET` | number of targets in a test cluster |
| `NUM_PROXY`  | number of proxies (gateways) in a test cluster |
| `NUM_CHUNKS` | when greater than zero, specifies the number of chunks each new PUT operation will produce |
| `SIGN_HMAC`  | when "true", enables HMAC signing and validation of all HTTP redirects |

See also:
* [scripts/clean_deploy.sh](https://github.com/NVIDIA/aistore/blob/main/scripts/clean_deploy.sh)
* [wait-for-cluster](https://github.com/NVIDIA/aistore/blob/main/ais/test/main_test.go#L47-L56)
* [api/env package readme](https://github.com/NVIDIA/aistore/blob/main/api/env/README.md)

## Kubernetes

| name | comment |
| ---- | ------- |
| `MY_POD` and `HOSTNAME` | Kubernetes POD name. `MY_POD` is used in [production](https://github.com/NVIDIA/ais-k8s/blob/main/operator/pkg/resources/cmn/env.go); `HOSTNAME`, on the other hand, is usually considered a Kubernetes default |
| `MY_NODE` | Kubernetes node name |
| `K8S_NS` and `POD_NAMESPACE` | Kubernetes namespace. `K8S_NS` is used in [production](https://github.com/NVIDIA/ais-k8s/blob/main/operator/pkg/resources/cmn/env.go), while `POD_NAMESPACE` - development |

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

| name | comment                                                                                                   |
| ---- |-----------------------------------------------------------------------------------------------------------|
| `S3_ENDPOINT` | global S3 endpoint to be used instead of `s3.amazonaws.com`                                               |
| `AWS_REGION` | default bucket region; can be set to override the global default 'us-east-1' location                     |
| `AWS_PROFILE` | global AWS profile with alternative (as far as the [default]) credentials and/or AWS region               |
| `AIS_S3_CONFIG_DIR` | directory containing any number of AWS `config` and `credentials` files to be loaded by the AIS S3 client |
## Package: backend

AIS natively supports 3 (three) [Cloud storages](/docs/providers.md).

The corresponding environment "belongs" to the internal [backend](https://github.com/NVIDIA/aistore/tree/main/ais/backend) package and includes:

| name                                                                                                            | comment |
|-----------------------------------------------------------------------------------------------------------------| ------- |
| `S3_ENDPOINT`, `AWS_PROFILE`, `AWS_REGION`, `AIS_S3_CONFIG_DIR`                                                 | see previous section |
| `GOOGLE_CLOUD_PROJECT`, `GOOGLE_APPLICATION_CREDENTIALS`                                                        | GCP account with permissions to access Google Cloud Storage buckets |
| `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY`                                                                    | Azure account with  permissions to access Blob Storage containers |
| `AIS_AZURE_URL`                                                                                                 | Azure endpoint, e.g. `http://<account_name>.blob.core.windows.net` |
| `OCI_TENANCY_OCID`, `OCI_USER_OCID`, `OCI_REGION`, `OCI_FINGERPRINT`, `OCI_PRIVATE_KEY`, `OCI_COMPARTMENT_OCID` | OCI account with permissions to access Object Storage buckets and compartments |

Notice in the table above that the variables `S3_ENDPOINT` and `AWS_PROFILE` are designated as _global_: cluster-wide.

The implication: it is possible to override one or both of them on a **per-bucket basis**:

* [configuring buckets with alternative S3 andpoint and/or credentials](/docs/cli/aws_profile_endpoint.md)

### AIS as S3 storage

Environment `S3_ENDPOINT` is _important_, and may also be a source of minor confusion. The reason: AIS itself provides S3 compatible interface.

For instance, on the client side you could say something like:

```console
export S3_ENDPOINT=https://10.0.4.53:51080/s3
```

and then run existing S3 applications against an AIS cluster at `10.0.4.53` - with no changes (to the application).

Moreover, configure AIS to handle S3 requests at its "/" root:

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

* you could run existing S3 apps (with no changes) against AIS by using `S3_ENDPOINT` on the client side

See also:
* [AIS buckets](/docs/cli/bucket.md)
* [Bucket configuration: AWS profiles](/docs/cli/aws_profile_endpoint.md)
* [Using AIS as S3 endpoint](/docs/s3compat.md)

## Package: stats

AIStore is a fully compliant [Prometheus exporter](https://prometheus.io/docs/instrumenting/writing_exporters/).

> StatsD was deprecated in v3.28 (Spring 2025) and completely removed in v4.0 (September 2025).

## Package: memsys

| name | comment |
| ---- | ------- |
| `AIS_MINMEM_FREE` | for details, see [Memory Manager, Slab Allocator (MMSA)](https://github.com/NVIDIA/aistore/blob/main/memsys/README.md) |
| `AIS_MINMEM_PCT_TOTAL` | same as above and, specifically, the section "Minimum Available Memory" |
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

| Variable               | Default Value    | Description                                                                              |
|------------------------|------------------|------------------------------------------------------------------------------------------|
| `AIS_AUTHN_ENABLED`    | `false`          | Enable AuthN server and token-based access in AIStore proxy (`true` to enable)           |
| `AIS_AUTHN_PORT`       | `52001`          | Port on which AuthN listens to requests                                                  |
| `AIS_AUTHN_TTL`        | `24h`            | Token expiration time. Can be set to `0` for no expiration                               |
| `AIS_AUTHN_USE_HTTPS`  | `false`          | Enable HTTPS for AuthN server. If `true`, requires `AIS_SERVER_CRT` and `AIS_SERVER_KEY` |
| `AIS_SERVER_CRT`       | `""`             | TLS certificate (pathname). Required when `AIS_AUTHN_USE_HTTPS` is `true`                |
| `AIS_SERVER_KEY`       | `""`             | pathname that contains X.509 certificate private key                                     |
| `AIS_AUTHN_SU_NAME`    | `admin`          | Superuser (admin) name for AuthN                                                         |
| `AIS_AUTHN_SU_PASS`    | None -- required | Superuser (admin) password for AuthN                                                     |
| `AIS_AUTHN_SECRET_KEY` | `""`             | Secret key used to sign tokens.                                                          |

Separately, there's also client-side AuthN environment that includes:

| Name                  | Description                                                                                                                          |
|-----------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| `AIS_AUTHN_URL`       | Used by [CLI](./cli/auth.md) to configure and query the authentication server (AuthN).                                            |
| `AIS_AUTHN_TOKEN_FILE`| Token file pathname; can be used to override the default `$HOME/.config/ais/cli/<fname.Token>`.                                      |
| `AIS_AUTHN_TOKEN`     | The JWT token itself (excluding the file and JSON); can be used to specify the token directly, bypassing the need for a token file.  |

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
