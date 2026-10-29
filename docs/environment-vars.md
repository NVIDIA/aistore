# AIStore Environment Variables

## Introduction

AIStore (AIS) operators and developers can use this page to find environment variables and understand their place among configuration sources:

1. Cluster (global) and node (local) configuration, including local overrides of inherited cluster configuration.
2. `aisnode` [command-line options](/docs/command_line.md).
3. Environment variables, covered on this page.
4. Low-level constants (hardcoded defaults) that rarely need to change.

This list excludes bucket configuration. New AIS buckets receive a snapshot of applicable cluster defaults at creation; changing cluster configuration does not update the properties of existing buckets. You can set bucket properties at creation or change them later per bucket.

For additional sources, refer to [References](#references). The rest of this page covers only environment variables, item 3 above.

## Rules

Environment variables have different consumers and precedence rules. Check the setting and deployment path before relying on an override:

* The daemon reads some variables at runtime or startup, such as `AIS_READ_HEADER_TIMEOUT` (instead of the `apc.ReadHeaderTimeout` [constant](https://github.com/NVIDIA/aistore/blob/main/api/apc/const.go)) and `AIS_DAEMON_ID` (a startup node-ID override). These do not establish a general rule for persisted cluster configuration.
* CLI and other client tools read their own environment settings, such as `AIS_ENDPOINT` and client TLS variables. These settings do not change daemon configuration.
* Deployment generators use variables such as `AIS_USE_HTTPS` to create initial configuration. The daemon loads persisted cluster configuration when available, rather than reapplying those generator inputs on restart. To change an existing cluster's HTTPS mode, follow [Switching cluster between HTTP and HTTPS](/docs/switch_https.md).

Some variables are required in particular environments, such as the Kubernetes variables listed below; do not assume every setting is optional.

### Table of Contents

The remainder of this text groups AIS environment variables by their respective usages, and is structured as follows:

- [Build Tags](#build-tags)
- [Primary](#primary)
- [Network](#network)
- [Node](#node)
- [HTTPS](#https)
- [Local playground](#local-playground)
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

For a new local playground deployment, the generator writes the following values into the daemon's initial `net.http` configuration:

| name | initial configuration use |
| ---- | ------------------------- |
| `AIS_USE_HTTPS`       | writes `net.http.use_https` for the public and intra-cluster listeners |
| `AIS_SERVER_CRT`      | writes the server certificate pathname (`net.http.server_crt`) |
| `AIS_SERVER_KEY`      | writes the server private-key pathname (`net.http.server_key`) |
| `AIS_CLIENT_CA_TLS`   | writes `net.http.client_ca_tls` |
| `AIS_CLIENT_AUTH_TLS` | writes `net.http.client_auth_tls` |
| `AIS_SKIP_VERIFY_CRT` | writes `net.http.skip_verify` |

For example, set `AIS_USE_HTTPS=true` before a fresh `make deploy`. These inputs do not override existing persisted `net.http` configuration. Daemon listeners use `net.http` TLS settings, including `client_ca_tls` and `client_auth_tls` for server-side client authentication. Embedded intra-cluster clients also derive their TLS settings from `net.http`, not from `AIS_CRT` or `AIS_CLIENT_CA`. Refer to [Public and intra-cluster TLS configuration](/docs/https.md#public-and-intra-cluster-tls-configuration).

CLI and other tools that call `EnvToTLS` read the following client-side environment variables; they do not configure daemon intra-cluster clients:

| name | client-tool use |
| ---- | --------------- |
| `AIS_CRT`             | client certificate pathname |
| `AIS_CRT_KEY`         | client private-key pathname |
| `AIS_CLIENT_CA`       | certificate authority pathname used by the client tool |
| `AIS_SKIP_VERIFY_CRT` | skips certificate verification for the client tool when set to true |

### Further references

- [Generating self-signed certificates](/docs/https.md#generating-self-signed-certificates)
- [Deploying: 4 targets, 1 gateway, 6 mountpaths, AWS backend](/docs/https.md#deploying-4-targets-1-gateway-6-mountpaths-aws-backend)
- [Accessing HTTPS-based cluster](/docs/https.md#accessing-https-based-cluster)
- [Testing with self-signed certificates](/docs/https.md#testing-with-self-signed-certificates)
- [Observability: TLS related alerts](/docs/https.md#observability-tls-related-alerts)
- [Updating and reloading X.509 certificates](/docs/https.md#updating-and-reloading-x509-certificates)
- [Switching cluster between HTTP and HTTPS](/docs/switch_https.md)

## Local playground

This group of environment variables is used exclusively by development scripts and integration tests.

| name         | comment |
|--------------|---------|
| `AIS_SKIP_IP_DISCOVERY` | when true in a local playground (`test_fspaths.count > 0`), skip interface discovery and use loopback (`127.0.0.1` or `::1`); MTU remains unknown |
| `NUM_TARGET` | number of targets in a test cluster |
| `NUM_PROXY`  | number of proxies (gateways) in a test cluster |
| `NUM_CHUNKS` | when greater than zero, specifies the number of chunks each new PUT operation will produce |
| `SIGN_HMAC`  | when "true", enables HMAC signing and validation of all HTTP redirects |
| `RAND_NS`  | when "true", generate buckets with random namespaces, e.g.: `ais://#ns123/bucket1`, `s3://#ns456/bucket2`, etc.  |
| `AIS_FEATURES` | initial cluster feature flags as a decimal number, e.g.: `268435456` (`1 << 28`, bit 28) sets `Disable-KTLS` (disables kTLS transmit offload) |

See also:
* [scripts/clean_deploy.sh](https://github.com/NVIDIA/aistore/blob/main/scripts/clean_deploy.sh)
* [wait-for-cluster](https://github.com/NVIDIA/aistore/blob/main/ais/test/main_test.go#L47-L56)
* [api/env package readme](https://github.com/NVIDIA/aistore/blob/main/api/env/README.md)

## Kubernetes

| name                    | comment                                                                                                                                                                                                   |
|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `MY_POD` and `HOSTNAME` | Kubernetes POD name. `MY_POD` is used in [production](https://github.com/NVIDIA/ais-k8s); `HOSTNAME`, on the other hand, is usually considered a Kubernetes default. Required in Kubernetes deployments. |
| `MY_NODE`               | Kubernetes node name; injected via the downward API in both [production](https://github.com/NVIDIA/ais-k8s) and development, and required in Kubernetes deployments.                                      |
| `K8S_NS`                | Kubernetes namespace for the in-cluster client; set in [production](https://github.com/NVIDIA/ais-k8s) and development. When unset, the namespace is read from `/var/run/secrets/kubernetes.io/serviceaccount/namespace`, falling back to `default`. |

Kubernetes POD name is also reported via `ais show cluster` CLI - when it is a Kubernetes deployment, e.g.:

```console
$ ais show cluster 't[fXbarEnn]'
TARGET         MEM USED(%)   MEM AVAIL     CAP USED(%)   CAP AVAIL     LOAD AVERAGE    REBALANCE   UPTIME        K8s POD         STATUS  VERSION        BUILD TIME
t[fXbarEnn]    3.08%         367.66GiB     51%           8.414TiB      [0.9 1.1 1.3]   -           1852h19m40s   ais-target-26   online  3.20.92bc0c1   2023-09-18T19:12:52+0000
```

The `aisinit` init container, which generates node configuration in Kubernetes deployments, reads its own variables - for example, `AIS_PUBLIC_HOSTNAME` and `AIS_PUBLIC_HOSTNAME_OVERRIDE` (which, when set, takes precedence over all other public-hostname rules). See [aisinit](https://github.com/NVIDIA/aistore/blob/main/cmd/aisinit/README.md).

See related:
* [AIS K8s Operator: environment variables](https://github.com/NVIDIA/ais-k8s/blob/main/operator/pkg/resources/cmn/env.go)
* [AIS K8s Operator](https://github.com/NVIDIA/ais-k8s)

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

See also: [streaming intra-cluster transport](https://github.com/NVIDIA/aistore/blob/main/transport/README.md).

## AuthN

AIStore Authentication Server (**AuthN**) provides secure access control to AIStore via [JSON Web Tokens](https://datatracker.ietf.org/doc/html/rfc7519).

A single AuthN service can support multiple AIS clusters, with no fixed limit.

The following variables can be used to configure deployment scripts and [validation of tokens](/docs/auth_validation.md) in AIStore:

| Variable                     | Default Value    | Description                                                                              |
|------------------------------|------------------|------------------------------------------------------------------------------------------|
| `AIS_AUTHN_ENABLED`          | `false`          | Enable AuthN server and token-based access in AIStore proxy (`true` to enable)           |
| `AIS_AUTHN_DEPLOY`           | `false`          | Deploy AuthN alongside AIStore in deployment scripts (`false` to use existing service)   |
| `AIS_AUTHN_SECRET_KEY`       | `""`             | HMAC secret key used to sign tokens                                                      |
| `AIS_AUTHN_PUBLIC_KEY`       | `""`             | RSA public key (PEM or base64 DER) for verifying tokens signed by AuthN                  |
| `AIS_AUTHN_ALLOWED_ISS`      | `""`             | Comma-separated list of allowed OIDC issuer URLs for token validation (deploy scripts)   |

For configuring the AuthN service and clients, see the [AuthN Environment Variables](/docs/authn.md#environment-configuration).

When AuthN is disabled (i.e., not used), the respective `ais config` command will show something like:

```console
$ ais config cluster auth
PROPERTY                                         VALUE
auth.signature.key                               **********
auth.signature.method
auth.required_claims.aud                         []
auth.oidc.allowed_iss                            []
auth.oidc.issuer_ca_bundle
auth.oidc.jwks_cache.min_rotation_refresh        0s
auth.oidc.jwks_cache.min_background_refresh      0s
auth.intra_cluster.request_auth                  false ## (intra-cluster requests and redirects _not_ signed)
auth.intra_cluster.node_join_secret_path         ""    ## (node-join authentication not configured)
auth.intra_cluster.ttl                           0s
auth.intra_cluster.nonce_window                  0s
auth.intra_cluster.rotation_grace                0s
auth.client_auth_required                        false ## (client authentication not required)
```

Notice: this command is executed on the AIStore cluster, not AuthN.

See also:

* [AIS token validation](/docs/auth_validation.md)
* [AIS AuthN server](/docs/authn.md)
* [AIS AuthN server: CLI management](/docs/cli/auth.md)

## References

* `env` package [README](https://github.com/NVIDIA/aistore/blob/main/api/env/README.md)
* [system filenames](https://github.com/NVIDIA/aistore/blob/main/cmn/fname/fname.go)
* [cluster configuration](/docs/configuration.md)
* [local playground: cluster and node configuration](https://github.com/NVIDIA/aistore/blob/main/deploy/dev/local/aisnode_config.sh)
* [`ais config` command](/docs/cli/config.md)
* [performance tunables](/docs/performance.md)
* [feature flags](/docs/feature_flags.md)
