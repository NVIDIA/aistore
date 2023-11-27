Following is a brief summary of **all** currently used **environment variables**.

Note the common rules that apply across the board:

* all environment are **optional**
* if specified, environment variable will **override** (ie., take precedence over) the corresponding persistent **configuration** if the latter exists.

### Table of Contents

- [Primary](#primary)
- [Network](#network)
- [Node](#node)
- [HTTPS](#https)
- [Local Playground](#local-playground)
- [Kubernetes](#kubernetes)
- [Package: backend](#package-backend)
- [Package: memsys](#package-memsys)
- [Package: stats](#package-stats)
- [Package: transport](#package-transport)

and separately, authenication server's:
- [AuthN](#authn)

## Primary

| name | comment |
| ---- | ------- |
| `AIS_IS_PRIMARY` | at startup, tells the (starting-up) proxy to assume _primary_ role; e.g. usage: 'export AIS_IS_PRIMARY=true' |
| `AIS_PRIMARY_ID` | at startup, tells _all_ starting-up proxies which one is the _primary_; e.g. usage: 'export AIS_PRIMARY_ID=wfGrerUUk' |

## Network

| name | comment |
| ---- | ------- |
| `AIS_ENDPOINT` | http or https address of an arbitrary AIS gateway (proxy) in a given cluster |
| `AIS_CLUSTER_CIDR` | ais cluster [CIDR](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing); often can be understood/approximated as the cluster's subnet; when specified will be used to differentiate between clients within the same subnet vs outside |
| `AIS_READ_HEADER_TIMEOUT` | maximum time to receive request headers; e.g. usage: 'export AIS_READ_HEADER_TIMEOUT=10s' |

## Node

| name | comment |
| ---- | ------- |
| `AIS_DAEMON_ID` | ais node ID |
| `AIS_HOST_IP` | ais node IPv4 |
| `AIS_HOST_PORT` | ais node TCP port |

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
| `NUM_TARGET` | (development scipts; automation) |
| `NUM_PROXY` | (ditto) |

## Kubernetes

| name | comment |
| ---- | ------- |
| `MY_POD` | POD name |
| `K8S_NODE_NAME` | Kubernetes node name |
| `POD_NAMESPACE` | Kubernetes namespace |

## Package: backend

| name | comment |
| ---- | ------- |
| `S3_ENDPOINT` | e.g. usage: 'S3_ENDPOINT=https://swiftstack.io'; see also [aws profile](/docs/cli/aws_profile_endpoint.md) and [s3 compatibility](/docs/s3compat.md) |
| `AWS_PROFILE` | TODO |
| `GOOGLE_CLOUD_PROJECT` | TODO |
| `GOOGLE_APPLICATION_CREDENTIALS` | TODO |
| `AZURE_STORAGE_ACCOUNT` | TODO |
| `AZURE_STORAGE_KEY` | TODO |
| `AIS_AZURE_URL` | TODO |

## Package: memsys

| name | comment |
| ---- | ------- |
| `AIS_MINMEM_FREE` | TODO |
| `AIS_MINMEM_PCT_TOTAL` | TODO |
| `AIS_MINMEM_PCT_FREE` | TODO |

## Package: stats

| name | comment |
| ---- | ------- |
| `AIS_PROMETHEUS` | TODO |
| `AIS_STATSD_PORT` | TODO |
| `AIS_STATSD_PROBE` | TODO |

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
