## Table of Contents

- [Notation](#notation)
- [Overview](#overview)
- [Easy URL](#easy-url)
- [API Reference](#api-reference)
- [Backend Provider](#backend-provider)
- [Curl Examples](#curl-examples)
- [Querying information](#querying-information)

## Notation

In this README:

> `G` - denotes a (hostname:port) address of a **gateway** (any gateway in a given AIS cluster)

> `T` - (hostname:port) of a storage **target**

> `G-or-T` - (hostname:port) of **any node** member of the cluster

## Overview

AIStore supports a growing number and variety of RESTful operations. To illustrate common conventions, let's take a look at the example:

```console
$ curl -X GET http://G-or-T/v1/daemon?what=config
```

This command queries one of the AIS nodes (denoted as `G-or-T`) for its configuration. The query - as well as most of other control plane queries - results in a JSON-formatted output that can be viewed with any compatible JSON viewer.

Notice the 6 (six) elements of a RESTful operation:

1. **Command line option or flag**

The `-X` (or `--request`) and `-L` (`--location`) options are important. Run `curl --help` for help.

2. **HTTP verb** (aka method): `PUT`, `GET`, `HEAD`, `POST`, `DELETE`, or `PATCH`

For a brief summary of the standard HTTP verbs and their CRUD semantics, see [REST API tutorial](https://www.restapitutorial.com/introduction/httpmethods).

3. **Hostname** (or IPv4 address) and TCP port of one of the AIStore daemons

Most RESTful operations performed on an AIStore proxy/gateway will likely have a *clustered* scope. Exceptions include querying proxy's own configuration via `?what=config`.

> For developers and first-time users: if you deployed AIS locally having followed [these instructions](/README.md#local-non-containerized) then most likely you will have `http://localhost:8080` as the primary proxy, and generally, `http://localhost:808x` for all locally-deployed AIS daemons.

4. **URL path**: version of the REST API, RESTful *resource*, and possibly more forward-slash delimited specifiers

For example: `/v1/cluster` where `v1` is the API version and `cluster` is the resource. Resources include:

| RESTful resource | Description |
| --- | ---|
| `cluster` | cluster-wide control-plane operation |
| `daemon` (aka **node**) | control-plane request to update or query specific AIS daemon (proxy or target) |
| `buckets` | create, destroy, rename, copy, transform buckets; list objects; get bucket names and properties |
| `objects` | datapath request to GET, PUT and DELETE objects, read their properties |
| `download` | download external datasets and/or selected files from remote buckets |
| `sort` | distributed shuffle |

For the most recently updated URL paths, see:

* [`api/apc/urlpaths.go`](https://github.com/NVIDIA/aistore/blob/main/api/apc/urlpaths.go)

5. **URL query**, e.g., `?what=config`

All API requests that operate on a bucket carry the bucket's specification in the URL query, including [backend provider](/docs/providers.md) and [namespace](/docs/providers.md#unified-global-namespace). An empty backend provider indicates an AIS bucket; an empty namespace translates as global (default) namespace.

6. **HTTP request and response headers**

All supported query parameters and HTTP headers are enumerated in:

* [Query parameters](https://github.com/NVIDIA/aistore/blob/main/api/apc/query.go)
* [Headers](https://github.com/NVIDIA/aistore/blob/main/api/apc/headers.go)

## Easy URL

"Easy URL" is a simple alternative mapping of the AIS API to handle URL paths that look as follows:

* **GET http(s)://host:port/provider/[bucket[/object]]**
* **PUT http(s)://host:port/provider/[bucket[/object]]**

This enables convenient usage of your Internet Browser or `curl`. You can use simple intuitive URLs to execute GET, PUT, list-objects, and list-buckets.

| URL | Comment |
|--- | --- |
| `/gs/mybucket/myobject` | read, write, delete, and list objects in Google Cloud buckets |
| `/az/mybucket/myobject` | same, for Azure Blob Storage buckets |
| `/ais/mybucket/myobject` | AIS buckets |

```console
# Example: GET
$ curl -s -L -X GET 'http://aistore/gs/my-google-bucket/abc-train-0001.tar' -o abc-train-0001.tar

  # Using conventional AIS RESTful API, the same operation:
  $ curl -s -L -X GET 'http://aistore/v1/objects/my-google-bucket/abc-train-0001.tar?provider=gs' -o abc-train-0001.tar

# Example: PUT
$ curl -s -L -X PUT 'http://aistore/gs/my-google-bucket/abc-train-9999.tar' -T /tmp/9999.tar

  # Same without "easy URL":
  $ curl -s -L -X PUT 'http://aistore/v1/objects/my-google-bucket/abc-train-9999.tar?provider=gs' -T /tmp/9999.tar

# Example: list-objects
$ curl -s -L -X GET 'http://aistore/gs/my-google-bucket' | jq
```

> AIS provides S3 compatibility via its `/s3` endpoint. [S3 compatibility](/docs/s3compat.md) shall not be confused with "easy URL" mapping.

For more details, see [easy URL readme](/docs/easy_url.md).

## API Reference

For complete API reference documentation, see:

* **[HTTP API Reference](https://aistore.nvidia.com/docs/http-api)** - comprehensive OpenAPI-generated documentation

For programmatic access:

* **[Go API](https://github.com/NVIDIA/aistore/tree/main/api)** - native Go client
* **[Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore/sdk)** - Python client

### Probing liveness and readiness

```console
$ curl -i http://localhost:8080/v1/health
HTTP/1.1 200 OK
Ais-Cluster-Uptime: 295433144686
Ais-Node-Uptime: 310453738871
Date: Tue, 08 Nov 2022 14:11:57 GMT
Content-Length: 0
```

> Note: `http://localhost:8080` address must be understood as a placeholder for an _arbitrary_ AIStore endpoint (`AIS_ENDPOINT`).

Health probe during cluster startup:

```console
$ curl -i http://localhost:8080/v1/health?prr=true
HTTP/1.1 503 Service Unavailable
Ais-Cluster-Uptime: 5578879646
Ais-Node-Uptime: 20603416072
Content-Type: application/json
X-Content-Type-Options: nosniff
Date: Tue, 08 Nov 2022 14:17:59 GMT
Content-Length: 221

{"message":"p[lgGp8080] primary is not ready yet to start rebalance (started=true, starting-up=true)","method":"GET","url_path":"//v1/health","remote_addr":"127.0.0.1:42720","caller":"","node":"p[lgGp8080]","status":503}
```

The `prr=true` query parameter requests an additional check for whether the cluster is ready to rebalance upon membership changes.

## Backend Provider

Any storage bucket that AIS handles may originate in a 3rd party Cloud, in another AIS cluster, or be created in AIS itself. To resolve naming and partition namespace with respect to both physical isolation and QoS, AIS introduces the concept of *provider*.

* [Backend Provider](/docs/providers.md) - an abstraction that allows delineating between "remote" and "local" buckets.

Backend provider is an optional parameter across all AIStore APIs that handle access to user data and bucket configuration (GET, PUT, DELETE, and [Range/List](/docs/batch.md) operations).

See also:
- [Backend Providers](/docs/providers.md)
- [CLI: bucket operations](/docs/cli/bucket.md)
- [CLI: object operations](/docs/cli/object.md)
- [On-Disk Layout](/docs/on_disk_layout.md)

## Curl Examples

> **Note:** The examples below are provided for reference.
> For the most recently updated action names and message formats, see [`api/apc`](https://github.com/NVIDIA/aistore/blob/main/api/apc).
> For authoritative API documentation, see [HTTP API Reference](https://aistore.nvidia.com/docs/http-api).

### Basic operations

```console
# List a given AWS bucket
$ curl -s -L -X GET 'http://G/v1/objects/myS3bucket/myobject?provider=aws'

# Get archived file from a remote tar
$ curl -s -L -X GET 'http://localhost:8080/v1/objects/myGCPbucket/train-1234.tar?provider=gcp&archpath=567.jpg' --output /tmp/567.jpg

# Create bucket
$ curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "create-bck"}' 'http://G/v1/buckets/abc'

# Destroy bucket
$ curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action": "destroy-bck"}' 'http://G/v1/buckets/abc'

# PUT object
$ curl -s -L -X PUT 'http://G/v1/objects/myS3bucket/myobject' -T filenameToUpload

# GET object
$ curl -s -L -X GET 'http://G/v1/objects/myS3bucket/myobject?provider=s3' -o myobject

# Read range
$ curl -s -L -X GET -H 'Range: bytes=1024-1535' 'http://G/v1/objects/myS3bucket/myobject?provider=s3' -o myobject

# Delete object
$ curl -i -X DELETE -L 'http://G/v1/objects/mybucket/myobject'

# HEAD object (get properties)
$ curl -s -L --head 'http://G/v1/objects/mybucket/myobject'

# Check if remote object is cached
$ curl -s -L --head 'http://G/v1/objects/mybucket/myobject?check_cached=true'

# HEAD bucket (get properties)
$ curl -s -L --head 'http://G/v1/buckets/mybucket'
```

### Listing

```console
# List buckets
$ curl -s -L -X GET -H 'Content-Type: application/json' -d '{"action": "list"}' 'http://G/v1/buckets/'

# List only buckets present in the cluster
$ curl -s -L -X GET -H 'Content-Type: application/json' -d '{"action": "list"}' 'http://localhost:8080/v1/buckets?presence=2'

# List objects in bucket
$ curl -X GET -L -H 'Content-Type: application/json' -d '{"action": "list", "value":{"props": "size"}}' 'http://G/v1/buckets/myS3bucket'

# List objects (easy URL)
$ curl -s -L -X GET -H 'Content-Type: application/json' -d '{"action": "list"}' 'http://localhost:8080/ais/abc'

# List only cached objects in a remote bucket
$ curl -s -L -X GET -H 'Content-Type: application/json' -d '{"action": "list", "value": {"flags": "1"}}' 'http://localhost:8080/gs/nv'
```

### Cluster operations

```console
# Get cluster config
$ curl -X GET http://G/v1/cluster?what=config

# Set cluster config
$ curl -i -X PUT 'http://G/v1/cluster/set-config?stats_time=33s&log.loglevel=4'

# Reset cluster config
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "reset-config"}' 'http://G/v1/cluster'

# Shutdown cluster
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "shutdown"}' 'http://G-primary/v1/cluster'

# Decommission cluster
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "decommission"}' 'http://G-primary/v1/cluster'

# Shutdown node
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "shutdown-node", "value": {"sid": "43888:8083"}}' 'http://G/v1/cluster'

# Set primary proxy
$ curl -i -X PUT 'http://G-primary/v1/cluster/proxy/26869:8080'

# Rebalance cluster
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "start", "value": {"kind": "rebalance"}}' 'http://G/v1/cluster'

# Abort rebalance
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "stop", "value": {"kind": "rebalance"}}' 'http://G/v1/cluster'

# Resilver cluster
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "start", "value": {"kind": "resilver"}}' 'http://G/v1/cluster'

# Resilver specific target
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "start", "value": {"kind": "resilver", "node": "43888:8083"}}' 'http://G/v1/cluster'
```

### Node operations

```console
# Get node config
$ curl -X GET http://G-or-T/v1/daemon?what=config

# Set node config
$ curl -i -X PUT 'http://G-or-T/v1/daemon/set-config?stats_time=33s&log.loglevel=4'

# Reset node config
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "reset-config"}' 'http://G-or-T/v1/daemon'
```

### Bucket properties

```console
# Set bucket properties
$ curl -i -X PATCH -H 'Content-Type: application/json' -d '{"action":"set-bprops", "value": {"checksum": {"type": "sha256"}, "mirror": {"enable": true}, "force": false}}' 'http://G/v1/buckets/abc'

# Reset bucket properties
$ curl -i -X PATCH -H 'Content-Type: application/json' -d '{"action":"reset-bprops"}' 'http://G/v1/buckets/abc'

# Tie AIS bucket to remote backend
$ curl -i -X PATCH -H 'Content-Type: application/json' -d '{"action":"set-bprops", "value": {"backend_bck":{"name":"cloud_bucket", "provider":"gcp"}}}' 'http://G/v1/buckets/nnn'
```

### Multi-object operations

```console
# Prefetch list of objects
$ curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"prefetch-listrange", "value":{"objnames":["o1","o2","o3"]}}' 'http://G/v1/buckets/abc'

# Prefetch range of objects
$ curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"prefetch-listrange", "value":{"template":"__tst/test-{1000..2000}"}}' 'http://G/v1/buckets/abc'

# Delete list of objects
$ curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action":"delete-listrange", "value":{"objnames":["o1","o2","o3"]}}' 'http://G/v1/buckets/abc'

# Delete range of objects
$ curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action":"delete-listrange", "value":{"template":"__tst/test-{1000..2000}"}}' 'http://G/v1/buckets/abc'

# Evict list of objects
$ curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action":"evict-listrange", "value":{"objnames":["o1","o2","o3"]}}' 'http://G/v1/buckets/abc'

# Evict range of objects
$ curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action":"evict-listrange", "value":{"template":"__tst/test-{1000..2000}"}}' 'http://G/v1/buckets/abc'

# Evict remote bucket
$ curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action": "evict-remote-bck"}' 'http://G/v1/buckets/myS3bucket'
```

### Storage services

```console
# Configure n-way mirror
$ curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"make-n-copies", "value": 2}' 'http://G/v1/buckets/abc'

# Enable erasure coding
$ curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"ec-encode"}' 'http://G/v1/buckets/abc'
```

### Object operations

```console
# Rename object (ais buckets only)
$ curl -i -X POST -L -H 'Content-Type: application/json' -d '{"action": "rename-obj", "name": "dir2/DDDDDD"}' 'http://G/v1/objects/mybucket/dir1/CCCCCC'

# Promote files from target filesystem
$ curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"promote", "name":"/user/dir", "value": {"target": "234ed78", "trim_prefix": "/user/", "recurs": true, "keep": true}}' 'http://G/v1/buckets/abc'
```

## Querying information

Queries use `GET` with `?what=<...>`. Many operations can target either the entire cluster (`/v1/cluster`) or a specific node (`/v1/daemon`).

| Query | HTTP action | Example |
|--- | --- | ---|
| Cluster map | GET /v1/cluster | `curl -X GET http://G/v1/cluster?what=smap` |
| Cluster map | GET /v1/daemon | `curl -X GET http://G/v1/daemon?what=smap` |
| Node configuration | GET /v1/daemon | `curl -X GET http://G-or-T/v1/daemon?what=config` |
| Remote clusters | GET /v1/cluster | `curl -X GET http://G/v1/cluster?what=remote` |
| Node information | GET /v1/daemon | `curl -X GET http://G-or-T/v1/daemon?what=snode` |
| Node status | GET /v1/daemon | `curl -X GET http://G-or-T/v1/daemon?what=status` |
| Cluster statistics | GET /v1/cluster | `curl -X GET http://G/v1/cluster?what=stats` |
| Node statistics | GET /v1/daemon | `curl -X GET http://T/v1/daemon?what=stats` |
| System info (all nodes) | GET /v1/cluster | `curl -X GET http://G/v1/cluster?what=sysinfo` |
| Node system info | GET /v1/daemon | `curl -X GET http://G-or-T/v1/daemon?what=sysinfo` |
| Node log | GET /v1/daemon | `curl -X GET http://G-or-T/v1/daemon?what=log` |
| BMD (bucket metadata) | GET /v1/daemon | `curl -X GET http://T/v1/daemon?what=bmd` |
| Target mountpaths | GET /v1/daemon | `curl -X GET http://T/v1/daemon?what=mountpaths` |
| All mountpaths | GET /v1/cluster | `curl -X GET http://G/v1/cluster?what=mountpaths` |
| Target IPs | GET /v1/cluster | `curl -X GET http://G/v1/cluster?what=target_ips` |

### Example: querying remote clusters

```console
$ curl -s -L http://localhost:8080/v1/cluster?what=remote | jq
{
  "a": [
    {
      "url": "http://127.0.0.1:11080",
      "alias": "remais",
      "uuid": "cKEuiUYz-l",
      "smap": {
        ...
      }
    }
  ],
  "ver": 3
}
```

### Example: querying node log

```console
$ curl -s -L http://localhost:8081/v1/daemon?what=log | head -20

Started up at 2023/11/08 02:34:35, host ais-target-13, go1.21.4 for linux/amd64
W 02:34:35.701629 config:1238 control and data share one intra-cluster network
I 02:34:35.701785 config:1755 log.dir: "/var/log/ais"; l4.proto: tcp; pub port: 51081; verbosity: 3
...
```

### Example: cluster statistics

```console
$ curl -X GET http://G/v1/cluster?what=stats
```

This causes an intra-cluster broadcast where the requesting proxy consolidates results from all nodes into a JSON output containing proxy and target request counters, per-target capacities, and more.

## See Also

- [HTTP API Reference](https://aistore.nvidia.com/docs/http-api) - OpenAPI documentation
- [AIS CLI](/docs/cli.md) - command-line interface
- [Batch operations](/docs/batch.md)
- [ETL](/docs/etl.md)
