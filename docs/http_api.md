---
layout: post
title: RESTFUL API
permalink: /docs/http-api
redirect_from:
 - /http_api.md/
 - /docs/http_api.md/
---

# Table of Contents

- [Notation](#notation)
- [Overview](#overview)
- [Easy URL](#easy-url)
- [API Reference](#api-reference)
  - [Cluster Operations](#cluster-operations)
  - [Node Operations](#node-operations)
  - [Mountpaths and Disks](#mountpaths-and-disks)
  - [Bucket and Object Operations](#bucket-and-object-operations)
  - [Footnotes](#footnotes)
  - [Storage Services](#storage-services)
  - [Multi-Object Operations](#multi-object-operations)
  - [Working with archives (TAR, TGZ, ZIP)](#working-with-archives-tar-tgz-zip)
  - [Starting, stopping, and querying batch operations (jobs)](#starting-stopping-and-querying-batch-operations-jobs)
- [Backend Provider](#bucket-provider)
- [Curl Examples](#curl-examples)
- [Querying information](#querying-information)
- [Example: querying runtime statistics](#example-querying-runtime-statistics)
- [ETL](#etl)

# Notation

In this README:

> `G` - denotes a (hostname:port) address of a **gateway** (any gateway in a given AIS cluster)

> `T` - (hostname:port) of a storage **target**

> `G-or-T` - (hostname:port) of **any node** member of the cluster

# Overview

AIStore supports a growing number and variety of RESTful operations. To illustrate common conventions, let's take a look at the example:

```console
$ curl -X GET http://G-or-T/v1/daemon?what=config
```

This command queries one of the AIS nodes (denoted as `G-or-T`) for its configuration. The query - as well as most of other control plane queries - results in a JSON-formatted output that can be viewed with any compatible JSON viewer.

Notice the 5 (five) ubiquitous elements in the `curl` command line above:

1. **Option**

In particular, note the '-X` (`--request`) and `-L` (`--location`) options. Run `curl --help` for help.

2. **HTTP verb** aka method, one of: `PUT`, `GET`, `HEAD`, `POST`, `DELETE`, or `PATCH`.

In the example, it's a GET but it can also be POST, PUT, and DELETE. For a brief summary of the standard HTTP verbs and their CRUD semantics, see, for instance, this [REST API tutorial](http://www.restapitutorial.com/lessons/httpmethods.html).

3. **Hostname** (or IPv4 address) and TCP port of one of the AIStore daemons.

Most RESTful operations performed on an AIStore proxy/gateway will likely have a *clustered* scope. Exceptions may include querying proxy's own configuration via `?what=config`, and more.

4. **URL path**: version of the REST API, RESTful *resource* that is operated upon, and possibly more forward-slash delimited specifiers.

For example: /v1/cluster where `v1` is the currently supported API version and `cluster` is the (RESTful) resource. Other *resources* include (but are **not** limited to):

| RESTful resource | Description |
| --- | ---|
| `cluster` | cluster-wide control-plane operation |
| `daemon` (aka **node**) | control-plane request to update or query specific AIS daemon (proxy or target). In the documentation, the terms "daemon" and "node" are used interchangeably. |
| `buckets` | create, destroy, rename, copy, transform (entire) buckets; list objects in a given bucket; get bucket names for a given provider (or all providers); get bucket properties |
| `objects` | datapath request to GET, PUT and DELETE objects, read their properties |
| `download` | download external datasets and/or selected files from remote buckets, HDFS, or even specific HTTP locations |
| `dsort` | user-defined distributed shuffle |

and more.

5. **URL query**, e. g., `?what=config`.

In particular, all API requests that operate on a bucket carry the bucket's specification details in the URL's query. Those details may include [backend provider](providers.md) and [namespace](providers.md#unified-global-namespace) where an empty backend provider indicates an AIS bucket (with AIStore being, effectively, the default provider) while an empty namespace parameter translates as a global (default) namespace. For exact names of the bucket-specifying URL Query parameters, please refer to this [API source](/api/bucket.go).

> Combined, all these elements tell the following story. They specify the most generic action (e.g., GET) and designate the target aka "resource" of this action: e. g., an entire cluster or a given AIS node. Further, they may also include context-specific and query string encoded control message to, for instance, distinguish between getting system statistics (`?what=stats`) versus system configuration (`?what=config`).

> For developers and first-time users: if you deployed AIS locally having followed [these instructions](/README.md#local-non-containerized) then most likely you will have `http://localhost:8080` as the primary proxy, and generally, `http://localhost:808x` for all locally-deployed AIS daemons.

> The reference below is "formulated" in `curl` - i.e., using `curl` command lines. It is possible, however, and often much easier (and, therefore, **preferable**), to execute the same operations using [AIS CLI](/docs/cli.md).

# Easy URL

"Easy URL" (feature) has been added with AIS version 3.7 and is a simple alternative mapping of the AIS API to handle URLs paths that look as follows:

| URL | Comment |
|--- | --- |
| `/gs/mybucket/myobject` | read, write, delete, and list objects in Google Cloud buckets |
| `/az/mybucket/myobject` | same, for Azure Blob Storage buckets |
| `/ais/mybucket/myobject` | AIS buckets |

In other words, "easy URL" is a convenience to provide an intuitive ("easy") RESTful API for the most essential reading and writing operations, e.g.:

```console
# Example: GET
$ curl -L -X GET 'http://aistore/gs/my-google-bucket/abc-train-0001.tar'
# Example: PUT
$ curl -L -X PUT 'http://aistore/gs/my-google-bucket/abc-train-9999.tar -T /tmp/9999.tar'
# Example: LIST
$ curl -L -X GET 'http://aistore/gs/my-google-bucket'
```

**NOTE**:
> Amazon S3 is missing in the list that includes GCP and Azure. The reason for this is that AIS provides S3 compatibility layer via its "/s3" endpoint. [S3 compatibility](https://github.com/NVIDIA/aistore/blob/master/docs/s3compat.md) shall not be confused with a simple alternative URL Path mapping, whereby a path (e.g.) "gs/mybucket/myobject" gets replaced with "v1/objects/mybucket/myobject?provider=gcp" with _no_ other changes to the request and response parameters and components.

# API Reference

The entire AIStore RESTful API is substantial in size. It is also constantly growing, which is why this section is structured as several groups of related APIs.

In addition, the rightmost column references AIS [api](https://github.com/NVIDIA/aistore/tree/master/api) package and the specific Go-based API in it that performs the same **operation**. Needless to say - simply because we use it ourselves across a variety of Go-based clients and apps, the [api](https://github.com/NVIDIA/aistore/tree/master/api) package will always contain the most recently updated version of the API.

In other words, AIS [api](https://github.com/NVIDIA/aistore/tree/master/api) is always current and can be used to lookup the most recently updated version of the RESTful API.

## Cluster Operations

The operations that query cluster-wide information and/or involve or otherwise affect, directly or indirectly, all clustered nodes:

| Operation | HTTP action | Example | Go API |
|--- | --- | ---|--- |
| Add a node to cluster | (to be added) | (to be added) | `api.JoinCluster` |
| Put node in maintenance (that is, safely and temporarily remove the node from the cluster _upon rebalancing_ the node's data between remaining nodes) | (to be added) | (to be added) | `api.StartMaintenance` |
| Take node out of maintenance | (to be added) | (to be added) | `api.StopMaintenance` |
| Decommission a node | (to be added) | (to be added) | `api.Decommission` |
| Decommission entire cluster | PUT {"action": "decommission"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "decommission"}' 'http://G-primary/v1/cluster'` | `api.DecommissionCluster` |
| Shutdown ais node | PUT {"action": "shutdown_node", "value": {"sid": daemonID}} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "shutdown_node", "value": {"sid": "43888:8083"}}' 'http://G/v1/cluster'` | `api.ShutdownNode` |
| Decommission entire cluster | PUT {"action": "decommission"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "decommission"}' 'http://G-primary/v1/cluster'` | `api.DecommissionCluster` |
| Query cluster health | (to be added) | (to be added) | `api.Health` |
| Set primary proxy | PUT /v1/cluster/proxy/new primary-proxy-id | `curl -i -X PUT 'http://G-primary/v1/cluster/proxy/26869:8080'` | `api.SetPrimaryProxy` |
| Force-Set primary proxy (NOTE: advanced usage only!) | PUT /v1/daemon/proxy/proxyID | `curl -i -X PUT -G 'http://G-primary/v1/daemon/proxy/23ef189ed'  --data-urlencode "frc=true" --data-urlencode "can=http://G-new-designated-primary"` <sup id="a6">[6](#ft6)</sup>| `api.SetPrimaryProxy` |
| Get cluster configuration | (to be added) | (to be added) | `api.GetClusterConfig` |
| Get `BMD` | (to be added) | (to be added) | `api.GetBMD` |
| Set cluster-wide configuration **via JSON message** (proxy) | PUT {"action": "setconfig", "name": "some-name", "value": "other-value"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "setconfig","name": "stats_time", "value": "1s"}' 'http://G/v1/cluster'`<br>• Note below the alternative way to update cluster configuration<br>• For the list of named options, see [runtime configuration](configuration.md#runtime-configuration) | `api.SetClusterConfigUsingMsg` |
| Set cluster-wide configuration **via URL query** | PUT /v1/cluster/setconfig/?name1=value1&name2=value2&... | `curl -i -X PUT 'http://G/v1/cluster/setconfig?stats_time=33s&log.loglevel=4'`<br>• Allows to update multiple values in one shot<br>• For the list of named configuration options, see [runtime configuration](configuration.md#runtime-configuration) | `api.SetClusterConfig` |
| Reset cluster-wide configuration | PUT {"action": "resetconfig"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "resetconfig"}' 'http://G/v1/cluster'` | `api.ResetClusterConfig` |
| Shutdown cluster | PUT {"action": "shutdown"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "shutdown"}' 'http://G-primary/v1/cluster'` | `api.ShutdownCluster` |
| Rebalance cluster | PUT {"action": "start", "value": {"kind": "rebalance"}} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "start", "value": {"kind": "rebalance"}}' 'http://G/v1/cluster'` | `api.StartXaction` |
| Resilver cluster | PUT {"action": "start", "value": {"kind": "resilver"}} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "start", "value": {"kind": "resilver"}}' 'http://G/v1/cluster'` | `api.StartXaction` |
| Abort global (automated or manually started) rebalance (proxy) | PUT {"action": "stop", "value": {"kind": "rebalance"}} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "stop", "value": {"kind": "rebalance"}}' 'http://G/v1/cluster'` |  |
| Unregister storage target (NOTE: advanced usage only - use Maintenance API instead!) | DELETE /v1/cluster/daemon/daemonID | `curl -i -X DELETE 'http://G/v1/cluster/daemon/15205:8083'` | n/a |
| Register storage target (NOTE: advanced usage only - use JoinCluster API instead!)| POST /v1/cluster/register | `curl -i -X POST -H 'Content-Type: application/json' -d '{"daemon_type": "target", "node_ip_addr": "172.16.175.41", "daemon_port": "8083", "direct_url": "http://172.16.175.41:8083"}' 'http://localhost:8083/v1/cluster/register'` | n/a |
| Register proxy (aka "gateway") | POST /v1/cluster/register | `curl -i -X POST -H 'Content-Type: application/json' -d '{"daemon_type": "proxy", "node_ip_addr": "172.16.175.41", "daemon_port": "8083", "direct_url": "http://172.16.175.41:8083"}' 'http://localhost:8083/v1/cluster/register'` | n/a |
| Get Cluster Map | (to be added) | (to be added) | `api.GetClusterMap` |
| Get Cluster Map from a specific node (any node in the cluster) | (to be added) | (to be added) | `api.GetNodeClusterMap` |
| Get Cluster System information | (to be added) | (to be added) | `api.GetClusterSysInfo` |
| Get Cluster statistics | (to be added) | (to be added) | `api.GetClusterStats` |
| Get remote AIS-cluster information (access URL, primary gateway, cluster map version, and more) | (to be added) | (to be added) | `api.GetRemoteAIS` |
| Attach remote AIS cluster | (to be added) | (to be added) | `api.AttachRemoteAIS` |
| Detach remote AIS cluster | (to be added) | (to be added) | `api.DetachRemoteAIS` |

## Node Operations

The operations that are limited in scope to a single specified node and that usually require node ID:

| Operation | HTTP action | Example | Go API |
|--- | --- | ---|--- |
| Resilver storage target | PUT {"action": "start", "value": {"kind": "resilver", "node": targetID}} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "start", "value": {"kind": "resilver", "node": "43888:8083"}}' 'http://G/v1/cluster'` | `api.StartXaction` |
| Get target IO (aka disk) statistics | (to be added) | (to be added) | `api.GetTargetDiskStats` |
| Get node log | (to be added) | (to be added) | `api.GetDaemonLog` |
| Get node status | (to be added) | (to be added) | `api.GetDaemonStatus` |
| Get node config | (to be added) | (to be added) | `api.GetDaemonConfig` |
| Set node configuration | PUT /v1/daemon/setconfig/?name1=value1&name2=value2&... | `curl -i -X PUT 'http://G-or-T/v1/daemon/setconfig?stats_time=33s&log.loglevel=4'`<br>• Allows to update multiple values in one shot<br>• For the list of named configuration options, see [runtime configuration](configuration.md#runtime-configuration) | `api.SetDaemonConfig` |
| Reset node configuration | PUT {"action": "resetconfig"} /v1/daemon | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "resetconfig"}' 'http://G-or-T/v1/daemon'` | `api.ResetDaemonConfig` |
| Get target IO (aka disk) statistics | (to be added) | (to be added) | `api.GetTargetDiskStats` |
| Set (i.e., update) node config | (to be added) | (to be added) | `api.SetDaemonConfig` |
| Reset AIS node configuration | PUT {"action": "resetconfig"} /v1/daemon | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "resetconfig"}' 'http://G-or-T/v1/daemon'` | `api.ResetDaemonConfig` |

## Mountpaths and Disks

Special subset of node operations (see previous section) to manage disks attached to specific storage target. The corresponding AIS abstraction is called [mountpath](overview.md#terminology).

These APIs also require specific node ID (to identify the target in the cluster to operate on):

| Operation | HTTP action | Example | Go API |
|--- | --- | ---|--- |
| Get target's mountpath info | (to be added) | (to be added) | `api.GetMountpaths` |
| Attach mountpath | (to be added) | (to be added) | `api.AddMountpath` |
| Remove mountpath | (to be added) | (to be added) | `api.RemoveMountpath` |
| Enable mountpath | (to be added) | (to be added) | `api.EnableMountpath` |
| Disable mountpath | (to be added) | (to be added) | `api.DisableMountpath` |

## Bucket and Object Operations

| Operation | HTTP action | Example | Go API |
|--- | --- | ---|--- |
| Get [bucket](bucket.md) names (that is, list all or selected buckets that AIS can "see") | GET /v1/buckets/\* | `curl -X GET 'http://G/v1/buckets/*'` | `api.ListBuckets` |
| Get bucket summaries | (to be added) | (to be added) | `api.GetBucketsSummaries` |
| Check whether bucket exists | (to be added) | (to be added) | `api.DoesBucketExist` |
| Create [bucket](bucket.md) | POST {"action": "create_bck"} /v1/buckets/bucket-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "create_bck"}' 'http://G/v1/buckets/abc'` | `api.CreateBucket` |
| Destroy [bucket](bucket.md) | DELETE {"action": "destroy_bck"} /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action": "destroy_bck"}' 'http://G/v1/buckets/abc'` | `api.DestroyBucket` |
| Rename ais [bucket](bucket.md) | POST {"action": "move_bck"} /v1/buckets/from-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "move_bck" }' 'http://G/v1/buckets/from-name?bck=<bck>&bckto=<to-bck>'` | `api.RenameBucket` |
| Copy [bucket](bucket.md) | POST {"action": "copybck"} /v1/buckets/from-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "copybck", }}}' 'http://G/v1/buckets/from-name?bck=<bck>&bckto=<to-bck>'` | `api.CopyBucket` |
| Rename/move object (ais buckets only) | POST {"action": "rename", "name": new-name} /v1/objects/bucket-name/object-name | `curl -i -X POST -L -H 'Content-Type: application/json' -d '{"action": "rename", "name": "dir2/DDDDDD"}' 'http://G/v1/objects/mybucket/dir1/CCCCCC'` <sup id="a3">[3](#ft3)</sup> | `api.RenameObject` |
| Check if an object from a remote bucket *is present*  | HEAD /v1/objects/bucket-name/object-name | `curl -L --head 'http://G/v1/objects/mybucket/myobject?check_cached=true'` | `api.HeadObject` |
| GET object | GET /v1/objects/bucket-name/object-name | `curl -L -X GET 'http://G/v1/objects/myS3bucket/myobject' -o myobject` <sup id="a1">[1](#ft1)</sup> | `api.GetObject`, `api.GetObjectWithValidation`, `api.GetObjectReader`, `api.GetObjectWithResp` |
| Read range | GET /v1/objects/bucket-name/object-name | `curl -L -X GET -H 'Range: bytes=1024-1535' 'http://G/v1/objects/myS3bucket/myobject' -o myobject`<br> Note: For more information about the HTTP Range header, see [this](https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35)  | `` |
| List objects in a given [bucket](bucket.md) | GET {"action": "list", "value": { properties-and-options... }} /v1/buckets/bucket-name | `curl -X GET -L -H 'Content-Type: application/json' -d '{"action": "listobj", "value":{"props": "size"}}' 'http://G/v1/buckets/myS3bucket'` <sup id="a2">[2](#ft2)</sup> | `api.ListObjects` (see also `api.ListObjectsPage`) |
| Get [bucket properties](bucket.md#bucket-properties) | HEAD /v1/buckets/bucket-name | `curl -L --head 'http://G/v1/buckets/mybucket'` | `api.HeadBucket` |
| Get object props | HEAD /v1/objects/bucket-name/object-name | `curl -L --head 'http://G/v1/objects/mybucket/myobject'` | `api.HeadObject` |
| Set object's custom (user-defined) properties | (to be added) | (to be added) | `api.SetObjectCustomProps` |
| PUT object | PUT /v1/objects/bucket-name/object-name | `curl -L -X PUT 'http://G/v1/objects/myS3bucket/myobject' -T filenameToUpload` | `api.PutObject` |
| APPEND to object | PUT /v1/objects/bucket-name/object-name?appendty=append&handle= | `curl -L -X PUT 'http://G/v1/objects/myS3bucket/myobject?appendty=append&handle=' -T filenameToUpload-partN`  <sup>[8](#ft8)</sup> | `api.AppendObject` |
| Finalize APPEND | PUT /v1/objects/bucket-name/object-name?appendty=flush&handle=obj-handle | `curl -L -X PUT 'http://G/v1/objects/myS3bucket/myobject?appendty=flush&handle=obj-handle'`  <sup>[8](#ft8)</sup> | `api.FlushObject` |
| Delete object | DELETE /v1/objects/bucket-name/object-name | `curl -i -X DELETE -L 'http://G/v1/objects/mybucket/myobject'` | `api.DeleteObject` |
| Set [bucket properties](bucket.md#bucket-properties) (proxy) | PATCH {"action": "setbprops"} /v1/buckets/bucket-name | `curl -i -X PATCH -H 'Content-Type: application/json' -d '{"action":"setbprops", "value": {"checksum": {"type": "sha256"}, "mirror": {"enable": true}, "force": false}' 'http://G/v1/buckets/abc'`  <sup>[9](#ft9)</sup> | `api.SetBucketProps` |
| Reset [bucket properties](bucket.md#bucket-properties) (proxy) | PATCH {"action": "resetbprops"} /v1/buckets/bucket-name | `curl -i -X PATCH -H 'Content-Type: application/json' -d '{"action":"resetbprops"}' 'http://G/v1/buckets/abc'` | `api.ResetBucketProps` |
| [Evict](bucket.md#prefetchevict-objects) object | DELETE '{"action": "evictobj"}' /v1/objects/bucket-name/object-name | `curl -i -X DELETE -L -H 'Content-Type: application/json' -d '{"action": "evictobj"}' 'http://G/v1/objects/mybucket/myobject'` | `api.EvictObject` |
| [Evict](bucket.md#evict-bucket) remote bucket | DELETE {"action": "evictcb"} /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action": "evictcb"}' 'http://G/v1/buckets/myS3bucket'` | `api.EvictRemoteBucket` |
| Promote file or directory | POST {"action": "promote", "name": "/home/user/dirname", "value": {"target": "234ed78", "recurs": true, "keep": true}} /v1/buckets/bucket-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"promote", "name":"/user/dir", "value": {"target": "234ed78", "trim_prefix": "/user/", "recurs": true, "keep": true} }' 'http://G/v1/buckets/abc'` <sup>[7](#ft7)</sup>| `api.PromoteFileOrDir` |

## Footnotes

<a name="ft1">1</a>) This will fetch the object "myS3object" from the bucket "myS3bucket". Notice the -L - this option must be used in all AIStore supported commands that read or write data - usually via the URL path /v1/objects/. For more on the -L and other useful options, see [Everything curl: HTTP redirect](https://ec.haxx.se/http-redirects.html).

<a name="ft2">2</a>) See the [List Objects section](bucket.md#list-objects) for details. [↩](#a2)

<a name="ft3">3</a>) Notice the -L option here and elsewhere. [↩](#a3)

<a name="ft4">4</a>) See the [List/Range Operations section](batch.md#listrange-operations) for details.

<a name="ft5">5</a>) The request returns an HTTP status code 204 if the mountpath is already enabled/disabled or 404 if mountpath was not found.

<a name="ft6">6</a>) Advanced usage only. Use it to reassign the primary *role* administratively or if a cluster ever gets in a so-called [split-brain mode](https://en.wikipedia.org/wiki/Split-brain_(computing)). [↩](#a6)

<a name="ft7">7</a>) The request promotes files to objects; note that the files must be present inside AIStore targets and be referenceable via local directories or fully qualified names. The example request promotes recursively all files of a directory `/user/dir` that is on the target with ID `234ed78` to objects of a bucket `abc`. As `trim_prefix` is set, the names of objects are the file paths with the base trimmed: `dir/file1`, `dir/file2`, `dir/subdir/file3` etc.

<a name="ft8">8</a>) When putting the first part of an object, `handle` value must be empty string or omitted. On success, the first request returns an object handle. The subsequent `AppendObject` and `FlushObject` requests must pass the handle to the API calls. The object gets accessible and appears in a bucket only after `FlushObject` is done.

<a name="ft9">9</a>) Use option `"force": true` to ignore non-critical errors. E.g, to modify `ec.objsize_limit` when EC is already enabled, or to enable EC if the number of target is less than `ec.data_slices + ec.parity_slices + 1`.

## Storage Services

| Operation | HTTP action | Example | Go API |
|--- | --- | ---|--- |
| Erasure code entire bucket | (to be added) | (to be added) | `api.ECEncodeBucket` |
| Configure bucket as [n-way mirror](storage_svcs.md#n-way-mirror) | POST {"action": "makencopies", "value": n} /v1/buckets/bucket-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"makencopies", "value": 2}' 'http://G/v1/buckets/abc'` | `api.MakeNCopies` |
| Enable [erasure coding](storage_svcs.md#erasure-coding) protection for all objects (proxy) | POST {"action": "ecencode"} /v1/buckets/bucket-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"ecencode"}' 'http://G/v1/buckets/abc'` | (to be added) |

## Multi-Object Operations

| Operation | HTTP action | Example | Go API |
|--- | --- | ---|--- |
| [Prefetch](bucket.md#prefetchevict-objects) a list of objects | POST '{"action":"prefetch", "value":{"objnames":"[o1[,o]]"}}' /v1/buckets/bucket-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"prefetch", "value":{"objnames":["o1","o2","o3"]}}' 'http://G/v1/buckets/abc'` <sup>[4](#ft4)</sup> | `api.PrefetchList` |
| [Prefetch](bucket.md#prefetchevict-objects) a range of objects| POST '{"action":"prefetch", "value":{"template":"your-prefix{min..max}" }}' /v1/buckets/bucket-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"prefetch", "value":{"template":"__tst/test-{1000..2000}"}}' 'http://G/v1/buckets/abc'` <sup>[4](#ft4)</sup> | `api.PrefetchRange` |
| Delete a list of objects | DELETE '{"action":"delete", "value":{"objnames":"[o1[,o]]"}}' /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action":"delete", "value":{"objnames":["o1","o2","o3"]}}' 'http://G/v1/buckets/abc'` <sup>[4](#ft4)</sup> | `api.DeleteList` |
| Delete a range of objects | DELETE '{"action":"delete", "value":{"template":"your-prefix{min..max}"}}' /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action":"delete", "value":{"template":"__tst/test-{1000..2000}"}}' 'http://G/v1/buckets/abc'` <sup>[4](#ft4)</sup> | `api.DeleteRange` |
| | (to be added) | (to be added) | |
| [Evict](bucket.md#prefetchevict-objects) a list of objects | DELETE '{"action":"evictobj", "value":{"objnames":"[o1[,o]]"}}' /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action":"evictobj", "value":{"objnames":["o1","o2","o3"]}}' 'http://G/v1/buckets/abc'` <sup>[4](#ft4)</sup> | `api.EvictList` |
| [Evict](bucket.md#prefetchevict-objects) a range of objects| DELETE '{"action":"evictobj", "value":{"template":"your-prefix{min..max}"}}' /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action":"evictobj", "value":{"template":"__tst/test-{1000..2000}"}}' 'http://G/v1/buckets/abc'` <sup>[4](#ft4)</sup> | `api.EvictRange` |
| Copy multiple objects from bucket to bucket | (to be added) | (to be added) | `api.CopyMultiObj` |
| Copy and, simultaneously, transform multiple objects (i.e., perform user-defined offline transformation) | (to be added) | (to be added) | `api.ETLMultiObj` |

## Working with archives (TAR, TGZ, ZIP)

| Operation | HTTP action | Example | Go API |
|--- | --- | ---|--- |
| Create archive | (to be added) | (to be added) | `api.CreateArchMultiObj` |
| Append to existing archive | (to be added) | (to be added) | `api.CreateArchMultiObj` |

## Starting, stopping, and querying batch operations (jobs)

The term we use in the code and elsewhere is [xaction](overview.md#terminology) - a shortcut for *eXtended action*. For definition and further references, see:

* [Terminology](overview.md#terminology)
* [Batch operations](batch.md)

| Operation | HTTP action | Example | Go API |
|--- | --- | ---|--- |
| Start xaction | (to be added) | (to be added) | `api.StartXaction` |
| Abort xaction | (to be added) | (to be added) | `api.AbortXaction` |
| Get xaction stats by ID | (to be added) | (to be added) | `api.GetXactionStatsByID` |
| Query xaction stats | (to be added) | (to be added) | `api.QueryXactionStats` |
| Get xaction status | (to be added) | (to be added) | `api.GetXactionStatus` |
| Wait for xaction to finish | (to be added) | (to be added) | `api.WaitForXaction` |
| Wait for xaction to become idle | (to be added) | (to be added) | `api.WaitForXactionIdle` |

# Backend Provider

Any storage bucket that AIS handles may originate in a 3rd party Cloud, or in another AIS cluster, or - the 3rd option - be created (and subsequently filled-in) in the AIS itself. But what if there's a pair of buckets, a Cloud-based and, separately, an AIS bucket that happen to share the same name? To resolve all potential naming, and (arguably, more importantly) partition namespace with respect to both physical isolation and QoS, AIS introduces the concept of *provider*.

* [Backend Provider](providers.md) - an abstraction, and simultaneously an API-supported option, that allows to delineate between "remote" and "local" buckets with respect to a given AIS cluster.

> Backend provider is realized as an optional parameter across all AIStore APIs that handle access to user data and bucket configuration. The list (of those APIs) includes GET, PUT, DELETE and [Range/List](batch.md) operations. For supported backend providers, please refer to [Providers](providers.md) and/or [Buckets: introduction and detailed overview](bucket.md) documents.

For even more information, CLI examples, and the most recent updates, please see:
- [Backend Providers](providers.md)
- [CLI: operations on buckets](/docs/cli/bucket.md)
- [CLI: operations on objects](/docs/cli/object.md)
- [On-Disk Layout](on_disk_layout.md)

# Curl Examples

```console
# List a given AWS bucket
$ curl -L -X GET 'http://G/v1/objects/myS3bucket/myobject?provider=aws'

# Using locally deployed AIS, get archived file from a remote named tar:
$ curl -L -X GET 'http://localhost:8080/v1/objects/myGCPbucket/train-1234.tar?provider=gcp&archpath=567.jpg' --output /tmp/567.jpg
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   201  100   201    0     0   196k      0 --:--:-- --:--:-- --:--:--  196k
100 44327  100 44327    0     0  2404k      0 --:--:-- --:--:-- --:--:-- 2404k
$ file /tmp/567.jpg
/tmp/567.jpg: JPEG image data, JFIF standard 1.01, aspect ratio, density 1x1, segment length 16, baseline, precision 8, 294x312, frames 3
```

# Querying information

AIStore provides an extensive list of RESTful operations to retrieve cluster current state:

| Operation                                                   | HTTP action                     | Example                                                      |
| ----------------------------------------------------------- | ------------------------------- | ------------------------------------------------------------ |
| Get cluster map                                             | GET /v1/daemon                  | `curl -X GET http://G/v1/daemon?what=smap`                   |
| Get proxy/target configuration                              | GET /v1/daemon                  | `curl -X GET http://G-or-T/v1/daemon?what=config`            |
| Get proxy/target snode                                      | GET /v1/daemon                  | `curl -X GET http://G-or-T/v1/daemon?what=snode`             |
| Get proxy/target status                                     | GET /v1/daemon                  | `curl -X GET http://G-or-T/v1/daemon?what=status`            |
| Get cluster statistics (proxy)                              | GET /v1/cluster                 | `curl -X GET http://G/v1/cluster?what=stats`                 |
| Get target statistics                                       | GET /v1/daemon                  | `curl -X GET http://T/v1/daemon?what=stats`                  |
| Get process info for all nodes in cluster (proxy)           | GET /v1/cluster                 | `curl -X GET http://G/v1/cluster?what=sysinfo`               |
| Get proxy/target system info                                | GET /v1/daemon                  | `curl -X GET http://G-or-T/v1/daemon?what=sysinfo`           |
| Get xactions' statistics (proxy) [More](/xaction/README.md) | GET /v1/cluster                 | `curl -i -X GET  -H 'Content-Type: application/json' -d '{"action": "stats", "name": "xactionname", "value":{"bucket":"bckname"}}' 'http://G/v1/cluster?what=xaction'` |
| Get list of target's filesystems (target)                   | GET /v1/daemon?what=mountpaths  | `curl -X GET http://T/v1/daemon?what=mountpaths`             |
| Get list of all targets' filesystems (proxy)                | GET /v1/cluster?what=mountpaths | `curl -X GET http://G/v1/cluster?what=mountpaths`            |
| Get bucket list from a given target                         | GET /v1/daemon                  | `curl -X GET http://T/v1/daemon?what=bucketmd`               |
| Get IPs of all targets                                      | GET /v1/cluster                 | `curl -X GET http://G/v1/cluster?what=target_ips`            |

## Example: querying runtime statistics

```console
$ curl -X GET http://G/v1/cluster?what=stats
```

This single command causes execution of multiple `GET ?what=stats` requests within the AIStore cluster, and results in a JSON-formatted consolidated output that contains both http proxy and storage targets request counters, as well as per-target used/available capacities. For example:

**NOTE:** the picture shown below is **outdated**.

![AIStore statistics](images/ais-get-stats.png)

More usage examples can be found in the [README that describes AIS configuration](configuration.md).

# ETL

For API Reference of ETL please refer to [ETL Readme](etl.md#api-reference)





<a name="ft1">1</a>) This will fetch the object "myS3object" from the bucket "myS3bucket". Notice the -L - this option must be used in all AIStore supported commands that read or write data - usually via the URL path /v1/objects/. For more on the -L and other useful options, see [Everything curl: HTTP redirect](https://ec.haxx.se/http-redirects.html).

<a name="ft2">2</a>) See the [List Objects section](bucket.md#list-objects) for details. [↩](#a2)

<a name="ft3">3</a>) Notice the -L option here and elsewhere. [↩](#a3)

<a name="ft4">4</a>) See the [List/Range Operations section](batch.md#listrange-operations) for details.

<a name="ft5">5</a>) The request returns an HTTP status code 204 if the mountpath is already enabled/disabled or 404 if mountpath was not found.

<a name="ft6">6</a>) Advanced usage only. Use it to reassign the primary *role* administratively or if a cluster ever gets in a so-called [split-brain mode](https://en.wikipedia.org/wiki/Split-brain_(computing)). [↩](#a6)

<a name="ft7">7</a>) The request promotes files to objects; note that the files must be present inside AIStore targets and be referenceable via local directories or fully qualified names. The example request promotes recursively all files of a directory `/user/dir` that is on the target with ID `234ed78` to objects of a bucket `abc`. As `trim_prefix` is set, the names of objects are the file paths with the base trimmed: `dir/file1`, `dir/file2`, `dir/subdir/file3` etc.

<a name="ft8">8</a>) When putting the first part of an object, `handle` value must be empty string or omitted. On success, the first request returns an object handle. The subsequent `AppendObject` and `FlushObject` requests must pass the handle to the API calls. The object gets accessible and appears in a bucket only after `FlushObject` is done.

<a name="ft9">9</a>) Use option `"force": true` to ignore non-critical errors. E.g, to modify `ec.objsize_limit` when EC is already enabled, or to enable EC if the number of target is less than `ec.data_slices + ec.parity_slices + 1`.

# Backend Provider

Any storage bucket that AIS handles may originate in a 3rd party Cloud, or in another AIS cluster, or - the 3rd option - be created (and subsequently filled-in) in the AIS itself. But what if there's a pair of buckets, a Cloud-based and, separately, an AIS bucket that happen to share the same name? To resolve all potential naming, and (arguably, more importantly) partition namespace with respect to both physical isolation and QoS, AIS introduces the concept of *provider*.

* [Backend Provider](providers.md) - an abstraction, and simultaneously an API-supported option, that allows to delineate between "remote" and "local" buckets with respect to a given AIS cluster.

> Backend provider is realized as an optional parameter across all AIStore APIs that handle access to user data and bucket configuration. The list (of those APIs) includes GET, PUT, DELETE and [Range/List](batch.md) operations. For supported backend providers, please refer to [Providers](providers.md) and/or [Buckets: introduction and detailed overview](bucket.md) documents.

For even more information, CLI examples, and the most recent updates, please see:
- [Backend Providers](providers.md)
- [CLI: operations on buckets](/docs/cli/bucket.md)
- [CLI: operations on objects](/docs/cli/object.md)
- [On-Disk Layout](on_disk_layout.md)

# Curl Examples

```console
# List a given AWS bucket
$ curl -L -X GET 'http://G/v1/objects/myS3bucket/myobject?provider=aws'

# Using locally deployed AIS, get archived file from a remote named tar:
$ curl -L -X GET 'http://localhost:8080/v1/objects/myGCPbucket/train-1234.tar?provider=gcp&archpath=567.jpg' --output /tmp/567.jpg
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   201  100   201    0     0   196k      0 --:--:-- --:--:-- --:--:--  196k
100 44327  100 44327    0     0  2404k      0 --:--:-- --:--:-- --:--:-- 2404k
$ file /tmp/567.jpg
/tmp/567.jpg: JPEG image data, JFIF standard 1.01, aspect ratio, density 1x1, segment length 16, baseline, precision 8, 294x312, frames 3
```

# Querying information

AIStore provides an extensive list of RESTful operations to retrieve cluster current state:

| Operation | HTTP action | Example |
|--- | --- | ---|
| Get cluster map | GET /v1/daemon | `curl -X GET http://G/v1/daemon?what=smap` |
| Get proxy/target configuration| GET /v1/daemon | `curl -X GET http://G-or-T/v1/daemon?what=config` |
| Get proxy/target snode | GET /v1/daemon | `curl -X GET http://G-or-T/v1/daemon?what=snode` |
| Get proxy/target status | GET /v1/daemon | `curl -X GET http://G-or-T/v1/daemon?what=status` |
| Get cluster statistics (proxy) | GET /v1/cluster | `curl -X GET http://G/v1/cluster?what=stats` |
| Get target statistics | GET /v1/daemon | `curl -X GET http://T/v1/daemon?what=stats` |
| Get process info for all nodes in cluster (proxy) | GET /v1/cluster | `curl -X GET http://G/v1/cluster?what=sysinfo` |
| Get proxy/target system info | GET /v1/daemon | `curl -X GET http://G-or-T/v1/daemon?what=sysinfo` |
| Get xactions' statistics (proxy) [More](/xaction/README.md)| GET /v1/cluster | `curl -i -X GET  -H 'Content-Type: application/json' -d '{"action": "stats", "name": "xactionname", "value":{"bucket":"bckname"}}' 'http://G/v1/cluster?what=xaction'` |
| Get list of target's filesystems (target) | GET /v1/daemon?what=mountpaths | `curl -X GET http://T/v1/daemon?what=mountpaths` |
| Get list of all targets' filesystems (proxy) | GET /v1/cluster?what=mountpaths | `curl -X GET http://G/v1/cluster?what=mountpaths` |
| Get bucket list from a given target | GET /v1/daemon | `curl -X GET http://T/v1/daemon?what=bucketmd` |
| Get IPs of all targets | GET /v1/cluster | `curl -X GET http://G/v1/cluster?what=target_ips` |

## Example: querying runtime statistics

```console
$ curl -X GET http://G/v1/cluster?what=stats
```

This single command causes execution of multiple `GET ?what=stats` requests within the AIStore cluster, and results in a JSON-formatted consolidated output that contains both http proxy and storage targets request counters, as well as per-target used/available capacities. For example:

![AIStore statistics](images/ais-get-stats.png)

More usage examples can be found in the [README that describes AIS configuration](configuration.md).

# ETL

For API Reference of ETL please refer to [ETL Readme](etl.md#api-reference)



<a name="ft1">1</a>) This will fetch the object "myS3object" from the bucket "myS3bucket". Notice the -L - this option must be used in all AIStore supported commands that read or write data - usually via the URL path /v1/objects/. For more on the -L and other useful options, see [Everything curl: HTTP redirect](https://ec.haxx.se/http-redirects.html).

<a name="ft2">2</a>) See the [List Objects section](bucket.md#list-objects) for details. [↩](#a2)

<a name="ft3">3</a>) Notice the -L option here and elsewhere. [↩](#a3)

<a name="ft4">4</a>) See the [List/Range Operations section](batch.md#listrange-operations) for details.

<a name="ft5">5</a>) The request returns an HTTP status code 204 if the mountpath is already enabled/disabled or 404 if mountpath was not found.

<a name="ft6">6</a>) Advanced usage only. Use it to reassign the primary *role* administratively or if a cluster ever gets in a so-called [split-brain mode](https://en.wikipedia.org/wiki/Split-brain_(computing)). [↩](#a6)

<a name="ft7">7</a>) The request promotes files to objects; note that the files must be present inside AIStore targets and be referenceable via local directories or fully qualified names. The example request promotes recursively all files of a directory `/user/dir` that is on the target with ID `234ed78` to objects of a bucket `abc`. As `trim_prefix` is set, the names of objects are the file paths with the base trimmed: `dir/file1`, `dir/file2`, `dir/subdir/file3` etc.

<a name="ft8">8</a>) When putting the first part of an object, `handle` value must be empty string or omitted. On success, the first request returns an object handle. The subsequent `AppendObject` and `FlushObject` requests must pass the handle to the API calls. The object gets accessible and appears in a bucket only after `FlushObject` is done.

<a name="ft9">9</a>) Use option `"force": true` to ignore non-critical errors. E.g, to modify `ec.objsize_limit` when EC is already enabled, or to enable EC if the number of target is less than `ec.data_slices + ec.parity_slices + 1`.

# Backend Provider

Any storage bucket that AIS handles may originate in a 3rd party Cloud, or in another AIS cluster, or - the 3rd option - be created (and subsequently filled-in) in the AIS itself. But what if there's a pair of buckets, a Cloud-based and, separately, an AIS bucket that happen to share the same name? To resolve all potential naming, and (arguably, more importantly) partition namespace with respect to both physical isolation and QoS, AIS introduces the concept of *provider*.

* [Backend Provider](providers.md) - an abstraction, and simultaneously an API-supported option, that allows to delineate between "remote" and "local" buckets with respect to a given AIS cluster.

> Backend provider is realized as an optional parameter across all AIStore APIs that handle access to user data and bucket configuration. The list (of those APIs) includes GET, PUT, DELETE and [Range/List](batch.md) operations. For supported backend providers, please refer to [Providers](providers.md) and/or [Buckets: introduction and detailed overview](bucket.md) documents.

For even more information, CLI examples, and the most recent updates, please see:
- [Backend Providers](providers.md)
- [CLI: operations on buckets](/docs/cli/bucket.md)
- [CLI: operations on objects](/docs/cli/object.md)
- [On-Disk Layout](on_disk_layout.md)

# Curl Examples

```console
# List a given AWS bucket
$ curl -L -X GET 'http://G/v1/objects/myS3bucket/myobject?provider=aws'

# Using locally deployed AIS, get archived file from a remote named tar:
$ curl -L -X GET 'http://localhost:8080/v1/objects/myGCPbucket/train-1234.tar?provider=gcp&archpath=567.jpg' --output /tmp/567.jpg
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   201  100   201    0     0   196k      0 --:--:-- --:--:-- --:--:--  196k
100 44327  100 44327    0     0  2404k      0 --:--:-- --:--:-- --:--:-- 2404k
$ file /tmp/567.jpg
/tmp/567.jpg: JPEG image data, JFIF standard 1.01, aspect ratio, density 1x1, segment length 16, baseline, precision 8, 294x312, frames 3
```

# Querying information

AIStore provides an extensive list of RESTful operations to retrieve cluster current state:

| Operation | HTTP action | Example |
|--- | --- | ---|
| Get cluster map | GET /v1/daemon | `curl -X GET http://G/v1/daemon?what=smap` |
| Get proxy/target configuration| GET /v1/daemon | `curl -X GET http://G-or-T/v1/daemon?what=config` |
| Get proxy/target snode | GET /v1/daemon | `curl -X GET http://G-or-T/v1/daemon?what=snode` |
| Get proxy/target status | GET /v1/daemon | `curl -X GET http://G-or-T/v1/daemon?what=status` |
| Get cluster statistics (proxy) | GET /v1/cluster | `curl -X GET http://G/v1/cluster?what=stats` |
| Get target statistics | GET /v1/daemon | `curl -X GET http://T/v1/daemon?what=stats` |
| Get process info for all nodes in cluster (proxy) | GET /v1/cluster | `curl -X GET http://G/v1/cluster?what=sysinfo` |
| Get proxy/target system info | GET /v1/daemon | `curl -X GET http://G-or-T/v1/daemon?what=sysinfo` |
| Get xactions' statistics (proxy) [More](/xaction/README.md)| GET /v1/cluster | `curl -i -X GET  -H 'Content-Type: application/json' -d '{"action": "stats", "name": "xactionname", "value":{"bucket":"bckname"}}' 'http://G/v1/cluster?what=xaction'` |
| Get list of target's filesystems (target) | GET /v1/daemon?what=mountpaths | `curl -X GET http://T/v1/daemon?what=mountpaths` |
| Get list of all targets' filesystems (proxy) | GET /v1/cluster?what=mountpaths | `curl -X GET http://G/v1/cluster?what=mountpaths` |
| Get bucket list from a given target | GET /v1/daemon | `curl -X GET http://T/v1/daemon?what=bucketmd` |
| Get IPs of all targets | GET /v1/cluster | `curl -X GET http://G/v1/cluster?what=target_ips` |

## Example: querying runtime statistics

```console
$ curl -X GET http://G/v1/cluster?what=stats
```

This single command causes execution of multiple `GET ?what=stats` requests within the AIStore cluster, and results in a JSON-formatted consolidated output that contains both http proxy and storage targets request counters, as well as per-target used/available capacities. For example:

![AIStore statistics](images/ais-get-stats.png)

More usage examples can be found in the [README that describes AIS configuration](configuration.md).

# ETL

For API Reference of ETL please refer to [ETL Readme](etl.md#api-reference)
