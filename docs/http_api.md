## RESTful API

## Table of Contents
- [Notation](#notation)
- [Overview](#overview)
- [API Reference](#api-reference)
- [Backend Provider](#bucket-provider)
- [Curl Examples](#curl-examples)
- [Querying information](#querying-information)
- [Example: querying runtime statistics](#example-querying-runtime-statistics)
- [ETL](#etl)

### Notation

In this README:

> `G` - denotes a (hostname:port) address of a **gateway** (any gateway in a given AIS cluster)

> `T` - (hostname:port) of a storage **target**

> `G-or-T` - (hostname:port) of **any node** member of the cluster

### Overview
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

For example: /v1/cluster where `v1` is the currently supported API version and `cluster` is the (RESTful) resource. Other *resources* include:

| RESTful resource | Description |
| --- | ---|
| `cluster` | cluster-wide control-plane operation |
| `daemon` (aka node) | control-plane request to update or query specific AIS daemon (proxy or target). In the documentation, the terms "daemon" and "node" are used interchangeably. |
| `buckets` | create, destroy, rename, copy, transform (entire) buckets; list objects in a given bucket; get bucket names for a given provider (or all providers); get bucket properties |
| `objects` | datapath request to GET, PUT and DELETE objects, read their properties |
| `download` | download external resources (datasets, files) into cluster |

5. **URL query**, e. g., `?what=config`.

In particular, all API requests that operate on a bucket carry the bucket's specification details in the URL's query. Those details may include [backend provider](providers.md) and [namespace](providers.md#unified-global-namespace) where an empty backend provider indicates an AIS bucket (with AIStore being, effectively, the default provider) while an empty namespace parameter translates as a global (default) namespace. For exact names of the bucket-specifying URL Query parameters, please refer to this [API source](/api/bucket.go).

> Combined, all these elements tell the following story. They specify the most generic action (e.g., GET) and designate the target aka "resource" of this action: e. g., an entire cluster or a given AIS node. Further, they may also include context-specific and query string encoded control message to, for instance, distinguish between getting system statistics (`?what=stats`) versus system configuration (`?what=config`).

> For developers and first-time users: if you deployed AIS locally having followed [these instructions](/README.md#local-non-containerized) then most likely you will have `http://localhost:8080` as the primary proxy, and generally, `http://localhost:808x` for all locally-deployed AIS daemons.

> The reference below is "formulated" in `curl` - i.e., using `curl` command lines. It is possible, however, and often much easier (and, therefore, **preferable**), to execute the same operations using [AIS CLI](/cmd/cli/README.md).

### API Reference

| Operation | HTTP action | Example |
|--- | --- | ---|
| Unregister storage target | DELETE /v1/cluster/daemon/daemonID | `curl -i -X DELETE 'http://G/v1/cluster/daemon/15205:8083'` |
| Register storage target | POST /v1/cluster/register | `curl -i -X POST -H 'Content-Type: application/json' -d '{"daemon_type": "target", "node_ip_addr": "172.16.175.41", "daemon_port": "8083", "direct_url": "http://172.16.175.41:8083"}' 'http://localhost:8083/v1/cluster/register'` |
| Register proxy (gateway) | POST /v1/cluster/register | `curl -i -X POST -H 'Content-Type: application/json' -d '{"daemon_type": "proxy", "node_ip_addr": "172.16.175.41", "daemon_port": "8083", "direct_url": "http://172.16.175.41:8083"}' 'http://localhost:8083/v1/cluster/register'` |
| Set primary proxy | PUT /v1/cluster/proxy/new primary-proxy-id | `curl -i -X PUT 'http://G-primary/v1/cluster/proxy/26869:8080'` |
| Force-Set primary proxy | PUT /v1/daemon/proxy/proxyID | `curl -i -X PUT -G 'http://G-primary/v1/daemon/proxy/23ef189ed'  --data-urlencode "frc=true" --data-urlencode "can=http://G-new-designated-primary"`  <sup id="a6">[6](#ft6)</sup>|
| Set AIS node configuration **via JSON message** | PUT {"action": "setconfig", "name": "some-name", "value": "other-value"} /v1/daemon | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "setconfig","name": "stats_time", "value": "1s"}' 'http://G-or-T/v1/daemon'`<br>• For the list of named options, see [runtime configuration](configuration.md#runtime-configuration) |
| Set AIS node configuration **via URL query** | PUT /v1/daemon/setconfig/?name1=value1&name2=value2&... | `curl -i -X PUT 'http://G-or-T/v1/daemon/setconfig?stats_time=33s&log.loglevel=4'`<br>• Allows to update multiple values in one shot<br>• For the list of named configuration options, see [runtime configuration](configuration.md#runtime-configuration) |
| Set cluster-wide configuration **via JSON message** (proxy) | PUT {"action": "setconfig", "name": "some-name", "value": "other-value"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "setconfig","name": "stats_time", "value": "1s"}' 'http://G/v1/cluster'`<br>• Note below the alternative way to update cluster configuration<br>• For the list of named options, see [runtime configuration](configuration.md#runtime-configuration) |
| Set cluster-wide configuration **via URL query** | PUT /v1/cluster/setconfig/?name1=value1&name2=value2&... | `curl -i -X PUT 'http://G/v1/cluster/setconfig?stats_time=33s&log.loglevel=4'`<br>• Allows to update multiple values in one shot<br>• For the list of named configuration options, see [runtime configuration](configuration.md#runtime-configuration) |
| Reset AIS node configuration | PUT {"action": "resetconfig"} /v1/daemon | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "resetconfig"}' 'http://G-or-T/v1/daemon'` |
| Reset cluster-wide configuration | PUT {"action": "resetconfig"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "resetconfig"}' 'http://G/v1/cluster'` |
| Shutdown a given ais cluster (target or proxy) | PUT {"action": "shutdown_node", "value": {"sid": daemonID}} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "shutdown_node", "value": {"sid": "43888:8083"}}' 'http://G/v1/cluster'` |
| Shutdown cluster | PUT {"action": "shutdown"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "shutdown"}' 'http://G-primary/v1/cluster'` |
| Decommission cluster | PUT {"action": "decommission"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "shutdown"}' 'http://G-primary/v1/cluster'` |
| Rebalance cluster | PUT {"action": "start", "value": {"kind": "rebalance"}} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "start", "value": {"kind": "rebalance"}}' 'http://G/v1/cluster'` |
| Resilver cluster | PUT {"action": "start", "value": {"kind": "resilver"}} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "start", "value": {"kind": "resilver"}}' 'http://G/v1/cluster'` |
| Resilver storage target | PUT {"action": "start", "value": {"kind": "resilver", "node": targetID}} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "start", "value": {"kind": "resilver", "node": "43888:8083"}}' 'http://G/v1/cluster'` |
| Abort global (automated or manually started) rebalance (proxy) | PUT {"action": "stop", "value": {"kind": "rebalance"}} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "stop", "value": {"kind": "rebalance"}}' 'http://G/v1/cluster'` |
| Create [bucket](bucket.md) | POST {"action": "create_bck"} /v1/buckets/bucket-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "create_bck"}' 'http://G/v1/buckets/abc'` |
| Destroy [bucket](bucket.md) | DELETE {"action": "destroy_bck"} /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action": "destroy_bck"}' 'http://G/v1/buckets/abc'` |
| Rename ais [bucket](bucket.md) | POST {"action": "move_bck"} /v1/buckets/from-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "move_bck" }' 'http://G/v1/buckets/from-name?bck=<bck>&bckto=<to-bck>'` |
| Copy [bucket](bucket.md) | POST {"action": "copybck"} /v1/buckets/from-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "copybck", }}}' 'http://G/v1/buckets/from-name?bck=<bck>&bckto=<to-bck>'` |
| Rename/move object (ais buckets only) | POST {"action": "rename", "name": new-name} /v1/objects/bucket-name/object-name | `curl -i -X POST -L -H 'Content-Type: application/json' -d '{"action": "rename", "name": "dir2/DDDDDD"}' 'http://G/v1/objects/mybucket/dir1/CCCCCC'` <sup id="a3">[3](#ft3)</sup> |
| Check if an object from a remote bucket *is cached*  | HEAD /v1/objects/bucket-name/object-name | `curl -L --head 'http://G/v1/objects/mybucket/myobject?check_cached=true'` |
| GET object | GET /v1/objects/bucket-name/object-name | `curl -L -X GET 'http://G/v1/objects/myS3bucket/myobject' -o myobject` <sup id="a1">[1](#ft1)</sup> |
| Read range | GET /v1/objects/bucket-name/object-name | `curl -L -X GET -H 'Range: bytes=1024-1535' 'http://G/v1/objects/myS3bucket/myobject' -o myobject`<br> Note: For more information about the HTTP Range header, see [this](https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35)  |
| Get [bucket](bucket.md) names | GET /v1/buckets/\* | `curl -X GET 'http://G/v1/buckets/*'` |
| List objects in a given [bucket](bucket.md) | GET {"action": "list", "value": { properties-and-options... }} /v1/buckets/bucket-name | `curl -X GET -L -H 'Content-Type: application/json' -d '{"action": "listobj", "value":{"props": "size"}}' 'http://G/v1/buckets/myS3bucket'` <sup id="a2">[2](#ft2)</sup> |
| Get [bucket properties](bucket.md#bucket-properties) | HEAD /v1/buckets/bucket-name | `curl -L --head 'http://G/v1/buckets/mybucket'` |
| Get object props | HEAD /v1/objects/bucket-name/object-name | `curl -L --head 'http://G/v1/objects/mybucket/myobject'` |
| PUT object | PUT /v1/objects/bucket-name/object-name | `curl -L -X PUT 'http://G/v1/objects/myS3bucket/myobject' -T filenameToUpload` |
| APPEND to object | PUT /v1/objects/bucket-name/object-name?appendty=append&handle= | `curl -L -X PUT 'http://G/v1/objects/myS3bucket/myobject?appendty=append&handle=' -T filenameToUpload-partN`  <sup>[8](#ft8)</sup> |
| Finalize APPEND | PUT /v1/objects/bucket-name/object-name?appendty=flush&handle=obj-handle | `curl -L -X PUT 'http://G/v1/objects/myS3bucket/myobject?appendty=flush&handle=obj-handle'`  <sup>[8](#ft8)</sup> |
| Delete object | DELETE /v1/objects/bucket-name/object-name | `curl -i -X DELETE -L 'http://G/v1/objects/mybucket/myobject'` |
| Delete a list of objects | DELETE '{"action":"delete", "value":{"objnames":"[o1[,o]]"}}' /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action":"delete", "value":{"objnames":["o1","o2","o3"]}}' 'http://G/v1/buckets/abc'` <sup>[4](#ft4)</sup> |
| Delete a range of objects | DELETE '{"action":"delete", "value":{"template":"your-prefix{min..max}"}}' /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action":"delete", "value":{"template":"__tst/test-{1000..2000}"}}' 'http://G/v1/buckets/abc'` <sup>[4](#ft4)</sup> |
| Configure bucket as [n-way mirror](storage_svcs.md#n-way-mirror) (proxy) | POST {"action": "makencopies", "value": n} /v1/buckets/bucket-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"makencopies", "value": 2}' 'http://G/v1/buckets/abc'` |
| Enable [erasure coding](storage_svcs.md#erasure-coding) protection for all objects (proxy) | POST {"action": "ecencode"} /v1/buckets/bucket-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"ecencode"}' 'http://G/v1/buckets/abc'` |
| Set [bucket properties](bucket.md#bucket-properties) (proxy) | PATCH {"action": "setbprops"} /v1/buckets/bucket-name | `curl -i -X PATCH -H 'Content-Type: application/json' -d '{"action":"setbprops", "value": {"checksum": {"type": "sha256"}, "mirror": {"enable": true}, "force": false}' 'http://G/v1/buckets/abc'`  <sup>[9](#ft9)</sup> |
| Reset [bucket properties](bucket.md#bucket-properties) (proxy) | PATCH {"action": "resetbprops"} /v1/buckets/bucket-name | `curl -i -X PATCH -H 'Content-Type: application/json' -d '{"action":"resetbprops"}' 'http://G/v1/buckets/abc'` |
| [Prefetch](bucket.md#prefetchevict-objects) a list of objects | POST '{"action":"prefetch", "value":{"objnames":"[o1[,o]]"}}' /v1/buckets/bucket-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"prefetch", "value":{"objnames":["o1","o2","o3"]}}' 'http://G/v1/buckets/abc'` <sup>[4](#ft4)</sup> |
| [Prefetch](bucket.md#prefetchevict-objects) a range of objects| POST '{"action":"prefetch", "value":{"template":"your-prefix{min..max}" }}' /v1/buckets/bucket-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"prefetch", "value":{"template":"__tst/test-{1000..2000}"}}' 'http://G/v1/buckets/abc'` <sup>[4](#ft4)</sup> |
| [Evict](bucket.md#prefetchevict-objects) object from cache | DELETE '{"action": "evictobj"}' /v1/objects/bucket-name/object-name | `curl -i -X DELETE -L -H 'Content-Type: application/json' -d '{"action": "evictobj"}' 'http://G/v1/objects/mybucket/myobject'` |
| [Evict](bucket.md#evict-bucket) remote bucket | DELETE {"action": "evictcb"} /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action": "evictcb"}' 'http://G/v1/buckets/myS3bucket'` |
| [Evict](bucket.md#prefetchevict-objects) a list of objects | DELETE '{"action":"evictobj", "value":{"objnames":"[o1[,o]]"}}' /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action":"evictobj", "value":{"objnames":["o1","o2","o3"]}}' 'http://G/v1/buckets/abc'` <sup>[4](#ft4)</sup> |
| [Evict](bucket.md#prefetchevict-objects) a range of objects| DELETE '{"action":"evictobj", "value":{"template":"your-prefix{min..max}"}}' /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action":"evictobj", "value":{"template":"__tst/test-{1000..2000}"}}' 'http://G/v1/buckets/abc'` <sup>[4](#ft4)</sup> |
| Disable mountpath (target) | POST {"action": "disable", "value": "/existing/mountpath"} /v1/daemon/mountpaths | `curl -X POST -L -H 'Content-Type: application/json' -d '{"action": "disable", "value":"/mount/path"}' 'http://T/v1/daemon/mountpaths'`<sup>[5](#ft5)</sup> |
| Enable mountpath (target) | POST {"action": "enable", "value": "/existing/mountpath"} /v1/daemon/mountpaths | `curl -X POST -L -H 'Content-Type: application/json' -d '{"action": "enable", "value":"/mount/path"}' 'http://T/v1/daemon/mountpaths'`<sup>[5](#ft5)</sup> |
| Add mountpath (target) | PUT {"action": "add", "value": "/new/mountpath"} /v1/daemon/mountpaths | `curl -X PUT -L -H 'Content-Type: application/json' -d '{"action": "add", "value":"/mount/path"}' 'http://T/v1/daemon/mountpaths'` |
| Remove mountpath from target | DELETE {"action": "remove", "value": "/existing/mountpath"} /v1/daemon/mountpaths | `curl -X DELETE -L -H 'Content-Type: application/json' -d '{"action": "remove", "value":"/mount/path"}' 'http://T/v1/daemon/mountpaths'` |
| Promote file/directory(proxy) | POST {"action": "promote", "name": "/home/user/dirname", "value": {"target": "234ed78", "recurs": true, "keep": true}} /v1/buckets/bucket-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"promote", "name":"/user/dir", "value": {"target": "234ed78", "trim_prefix": "/user/", "recurs": true, "keep": true} }' 'http://G/v1/buckets/abc'` <sup>[7](#ft7)</sup>|
___

<a name="ft1">1</a>: This will fetch the object "myS3object" from the bucket "myS3bucket". Notice the -L - this option must be used in all AIStore supported commands that read or write data - usually via the URL path /v1/objects/. For more on the -L and other useful options, see [Everything curl: HTTP redirect](https://ec.haxx.se/http-redirects.html).

<a name="ft2">2</a>: See the [List Objects section](bucket.md#list-objects) for details. [↩](#a2)

<a name="ft3">3</a>: Notice the -L option here and elsewhere. [↩](#a3)

<a name="ft4">4</a>: See the [List/Range Operations section](batch.md#listrange-operations) for details.

<a name="ft5">5</a>: The request returns an HTTP status code 204 if the mountpath is already enabled/disabled or 404 if mountpath was not found.

<a name="ft6">6</a>: Advanced usage only. Use it to reassign the primary *role* administratively or if a cluster ever gets in a so-called [split-brain mode](https://en.wikipedia.org/wiki/Split-brain_(computing)). [↩](#a6)

<a name="ft7">7</a>: The request promotes files to objects; note that the files must be present inside AIStore targets and be referenceable via local directories or fully qualified names. The example request promotes recursively all files of a directory `/user/dir` that is on the target with ID `234ed78` to objects of a bucket `abc`. As `trim_prefix` is set, the names of objects are the file paths with the base trimmed: `dir/file1`, `dir/file2`, `dir/subdir/file3` etc.

<a name="ft8">8</a>: When putting the first part of an object, `handle` value must be empty string or omitted. On success, the first request returns an object handle. The subsequent `AppendObject` and `FlushObject` requests must pass the handle to the API calls. The object gets accessible and appears in a bucket only after `FlushObject` is done.

<a name="ft9">9</a>: Use option `"force": true` to ignore non-critical errors. E.g, to modify `ec.objsize_limit` when EC is already enabled, or to enable EC if the number of target is less than `ec.data_slices + ec.parity_slices + 1`.

### Backend Provider

Any storage bucket that AIS handles may originate in a 3rd party Cloud, or in another AIS cluster, or - the 3rd option - be created (and subsequently filled-in) in the AIS itself. But what if there's a pair of buckets, a Cloud-based and, separately, an AIS bucket that happen to share the same name? To resolve all potential naming, and (arguably, more importantly) partition namespace with respect to both physical isolation and QoS, AIS introduces the concept of *provider*.

* [Backend Provider](providers.md) - an abstraction, and simultaneously an API-supported option, that allows to delineate between "remote" and "local" buckets with respect to a given AIS cluster.

> Backend provider is realized as an optional parameter across all AIStore APIs that handle access to user data and bucket configuration. The list (of those APIs) includes GET, PUT, DELETE and [Range/List](batch.md) operations. For supported backend providers, please refer to [Providers](providers.md) and/or [Buckets: introduction and detailed overview](bucket.md) documents.

For even more information, CLI examples, and the most recent updates, please see:
- [Backend Providers](providers.md)
- [CLI: operations on buckets](/cmd/cli/resources/bucket.md)
- [CLI: operations on objects](/cmd/cli/resources/object.md)
- [On-Disk Layout](on-disk-layout.md)

### Curl Examples

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

#### Supported APIs

| RESTful verb | AIS APIs |
| --- | --- |
| GET | GET object, Read range, Get bucket names |
| PUT | PUT object, APPEND object, Set single bucket property, Set bucket properties |
| DELETE | DELETE object, DELETE list of objects, DELETE range of objects |
| HEAD | Get bucket properties, Get object properties |


### Querying information

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

### Example: querying runtime statistics

```console
$ curl -X GET http://G/v1/cluster?what=stats
```

This single command causes execution of multiple `GET ?what=stats` requests within the AIStore cluster, and results in a JSON-formatted consolidated output that contains both http proxy and storage targets request counters, as well as per-target used/available capacities. For example:

<img src="images/ais-get-stats.png" alt="AIStore statistics" width="256">

More usage examples can be found in the [README that describes AIS configuration](configuration.md).

### ETL

For API Reference of ETL please refer to [ETL Readme](etl.md#api-reference)
