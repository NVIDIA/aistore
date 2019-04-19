## RESTful API

## Table of Contents
- [Notation](#notation)
- [Overview](#overview)
- [API Reference](#api-reference)
- [Bucket Provider](#bucket-provider)
- [Querying information](#querying-information)
- [Example: querying runtime statistics](#example-querying-runtime-statistics)

### Notation

In this README:

> `G` - denotes a (hostname:port) address of a **gateway** (any gateway in a given AIS cluster)

> `T` - (hostname:port) of a storage **target**

> `G-or-T` - (hostname:port) of **any node** member of the cluster

### Overview
AIStore supports a growing number and variety of RESTful operations. To illustrate common conventions, let's take a look at the example:

```shell
$ curl -X GET http://G-or-T/v1/daemon?what=config
```

This command queries one of the AIS nodes (denoted as `G-or-T`) for its configuration. The query - as well as most of other control plane queries - results in a JSON-formatted output that can be viewed with any compatible JSON viewer.

Notice the 4 (four) ubiquitous elements in the `curl` command line above:

1. HTTP verb aka method, one of: `PUT`, `GET`, `HEAD`, `POST`, or `DELETE`.

In the example, it's a GET but it can also be POST, PUT, and DELETE. For a brief summary of the standard HTTP verbs and their CRUD semantics, see, for instance, this [REST API tutorial](http://www.restapitutorial.com/lessons/httpmethods.html).

2. Hostname (or IPv4 address) and TCP port of one of the AIStore daemons.

Most RESTful operation performed on an AIStore proxy/gateway will likely have a *clustered* scope. Exceptions may include querying proxy's own configuration via `?what=config`, and more.

3. URL path: version of the REST API, RESTful *resource* that is operated upon, and possibly more forward-slash delimited specifiers.

For example: /v1/cluster where `v1` is the currently supported API version and `cluster` is the (RESTful) resource. Other *resources* include:

| RESTful resource | Description |
| --- | ---|
| `cluster` | cluster-wide control-plane operation |
| `daemon` | control-plane request to update or query specific AIS daemon (proxy or target) |
| `buckets` | create, destroy, rename and list bucket(s), get bucket names, get bucket properties |
| `objects` | datapath request to GET, PUT and DELETE objects, read their properties |
| `download` | download external resources (datasets, files) into cluster |

4. Control message in the query string parameter, e.g. `?what=config`.

> Combined, all these elements tell the following story. They specify the most generic action (e.g., GET) and designate the target aka "resource" of this action: e.g., an entire cluster or a given daemon. Further, they may also include context-specific and query string encoded control message to, for instance, distinguish between getting system statistics (`?what=stats`) versus system configuration (`?what=config`).

> For developers and first-time users: if you deployed AIS locally having followed [these instructions](../README.md#local-non-containerized) then most likely you will have `http://localhost:8080` as the primary proxy, and generally, `http://localhost:808x` for all locally-deployed AIS daemons.

### API Reference

| Operation | HTTP action | Example |
|--- | --- | ---|
| Unregister storage target | DELETE /v1/cluster/daemon/daemonID | `curl -i -X DELETE 'http://G/v1/cluster/daemon/15205:8083'` |
| Register storage target | POST /v1/cluster/register | `curl -i -X POST -H 'Content-Type: application/json' -d '{"daemon_type": "target", "node_ip_addr": "172.16.175.41", "daemon_port": "8083", "daemon_id": "43888:8083", "direct_url": "http://172.16.175.41:8083"}' 'http://localhost:8083/v1/cluster/register'` |
| Register storage proxy | POST /v1/cluster/register | `curl -i -X POST -H 'Content-Type: application/json' -d '{"daemon_type": "proxy", "node_ip_addr": "172.16.175.41", "daemon_port": "8083", "daemon_id": "43888:8083", "direct_url": "http://172.16.175.41:8083"}' 'http://localhost:8083/v1/cluster/register'` |
| Set primary proxy (primary proxy only)| PUT /v1/cluster/proxy/new primary-proxy-id | `curl -i -X PUT 'http://G-primary/v1/cluster/proxy/26869:8080'` |
| Force-Set primary proxy (primary proxy)| PUT /v1/daemon/proxy/proxyID | `curl -i -X PUT -G 'http://G-primary/v1/daemon/proxy/23ef189ed'  --data-urlencode "frc=true" --data-urlencode "can=http://G-new-designated-primary"`  <sup id="a6">[6](#ft6)</sup>|
| Set AIS node configuration **via JSON message** | PUT {"action": "setconfig", "name": "some-name", "value": "other-value"} /v1/daemon | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "setconfig","name": "stats_time", "value": "1s"}' 'http://G-or-T/v1/daemon'`<br>• For the list of named options, see [runtime configuration](./configuration.md#runtime-configuration) |
| Set AIS node configuration **via URL query** | PUT /v1/daemon/setconfig/?name1=value1&name2=value2&... | `curl -i -X PUT 'http://G-or-T/v1/daemon/setconfig?stats_time=33s&log.loglevel=4'`<br>• Allows to update multiple values in one shot<br>• For the list of named configuration options, see [runtime configuration](./configuration.md#runtime-configuration) |
| Set cluster-wide configuration **via JSON message** (proxy) | PUT {"action": "setconfig", "name": "some-name", "value": "other-value"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "setconfig","name": "stats_time", "value": "1s"}' 'http://G/v1/cluster'`<br>• Note below the alternative way to update cluster configuration<br>• For the list of named options, see [runtime configuration](./configuration.md#runtime-configuration) |
| Set cluster-wide configuration **via URL query** (proxy) | PUT /v1/cluster/setconfig/?name1=value1&name2=value2&... | `curl -i -X PUT 'http://G/v1/cluster/setconfig?stats_time=33s&log.loglevel=4'`<br>• Allows to update multiple values in one shot<br>• For the list of named configuration options, see [runtime configuration](./configuration.md#runtime-configuration) |
| Shutdown target/proxy | PUT {"action": "shutdown"} /v1/daemon | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "shutdown"}' 'http://G-or-T/v1/daemon'` |
| Shutdown cluster (proxy) | PUT {"action": "shutdown"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "shutdown"}' 'http://G-primary/v1/cluster'` |
| Rebalance cluster (proxy) | PUT {"action": "rebalance", "value": "start"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "rebalance", "value": "start"}' 'http://G/v1/cluster'` |
| Abort global (automated or manually started) rebalance (proxy) | PUT {"action": "rebalance", "value": "abort"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "rebalance", "value": "abort"}' 'http://G/v1/cluster'` |
| Create local [bucket](bucket.md) (proxy) | POST {"action": "createlb"} /v1/buckets/bucket-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "createlb"}' 'http://G/v1/buckets/abc'` |
| Destroy local [bucket](bucket.md) (proxy) | DELETE {"action": "destroylb"} /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action": "destroylb"}' 'http://G/v1/buckets/abc'` |
| Rename local [bucket](bucket.md) (proxy) | POST {"action": "renamelb"} /v1/buckets/bucket-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "renamelb", "name": "newname"}' 'http://G/v1/buckets/oldname'` |
| Recover buckets [bucket](bucket.md) (proxy) | POST {"action": "recoverbck"} /v1/buckets?force=true | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "recoverbck"}' 'http://G/v1/buckets'` |
| Rename/move object (local buckets) | POST {"action": "rename", "name": new-name} /v1/objects/bucket-name/object-name | `curl -i -X POST -L -H 'Content-Type: application/json' -d '{"action": "rename", "name": "dir2/DDDDDD"}' 'http://G/v1/objects/mylocalbucket/dir1/CCCCCC'` <sup id="a3">[3](#ft3)</sup> |
| Check if an object *is cached*  | HEAD /v1/objects/bucket-name/object-name | `curl -L --head 'http://G/v1/objects/mybucket/myobject?check_cached=true'` |
| Get object (proxy) | GET /v1/objects/bucket-name/object-name | `curl -L -X GET 'http://G/v1/objects/myS3bucket/myobject' -o myobject` <sup id="a1">[1](#ft1)</sup> |
| Read range (proxy) | GET /v1/objects/bucket-name/object-name?offset=&length= | `curl -L -X GET 'http://G/v1/objects/myS3bucket/myobject?offset=1024&length=512' -o myobject` |
| Get [bucket](bucket.md) names | GET /v1/buckets/\* | `curl -X GET 'http://G/v1/buckets/*'` |
| List objects in a given [bucket](bucket.md) | POST {"action": "listobjects", "value":{  properties-and-options... }} /v1/buckets/bucket-name | `curl -X POST -L -H 'Content-Type: application/json' -d '{"action": "listobjects", "value":{"props": "size"}}' 'http://G/v1/buckets/myS3bucket'` <sup id="a2">[2](#ft2)</sup> |
| List object names in a given [bucket](bucket.md) fast<br>(returns only object names) | POST /v1/buckets/bucket-name/listobjects?prefix= <br> or <br> POST {"action": "listobjects", "value":{ "fast": true }} /v1/buckets/bucket-name | `curl -X POST -L 'http://G/v1/buckets/myS3bucket/listobjects?prefix=image01'` <sup id="a8">[8](#ft8)<br>or<br> </sup> `curl -X POST -L -H 'Content-Type: application/json' -d '{"action": "listobjects", "value":{"props": "size", "fast": true}}' 'http://G/v1/buckets/myS3bucket'` |
| Get [bucket properties](bucket.md#properties-and-options) | HEAD /v1/buckets/bucket-name | `curl -L --head 'http://G/v1/buckets/mybucket'` |
| Get object props | HEAD /v1/objects/bucket-name/object-name | `curl -L --head 'http://G/v1/objects/mybucket/myobject'` |
| Put object (proxy) | PUT /v1/objects/bucket-name/object-name | `curl -L -X PUT 'http://G/v1/objects/myS3bucket/myobject' -T filenameToUpload` |
| Delete object | DELETE /v1/objects/bucket-name/object-name | `curl -i -X DELETE -L 'http://G/v1/objects/mybucket/myobject'` |
| Delete a list of objects | DELETE '{"action":"delete", "value":{"objnames":"[o1[,o]]"[, deadline: string][, wait: bool]}}' /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action":"delete", "value":{"objnames":["o1","o2","o3"], "deadline": "10s", "wait":true}}' 'http://G/v1/buckets/abc'` <sup>[4](#ft4)</sup> |
| Delete a range of objects | DELETE '{"action":"delete", "value":{"prefix":"your-prefix","regex":"your-regex","range","min:max" [, deadline: string][, wait:bool]}}' /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action":"delete", "value":{"prefix":"__tst/test-", "regex":"\\d22\\d", "range":"1000:2000", "deadline": "10s", "wait":true}}' 'http://G/v1/buckets/abc'` <sup>[4](#ft4)</sup> |
| Configure bucket as [n-way mirror](storage_svcs.md#n-way-mirror) (proxy) | POST {"action": "makencopies", "value": n} /v1/buckets/bucket-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"makencopies", "value": 2}' 'http://G/v1/buckets/abc'` |
| Set all [bucket properties](bucket.md#properties-and-options) (proxy) | PUT {"action": "setprops"} /v1/buckets/bucket-name | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"setprops", "value": {"next_tier_url": "http://G-other", "cloud_provider": "ais", "read_policy": "cloud", "write_policy": "next_tier", "cksum": { "type": "inherit" }}}' 'http://G/v1/buckets/abc'` |
| Set [bucket properties](bucket.md#properties-and-options) (proxy) | PUT /v1/buckets/bucket-name/setprops | `curl -i -X PUT 'http://G/v1/buckets/abc/setprops?mirror.enabled=true&ec.enabled=true&mirror.copies=5'` <sup id="a7">[7](#ft7)</sup> |
| Reset [bucket properties](bucket.md#properties-and-options) (proxy) | PUT {"action": "resetprops"} /v1/buckets/bucket-name | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"resetprops"}' 'http://G/v1/buckets/abc'` |
| [Prefetch](bucket.md#prefetchevict-objects) a list of objects | POST '{"action":"prefetch", "value":{"objnames":"[o1[,o]]"[, deadline: string][, wait: bool]}}' /v1/buckets/bucket-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"prefetch", "value":{"objnames":["o1","o2","o3"], "deadline": "10s", "wait":true}}' 'http://G/v1/buckets/abc'` <sup>[4](#ft4)</sup> |
| [Prefetch](bucket.md#prefetchevict-objects) a range of objects| POST '{"action":"prefetch", "value":{"prefix":"your-prefix","regex":"your-regex","range","min:max" [, deadline: string][, wait:bool]}}' /v1/buckets/bucket-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"prefetch", "value":{"prefix":"__tst/test-", "regex":"\\d22\\d", "range":"1000:2000", "deadline": "10s", "wait":true}}' 'http://G/v1/buckets/abc'` <sup>[4](#ft4)</sup> |
| [Evict](bucket.md#prefetchevict-objects) object from cache | DELETE '{"action": "evictobjects"}' /v1/objects/bucket-name/object-name | `curl -i -X DELETE -L -H 'Content-Type: application/json' -d '{"action": "evictobjects"}' 'http://G/v1/objects/mybucket/myobject'` |
| [Evict](bucket.md#evict-bucket) cloud bucket (proxy) | DELETE {"action": "evictcb"} /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action": "evictcb"}' 'http://G/v1/buckets/myS3bucket'` |
| [Evict](bucket.md#prefetchevict-objects) a list of objects | DELETE '{"action":"evictobjects", "value":{"objnames":"[o1[,o]]"[, deadline: string][, wait: bool]}}' /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action":"evictobjects", "value":{"objnames":["o1","o2","o3"], "deadline": "10s", "wait":true}}' 'http://G/v1/buckets/abc'` <sup>[4](#ft4)</sup> |
| [Evict](bucket.md#prefetchevict-objects) a range of objects| DELETE '{"action":"evictobjects", "value":{"prefix":"your-prefix","regex":"your-regex","range","min:max" [, deadline: string][, wait:bool]}}' /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action":"evictobjects", "value":{"prefix":"__tst/test-", "regex":"\\d22\\d", "range":"1000:2000", "deadline": "10s", "wait":true}}' 'http://G/v1/buckets/abc'` <sup>[4](#ft4)</sup> |
| Disable mountpath (target) | POST {"action": "disable", "value": "/existing/mountpath"} /v1/daemon/mountpaths | `curl -X POST -L -H 'Content-Type: application/json' -d '{"action": "disable", "value":"/mount/path"}' 'http://T/v1/daemon/mountpaths'`<sup>[5](#ft5)</sup> |
| Enable mountpath (target) | POST {"action": "enable", "value": "/existing/mountpath"} /v1/daemon/mountpaths | `curl -X POST -L -H 'Content-Type: application/json' -d '{"action": "enable", "value":"/mount/path"}' 'http://T/v1/daemon/mountpaths'`<sup>[5](#ft5)</sup> |
| Add mountpath (target) | PUT {"action": "add", "value": "/new/mountpath"} /v1/daemon/mountpaths | `curl -X PUT -L -H 'Content-Type: application/json' -d '{"action": "add", "value":"/mount/path"}' 'http://T/v1/daemon/mountpaths'` |
| Remove mountpath from target | DELETE {"action": "remove", "value": "/existing/mountpath"} /v1/daemon/mountpaths | `curl -X DELETE -L -H 'Content-Type: application/json' -d '{"action": "remove", "value":"/mount/path"}' 'http://T/v1/daemon/mountpaths'` |
___
<a name="ft1">1</a>: This will fetch the object "myS3object" from the bucket "myS3bucket". Notice the -L - this option must be used in all AIStore supported commands that read or write data - usually via the URL path /v1/objects/. For more on the -L and other useful options, see [Everything curl: HTTP redirect](https://ec.haxx.se/http-redirects.html).

<a name="ft2">2</a>: See the [List Bucket section](bucket.md#list-bucket) for details. [↩](#a2)

<a name="ft3">3</a>: Notice the -L option here and elsewhere. [↩](#a3)

<a name="ft4">4</a>: See the [List/Range Operations section](batch.md#listrange-operations) for details.

<a name="ft5">5</a>: The request returns an HTTP status code 204 if the mountpath is already enabled/disabled or 404 if mountpath was not found.

<a name="ft6">6</a>: Advanced usage only. Use it to reassign the primary *role* administratively or if a cluster ever gets in a so-called [split-brain mode](https://en.wikipedia.org/wiki/Split-brain_(computing)). [↩](#a6)

<a name="ft7">7</a>: The difference between "Set all bucket properties" and "Set bucket properties" is that the "Set bucket properties" action takes in key-value pairs of properties via URL query string. In the case of "Set all bucket properties", the `value` must be correctly-filled `cmn.BucketProps` structure (all fields must be set!). For the list of supported properties, see [API constants](/cmn/api.go) and look for a section titled 'Header Key enum'[↩](#a7)

<a name="ft8">8</a>: This request does not use payload and returns only object names and sizes sorted in alphabetical order. It makes the request much faster for local buckets comparing to getting a list of objects using URL `/v1/buckets/bucket-name` and payload. Requests to cloud buckets do not get any performance boost. The only supported parameter is URL query value `prefix` that filters out from the result objects which names do not start with the prefix. NOTE for local buckets: the request does not do extra checks for object correct location, that may result in the list containing moved or deleted objects that are not accessible any longer. [↩](#a8)

### Bucket Provider

Any storage bucket that AIS handles may originate in a 3rd party Cloud, or be created (and subsequently filled-in) in the AIS itself. But what if there's a pair of buckets, a Cloud-based and, separately, a local one, that happen to share the same name? To resolve the potential naming conflict, AIS 2.0 introduces the concept of *bucket provider*.

> Bucket provider is realized as an optional parameter in the GET, PUT, DELETE and [Range/List](batch.md) operations with supported enumerated values: `local` and `ais` for local buckets, and `cloud`, `aws`, `gcp` for cloud buckets.

In all those cases users can add an optional `?bprovider=local` or `?bprovider=cloud` query to the GET (PUT, DELETE, List/Range) request.

Example: `curl -L -X GET 'http://G/v1/objects/myS3bucket/myobject?bprovider=local'`

#### Supported APIs for Bucket Provider

| Method | Supported APIs |
| --- | --- |
| GET | Get object, Read range, Get bucket names |
| PUT | Put object, Set single bucket property, Set bucket properties |
| DELETE | Delete object, Delete list of objects, Delete range of objects |
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
| Get xactions' statistics (proxy) [More](xaction.md)| GET /v1/cluster | `curl -i -X GET  -H 'Content-Type: application/json' -d '{"action": "stats", "name": "xactionname", "value":{"bucket":"bckname"}}' 'http://G/v1/cluster?what=xaction'` |
| Get list of target's filesystems (target) | GET /v1/daemon?what=mountpaths | `curl -X GET http://T/v1/daemon?what=mountpaths` |
| Get list of all targets' filesystems (proxy) | GET /v1/cluster?what=mountpaths | `curl -X GET http://G/v1/cluster?what=mountpaths` |
| Get bucket list from a given target | GET /v1/daemon | `curl -X GET http://T/v1/daemon?what=bucketmd` |

### Example: querying runtime statistics

```shell
$ curl -X GET http://G/v1/cluster?what=stats
```

This single command causes execution of multiple `GET ?what=stats` requests within the AIStore cluster, and results in a JSON-formatted consolidated output that contains both http proxy and storage targets request counters, as well as per-target used/available capacities. For example:

<img src="images/ais-get-stats.png" alt="AIStore statistics" width="256">

More usage examples can be found in the [README that describes AIS configuration](configuration.md).
