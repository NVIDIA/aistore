## RESTful API

## Table of Contents
- [Overview](#overview)
- [API Reference](#api-reference)
- [Querying information](#querying-information)
- [Example: querying runtime statistics](#example-querying-runtime-statistics)

### Overview
AIStore supports a growing number and variety of RESTful operations. To illustrate common conventions, let's take a look at the example:

```shell
$ curl -X GET http://localhost:8080/v1/daemon?what=config
```

This command queries the AIStore configuration; at the time of this writing it'll result in a JSON output that looks as follows:

> {"smap":{"":{"node_ip_addr":"","daemon_port":"","daemon_id":"","direct_url":""},"15205:8081":{"node_ip_addr":"localhost","daemon_port":"8081","daemon_id":"15205:8081","direct_url":"http://localhost:8081"},"15205:8082":{"node_ip_addr":"localhost","daemon_port":"8082","daemon_id":"15205:8082","direct_url":"http://localhost:8082"},"15205:8083":{"node_ip_addr":"localhost","daemon_port":"8083","daemon_id":"15205:8083","direct_url":"http://localhost:8083"}},"version":5}

Notice the 4 (four) ubiquitous elements in the `curl` command line above:

1. HTTP verb aka method.

In the example, it's a GET but it can also be POST, PUT, and DELETE. For a brief summary of the standard HTTP verbs and their CRUD semantics, see, for instance, this [REST API tutorial](http://www.restapitutorial.com/lessons/httpmethods.html).

2. URL path: hostname or IP address of one of the AIStore servers.

By convention, a RESTful operation performed on a AIStore proxy server usually implies a "clustered" scope. Exceptions include querying
proxy's own configuration via `?what=config` query string parameter.

3. URL path: version of the REST API, resource that is operated upon, and possibly more forward-slash delimited specifiers.

For example: /v1/cluster where 'v1' is the currently supported API version and 'cluster' is the resource.

4. Control message in the query string parameter, e.g. `?what=config`.

> Combined, all these elements tell the following story. They specify the most generic action (e.g., GET) and designate the target aka "resource" of this action: e.g., an entire cluster or a given daemon. Further, they may also include context-specific and query string encoded control message to, for instance, distinguish between getting system statistics (`?what=stats`) versus system configuration (`?what=config`).

Note that 'localhost' in the examples below is mostly intended for developers and first time users that run the entire AIStore system on their Linux laptops. It is implied, however, that the gateway's IP address or hostname is used in all other cases/environments/deployment scenarios.

### API Reference

| Operation | HTTP action | Example |
|--- | --- | ---|
| Unregister storage target | DELETE /v1/cluster/daemon/daemonID | `curl -i -X DELETE http://localhost:8080/v1/cluster/daemon/15205:8083` |
| Register storage target | POST /v1/cluster/register | `curl -i -X POST -H 'Content-Type: application/json' -d '{"node_ip_addr": "172.16.175.41", "daemon_port": "8083", "daemon_id": "43888:8083", "direct_url": "http://172.16.175.41:8083"}' http://localhost:8083/v1/cluster/register` |
| Set primary proxy forcefully(primary proxy)| PUT /v1/daemon/proxy/proxyID | `curl -i -X PUT -G http://localhost:8083/v1/daemon/proxy/23ef189ed  --data-urlencode "frc=true" --data-urlencode "can=http://localhost:8084"`  <sup id="a8">[8](#ft8)</sup>|
| Update individual AIStore daemon (proxy or target) configuration | PUT {"action": "setconfig", "name": "some-name", "value": "other-value"} /v1/daemon | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "setconfig","name": "stats_time", "value": "1s"}' http://localhost:8081/v1/daemon`<br>Please see [runtime configuration](#runtime-configuration) for the option list |
| Set cluster-wide configuration (proxy) | PUT {"action": "setconfig", "name": "some-name", "value": "other-value"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "setconfig","name": "stats_time", "value": "1s"}' http://localhost:8080/v1/cluster`<br>Please see [runtime configuration](#runtime-configuration) for the option list |
| Shutdown target/proxy | PUT {"action": "shutdown"} /v1/daemon | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "shutdown"}' http://localhost:8082/v1/daemon` |
| Shutdown cluster (proxy) | PUT {"action": "shutdown"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "shutdown"}' http://localhost:8080/v1/cluster` |
| Rebalance cluster (proxy) | PUT {"action": "rebalance"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "rebalance"}' http://localhost:8080/v1/cluster` |
| Get object (proxy) | GET /v1/objects/bucket-name/object-name | `curl -L -X GET http://localhost:8080/v1/objects/myS3bucket/myobject -o myobject` <sup id="a1">[1](#ft1)</sup> |
| Read range (proxy) | GET /v1/objects/bucket-name/object-name?offset=&length= | `curl -L -X GET http://localhost:8080/v1/objects/myS3bucket/myobject?offset=1024&length=512 -o myobject` |
| Put object (proxy) | PUT /v1/objects/bucket-name/object-name | `curl -L -X PUT http://localhost:8080/v1/objects/myS3bucket/myobject -T filenameToUpload` |
| Get bucket names | GET /v1/buckets/\* | `curl -X GET http://localhost:8080/v1/buckets/*` <sup>[6](#ft6)</sup> |
| List objects in bucket | POST {"action": "listobjects", "value":{  properties-and-options... }} /v1/buckets/bucket-name | `curl -X POST -L -H 'Content-Type: application/json' -d '{"action": "listobjects", "value":{"props": "size"}}' http://localhost:8080/v1/buckets/myS3bucket` <sup id="a2">[2](#ft2)</sup> |
| Rename/move object (local buckets) | POST {"action": "rename", "name": new-name} /v1/objects/bucket-name/object-name | `curl -i -X POST -L -H 'Content-Type: application/json' -d '{"action": "rename", "name": "dir2/DDDDDD"}' http://localhost:8080/v1/objects/mylocalbucket/dir1/CCCCCC` <sup id="a3">[3](#ft3)</sup> |
| Copy object | PUT /v1/objects/bucket-name/object-name?from_id=&to_id= | `curl -i -X PUT http://localhost:8083/v1/objects/mybucket/myobject?from_id=15205:8083&to_id=15205:8081` <sup id="a4">[4](#ft4)</sup> |
| Delete object | DELETE /v1/objects/bucket-name/object-name | `curl -i -X DELETE -L http://localhost:8080/v1/objects/mybucket/mydirectory/myobject` |
| Evict object from cache | DELETE '{"action": "evict"}' /v1/objects/bucket-name/object-name | `curl -i -X DELETE -L -H 'Content-Type: application/json' -d '{"action": "evict"}' http://localhost:8080/v1/objects/mybucket/myobject` |
| Create local bucket (proxy) | POST {"action": "createlb"} /v1/buckets/bucket-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "createlb"}' http://localhost:8080/v1/buckets/abc` |
| Destroy local bucket (proxy) | DELETE {"action": "destroylb"} /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action": "destroylb"}' http://localhost:8080/v1/buckets/abc` |
| Rename local bucket (proxy) | POST {"action": "renamelb"} /v1/buckets/bucket-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "renamelb", "name": "newname"}' http://localhost:8080/v1/buckets/oldname` |
| Set bucket props (proxy) | PUT {"action": "setprops"} /v1/buckets/bucket-name | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"setprops", "value": {"next_tier_url": "http://localhost:8082", "cloud_provider": "ais", "read_policy": "cloud", "write_policy": "next_tier"}}' 'http://localhost:8080/v1/buckets/abc'` |
| Prefetch a list of objects | POST '{"action":"prefetch", "value":{"objnames":"[o1[,o]]"[, deadline: string][, wait: bool]}}' /v1/buckets/bucket-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"prefetch", "value":{"objnames":["o1","o2","o3"], "deadline": "10s", "wait":true}}' http://localhost:8080/v1/buckets/abc` <sup>[5](#ft5)</sup> |
| Prefetch a range of objects| POST '{"action":"prefetch", "value":{"prefix":"your-prefix","regex":"your-regex","range","min:max" [, deadline: string][, wait:bool]}}' /v1/buckets/bucket-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"prefetch", "value":{"prefix":"__tst/test-", "regex":"\\d22\\d", "range":"1000:2000", "deadline": "10s", "wait":true}}' http://localhost:8080/v1/buckets/abc` <sup>[5](#ft5)</sup> |
| Delete a list of objects | DELETE '{"action":"delete", "value":{"objnames":"[o1[,o]]"[, deadline: string][, wait: bool]}}' /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action":"delete", "value":{"objnames":["o1","o2","o3"], "deadline": "10s", "wait":true}}' http://localhost:8080/v1/buckets/abc` <sup>[5](#ft5)</sup> |
| Delete a range of objects| DELETE '{"action":"delete", "value":{"prefix":"your-prefix","regex":"your-regex","range","min:max" [, deadline: string][, wait:bool]}}' /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action":"delete", "value":{"prefix":"__tst/test-", "regex":"\\d22\\d", "range":"1000:2000", "deadline": "10s", "wait":true}}' http://localhost:8080/v1/buckets/abc` <sup>[5](#ft5)</sup> |
| Evict a list of objects | DELETE '{"action":"evict", "value":{"objnames":"[o1[,o]]"[, deadline: string][, wait: bool]}}' /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action":"evict", "value":{"objnames":["o1","o2","o3"], "dea1dline": "10s", "wait":true}}' http://localhost:8080/v1/buckets/abc` <sup>[5](#ft5)</sup> |
| Evict a range of objects| DELETE '{"action":"evict", "value":{"prefix":"your-prefix","regex":"your-regex","range","min:max" [, deadline: string][, wait:bool]}}' /v1/buckets/bucket-name | `curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action":"evict", "value":{"prefix":"__tst/test-", "regex":"\\d22\\d", "range":"1000:2000", "deadline": "10s", "wait":true}}' http://localhost:8080/v1/buckets/abc` <sup>[5](#ft5)</sup> |
| Get bucket props | HEAD /v1/buckets/bucket-name | `curl -L --head http://localhost:8080/v1/buckets/mybucket` |
| Get object props | HEAD /v1/objects/bucket-name/object-name | `curl -L --head http://localhost:8080/v1/objects/mybucket/myobject` |
| Check if an object is cached | HEAD /v1/objects/bucket-name/object-name | `curl -L --head http://localhost:8080/v1/objects/mybucket/myobject?check_cached=true` |
| Set primary proxy (primary proxy only)| PUT /v1/cluster/proxy/new primary-proxy-id | `curl -i -X PUT http://localhost:8080/v1/cluster/proxy/26869:8080` |
| Disable mountpath in target | POST {"action": "disable", "value": "/existing/mountpath"} /v1/daemon/mountpaths | `curl -X POST -L -H 'Content-Type: application/json' -d '{"action": "disable", "value":"/mount/path"}' http://localhost:8083/v1/daemon/mountpaths`<sup>[7](#ft7)</sup> |
| Enable mountpath in target | POST {"action": "enable", "value": "/existing/mountpath"} /v1/daemon/mountpaths | `curl -X POST -L -H 'Content-Type: application/json' -d '{"action": "enable", "value":"/mount/path"}' http://localhost:8083/v1/daemon/mountpaths`<sup>[7](#ft7)</sup> |
| Add mountpath in target | PUT {"action": "add", "value": "/new/mountpath"} /v1/daemon/mountpaths | `curl -X PUT -L -H 'Content-Type: application/json' -d '{"action": "add", "value":"/mount/path"}' http://localhost:8083/v1/daemon/mountpaths` |
| Remove mountpath from target | DELETE {"action": "remove", "value": "/existing/mountpath"} /v1/daemon/mountpaths | `curl -X DELETE -L -H 'Content-Type: application/json' -d '{"action": "remove", "value":"/mount/path"}' http://localhost:8083/v1/daemon/mountpaths` |
___
<a name="ft1">1</a>: This will fetch the object "myS3object" from the bucket "myS3bucket". Notice the -L - this option must be used in all AIStore supported commands that read or write data - usually via the URL path /v1/objects/. For more on the -L and other useful options, see [Everything curl: HTTP redirect](https://ec.haxx.se/http-redirects.html).

<a name="ft2">2</a>: See the List Bucket section for details. [↩](#a2)

<a name="ft3">3</a>: Notice the -L option here and elsewhere. [↩](#a3)

<a name="ft4">4</a>: Advanced usage only. [↩](#a4)

<a name="ft5">5</a>: See the List/Range Operations section for details.

<a name="ft6">6</a>: Query string parameter `?local=true` can be used to retrieve just the local buckets.

<a name="ft7">7</a>: The request returns an HTTP status code 204 if the mountpath is already enabled/disabled or 404 if mountpath was not found.

<a name="ft8">8</a>: Advanced usage only. Use it when the cluster is in split-brain mode. E.g, if the original primary proxy's network gets down for a while, the rest proxies vote and select new primary. After network is back the original proxy does not join the new primary automatically. It results in two primary proxies in a cluster. [↩](#a8)

### Querying information

AIStore provides an extensive list of RESTful operations to retrieve cluster current state:

| Operation | HTTP action | Example |
|--- | --- | ---|
| Get cluster map | GET /v1/daemon | `curl -X GET http://localhost:8080/v1/daemon?what=smap` |
| Get proxy or target configuration| GET /v1/daemon | `curl -X GET http://localhost:8080/v1/daemon?what=config` |
| Get proxy/target info | GET /v1/daemon | `curl -X GET http://localhost:8083/v1/daemon?what=daemoninfo` |
| Get cluster statistics (proxy) | GET /v1/cluster | `curl -X GET http://localhost:8080/v1/cluster?what=stats` |
| Get target statistics | GET /v1/daemon | `curl -X GET http://localhost:8083/v1/daemon?what=stats` |
| Get rebalance statistics (proxy) | GET /v1/cluster | `curl -X GET 'http://localhost:8080/v1/cluster?what=xaction&props=rebalance'` |
| Get prefetch statistics (proxy) | GET /v1/cluster | `curl -X GET 'http://localhost:8080/v1/cluster?what=xaction&props=prefetch'` |
| Get list of target's filesystems (target) | GET /v1/daemon?what=mountpaths | `curl -X GET http://localhost:8084/v1/daemon?what=mountpaths` |
| Get list of all targets' filesystems (proxy) | GET /v1/cluster?what=mountpaths | `curl -X GET http://localhost:8080/v1/cluster?what=mountpaths` |
| Get target bucket list | GET /v1/daemon | `curl -X GET http://localhost:8083/v1/daemon?what=bucketmd` |

### Example: querying runtime statistics

```shell
$ curl -X GET http://localhost:8080/v1/cluster?what=stats
```

This single command causes execution of multiple `GET ?what=stats` requests within the AIStore cluster, and results in a JSON-formatted consolidated output that contains both http proxy and storage targets request counters, as well as per-target used/available capacities. For example:

<img src="images/ais-get-stats.png" alt="AIStore statistics" width="440">

More usage examples can be found in the [the source](ais/tests/regression_test.go).

