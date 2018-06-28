DFC: Distributed File Cache with Amazon and Google Cloud backends
-----------------------------------------------------------------

## Overview

DFC is a simple distributed caching service written in Go. The service
consists of arbitrary number of gateways (realized as http **proxy** servers),
and any number of storage **targets** utilizing local disks:

<img src="images/dfc-overview-mp.png" alt="DFC overview" width="480">

Users connect to the proxies and execute RESTful commands. Data then moves
directly between storage targets that cache this data and the requesting http(s) clients.

## Prerequisites

* Linux or Mac
* Go 1.8 or later
* Optionally, extended attributes (xattrs)
* Optionally, Amazon (AWS) or Google Cloud (GCP) account

The capability called [extended attributes](https://en.wikipedia.org/wiki/Extended_file_attributes),
or xattrs, is currently supported by all mainstream filesystems. Unfortunately, xattrs may not
always be enabled in the OS kernel (configurations) - the fact that can be easily
found out by running setfattr (Linux) or xattr (macOS) command as shown in this
[single-host local deployment script](dfc/setup/deploy.sh).
If this is the case - that is, if you happen not to have xattrs handy, you can configure DFC
not to use them at all (section **Configuration** below).

To get started, it is also optional (albeit desirable) to have access to an Amazon S3 or GCP bucket.
If you don't have Amazon and/or Google Cloud accounts, you can use DFC local buckets as illustrated
a) in the **API** section below and b) in the [test sources](dfc/tests/regression_test.go).
Note that local and Cloud-based buckets support the same API with minor exceptions
(only local buckets can be renamed, for instance).

## Getting Started

### Quick start with Docker

To get started quickly with a containerized, one-proxy, one-target deployment of DFC, see [Getting started quickly with DFC using Docker](docker/quick_start/README.md).

### Regular installation

If you've already installed Go and [dep](https://github.com/golang/dep), getting started with DFC takes about 30 seconds:

```
$ go get -u -v github.com/NVIDIA/dfcpub/dfc
$ cd $GOPATH/src/github.com/NVIDIA/dfcpub/dfc
$ dep ensure
$ make deploy
$ go test ./tests -v -run=down -numfiles=2 -bucket=<your bucket name>
```

The 1st and 3rd commands will install the DFC source code and all its versioned dependencies
under your configured $GOPATH.

The 4th - deploys DFC daemons locally (for details, please see [the script](dfc/setup/deploy.sh)). If you want to enable optional DFC authentication server(AuthN) execute instead:

```
$ CREDDIR=/tmp/creddir AUTHENABLED=true make deploy

```
For more details about AuthN server please see [AuthN documetation](./authn/README.md)

Finally, for the last 4th command to work, you'll need to have a name - the name of a bucket.
The bucket could be an AWS or GCP based one, or a DFC-own so-called "local bucket".

Assuming the bucket exists, the 'go test' command above will download 2 (two) objects. Similarly:


```
$ go test ./tests -v -run=download -args -numfiles=100 -match='a\d+' -bucket=myS3bucket
```

downloads up to 100 objects from the bucket called myS3bucket, whereby names of those objects
will match 'a\d+' regex.

For more testing commands and command line options, please refer to the corresponding
[README](dfc/tests/README.md) and/or the [test sources](dfc/tests/).

For other useful commands, see the [Makefile](dfc/Makefile).

## Helpful Links: Go

* [How to write Go code](https://golang.org/doc/code.html)

* [How to install Go binaries and tools](https://golang.org/doc/install)

* [The Go Playground](https://play.golang.org/)

* [Go language support for Vim](https://github.com/fatih/vim-go)
  (note: if you are a VIM user vim-go plugin is invaluable)

* [Go lint tools to check Go source for errors and warnings](https://github.com/alecthomas/gometalinter)

## Helpful Links: AWS

* [AWS Command Line Interface](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html)

* [AWS S3 Tutorial For Beginners](https://www.youtube.com/watch?v=LfBn5Y1X0vE)


## Configuration

DFC configuration is consolidated in a single [JSON file](dfc/setup/config.sh) where all of the knobs must be self-explanatory and the majority of those, except maybe just a few, have pre-assigned default values. The notable exceptions include:

<img src="images/dfc-config-1.png" alt="DFC configuration: TCP port and URL" width="512">

and

<img src="images/dfc-config-2-commented.png" alt="DFC configuration: local filesystems" width="548">

### Disabling extended attributes

To make sure that DFC does not utilize xattrs, configure "checksum"="none" and "versioning"="none" for all
targets in a DFC cluster. This can be done via the [common configuration "part"](dfc/setup/config.sh)
that'd be further used to deploy the cluster.

### Enabling HTTPS

To switch from HTTP protocol to an encrypted HTTPS, configure "use_https"="true" and modify
"server_certificate" and "server_key" values so they point to your OpenSSL cerificate and key
files respectively.

## Miscellaneous

The following sequence downloads 100 objects from the bucket called "myS3bucket":

```
$ go test -v -run=down -bucket=myS3bucket
```

and then finds the corresponding cached objects in the local bucket and cloud buckets, respectively:

```
$ find /tmp/dfc -type f | grep local
$ find /tmp/dfc -type f | grep cloud
```

This, of course, assumes that all DFC daemons are local and non-containerized
 - don't forget to run 'make deploy' to make it happen - and that the "test_fspaths"
section in their respective configurations points to the /tmp/dfc directory
(see section **Configuration** for details).

Further, to locate all the logs, run:

```
$ find $LOGDIR -type f | grep log
```

where $LOGDIR is the configured logging directory as per [DFC configuration](dfc/setup/config.sh).


To terminate a running DFC service and cleanup local caches, run:
```
$ make kill
$ make rmcache
```

## REST operations


DFC supports a growing number and variety of RESTful operations. To illustrate common conventions, let's take a look at the example:

```
$ curl -X GET http://localhost:8080/v1/daemon?what=config
```

This command queries the DFC configuration; at the time of this writing it'll result in a JSON output that looks as follows:

> {"smap":{"":{"node_ip_addr":"","daemon_port":"","daemon_id":"","direct_url":""},"15205:8081":{"node_ip_addr":"localhost","daemon_port":"8081","daemon_id":"15205:8081","direct_url":"http://localhost:8081"},"15205:8082":{"node_ip_addr":"localhost","daemon_port":"8082","daemon_id":"15205:8082","direct_url":"http://localhost:8082"},"15205:8083":{"node_ip_addr":"localhost","daemon_port":"8083","daemon_id":"15205:8083","direct_url":"http://localhost:8083"}},"version":5}

Notice the 4 (four) ubiquitous elements in the `curl` command line above:

1. HTTP verb aka method.

In the example, it's a GET but it can also be POST, PUT, and DELETE. For a brief summary of the standard HTTP verbs and their CRUD semantics, see, for instance, this [REST API tutorial](http://www.restapitutorial.com/lessons/httpmethods.html).

2. URL path: hostname or IP address of one of the DFC servers.

By convention, a RESTful operation performed on a DFC proxy server usually implies a "clustered" scope. Exceptions include querying
proxy's own configuration via `?what=config` query string parameter.

3. URL path: version of the REST API, resource that is operated upon, and possibly more forward-slash delimited specifiers.

For example: /v1/cluster where 'v1' is the currently supported API version and 'cluster' is the resource.

4. Control message in the query string parameter, e.g. `?what=config`.

> Combined, all these elements tell the following story. They specify the most generic action (e.g., GET) and designate the target aka "resource" of this action: e.g., an entire cluster or a given daemon. Further, they may also include context-specific and query string encoded control message to, for instance, distinguish between getting system statistics (`?what=stats`) versus system configuration (`?what=config`).

Note that 'localhost' in the examples below is mostly intended for developers and first time users that run the entire DFC system on their Linux laptops. It is implied, however, that the gateway's IP address or hostname is used in all other cases/environments/deployment scenarios.

| Operation | HTTP action | Example |
|--- | --- | ---|
| Unregister storage target | DELETE /v1/cluster/daemon/daemonID | `curl -i -X DELETE http://localhost:8080/v1/cluster/daemon/15205:8083` |
| Register storage target | POST /v1/cluster/register | `curl -i -X POST -H 'Content-Type: application/json' -d '{"node_ip_addr": "172.16.175.41", "daemon_port": "8083", "daemon_id": "43888:8083", "direct_url": "http://172.16.175.41:8083"}' http://localhost:8083/v1/cluster/register` |
| Get cluster map | GET /v1/daemon | `curl -X GET http://localhost:8080/v1/daemon?what=smap` |
| Get proxy or target configuration| GET /v1/daemon | `curl -X GET http://localhost:8080/v1/daemon?what=config` |
| Update individual DFC daemon (proxy or target) configuration | PUT {"action": "setconfig", "name": "some-name", "value": "other-value"} /v1/daemon | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "setconfig","name": "stats_time", "value": "1s"}' http://localhost:8081/v1/daemon` |
| Update individual DFC daemon (proxy or target) configuration | PUT {"action": "setconfig", "name": "some-name", "value": "other-value"} /v1/daemon | ` curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"setconfig","name":"loglevel","value":"4"}' http://localhost:8080/v1/daemon` |
| Set cluster-wide configuration (proxy) | PUT {"action": "setconfig", "name": "some-name", "value": "other-value"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "setconfig","name": "stats_time", "value": "1s"}' http://localhost:8080/v1/cluster` |
| Shutdown target/proxy | PUT {"action": "shutdown"} /v1/daemon | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "shutdown"}' http://localhost:8082/v1/daemon` |
| Shutdown cluster (proxy) | PUT {"action": "shutdown"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "shutdown"}' http://localhost:8080/v1/cluster` |
| Rebalance cluster (proxy) | PUT {"action": "rebalance"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "rebalance"}' http://localhost:8080/v1/cluster` |
| Get cluster statistics (proxy) | GET /v1/cluster | `curl -X GET http://localhost:8080/v1/cluster?what=stats` |
| Get rebalance statistics (proxy) | GET /v1/cluster | `curl -X GET 'http://localhost:8080/v1/cluster?what=xaction&props=rebalance'` |
| Get target statistics | GET /v1/daemon | `curl -X GET http://localhost:8083/v1/daemon?what=stats` |
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
| Set bucket props (proxy) | PUT {"action": "setprops"} /v1/buckets/bucket-name | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"setprops", "value": {"next_tier_url": "http://localhost:8082", "cloud_provider": "dfc", "read_policy": "cloud", "write_policy": "next_tier"}}' 'http://localhost:8080/v1/buckets/abc'` |
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
___
<a name="ft1">1</a>: This will fetch the object "myS3object" from the bucket "myS3bucket". Notice the -L - this option must be used in all DFC supported commands that read or write data - usually via the URL path /v1/objects/. For more on the -L and other useful options, see [Everything curl: HTTP redirect](https://ec.haxx.se/http-redirects.html).

<a name="ft2">2</a>: See the List Bucket section for details. [↩](#a2)

<a name="ft3">3</a>: Notice the -L option here and elsewhere. [↩](#a3)

<a name="ft4">4</a>: Advanced usage only. [↩](#a4)

<a name="ft5">5</a>: See the List/Range Operations section for details.

<a name="ft6">6</a>: Query string parameter `?local=true` can be used to retrieve just the local buckets.

### Example: querying runtime statistics

```
$ curl -X GET http://localhost:8080/v1/cluster?what=stats
```

This single command causes execution of multiple `GET ?what=stats` requests within the DFC cluster, and results in a JSON-formatted consolidated output that contains both http proxy and storage targets request counters, as well as per-target used/available capacities. For example:

<img src="images/dfc-get-stats.png" alt="DFC statistics" width="440">

More usage examples can be found in the [the source](dfc/tests/regression_test.go).

## List Bucket

The ListBucket API returns a page of object names (and, optionally, their properties including sizes, creation times, checksums, and more), in addition to a token allowing the next page to be retrieved.

### properties-and-options
The properties-and-options specifier must be a JSON-encoded structure, for instance '{"props": "size"}' (see examples). An empty structure '{}' results in getting just the names of the objects (from the specified bucket) with no other metadata.

| Property/Option | Description | Value |
| --- | --- | --- |
| props | The properties to return with object names | A comma-separated string containing any combination of: "checksum","size","atime","ctime","iscached","bucket","version","targetURL". <sup id="a6">[6](#ft6)</sup> |
| time_format | The standard by which times should be formatted | Any of the following [golang time constants](http://golang.org/pkg/time/#pkg-constants): RFC822, Stamp, StampMilli, RFC822Z, RFC1123, RFC1123Z, RFC3339. The default is RFC822. |
| prefix | The prefix which all returned objects must have | For example, "my/directory/structure/" |
| pagemarker | The token identifying the next page to retrieve | Returned in the "nextpage" field from a call to ListBucket that does not retrieve all keys. When the last key is retrieved, NextPage will be the empty string |
| pagesize | The maximum number of object names returned in response | Default value is 1000. GCP and local bucket support greater page sizes. AWS is unable to return more than [1000 objects in one page](https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html). |\b

 <a name="ft6">6</a>: The objects that exist in the Cloud but are not present in the DFC cache will have their atime property empty (""). The atime (access time) property is supported for the objects that are present in the DFC cache. [↩](#a6)

### Example: listing local and Cloud buckets

To list objects in the smoke/ subdirectory of a given bucket called 'myBucket', and to include in the listing their respective sizes and checksums, run:

```
$ curl -X POST -L -H 'Content-Type: application/json' -d '{"action": "listobjects", "value":{"props": "size, checksum", "prefix": "smoke/"}}' http://localhost:8080/v1/buckets/myBucket
```

This request will produce an output that (in part) may look as follows:

<img src="images/dfc-ls-subdir.png" alt="DFC list directory" width="440">

For many more examples, please refer to the [test sources](dfc/tests/) in the repository.

### Example: Listing All Pages

The following Go code retrieves a list of all of object names from a named bucket (note: error handling omitted):

```go
// e.g. proxyurl: "http://localhost:8080"
url := proxyurl + "/v1/buckets/" + bucket

msg := &dfc.ActionMsg{Action: dfc.ActListObjects}
fullbucketlist := &dfc.BucketList{Entries: make([]*dfc.BucketEntry, 0)}
for {
    // 1. First, send the request
    jsbytes, _ := json.Marshal(msg)
    r, _ := http.DefaultClient.Post(url, "application/json", bytes.NewBuffer(jsbytes))

    defer func(r *http.Response){
        r.Body.Close()
    }(r)

    // 2. Unmarshal the response
    pagelist := &dfc.BucketList{}
    respbytes, _ := ioutil.ReadAll(r.Body)
    _ = json.Unmarshal(respbytes, pagelist)

    // 3. Add the entries to the list
    fullbucketlist.Entries = append(fullbucketlist.Entries, pagelist.Entries...)
    if pagelist.PageMarker == "" {
        // If PageMarker is the empty string, this was the last page
        break
    }
    // If not, update PageMarker to the next page returned from the request.
    msg.GetPageMarker = pagelist.PageMarker
}
```

Note that the PageMarker returned as a part of pagelist is for the next page.

## Cache Rebalancing

DFC rebalances its cached content based on the DFC cluster map. When cache servers join or leave the cluster, the next updated version (aka generation) of the cluster map gets centrally replicated to all storage targets. Each target then starts, in parallel, a background thread to traverse its local caches and recompute locations of the cached items.

Thus, the rebalancing process is completely decentralized. When a single server joins (or goes down in a) cluster of N servers, approximately 1/Nth of the content will get rebalanced via direct target-to-target transfers.

## List/Range Operations

DFC provides two APIs to operate on groups of objects: List, and Range. Both of these share two optional parameters:

| Parameter | Description | Default |
|--- | --- | --- |
| deadline | The amount of time before the request expires formatted as a [golang duration string](https://golang.org/pkg/time/#ParseDuration). A timeout of 0 means no timeout.| 0 |
| wait | If true, a response will be sent only when the operation completes or the deadline passes. When false, a response will be sent once the operation is initiated. When setting wait=true, ensure your request has a timeout at least as long as the deadline. | false |

### List

List APIs take a JSON array of object names, and initiate the operation on those objects.

| Parameter | Description |
| --- | --- |
| objnames | JSON array of object names |

### Range

Range APIs take an optional prefix, a regular expression, and a numeric range. A matching object name will begin with the prefix and contain a number that satisfies both the regex and the range as illustrated below.


| Parameter | Description |
| --- | --- |
| prefix | The prefix that all matching object names will begin with. Empty prefix ("") will match all names. |
| regex | The regular expression, represented as an escaped string, to match the number embedded in the object name. Note that the regular expression applies to the entire name - the prefix (if provided) is not excluded. |
| range | Represented as "min:max", corresponding to the inclusive range from min to max. Either or both of min and max may be empty strings (""), in which case they will be ignored. If regex is an empty string, range will be ignored. |

#### Examples
| Prefix | Regex |  Escaped Regex | Range | Matches<br>(the match is highlighted) | Doesn't Match |
| --- | --- | --- | --- | --- | --- |
| "__tst/test-" | `"\d22\d"` | `"\\d22\\d"` | "1000:2000" | "__tst/test-`1223`"<br>"__tst/test-`1229`-4000.dat"<br>"__tst/test-1111-`1229`.dat"<br>"__tst/test-`1222`2-40000.dat" | "__prod/test-1223"<br>"__tst/test-1333"<br>"__tst/test-2222-4000.dat" |
| "a/b/c" | `"^\d+1\d"` | `"^\\d+1\\d"` | ":100000" | "a/b/c/`110`"<br>"a/b/c/`99919`-200000.dat"<br>"a/b/c/`2314`video-big" | "a/b/110"<br>"a/b/c/d/110"<br>"a/b/c/video-99919-20000.dat"<br>"a/b/c/100012"<br>"a/b/c/30331" |

## Multiple Proxies

DFC can be run with multiple proxies. When there are multiple proxies, one of them is the primary proxy, and any others are secondary proxies. The primary proxy is the only one allowed to be used for actions related to the Smap (Registration, Local Bucket actions). The URL of the current primary proxy must be specified in the config file at the time a proxy or target is run. On startup, a proxy will start as Primary if the environment variable DFCPRIMARYPROXY is set to any non-empty string. If it is unset, it will start as primary if its id matches the id of the current primary proxy in the configuration file, unless the command line variable -proxyurl is set.

When any target or proxy discovers that the primary proxy is not working (because a keepalive fails), they intitiate a vote to determine the next primary proxy.
The election process is as follows:

- A candidate is selected via Highest Random Weight
- That candidate is notified that an election is beginning
- After the candidate confirms that the current primary proxy is down, it sends vote requests to all other proxies/targets
- Each recipient responds affirmatively if they have not recently communicated with the primary proxy, and the candidate proxy has the Highest Random Weight according to their local Smap.
- If the candidate receives a majority of affirmative responses it sends a confirmation message to all other targets and proxies and becomes the primary proxy.
- Upon reception of the confirmation message, a recipient removes the previous primary proxy from their local Smap, and updates the primary proxy to the winning candidate.

### Proxy Startup Process

While it is running, a proxy persists the cluster map when it changes, loading it as the discovery cluster map on startup. When a proxy starts up as primary, it performs the following process:

- It requests the cluster map from each proxy and target in the union of the current cluster map and the discovery cluster map.
- If any target or proxy signaled that a vote is in progress, it waits a short time, and restarts the process.
- If not, it picks the cluster map with the maximum version to be the current cluster map.
- If it is the primary proxy in that cluster map: It continues as primary.
- If it is not the primary proxy in that cluster map: It registers to the primary proxy from that cluster map, becoming non-primary.

This process allows a proxy to be rerun with the same command and environment variables, even if it should no longer be primary.

## WebDAV

WebDAV aka "Web Distributed Authoring and Versioning" is the IETF standard that defines HTTP extension for collaborative file management and editing. DFC WebDAV server is a reverse proxy (with interoperable WebDAV on the front and DFC's RESTful interface on the back) that can be used with any of the popular [WebDAV-compliant clients](https://en.wikipedia.org/wiki/Comparison_of_WebDAV_software).

For information on how to run it and details, please refer to the [WebDAV README](webdav/README.md).

## Extended Action (xaction)

Extended actions (xactions) are the operations that may take seconds, sometimes even minutes, to execute, that run asynchronously, have one of the enumerated kinds, start/stop times, and xaction-specific statistics.

Examples of the supported extended actions include:

* Cluster-wide rebalancing
* LRU-based eviction
* Prefetch
* Consensus voting when electing a new leader

At the time of this writing the corresponding RESTful API can query two xaction kinds: "rebalance" and "prefetch". The following command, for instance, will query the cluster for an active/pending rebalancing operation (if presently running), and report associated statistics:

```
$ curl -X GET http://localhost:8080/v1/cluster?what=xaction&props=rebalance
```

## Multi-tiering

DFC can be deployed with multiple consecutive DFC clusters aka "tiers" sitting behind a primary tier. This provides the option to use a multi-level cache architecture.

<img src="images/multi-tier.png" alt="DFC multi-tier overview" width="680">

Tiering is configured at the bucket level by setting bucket properties, for example:

```
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"setprops", "value": {"next_tier_url": "http://localhost:8082", "read_policy": "cloud", "write_policy": "next_tier"}}' 'http://localhost:8080/v1/buckets/<bucket-name>'
```

The following fields are used to configure multi-tiering:

* `next_tier_url`: an absolute URI corresponding to the primary proxy of the next tier configured for the bucket specified
* `read_policy`: `"next_tier"` or `"cloud"` (defaults to `"next_tier"` if not set)
* `write_policy`: `"next_tier"` or `"cloud"` (defaults to `"cloud"` if not set)

For the `"next_tier"` policy, a tier will read or write to the next tier specified by the `next_tier_url` field. On failure, it will read or write to the cloud (aka AWS or GCP).

For the `"cloud"` policy, a tier will read or write to the cloud (aka AWS or GCP) directly from that tier.

Currently, the endpoints which support multi-tier policies are the following:

* GET /v1/objects/bucket-name/object-name
* PUT /v1/objects/bucket-name/object-name

## DFC Limitations

- The current primary proxy is determined at startup, through either the configuration file or the -proxyurl command line variable. This means that if the primary proxy changes, the configuration file of any new targets joining the cluster must change. This limitation does not apply to targets that are a part of the cluster when the primary proxy changes, fails, or rejoins.
- DFC does not currently handle the case where the primary proxy and the next highest random weight proxy both fail at the same time, so this will result in no new primary proxy being chosen.
- Currently, only the candidate primary proxy keeps track of the fact that a vote is happening. This means that if the candidate primary proxy is not in the discovery cluster map when a proxy starts up, a proxy may start as primary at the same time as an election completes, changing the primary proxy.

