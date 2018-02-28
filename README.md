DFC: Distributed File Cache with Amazon and Google Cloud backends
-----------------------------------------------------------------

## Overview

DFC is a simple distributed caching service written in Go. The service
currently consists of a single http proxy (with a well-known address)
and an arbitrary number of storage targets (aka targets):

<img src="images/dfc-overview.png" alt="DFC overview" width="440">

Users (i.e., http/https clients) connect to the proxy and execute RESTful
commands. Data then moves directly between storage targets (that cache this data)
and the requesting user.

## Getting Started

If you've already installed Go and happen to have AWS or GCP account, getting
started with DFC takes about 30 seconds and consists in executing the following
4 steps:

```
$ go get -u -v github.com/NVIDIA/dfcpub/dfc
$ cd $GOPATH/src/github.com/NVIDIA/dfcpub/dfc
$ make deploy
$ go test -v -run=down -numfiles=2 -bucket=<your bucket name>
```

The 1st command will install both the DFC source code and all its dependencies
under your configured $GOPATH.

The 3rd - deploys DFC daemons locally (for details, please see [the script](dfc/setup/deploy.sh)).

Finally, the 4th command executes a smoke test to download 2 (two) files
from your own named Amazon S3 or Google Cloud Storage bucket. Here's another example:

```
$ go test -v -run=download -args -numfiles=100 -match='a\d+' -bucket=myS3bucket
```
This will download up to 100 objects from the bucket called myS3bucket.
The names of those objects will match 'a\d+' regex.

For more testing/running command line options, please refer to [the source](dfc/main_test.go).

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

## Miscellaneous

The following sequence downloads 100 objects from the bucket "myS3bucket", and then
finds the corresponding cached files and generated logs:
```
$ go test -v -run=down -bucket=myS3bucket
$ find /tmp/dfc/ -type f | grep cache
$ find /tmp/dfc/ -type f | grep log
```

Don't forget, though, to run 'make deploy' first. 

To terminate a running DFC service and cleanup local caches, run:
```
$ make kill
$ make rmcache
```
## REST operations

DFC supports a growing number and variety of RESTful operations. To illustrate common conventions, let's take a look at the example:

```
$ curl -X GET -H 'Content-Type: application/json' -d '{"what": "config"}' http://192.168.176.128:8080/v1/cluster
```

This command queries the DFC configuration; at the time of this writing it'll result in a JSON output that looks as follows:

> {"smap":{"":{"node_ip_addr":"","daemon_port":"","daemon_id":"","direct_url":""},"15205:8081":{"node_ip_addr":"192.168.176.128","daemon_port":"8081","daemon_id":"15205:8081","direct_url":"http://192.168.176.128:8081"},"15205:8082":{"node_ip_addr":"192.168.176.128","daemon_port":"8082","daemon_id":"15205:8082","direct_url":"http://192.168.176.128:8082"},"15205:8083":{"node_ip_addr":"192.168.176.128","daemon_port":"8083","daemon_id":"15205:8083","direct_url":"http://192.168.176.128:8083"}},"version":5}

Notice the 4 (four) ubiquitous elements in the `curl` command line above:

1. HTTP verb aka method.

In the example, it's a GET but it can also be POST, PUT, and DELETE. For a brief summary of the standard HTTP verbs and their CRUD semantics, see, for instance, this [REST API tutorial](http://www.restapitutorial.com/lessons/httpmethods.html).

2. URL path: hostname or IP address of one of the DFC servers.

By convention, REST operation implies a "clustered" scope when performed on a DFC proxy server.

3. URL path: version of the REST API, resource that is operated upon, and possibly more forward-slash delimited specifiers.

For example: /v1/cluster where 'v1' is the currently supported version and 'cluster' is the resource.

4. Control message in JSON format, e.g. `{"what": "config"}`.

> Combined, all these elements tell the following story. They tell us what to do in the most generic terms (e.g., GET), designate the target aka "resource" (e.g., cluster), and may also include context-specific and JSON-encoded control message to, for instance, distinguish between getting system statistics (`{"what": "stats"}`) versus system configuration (`{"what": "config"}`).

| Operation | HTTP action | Example |
|--- | --- | ---|
| Unregister storage target (proxy only) | DELETE /v1/cluster/daemon/daemonID | `curl -i -X DELETE http://192.168.176.128:8080/v1/cluster/daemon/15205:8083` |
| Register storage target | POST /v1//daemon | `curl -i -X POST http://192.168.176.128:8083/v1/daemon` |
| Get cluster map (proxy only) | GET {"what": "smap"} /v1/cluster | `curl -X GET -H 'Content-Type: application/json' -d '{"what": "smap"}' http://192.168.176.128:8080/v1/cluster` |
| Get proxy or target configuration| GET {"what": "config"} /v1/daemon | `curl -X GET -H 'Content-Type: application/json' -d '{"what": "config"}' http://192.168.176.128:8080/v1/daemon` |
| Set proxy or target configuration | PUT {"action": "setconfig", "name": "some-name", "value": "other-value"} /v1/daemon | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "setconfig","name": "stats_time", "value": "1s"}' http://192.168.176.128:8081/v1/daemon` |
| Set cluster configuration  (proxy only) | PUT {"action": "setconfig", "name": "some-name", "value": "other-value"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "setconfig","name": "stats_time", "value": "1s"}' http://192.168.176.128:8080/v1/cluster` |
| Shutdown target | PUT {"action": "shutdown"} /v1/daemon | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "shutdown"}' http://192.168.176.128:8082/v1/daemon` |
| Shutdown cluster (proxy only) | PUT {"action": "shutdown"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "shutdown"}' http://192.168.176.128:8080/v1/cluster` |
| Rebalance cluster (proxy only) | PUT {"action": "rebalance"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "rebalance"}' http://192.168.176.128:8080/v1/cluster` |
| Get cluster statistics (proxy only) | GET {"what": "stats"} /v1/cluster | `curl -X GET -H 'Content-Type: application/json' -d '{"what": "stats"}' http://192.168.176.128:8080/v1/cluster` |
| Get target statistics | GET {"what": "stats"} /v1/daemon | `curl -X GET -H 'Content-Type: application/json' -d '{"what": "stats"}' http://192.168.176.128:8083/v1/daemon` |
| Get object (proxy only) | GET /v1/files/bucket/object | `curl -L -X GET http://192.168.176.128:8080/v1/files/myS3bucket/myS3object -o myS3object` (`*`) |
| List bucket | GET { properties-and-options... } /v1/files/bucket | `curl -X GET -L -H 'Content-Type: application/json' -d '{"props": "size"}' http://192.168.176.128:8080/v1/files/myS3bucket` (`**`) |
| Rename/move file (local buckets only) | POST {"action": "rename", "name": new-name} /v1/files/bucket | `curl -i -X POST -L -H 'Content-Type: application/json' -d '{"action": "rename", "name": "dir2/DDDDDD"}' http://192.168.176.128:8080/v1/files/mylocalbucket/dir1/CCCCCC` (`***`)|
| Copy file | PUT /v1/files/from_id/to_id/bucket/object | `curl -i -X PUT http://192.168.176.128:8083/v1/files/from_id/15205:8083/to_id/15205:8081/myS3bucket/myS3object` (`****`) |
| Delete file | DELETE /v1/files/bucket/object | `curl -i -X DELETE -L http://192.168.176.128:8080/v1/files/mybucket/mydirectory/myobject` |
| Evict file from cache | DELETE '{"action": "evict"}' /v1/files/bucket/object | `curl -i -X DELETE -L -H 'Content-Type: application/json' -d '{"action": "evict"}' http://192.168.176.128:8080/v1/files/mybucket/myobject` |
| Create local bucket (proxy only) | POST {"action": "createlb"} /v1/files/bucket | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "createlb"}' http://192.168.176.128:8080/v1/files/abc` |
| Destroy local bucket (proxy only) | DELETE /v1/files/bucket | `curl -i -X DELETE http://192.168.176.128:8080/v1/files/abc` |

> (`*`) This will fetch the object "myS3object" from the bucket "myS3bucket". Notice the -L - this option must be used in all DFC supported commands that read or write data - usually via the URL path /v1/files/. For more on the -L and other useful options, see [Everything curl: HTTP redirect](https://ec.haxx.se/http-redirects.html).

> (`**`) The API returns object names and, optionally, their properties including sizes, creation times, checksums, and more. The properties-and-options specifier must be a JSON-encoded structure, for instance '{"props": "size"}' (see above). An empty structure '{}' results in getting just the names of the objects (from the specified bucket) with no other metadata.

> (`***`) Notice the -L option here and elsewhere.

> (`****`) Advanced usage only.

### Example: querying runtime statistics

```
$ curl -X GET -H 'Content-Type: application/json' -d '{"what": "stats"}' http://192.168.176.128:8080/v1/cluster
```

This single command causes execution of multiple `GET {"what": "stats"}` requests within the DFC cluster, and results in a JSON-formatted consolidated output that contains both http proxy and storage targets request counters, as well as per-target used/available capacities. For example:

<img src="images/dfc-get-stats.png" alt="DFC statistics" width="440">

More usage examples can be found in the [the source](dfc/regression_test.go).

### Example: listing local and Cloud buckets

To list objects in the smoke/ subdirectory of a given bucket called 'myBucket', and to include in the listing their respective sizes and checksums, run:

```
$ curl -X GET -L -H 'Content-Type: application/json' -d '{"props": "size, checksum", "prefix": "smoke/"}' http://192.168.176.128:8080/v1/files/myBucket
```

This request will produce an output that (in part) may look as follows:

<img src="images/dfc-ls-subdir.png" alt="DFC list directory" width="440">

For many more examples, please refer to the dfc/*_test.go files in the repository.

## Cache Rebalancing

DFC rebalances its cached content based on the DFC cluster map. When cache servers join or leave the cluster, the next updated version (aka generation) of the cluster map gets centrally replicated to all storage targets. Each target then starts, in parallel, a background thread to traverse its local caches and recompute locations of the cached items.

Thus, the rebalancing process is completely decentralized. When a single server joins (or goes down in a) cluster of N servers, approximately 1/Nth of the content will get rebalanced via direct target-to-target transfers.
