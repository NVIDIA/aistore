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
$ go get -u -v github.com/NVIDIA/dfcpub
$ cd $GOPATH/src/github.com/NVIDIA/dfcpub/dfc
$ make deploy
$ go test -v -run=down -numfiles=2 -bucket=<your bucket name>
```

The 1st command will install both the DFC source code and all its dependencies 
under your configured $GOPATH.

The 3rd - deploys DFC daemons locally (for details, please see [the script](dfc/setup/deploy.sh)).

Finally, the 4th command executes a smoke test to download 2 (two) files
from your own named Amazon S3 or Google Cloud Storage bucket.

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
$ find /tmp/nvidia/ -type f | grep cache
$ find /tmp/nvidia/ -type f | grep log
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
|Unregister storage target| DELETE /v1/cluster/daemon/daemonID | `curl -i -X DELETE http://192.168.176.128:8080/v1/cluster/daemon/15205:8081` |
|Get cluster configuration| GET {"what": "config"} /v1/cluster | `curl -X GET -H 'Content-Type: application/json' -d '{"what": "config"}' http://192.168.176.128:8080/v1/cluster` |
| Shutdown target | PUT {"action": "shutdown"} /v1/daemon | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "shutdown"}' http://192.168.176.128:8082/v1/daemon` |
| Shutdown DFC cluster | PUT {"action": "shutdown"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "shutdown"}' http://192.168.176.128:8080/v1/cluster` |
| Synchronize cluster map | PUT {"action": "syncsmap"} /v1/cluster | `curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "syncsmap"}' http://192.168.176.128:8080/v1/cluster` |
| Get cluster statistics | GET {"what": "stats"} /v1/cluster | `curl -X GET -H 'Content-Type: application/json' -d '{"what": "stats"}' http://192.168.176.128:8080/v1/cluster` |
| Get target statistics | GET {"what": "stats"} /v1/daemon | `curl -X GET -H 'Content-Type: application/json' -d '{"what": "stats"}' http://192.168.176.128:8083/v1/daemon` |
| Get object | GET /v1/files/bucket-name/object-name | `curl -L -X GET http://192.168.176.128:8080/v1/files/myS3bucket/myS3object -o myS3object` (*) |
| Get bucket contents | GET /v1/files/bucket-name | `curl -L -X GET http://192.168.176.128:8080/v1/files/myS3bucket` |

> (*) This will fetch the object "myS3object" from the bucket "myS3bucket". Notice the -L - this option must be used in all DFC supported commands that read or write data - usually via the URL path /v1/files/. For more on the -L and other useful options, see [Everything curl: HTTP redirect](https://ec.haxx.se/http-redirects.html).

### Example: querying runtime statistics


```
$ curl -X GET -H 'Content-Type: application/json' -d '{"what": "stats"}' http://192.168.176.128:8080/v1/cluster
```

This single command causes execution of multiple `GET {"what": "stats"}` requests within the DFC cluster, and results in a JSON-formatted consolidated output containing both summary and per-target counters, for example:

>{"proxystats":{"numget":95,"numpost":3,"numdelete":0,"numerr":0},"storstats":{"15205:8081":{"numget":26,"numcoldget":4,"bytesloaded":8388608,"bytesevicted":0,"filesevicted":0,"numerr":0},"15205:8082":{"numget":31,"numcoldget":2,"bytesloaded":4194304,"bytesevicted":0,"filesevicted":0,"numerr":0},"15205:8083":{"numget":38,"numcoldget":2,"bytesloaded":4194304,"bytesevicted":0,"filesevicted":0,"numerr":0}}}

When fed into any compatible JSON viewer, the printout may look something as follows:

<img src="images/dfc-get-stats.png" alt="DFC GET stats" width="200">
