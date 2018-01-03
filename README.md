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
