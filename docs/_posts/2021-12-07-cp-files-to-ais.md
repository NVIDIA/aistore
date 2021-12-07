---
layout: post
title:  "Copying existing file datasets in two easy steps"
date:   Dec 7, 2021
author: Alex Aizman
categories: aistore migration replication
---

AIStore supports [numerous ways](https://github.com/NVIDIA/aistore/blob/master/docs/overview.md#existing-datasets) to copy, download, or otherwise transfer existing datasets. Much depends on *where is* the dataset in question, and whether we can access this location using some of sort of HTTP-based interface. I'll put more references below. But in this post, let's talk about datasets that already reside *on premises*.

> The term *on premises* here includes a super-wide spectrum of use cases ranging from commercial high-end (and, possibly, distributed) filers to your own Linux or Mac (where you may, or may not, want to run AIStore itself, etc.).

Ultimately, the only precondition is that there is a *directory* you can access that contains files to migrate or copy. It turns out that **everything else** can be done in two easy steps:

1) run local http server
2) [prefetch](https://github.com/NVIDIA/aistore/blob/master/docs/cli/object.md#operations-on-lists-and-ranges) or [download](https://github.com/NVIDIA/aistore/blob/master/docs/downloader.md) the files.

Implementation-wise, both #1 and #2 have multiple variations, and we'll consider at least some of them below. But first, let's take a look at an example:

```bash
# Let's assume, the files we want to copy are located under /tmp/abc:
$ cd /tmp
$ ls abc
hello-world
shard-001.tar
...
shard-999.tar

# Step 1. run local http server =============================
$ python3 -m http.server --bind 0.0.0.0 51061
  # use AIS CLI to make sure the files are readable
$ ais object get http://localhost:51061/abc/hello-world

# Step 2. prefetch the files ===================
  # keep using AIS CLI to list HTTP buckets
  # (and note that AIS will create one on the fly after the very first successful `GET`)
$ ais ls ht://
ht://ZDE1YzE0NzhiNWFkMQ
  # run batch `prefetch` job to load bash-expansion templated names from this bucket
$ ais job start prefetch ht://ZDE1YzE0NzhiNWFkMQ --template 'shard-{001..999}.tar'
```

Here we run Python's own `http.server` to listen on port `51061` and serve the files from the directory that we have previously `cd-ed` into (`/tmp`, in the example).

Of course, the port, the directory, and the filenames above are all randomly chosen for purely **illustrative purposes**. The main point the example is trying to make is that HTTP connectivity of any kind immediately opens up a number of easy ways to migrate or replicate any data that exists in files.  

As far as aforementioned *implementation variations*, they include running, for instance, Go-based HTTP server instead of the Python's:

```go
# cat htserver.go
1 package main
2
3 import (
4 	"net/http"
5 )
6
7 func main() {
8 	http.ListenAndServe(":52062", http.FileServer(http.Dir("/tmp")))
9 }
# Step 1. run local http server =============================
$ go run htserver.go
```

 and using AIS [downloader](https://github.com/NVIDIA/aistore/blob/master/docs/downloader.md) extension instead of the exemplified above multi-object `prefetch`:

```bash
# Step 2. download files =============================
  # `hostname` below indicates the hostname or IP address of the machine where
  # we are running `go run htserver.go` command;
  # also note that the destination bucket `ais://abc` will be created iff it doesn't exist
$ ais job start download "http://hostname:52062/abc/shard-{001..010}.tar" ais://abc
GUsQcjEPY
Run `ais show job download GUsQcjEPY --progress` to monitor the progress.
$ ais ls ais://abc
NAME             SIZE
shard-001.tar    151.13KiB
shard-002.tar    147.98KiB
shard-003.tar    101.45KiB
shard-004.tar    150.37KiB
shard-005.tar    146.00KiB
shard-006.tar    130.70KiB
shard-007.tar    129.04KiB
shard-008.tar    157.53KiB
shard-009.tar    161.32KiB
shard-010.tar    124.11KiB
```

Other than a different, albeit still arbitrary, listening port and a user-selected destination bucket, other minor differences include explicitly naming the directory from which we want to serve files. And also an attempt to indicate that if *it* works for `localhost` it'll work for any valid `hostname` or IP address. For as long as the latter is visible over HTTP.

## References

* [Using AIS Downloader](https://github.com/NVIDIA/aistore/blob/master/docs/cli/download.md)
* [Multi-object operations](https://github.com/NVIDIA/aistore/blob/master/docs/cli/object.md#operations-on-lists-and-ranges)
* [Promote files and directories](https://github.com/NVIDIA/aistore/blob/master/docs/cli/object.md#promote-files-and-directories)
