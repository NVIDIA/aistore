---
layout: post
title: Blob Downloader
permalink: /docs/blob_downloader
redirect_from:
 - /blob_downloader.md/
 - /docs/blob_downloader.md/
---

## Background

AIStore supports multiple ways to populate itself with existing datasets, including (but not limited to):

* **on demand**, often during the first epoch;
* **copy** entire bucket or its selected virtual subdirectories;
* **copy** multiple matching objects;
* **archive** multiple objects
* **prefetch** remote bucket or parts of thereof;
* **download** raw http(s) addressable directories, including (but not limited to) Cloud storages;
* **promote** NFS or SMB shares accessible by one or multiple (or all) AIS target nodes;

> The on-demand "way" is maybe the most popular, whereby users just start running their workloads against a [remote bucket](docs/providers.md) with AIS cluster positioned as an intermediate fast tier.

But there's more. In particular, v3.22 introduces a special facility to download very large remote objects a.k.a. BLOBs.

We call this (new facility):

## Blob Downloader

AIS blob downloader features multiple concurrent workers - chunk readers - that run in parallel and, well, read certain fixed-size chunks from the remote object.

User can control (or tune-up) the number of workers and the chunk size(s), among other configurable tunables. The tunables themselves are backed up by system defaults - in particular:

| Name | Comment |
| --- | --- |
| default chunk size  | 2 MiB |
| minimum chunk size  | 32 KiB |
| maximum chunk size  | 16 MiB |
| default number of workers | 4 |

In addition to massively parallel reading (**), blob downloader also:

* stores and _finalizes_ (checksums, replicates, erasure codes - as per bucket configuration) downloaded object;
* optionally(**), concurrently transmits the loaded content to requesting user.

> (**) assuming sufficient and _not_ rate-limited network bandwidth

> (**) see [GET](#2-get-via-blob-downloader) section below

## Flavors

For users, blob downloader is currently(**) available in 3 distinct flavors:

| Name | Go API | CLI |
| --- | --- | --- |
| 1. `blob-download` job | [api.BlobDownload](https://github.com/NVIDIA/aistore/blob/main/api/blob.go) | `ais blob-download`  |
| 2. `GET` request | [api.GetObject](https://github.com/NVIDIA/aistore/blob/main/api/object.go) and friends  | `ais get`  |
| 3. `prefetch` job | [api.Prefetch](https://github.com/NVIDIA/aistore/blob/main/api/multiobj.go) | `ais prefetch`  |


> (**) There's a plan to integrate blob downloader with [Internet Downloader](downloader.md) and, generally, all supported mechanisms that one way or another read remote objects and files.

> (**) At the time of this writing, none of the above is supported (yet) in our [Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore/sdk).

Rest of this text talks separately about each of the 3 "flavors" providing additional details, insights, and context.

## 1. Usage

To put some of the blob downloader's functionality into immediate perspective, let's see some CLI:

```console
$ ais blob-download --help
NAME:
   ais blob-download - run a job to download large object(s) from remote storage to aistore cluster, e.g.:
     - 'blob-download s3://ab/largefile --chunk-size=2mb --progress'       - download one blob at a given chunk size
     - 'blob-download s3://ab --list "f1, f2" --num-workers=4 --progress'  - use 4 concurrent readers to download each of the 2 blobs
   When _not_ using '--progress' option, run 'ais show job' to monitor.

USAGE:
   ais blob-download [command options] BUCKET/OBJECT_NAME

OPTIONS:
   --refresh value      interval for continuous monitoring;
                        valid time units: ns, us (or µs), ms, s (default), m, h
   --progress           show progress bar(s) and progress of execution in real time
   --list value         comma-separated list of object or file names, e.g.:
                        --list 'o1,o2,o3'
                        --list "abc/1.tar, abc/1.cls, abc/1.jpeg"
                        or, when listing files and/or directories:
                        --list "/home/docs, /home/abc/1.tar, /home/abc/1.jpeg"
   --chunk-size value   chunk size in IEC or SI units, or "raw" bytes (e.g.: 4mb, 1MiB, 1048576, 128k; see '--units')
   --num-workers value  number of concurrent blob-downloading workers (readers); system default when omitted or zero (default: 0)
   --wait               wait for an asynchronous operation to finish (optionally, use '--timeout' to limit the waiting time)
   --timeout value      maximum time to wait for a job to finish; if omitted: wait forever or until Ctrl-C;
                        valid time units: ns, us (or µs), ms, s (default), m, h
   --latest             check in-cluster metadata and, possibly, GET, download, prefetch, or copy the latest object version
                        from the associated remote bucket:
                        - provides operation-level control over object versioning (and version synchronization)
                          without requiring to change bucket configuration
                        - the latter can be done using 'ais bucket props set BUCKET versioning'
                        - see also: 'ais ls --check-versions', 'ais cp', 'ais prefetch', 'ais get'
```

## 2. GET via blob downloader

Some of the common use cases boil down to the following:

* user "knows" the size of an object to be read (or downloaded) from remote (cold) storage;
* there's also an idea of a certain size _threshold_ beyond which the latency of the operation becomes prohibitive.

Thus, when the size in question is greater than the _threshold_ there's a motivation to speed up.

To meet this motivation, AIS now supports `GET` request with additional (and optional) http headers:

| Header | Values (examples) | Comments |
| --- | --- | --- |
| `ais-blob-download` | "true", ""  | NOTE: to engage blob downloader, this http header must be present and must be "true" (or "y", "yes", "on" case-insensitive) |
| `ais-blob-chunk` | "1mb", "1234567", "128KiB"  | [system defaults](#blob-downloader) above |
| `ais-blob-workers` | "3", "7", "16"  | ditto |

* HTTP headers that AIStore recognizes and supports are always prefixed with "ais-". For the most recently updated list (of headers), please see [the source](https://github.com/NVIDIA/aistore/blob/main/api/apc/headers.go).

## 3. Prefetch remote buckets w/ blob size threshold

`Prefetch` is another batch operation, one of the supported job types that can be invoked both via Go or Python call, or command line.

The idea of size threshold applies here as well, with the only difference being the _scope_: single object in [GET](#2-get-via-blob-downloader), all matching objects in `prefetch`.

> The `prefetch` operation supports multi-object selection via the usual `--list`, `--template`, and `--prefix` options.

But first thing first, let's see an example.

```console
$ ais ls s3://abc
NAME             SIZE            CACHED
aisloader        39.30MiB        no
largefile        5.76GiB         no
smallfile        100.00MiB       no
```

Given the bucket (above), we now run `prefetch` with 1MB size threshold:

```console
$ ais prefetch s3://abc --blob-threshold 1mb
prefetch-objects[E-w0gjdm1z]: prefetch entire bucket s3://abc. To monitor the progress, run 'ais show job E-w0gjdm1z'
```

But notice, `prefetch` stats do not move:

```console
$ ais show job E-w0gjdm1z
NODE             ID              KIND                 BUCKET     OBJECTS      BYTES       START        END     STATE
CAHt8081         E-w0gjdm1z      prefetch-listrange   s3://abc   -            -           10:08:24     -       Running
```

And that is because it is the blob downloader that actually does all the work behind the scenes:

```console
$ ais show job blob-download
blob-download[lP3Lpe5jJ]
NODE             ID              KIND            BUCKET         OBJECTS      BYTES        START        END     STATE
CAHt8081         lP3Lpe5jJ       blob-download   s3://abc       -            20.00MiB     10:08:25     -       Running
```

The work that shortly thereafter results in:

```console
$ ais ls s3://abc
NAME             SIZE            CACHED
aisloader        39.30MiB        yes
largefile        5.76GiB         yes
smallfile        100.00MiB       yes
```
