---
layout: post
title: Blob downloader
permalink: /docs/cli/blob-downloader
redirect_from:
 - /cli/blob-downloader.md/
 - /docs/cli/blob-downloader.md/
---

AIS comes with built-in blob downloading facility that employs multiple concurrent readers to speed-up reading very large remote objects.

In terms of its from/to operation, blob downloading can be compared to [prefetching](/docs/cli/object.md#prefetch-objects).

More precisely, the list of "comparables" includes:
* [prefetch remote content](/docs/cli/object.md#prefetch-objects)
* [copy (list, range, and/or prefix) selected objects or entire (in-cluster or remote) buckets](/docs/cli/bucket.md#copy-list-range-andor-prefix-selected-objects-or-entire-in-cluster-or-remote-buckets)

In all these cases, destination of the remote content is aistore cluster (and not the client requesting the operation).

In CLI, `ais blob-download` will run a job, or multiple jobs, to download user-specified remote blob(s). Command line options and examples follow below.

## Options

```console
$ ais blob-download --help
NAME:
   ais blob-download - run a job to download large object(s) from remote storage to aistore cluster, e.g.:
    - 'blob-download s3://abc/largefile --chunk-size=2mb --progress'          - download one blob at a given chunk size
    - 'blob-download s3://abc --list "f1, f2, f3" --num-workers=4 --progress' - use 4 concurrent readers to download each of the 3 blobs
   Note: when _not_ using '--progress' option, run 'ais show job' to monitor.

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
   --num-workers value  number of concurrent blob-downloading workers (readers); system default when omitted or zero
   --wait               wait for an asynchronous operation to finish (optionally, use '--timeout' to limit the waiting time)
   --timeout value      maximum time to wait for a job to finish; if omitted: wait forever or until Ctrl-C;
                        valid time units: ns, us (or µs), ms, s (default), m, h
   --latest             check in-cluster metadata and, possibly, GET, download, prefetch, or copy the latest object version
                        from the associated remote bucket:
                        - provides operation-level control over object versioning (and version synchronization)
                          without requiring to change bucket configuration
                        - the latter can be done using 'ais bucket props set BUCKET versioning'
                        - see also: 'ais ls --check-versions', 'ais cp', 'ais prefetch', 'ais get'
   --help, -h           show help
```

## Usage example

```console
$ ais blob-download s3://abc --list "file-2gb, file-100mb" --chunk-size=2mb --progress

blob-download[Qxz3EClVN]
blob-download[xvC3nClSF]
s3://abc/file-2gb  513 MiB / 2 GiB      [==============>-----------------------------------------------] 24 %
s3://abc/file-100mb 44.17 MiB / 100 MiB [==========================>-----------------------------------] 44 %
```
