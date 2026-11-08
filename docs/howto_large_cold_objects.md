# Reading Large Objects from Remote Buckets

When a client reads an object from a remote bucket (`s3://`, `gs://`, `az://`, `oc://`) and the object is not yet in the cluster, AIS performs a _cold GET_: the target fetches the object from the backend, stores it, and serves it. The regular path normally uses one backend request and finishes caching before serving the client; buckets with auto-chunking enabled read large objects in parallel, and the native OCI backend has its own multipart reader. For multi-gigabyte objects, a single backend connection can limit throughput and delay the response.

AIS offers several ways to fetch large objects in parallel or ahead of time. Which one fits depends on the client you use, and on whether you know the objects in advance.

## Table of Contents

- [Summary](#summary)
- [Request scope vs. bucket scope](#request-scope-vs-bucket-scope)
- [Concurrent reads of a cold object](#concurrent-reads-of-a-cold-object)
- [Prefetch before the workload runs](#prefetch-before-the-workload-runs)
- [Blob-download job](#blob-download-job)
- [GET with blob download](#get-with-blob-download)
  - [Go](#go)
  - [Python SDK](#python-sdk)
  - [HTTP headers](#http-headers)
  - [Boto3 and other S3 clients](#boto3-and-other-s3-clients)
- [Cold GET into a chunked bucket](#cold-get-into-a-chunked-bucket)
- [Streaming cold GET](#streaming-cold-get)
- [OCI buckets](#oci-buckets)
- [What does not help](#what-does-not-help)
- [Why isn't blob download automatic?](#why-isnt-blob-download-automatic)
- [Limits and defaults](#limits-and-defaults)
- [References](#references)

## Summary

| Option | Clients | Parallel backend reads | Scope | Best for |
|---|---|---|---|---|
| [Prefetch](#prefetch-before-the-workload-runs) with `--blob-threshold` | CLI, Go, Python | yes, for objects at or above the threshold | bucket: chunking config wins | datasets known in advance |
| [Blob-download job](#blob-download-job) | CLI, Go, Python | yes | job: explicit tunables apply | specific large objects |
| [GET with blob download](#get-with-blob-download) | Go, Python, any HTTP client, boto3 via an event hook | yes | request: client tunables apply | on-demand reads of large cold objects |
| [Cold GET into a chunked bucket](#cold-get-into-a-chunked-bucket) | any, including unmodified S3 clients | yes, up to [`chunks.cold_readers`](#number-of-readers) per GET (4 by default) | bucket: chunking config | on-demand reads of large cold objects, no client changes |
| [Streaming cold GET](#streaming-cold-get) | any, including unmodified S3 clients | does not add parallelism | bucket or cluster feature flag | faster first byte, no client changes |
| [OCI native backend](#oci-buckets) | any, `oc://` buckets only | yes | node environment | data stored in OCI |

Prefetch with `--blob-threshold`, blob-download jobs, and GET with blob download use the [blob downloader](/docs/blob_downloader.md): it splits an object into chunks, reads them from the backend concurrently, and writes them into the cluster as a chunked object. Cold GET into a chunked bucket performs parallel range reads within the request itself, without a blob-download job. Native OCI multipart downloading is implemented separately.

Options that need no changes to the reading application: prefetch, blob-download jobs, cold GET into a chunked bucket, streaming cold GET, and the native OCI backend.

## Request scope vs. bucket scope

Settings apply differently to explicit blob downloads and bucket maintenance:

* **A GET request or standalone blob-download job** uses its explicit blob-download tunables. Chunk size and worker count are subject to supported bounds, automatic worker tuning, and resource admission.
* **Prefetch** follows the bucket's chunking configuration (`chunks`) when auto-chunking is enabled: its chunk size and object-size limit take precedence over prefetch options, as described below.
* **A regular cold GET** into a bucket with auto-chunking enabled follows the same configuration: `chunks.objsize_limit` determines eligibility for parallel range reads, and `chunks.chunk_size` the size of each range (see [below](#cold-get-into-a-chunked-bucket)).
* **Rechunk** derives the desired layout from the bucket's `chunks` configuration, including restoring monolithic storage where that configuration calls for it. It reorganizes cached objects; it does not parallelize a future cold GET.

## Concurrent reads of a cold object

A regular cold GET holds the object's write lock while it reads from the backend and stores the object in the cluster, then downgrades it to a read lock to serve the response.

* Other regular GETs of the same object, including range reads and archive extraction, wait for the cold GET to complete and are then served from the in-cluster copy. Concurrent readers do not start separate backend downloads.
* Requests that find the object cold at the same time contend for the write lock: one proceeds, and the others back off, retry, and then wait as above. If the contention persists beyond `timeout.cold_get_conflict` (default 5s), the request fails with HTTP 409 and error type `ErrBusy`. Only the backoff counts toward this timeout, not the time spent waiting for an in-progress cold GET.
* The wait lasts for the entire cold GET: clients reading large cold objects need timeouts longer than the time to download and store the object.
* A client that disconnects does not cancel the cold GET; the object is still stored. [Streaming cold GET](#streaming-cold-get) is the exception.
* If the backend download or cache finalization fails, waiting requests recheck whether an in-cluster copy is available and retry the cold GET if needed.

## Prefetch before the workload runs

If you know which objects your workload will read, prefetch them first. Prefetch is a multi-object job that brings remote objects into the cluster; with `--blob-threshold`, large objects are fetched via the blob downloader, and the rest via regular cold GET.

```console
$ ais prefetch s3://training-data --prefix shards/ --blob-threshold 256MiB --wait --progress
```

The effective threshold and chunk size follow the bucket:

* with bucket auto-chunking enabled, objects at or above `max(--blob-threshold, chunks.objsize_limit)` use the blob downloader, with `chunks.chunk_size`; `--blob-chunk-size` is ignored (with a warning);
* with auto-chunking disabled, objects at or above `--blob-threshold` use the blob downloader, and `--blob-chunk-size` applies.

Under critical memory pressure, or when resource admission rejects a blob download, prefetch falls back to a regular cold GET for that object. Background downloads can proceed under high memory pressure. Positive thresholds below the prefetch minimum (see [limits](#limits-and-defaults)) are raised to it; zero disables blob downloading for prefetch.

For details and threshold selection, see [Blob Downloader: prefetch with blob-threshold](/docs/blob_downloader.md#2-prefetch-with-blob-threshold).

## Blob-download job

To fetch specific large objects ahead of time, start a blob-download job:

```console
$ ais blob-download s3://training-data/models/checkpoint-0420.bin --num-workers 16 --wait --progress

$ ais blob-download s3://training-data --list "shard-000001.tar,shard-000002.tar" --wait --progress
```

From Go, the same job is `api.BlobDownload()`; from Python, it is `Object.blob_download()`.

## GET with blob download

A client can ask for blob download as part of a regular GET. The target then:

1. serves the object as usual if it is already in the cluster;
2. otherwise, checks the object's size in the remote bucket;
3. below the threshold, or below the GET minimum for blob download (see [limits](#limits-and-defaults)), performs a regular cold GET;
4. at or above it, runs a blob download and streams the object to the client as chunks arrive.

Notes:

* blob download applies only to remote buckets and to whole-object reads; downloading an entire archive (shard) is supported;
* range reads and archive extraction use the regular GET path when requested with the `Ais-Blob-Threshold` header, and fail with `400 Bad Request` when combined with explicit `Ais-Blob-Download`; the Python SDK additionally validates option combinations before sending the request (see below);
* when the cluster cannot admit another blob download (resource pressure, range-reader budget, remote backoff), an explicit `Ais-Blob-Download` request fails with `429 Too Many Requests` (retry, or read without blob download), while a request with `Ais-Blob-Threshold` only proceeds as a regular GET;
* an explicit blob GET can also return a busy error when the object is being written; it does not always wait as an ordinary GET would;
* chunk size and number of workers, if specified, apply to this request (see [request scope](#request-scope-vs-bucket-scope)).

### Go

```go
args := api.GetArgs{
	Writer:        w,
	BlobThreshold: 256 * cos.MiB, // blob download for cold objects >= 256MiB
}
_, err := api.GetObject(bp, bck, objName, &args)
```

### Python SDK

> `BlobDownloadConfig.threshold` and `read_timeout` require aistore Python SDK 2.1.0 or later. Earlier SDKs support `BlobDownloadConfig` with chunk size and worker count.

```python
from aistore.sdk import Client
from aistore.sdk.blob_download_config import BlobDownloadConfig

client = Client("https://ais-gateway.example.com:51080")
obj = client.bucket("training-data", provider="s3").object("models/checkpoint-0420.bin")

# blob download for cold objects >= 256MiB
reader = obj.get_reader(
    blob_download_config=BlobDownloadConfig(threshold=256 * 1024 * 1024)
)

# blob download regardless of size (above the GET minimum), with explicit tunables
reader = obj.get_reader(
    blob_download_config=BlobDownloadConfig(chunk_size="8MiB", num_workers="16", read_timeout="30s")
)
```

An omitted `threshold` forces blob download for eligible cold objects; zero selects regular GET. `blob_download_config` cannot be combined with `byte_range` or client-side `num_workers`. For a range read, omit the blob configuration; to retrieve an archive member, use `archive_config` with a threshold rather than forcing blob download. See [Blob Downloader](/docs/blob_downloader.md#3-streaming-get).

### HTTP headers

Any HTTP client can request blob download with these headers:

| Header | Value | Meaning |
|---|---|---|
| `Ais-Blob-Threshold` | size in bytes | use blob download for cold objects at or above this size |
| `Ais-Blob-Download` | `true` | use blob download regardless of size (above the GET minimum, see [limits](#limits-and-defaults)) |
| `Ais-Blob-Chunk` | size, e.g. `8MiB` | chunk size (optional) |
| `Ais-Blob-Workers` | integer | number of concurrent backend readers (optional) |
| `Ais-Blob-Read-Timeout` | duration, e.g. `30s` | per-attempt backend read timeout (optional) |

The headers work through the S3 API endpoint as well.

### Boto3 and other S3 clients

Unmodified S3 clients do not send AIS headers. Boto3 (and other botocore-based applications) can add one with an event hook:

```python
from aistore.botocore_patch import botocore  # AIS redirect handling; see s3compat.md
import boto3

client = boto3.client(
    "s3",
    endpoint_url="https://ais-gateway.example.com:51080/s3",
    aws_access_key_id="ACCESS_KEY",
    aws_secret_access_key="SECRET_KEY",
)

BLOB_THRESHOLD = 256 * 1024 * 1024  # bytes

def add_blob_threshold(request, **kwargs):
    # blob download does not apply to range reads; leave those untouched
    if "Range" not in request.headers:
        request.headers["Ais-Blob-Threshold"] = str(BLOB_THRESHOLD)

client.meta.events.register("before-sign.s3.GetObject", add_blob_threshold)

resp = client.get_object(Bucket="training-data", Key="models/checkpoint-0420.bin")
with open("checkpoint-0420.bin", "wb") as f:
    for chunk in resp["Body"].iter_chunks(chunk_size=8 * 1024 * 1024):
        f.write(chunk)
```

Use `get_object` for large objects. `download_file` and other managed transfers split large objects into range reads, which the hook leaves alone, and which do not help on cold objects (see [What does not help](#what-does-not-help)).

For general boto3 setup, see [S3 compatibility: Boto3 examples](/docs/s3compat.md#boto3-examples).

## Cold GET into a chunked bucket

With auto-chunking enabled, a regular cold GET of a large object reads it from the backend in parallel. Neither client changes nor AIS headers are required, so unmodified S3 clients benefit as well:

```console
$ ais bucket props set s3://training-data chunks.objsize_limit=256MiB chunks.chunk_size=128MiB
```

Changing the chunk layout also starts rechunk of the objects already in the cluster. See [Storage layout](/docs/storage_svcs.md#rechunk).

For an eligible object, the target:

1. issues the usual full-object GET to the backend, and uses it for the first chunk only;
2. reads the remaining chunks as concurrent range requests, up to `chunks.cold_readers` readers per GET (see [below](#number-of-readers));
3. pins every range read to the remote object version (ETag, generation, or version) returned by the initial GET;
4. writes the chunks into the cluster, finalizes the object, and serves it through the regular GET path, including range reads and archive extraction.

If a range read returns HTTP 412 (the remote object changed), the target discards the partial result and fetches the new version sequentially, within the same request.

Remote throttling (HTTP 429 or 503) depends on the bucket's [backend rate limiting](/docs/rate_limit.md):

* with `rate_limit.backend` enabled, the rate limiter paces backend requests, including range reads, and retries throttling errors reported by supported cloud backends. Remote AIS 429/503 responses fail the request;
* otherwise, if a range read is throttled, the target discards the partial result and refetches sequentially within the same request. A throttle on the initial full GET fails that request. In both cases, cold GETs on that target for buckets without backend rate limiting use sequential reads for twice `timeout.cold_get_conflict` (10s by default), with a 5s minimum and `timeout.max_host_busy` as the maximum.

Other backend errors fail the request.

Locking is the same as for any [cold GET](#concurrent-reads-of-a-cold-object): the write lock is held across all range reads until the object is finalized. Concurrent readers wait for the entire download and never see a partial object; a failed parallel cold GET removes the chunks written so far.

An object qualifies when all of the following hold:

* the bucket has auto-chunking enabled, and the object is at or above `chunks.objsize_limit` and the minimum size for parallel cold GET (see [limits](#limits-and-defaults));
* the object spans at least two chunks of `chunks.chunk_size`;
* the bucket does not validate cold GETs (`checksum.validate_cold_get`) and is not erasure-coded;
* the backend reports a usable version for the object. Objects in unversioned remote AIS buckets and GCS objects served with decompressive transcoding use the regular cold GET;
* the bucket is not an `oc://` bucket; the native OCI backend uses its own multipart reader;
* the target is not under high load and, for buckets without `rate_limit.backend`, is outside the backoff window after a remote 429 or 503; otherwise the cold GET is sequential;
* the target has room in its budget for extra range workers across all cold GETs and [blob downloads](/docs/blob_downloader.md). The budget is the number of available mountpaths times a media-type multiplier (see [limits](#limits-and-defaults)). The GET's own worker also reads ranges and runs outside this budget. Near the limit, the cold GET uses fewer workers. At the limit, it is sequential.

Notes:

* parallel range reads aim to improve backend download throughput while preserving regular GET semantics: the object is cached and finalized before serving the client. A faster download can shorten the first reader's wait, but that reader still waits for the entire object;

* parallelism is bounded by the number of chunks. With the default `chunks.chunk_size` of 1 GiB, a 2 GiB object can use at most two readers. To make use of all readers, divide the typical object size by the effective reader limit and use that chunk size or smaller. Note that `chunks.cold_readers=0` defaults to a limit of four, and `1` disables parallel cold GETs altogether.

* the whole-object checksum is the one supplied by the remote, if any, as with a sequential cold GET; otherwise, chunks arrive out of order, so it is CRC32C, combined from per-chunk checksums, regardless of the bucket's checksum type. With `checksum.type` set to `none`, no checksum is stored.

### Number of readers

`chunks.cold_readers` sets the maximum number of concurrent range readers per cold GET, including the one reading the first chunk:

* `0` or unset uses the default of four readers.
* `1` bypasses AIS's parallel cold GET path.
* Values from `2` to `32` set the per-GET limit.

The value is an upper bound: the number of chunks and the target-wide budget of range readers (see [limits](#limits-and-defaults)) can reduce it, and high load disables parallel reads altogether. As with other `chunks` properties, a bucket inherits the cluster-wide value at creation. Unlike the chunking layout (`chunks.objsize_limit`, `chunks.chunk_size`, and `chunks.max_monolithic_size`), changing `chunks.cold_readers` does not start rechunk.

```console
$ ais bucket props set s3://training-data chunks.cold_readers=8
```

## Streaming cold GET

The `Streaming-Cold-GET` feature flag overlaps backend download, local writes, and client transmission, avoiding a separate read of the completed in-cluster object to serve the response. The client can therefore start receiving data before the full download completes, but also before the in-cluster object is finalized. It requires no client changes for eligible on-demand GETs.

The CLI marks this performance and integrity tradeoff as `perf,integrity-`. For example, with no cluster-wide feature flags enabled:

```console
$ ais config cluster features
PROPERTY         VALUE
features         none

...
Streaming-Cold-GET                       perf,integrity-         write and transmit cold-GET content back to user in parallel, without _finalizing_ in-cluster object
```

To enable it for a bucket:

```console
$ ais bucket props set s3://training-data features Streaming-Cold-GET
```

The flag can also be set cluster-wide (`ais config cluster features`).

Limitations:

* the flag does not add parallel backend reads; the backend retains its usual download behavior, including native OCI multipart downloading;
* it does not apply to range reads, archive extraction, buckets that validate cold GETs (`checksum.validate_cold_get`), or buckets with auto-chunking enabled (for those, see [Cold GET into a chunked bucket](#cold-get-into-a-chunked-bucket)). Reading a whole archive object is eligible;
* the write lock is held while the response is sent, so other readers of the same object also wait for the streaming client; a client that disconnects aborts the cold GET;
* the response starts before the in-cluster object is finalized. A failure while copying can truncate the response; a later cache-finalization failure can occur after the client has received all bytes;
* it is mutually exclusive with `Disable-Cold-GET`.

See [Feature flags](/docs/feature_flags.md).

## OCI buckets

The native OCI backend (`oc://`) downloads objects above its multipart threshold in parallel as part of an ordinary cold GET, without blob-download headers. It is configured per node with environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `OCI_MULTI_PART_DOWNLOAD_THRESHOLD` | 512 MiB | objects larger than this size are downloaded in parts |
| `OCI_MAX_DOWNLOAD_SEGMENT_SIZE` | 256 MiB | part size |
| `OCI_MULTI_PART_DOWNLOAD_MAX_THREADS` | 16 | maximum concurrent parts per object |

> As of v5.2, the OCI backend is pre-1.0 and receives maintenance fixes only. For other providers, including S3, use the options above.

## What does not help

**Client-side parallel range reads of a cold object.** This includes boto3's `download_file` and `TransferConfig`, the Python SDK's `get_reader(num_workers=...)`, and `ais get --mpd`.

AIS serves range reads from the in-cluster copy of an object. A range read of a cold object therefore first fetches the entire object from the backend, and concurrent range reads of the same object wait for it. That cold GET normally uses a single backend request, with two exceptions: [cold GET into a chunked bucket](#cold-get-into-a-chunked-bucket) and native OCI multipart downloading. Issuing more client-side range requests does not add backend parallelism to the cold GET; it can help serve the object once it is in the cluster.

To parallelize the backend read, enable auto-chunking for the bucket, use one of the blob-download options above, or prefetch.

## Why isn't blob download automatic?

For buckets with auto-chunking enabled, parallel cold GET is automatic: see [Cold GET into a chunked bucket](#cold-get-into-a-chunked-bucket). This addresses [GitHub issue #340](https://github.com/NVIDIA/aistore/issues/340): unmodified S3 clients get parallel cold GETs, including after cache eviction or target loss, configured server-side per bucket.

The explicit blob GET remains opt-in, because it is not a transparent replacement for a regular cold GET:

* it can return busy or admission errors;
* it applies request-specific tunables;
* it streams the response before the object is finalized, so a later failure cannot fall back to a regular cold GET.

The automatic path avoids these differences. It downloads and finalizes the object before serving it, falls back to a sequential cold GET under high load, and refetches sequentially when the remote object changes during the download. Its goal is higher backend download throughput; serving the response still waits for the entire object to be cached and finalized.

Streaming during an automatic parallel cold GET, and parallel cold GET for buckets without auto-chunking, are not supported. Future work in this direction will be guided by demand.

## Limits and defaults

Fixed values (current heuristics, not configuration), defined in [core/get_tunables.go](https://github.com/NVIDIA/aistore/blob/main/core/get_tunables.go):

| Value | Applies to | Meaning | Constant |
|---|---|---|---|
| 16 MiB | parallel cold GET | minimum object size | `ColdRangeMinSize` |
| 2 per mountpath for rotational disks or slow filesystems; 16 otherwise | parallel cold GET, blob download | target-wide budget of extra range readers; downloads without a client stream use up to half | `RRMultHDD`, `RRMultNVMe` |
| 1 per mountpath for rotational disks or slow filesystems; 4 for SSD; 8 for NVMe | multi-object jobs, blob download | default number of workers before load and size limits; blob download uses at least two | `NwpMultHDD`, `NwpMultSSD`, `NwpMultNVMe` |
| 128 KiB | GET with blob download | smaller objects use a regular GET | `MinBlobDlGetSize` |
| 1 MiB | prefetch | minimum `--blob-threshold`; smaller positive values are raised to it | `MinBlobDlPrefetchSize` |
| 4 MiB | blob download | default chunk size, when neither the request nor (for prefetch) the bucket sets it | `BlobDlChunkSize` |
| 64 MiB | GET with blob download | maximum chunk size | `BlobDlMaxStreamingChunkSize` |
| 256 MiB, 32 | blob download | size-based worker cap, including explicit requests: one per 256 MiB of object size, rounded up (at least two), and at most 32 | `BlobDlMinBytesPerWorker`, `BlobDlMaxWorkers` |

The requested number of blob-download workers is part of the API: zero (default) or 2 to 128 (`apc.MinBlobWorkers`, `apc.MaxBlobWorkers`).

## References

- [Blob Downloader](/docs/blob_downloader.md)
- [Prefetch and evict](/docs/bucket.md#prefetch-and-evict)
- [S3 compatibility](/docs/s3compat.md)
- [Feature flags](/docs/feature_flags.md)
- [Buckets: chunking configuration](/docs/bucket.md)
