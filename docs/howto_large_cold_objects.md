# Reading Large Objects from Remote Buckets

When a client reads an object from a remote bucket (`s3://`, `gs://`, `az://`, `oc://`) and the object is not yet in the cluster, AIS performs a _cold GET_: the target fetches the object from the backend, stores it, and serves it. The regular path normally uses one backend request and finishes caching before serving the client; the native OCI backend has its own multipart reader. For multi-gigabyte objects, a single backend connection can limit throughput and delay the response.

AIS offers several ways to fetch large objects in parallel or ahead of time. Which one fits depends on the client you use, and on whether you know the objects in advance.

## Table of Contents

- [Summary](#summary)
- [Request scope vs. bucket scope](#request-scope-vs-bucket-scope)
- [Prefetch before the workload runs](#prefetch-before-the-workload-runs)
- [Blob-download job](#blob-download-job)
- [GET with blob download](#get-with-blob-download)
  - [Go](#go)
  - [Python SDK](#python-sdk)
  - [HTTP headers](#http-headers)
  - [Boto3 and other S3 clients](#boto3-and-other-s3-clients)
- [Streaming cold GET](#streaming-cold-get)
- [OCI buckets](#oci-buckets)
- [What does not help](#what-does-not-help)
- [Why isn't blob download automatic?](#why-isnt-blob-download-automatic)
- [References](#references)

## Summary

| Option | Clients | Parallel backend reads | Scope | Best for |
|---|---|---|---|---|
| [Prefetch](#prefetch-before-the-workload-runs) with `--blob-threshold` | CLI, Go, Python | yes, for objects at or above the threshold | bucket: chunking config wins | datasets known in advance |
| [Blob-download job](#blob-download-job) | CLI, Go, Python | yes | job: explicit tunables apply | specific large objects |
| [GET with blob download](#get-with-blob-download) | Go, Python, any HTTP client, boto3 via an event hook | yes | request: client tunables apply | on-demand reads of large cold objects |
| [Streaming cold GET](#streaming-cold-get) | any, including unmodified S3 clients | does not add parallelism | bucket or cluster feature flag | faster first byte, no client changes |
| [OCI native backend](#oci-buckets) | any, `oc://` buckets only | yes | node environment | data stored in OCI |

Prefetch with `--blob-threshold`, blob-download jobs, and GET with blob download use the [blob downloader](/docs/blob_downloader.md): it splits an object into chunks, reads them from the backend concurrently, and writes them into the cluster as a chunked object. Native OCI multipart downloading is implemented separately.

Options that need no changes to the reading application: prefetch, blob-download jobs, streaming cold GET, and the native OCI backend.

## Request scope vs. bucket scope

Settings apply differently to explicit blob downloads and bucket maintenance:

* **A GET request or standalone blob-download job** uses its explicit blob-download tunables. Chunk size and worker count are subject to supported bounds, automatic worker tuning, and resource admission.
* **Prefetch** follows the bucket's chunking configuration (`chunks`) when auto-chunking is enabled: its chunk size and object-size limit take precedence over prefetch options, as described below.
* **Rechunk** derives the desired layout from the bucket's `chunks` configuration, including restoring monolithic storage where that configuration calls for it. It reorganizes cached objects; it does not parallelize a future cold GET.

## Prefetch before the workload runs

If you know which objects your workload will read, prefetch them first. Prefetch is a multi-object job that brings remote objects into the cluster; with `--blob-threshold`, large objects are fetched via the blob downloader, and the rest via regular cold GET.

```console
$ ais prefetch s3://training-data --prefix shards/ --blob-threshold 256MiB --wait --progress
```

The effective threshold and chunk size follow the bucket:

* with bucket auto-chunking enabled, objects at or above `max(--blob-threshold, chunks.objsize_limit)` use the blob downloader, with `chunks.chunk_size`; `--blob-chunk-size` is ignored (with a warning);
* with auto-chunking disabled, objects at or above `--blob-threshold` use the blob downloader, and `--blob-chunk-size` applies.

Under critical memory pressure, or when resource admission rejects a blob download, prefetch falls back to a regular cold GET for that object. Background downloads can proceed under high memory pressure. Positive thresholds below 1 MiB are raised to 1 MiB; zero disables blob downloading for prefetch.

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
3. below the threshold, or below 128 KiB, performs a regular cold GET;
4. at or above it, runs a blob download and streams the object to the client as chunks arrive.

Notes:

* blob download applies only to remote buckets and to whole-object reads; downloading an entire archive (shard) is supported;
* range reads and archive extraction use the regular GET path when requested with the `Ais-Blob-Threshold` header, and fail with `400 Bad Request` when combined with explicit `Ais-Blob-Download`; the Python SDK additionally validates option combinations before sending the request (see below);
* when the cluster cannot admit another blob download, the request fails with `429 Too Many Requests`; retry, or read without blob download;
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

# blob download regardless of size (above 128KiB), with explicit tunables
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
| `Ais-Blob-Download` | `true` | use blob download regardless of size (above 128 KiB) |
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

## Streaming cold GET

The `Streaming-Cold-GET` feature flag changes how a regular cold GET is served: the backend response is written to the cluster and to the client at the same time, so the client can start receiving data before the full download completes. It requires no client changes for eligible on-demand GETs.

```console
$ ais bucket props set s3://training-data features Streaming-Cold-GET
```

The flag can also be set cluster-wide (`ais config cluster features`).

Limitations:

* the flag does not add parallel backend reads; the backend retains its usual download behavior, including native OCI multipart downloading;
* it does not apply to range reads, archive extraction, buckets that validate cold GETs (`checksum.validate_cold_get`), or buckets with auto-chunking enabled. Reading a whole archive object is eligible;
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

AIS serves range reads from the in-cluster copy of an object. A range read of a cold object therefore first fetches the entire object from the backend, and concurrent range reads of the same object wait for it. That fill normally uses a single backend request, with native OCI multipart downloading as an exception. Issuing more client-side range requests does not add backend parallelism to the fill; it can help serve the object once it is in the cluster.

To parallelize the backend read, use one of the blob-download options above, or prefetch.

## Why isn't blob download automatic?

A server-side, per-bucket way to enable blob download for ordinary GETs is tracked in [GitHub issue #340](https://github.com/NVIDIA/aistore/issues/340). This would let unmodified S3 clients benefit from parallel cold fills, including after cache eviction or target loss, without requiring prefetch or application-specific headers.

The existing explicit blob GET is not a transparent replacement for regular cold GET. It can return busy or admission errors, writes a chunked object, and streams the response before cache finalization. Selecting it automatically must preserve the behavior expected by clients that never requested it:

* concurrent readers must wait for the same object's fill and recheck its result;
* chunk size and eligibility must follow bucket configuration, and resource rejection must permit a regular cold GET;
* failure cleanup and fallback must not expose a partial object or race with another writer;
* range reads and archive extraction must retain their response semantics; all downloaded parts must belong to the same remote object version;
* checksum validation and redundancy settings must remain effective, or those buckets must continue to use regular cold GET;
* once response bytes have been sent, AIS cannot restart the operation as an ordinary GET without corrupting that response.

One possible first step is to fill the cache in parallel, finalize the object, and then serve it through the existing GET path. This targets total fill time and can preserve range and archive handling, but the first reader still waits for the fill. Streaming during an automatic fill would additionally target first-byte latency, with more demanding failure and buffering requirements. Both approaches increase concurrent backend requests and need admission control across simultaneous cold fills.

The presented options provide several distinct ways to improve large-object cold reads already _today_.

Future development toward transparent, bucket-configured parallel cold GETs will be guided by demand and requirements to support certain workloads, including workloads by applications that cannot (or will not) customize their S3 clients.

## References

- [Blob Downloader](/docs/blob_downloader.md)
- [Prefetch and evict](/docs/bucket.md#prefetch-and-evict)
- [S3 compatibility](/docs/s3compat.md)
- [Feature flags](/docs/feature_flags.md)
- [Buckets: chunking configuration](/docs/bucket.md)
