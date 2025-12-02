## Blob Downloader

Blob downloader is AIStore's facility for **downloading large remote objects (BLOBs)** using **concurrent range-reads**.  
Instead of pulling a 10–100+ GiB object with a single sequential stream, blob downloader:

- **splits the object into chunks** (configurable chunk size),
- **fetches those chunks in parallel** from the remote backend (configurable number of workers),
- **writes them directly into AIStore's chunked object layout** so all target disks are writing in parallel, effectively aggregating the full disk write bandwidth of the node.

![Blob Downloader](/docs/assets/blob_downloader/blob_downloader_workflow.png)

The result is that, beyond a certain object size, blob downloader can deliver **much higher throughput** than a regular cold GET. In our internal benchmarks, a 4 GiB S3 object fetched with blob downloader was up to **4× faster** than a monolithic cold GET.

Blob downloader is also **load‑aware**: it consults AIStore's internal load advisors to avoid overcommitting memory or disks, backing off when the node is under pressure and running at full speed when the system has headroom.

For a deeper dive into the internals and detailed benchmarks, see the [blog post](https://aistore.nvidia.com/blog/2025/11/26/blob-downloader).

---

## Usage

AIStore exposes blob download functionality through three distinct interfaces, each suited to different use cases.

- **Single object blob-download job** – explicitly start a blob-download job for one or more objects.
- **Prefetch + blob-threshold** – route large objects in the prefetch job through blob downloader.
- **Streaming GET** – stream a large object from blob downloader while it is being cached in AIS.

### 1. Single object blob-download job

Use this when you want direct control over which/how objects are fetched with blob downloader.

**Help and options**:

```console
$ ais blob-download --help

NAME:
   ais blob-download - (alias for "job start blob-download") Download a large object or multiple objects from remote storage, e.g.:
     - 'blob-download s3://ab/largefile --chunk-size=2mb --progress'       - download one blob at a given chunk size
     - 'blob-download s3://ab --list "f1, f2" --num-workers=4 --progress'  - run 4 concurrent readers to download 2 (listed) blobs
   When _not_ using '--progress' option, run 'ais show job' to monitor.

USAGE:
   ais blob-download BUCKET/OBJECT_NAME [command options]

OPTIONS:
   chunk-size value   Chunk size in IEC or SI units, or "raw" bytes (e.g.: 4mb, 1MiB, 1048576, 128k)
   num-workers value  Number of concurrent blob-downloading workers (readers); system default when omitted or zero (default: 0)
   list value         Comma-separated list of object or file names
   latest             Check and optionally synchronize the latest object version from the remote bucket
   progress           Show progress bar(s) in real time
   wait               Block until the job finishes (optionally use '--timeout' to limit waiting time)
   ...
```

**Examples**:

- **Single large object**

  ```console
  $ ais blob-download s3://my-bucket/large-model.bin \
        --chunk-size 4MiB \
        --num-workers 8 \
        --wait --progress
  ```

- **Multiple objects in one job**

  ```console
  $ ais blob-download s3://my-bucket \
        --list "obj1.tar,obj2.bin,obj3.dat" \
        --chunk-size 8MiB \
        --num-workers 4 \
        --wait --progress
  ```

### 2. Prefetch with blob-threshold

`prefetch` is AIStore's **multi‑object “warm‑up” job** for remote buckets. When you add a **blob size threshold**, it automatically decides which objects are large enough to benefit from blob downloader:

- Objects **≥ `--blob-threshold`** are fetched via blob downloader (parallel range‑reads, chunked writes).
- Objects **< `--blob-threshold`** are fetched with the normal cold GET path.

This lets you get the large‑object gains of blob downloader by just tuning prefetch's knobs.

**Example**:

```console
# Inspect a remote bucket
$ ais ls s3://my-bucket
NAME             SIZE            CACHED
model.ckpt       12.50GiB        no
dataset.tar      8.30GiB         no
config.json      4.20KiB         no

# Prefetch with 1 GiB threshold:
# - objects ≥ threshold use blob downloader (parallel chunks)
# - objects < threshold use standard cold GET
$ ais prefetch s3://my-bucket \
      --blob-threshold 1GiB \
      --blob-chunk-size 8MiB \
      --wait --progress
prefetch-objects[E-abc123]: prefetch entire bucket s3://my-bucket
```

Key prefetch options:

- **`--blob-threshold SIZE`**: turn blob downloader on for objects at/above `SIZE`.
- **`--blob-chunk-size SIZE`** (if available in your build): override default blob chunk size for this prefetch.
- **`--prefix` / `--list` / `--template`**: scope which objects are prefetched.

### 3. Streaming GET (Python SDK Only)

In addition to CLI jobs, blob downloader can be used to stream large objects while they are concurrently downloaded in the cluster. This is useful when you want to feed data directly into an application (for example, model loading or preprocessing) and still keep a local cached copy in AIS.

```python
from aistore import Client
from aistore.sdk.blob_download_config import BlobDownloadConfig

# Set up AIS client and bucket
client = Client("AIS_ENDPOINT")
bucket = client.bucket(name="my_bucket", provider="aws")

# Configure blob downloader (4 MiB chunks, 16 workers)
blob_cfg = BlobDownloadConfig(chunk_size="4MiB", num_workers="16")

# Stream large object using blob downloader settings
reader = bucket.object("my_large_object").get_reader(blob_download_config=blob_cfg)
data = reader.read_all()
```

---

## Selecting an effective blob-threshold for prefetch

The ideal `--blob-threshold` depends on your cluster (CPU, disks, network), backend (S3/GCS/…​), and object size distribution.  
Running full `prefetch` experiments for many candidate values can easily take **hours**, so instead we recommend using a **shorter single‑object blob-download benchmark** to pick a good starting point and then using that value directly in your prefetch job.

To do this in practice, **compare cold GET vs. blob-download on a single object**:

1. **Pick a representative large remote object** in your bucket (for example, a model shard or big archive).
2. **Evict it from AIStore** to ensure a cold path:

   ```console
   $ ais evict s3://my-bucket --list "large-model.bin"
   ```

3. **Measure cold GET time** for that object:

   ```console
   $ time ais get s3://my-bucket/large-model.bin /dev/null
   ```

4. **Measure blob-download time** for the same object:

   ```console
   $ ais evict s3://my-bucket --list "large-model.bin"

   $ time ais blob-download s3://my-bucket/large-model.bin --wait
   ```

5. Repeat the above for a few object sizes (for example: 64 MiB, 256 MiB, 1 GiB, 4 GiB) until you see a pattern:

- **Below some size**, cold GET is as fast or faster (blob overhead dominates).
- **Above that size**, blob-download is consistently faster.

The **crossover size** where blob-download _wins_ is your **blob-threshold** for prefetch: use that size as `--blob-threshold` when you run your real `ais prefetch` job. This single‑object comparison gives you a quick, reasonable approximation.

In our internal 1.56 TiB S3 benchmark, applying this method led us to a threshold of about **256 MiB**. This value provided the best trade‑off for that specific cluster and workload and delivered roughly **2.3× faster** end‑to‑end prefetch compared to a pure cold‑GET baseline.
