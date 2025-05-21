# AIStore & Amazon S3 Compatibility

**AIStore** (AIS) is a lightweight, distributed object storage system designed to scale linearly with each added storage node. It provides a uniform API across various storage backends while maintaining high performance for AI/ML and data analytics workloads.

AIS integrates with Amazon S3 on **three fronts**:

1. **Backend storage** – via the [backend provider abstraction](/docs/overview.md#backend-provider), AIStore can be utiized to access (and cache or reliably store in-cluster) a remote cloud bucket such as `s3://my-bucket` (`aws://` is accepted as an alias). This provides seamless access to existing S3 data.

2. **Front‑end compatibility** – every gateway speaks the S3 REST API. The default endpoint is `http(s)://gw-host:port/s3`, but you can enable the `S3-API-via-Root` [feature flag](/docs/feature_flags.md) to serve requests at the cluster root (`http(s)://gw-host:port/`). The same API works uniformly across all bucket types—native `ais://`, cloud‑backed `s3://`, `gs://`, and more.

3. **Presigned request offload** – AIS can receive a presigned S3 URL, execute it, and store the resulting object in the cluster. This lets you leverage S3's authentication while using AIS for storage.

---

**Which interface should I use?**

AIS exposes a *pure* S3 surface for seamless compatibility and a *native* API for advanced, cluster‑aware workloads. The table below helps decide which path fits your scenario:

| Use the **S3 compatibility API** when you…                                                                              | Use the **native AIS API / CLI** when you…                                                                      |
| ----------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| Need drop‑in support for unmodified S3 tools & SDKs (`aws`, `boto3`, `s3cmd`, …)                                        | Want cluster‑wide batch jobs (`ais etl`, `ais prefetch`, `ais copy`, `ais archive`, …)                          |
| Rely on an existing S3‑centric workflow or third‑party app                                                              | Need fine‑grained control‑plane ops (`ais cluster`, `ais bucket props set`, node lifecycle)                     |
| Accept MD5‑based ETag semantics—even though MD5 is slower and not crypto‑secure                                         | Value AIS‑native features: virtual directories, adaptive rate‑limiting, WebSocket ETL, streaming cold‑GET, etc. |
| Accept that some S3 features (CORS, Website hosting, CloudFront) are **not** yet implemented                            | Care about advanced list-objects options (to list [shards](/docs/overview.md#shard)), working with [remote clusters](/docs/overview.md#unified-namespace), non-S3 buckets)                              |
| Are okay with slight performance overhead from the S3‑to‑AIS adaptation layer (MD5 hashing, XML marshaling/translation) | Want full [Prometheus](/docs/monitoring-prometheus.md) visibility with AIS‑rich metrics & labels                                                  |

---

## Table of Contents

* [Quick Start](#quick-start)
  * [aws CLI](#quick-start-with-aws-cli)
  * [s3cmd](#quick-start-with-s3cmd)
* [Configuring Clients](#configuring-clients)
  * [Finding the AIS endpoint](#finding-the-ais-endpoint)
  * [Checksum considerations](#checksum-considerations)
  * [HTTPS vs HTTP](#https-vs-http)
* [Using s3cmd with AIS](#using-s3cmd-with-ais)
  * [Interactive `s3cmd --configure` transcript](#interactive-s3cmd---configure-transcript)
  * [Example `.s3cfg`](#example-s3cfg)
  * [Multipart uploads](#multipart-uploads-with-s3cmd)
  * [JWT authentication](#authentication-jwt-tips)
* [Supported Operations](#supported-operations)
  * [PUT / GET / HEAD](#put--get--head)
  * [Range reads](#range-reads)
  * [Multipart uploads (aws CLI)](#multipart-uploads-with-aws-cli)
  * [Presigned requests](#presigned-s3-requests)
* [S3 Bucket Inventory](#s3-bucket-inventory-support)
  * [Why inventories matter](#why-inventories-matter)
  * [Enabling inventory via AWS CLI](#enabling-inventory-via-aws-cli)
  * [Managing inventories with helper scripts](#managing-inventories-with-helper-scripts)
  * [Inventory XML example](#inventory-xml-example)
* [Compatibility Matrix](#compatibility-matrix)
* [Boto3 Examples](#boto3-examples)
* [FAQs & Troubleshooting](#faqs--troubleshooting)
* [Further reading](#further-reading)

---

> **Environment assumption – Local Playground**
> 
> The CLI examples below use `localhost:8080`, which is the default endpoint when running AIS in the [Local Playground](/docs/getting_started.md#local-playground).
>
> For other deployment modes (including [Kubernetes Playground](/docs/getting_started.md#kubernetes-playground), Docker Compose, bare-metal cluster, or [Kubernetes for production deployments](https://github.com/NVIDIA/ais-k8s)) — replace the `host:port` with any **AIS gateway** endpoint.
> 
> See [Deployment Options](/docs/getting_started.md#multiple-deployment-options) and the main project [Features](https://github.com/NVIDIA/aistore/tree/main?tab=readme-ov-file#features) list for a broader overview.

---

## Quick Start

### Quick start with `aws CLI`

```console
AWS_EP=http://localhost:8080/s3
aws --endpoint-url "$AWS_EP" s3 mb s3://demo
aws --endpoint-url "$AWS_EP" s3 cp README.md s3://demo/
aws --endpoint-url "$AWS_EP" s3 ls s3://demo
# Expected output:
# 2023-05-14 14:25     10493   s3://demo/README.md
```

### Quick start with `s3cmd`

One‑liner (HTTP):

```console
s3cmd put README.md s3://demo \
  --no-ssl \
  --host=localhost:8080/s3 \
  --host-bucket="localhost:8080/s3/%(bucket)"

# Expected output:
# upload: 'README.md' -> 's3://demo/README.md' [1 of 1]
# 10493 of 10493   100% in    0s     4.20 MB/s  done
```

> **Tip — use a cluster‑specific `.s3cfg`** so you can drop the `--host*` flags. See the [Example .s3cfg](#example-s3cfg) section below.

---

## Configuring Clients

### Finding the AIS endpoint

Choose **any** gateway's `host:port` and append `/s3`, e.g. `10.10.0.1:51080/s3`. All gateways accept reads and writes, so you can connect to any of them.

> In fact, AIS gateways are completely equivalent, [API-wise](/docs/overview.md#aistore-api).

---

### Checksum considerations

Amazon S3's `ETag` is MD5 (or a multipart hash); AIS defaults to **xxhash** for better performance. To avoid client mismatch warnings, set MD5 per bucket:

```console
ais bucket props set ais://demo checksum.type=md5      # per bucket
# OR cluster‑wide default
ais config cluster checksum.type=md5
```

Setting the checksum type to MD5 ensures compatibility with S3 clients that validate checksums, though it comes with a minor performance cost compared to xxhash.

---

### HTTPS vs HTTP

Enable TLS in `ais.json` (`net.http.use_https=true`) or pass `--no-ssl`/`--insecure` flags when using tools. By default, AIS uses HTTP, while many S3 clients expect HTTPS.

---

## Using `s3cmd` with AIS

### Interactive `s3cmd --configure` transcript

```text
$ s3cmd --configure
Enter new values or accept defaults in brackets with Enter.
Access Key: FAKEKEY
Secret Key: FakeSecret
Default Region [US]:
Use HTTPS protocol [Yes]: n
HTTP Proxy server name:
New settings:
  Access Key: FAKEKEY
  Secret Key: FAKESECRET
  Region: US
  Use HTTPS protocol: False
  HTTP Proxy server name:

Test access with supplied credentials? [Y/n] y
Please wait, attempting to list buckets...
```

During the test, **enter your AIS endpoint** when prompted:

```
Hostname: localhost:8080/s3
Bucket host: localhost:8080/s3/%(bucket)
```

```
Success. Your access key and secret key worked fine :-)
...
Save settings? [y/N] y
Configuration saved to ~/.s3cfg
```

### Example `.s3cfg`

Edit your `~/.s3cfg` file to include these lines (replace with your actual gateway endpoint):

```ini
host_base = 10.10.0.1:51080/s3
host_bucket = 10.10.0.1:51080/s3/%(bucket)
access_key = FAKEKEY
secret_key = FAKESECRET
signature_v2 = False
use_https = False
```

This configuration allows you to run `s3cmd` commands without having to specify the host parameters each time.

### Multipart uploads with s3cmd

For large files, use multipart uploads to improve reliability and performance:

```console
s3cmd put ./large.bin s3://demo --multipart-chunk-size-mb=8
```

The optimal chunk size depends on your network conditions and file size, but 8-16MB chunks work well for most cases.

### Authentication (JWT) tips

If AIS Authentication is enabled, you'll need to attach a JWT token manually because `s3cmd`'s signer overwrites the `--add-header` option:

```diff
# s3cmd/S3/S3.py (single‑line patch)
-        pass
+        self.headers["Authorization"] = "Bearer <token>"
```

Replace `<token>` with your actual JWT token. This modification ensures the token is included in every request.

---

## Supported Operations

### PUT / GET / HEAD

Regular verbs work with `aws`, `s3cmd`, or the native `ais` CLI:

```console
# Using aws CLI
aws --endpoint-url "$AWS_EP" s3api put-object --bucket demo --key obj --body file.txt
aws --endpoint-url "$AWS_EP" s3api get-object --bucket demo --key obj output.txt
aws --endpoint-url "$AWS_EP" s3api head-object --bucket demo --key obj
# Output:
# {
#     "AcceptRanges": "bytes",
#     "LastModified": "Wed, 14 May 2025 14:30:22 GMT",
#     "ContentLength": 1024,
#     "ETag": "\"a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6\"",
#     "ContentType": "text/plain"
# }

# Native AIS CLI equivalents
ais put file.txt ais://demo/obj
ais get ais://demo/obj output.txt
ais object show ais://demo/obj
```

### Range reads

S3 API supports byte range requests for partial object downloads:

```console
aws s3api get-object \
  --range bytes=0-99 \
  --bucket demo --key README.md \
  part.txt \
  --endpoint-url "$AWS_EP"
# Output:
# {
#     "AcceptRanges": "bytes",
#     "LastModified": "Wed, 14 May 2025 14:30:22 GMT",
#     "ContentRange": "bytes 0-99/10493",
#     "ContentLength": 100,
#     "ETag": "\"a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6\"",
#     "ContentType": "text/plain"
# }
```

This would download only the first 100 bytes of the file.

---

### Multipart uploads with aws CLI

```console
# 1 — initiate
aws s3api create-multipart-upload --bucket demo --key big \
  --endpoint-url "$AWS_EP"
# Output:
# {
#     "Bucket": "demo",
#     "Key": "big",
#     "UploadId": "xu3DvVzJK"
# }

# 2 — upload parts individually
aws s3api upload-part --bucket demo --key big \
  --part-number 1 --body part1.bin \
  --upload-id "YOUR-UPLOAD-ID" \
  --endpoint-url "$AWS_EP"
# Repeat for each part, incrementing the part-number
# Output:
# {
#     "ETag": "\"a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6\""
# }

# 3 — complete (requires a JSON file listing all parts)
aws s3api complete-multipart-upload --bucket demo --key big \
  --upload-id "YOUR-UPLOAD-ID" --multipart-upload file://parts.json \
  --endpoint-url "$AWS_EP"
```

Example `parts.json`:
```json
{
  "Parts": [
    {
      "PartNumber": 1,
      "ETag": "etag-from-upload-part-response"
    },
    {
      "PartNumber": 2,
      "ETag": "etag-from-upload-part-response"
    }
  ]
}
```

### Presigned S3 requests

Presigned URLs allow temporary access to objects without sharing credentials:

1. Enable feature:

   ```console
   ais config cluster features S3-Presigned-Request
   ```
2. Generate URL (typically done on the system with AWS credentials):

   ```console
   aws s3 presign s3://demo/README.md --endpoint-url https://s3.amazonaws.com
   # Returns a URL with authentication parameters in the query string
   ```
3. Replace the host with `AIS_ENDPOINT/s3` and add header when using path style:

   ```console
   curl -H 'ais-s3-signed-request-style: path' '<PRESIGNED_URL>' -o README.md
   ```

This allows AIS to handle the authenticated S3 request on behalf of the client.

---

## S3 Bucket Inventory Support

### Why inventories matter

For buckets with millions of objects, using S3's `ListObjectsV2` API can be slow, expensive, and time-consuming. S3 Bucket Inventory provides a manifest file (CSV/ORC/Parquet) of all objects that AIS can process instantly. This is crucial for efficiently working with very large buckets.

For example, listing a 10-million object bucket could take:
- Without inventory: Minutes of API calls, potentially costing money
- With inventory: Seconds to parse a pre-generated manifest file

---

### Enabling inventory via AWS CLI

```console
aws s3api put-bucket-inventory-configuration \
  --bucket webdataset \
  --id ais-scan \
  --inventory-configuration file://inventory.json
```

**`inventory.json`**

```json
{
  "Id": "ais-scan",
  "IsEnabled": true,
  "IncludedObjectVersions": "All",
  "Destination": {
    "S3BucketDestination": {
      "Bucket": "arn:aws:s3:::webdataset-inv",
      "Format": "CSV"
    }
  },
  "Prefix": "",
  "Schedule": { "Frequency": "Daily" }
}
```

### Managing inventories with helper scripts

AIS repo ships ready‑to‑use wrappers in `scripts/s3/`:

| Script                       | Purpose                    |
| ---------------------------- | -------------------------- |
| `put-bucket-inventory.sh`    | create/update an inventory |
| `list-bucket-inventory.sh`   | list existing configs      |
| `delete-bucket-inventory.sh` | disable inventory          |
| `get-bucket-inventory.sh`    | show detailed info for a specific inventory |
| `put-bucket-policy.sh`       | grant access for S3 to store inventory files |

These scripts simplify inventory management with minimal required parameters.

---

### Inventory XML example

```xml
<InventoryConfiguration>
  <Id>ais-scan</Id>
  <IsEnabled>true</IsEnabled>
  <IncludedObjectVersions>All</IncludedObjectVersions>
  <Schedule>
    <Frequency>Daily</Frequency>
  </Schedule>
  <Destination>
    <S3BucketDestination>
      <Bucket>arn:aws:s3:::webdataset-inv</Bucket>
      <Format>CSV</Format>
    </S3BucketDestination>
  </Destination>
</InventoryConfiguration>
```

Once inventory lands (typically after 24 hours for the first run), you can instantly list bucket contents:

```console
ais ls s3://webdataset --all --inventory
```

This is dramatically faster than listing objects via the standard API calls, particularly for large buckets.

---

## Compatibility Matrix

| S3 feature              | AIS         | s3cmd            | aws CLI                |
| ----------------------- | ----------- | ---------------- | ---------------------- |
| Create/Destroy bucket   | ✅           | ✅ `mb/rb`        | ✅ `mb/rb`              |
| PUT / GET / HEAD object | ✅           | ✅ `put/get/info` | ✅ `cp/head`            |
| Range reads             | ✅           | —                | ✅ `get-object --range` |
| Multipart upload        | ✅           | ✅                | ✅                      |
| Copy object             | S3 API only | partial          | ✅                      |
| Inventory listing       | ✅           | —                | —                      |
| Authentication          | JWT         | modified         | ✅                      |
| Presigned URLs          | ✅           | —                | ✅                      |

> **Not yet supported**: Regions, CORS, Website hosting, CloudFront; full ACL parity (AIS uses its own ACL model).

---

## Boto3 Examples

Python applications can use [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) (the AWS SDK for Python) to connect to AIStore. Since AIStore implements S3 API compatibility, most standard Boto3 S3 operations work with minimal changes.

### Prerequisites

For Boto3 to work with AIStore, you need to patch Boto3's redirect handling:

```python
# Import the patch before using boto3
from aistore.botocore_patch import botocore
import boto3
```

This patch modifies Boto3's HTTP client behavior to handle AIStore's redirect-based load balancing. For details, see the [Boto3 compatibility documentation](/python/aistore/botocore_patch/README.md).

### Client Initialization

```python
client = boto3.client(
    "s3",
    region_name="us-east-1",  # Any valid region will work
    endpoint_url="http://localhost:8080/s3",  # Your AIStore endpoint
    aws_access_key_id="YOUR_ACCESS_KEY",      # Can be dummy values when
    aws_secret_access_key="YOUR_SECRET_KEY",  # AIS auth is disabled
)
```

### Basic Bucket Operations

```python
# Create a bucket
client.create_bucket(Bucket="my-bucket")

# List all buckets
response = client.list_buckets()
bucket_names = [bucket["Name"] for bucket in response["Buckets"]]
print(f"Existing buckets: {bucket_names}")

# Delete a bucket
client.delete_bucket(Bucket="my-bucket")
```

### Object Operations

```python
# Upload an object
client.put_object(
    Bucket="my-bucket",
    Key="sample.txt",
    Body="Hello, AIStore!"
)

# Download an object
response = client.get_object(Bucket="my-bucket", Key="sample.txt")
content = response["Body"].read().decode("utf-8")
print(f"Object content: {content}")

# Delete an object
client.delete_object(Bucket="my-bucket", Key="sample.txt")
```

### Multipart Upload Example

For large files, you can use multipart uploads:

```python
import boto3
from aistore.botocore_patch import botocore

# Initialize the client
client = boto3.client(
    "s3",
    region_name="us-east-1",
    endpoint_url="http://localhost:8080/s3",
    aws_access_key_id="FAKEKEY",
    aws_secret_access_key="FAKESECRET",
)

# Create a bucket if it doesn't exist
bucket_name = "multipart-demo"
try:
    client.head_bucket(Bucket=bucket_name)
except:
    client.create_bucket(Bucket=bucket_name)

# Prepare data for multipart upload
object_key = "large-file.txt"
data = "x" * 10_000_000  # 10MB of data
chunk_size = 5_000_000   # 5MB chunks
chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]

# Initiate multipart upload
response = client.create_multipart_upload(Bucket=bucket_name, Key=object_key)
upload_id = response["UploadId"]

# Upload individual parts
parts = []
for i, chunk in enumerate(chunks):
    part_num = i + 1  # Part numbers start at 1
    response = client.upload_part(
        Body=chunk,
        Bucket=bucket_name,
        Key=object_key,
        PartNumber=part_num,
        UploadId=upload_id,
    )
    parts.append({"PartNumber": part_num, "ETag": response["ETag"]})

# Complete the multipart upload
client.complete_multipart_upload(
    Bucket=bucket_name,
    Key=object_key,
    UploadId=upload_id,
    MultipartUpload={"Parts": parts}
)

# Verify upload was successful
response = client.head_object(Bucket=bucket_name, Key=object_key)
print(f"Successfully uploaded {response['ContentLength']} bytes")
```

---

## FAQs & Troubleshooting

| Symptom                                        | Cause & Fix                                                         |
| ---------------------------------------------- | ------------------------------------------------------------------- |
| `MD5 sum mismatch`                             | Set bucket checksum to `md5` with `ais bucket props set BUCKET checksum.type=md5` |
| `SSL certificate problem`                      | Self-signed cert ➜ use `--no-ssl` or `--insecure`                  |
| Presigned URL fails                            | Add header `ais-s3-signed-request-style: path` for path‑style URLs |
| `Authorization header missing` (s3cmd + AuthN) | Patch `S3.py` to include JWT as shown in [Authentication](#authentication-jwt-tips) |
| Upload fails with timeout                      | Try with smaller multipart chunks (e.g., `--multipart-chunk-size-mb=5`) |
| Unable to list large S3 bucket                 | Use [bucket inventory](#s3-bucket-inventory-support) to efficiently list contents |
| Boto3/TensorFlow integration issues            | See [Boto3 compatibility patch]((https://github.com/NVIDIA/aistore/blob/main/python/aistore/botocore_patch/README.md)) for redirects |

---

## Further Reading

* [Backend Providers](/docs/overview.md#backend-provider)
* [AIS CLI reference](/docs/cli.md)
* [Auth & ACL](/docs/auth.md)
* [Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore)
* [Boto3 compatibility](https://github.com/NVIDIA/aistore/blob/main/python/aistore/botocore_patch/README.md)
