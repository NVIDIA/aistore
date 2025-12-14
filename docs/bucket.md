# AIS Buckets: Design and Operations

A **bucket** is a named container for objects - monolithic files or chunked representations - with associated metadata.

Buckets are the primary unit of data organization and policy application in AIStore (AIS).

> Object metadata includes checksum, version, size, access time, replica/EC placement, unique bucket ID (`BID`), and custom user-defined attributes.
> For remote buckets, AIS may also store backend-specific metadata such as ETag, LastModified timestamps, backend version identifiers, and provider checksums when available.
>
> Metadata v2 includes additional flags used by AIS features (for example, chunked object representation).

AIS uses a **flat hierarchy**: `bucket-name/object-name` key space. It supports [virtual directories](/docs/howto_virt_dirs.md) through prefix-based naming with recursive and non-recursive operations.

This document is organized in two parts:

- [**Part I: Design**](#part-i-design) — covers the bucket abstraction, identity model, namespaces, remote clusters, and backend buckets
- [**Part II: How-To**](#part-ii-how-to) — practical operations: working with same-name buckets, prefetch/evict, access control, provider configuration, and [CLI](/docs/cli.md) reference

---

# **Part I: Design**

AIS does not treat a bucket as a passive container. A bucket is a *logical namespace* that AIS materializes lazily (for remote backends), configures dynamically, and manages cluster-wide.

**Table of Contents**
1. [Motivation](#motivation)
   - [Creation](#creation)
   - [Bucket Identity](#bucket-identity)
2. [The Bucket](#the-bucket)
   - [Provider](#provider)
   - [Namespace](#namespace)
   - [Bucket Properties](#bucket-properties)
   - [Feature Flags](#feature-flags)
3. [Bucket Lifecycle](#bucket-lifecycle)
   - [Implicit creation (lazy discovery)](#implicit-creation-lazy-discovery)
   - [Explicit creation](#explicit-creation-1)
   - [Deletion and eviction](#deletion-and-eviction)
4. [Namespaces](#namespaces)
   - [Syntax](#syntax)
   - [In BMD](#in-bmd)
5. [Remote AIS Clusters](#remote-ais-clusters)
   - [Attaching clusters](#attaching-clusters)
   - [Accessing remote buckets](#accessing-remote-buckets)
   - [Namespace encoding](#namespace-encoding)
6. [Backend Buckets](#backend-buckets)
   - [Creating backend relationships](#creating-backend-relationships)
   - [Use cases](#use-cases)
   - [Disconnecting](#disconnecting)

## Motivation

The idea is to provide a unified storage abstraction. Instead of maintaining different APIs for in-cluster storage, Cloud providers, other remote backends - AIS exposes everything through a single, consistent bucket abstraction.

The design goals were (and remain):

* **Operational Simplicity**: Eliminate "registration overhead." If a bucket exists in the backend, it should be immediately usable in AIS.
* **Provider Agnostic**: The API remains identical whether the data resides on local NVMe drives, a remote AIS cluster, or a public cloud provider.
* **Dynamic Configuration**: Buckets are not passive containers; they are logical namespaces where data protection (EC, Mirroring) and caching policies (LRU) are applied dynamically.

Users interact with buckets uniformly, regardless of where they live:
* Local disks (AIS provider)
* AWS, GCP, Azure, OCI
* S3-compatible systems (SwiftStack, Cloudian, MinIO, Oracle OCI, etc.)
* Other AIS clusters

The provider and namespace differentiate the backend; the API stays the same.

| Type | Description | Example |
|------|-------------|---------|
| AIS Bucket | Native bucket managed by _this_ cluster (the one addressed by the AIS endpoint that you have) | `ais://mybucket` |
| Remote AIS Bucket | Bucket in a remote AIS cluster | `ais://@remais/mybucket` |
| Cloud Bucket | Remote bucket (S3, GCS, Azure, OCI) | `s3://dataset` |
| Backend Bucket | AIS bucket linked to a remote bucket | `ais://cache => s3://origin` |

### Creation

Another core design goal was to eliminate boilerplate: if a bucket exists in the remote backend (Cloud, Remote AIS, etc.) and is accessible, AIS makes it immediately usable.
Remote buckets are added lazily, on first reference, without a separate creation step.

Explicit creation is supported when additional control is required - credentials, endpoints, namespaces, or properties that must be set before first access.

Further details - in section [Bucket Lifecycle](#bucket-lifecycle) below.

### Bucket Identity

Once added to BMD, a bucket's identity becomes cluster-wide and immutable:

> **Identity = Provider + Namespace + Name**

AIS never guesses or rewrites identity. `s3://#ns1/bucket` and `s3://#ns2/bucket` are distinct buckets.

---

## The Bucket

```
          ┌────────────── Bucket Identity ─────────────┐
          │                                            │
          │   ( Provider ,  Namespace ,  Name )        │
          │                                            │
          └────────────────────────────────────────────┘
                           │
                           ▼
                 ┌────────────────────┐
                 │     Properties     │
                 │      (Bprops)      │
                 └────────────────────┘
```

**Example: S3 bucket with namespace**

```
        ┌───────────────────────────────────────┐
        │ Identity:                             │
        │    • Provider:    aws                 │
        │    • Namespace:   #prod-account       │
        │    • Name:        logs                │
        │                                       │
        │ Bprops:                               │
        │    • versioning.enabled = true        │
        │    • extra.aws.profile = prod         │
        │    • mirror.copies = 2                │
        └───────────────────────────────────────┘
```

**Example: AIS bucket with backend**

```
        ┌───────────────────────────────────────┐
        │ Identity:                             │
        │    • Provider:    ais                 │
        │    • Namespace:   (global)            │
        │    • Name:        cache               │
        │                                       │
        │ Bprops:                               │
        │    • backend_bck = s3://source-logs   │
        │    • lru.enabled = true               │
        └───────────────────────────────────────┘
```

### Provider

Indicates the storage backend:

| Provider | Backend |
|----------|---------|
| `ais` | Native AIS bucket |
| `aws` or `s3` | Amazon S3 or S3-compatible |
| `gcp` or `gs` | Google Cloud Storage |
| `azure` or 'az' | Azure Blob Storage |
| `oci` or `oc' | Oracle Cloud Infrastructure |

Remote AIS clusters use the `ais` provider with a namespace referencing the cluster alias or UUID:

```
ais://@remais//bucket-name

# Or alternative syntax using remote cluster's UUID:
ais://@uuid/bucket-name
```

### Namespace

Namespaces disambiguate buckets that share the same name.

Originally, all cloud buckets had an implicit *global* namespace. That model breaks when:

* Different AWS accounts contain same-name buckets
* S3-compatible endpoints host same-name buckets
* SwiftStack/Cloudian accounts scope buckets by user

Namespaces fix this:

```
s3://#account1/images
s3://#account2/images
```

These resolve to:

```go
Bck{Provider: "aws", Ns: Ns{Name: "account1"}, Name: "images"}
Bck{Provider: "aws", Ns: Ns{Name: "account2"}, Name: "images"}
```

They are independent in every way - separate BMD entries, credentials, and [on-disk paths](#appendix-a-on-disk-layout).

> **Note**: The `Ns` struct has two fields: UUID (for remote AIS clusters) and Name (for logical namespaces). For cloud buckets, namespace identifier (e.g., #prod in s3://#prod/bucket) enables multiple same-name buckets with different credentials or endpoints.

For remote AIS clusters, the namespace _additionally_ carries the cluster's UUID:

```go
ais://@remais/bucket => Bck{Provider: "ais", Ns: Ns{UUID: "<cluster-uuid>"}, Name: "bucket"}
```

> **Note**: The bucket namespace you choose - whether it represents an AWS profile, a GCS account, or simply a human-readable alias - becomes part of the bucket's physical [on-disk path](#appendix-a-on-disk-layout). What starts as a logical identifier materializes into on-disk naming structure.

### Bucket Properties

Bucket properties - stored in BMD, inherited from cluster config, overridable per-bucket - control data protection (checksums, EC, mirroring), chunked representation, versioning and synchronization with remote sources, LRU eviction, rate limiting, access permissions, provider-specific settings, and more.

The properties:
* are **inherited** from cluster-wide configuration at bucket creation time;
* can be overridden at creation time and/or at any time via `ais bucket props set` or the corresponding Go or Python API;
* are applied cluster-wide via metasync;
* include data layout, checksumming, EC/mirroring, LRU, rate limiting, backend linkage, access control, and more.

At the top level:

| JSON key       | Type              | What it controls                                                            |
| -------------- | ----------------- | --------------------------------------------------------------------------- |
| `provider`     | `string`          | Backend provider (`ais`, `aws`, `gcp`, `azure`, `oci`,  …).                 |
| `backend_bck`  | `Bck`             | Optional "backend bucket" AIS proxies to (see [Backend Buckets](#backend-buckets)). |
| `write_policy` | `WritePolicyConf` | When/how metadata is persisted (`immediate`, `delayed`, `never`).           |
| `checksum`     | `CksumConf`       | Checksum algorithm and validation policies for cold/warm GET.               |
| `versioning`   | `VersionConf`     | Versioning enablement and synchronization with the backend.                 |
| `mirror`       | `MirrorConf`      | N-way mirroring (on/off, number of copies).                                 |
| `ec`           | `ECConf`          | Erasure coding (data/parity slices, size thresholds).                       |
| `chunks`       | `ChunksConf`      | Chunked-object layout and multipart-upload behavior.                        |
| `lru`          | `LRUConf`         | LRU caching policy: watermarks, enable/disable.                             |
| `rate_limit`   | `RateLimitConf`   | Frontend and backend rate limiting (bursty/adaptive shaping).               |
| `extra`        | `ExtraProps`      | Provider-specific extras (e.g., `extra.aws.profile`, `extra.aws.endpoint`). |
| `access`       | `AccessAttrs`     | Bucket access mask (GET, PUT, DELETE, etc.).                                |
| `features`     | `feat.Flags`      | [Feature flags](#feature-flags) to flip assorted defaults (e.g., S3 path-style). |
| `bid`          | `uint64`          | Unique bucket ID (assigned by AIS, read-only).                              |
| `created`      | `int64`           | Bucket creation time (Unix timestamp, read-only).                           |
| `renamed`      | `string`          | **Deprecated**: non-empty only for buckets that have been renamed.          |

```console
# Validate remote version and, possibly, update in-cluster ("cached") copy;
# delete in-cluster object if its remote counterpart does not exist
ais create gs://abc --props="versioning.validate_warm_get=false versioning.synchronize=true"

# Enable mirroring at creation time
ais create ais://abc --props="mirror.enabled=true mirror.copies=3"

# Or using JSON:
ais create ais://abc --props='{"mirror": {"enabled": true, "copies": 3}}'

# Enable mirroring and tweak checksum configuration
ais create ais://abc \
  --props='{
    "mirror":   {"enabled": true, "copies": 3},
    "checksum": {"type": "xxhash", "validate_warm_get": true}
  }'

# Configure a cloud bucket with provider-specific extras and rate limiting
ais create s3://logs \
  --props='{
    "extra":      {"aws": {"profile": "prod", "endpoint": "https://s3.example.com"}},
    "rate_limit": {"backend": {"enabled": true, "max_bps": "800MB"}}
  }'
```

### Feature Flags

[Feature flags](/docs/feature_flags.md) are a 64-bit bitmask controlling assorted runtime behaviors. Most flags are cluster-wide, but a subset can be configured per-bucket.

#### Bucket-level flags

| Flag | Tags | Description |
|------|------|-------------|
| `Skip-Loading-VersionChecksum-MD` | `perf,integrity-` | Skip loading existing object's metadata (version, checksum) |
| `Fsync-PUT` | `integrity+,overhead` | Sync object payload to stable storage on PUT |
| `S3-Presigned-Request` | `s3,security,compat` | Pass-through presigned S3 requests for backend authentication |
| `S3-Use-Path-Style` | `s3,compat` | Use path-style S3 addressing (e.g., `s3.amazonaws.com/BUCKET/KEY`) |
| `Resume-Interrupted-MPU` | `mpu,ops` | Resume interrupted multipart uploads |
| ... | ... | ... |

> For the full list, see this separate [Feature Flags](/docs/feature_flags.md) document.

#### Tag meanings

- `integrity+` — enhances data safety
- `integrity-` — trades safety for performance
- `perf` — performance optimization
- `overhead` — may impact performance
- `s3,compat` — S3 compatibility

#### Setting bucket features

```console
# View available bucket features
ais bucket props set ais://mybucket features <TAB-TAB>

# Enable a feature
ais bucket props set ais://mybucket features S3-Presigned-Request

# Enable multiple features
ais bucket props set ais://mybucket features Fsync-PUT S3-Use-Path-Style

# Reset to defaults (none)
ais bucket props set ais://mybucket features none
```

> Some flags are mutually exclusive. For example, `Disable-Cold-GET` and `Streaming-Cold-GET` cannot both be set — the system will reject the configuration. For complete details on all feature flags (cluster-wide and bucket-level), see [Feature Flags](/docs/feature_flags.md).

## Bucket Lifecycle

The distinction between implicit bucket discovery and explicit creation is best summarized by the AIS [CLI](/docs/cli.md) itself.
When you run `ais create --help`, it outlines the specific scenarios where 'on-the-fly' discovery isn't enough:


```
$ ais create --help
NAME:
   ais create - (alias for "bucket create") Create AIS buckets or explicitly attach remote buckets with non-default credentials/properties.
     Normally, AIS auto-adds remote buckets on first access (ls/get/put): when a user references a new bucket,
     AIS looks it up behind the scenes, confirms its existence and accessibility, and "on-the-fly" updates its
     cluster-wide global (BMD) metadata containing bucket definitions, management policies, and properties.
     Use this command when you need to:
       1) create an ais:// bucket in this cluster;
       2) create a bucket in a remote AIS cluster (e.g., 'ais://@remais/BUCKET');
       3) set up a cloud bucket with a custom profile and/or endpoint/region;
       4) set bucket properties before first access;
       5) attach multiple same-name cloud buckets under different namespaces (e.g., 's3://#ns1/bucket', 's3://#ns2/bucket');
       6) and finally, register a cloud bucket that is not (yet) accessible (advanced-usage '--skip-lookup' option).
...
```

### Implicit creation (lazy discovery)

On first reference:

```console
ais ls s3://images --all
ais get s3://logs/foo.txt
```

AIS:

1. Parses the bucket URI into internal control structure (`cmn.Bck`)
2. Checks BMD for existing entry
3. If missing: performs `HEAD(bucket)` to validate access
4. Inserts the bucket into BMD with default properties
5. Metasyncs the latter to all nodes

This behavior is foundational, motivated by removing the operational overhead of bucket management.

### Explicit creation

Invoked with:

```console
ais create s3://bucket --props="extra.aws.profile=prod"
```

AIS:

1. Parses URI and properties
2. Issues `HEAD` (unless `--skip-lookup` or bucket already in BMD)
3. Creates BMD entry with specified properties
4. Metasyncs to all nodes

Use `--skip-lookup` when default credentials cannot access the bucket:

```console
ais create s3://restricted --skip-lookup \
  --props="extra.aws.profile=special"
```

### Deletion and eviction

**AIS buckets:**

```console
ais bucket rm ais://bucket
```

Destroys the bucket and all objects permanently.

**Cloud buckets:**

```console
ais bucket rm s3://bucket
# or equivalently:
ais evict s3://bucket
```

Removes AIS state (BMD entry, cached objects). Cloud data remains untouched.

**Eviction options:**

| Command | Effect |
|---------|--------|
| `ais evict s3://bucket` | Remove BMD entry and all cached objects |
| `ais evict s3://bucket --keep-md` | Keep BMD entry, remove cached objects |
| `ais evict s3://bucket --prefix images/` | Evict only matching objects |
| `ais evict s3://bucket --template "shard-{0..999}.tar"` | Evict by template |

Eviction is namespace-aware:

```console
ais evict s3://#prod/data     # only this namespace
ais evict s3://#dev/data      # independent operation
```

> See also: [Three Ways to Evict Remote Bucket](/docs/cli/evicting_buckets_andor_data.md)

---

## Namespaces

Namespaces solve real-world scenarios that global namespace cannot handle:

* **Account-scoped buckets** - SwiftStack, Cloudian bucket names are per-account
* **Multiple credentials** - Different AWS profiles with overlapping bucket names
* **Environment separation** - Same bucket name across dev/staging/prod
* **Multiple endpoints** - Oracle OCI, SwiftStack, and AWS S3 in the same cluster

### Syntax

```
<provider>://#<namespace>/<bucket>
```

Examples:

```console
# Two buckets named "data", different AWS accounts
ais create s3://#prod/data --props="extra.aws.profile=prod-account"
ais create s3://#dev/data --props="extra.aws.profile=dev-account"

# SwiftStack with account-scoped buckets
ais create s3://#swift-tenant/bucket \
  --props="extra.aws.profile=swift extra.aws.endpoint=https://swift.example.com"

# S3-compatible with custom endpoint
ais create s3://#minio/images \
  --props="extra.aws.endpoint=http://minio.local:9000"
```

> **Note**: The bucket namespace you choose - whether it represents an AWS profile, a GCS account, or simply a human-readable alias - becomes part of the bucket's physical [on-disk path](#appendix-a-on-disk-layout).

### In BMD

Metadata-wise, each bucket receives:

* A unique BID (bucket ID)
* Its own bucket props (`Bprops`)
* Its own credential configuration

---

## Remote AIS Clusters

AIS clusters can attach to each other, forming a global namespace of distributed datasets.

### Attaching clusters

```console
# Attach with alias
ais cluster remote-attach remais=http://remote-proxy:8080

# Verify attachment
ais show remote-cluster
```

### Accessing remote buckets

```console
# List all remote AIS buckets
ais ls ais://@

# List buckets in specific remote cluster
ais ls ais://@remais/

# Access objects
ais get ais://@remais/bucket/object local-file
```

### Namespace encoding

The alias resolves to the remote cluster's UUID, stored in the namespace:

```go
ais://@remais/bucket => Bck{Provider: "ais", Ns: Ns{UUID: "Cjl2Ht4gE"}, Name: "bucket"}
```

> See also: [Remote AIS Cluster](/docs/providers.md#remote-ais-cluster)

---

## Backend Buckets

Backend buckets represent **indirection** - an AIS bucket that proxies to a remote bucket. This is fundamentally different from namespaces.

| Aspect | Namespace | Backend Bucket |
|--------|-----------|----------------|
| Purpose | Disambiguate identity | Proxy/cache |
| Bucket count | 1 (the cloud bucket itself) | 2 (AIS + cloud) |
| On-disk path | `@aws/#ns/bucket/` | `@ais/cache-bucket/` |
| Use case | Multi-account, multi-endpoint | Caching, ETL, aliasing |

> **Note:** See section [Working with Same-Name Remote Buckets](#working-with-same-name-remote-buckets) below for further guidelines and usage examples.

### Creating backend relationships

```console
ais create ais://cache
ais bucket props set ais://cache backend_bck=s3://origin
```

Now reads/writes to `ais://cache` transparently forward to `s3://origin`.

### Use cases

**Hot cache for cold storage:**

```console
ais create ais://hot-cache
ais bucket props set ais://hot-cache backend_bck=s3://cold-archive lru.enabled=true
```

**Dataset aliasing:**

```console
# Always point to latest processed dataset
ais create ais://dataset-latest
ais bucket props set ais://dataset-latest backend_bck=gs://processed-2024-01-15

# Update when new version available
ais bucket props set ais://dataset-latest backend_bck=gs://processed-2024-01-20
```

**Access control:**

```console
# Expose subset of cloud data under controlled name
ais create ais://public-subset
ais bucket props set ais://public-subset backend_bck=s3://internal-data
```

### Disconnecting

```console
ais bucket props set ais://cache backend_bck=none
```

Cached objects remain in the AIS bucket.

> See also: [Backend Bucket CLI examples](/docs/cli/bucket.md#connectdisconnect-ais-bucket-tofrom-cloud-bucket)

--------------------

# **Part II: How-To**

**Table of Contents**
1. [Working with Same-Name Remote Buckets](#working-with-same-name-remote-buckets)
   - [Option A: Namespaces (direct access)](#option-a-namespaces-direct-access)
   - [Option B: Backend buckets (via AIS proxy)](#option-b-backend-buckets-via-ais-proxy)
   - [Which to use?](#which-to-use)
2. [Working with Remote AIS Clusters](#working-with-remote-ais-clusters)
   - [Attach remote cluster](#attach-remote-cluster)
   - [List buckets and objects in remote clusters](#list-buckets-and-objects-in-remote-clusters)
3. [Prefetch and Evict](#prefetch-and-evict)
   - [Prefetching](#prefetching)
   - [Monitoring prefetch](#monitoring-prefetch)
   - [Evicting](#evicting)
4. [Access Control](#access-control)
   - [Setting access](#setting-access)
   - [Predefined values](#predefined-values)
5. [Provider-Specific Configuration](#provider-specific-configuration)
   - [AWS / S3-compatible](#aws--s3-compatible)
   - [Google Cloud](#google-cloud)
   - [Azure](#azure)
6. [List Objects](#list-objects)
   - [Basic usage](#basic-usage)
   - [Properties](#properties)
   - [Flags](#flags)
   - [Pagination](#pagination)
7. [Operations Summary](#operations-summary)
8. [CLI Quick Reference](#cli-quick-reference)

## Working with Same-Name Remote Buckets

A common scenario: you have buckets with identical names across different AWS accounts, S3-compatible endpoints, or cloud providers. AIS handles this two ways.

### Option A: Namespaces (direct access)

Create each bucket with its own namespace and credentials:

```console
ais create s3://#prod/data --props="extra.aws.profile=prod-account"
ais create s3://#dev/data --props="extra.aws.profile=dev-account"
```

Now `s3://#prod/data` and `s3://#dev/data` are distinct buckets - separate BMD entries, separate on-disk paths, separate credentials. Access them directly:

```console
ais ls s3://#prod/data
ais get s3://#dev/data/file.txt ./local
```

### Option B: Backend buckets (via AIS proxy)

Create AIS buckets that front the remote buckets:

```console
# First, create the namespaced S3 buckets with proper credentials
ais create s3://#prod/data --props="extra.aws.profile=prod-account"
ais create s3://#dev/data --props="extra.aws.profile=dev-account"

# Then create AIS buckets fronting them
ais create ais://prod-data --props="backend_bck=s3://#prod/data"
ais create ais://dev-data --props="backend_bck=s3://#dev/data"
```

Access through the AIS buckets:

```console
ais ls ais://prod-data

## GET and discard locally, with a side effect of **cold-GET**ting an object from remote storage
ais get ais://dev-data/images.jpg /dev/null
```

### Which to use?

| Scenario | Recommended |
|----------|-------------|
| Direct multi-account access, no caching logic | Namespaces (Option A) |
| Need LRU eviction, local caching policies | Backend buckets (Option B) |
| ETL pipelines, dataset transformation | Backend buckets (Option B) |
| Want to rename or alias cloud buckets | Backend buckets (Option B) |
| Simplest setup, fewest moving parts | Namespaces (Option A) |

Namespaces give you direct access with minimal overhead. Backend buckets add a layer of indirection but unlock full AIS bucket capabilities - LRU, mirroring, erasure coding, and transformation pipelines.

Note that Option B requires the namespaced S3 bucket to exist first. You can't skip straight to `backend_bck=s3://data` with custom credentials - AIS needs to resolve the backend bucket, which requires proper credentials already in place. Create the namespaced cloud bucket first, then front it with an AIS bucket if needed.

> See also: [AWS Profiles and S3 Endpoints](/docs/cli/aws_profile_endpoint.md)

---

## Working with Remote AIS Clusters

AIS clusters can be attached to each other, forming a global namespace of all individually hosted datasets. For background and configuration details, see [Remote AIS Cluster](providers.md#remote-ais-cluster).

### Attach remote cluster

```console
# attach a remote AIS cluster with alias `teamZ`
$ ais cluster attach teamZ=http://cluster.ais.org:51080
Remote cluster (teamZ=http://cluster.ais.org:51080) successfully attached

# Verify the attachment
$ ais show remote-cluster

UUID      URL                            Alias     Primary      Smap   Targets  Online
MCBgkFqp  http://cluster.ais.org:51080   teamZ     p[primary]   v317   10       yes
```

### List buckets and objects in remote clusters

```console
# List all buckets in all remote AIS clusters
# By convention, `@` prefixes remote cluster UUIDs
$ ais ls ais://@

AIS Buckets (4)
  ais://@MCBgkFqp/imagenet
  ais://@MCBgkFqp/coco
  ais://@MCBgkFqp/imagenet-augmented
  ais://@MCBgkFqp/imagenet-inflated

# List buckets in a specific remote cluster (by alias or UUID)
$ ais ls ais://@teamZ

AIS Buckets (4)
  ais://@MCBgkFqp/imagenet
  ais://@MCBgkFqp/coco
  ais://@MCBgkFqp/imagenet-augmented
  ais://@MCBgkFqp/imagenet-inflated

# List objects in a remote bucket
$ ais ls ais://@teamZ/imagenet-augmented
NAME              SIZE
train-001.tgz     153.52KiB
train-002.tgz     136.44KiB
...
```

---

## Prefetch and Evict

### Prefetching

Proactively fetch objects from remote storage into AIS cache:

```console
# Prefetch by list
ais prefetch s3://bucket --list "obj1,obj2,obj3"

# Prefetch by prefix
ais prefetch s3://bucket --prefix "images/"

# Prefetch by template
ais prefetch s3://bucket --template "shard-{0000..0999}.tar"

# With parallelism control
ais prefetch s3://bucket --prefix data/ --num-workers 16
```

### Monitoring prefetch

```console
# Check progress
ais show job prefetch

# With auto-refresh
ais show job prefetch --refresh 5

# Wait for completion
ais wait prefetch JOB_ID
```

### Evicting

Remove cached objects (cloud data untouched):

```console
# Evict entire bucket
ais evict s3://bucket

# Keep metadata, remove objects
ais evict s3://bucket --keep-md

# Evict by prefix
ais evict s3://bucket --prefix old-data/

# Evict by template
ais evict s3://bucket --template "temp-{0..999}.dat"
```

> **Note:** The terms "cached" and "in-cluster" are used interchangeably. A "cached" object is one that exists in AIS storage regardless of its origin.

---

## Access Control

Bucket access is controlled by a 64-bit `access` property. Bits map to operations:

> **Note**: When [enabled](docs/authn.md), access permissions are enforced by AIS and apply to both local and backend operations; misconfiguration can block cold GETs or deletes. See [version 4.1 release notes](https://github.com/NVIDIA/aistore/releases/tag/v1.4.1#authentication-and-security) for additional pointers on the topics of authentication and security.

| Operation | Bit | Hex |
|-----------|-----|-----|
| GET | 0 | 0x1 |
| HEAD | 1 | 0x2 |
| PUT, APPEND | 2 | 0x4 |
| Cold GET | 3 | 0x8 |
| DELETE | 4 | 0x10 |

### Setting access

```console
# Make bucket read-only
ais bucket props set ais://data access=ro

# Disable DELETE
ais bucket props set ais://data access=0xFFFFFFFFFFFFFFEF
```

### Predefined values

| Name | Meaning |
|------|---------|
| `ro` | Read-only (GET + HEAD) |
| `rw` | Full access (default) |

> See also: [Authentication and Access Control](/docs/authn.md)

---

## Provider-Specific Configuration

### AWS / S3-compatible

| Property | Description |
|----------|-------------|
| `extra.aws.profile` | Named AWS profile from `~/.aws/credentials` |
| `extra.aws.endpoint` | Custom S3 endpoint URL |
| `extra.aws.region` | Region override |

```console
# Use named profile
ais create s3://bucket --props="extra.aws.profile=production"

# S3-compatible endpoint (SwiftStack, Oracle OCI, AWS S3, etc.)
ais create s3://#minio/bucket \
  --props="extra.aws.endpoint=http://minio:9000 extra.aws.profile=minio-creds"
```

> See also: [AWS Profiles and S3 Endpoints](/docs/cli/aws_profile_endpoint.md)

### Google Cloud

| Property | Description |
|----------|-------------|
| `extra.gcp.project_id` | GCP project ID |

### Azure

| Property | Description |
|----------|-------------|
| `extra.azure.account_name` | Storage account name |
| `extra.azure.account_key` | Storage account key |

---

## List Objects

`ListObjects` and `ListObjectsPage` API (Go and Python) return object names and properties. For large - many millions of objects - buckets we strongly recommend the "paginated" version of the API.

AIS CLI supports both - a quick glance at `ais ls --help` will provide an idea of all (numerous) supported options.

### Basic usage

As always with AIS CLI, a quick look at the command's help (`ais ls --help` in this case) _may_ save time.

```console
# When remote bucket does not exist in AIS _and_ is accessible with default profile/endpoint
$ ais ls s3://bucket
Error: ErrRemoteBckNotFound: aws bucket "s3://ais-vm" does not exist
Tip: use '--all' to list all objects including remote

# and note: `--all` can be used just once and only when the remote is not in AIS
$ ais ls s3://bucket --all --limit 4
```

More basic examples follow below:

```console
# List all objects
ais ls s3://bucket

# List with prefix
ais ls s3://bucket --prefix images/

# Same as above
ais ls s3://bucket/images/

# List cached only
ais ls s3://bucket --cached

# Summary (counts and sizes)
ais ls s3://bucket --summary
```

### Properties

Request specific properties with `--props`:

| Property | Description |
|----------|-------------|
| `name` | Object name (always included) |
| `size` | Object size |
| `version` | Object version |
| `checksum` | Object checksum |
| `atime` | Last access time |
| `location` | Target and mountpath |
| `copies` | Number of copies |
| `ec` | Erasure coding info |
| `status` | Object status |

```console
ais ls s3://bucket --props "name,size,atime,copies"
```

### Flags

| Flag | Description |
|------|-------------|
| `--cached` | Only objects present in AIS |
| `--all` | Include all buckets (remote and present) |
| `--regex` | Filter by regex pattern |
| `--summary` | Show aggregate statistics |
| `--limit N` | Return at most N objects |

### Pagination

For large buckets, results are paginated:

```console
# API returns continuation_token for next page
# CLI handles pagination automatically
ais ls s3://large-bucket --limit 10000
```

> See also: [CLI: List Objects](/docs/cli/bucket.md#list-objects)

---

## Operations Summary

| Command | Behavior |
|---------|----------|
| `ais ls <bucket>` | List objects; implicit create for remote buckets |
| `ais create <bucket>` | Explicit creation with optional properties |
| `ais bucket rm <ais-bucket>` | Destroy AIS bucket and all objects |
| `ais bucket rm <cloud-bucket>` | Remove from BMD, evict cached objects |
| `ais evict <bucket>` | Same as rm for cloud buckets |
| `ais prefetch <bucket>` | Proactively cache remote objects |
| `ais bucket props set` | Update properties, metasync cluster-wide |
| `ais bucket props reset` | Restore cluster defaults |
| `ais bucket props show` | Display current properties |

All operations respect namespaces. `ais ls s3://#ns1/bucket` and `ais ls s3://#ns2/bucket` operate on different buckets.

---

## CLI Quick Reference

```console
# ─────────────────────────────────────────────────────────────
# Implicit creation - bucket added on first access
# ─────────────────────────────────────────────────────────────
ais ls s3://my-bucket
ais get s3://my-bucket/file.txt ./local-file

# ─────────────────────────────────────────────────────────────
# Explicit creation - custom credentials
# ─────────────────────────────────────────────────────────────
ais create s3://my-bucket --props="extra.aws.profile=prod"

# ─────────────────────────────────────────────────────────────
# Namespaced buckets - same name, different accounts
# ─────────────────────────────────────────────────────────────
ais create s3://#acct1/data --props="extra.aws.profile=acct1"
ais create s3://#acct2/data --props="extra.aws.profile=acct2"

# ─────────────────────────────────────────────────────────────
# Skip lookup - bucket exists but default creds can't reach it
# ─────────────────────────────────────────────────────────────
ais create s3://restricted --skip-lookup \
  --props="extra.aws.profile=special"

# ─────────────────────────────────────────────────────────────
# Remote AIS cluster
# ─────────────────────────────────────────────────────────────
ais cluster remote-attach remais=http://remote:8080
ais ls ais://@remais/
ais create ais://@remais/new-bucket

# ─────────────────────────────────────────────────────────────
# Backend buckets - AIS bucket fronting cloud
# ─────────────────────────────────────────────────────────────
ais create ais://cache
ais bucket props set ais://cache backend_bck=s3://origin

# ─────────────────────────────────────────────────────────────
# Prefetch and evict
# ─────────────────────────────────────────────────────────────
ais prefetch s3://bucket --prefix data/
ais evict s3://bucket --keep-md

# ─────────────────────────────────────────────────────────────
# Properties
# ─────────────────────────────────────────────────────────────
ais bucket props show s3://bucket
ais bucket props set s3://bucket mirror.enabled=true mirror.copies=2
ais bucket props reset s3://bucket
```

--------------------

## Appendix A: On-Disk Layout

**Note:** This section is provided for advanced troubleshooting and debugging only.

The bucket identity you specify in CLI or API - provider, namespace, bucket name - materializes as directory structure on every [mountpath](/docs/overview.md#mountpath). This isn't just metadata; it's physical layout.

Say, we have an S3 bucket called `s3://dataset`, and an object `images/cat.jpg` in it. Given two different bucket namespaces, the respective FQNs inside AIStore may look like:

```
/ais/mp1/@aws/#prod/dataset/%ob/images/cat.jpg

and

/ais/mp4/@aws/#dev/dataset/%ob/images/cat.jpg
```

where:

| Component | Example 1 | Example 2 | Meaning |
|-----------|-----------|-----------|---------|
| Mountpath | `/ais/mp1` | `/ais/mp4` | Physical disk or partition |
| Provider | `@aws` | `@aws` | Backend provider |
| Namespace | `#prod` | `#dev` | Account, profile, or user-defined alias |
| Bucket | `dataset` | `dataset` | Bucket name |
| Content type | `%ob` | `%ob` | Content kind: objects, EC slices, chunks, metadata |
| Object | `images/cat.jpg` | `images/cat.jpg` | Object name (preserves virtual directory structure) |

> Note: disk partitioning not recommended, may degrade performance.

The namespace you choose - whether it maps to an AWS profile, a SwiftStack account, or just a human-readable tag like `#prod` - becomes a physical directory on every target node. This guarantees:

- **Isolation**: `s3://#acct1/data` and `s3://#acct2/data` never share storage paths
- **No collision**: Same-name buckets with different namespaces coexist without conflict

What starts as a logical identifier in `ais create s3://#prod/bucket` ends up as `/mpath/@aws/#prod/bucket/` on disk.

> For details, see [On-Disk Layout](/docs/on_disk_layout.md) document.

---

## References

* [Backend Providers](/docs/providers.md)
* [Storage Services](/docs/storage_svcs.md)
* [Configuration](/docs/configuration.md)
* [CLI: Bucket Commands](/docs/cli/bucket.md)
* [CLI: AWS Profiles and Endpoints](/docs/cli/aws_profile_endpoint.md)
* [Authentication](/docs/authn.md)
* [Out-of-Band Updates](/docs/out_of_band.md)
