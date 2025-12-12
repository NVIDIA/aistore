## Feature flags

`Feature flags` are represented as a 64-bit bitmask field in aistore cluster configuration denoting assorted (named) capabilities that can be individually enabled at runtime.

The features themselves are enumerated below. Not all feature flags - henceforth, "features" - are cluster-global.

Assorted features, denoted by `(*)` below, can also be changed on a per-bucket basis.

By default, all features are disabled, and the corresponding 64-bit field is set to zero.

**Table of Contents**

- [Tagging system](#tagging-system)
- [Validation and conflicts](#validation-and-conflicts)
- [Names and comments](#names-and-comments)
- [Global features](#global-features)
- [Bucket features](#bucket-features)

## Tagging system

Feature flags are now organized with descriptive tags to help users understand their purpose and impact. The CLI displays features in a 3-column format: **FEATURE | TAGS | DESCRIPTION**.

### Tag categories

- **Domain-specific**: `s3`, `lz4`, `etl`, `mpu`, `telemetry`
- **Performance impact**: `perf`, `overhead`
- **Network/operations**: `net`, `ops`, `security`
- **Compatibility**: `compat`, `promote`

### Integrity impact indicators

Feature flags that affect data integrity are marked with directional indicators:

- **`integrity+`** - Enhances data integrity (safer, more conservative)
- **`integrity-`** - Potentially compromises data integrity (trading safety for performance)
- **`integrity?`** - Complex integrity implications (depends on deployment and runtime context)

This helps operators quickly identify flags that involve safety trade-offs.

## Validation and conflicts

Feature flags now include validation logic to prevent conflicting configurations:

- **`Disable-Cold-GET`** and **`Streaming-Cold-GET`** are mutually exclusive
- Additional validation rules may be added for other conflicting combinations

The validation occurs both at the cluster level and when setting bucket properties.

## Names and comments

| name | tags | comment |
| --- | --- | ------- |
| `Enforce-IntraCluster-Access` | `security` | when enabled, aistore targets will make sure _not_ to execute direct (ie., not redirected) API calls |
| `Skip-Loading-VersionChecksum-MD(*)` | `perf,integrity-` | skip loading existing object's metadata, Version and Checksum (VC) in particular |
| `Do-not-Auto-Detect-FileShare` | `promote,ops` | do not auto-detect file share (NFS, SMB) when _promoting_ shared files to AIS |
| `S3-API-via-Root` | `s3,compat,ops` | handle S3 requests via `aistore-hostname/` (whereby the default: `aistore-hostname/s3`) |
| `Fsync-PUT(*)` | `integrity+,overhead` | PUT and cold-GET: commit (or sync) the object payload to stable storage |
| `LZ4-Block-1MB` | `lz4` | .tar.lz4 format, lz4 compression: maximum uncompressed block size=1MB (default: 256K) |
| `LZ4-Frame-Checksum` | `lz4` | checksum lz4 frames |
| `Do-not-Allow-Passing-FQN-to-ETL` | `etl,security` | do not allow passing fully-qualified name of a locally stored object to (local) ETL containers |
| `Ignore-LimitedCoexistence-Conflicts` | `ops,integrity-` | run in presence of "limited coexistence" type conflicts |
| `S3-Presigned-Request(*)` | `s3,security,compat` | pass-through client-signed (presigned) S3 requests for subsequent authentication by S3 |
| `Do-not-Optimize-Listing-Virtual-Dirs` | `overhead` | when prefix doesn't end with '/' and is a subdirectory: don't assume there are no _prefixed_ object names |
| `Disable-Cold-GET` | `perf,integrity-` | do not perform [cold GET](/docs/overview.md#existing-datasets) request when using remote bucket |
| `Streaming-Cold-GET` | `perf,integrity-` | write and transmit cold-GET content back to user in parallel, without _finalizing_ in-cluster object |
| `S3-Reverse-Proxy` | `s3,net,ops` | use reverse proxy calls instead of HTTP-redirect for S3 API |
| `S3-Use-Path-Style` | `s3,compat` | use older path-style addressing (as opposed to virtual-hosted style), e.g., https://s3.amazonaws.com/BUCKET/KEY |
| `Do-not-Delete-When-Rebalancing` | `integrity?,ops` | disable lazy deletion during global rebalance: do not delete misplaced sources of the migrated objects |
| `Do-not-Set-Control-Plane-ToS` | `net,ops` | intra-cluster control plane: use default network priority (do not set IPv4 ToS to low-latency) |
| `Trust-Crypto-Safe-Checksums` | `integrity+,overhead` | when checking whether objects are identical trust only cryptographically secure checksums |
| `S3-ListObjectVersions` | `s3,overhead` | when versioning info is requested, use ListObjectVersions API (beware: extremely slow, versioned S3 buckets only) |
| `Enable-Detailed-Prom-Metrics` | `telemetry,overhead` | include (bucket, xaction) Prometheus variable labels with every GET and PUT transaction |
| `System-Reserved` | `ops` | system-reserved (do not set: the flag may be redefined or removed at any time) |
| `Resume-Interrupted-MPU` | `mpu,ops` | resume interrupted multipart uploads from persisted partial manifests |
| `Keep-Unknown-FQN` | `integrity?,ops` | do not delete unrecognized/invalid FQNs during space cleanup ('ais space-cleanup') |
| `Load-Balance-GET` | `perf` | when bucket is n-way mirrored read object replica from the least-utilized mountpath |

## Global features

```console
$ ais config cluster features <TAB-TAB>

Enforce-IntraCluster-Access            S3-Presigned-Request                   S3-ListObjectVersions
Skip-Loading-VersionChecksum-MD        Do-not-Optimize-Listing-Virtual-Dirs   Enable-Detailed-Prom-Metrics
Do-not-Auto-Detect-FileShare           Disable-Cold-GET                       System-Reserved
S3-API-via-Root                        Streaming-Cold-GET                     Resume-Interrupted-MPU
Fsync-PUT                              S3-Reverse-Proxy                       Keep-Unknown-FQN
LZ4-Block-1MB                          S3-Use-Path-Style                      Load-Balance-GET
LZ4-Frame-Checksum                     Do-not-Delete-When-Rebalancing         none
Do-not-Allow-Passing-FQN-to-ETL        Do-not-Set-Control-Plane-ToS
Ignore-LimitedCoexistence-Conflicts    Trust-Crypto-Safe-Checksums
```

For example:

```console
$ ais config cluster features S3-API-via-Root  Skip-Loading-VersionChecksum-MD  Load-Balance-GET

PROPERTY         VALUE
features         Skip-Loading-VersionChecksum-MD
                 S3-API-via-Root
                 Load-Balance-GET

FEATURE                              TAGS                    DESCRIPTION
Enforce-IntraCluster-Access          security               enforce intra-cluster access
Skip-Loading-VersionChecksum-MD      perf,integrity-        (*) skip loading existing object's metadata, Version and Checksum (VC) in particular              <<< colored
Do-not-Auto-Detect-FileShare         promote,ops            do not auto-detect file share (NFS, SMB) when _promoting_ shared files to AIS
S3-API-via-Root                      s3,compat,ops          handle s3 requests via `aistore-hostname/` (default: `aistore-hostname/s3`)                       <<< colored
Fsync-PUT                            integrity+,overhead    (*) when finalizing PUT(object): fflush prior to (close, rename) sequence
LZ4-Block-1MB                        lz4                    .tar.lz4 format, lz4 compression: max uncompressed block size=1MB (default: 256K)
LZ4-Frame-Checksum                   lz4                    checksum lz4 frames (default: don't)
Do-not-Allow-Passing-FQN-to-ETL      etl,security           do not allow passing fully-qualified name of a locally stored object to (local) ETL containers
Ignore-LimitedCoexistence-Conflicts  ops,integrity-         run in presence of _limited coexistence_ type conflicts (same as e.g. CopyBckMsg.Force but globally)
S3-Presigned-Request                 s3,security,compat     (*) pass-through client-signed (presigned) S3 requests for subsequent authentication by S3
Do-not-Optimize-Listing-Virtual-Dirs overhead               when prefix doesn't end with '/' and is a subdirectory: don't assume there are no _prefixed_ obj names
Disable-Cold-GET                     perf,integrity-        disable cold-GET (from remote bucket)
Streaming-Cold-GET                   perf,integrity-        write and transmit cold-GET content back to user in parallel, without _finalizing_ in-cluster object
S3-Reverse-Proxy                     s3,net,ops             intra-cluster communications: instead of regular HTTP redirects reverse-proxy S3 API calls to designated targets
S3-Use-Path-Style                    s3,compat              use older path-style addressing (as opposed to virtual-hosted style), e.g., https://s3.amazonaws.com/BUCKET/KEY
Do-not-Delete-When-Rebalancing       integrity?,ops         disable lazy deletion during global rebalance: do not delete misplaced sources of the migrated objects
Do-not-Set-Control-Plane-ToS         net,ops                intra-cluster control plane: use default network priority (do not set IPv4 ToS to low-latency)
Trust-Crypto-Safe-Checksums          integrity+,overhead    when checking whether objects are identical trust only cryptographically secure checksums
S3-ListObjectVersions                s3,overhead            when versioning info is requested, use ListObjectVersions API (beware: extremely slow, versioned S3 buckets only)
Enable-Detailed-Prom-Metrics         telemetry,overhead     include (bucket, xaction) Prometheus variable labels with every GET and PUT transaction
System-Reserved                      ops                    system-reserved (do not set: the flag may be redefined or removed at any time)
Resume-Interrupted-MPU               mpu,ops                resume interrupted multipart uploads from persisted partial manifests
Keep-Unknown-FQN                     integrity?,ops         do not delete unrecognized/invalid FQNs during space cleanup ('ais space-cleanup')
Load-Balance-GET                     perf                   when bucket is n-way mirrored read object replica from the least-utilized mountpath               <<< colored

Cluster config updated
```

> Notice the [FEATURE, TAGS, DESCRIPTION] table above that shows all supported feature flags along with their respective tags and descriptions. Currently selected features are shown in color. Tags help identify the purpose and impact of each feature at a glance.

> **Tip**: Use the tags to quickly identify features by category. For example, look for `integrity-` tags to identify features that may trade data integrity for performance, or `overhead` tags for features that may impact performance.

To view the current (configured) setting, type the same command and hit `Enter`:

```console
$ ais config cluster features

PROPERTY         VALUE
features         Skip-Loading-VersionChecksum-MD
                 S3-API-via-Root
                 Load-Balance-GET

FEATURE                              TAGS                    DESCRIPTION
Enforce-IntraCluster-Access          security               enforce intra-cluster access
Skip-Loading-VersionChecksum-MD      perf,integrity-        (*) skip loading existing object's metadata, Version and Checksum (VC) in particular               <<< colored
Do-not-Auto-Detect-FileShare         promote,ops            do not auto-detect file share (NFS, SMB) when _promoting_ shared files to AIS
S3-API-via-Root                      s3,compat,ops          handle s3 requests via `aistore-hostname/` (default: `aistore-hostname/s3`)                        <<< colored
Fsync-PUT                            integrity+,overhead    (*) when finalizing PUT(object): fflush prior to (close, rename) sequence
LZ4-Block-1MB                        lz4                    .tar.lz4 format, lz4 compression: max uncompressed block size=1MB (default: 256K)
LZ4-Frame-Checksum                   lz4                    checksum lz4 frames (default: don't)
Do-not-Allow-Passing-FQN-to-ETL      etl,security           do not allow passing fully-qualified name of a locally stored object to (local) ETL containers
Ignore-LimitedCoexistence-Conflicts  ops,integrity-         run in presence of _limited coexistence_ type conflicts (same as e.g. CopyBckMsg.Force but globally)
S3-Presigned-Request                 s3,security,compat     (*) pass-through client-signed (presigned) S3 requests for subsequent authentication by S3
Do-not-Optimize-Listing-Virtual-Dirs overhead               when prefix doesn't end with '/' and is a subdirectory: don't assume there are no _prefixed_ obj names
Disable-Cold-GET                     perf,integrity-        disable cold-GET (from remote bucket)
Streaming-Cold-GET                   perf,integrity-        write and transmit cold-GET content back to user in parallel, without _finalizing_ in-cluster object
S3-Reverse-Proxy                     s3,net,ops             intra-cluster communications: instead of regular HTTP redirects reverse-proxy S3 API calls to designated targets
S3-Use-Path-Style                    s3,compat              use older path-style addressing (as opposed to virtual-hosted style), e.g., https://s3.amazonaws.com/BUCKET/KEY
Do-not-Delete-When-Rebalancing       integrity?,ops         disable lazy deletion during global rebalance: do not delete misplaced sources of the migrated objects
Do-not-Set-Control-Plane-ToS         net,ops                intra-cluster control plane: use default network priority (do not set IPv4 ToS to low-latency)
Trust-Crypto-Safe-Checksums          integrity+,overhead    when checking whether objects are identical trust only cryptographically secure checksums
S3-ListObjectVersions                s3,overhead            when versioning info is requested, use ListObjectVersions API (beware: extremely slow, versioned S3 buckets only)
Enable-Detailed-Prom-Metrics         telemetry,overhead     include (bucket, xaction) Prometheus variable labels with every GET and PUT transaction
System-Reserved                      ops                    system-reserved (do not set: the flag may be redefined or removed at any time)
Resume-Interrupted-MPU               mpu,ops                resume interrupted multipart uploads from persisted partial manifests
Keep-Unknown-FQN                     integrity?,ops         do not delete unrecognized/invalid FQNs during space cleanup ('ais space-cleanup')
Load-Balance-GET                     perf                   when bucket is n-way mirrored read object replica from the least-utilized mountpath               <<< colored
```

The same in JSON:

```console
$ ais config cluster features --json
{
    "features": "1552"
}
```

Finally, to reset feature flags back to the system defaults, `<TAB-TAB>` to select `none` from the completion list, or simply run:

```console
$ ais config cluster features none

PROPERTY         VALUE
features         none

Cluster config updated
```

## Bucket features

By default, created and added (ie., discovered upon remote lookup) buckets inherit their properties from the cluster.

> For background and details, see [Bucket Properties](/docs/bucket.md#bucket-properties).

It is possible, however, to change the defaults both at bucket creation time and/or at any later time.

Here's a brief 1-2-3 demonstration in re specifically: feature flags.

#### 1. show existing bucket-scope features

```console
$ ais bucket props set ais://nnn features <TAB-TAB>

Skip-Loading-VersionChecksum-MD   Disable-Cold-GET                  S3-ListObjectVersions
Fsync-PUT                         Streaming-Cold-GET                Resume-Interrupted-MPU
S3-Presigned-Request              S3-Use-Path-Style                 none
```

#### 2. select and set

```console
$ ais bucket props set ais://nnn features S3-Presigned-Request

"features" set to: "S3-Presigned-Request" (was: "none")

Bucket props successfully updated.

FEATURE                          TAGS                    DESCRIPTION
Skip-Loading-VersionChecksum-MD  perf,integrity-        (*) skip loading existing object's metadata, Version and Checksum (VC) in particular
Fsync-PUT                        integrity+,overhead    (*) when finalizing PUT(object): fflush prior to (close, rename) sequence
S3-Presigned-Request             s3,security,compat     (*) pass-through client-signed (presigned) S3 requests for subsequent authentication by S3   <<<<<< colored
Disable-Cold-GET                 perf,integrity-        disable cold-GET (from remote bucket)
Streaming-Cold-GET               perf,integrity-        write and transmit cold-GET content back to user in parallel, without _finalizing_ in-cluster object
S3-Use-Path-Style                s3,compat              use older path-style addressing (as opposed to virtual-hosted style), e.g., https://s3.amazonaws.com/BUCKET/KEY
```

#### 3. reset feature flags back to zero (or 'none')

```console
$ ais bucket props set ais://nnn features none
"features" set to: "0" (was: "S3-Presigned-Request")

Bucket props successfully updated.

FEATURE                          TAGS                    DESCRIPTION
Skip-Loading-VersionChecksum-MD  perf,integrity-        (*) skip loading existing object's metadata, Version and Checksum (VC) in particular
Fsync-PUT                        integrity+,overhead    (*) when finalizing PUT(object): fflush prior to (close, rename) sequence
S3-Presigned-Request             s3,security,compat     (*) pass-through client-signed (presigned) S3 requests for subsequent authentication by S3
Disable-Cold-GET                 perf,integrity-        disable cold-GET (from remote bucket)
Streaming-Cold-GET               perf,integrity-        write and transmit cold-GET content back to user in parallel, without _finalizing_ in-cluster object
S3-Use-Path-Style                s3,compat              use older path-style addressing (as opposed to virtual-hosted style), e.g., https://s3.amazonaws.com/BUCKET/KEY
Resume-Interrupted-MPU           mpu,ops                resume interrupted multipart uploads from persisted partial manifests
S3-ListObjectVersions            s3,overhead            when versioning info is requested, use ListObjectVersions API (beware: extremely slow, versioned S3 buckets only)
```

### Validation errors

When setting conflicting feature flags, the system will reject the configuration - e.g.:

```console
$ ais config cluster features Disable-Cold-GET Streaming-Cold-GET

Error: feature flags "Disable-Cold-GET" and "Streaming-Cold-GET" are mutually exclusive
```
