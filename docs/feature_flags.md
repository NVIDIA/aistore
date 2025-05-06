---
layout: post
title: FEATURE FLAGS
permalink: /docs/feature-flags
redirect_from:
 - /feature_flags.md/
 - /docs/feature_flags.md/
---

## Table of Contents

- [Feature flags](#feature-flags)
- [Names and comments](#names-and-comments)
- [Global features](#global-features)
- [Bucket features](#bucket-features)

## Feature flags

`Feature flags` is a 64-bit (bit-wise) field in aistore cluster configuration denoting assorted (named) capabilities that can be individually enabled at runtime.

The features themselves are enumerated below. Not all feature flags - henceforth, "features" - are cluster-global.

Assorted features, denoted by `(*)` below, can also be changed on a per-bucket basis.

By default, all features are disabled, and the corresponding 64-bit field is set to zero.

## Names and comments

| name | comment |
| --- | ------- |
| `Enforce-IntraCluster-Access` | when enabled, aistore targets will make sure _not_ to execute direct (ie., not redirected) API calls |
| `S3-API-via-Root` | handle S3 requests via `aistore-hostname/` (whereby the default: `aistore-hostname/s3`) |
| `Do-not-Allow-Passing-FQN-to-ETL` |  do not allow passing fully-qualified name of a locally stored object to (local) ETL containers |
| `Fsync-PUT(*)` | PUT and cold-GET: commit (or sync) the object payload to stable storage |
| `Ignore-LimitedCoexistence-Conflicts` | run in presence of "limited coexistence" type conflicts |
| `Skip-Loading-VersionChecksum-MD(*)` | skip loading existing object's metadata, Version and Checksum (VC) in particular |
| `LZ4-Block-1MB` | .tar.lz4 format, lz4 compression: maximum uncompressed block size=1MB (default: 256K) |
| `LZ4-Frame-Checksum` | checksum lz4 frames |
| `Do-not-Auto-Detect-FileShare` | do not auto-detect file share (NFS, SMB) when _promoting_ shared files to AIS |
| `S3-Presigned-Request(*)` | pass-through client-signed (presigned) S3 requests for subsequent authentication by S3 |
| `Do-not-Optimize-Listing-Virtual-Dirs` | when prefix doesn't end with '/' and is a subdirectory: don't assume there are no _prefixed_ object names (as in: `a/subdir/obj1`, `a/subdir/obj2`, but also `a/subdir-obj3`) |
| `Disable-Cold-GET` | do not perform [cold GET](/docs/overview.md#existing-datasets) request when using remote bucket |
| `S3-Reverse-Proxy` | use reverse proxy calls instead of HTTP-redirect for S3 API |
| `S3-Use-Path-Style` | use older path-style addressing (as opposed to virtual-hosted style), e.g., https://s3.amazonaws.com/BUCKET/KEY |
| `Do-not-Delete-When-Rebalancing` | disable lazy deletion during global rebalance: do not delete misplaced sources of the migrated objects |
| `Do-not-Set-Control-Plane-ToS` | intra-cluster control plane: do not set IPv4 ToS field (to low-latency) |
| `Trust-Crypto-Safe-Checksums` | when checking whether objects are identical trust only cryptographically secure checksums |
| `S3-ListObjectVersions` | when versioning info is requested, use ListObjectVersions API (beware: extremely slow, versioned S3 buckets only) |
| `Enable-Detailed-Prom-Metrics` | include (bucket, xaction) Prometheus variable labels with every GET and PUT transaction |

## Global features

```console
$ ais config cluster features <TAB-TAB>

Enforce-IntraCluster-Access            Do-not-Allow-Passing-FQN-to-ETL        S3-Use-Path-Style
Skip-Loading-VersionChecksum-MD        Ignore-LimitedCoexistence-Conflicts    Do-not-Delete-When-Rebalancing
Do-not-Auto-Detect-FileShare           S3-Presigned-Request                   Do-not-Set-Control-Plane-ToS
S3-API-via-Root                        Do-not-Optimize-Listing-Virtual-Dirs   Trust-Crypto-Safe-Checksums
Fsync-PUT                              Disable-Cold-GET                       S3-ListObjectVersions
LZ4-Block-1MB                          Streaming-Cold-GET                     Enable-Detailed-Prom-Metrics
LZ4-Frame-Checksum                     S3-Reverse-Proxy                       none
```

For example:

```console
$ ais config cluster features S3-API-via-Root Skip-Loading-VersionChecksum-MD Ignore-LimitedCoexistence-Conflicts

PROPERTY         VALUE
features         Skip-Loading-VersionChecksum-MD
                 S3-API-via-Root
                 Ignore-LimitedCoexistence-Conflicts

FEATURE                              DESCRIPTION
Enforce-IntraCluster-Access          enforce intra-cluster access
Skip-Loading-VersionChecksum-MD      (*) skip loading existing object's metadata, Version and Checksum (VC) in particular                  <<< colored
Do-not-Auto-Detect-FileShare         do not auto-detect file share (NFS, SMB) when _promoting_ shared files to AIS
S3-API-via-Root                      handle s3 requests via `aistore-hostname/` (default: `aistore-hostname/s3`)
Fsync-PUT                            (*) when finalizing PUT(object): fflush prior to (close, rename) sequence                             <<< colored
LZ4-Block-1MB                        .tar.lz4 format, lz4 compression: max uncompressed block size=1MB (default: 256K)
LZ4-Frame-Checksum                   checksum lz4 frames (default: don't)
Do-not-Allow-Passing-FQN-to-ETL      do not allow passing fully-qualified name of a locally stored object to (local) ETL containers
Ignore-LimitedCoexistence-Conflicts  run in presence of _limited coexistence_ type conflicts (same as e.g. CopyBckMsg.Force but globally)  <<< colored
S3-Presigned-Request                 (*) pass-through client-signed (presigned) S3 requests for subsequent authentication by S3
Do-not-Optimize-Listing-Virtual-Dirs when prefix doesn't end with '/' and is a subdirectory: don't assume there are no _prefixed_ obj names
Disable-Cold-GET                     disable cold-GET (from remote bucket)
Streaming-Cold-GET                   write and transmit cold-GET content back to user in parallel, without _finalizing_ in-cluster object
S3-Reverse-Proxy                     intra-cluster communications: instead of regular HTTP redirects reverse-proxy S3 API calls to designated targets
S3-Use-Path-Style                    use older path-style addressing (as opposed to virtual-hosted style), e.g., https://s3.amazonaws.com/BUCKET/KEY
Do-not-Delete-When-Rebalancing       disable lazy deletion during global rebalance: do not delete misplaced sources of the migrated objects
Do-not-Set-Control-Plane-ToS         intra-cluster control plane: do not set IPv4 ToS field (to low-latency)
Trust-Crypto-Safe-Checksums          when checking whether objects are identical trust only cryptographically secure checksums
S3-ListObjectVersions                when versioning info is requested, use ListObjectVersions API (beware: extremely slow, versioned S3 buckets only)
Enable-Detailed-Prom-Metrics         include (bucket, xaction) Prometheus variable labels with every GET and PUT transaction

Cluster config updated
```

> Notice the [FEATURE, DESCRIPTION] table above that shows all supported feature flags along with their respective descriptions. In additional, currently selectly features will show up colored.

> **Tip**: to select multiple features, type the first letter (or the few first letters) of the one you select, press `<TAB-TAB>` to complete, and then press `<TAB-TAB>` again to keep going, if need be.

To view the current (configured) setting, type the same command and hit `Enter`:

```console
$ ais config cluster features

PROPERTY         VALUE
features         Skip-Loading-VersionChecksum-MD
                 S3-API-via-Root
                 Ignore-LimitedCoexistence-Conflicts

FEATURE                              DESCRIPTION
Enforce-IntraCluster-Access          enforce intra-cluster access
Skip-Loading-VersionChecksum-MD      (*) skip loading existing object's metadata, Version and Checksum (VC) in particular                  <<< colored
Do-not-Auto-Detect-FileShare         do not auto-detect file share (NFS, SMB) when _promoting_ shared files to AIS
S3-API-via-Root                      handle s3 requests via `aistore-hostname/` (default: `aistore-hostname/s3`)
Fsync-PUT                            (*) when finalizing PUT(object): fflush prior to (close, rename) sequence                             <<< colored
LZ4-Block-1MB                        .tar.lz4 format, lz4 compression: max uncompressed block size=1MB (default: 256K)
LZ4-Frame-Checksum                   checksum lz4 frames (default: don't)
Do-not-Allow-Passing-FQN-to-ETL      do not allow passing fully-qualified name of a locally stored object to (local) ETL containers
Ignore-LimitedCoexistence-Conflicts  run in presence of _limited coexistence_ type conflicts (same as e.g. CopyBckMsg.Force but globally)  <<< colored
S3-Presigned-Request                 (*) pass-through client-signed (presigned) S3 requests for subsequent authentication by S3
Do-not-Optimize-Listing-Virtual-Dirs when prefix doesn't end with '/' and is a subdirectory: don't assume there are no _prefixed_ obj names
Disable-Cold-GET                     disable cold-GET (from remote bucket)
Streaming-Cold-GET                   write and transmit cold-GET content back to user in parallel, without _finalizing_ in-cluster object
S3-Reverse-Proxy                     intra-cluster communications: instead of regular HTTP redirects reverse-proxy S3 API calls to designated targets
S3-Use-Path-Style                    use older path-style addressing (as opposed to virtual-hosted style), e.g., https://s3.amazonaws.com/BUCKET/KEY
Do-not-Delete-When-Rebalancing       disable lazy deletion during global rebalance: do not delete misplaced sources of the migrated objects
Do-not-Set-Control-Plane-ToS         intra-cluster control plane: do not set IPv4 ToS field (to low-latency)
Trust-Crypto-Safe-Checksums          when checking whether objects are identical trust only cryptographically secure checksums
S3-ListObjectVersions                when versioning info is requested, use ListObjectVersions API (beware: extremely slow, versioned S3 buckets only)
Enable-Detailed-Prom-Metrics         include (bucket, xaction) Prometheus variable labels with every GET and PUT transaction
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

> For background and details, see [Default Bucket Properties](/docs/bucket.md#default-bucket-properties).

It is possible, however, to change the defaults both at bucket creation time and/or at any later time.

Here's a brief 1-2-3 demonstration in re specifically: feature flags.

#### 1. show existing bucket-scope features

```console
$ ais bucket props set ais://nnn features <TAB-TAB>
Skip-Loading-VersionChecksum-MD   Fsync-PUT        S3-Presigned-Request       none
```

#### 2. select and set

```console
$ ais bucket props set ais://nnn features S3-Presigned-Request

"features" set to: "S3-Presigned-Request" (was: "none")

Bucket props successfully updated.

FEATURE                          DESCRIPTION
Skip-Loading-VersionChecksum-MD  (*) skip loading existing object's metadata, Version and Checksum (VC) in particular
Fsync-PUT                        (*) when finalizing PUT(object): fflush prior to (close, rename) sequence
S3-Presigned-Request             (*) pass-through client-signed (presigned) S3 requests for subsequent authentication by S3   <<<<<< colored
Disable-Cold-GET                 disable cold-GET (from remote bucket)
Streaming-Cold-GET               write and transmit cold-GET content back to user in parallel, without _finalizing_ in-cluster object
S3-Use-Path-Style                use older path-style addressing (as opposed to virtual-hosted style), e.g., https://s3.amazonaws.com/BUCKET/KEY
```

#### 3. reset feature flags back to zero (or 'none')

```console
$ ais bucket props set ais://nnn features none
"features" set to: "0" (was: "S3-Presigned-Request")

Bucket props successfully updated.

FEATURE                          DESCRIPTION
Skip-Loading-VersionChecksum-MD  (*) skip loading existing object's metadata, Version and Checksum (VC) in particular
Fsync-PUT                        (*) when finalizing PUT(object): fflush prior to (close, rename) sequence
S3-Presigned-Request             (*) pass-through client-signed (presigned) S3 requests for subsequent authentication by S3
Disable-Cold-GET                 disable cold-GET (from remote bucket)
Streaming-Cold-GET               write and transmit cold-GET content back to user in parallel, without _finalizing_ in-cluster object
S3-Use-Path-Style                use older path-style addressing (as opposed to virtual-hosted style), e.g., https://s3.amazonaws.com/BUCKET/KEY
```
