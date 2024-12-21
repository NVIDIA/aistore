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
| `Disable-Cold-GET` | do not perform cold GET request when using remote bucket |
| `S3-Reverse-Proxy` | use reverse proxy calls instead of HTTP-redirect for S3 API |
| `S3-Use-Path-Style` | use older path-style addressing (as opposed to virtual-hosted style), e.g., https://s3.amazonaws.com/BUCKET/KEY |
| `Do-not-Delete-When-Rebalancing` | when objects get _rebalanced_ to their proper locations, do not delete their respective _misplaced_ sources |
| `Do-not-Set-Control-Plane-ToS` | intra-cluster control plane: do not set IPv4 ToS field (to low-latency) |
| `Trust-Crypto-Safe-Checksums` | when checking whether objects are identical trust only cryptographically secure checksums |

## Global features

```console
$ ais config cluster features <TAB-TAB>

Enforce-IntraCluster-Access           Fsync-PUT                             Ignore-LimitedCoexistence-Conflicts
Skip-Loading-VersionChecksum-MD       LZ4-Block-1MB                         S3-Presigned-Request
Do-not-Auto-Detect-FileShare          LZ4-Frame-Checksum                    Do-not-Optimize-Listing-Virtual-Dirs
S3-API-via-Root                       Do-not-Allow-Passing-FQN-to-ETL         Disable-Cold-GET
S3-Reverse-Proxy                      none
```

For example:

```console
$ ais config cluster features S3-API-via-Root Skip-Loading-VersionChecksum-MD Ignore-LimitedCoexistence-Conflicts
PROPERTY         VALUE
features         S3-API-via-Root,Ignore-LimitedCoexistence-Conflicts,Skip-Loading-VersionChecksum-MD

Cluster config updated
```

> **Tip**: to select multiple features, type the first letter (or the few first letters) of the one you select, press `<TAB-TAB>` to complete, and then press `<TAB-TAB>` again to keep going, if need be.

To view the current (configured) setting, type the same command and hit `Enter`:

```console
$ ais config cluster features
PROPERTY         VALUE
features         S3-API-via-Root,Ignore-LimitedCoexistence-Conflicts,Skip-Loading-VersionChecksum-MD
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

```console
## 1. show existing bucket-scope features:

$ ais bucket props set ais://nnn features <TAB-TAB>
Skip-Loading-VersionChecksum-MD   Fsync-PUT        S3-Presigned-Request       none

## 2. select and set:

$ ais bucket props set ais://nnn features S3-Presigned-Request
"features" set to: "512" (was: "0")

Bucket props successfully updated.
$ ais bucket props set ais://nnn features
PROPERTY         VALUE
features         512

## 3. reset feature flags back to zero (or 'none'):

$ ais bucket props set ais://nnn features none
"features" set to: "0" (was: "512")

Bucket props successfully updated.
$ ais bucket props set ais://nnn features
PROPERTY         VALUE
features         0
```
