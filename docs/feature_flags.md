---
layout: post
title: FEATURE FLAGS
permalink: /docs/feature-flags
redirect_from:
 - /feature_flags.md/
 - /docs/feature_flags.md/
---

## Feature Flags

`Feature flags` is a 64-bit (bit-wise) field in aistore cluster configuration denoting assorted (named) capabilities that can be individually enabled at runtime.

The features themselves are enumerated below. By default, all features are disabled, and the corresponding 64-bit field is set to zero.

## How to see, how to view, how to change

```console
$ ais config cluster features <TAB-TAB>

Enforce-IntraCluster-Access           Provide-S3-API-via-Root               Dont-Allow-Passing-FQN-to-ETL
Do-not-HEAD-Remote-Bucket             Fsync-PUT                             Ignore-LimitedCoexistence-Conflicts
Skip-Loading-VersionChecksum-MD       LZ4-Block-1MB                         Do-not-Auto-Detect-FileShare
LZ4-Frame-Checksum                    Disable-Fast-Cold-GET                 none
```

For example:

```console
$ ais config cluster features Provide-S3-API-via-Root Skip-Loading-VersionChecksum-MD Ignore-LimitedCoexistence-Conflicts
PROPERTY         VALUE
features         Provide-S3-API-via-Root,Ignore-LimitedCoexistence-Conflicts,Skip-Loading-VersionChecksum-MD

Cluster config updated
```

> **Tip**: to select multiple features, type the first letter (or the few first letters) of the one you select, press `<TAB-TAB>` to complete, and then press `<TAB-TAB>` again to keep going, if need be.

To view the current (configured) setting, type the same command and hit `Enter`:

```console
$ ais config cluster features
PROPERTY         VALUE
features         Provide-S3-API-via-Root,Ignore-LimitedCoexistence-Conflicts,Skip-Loading-VersionChecksum-MD
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

## Names and comments

| name | comment |
| ---- | ------- |
| `Enforce-IntraCluster-Access` | when enabled, aistore targets will make sure _not_ to execute direct (ie., not redirected) API calls |
| `Provide-S3-API-via-Root` | handle S3 requests via `aistore-hostname/` (whereby the default: `aistore-hostname/s3`) |
| `Dont-Allow-Passing-FQN-to-ETL` |  do not allow passing fully-qualified name of a locally stored object to (local) ETL containers |
| `Fsync-PUT` | PUT and cold-GET: commit (or sync) the object payload to stable storage |
| `Ignore-LimitedCoexistence-Conflicts` | run in presence of "limited coexistence" type conflicts |
| `Skip-Loading-VersionChecksum-MD` | skip loading existing object's metadata, Version and Checksum (VC) in particular |
| `LZ4-Block-1MB` | .tar.lz4 format, lz4 compression: maximum uncompressed block size=1MB (default: 256K) |
| `LZ4-Frame-Checksum` | checksum lz4 frames |
| `Do-not-Auto-Detect-FileShare` | do not auto-detect file share (NFS, SMB) when _promoting_ shared files to AIS |
| `Disable-Fast-Cold-GET` | use regular datapath to execute cold-GET operations |
