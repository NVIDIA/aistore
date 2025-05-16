Commands for special use cases (e.g. scripting) and *advanced* usage scenarios, whereby a certain level of understanding of possible consequences is assumed (and required).

## Table of Contents
- [`ais advanced`](#ais-advanced)
- [Manual Resilvering](#manual-resilvering)
- [Preload bucket](#preload-bucket)
- [Remove node from Smap](#remove-node-from-smap)
- [Rotate logs: individual nodes or entire cluster](#rotate-logs-individual-nodes-or-entire-cluster)
- [Disable/Enable cloud backend at runtime](#disableenable-cloud-backend-at-runtime)

## `ais advanced`

```console
$ ais advanced --help

NAME:
   ais advanced - Special commands intended for development and advanced usage

USAGE:
   ais advanced command [arguments...]  [command options]

COMMANDS:
   resilver          Resilver user data on a given target (or all targets in the cluster); entails:
                     - fix data redundancy with respect to bucket configuration;
                     - remove migrated objects and old/obsolete workfiles.
   preload           Preload object metadata into in-memory cache
   remove-from-smap  Immediately remove node from cluster map (beware: potential data loss!)
   random-node       Print random node ID (by default, ID of a randomly selected target)
   random-mountpath  Print a random mountpath from a given target
   rotate-logs       Rotate aistore logs
   enable-backend    (Re)enable cloud backend (see also: 'ais config cluster backend')
   disable-backend   Disable cloud backend (see also: 'ais config cluster backend')

OPTIONS:
   --help, -h  Show help
```

## Manual Resilvering

`ais advanced resilver [TARGET_ID]`

Start resilvering objects across all drives on one or all targets.
If `TARGET_ID` is specified, only that node will be resilvered. Otherwise, all targets will be resilvered.

### Examples

```console
$ ais advanced resilver # all targets will be resilvered
Started resilver "NGxmOthtE", use 'ais show job xaction NGxmOthtE' to monitor the progress

$ ais advanced resilver BUQOt8086  # resilver a single node
Started resilver "NGxmOthtE", use 'ais show job xaction NGxmOthtE' to monitor the progress
```

## Preload bucket

`ais advanced preload BUCKET`

Preload objects metadata into in-memory cache.

```console
$ ais advanced preload ais://bucket
```

## Remove node from Smap

`ais advanced remove-from-smap NODE_ID`

Immediately remove node from the cluster map (a.k.a. Smap).

Beware! When the node in question is ais target, the operation may (and likely will) result in a data loss that cannot be undone. Use decommission and start/stop maintenance operations to perform graceful removal.

Any attempt to remove from the cluster map `primary` - ais gateway that currently acts as the primary (aka leader) - will fail.

### Examples

```console
$ ais show cluster proxy
PROXY            MEM USED %      MEM AVAIL       UPTIME
BcnQp8083        0.17%           31.12GiB        6m50s
xVMNp8081        0.16%           31.12GiB        6m50s
MvwQp8080[P]     0.18%           31.12GiB        6m40s
NnPLp8082        0.16%           31.12GiB        6m50s

$ ais advanced remove-from-smap MvwQp8080
Node MvwQp 8080 is primary: cannot remove

$ ais advanced remove-from-smap p[xVMNp8081]
```

And the result:

```console
$ ais show cluster proxy
PROXY            MEM USED %      MEM AVAIL       UPTIME
BcnQp8083        0.16%           31.12GiB        8m
NnPLp8082        0.16%           31.12GiB        8m
MvwQp8080[P]     0.19%           31.12GiB        7m50s
```

## Rotate logs: individual nodes or entire cluster

Usage: `ais advanced rotate-logs [NODE_ID]`

Example:

```console
$ ais show log t[kOktEWrTg]

Started up at 2023/11/07 18:06:22, host u2204, go1.21.1 for linux/amd64
W 18:06:22.930488 config:1713 load initial global config "/root/.ais1/ais.json"
...
...
```

Now, let's go ahead and rotate:

```console
$ ais advanced rotate-logs t[kOktEWrTg]
t[kOktEWrTg]: rotated logs

$ ais show log t[kOktEWrTg]
Rotated at 2023/11/07 18:07:31, host u2204, go1.21.1 for linux/amd64
Node t[kOktEWrTg], Version 3.21.1.69a90d64b, build time 2023-11-07T18:06:19-0500, debug false, CPUs(16, runtime=16)
...
```

## Disable/Enable cloud backend at runtime

AIStore build supports conditional linkage of the supported remote backends: [S3, GCS, Azure](https://github.com/NVIDIA/aistore/blob/main/docs/images/cluster-block-v3.26.png).

> For the most recently updated list, please see [3rd party Backend providers](/docs/providers.md).

To access remote data (and store it in-cluster), AIStore utilizes the respective provider's SDK.

> For Amazon S3, that would be `aws-sdk-go-v2`, for Azure - `azure-storage-blob-go`, and so on. Each SDK can be **conditionally linked** into `aisnode` executable - the decision to link or not to link is made prior to deployment.

### What if

But what if there's a need to disconnect a given linked-in remote backend at runtime, maybe temporarily?

This capability is now supported, and will be included in v3.24 release. And the easiest way to explain how it works is to show some examples.

### Examples

**1)** say, there's a cloud bucket with 4 objects:

```console
$ ais ls s3://test-bucket
NAME     SIZE            CACHED
111      15.97KiB        yes
222      15.97KiB        yes
333      15.97KiB        no
444      15.97KiB        no
```

Note that only 2 objects out of 4 are in-cluster.

**2)** disable s3 backend:

```console
$ ais advanced disable-backend <TAB-TAB>
gcp     aws     azure

$ ais advanced disable-backend aws
cluster: disabled aws backend
```

**3)** observe "offline" error when trying to list the bucket:

```console
$ ais ls s3://test-bucket
Error: ErrRemoteBucketOffline: bucket "s3://test-bucket" is currently unreachable
```

**4)** but (!) all in-cluster objects can still be listed:

```console
$ ais ls s3://test-bucket --cached
NAME     SIZE
111      15.97KiB
222      15.97KiB
```

**5)** and read:

```console
$ ais get s3://test-bucket/111 /dev/null
GET (and discard) 111 from s3://test-bucket (15.97KiB)
```

**6)** expectedly, remote objects are not accessible:

```console
$ ais get s3://test-bucket/333 /dev/null
Error: object "s3://test-bucket/333" does not exist
```

**7)** let's now reconnect s3:

```console
$ ais advanced enable-backend aws
cluster: enabled aws backend
```

**8)** finally, observe that both in-cluster and remote content is now again available:

```console
$ ais ls s3://test-bucket
NAME     SIZE            CACHED
111      15.97KiB        yes
222      15.97KiB        yes
333      15.97KiB        no
444      15.97KiB        no

$ ais get s3://test-bucket/333 /dev/null
GET (and discard) 333 from s3://test-bucket (15.97KiB)
```
