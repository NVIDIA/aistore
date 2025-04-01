This guide demonstrates three ways to evict remote bucket content from AIStore using the `ais evict` command.

## Overview

You can:
1. Evict an **entire** remote bucket (data + metadata).
2. Evict **only the data**, retaining bucket metadata.
3. Evict a **subset of objects**, using a prefix, template, or list.

---

## 1. Evict the Entire Remote Bucket (Remove Data and Metadata)

To **completely remove** a remote bucket from AIStore—including its metadata—run `ais evict` **without** `--keep-md`.

```console
$ ais evict s3://abc

Evicted bucket s3://abc from aistore
```

This operation removes all cached content **and** the bucket’s entry from AIStore’s bucket metadata (BMD).

Check eviction job status (it usually completes in a split second, so use `--all` to show finished jobs):

```console
$ ais show job evict --all

evict-remote-bucket[rmmd-XrjAI7JdC]
NODE             ID              KIND                BUCKET       START        END          STATE
nXItghtr         rmmd-XrjAI7JdC  evict-remote-bck    s3://abc     10:20:43     10:20:43     Finished
veFtyrfq         rmmd-XrjAI7JdC  evict-remote-bck    s3://abc     10:20:43     10:20:43     Finished
vugtciop         rmmd-XrjAI7JdC  evict-remote-bck    s3://abc     10:20:43     10:20:43     Finished
```

---

## 2. Evict Only Bucket Data (Preserve Metadata)

To remove only the **in-cluster objects** while **retaining** the bucket metadata, use `--keep-md` (or `-k`):

```console
$ ais evict s3://abc --keep-md

Evicted s3://abc contents from aistore: the bucket is now empty
```

This is useful for reclaiming space without losing bucket properties.

Confirm the bucket is still in-cluster:

```console
$ ais ls

NAME       PRESENT
s3://abc   yes
```

Check eviction job status (use `-all` to show running and already finished jobs):

```console
$ ais show job evict --all

evict-remote-bucket[kpmd-G7afG87fBC]
NODE          ID                   KIND                 BUCKET        START        END          STATE
nXItghtr      kpmd-G7afG87fBC      evict-remote-bck     s3://abc      10:22:05     10:22:05     Finished
veFtyrfq      kpmd-G7afG87fBC      evict-remote-bck     s3://abc      10:22:05     10:22:05     Finished
vugtciop      kpmd-G7afG87fBC      evict-remote-bck     s3://abc      10:22:05     10:22:05     Finished
```

---

## 3. Evict a Specific Prefix, Template, or List

You can evict subsets of objects by specifying:
- A prefix in the object path
- A `--template` pattern (e.g., brace expansions)
- A `--list` of object names

### Example: Evict by Prefix

Assume we’ve prefetched objects under the `copy` prefix:

```console
$ ais prefetch s3://abc/copy

prefetch-objects[acs9_7JB0]: prefetch "copy" from s3://abc. To monitor the progress, run 'ais show job acs9_7JB0'
```

Verify data was cached:

```console
$ ais bucket summary

NAME     OBJECTS (cached, remote)     OBJECT SIZES (min, avg, max)     TOTAL OBJECT SIZE (cached, remote)   USAGE(%)
-        200 0                        0B         1.00KiB    1.00KiB    200.00KiB 0B                         0%
```

Notice `200` "cached" (ie., in-cluster) objects. Now evict just that prefix:

```console
$ ais evict s3://abc/copy

evict-objects[HRdka7fd0]: evict "copy" from s3://abc. To monitor the progress, run 'ais show job HRdka7fd0'
```

Or alternatively, use an explicit prefix:

```console
$ ais evict s3://abc --prefix copy
```

> You can also use brace-expansion syntax, e.g. `ais evict "s3://abc/shard-{0000..9999}.tar.lz4`; see `--help` for details.

Check progress and notice that each target reports eviction statistics—object counts and their sizes.

> This is because prefix-, template-, or list-based eviction requires AIStore to visit each matching object individually.

```console
$ ais show job evict --all
```

Example output:
```console
evict-objects[HRdka7fd0] (run options: prefix:copy)
NODE             ID              KIND                    BUCKET          OBJECTS     BYTES       START       END         STATE
nXItghtr         HRdka7fd0       evict-listrange         s3://ais-aa     69          69.00KiB    10:24:09    10:24:10    Finished
veFtyrfq         HRdka7fd0       evict-listrange         s3://ais-aa     61          61.00KiB    10:24:09    10:24:10    Finished
vugtciop         HRdka7fd0       evict-listrange         s3://ais-aa     70          70.00KiB    10:24:09    10:24:10    Finished
```

> In this particular case, 3-node cluster reported `(69+61+70) = 200` evictions, consistent with `ais bucket summary` above.

---

## Summary

| Use Case                                 | Command Example                                         | Keeps Bucket Metadata? |
|------------------------------------------|----------------------------------------------------------|------------------|
| Fully evict bucket (data + metadata)     | `ais evict BUCKET`                                 | ❌ No            |
| Evict data only, retain bucket mount     | `ais evict BUCKET --keep-md` or `--k`              | ✅ Yes           |
| Evict subset by prefix/template/list     | `ais evict BUCKET/prefix`<br>`--prefix`, `--template`, `--list` | ✅ Yes |

Use `ais show job --all` to monitor and verify jobs.
