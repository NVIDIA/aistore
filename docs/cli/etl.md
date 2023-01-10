---
layout: post
title: ETL
permalink: /docs/cli/etl
redirect_from:
 - /cli/etl.md/
 - /docs/cli/etl.md/
---

# CLI Reference for ETLs

This section documents ETL management operations with `ais etl`. But first, note:

> As with [global rebalance](/docs/rebalance.md), [dSort](/docs/dsort.md), and [download](/docs/download.md), all ETL management commands can be also executed via `ais job` and `ais show` - the commands that, by definition, support all AIS *xactions*, including AIS-ETL

For background on AIS-ETL, getting-started steps, working examples, and tutorials, please refer to:

* [ETL documentation](/docs/etl.md)

## Table of Contents

- [Init ETL with spec](#init-etl-with-spec)
- [Init ELT with code](#init-etl-with-code)
- [List ETLs](#list-etls)
- [View ETL Logs](#view-etl-logs)
- [Stop ETL](#stop-etl)
- [Transform object on-the-fly with given ETL](#transform-object-on-the-fly-with-given-etl)
- [Transform a bucket offline with the given ETL](#transform-a-bucket-offline-with-the-given-etl)

## Init ETL with spec

`ais etl init spec --from-file=SPEC_FILE --name=UNIQUE_ID [--comm-type=COMMUNICATION_TYPE] [--wait-timeout=TIMEOUT]` or `ais job start etl init`

Init ETL with Pod YAML specification file. The `--name` CLI flag is used as a unique ID for ETL (ref: [here](/docs/etl.md#etl-name-specifications) for information on valid ETL name).

### Example

Initialize ETL that computes MD5 of the object.

```console
$ cat spec.yaml
apiVersion: v1
kind: Pod
metadata:
  name: transformer-md5
spec:
  containers:
    - name: server
      image: aistore/transformer_md5:latest
      ports:
        - name: default
          containerPort: 80
      command: ['/code/server.py', '--listen', '0.0.0.0', '--port', '80']
$ ais etl init spec --from-file=spec.yaml --name=transformer-md5 --comm-type=hpull:// --wait-timeout=1m
transformer-md5
```

## Init ETL with code

`ais etl init code --name=UNIQUE_ID --from-file=CODE_FILE --runtime=RUNTIME [--chunk-size=NUM_OF_BYTES] [--transform=TRANSFORM_FUNC] [--before=BEFORE_FUNC] [--after=AFTER_FUNC] [--deps-file=DEPS_FILE] [--comm-type=COMMUNICATION_TYPE] [--wait-timeout=TIMEOUT]`

Initializes ETL from provided `CODE_FILE` that contains a transformation function named `transform(input_bytes)` or `transform(input_bytes, context)`, an optional function executed prior to the transform function named `before(context)` which is supposed to initialize all the variables needed for the `transform(input_bytes, context)` and optional post transform function named `after(context)` which consolidates the results and returns to the user the transformed `output_bytes`.

The `--name` parameter is used to assign a user defined unique ID (ref: [here](/docs/etl.md#etl-name-specifications) for information on valid ETL name).

Based on the communication type used, there are mutiple ways you can initialize the `transform(input_bytes, context)`, `before(context)` and `after(context)` functions. Check [ETL Init Code Docs](docs/etl.md#init-code-request) for more info.

All available runtimes are listed [here](/docs/etl.md#runtimes).

Note:
- Default value of --transform is "transform".

### Example

Initialize ETL with code that computes MD5 of the object.

```console
$ cat code.py
import hashlib

def transform(input_bytes):
    md5 = hashlib.md5()
    md5.update(input_bytes)
    return md5.hexdigest().encode()

$ ais etl init code --from-file=code.py --runtime=python3.11v2 --name=transformer-md5
transformer-md5
```

With `before(context)` and `after(context)` function with streaming (`CHUNK_SIZE` > 0):
```console
$ cat code.py
import hashlib
def before(context):
    context["before"] = hashlib.md5()
    return context

def transform(input_bytes, context):
    context["before"].update(input_bytes)

def after(context):
    return context["before"].hexdigest().encode()

$ ais etl init code --name=etl-md5 --from-file=code.py --runtime=python3.11v2 --chunk-size=32768 --before=before --after=after
```

## List ETLs

`ais etl show` or, same, `ais job show etl`

Lists all available ETLs.

## View ETL Logs

`ais etl logs ETL_ID [TARGET_ID]`

Output logs produced by given ETL.
It is possible to pass an additional parameter to specify a particular `TARGET_ID` from which the logs must be retrieved.

## Stop ETL

`ais etl stop ETL_ID` or, same, `ais job stop etl`

Stop ETL with the specified id.


## Start ETL

`ais etl start ETL_ID` or, same, `ais job start etl`

Start ETL with the specified id.


## Transform object on-the-fly with given ETL

`ais etl object ETL_ID BUCKET/OBJECT_NAME OUTPUT`

Get object with ETL defined by `ETL_ID`.

### Examples

#### Transform object to STDOUT

Does ETL on `shards/shard-0.tar` object with `transformer-md5` ETL (computes MD5 of the object) and print the output to the STDOUT.

```console
$ ais etl object transformer-md5 ais://shards/shard-0.tar -
393c6706efb128fbc442d3f7d084a426
```

#### Transform object to output file

Do ETL on the `shards/shard-0.tar` object with `transformer-md5` ETL (computes MD5 of the object) and save the output to the `output.txt` file.

```console
$ ais etl object transformer-md5 ais://shards/shard-0.tar output.txt
$ cat output.txt
393c6706efb128fbc442d3f7d084a426
```

## Transform a bucket offline with the given ETL

`ais etl bucket ETL_ID SRC_BUCKET DST_BUCKET`

Transform all or selected objects and put them into another bucket.

| Flag | Type | Description |
| --- | --- | --- |
| `--list` | `string` | Comma-separated list of object names, e.g., 'obj1,obj2' |
| `--template` | `string` | Template for matching object names, e.g, 'obj-{000..100}.tar' |
| `--ext` | `string` | Mapping from old to new extensions of transformed objects, e.g. {jpg:txt}, "{ in1 : out1, in2 : out2 }"|
| `--prefix` | `string` | Prefix added to every new object name |
| `--wait` | `bool` | Wait until operation is finished |
| `--requests-timeout` | `duration` | Timeout for a single object transformation |
| `--dry-run` | `bool` | Don't actually transform the bucket, only display what would happen |

Flags `--list` and `--template` are mutually exclusive. If neither of them is set, the command transforms the whole bucket.

### Examples

#### Transform bucket with ETL

Transform every object from `src_bucket` with ETL and put new objects to `dst_bucket`.

```console
$ ais etl bucket transformer-md5 ais://src_bucket ais://dst_bucket
MMi9l8Z11
$ ais job wait xaction MMi9l8Z11
```

#### Transform bucket with ETL

The same as above, but wait for the ETL bucket to finish.

```console
$ ais etl bucket transformer-md5 ais://src_bucket ais://dst_bucket --wait
```

#### Transform selected objects in bucket with ETL

Transform objects `shard-10.tar`, `shard-11.tar`, and `shard-12.tar` from `src_bucket` with ETL and put new objects to `dst_bucket`.

```console
$ ais etl bucket transformer-md5 ais://src_bucket ais://dst_bucket --template "shard-{10..12}.tar"
```

#### Transform bucket with ETL and additional parameters

The same as above, but objects will have `etl-` prefix and objects with extension `.in1` will have `.out1` extension, objects with extension `.in2` will have `.out2` extension.

```console
$ ais bucket ls ais://src_bucket --props=name
NAME
obj1.in1
obj2.in2
(...)
$ ais etl bucket transformer-md5 ais://src_bucket ais://dst_bucket --ext="{in1:out1, in2:out2}" --prefix="etl-" --wait
$ ais bucket ls ais://dst_bucket --props=name
NAME
etl-obj1.out1
etl-obj2.out2
(...)
```

#### Transform bucket with ETL but with dry-run

Dry-run won't perform any actions but rather just show what would be transformed if we actually transformed a bucket.
This is useful for preparing the actual run.

```console
$ ais bucket ls ais://src_bucket --props=name,size
NAME        SIZE
obj1.in1    10MiB
obj2.in2    10MiB
(...)
$ ais etl bucket transformer-md5 ais://src_bucket ais://dst_bucket --dry-run --wait
[DRY RUN] No modifications on the cluster
2 objects (20MiB) would have been put into bucket ais://dst_bucket
```
