# CLI Reference for ETLs
This section lists ETL management operations the AIS CLI, with `ais etl`.
For more information and examples, please refer to [the ETL documentation](/docs/etl.md).

## Table of Contents
- [Initialize ETL](#initialize-etl)
- [Build ETL](#build-etl)
- [List ETLs](#list-etls)
- [View ETL Logs](#view-etl-logs)
- [Stop an ETL](#stop-an-etl)
- [Transform object on-the-fly with given ETL](#transform-object-on-the-fly-with-given-etl)
- [Transform the whole bucket offline with the given ETL](#transform-the-whole-bucket-offline-with-the-given-etl)
## Initialize ETL

`ais etl init SPEC_FILE`

Init ETL with Pod YAML specification file. The `metadata.name` attribute in the specification is used as unique ID for ETL (ref: [here](/docs/etl.md#etl-name-specifications) for information on valid ETL name).

Note: Currently, only one ETL can be run at a time. To run new ETLs, [stop any existing ETL](#stop-etl).

### Example

Initialize ETL that computes MD5 of the object.

```console
$ cat spec.yaml
apiVersion: v1
kind: Pod
metadata:
  name: transformer-md5
  annotations:
    communication_type: hpush://
    wait_timeout: 1m
spec:
  containers:
    - name: server
      image: aistore/transformer_md5:latest
      ports:
        - name: default
          containerPort: 80
      command: ['/code/server.py', '--listen', '0.0.0.0', '--port', '80']
$ ais etl init spec.yaml
JGHEoo89gg
```

## Build ETL

`ais etl build --from-file=CODE_FILE --runtime=RUNTIME [--deps-file=DEPS_FILE] [--name=UNIQUE_ID]`

Builds and initializes ETL from provided `CODE_FILE` that contains a transformation function named `transform`. The `--name` parameter is used to assign a user defined unique ID (ref: [here](/docs/etl.md#etl-name-specifications) for information on valid ETL name).
The `transform` function must take `input_bytes` (raw bytes of the objects) as parameters and return the transformed object (also raw bytes that will be saved into a new object).

Note: Currently, only one ETL can be run at a time. To run new ETLs, [stop any existing ETL](#stop-etl).

> The ETL crashes if the function panics or throws an exception.
> Therefore, error handling should be done inside the function.

Note: currently only `python3` and `python2` runtimes are supported.

### Example

Build ETL that computes MD5 of the object.

```console
$ cat code.py
import hashlib

def transform(input_bytes):
    md5 = hashlib.md5()
    md5.update(input_bytes)
    return md5.hexdigest().encode()
$ ais etl build --from-file=code.py --runtime=python3
JGHEoo89gg
```

## List ETLs

`ais etl ls`

Lists all available ETLs.

## View ETL Logs

`ais etl logs ETL_ID [TARGET_ID]`

Output logs produced by given ETL.
It is possible to pass an additional parameter to specify a particular `TARGET_ID` from which the logs must be retrieved.

## Stop an ETL

`ais etl stop ETL_ID`

Stop ETL with the specified id.

## Transform object on-the-fly with given ETL

`ais etl object ETL_ID BUCKET/OBJECT_NAME OUTPUT`

Get object with ETL defined by `ETL_ID`.

### Examples

#### Transform object to STDOUT

Does ETL on `shards/shard-0.tar` object with `JGHEoo89gg` ETL (computes MD5 of the object) and print the output to the STDOUT.

```console
$ ais etl object JGHEoo89gg ais://shards/shard-0.tar -
393c6706efb128fbc442d3f7d084a426
```

#### Transform object to output file

Do ETL on the `shards/shard-0.tar` object with `JGHEoo89gg` ETL (computes MD5 of the object) and save the output to the `output.txt` file.

```console
$ ais etl object JGHEoo89gg ais://shards/shard-0.tar output.txt
$ cat output.txt
393c6706efb128fbc442d3f7d084a426
```

## Transform the whole bucket offline with the given ETL

`ais etl bucket ETL_ID SRC_BUCKET DST_BUCKET`

### Examples

#### Transform bucket with ETL
 
Transform every object from `src_bucket` with ETL and put new objects to `dst_bucket`.

```console
$ ais etl bucket JGHEoo89gg ais://src_bucket ais://dst_bucket
MMi9l8Z11
$ ais job wait xaction MMi9l8Z11
```

#### Transform bucket with ETL

The same as above, but wait for the ETL bucket to finish.

```console
$ ais etl bucket JGHEoo89gg ais://src_bucket ais://dst_bucket --wait
```

#### Transform bucket with ETL and additional parameters

The same as above, but objects will have `etl-` prefix and objects with extension `.in1` will have `.out1` extension, objects with extension `.in2` will have `.out2` extension.

```console
$ ais bucket ls ais://src_bucket --props=name
NAME
obj1.in1
obj2.in2
(...)
$ ais etl bucket JGHEoo89gg ais://src_bucket ais://dst_bucket --ext="{'in1':'out1', 'in2':'out2'}" --prefix="etl-" --wait
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
$ ais etl bucket JGHEoo89gg ais://src_bucket ais://dst_bucket --dry-run --wait
[DRY RUN] No modifications on the cluster
2 objects (20MiB) would have been put into bucket ais://dst_bucket
```
