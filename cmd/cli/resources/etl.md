## Init ETL

`ais etl init SPEC_FILE`

Init ETL with Pod yaml specification file.

Note: as of AIStore v3.2 only one ETL at a time is supported.

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

`ais etl build --from-file=CODE_FILE --runtime=RUNTIME [--deps-file=DEPS_FILE]`

Builds and initializes ETL from provided `CODE_FILE` that contains transformation function named `transform`.
The `transform` function must take `input_bytes` (raw bytes of the objects) as parameters and return transformed object (also raw bytes which will be saved into new object).

> The ETL simply crashes if the function panics or throws exception.
> Therefore, error handling should be done inside the function.

Note: as of AIStore v3.2 only `python3` and `python2` runtimes are supported.

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

## Stop ETL

`ais etl stop ETL_ID`

Stop ETL with specified id.

## Transform object on-the-fly with given ETL

`ais etl object ETL_ID BUCKET/OBJECT_NAME OUTPUT`

Get object with ETL defined by `ETL_ID`.

### Examples

#### Transform object to STDOUT

Does ETL on `shards/shard-0.tar` object with `JGHEoo89gg` ETL (computes MD5 of the object) and print the output to the STDOUT.

```console
$ ais etl object JGHEoo89gg shards/shard-0.tar -
393c6706efb128fbc442d3f7d084a426
```

#### Transform object to output file

Do ETL on `shards/shard-0.tar` object with `JGHEoo89gg` ETL (computes MD5 of the object) and save output to `output.txt` file.

```console
$ ais etl object JGHEoo89gg shards/shard-0.tar output.txt
$ cat output.txt
393c6706efb128fbc442d3f7d084a426
```

## Transform the whole bucket offline with given ETL

`ais etl bucket ETL_ID BUCKET_FROM BUCKET_TO`

### Examples

#### Transform ever object from BUCKET1 with ETL and put new objects to BUCKET2

```console
$ XACT_ID=$(ais etl bucket JGHEoo89gg BUCKET1 BUCKET2)
$ ais wait xaction $XACT_ID # wait until offline ETL finishes
```

#### The same as above, but objects will have `etl-` prefix and objects with extension `.in1` will have `.out1` extension, objects with extension `.in2` will have `.out2` extension.

```console
$ ais ls bucket BUCKET1 --props=name
NAME
obj1.in1
obj2.in2
(...)
$ XACT_ID=$(ais etl bucket JGHEoo89gg BUCKET1 BUCKET2 --ext="{'in1':'out1', 'in2':'out2'}" --prefix="etl-")
$ ais wait xaction $XACT_ID # wait until offline ETL finishes
$ ais ls bucket BUCKET2 --props=name
NAME
etl-obj1.out1
etl-obj2.out2
(...)
```

#### The same as above, but don't actually perform any actions. Show what would have happened.

```console
$ ais ls bucket BUCKET1 --props=name,size
NAME        SIZE
obj1.in1    10MiB
obj2.in2    10MiB
(...)
$ ais etl bucket JGHEoo89gg BUCKET1 BUCKET2 --dry-run
[DRY RUN] No modifications on the cluster
2 objects (20MiB) would have been put into bucket BUCKET2
```




