## Init ETL

`ais etl init SPEC_FILE`

Init transform with Pod yaml specification file.

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
XACT_ID=$(ais etl bucket JGHEoo89gg BUCKET1 BUCKET2)
ais wait xaction $XACT_ID # wait until process finishes
```


