## Init transform

`ais transform init SPEC_FILE`

Init transform with Pod yaml specification file.

### Examples

#### Initialize transformation

Initialize transformation that computes MD5 of the object.

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
      image: quay.io/user/md5_server:v1
      ports:
        - containerPort: 80
      command: ['/code/server.py', '--listen', '0.0.0.0', '--port', '80']
$ ais transform init spec.yaml
JGHEoo89gg
```

## List transforms

`ais transform ls`

Lists all available transformations.

## Stop transform

`ais transform stop TRANSFORM_ID`

Stop transformation with specified id.

## Transform object 

`ais transform object TRANSFORM_ID BUCKET/OBJECT_NAME OUTPUT`

Get object with transformation defined by `TRANSFORM_ID`.

### Examples

#### Transform object to STDOUT

Transforms `shards/shard-0.tar` object with `JGHEoo89gg` (computes MD5 of the object) and print the output to the STDOUT.

```console
$ ais transform object JGHEoo89gg shards/shard-0.tar -
393c6706efb128fbc442d3f7d084a426
```

#### Transform object to output file

Transforms `shards/shard-0.tar` object with `JGHEoo89gg` transformation (computes MD5 of the object) and save output to `output.txt` file.

```console
$ ais transform object JGHEoo89gg shards/shard-0.tar output.txt
$ cat output.txt
393c6706efb128fbc442d3f7d084a426
```
