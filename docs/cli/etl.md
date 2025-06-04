# CLI Reference for ETLs

This section documents ETL management operations with `ais etl`. But first, note:

> As with [global rebalance](/docs/rebalance.md), [dSort](/docs/dsort.md), and [download](/docs/download.md), all ETL management commands can also be executed via `ais job` and `ais show`—the commands that, by definition, support all AIS *xactions*, including AIS-ETL.

For background on AIS-ETL, getting started, working examples, and tutorials, please refer to:

- [ETL documentation](/docs/etl.md)

## Table of Contents

- [Commands](#commands)
- [Initialize ETL with a specification file](#init-etl-with-spec)
- [Initialize ETL with code](#init-etl-with-code)
- [List all ETLs](#list-etls)
- [View ETL details](#view-etl-details)
- [View ETL logs](#view-etl-logs)
- [Stop ETL](#stop-etl)
- [Start ETL](#start-etl)
- [Transform an object on-the-fly](#transform-object-on-the-fly-with-given-etl)
- [Transform an entire bucket](#transform-a-bucket-offline-with-the-given-etl)

---

## Commands

Top-level ETL commands include `init`, `stop`, `show`, and more:

```console
$ ais etl --help
NAME:
   ais etl - Execute custom transformations on objects

USAGE:
   ais etl command [arguments...]  [command options]

COMMANDS:
   init       Start ETL job: 'spec' job (requires pod yaml specification) or 'code' job (with transforming function or script in a local file)
   show       Show ETL(s)
   view-logs  View ETL logs
   start      Start ETL
   stop       Stop ETL
   rm         Remove ETL
   object     Transform an object
   bucket     Transform entire bucket or selected objects (to select, use '--list', '--template', or '--prefix')

OPTIONS:
   --help, -h  Show help
```

Additionally, use `--help` to display any specific command.

## Init ETL with a specification file

```sh
ais etl init spec --from-file=SPEC_FILE [--name=ETL_NAME] [--comm-type=COMMUNICATION_TYPE] [--arg-type=ARGUMENT_TYPE] [--init-timeout=TIMEOUT] [--obj-timeout=TIMEOUT]
```

Initializes an ETL from a Pod YAML specification file. The `--name` parameter assigns a unique name to the ETL. See [ETL name specifications](/docs/etl.md#etl-name-specifications) for valid names.

### Example

Initialize an ETL that computes the MD5 hash of an object.

#### Option 1: ETL Specification File

```sh
$ cat spec.yaml
name: transformer-md5
runtime:
  image: aistore/transformer_md5:latest
  command: ["uvicorn", "fastapi_server:fastapi_app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4", "--no-access-log"]
communication: hpull://

$ ais etl init spec --from-file=spec.yaml
```

#### Option 2: Pod Specification File

```sh
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
          containerPort: 8000
      command: ["uvicorn", "fastapi_server:fastapi_app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4", "--no-access-log"]

$ ais etl init spec --from-file=spec.yaml --name=transformer-md5 --comm-type=hpull:// --wait-timeout=1m
transformer-md5
```

---

## Init ETL with code

```sh
ais etl init code --name=ETL_NAME --from-file=CODE_FILE --runtime=RUNTIME [--chunk-size=NUM_OF_BYTES] [--transform=TRANSFORM_FUNC] [--before=BEFORE_FUNC] [--after=AFTER_FUNC] [--deps-file=DEPS_FILE] [--comm-type=COMMUNICATION_TYPE] [--wait-timeout=TIMEOUT] [--arg-type=ARGUMENT_TYPE]
```

This initializes an ETL from a provided `CODE_FILE` that contains:

- `transform(input_bytes)`: The main transformation function.
- `before(context)`: An optional pre-processing function.
- `after(context)`: An optional post-processing function.

The `--name` parameter assigns a unique name to the ETL (see [ETL name specifications](/docs/etl.md#etl-name-specifications)).

**Note:**  
- The default value for `--transform` is `"transform"`.
- Available runtimes are listed [here](/docs/etl.md#runtimes).

### Example

Initialize an ETL that computes the MD5 hash of an object.

```sh
$ cat code.py
import hashlib

def transform(input_bytes):
    md5 = hashlib.md5()
    md5.update(input_bytes)
    return md5.hexdigest().encode()

$ ais etl init code --from-file=code.py --runtime=python3.11v2 --name=transformer-md5 --comm-type hpull

transformer-md5
```

With `before(context)` and `after(context)` functions using streaming (`CHUNK_SIZE` > 0):

```sh
$ cat code.py
import hashlib

def before(context):
    context["before"] = hashlib.md5()
    return context

def transform(input_bytes, context):
    context["before"].update(input_bytes)

def after(context):
    return context["before"].hexdigest().encode()

$ ais etl init code --name=etl-md5 --from-file=code.py --runtime=python3.11v2 --chunk-size=32768 --before=before --after=after --comm-type hpull
```

---

## List ETLs

```sh
ais etl show
```
or equivalently:
```sh
ais job show etl
```
Lists all available ETLs.

---

## View ETL details

```sh
ais etl show details <ETL_NAME>
```

Displays details about a specific ETL, including:

- **ETL Name**
- **Communication Type**
- **Specification or Code**
- **Argument Type**  

---

## View ETL errors

```sh
ais etl show errors <ETL_NAME>

OBJECT           ECODE   ERROR
non-exist-obj    0       stat /ais/nvme9n1/@ais/nnn/%ob/non-exist-obj: no such file or directory
...
```

Shows errors encountered during ETL processing of objects.

## View ETL Logs

```sh
ais etl view-logs ETL_NAME [TARGET_ID]
```

Outputs logs for the given ETL. An optional `TARGET_ID` can be specified to retrieve logs from a particular target node.

---

## Stop ETL

```sh
ais etl stop ETL_NAME
```

Stops the specified ETL.

---

## Start ETL

```sh
ais etl start ETL_NAME
```

Starts the specified ETL.

---

## Transform an object on-the-fly with a given ETL

```sh
ais etl object ETL_NAME BUCKET/OBJECT_NAME OUTPUT
```

### Examples

#### Transform object to STDOUT
Compute the MD5 hash of `shards/shard-0.tar` and print it.

```sh
$ ais etl object transformer-md5 ais://shards/shard-0.tar -
393c6706efb128fbc442d3f7d084a426
```

#### Transform object and save to file
```sh
$ ais etl object transformer-md5 ais://shards/shard-0.tar output.txt
$ cat output.txt
393c6706efb128fbc442d3f7d084a426
```

#### Transform object with arguments
Compute the hash of `shards/shard-0.tar` with argument as seed.

**Note:**

- Arguments are currently supported only in the init spec.
- The provided argument value is passed as the `etl_args` query parameter in the request.
- The transformer server is responsible for receiving and processing the argument from the `etl_args` query parameter.

```sh
# init the `hash-with-args` example transformer
# see https://github.com/NVIDIA/ais-etl/blob/main/transformers/hash_with_args/pod.yaml

$ ais etl object transformer-hash-with-args ais://shards/shard-0.tar - --args=123
4af87d32ee1fb306
```

---

## Transform a bucket offline with the given ETL

```sh
ais etl bucket ETL_NAME SRC_BUCKET DST_BUCKET
```

Transforms all or selected objects and places them in another bucket.

### Available Flags

| Flag | Description |
| --- | --- |
| `--list` | Comma-separated list of object names (e.g., 'obj1,obj2'). |
| `--template` | Template for matching object names (e.g., 'obj-{000..100}.tar'). |
| `--ext` | Mapping for extension transformation (e.g., `{jpg:txt}`). |
| `--prefix` | Prefix for transformed objects. |
| `--wait` | Wait for operation to finish. |
| `--requests-timeout` | Timeout for a single object transformation. |
| `--dry-run` | Show transformation results without applying changes. |
| `--num-workers` | Number of concurrent workers. |

### Examples

#### Transform an entire bucket
```sh
$ ais etl bucket transformer-md5 ais://src_bucket ais://dst_bucket
$ ais wait xaction <XACTION_ID>
```

#### Transform selected objects
```sh
$ ais etl bucket transformer-md5 ais://src_bucket ais://dst_bucket --template "shard-{10..12}.tar"
```

#### Transform bucket with extension mapping
```sh
$ ais etl bucket transformer-md5 ais://src_bucket ais://dst_bucket --ext="{in1:out1, in2:out2}" --prefix="etl-" --wait
```

#### Perform a dry-run
```sh
$ ais etl bucket transformer-md5 ais://src_bucket ais://dst_bucket --dry-run --wait
[DRY RUN] No modifications on the cluster
2 objects (20MiB) would have been put into bucket ais://dst_bucket
```
