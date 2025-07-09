# CLI Reference for ETLs

This section documents ETL management operations with `ais etl`. But first, note:

> As with [global rebalance](/docs/rebalance.md), [dSort](/docs/dsort.md), and [download](/docs/download.md), all ETL management commands can also be executed via `ais job` and `ais show`—the commands that, by definition, support all AIS *xactions*, including AIS-ETL.

For background on AIS-ETL, getting started, working examples, and tutorials, please refer to:

- [ETL documentation](/docs/etl.md)

## Table of Contents

### Getting Started

* [Commands](#commands)

### Initializing an ETL

* [Using a Runtime ETL Specification (Recommended)](#1-using-a-runtime-etl-specification-recommended)
* [Using a Full Kubernetes Pod Spec (Advanced)](#2-using-a-full-kubernetes-pod-spec-advanced)

### ETL Management

* [Listing ETLs](#listing-etls)
* [View ETL Details](#view-etl-details)
* [View ETL Errors](#view-etl-errors)
* [View ETL Logs](#view-etl-logs)

### ETL Lifecycle Operations

* [Stop ETL](#stop-etl)
* [Start ETL](#start-etl)
* [Delete (RM) ETL](#remove-delete-etl)

### Data Transformation

* [Inline Transformation](#inline-transformation)
* [Offline Transformation](#offline-transformation)

---

## Commands

Top-level ETL commands include `init`, `stop`, `show`, and more:

```console
$ ais etl -h
NAME:
   ais etl - Manage and execute custom ETL (Extract, Transform, Load) jobs

USAGE:
   ais etl command [arguments...]  [command options]

COMMANDS:
   init  Initialize ETL using a runtime spec or full Kubernetes Pod spec YAML file (local or remote).
   Examples:
     - 'ais etl init -f my-etl.yaml'                      deploy ETL from a local YAML file;
     - 'ais etl init -f https://example.com/etl.yaml'     deploy ETL from a remote YAML file;
     - 'ais etl init -f multi-etl.yaml'                   deploy multiple ETLs from a single file (separated by '---');
     - 'ais etl init -f spec.yaml --name my-custom-etl'   override ETL name from command line;
     - 'ais etl init -f spec.yaml --comm-type hpull'      override communication type;
     - 'ais etl init -f spec.yaml --object-timeout 30s'   set custom object transformation timeout.
     - 'ais etl init --spec <file|URL>'                   deploy ETL jobs from a local spec file, remote URL, or multi-ETL YAML.
   
Additional Info:
   - You may define multiple ETLs in a single spec file using YAML document separators ('---').
   - CLI flags like '--name' or '--comm-type' can override values in the spec, but not when multiple ETLs are defined.

   show  Show ETL(s).
   Examples:
              - 'ais etl show'                             list all ETL jobs with their status and details;
              - 'ais etl show my-etl'                      show detailed specification for a specific ETL job;
              - 'ais etl show my-etl another-etl'          show detailed specifications for multiple ETL jobs;
              - 'ais etl show errors my-etl'               show transformation errors for inline object transformations;
              - 'ais etl show errors my-etl job-123'       show errors for a specific offline (bucket-to-bucket) transform job.
   view-logs  View ETL logs.
   Examples:
          - 'ais etl view-logs my-etl'                      show logs from all target nodes for the specified ETL;
          - 'ais etl view-logs my-etl target-001'           show logs from a specific target node;
          - 'ais etl view-logs data-converter target-002'   view logs from target-002 for data-converter ETL.
   start  Start ETL.
   Examples:
     - 'ais etl start my-etl'                            start the specified ETL (transitions from stopped to running state);
     - 'ais etl start my-etl another-etl'                start multiple ETL jobs by name;
     - 'ais etl start -f spec.yaml'                      start ETL jobs defined in a local YAML file;
     - 'ais etl start -f https://example.com/etl.yaml'   start ETL jobs defined in a remote YAML file;
     - 'ais etl start -f multi-etl.yaml'                 start all ETL jobs defined in a multi-ETL file;
     - 'ais etl start --spec <file|URL>'                 start ETL jobs from a local spec file, remote URL, or multi-ETL YAML.

   stop  Stop ETL. Also aborts related offline jobs and can be used to terminate ETLs stuck in 'initializing' state.
   Examples:
     - 'ais etl stop my-etl'                              stop the specified ETL (transitions from running to stopped state);
     - 'ais etl stop my-etl another-etl'                  stop multiple ETL jobs by name;
     - 'ais etl stop --all'                               stop all running ETL jobs;
     - 'ais etl stop -f spec.yaml'                        stop ETL jobs defined in a local YAML file;
     - 'ais etl stop -f https://example.com/etl.yaml'     stop ETL jobs defined in a remote YAML file;
     - 'ais etl stop stuck-etl'                           terminate ETL that is stuck in 'initializing' state;
     - 'ais etl stop --spec <file|URL>'                   stop ETL jobs from a local spec file, remote URL, or multi-ETL YAML.

   rm  Remove ETL.
   Examples:
           - 'ais etl rm my-etl'                              remove (delete) the specified ETL;
           - 'ais etl rm my-etl another-etl'                  remove multiple ETL jobs by name;
           - 'ais etl rm --all'                               remove all ETL jobs;
           - 'ais etl rm -f spec.yaml'                        remove ETL jobs defined in a local YAML file;
           - 'ais etl rm -f https://example.com/etl.yaml'     remove ETL jobs defined in a remote YAML file;
           - 'ais etl rm running-etl'q                        remove ETL that is currently running (will be stopped first).
           - 'ais etl rm --spec <file|URL>'                   remove ETL jobs from a local spec file, remote URL, or multi-ETL YAML.
             NOTE: If an ETL is in 'running' state, it will be stopped automatically before removal.
   object  Transform an object.
   Examples:
           - 'ais etl object my-etl ais://src/image.jpg /tmp/output.jpg'                   transform object and save to file;
           - 'ais etl object my-etl ais://src/data.json -'                                 transform and output to stdout;
           - 'ais etl object my-etl ais://src/doc.pdf /dev/null'                           transform and discard output;
           - 'ais etl object my-etl cp ais://src/image.jpg ais://dst/'                     transform and copy to another bucket;
           - 'ais etl object my-etl ais://src/data.xml output.json --args "format=json"'   transform with custom arguments.
   bucket  Transform entire bucket or selected objects (to select, use '--list', '--template', or '--prefix').
   Examples:
     - 'ais etl bucket my-etl ais://src ais://dst'                                       transform all objects from source to destination bucket;
     - 'ais etl bucket my-etl ais://src ais://dst --prefix images/'                      transform objects with prefix 'images/';
     - 'ais etl bucket my-etl ais://src ais://dst --template "shard-{0001..0999}.tar"'   transform objects matching the template;
     - 'ais etl bucket my-etl s3://remote-src ais://dst --all'                           transform all objects including non-cached ones;
     - 'ais etl bucket my-etl ais://src ais://dst --dry-run'                             preview transformation without executing;
     - 'ais etl bucket my-etl ais://src ais://dst --num-workers 8'                       use 8 concurrent workers for transformation;
     - 'ais etl bucket my-etl ais://src ais://dst --prepend processed/'                  add prefix to transformed object names.

OPTIONS:
   --help, -h  Show help
```

Additionally, use `--help` to display any specific command.

## Initializing an ETL

AIStore provides two ways to initialize an ETL using the CLI:

---

### 1. **Using a Runtime ETL Specification (Recommended)**

This method uses a YAML file that defines how your ETL should be initialized and run.

#### Key Fields in the Spec

| Field                | Description                                                                                                                                           | Default      |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| `name`               | Unique name for the ETL. [See naming rules](https://github.com/NVIDIA/aistore/blob/main/docs/etl.md#etl-name-specifications)                          | **Required** |
| `runtime.image`      | Docker image for the ETL container                                                                                                                    | **Required** |
| `runtime.command`    | (Optional) Override the container's default `ENTRYPOINT` with custom command and arguments                                                            | `None`       |
| `communication`      | (Optional) [Communication method](https://github.com/NVIDIA/aistore/blob/main/docs/etl.md#communication-mechanisms) between AIS and the ETL container | `hpush://`   |
| `argument`           | (Optional) Argument passing method: `""` (default) or `"fqn"` (mounts host filesystem)                                                                | `""`         |
| `init_timeout`       | (Optional) Max time to wait for ETL to become ready                                                                                                   | `5m`         |
| `obj_timeout`        | (Optional) Max time to process a single object                                                                                                        | `45s`        |
| `support_direct_put` | (Optional) Enable [direct put optimization](https://github.com/NVIDIA/aistore/blob/main/docs/etl.md#direct-put-optimization) for offline transforms   | `false`      |

#### Sample ETL Spec

```yaml
name: hello-world-etl
runtime:
  image: aistorage/transformer_hello_world:latest
  # Optional: Override the container entrypoint
  # command: ["uvicorn", "fastapi_server:fastapi_app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]

communication: hpush://
argument: fqn
init_timeout: 5m
obj_timeout: 45s
support_direct_put: true
```

#### CLI Usage

```bash
# From a local file
$ ais etl init -f spec.yaml

# From a remote URL
$ ais etl init -f <URL>

# Override values from the spec
$ ais etl init -f <URL> \
  --name=ETL_NAME \
  --comm-type=COMMUNICATION_TYPE \
  --arg-type=ARGUMENT_TYPE \
  --init-timeout=TIMEOUT \
  --obj-timeout=TIMEOUT
```

> **Note:** CLI parameters take precedence over the spec file.

---

### 2. **Using a Full Kubernetes Pod Spec (Advanced)**

Use this option if you need full control over the ETL container’s deployment—such as advanced init containers, health checks, or if you're not using the AIS ETL framework.

#### Example Pod Spec

```yaml
# pod_spec.yaml
apiVersion: v1
kind: Pod
metadata:
  name: etl-echo
  annotations:
    communication_type: "hpush://"
    wait_timeout: "5m"
spec:
  containers:
    - name: server
      image: aistorage/transformer_md5:latest
      ports: [{ name: default, containerPort: 8000 }]
      command: ["uvicorn", "fastapi_server:fastapi_app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4", "--log-level", "info", "--ws-max-size", "17179869184", "--ws-ping-interval", "0", "--ws-ping-timeout", "86400"]
      readinessProbe:
        httpGet: { path: /health, port: default }
```

#### CLI Usage

```bash
# Initialize ETL from a Pod spec
$ ais etl init -f pod_spec.yaml --name transformer-md5
```

### Additional Notes

* You can define **multiple ETLs in a single YAML file** by separating them with the standard YAML document separator `---`.

  **Example:**

  ```yaml
  name: hello-world-etl
  runtime:
    image: aistorage/transformer_hello_world:latest
  ---
  name: md5-etl
  runtime:
    image: aistorage/transformer_md5:latest
  ```

* You may override fields in the spec using CLI flags such as `--name`, `--comm-type`, `--arg-type`, etc.

  **However**, if your YAML file contains multiple ETL definitions, override flags **cannot** be used and will result in an error.

  In such cases, you should either:

  * Remove the override flags and apply the full multi-ETL spec as-is, or
  * Split the YAML file into individual files and initialize each ETL separately:

---

## Listing ETLs

To view all currently initialized ETLs in the AIStore cluster, use either of the following commands:

```bash
ais etl show
```

or the equivalent:

```bash
ais job show etl
```

This will display all available ETLs along with their current status (`initializing`, `running`, `stopped`, etc.).

---

## View ETL Specification

To view detailed information about one or more ETL jobs and their configuration, use:

```bash
ais etl show <ETL_NAME> [<ETL_NAME> ...]
```

This command displays detailed attributes of each ETL, including:

* **ETL Name**
* **Communication Type**
* **Argument Type** (e.g., "" or "fqn"(fully qualified path))
* **Runtime Configuration**
  * Container image
  * Command
  * Environment variables
* **ETL Source** (Full Pod specification, if applicable)

> **Note:** You can also use the alias `ais show etl <ETL_NAME> [<ETL_NAME> ...]` for the same functionality.

---

## View ETL Errors

Use this command to view errors encountered during ETL processing—either during inline transformations or offline (bucket-to-bucket) jobs.

### Inline ETL Errors

To list errors from **inline** object transformations:

```bash
ais etl show errors <ETL_NAME>
```

**Example Output:**

```
OBJECT                 ECODE   ERROR
ais://non-exist-obj    404     object not found
```

---

### Offline ETL (Bucket-to-Bucket) Errors

To list errors from a specific **offline ETL job**, include the job ID:

```bash
ais etl show errors <ETL_NAME> <OFFLINE-JOB-ID>
```

**Example Output:**

```
OBJECT                   ECODE   ERROR
ais://test-src/7         500     ETL error: <your-custom-error>
ais://test-src/8         500     ETL error: <your-custom-error>
ais://test-src/6         500     ETL error: <your-custom-error>
```

Here, `<your-custom-error>` refers to the error raised from within your custom transform function (e.g., in Python).

---

## View ETL Logs

Use the following command to view logs for a specific ETL container:

```bash
ais etl view-logs <ETL_NAME> [TARGET_ID]
```

* `<ETL_NAME>`: Name of the ETL.
* `[TARGET_ID]` (optional): Retrieve logs from a specific target node. If omitted, logs from all targets will be aggregated.

---

## Stop ETL

Stops a running ETL and tears down its underlying Kubernetes resources.

```bash
ais etl stop <ETL_NAME> [<ETL_NAME> ...]
```

* Frees up system resources without deleting the ETL definition.
* ETL can be restarted later without reinitialization.

You can also stop ETLs from a specification file:

```bash
ais etl stop -f <spec-file.yaml>   # Local file with one or more ETL specs
ais etl stop -f <URL>              # Remote spec file over HTTP(S)
```

* Supports multi-ETL YAML files separated by `---`.

> More info [ETL Pod Lifecycle](https://github.com/NVIDIA/aistore/blob/main/docs/etl.md#etl-pod-lifecycle)
---

## Start ETL

Restarts a previously stopped ETL by recreating its associated containers on each target.

```bash
ais etl start <ETL_NAME> [<ETL_NAME> ...]
```

* Useful when resuming work after a manual or error-triggered stop.
* Retains all original configuration and transformation logic.

You can also start ETLs from a specification file:

```bash
ais etl start -f <spec-file.yaml>   # Local file with one or more ETL specs
ais etl start -f <URL>              # Remote spec file over HTTP(S)
```

* Supports multi-ETL YAML files separated by `---`.

> More info [ETL Pod Lifecycle](https://github.com/NVIDIA/aistore/blob/main/docs/etl.md#etl-pod-lifecycle)

---

## Remove (Delete) ETL

Remove (delete) ETL jobs.

```bash
ais etl rm <ETL_NAME> [<ETL_NAME> ...]
```

* Useful when resuming work after a manual or error-triggered stop.
* Retains all original configuration and transformation logic.

You can also remove ETLs from a specification file:

```bash
ais etl rm -f <spec-file.yaml>   # Local file with one or more ETL specs
ais etl rm -f <URL>              # Remote spec file over HTTP(S)
```

* Supports multi-ETL YAML files separated by `---`.

> More info [ETL Pod Lifecycle](https://github.com/NVIDIA/aistore/blob/main/docs/etl.md#etl-pod-lifecycle)

---

## Inline Transformation

Use inline transformation to process an object **on-the-fly** with a registered ETL. The transformed output is streamed directly to the client.

```bash
ais etl object <ETL_NAME> <BUCKET/OBJECT_NAME> <OUTPUT>
```

### Examples

#### Transform an object and print to STDOUT

```bash
ais etl object transformer-md5 ais://shards/shard-0.tar -
```

*Output:*

```
393c6706efb128fbc442d3f7d084a426
```

#### Transform an object and save the output to a file

```bash
ais etl object transformer-md5 ais://shards/shard-0.tar output.txt
cat output.txt
```

*Output:*

```
393c6706efb128fbc442d3f7d084a426
```

#### Transform an object using ETL arguments

Use runtime arguments for customizable transformations. The argument is passed as a query parameter (`etl_args`) and must be handled by the ETL web server.

```bash
ais etl object transformer-hash-with-args ais://shards/shard-0.tar - --args=123
```

*Output:*

```
4af87d32ee1fb306
```

> Learn more: [Inline ETL Transformation](https://github.com/NVIDIA/aistore/blob/main/docs/etl.md#inline-etl-transformation)

---

## Offline Transformation

Use offline transformation to process entire buckets or a selected set of objects. The result is saved in a **new destination bucket**.

```bash
ais etl bucket <ETL_NAME> <SRC_BUCKET> <DST_BUCKET>
```

### Available Flags

| Flag                 | Description                                                |
| -------------------- | ---------------------------------------------------------- |
| `--list`             | Comma-separated list of object names (`obj1,obj2`).        |
| `--template`         | Template pattern for object names (`obj-{000..100}.tar`).  |
| `--ext`              | Extension transformation map (`{jpg:txt}`).                |
| `--prefix`           | Prefix to apply to output object names.                    |
| `--wait`             | Block until transformation is complete.                    |
| `--requests-timeout` | Per-object timeout for transformation.                     |
| `--dry-run`          | Simulate transformation without modifying cluster state.   |
| `--num-workers`      | Number of concurrent workers to use during transformation. |

### Examples

#### Transform an entire bucket

```bash
ais etl bucket transformer-md5 ais://src_bucket ais://dst_bucket
ais wait xaction <XACTION_ID>
```

#### Transform a subset of objects using a template

```bash
ais etl bucket transformer-md5 ais://src_bucket ais://dst_bucket --template "shard-{10..12}.tar"
```

#### Apply extension mapping and add a prefix

```bash
ais etl bucket transformer-md5 ais://src_bucket ais://dst_bucket --ext="{in1:out1,in2:out2}" --prefix="etl-" --wait
```

#### Perform a dry-run to preview changes

```bash
ais etl bucket transformer-md5 ais://src_bucket ais://dst_bucket --dry-run --wait
```

*Output:*

```
[DRY RUN] No modifications on the cluster
2 objects (20MiB) would have been put into bucket ais://dst_bucket
```

> Learn more: [Offline ETL Transformation](https://github.com/NVIDIA/aistore/blob/main/docs/etl.md#offline-etl-transformation)
