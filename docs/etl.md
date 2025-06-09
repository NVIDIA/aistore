**ETL** — **Extract**, **Transform**, **Load** — is a core workflow in data pipelines:

- **Extract** data from various formats and sources
- **Transform** it into a unified format optimized for compute (e.g., deep learning)
- **Load** the result into a new destination

The AIStore ETL system is designed from the ground up to execute all 3 stages of the ETL process *locally*. AIStore and any of its [supported backends](https://github.com/NVIDIA/aistore/blob/main/docs/images/supported-backends.png) can serve as both the source for extraction and the destination for loading. Unlike traditional ETL pipelines that extract data out of storage, transform it externally, and push it back, AIStore deploys custom transformation logic directly on the nodes that store the data. This drastically **reduces data movement**, **improves performance**, and **eliminates infrastructure overhead**.

AIStore ETL is fully language- and framework-agnostic. You can deploy your own custom web server as a transformation pod or use our plug-and-play ETL servers by simply providing the transformation logic. It supports both **inline transformations** (real-time processing via GET requests) and **offline batch transformations** (bucket-to-bucket), delivering up to **50×** performance gains over traditional client-side ETL workflows.

![ETL Inline & Offline Transformation Flow](assets/ais_etl_series/etl-inline-offline.gif)

Learn more about the architecture and implementation [here](https://github.com/NVIDIA/aistore/tree/main/ext/etl).

**Note:** AIStore ETL requires [Kubernetes](https://kubernetes.io).

## Table of Contents

- [Quick Start](#quick-start)
  - [Inline ETL Transformation](#inline-etl-transformation)
  - [Offline ETL Transformation](#offline-etl-transformation)
- [Communication Mechanisms](#communication-mechanisms)
- [*init code* - Transform by User-Defined Python Functions](#init-code-request---transform-by-user-defined-python-functions)
  - [`hpush://` and `hpull://` Communication](#hpush-and-hpull-communications)
  - [`io://` Communication](#io-communication)
  - [Runtimes](#runtimes)
  - [Argument Types](#argument-types)
- [*init spec* - Transform by Custom ETL Container](#init-spec---transform-by-custom-etl-container)
  - [Build Custom ETL Container Image](#build-custom-etl-container-image)
    - [Build from AIStore-Provided Frameworks](#recommended-approach-use-aistore-provided-frameworks)
    - [Build from Scratch](#advanced-approach-build-container-image-from-scratch)
  - [Specification YAML](#specification-yaml)
    - [Required or Additional Fields](#required-or-additional-fields)
    - [Forbidden Fields](#forbidden-fields)
  - [Argument Types](#argument-types-1)
  - [Direct Put Optimization](#direct-put-optimization)
- [ETL Pod Lifecycle](#etl-pod-lifecycle)
- [API Reference](#api-reference)
- [ETL Name Specifications](#etl-name-specifications)

## Quick Start

To begin using ETLs in AIStore, you'll need to deploy AIStore on a Kubernetes cluster. There are several ways to achieve this, each suited for different purposes:

1. **AIStore Development with Local Kubernetes**:
   - Folder: [deploy/dev/k8s/kustomize](https://github.com/NVIDIA/aistore/tree/main/deploy/dev/k8s/kustomize)
   - Intended for: AIStore development using local Kubernetes.
   - How to use: Run a local Kubernetes cluster and deploy an AIS cluster on it using the carefully documented steps available [here](https://github.com/NVIDIA/aistore/tree/main/deploy/dev/k8s/kustomize).
   - Documentation: [README](https://github.com/NVIDIA/aistore/tree/main/deploy/dev/k8s/kustomize)

2. **Production Deployment with Kubernetes**:
   - Folder: [deploy/prod/k8s](https://github.com/NVIDIA/aistore/tree/main/deploy/prod/k8s)
   - Intended for: Production use
   - How to use: Utilize the Dockerfiles in this folder to build AIS images for production deployment. For this purpose, there is a separate dedicated [repository](https://github.com/NVIDIA/ais-k8s) that contains corresponding tools, scripts, and documentation.
   - Documentation: [AIS/K8s Operator and Deployment Playbooks](https://github.com/NVIDIA/ais-k8s)

To verify that your deployment is correctly set up, execute the following [CLI](/docs/cli.md) command:

```console
$ ais etl show
```

If you receive an empty response without any errors, your AIStore cluster is now ready to run ETL tasks.

**Note**: Unlike most AIStore features that are available immediately via a single command, ETL requires an *initialization step* where the transformation logic is defined and plugged into the system.

### Inline ETL Transformation

ETL inline transform involves transforming datasets on the fly, where the data is read, transformed, and streamed directly to the requesting clients. You can think of inline transformation as a variant of [GET object](/docs/cli/object.md#get-object) operation, except the object content is passed through a user-defined transformation before being returned.

To follow this and subsequent examples, make sure you have the [AIS CLI](/docs/cli.md) installed on your system.

```console
# Prerequisites:
# AIStore must be running on Kubernetes. AIS CLI (Command-Line Interface) must be installed. Make sure you have a running AIStore cluster before proceeding. 

# Step 1: Create a new bucket
$ ais bucket create ais://src

# Step 2: Show existing ETLs
$ ais etl show

# Step 3: Create a temporary file and add it to the bucket
$ echo "hello world" > text.txt
$ ais put text.txt ais://src

# Step 4: Create a spec file to initialize the ETL
# In this example, we are using the MD5 transformer as a sample ETL.
$ curl -s https://raw.githubusercontent.com/NVIDIA/ais-etl/main/transformers/md5/pod.yaml -o md5_spec.yaml

# Step 5: Initialize the ETL
$ ais etl init spec --from-file md5_spec.yaml --name etl-md5 --comm-type hpull://

# Step 6: Check if the ETL is running
$ ais etl show

# Step 7: Run the transformation
# Replace 'ais://src/text.txt' with the appropriate source object location and '-' with the desired destination (e.g., local file path or another AIS bucket).
$ ais etl object etl-md5 ais://src/text.txt -
```

### Offline ETL Transformation

Offline ETL transformations produce a new dataset (bucket) as output, which AIStore stores and makes available for future use. You can think of an ETL offline transformation as a variant of [Bucket Copy](/docs/cli/bucket.md#copy-ais-bucket) operation, except each object copied into the destination bucket is passed through the user-defined ETL transformation.

```console
$ # Download Imagenet dataset
$ wget -O imagenet.tar https://image-net.org/data/ILSVRC/2012/ILSVRC2012_img_val.tar --no-check-certificate

$ # Extract Imagenet
$ mkdir imagenet && tar -C imagenet -xvf imagenet.tar >/dev/null

$ # Check if the dataset is extracted correctly
$ cd imagenet
$ ls | head -5

$ # Add the entire dataset to a bucket
$ ais bucket create ais://imagenet
$ ais put . ais://imagenet -r

$ # Check if the dataset is available in the bucket
$ ais ls ais://imagenet | head -5

$ # Create a custom transformation using torchvision
$ # The `code.py` file contains the Python code for the transformation function, and `deps.txt` lists the dependencies required to run `code.py`
$ cat code.py
import io
from PIL import Image
from torchvision import transforms as T

preprocess = T.Compose(
    [
        T.Resize(256),
        T.CenterCrop(224),
        T.ToTensor(),
        T.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        T.ToPILImage(),
    ]
)

# Define the transform function
def transform(data: bytes) -> bytes:
    image = Image.open(io.BytesIO(data))
    processed = preprocess(image)
    buf = io.BytesIO()
    processed.save(buf, format='JPEG')
    byte_im = buf.getvalue()
    return byte_im

$ cat deps.txt
torch==2.0.1
torchvision==0.15.2

$ ais etl init code --name etl-torchvision --from-file code.py --deps-file deps.txt --runtime python3.11v2 --comm-type hpull

$ # Perform an offline transformation
$ ais etl bucket etl-torchvision ais://imagenet ais://imagenet-transformed --ext="{JPEG:JPEG}"

$ # Check if the transformed dataset is available in the bucket
$ ais ls ais://imagenet-transformed | head -5

$ # Verify one of the images by downloading its content
$ ais object get ais://imagenet-transformed/ILSVRC2012_val_00050000.JPEG test.JPEG
```

**Note**: there are two ways to run ETL initialization and transformation:
- [ETL CLI](/docs/cli/etl.md)
- [Python SDK](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/README.md#etls)


## Communication Mechanisms

AIS currently supports 4 distinct target_to_container communication mechanisms to facilitate inline or offline transformation.

![ETL Communication Types](assets/ais_etl_series/etl-communication-types.png)

Users can choose and specify any of the following:

| Name | Value | Description |
|---|---|---|
| **HTTP Push** | `hpush://` | A target issues a PUT request to its ETL container with the body containing the requested object. After finishing the request, the target forwards the response from the ETL container to the user. |
| **HTTP Redirect** | `hpull://` | A target uses [HTTP redirect](https://developer.mozilla.org/en-US/docs/Web/HTTP/Redirections) to send a (GET) request to cluster using an ETL container. ETL container should make a GET request to the target, transform bytes, and return it to a user. |
| **WebSocket** | `ws://` | A target uses [WebSocket](https://developer.mozilla.org/en-US/docs/Web/API/WebSocket) to send the requested object to its ETL container as individual messages. |
| **Input/Output** | `io://` | A target remotely runs the binary or the code and sends the data to standard input and excepts the transformed bytes to be sent on standard output. |

> ETL container will have `AIS_TARGET_URL` environment variable set to the URL of its corresponding target.
> To make a request for a given object it is required to add `<bucket-name>/<object-name>` to `AIS_TARGET_URL`, eg. `requests.get(env("AIS_TARGET_URL") + "/" + bucket_name + "/" + object_name)`.

## *init code* - Transform by User-Defined Python Functions

The *init code* request allows you to define a custom `transform` function that takes input object bytes as a parameter and returns output bytes (the transformed object's content).

Once initialized, this transformation function is immediately available across the entire cluster and can be applied to any AIStore-accessible dataset, in-cluster or remote.

> In effect, a user can skip the entire step of writing their Dockerfile and building a custom ETL container - the *init code* capability allows the user to skip this step entirely.
> If you are familiar with [FasS](https://en.wikipedia.org/wiki/Function_as_a_service), then you probably will find this type of ETL initialization the most intuitive.

The `init_code` request currently supports three communication types:

### `hpush://` and `hpull://` communications

In `hpush` or `hpull://` communication types, the user has to define a function that takes bytes as a parameter, processes it and returns bytes.

```python
def transform(input_bytes: bytes) -> bytes
```

**Note:**
- If the function requires external dependencies, the user can provide an optional dependencies file or specify them in the elt().init() function of the Python SDK.
- These requirements will be installed on the machine executing the `transform` function and will be available for the function.


### `io://` communication

In `io://` communication type, users have to define a `transform()` function that reads bytes from [`sys.stdin`](https://docs.python.org/3/library/sys.html#sys.stdin), carries out transformations over it, and then writes bytes to [`sys.stdout`](https://docs.python.org/3/library/sys.html#sys.stdout).

```python
def transform() -> None:
    input_bytes = sys.stdin.buffer.read()
    # output_bytes = process(input_bytes)
    sys.stdout.buffer.write(output_bytes)
```

### Runtimes

AIS-ETL provides several *runtimes* out of the box for the *init code* request.
Each *runtime* determines the programming language of your custom `transform` function and the set of pre-installed packages and tools that your `transform` can utilize.

Currently, the following runtimes are supported:

| Name | Description |
| --- | --- |
| `python3.9v2` | `python:3.9` is used to run the code. |
| `python3.10v2` | `python:3.10` is used to run the code. |
| `python3.11v2` | `python:3.11` is used to run the code. |
| `python3.12v2` | `python:3.12` is used to run the code. |
| `python3.13v2` | `python:3.13` is used to run the code. |

More *runtimes* will be added in the future, with plans to support the most popular ETL toolchains.
Still, since the number of supported  *runtimes* will always remain somewhat limited, there's always the second way: build your ETL container and deploy it via [*init spec* request](#init-spec-request).

### Argument Types

The AIStore `etl init code` provides two `arg_type` parameter options for specifying the type of object specification between the AIStore and ETL container. These options are utilized as follows:

| Parameter Value | Description |
|-----------------|-------------|
| "" (Empty String) | This serves as the default option, allowing the object to be passed as bytes. When initializing ETLs, the `arg_type` parameter can be entirely omitted, and it will automatically default to passing the object as bytes to the transformation function. |
| "url" | When set to "url," this option allows the passing of the URL of the objects to be transformed to the user-defined transform function. In this scenario, the user is responsible for implementing the logic to fetch objects from the buckets based on the URL of the object received as a parameter. |

## *init spec* - Transform by Custom ETL Container

*init spec* request covers all, even the most sophisticated ETL use case.
It allows running any container image that implements certain requirements on communication with AIStore cluster.

### Build Custom ETL Container Image

A custom ETL container must meet the following requirements:

1. Start a web server that supports at least one of the listed [communication mechanisms](#communication-mechanisms).
2. The server can listen on any port, but the port must be specified in Pod spec with `containerPort` - the cluster
 must know how to contact the Pod.
3. AIS target(s) may send requests in parallel to the web server inside the ETL container, any necessary synchronization must be handled within the server implementation.

#### Recommended Approach: Use AIStore-Provided Frameworks

AIStore provides extensible frameworks that fulfill all the requirements for building custom ETL containers using any supported communication mechanism and argument types.

Much like the `init code` approach, these frameworks hide the details of web server implementation and internal optimizations, allowing users to focus solely on defining the transformation logic. They also offer mechanisms to programmatically manage the state of the ETL instance used to process individual objects.

Below is the minimum example `md5.py` (and the corresponding [`Dockerfile`](https://github.com/NVIDIA/ais-etl/blob/main/transformers/echo/Dockerfile)) to implement a custom ETL that transforms objects into their MD5 checksum value.
```python
import hashlib
from aistore.sdk.etl.webserver.fastapi_server import FastAPIServer

class MD5Server(FastAPIServer):
    """
    FastAPI-based HTTP server for MD5 hashing.

    Inherits from FastAPIServer to handle concurrent transform requests.
    """
    def transform(self, data: bytes, path: str, args: str) -> bytes:
        """
        Compute the MD5 digest of the request payload.
        """
        return hashlib.md5(data).hexdigest().encode()

fastapi_server = MD5Server(port=8000)
fastapi_app = fastapi_server.app  # Expose the FastAPI app
```

> Check the [AIStore ETL Webserver Python SDK Document](https://github.com/NVIDIA/aistore/tree/main/python/aistore/sdk/etl/webserver/README.md) for more details and examples on how to fully utilize the Python webserver frameworks.

Here’s the equivalent Golang version, along with the related [`Dockerfile`](https://github.com/NVIDIA/ais-etl/blob/main/transformers/go_echo/Dockerfile) and [`dependency`](https://github.com/NVIDIA/ais-etl/tree/main/transformers/go_echo) files:
```go
import (
	"crypto/md5"
	"io"
	"bytes"

	"github.com/NVIDIA/aistore/ext/etl/webserver"
)

type MD5Server struct {
    webserver.ETLServer
}

func (*MD5Server) Transform(input io.ReadCloser, path, args string) (io.ReadCloser, error) {
	hash := md5.New()
	if _, err := io.Copy(hash, input); err != nil {
		return nil, err
	}
	input.Close()
	return io.NopCloser(bytes.NewReader(hash.Sum(nil))), nil
}

// Interface guard to ensure MD5Server satisfies webserver.ETLServer at compile time
var _ webserver.ETLServer = (*MD5Server)(nil)

func main() {
	svr := &MD5Server{}
	webserver.Run(svr, "0.0.0.0", 8000)
}
```

> Check the [AIStore Golang ETL Webserver Document](https://github.com/NVIDIA/aistore/tree/main/ext/etl/webserver/README.md) for more details and examples on how to fully utilize the Golang webserver frameworks.

After these setup, build a container image from the code:
```
$ docker build -t docker.io/<your-profile>/md5:latest .
$ docker push docker.io/<your-profile>/md5:latest
```

**Note**: In the examples above, the `etl_args` string corresponds to the value provided via the `--args` flag when using the `ais etl object` CLI command, or the `args` parameter in the `ETLConfig` class when using the Python SDK. This argument allows users to dynamically pass additional parameters to the inline transformation logic at runtime.

#### Advanced Approach: Build Container Image from Scratch

For a detailed step-by-step tutorial on building a custom ETL container from scratch for *init spec* request, please see the [MD5 ETL playbook](/docs/tutorials/etl/compute_md5.md).

### Specification YAML

Specification of an ETL should be in the form of a YAML file.
It is required to follow the Kubernetes [Pod template format](https://kubernetes.io/docs/concepts/workloads/pods/#pod-templates)
and contain all necessary fields to start the Pod.

#### Required or additional fields

| Path | Required | Description | Default |
| --- | --- | --- | --- |
| `metadata.annotations.communication_type` | `false` | [Communication type](#communication-mechanisms) of an ETL. | `hpush://` |
| `metadata.annotations.wait_timeout` | `false` | Timeout on ETL Pods starting on target machines. See [annotations](#annotations) | infinity |
| `metadata.annotations.support_direct_put` | `false` | Enable [direct put](#direct-put-optimization) optimization of an ETL. | - |
| `spec.containers` | `true` | Containers running inside a Pod, exactly one required. | - |
| `spec.containers[0].image` | `true` | Docker image of ETL container. | - |
| `spec.containers[0].ports` | `true` (except `io://` communication type) | Ports exposed by a container, at least one expected. | - |
| `spec.containers[0].ports[0].Name` | `true` | Name of the first Pod should be `default`. | - |
| `spec.containers[0].ports[0].containerPort` | `true` | Port which a cluster will contact containers on. | - |
| `spec.containers[0].readinessProbe` | `true` (except `io://` communication type) | ReadinessProbe of a container. | - |
| `spec.containers[0].readinessProbe.timeoutSeconds` | `false` | Timeout for a readiness probe in seconds. | `5` |
| `spec.containers[0].readinessProbe.periodSeconds` | `false` | Period between readiness probe requests in seconds. | `10` |
| `spec.containers[0].readinessProbe.httpGet.Path` | `true` | Path for HTTP readiness probes. | - |
| `spec.containers[0].readinessProbe.httpGet.Port` | `true` | Port for HTTP readiness probes. Required `default`. | - |

#### Forbidden fields

| Path | Reason |
| --- | --- |
| `spec.affinity.nodeAffinity` | Used by AIStore to colocate ETL containers with targets. |

### Argument Types

The AIStore `etl init spec` provides three `arg_type` parameter options for specifying the type of object specification between the AIStore and ETL container. These options are utilized as follows:
> ETL container will have `ARG_TYPE` environment variable set to the corresponding parameter value.

| Parameter Value | Description |
|-----------------|-------------|
| "" (Empty String) | This serves as the default option, allowing the object to be passed as bytes. When initializing ETLs, the `arg_type` parameter can be entirely omitted, and it will automatically default to passing the object as bytes to the transformation function. |
| "url" | Pass the URL of the objects to be transformed to the user-defined transform function. In this scenario, the user is responsible for implementing the logic to fetch objects from the buckets based on the URL of the object received as a parameter. |
| "fqn" | Pass a fully-qualified name (FQN) of the locally stored object. User is responsible for opening, reading, transforming, and closing the corresponding file. |

### Direct Put Optimization

> _Applicable only to bucket-to-bucket offline transformations._

In bucket-to-bucket offline transformations, the destination target for a transformed object may differ from the original target. By default, the ETL container sends the transformed data back to the original target, which then forwards it to the destination. The **direct put** optimization streamlines this flow by allowing the ETL container to send the transformed object directly to the destination target. Our stress tests across multiple transformation types consistently show a 3 - 5x performance improvement with direct put enabled.

> The ETL container will have the `DIRECT_PUT` environment variable set to `"true"` or `"false"` accordingly.

The destination address is provided based on the communication mechanism in use:

| Communication Mechanism | Execution Steps |
|-------------------------|-----------------|
| **HTTP Push/Redirect** | For each HTTP request, the destination target's address is provided in the `ais-node-url` header. The ETL container should perform an additional `PUT` request to that address with the transformed object as the payload. |
| **WebSocket** | Since WebSocket preserves message order and boundaries, the ETL container receives two consecutive messages: (1) a control message in JSON format containing the destination address, FQN, and associated ETL argument, and (2) a binary message with the object content. The container should process them in order and issue a `PUT` request to the destination with the transformed object. |

## ETL Pod Lifecycle

ETL follows a structured lifecycle to enhance observability. The lifecycle consists of three stages: `Initializing`, `Running`, and `Stopped`. This design prevents ETL from consuming resources when not in use while maintaining visibility into failures.

### Lifecycle Stages & Transitions

```sh
+--------------+     Success      +---------+
| Initializing |  ------------->  | Running |
+--------------+                  +---------+
       | ^                             |
 Error | | User Restarts               |
       v |                             |
+--------------+      Runtime Error    |
|   Stopped    |  <--------------------+
+--------------+       User Stops
        |
        | User Deletes
        v
    (Removed)
```

#### 1. `Initializing` Stage
The ETL enters this stage when created via an [Init](#api-reference) requests. The system provisions the required Kubernetes resources, including pods and services.
- **Success**: Transitions to `Running` stage.
- **Failure (Pod Initialization Error/Timeout)**: Transitions to `Stopped` stage.
#### 2. `Running` Stage
The ETL is actively processing requests and remains in this stage unless stopped manually or due to an error.
- **User Sends [Stop ETL](#api-reference)**: Transitions to `Stopped` stage with error message `user abort`.
- **Runtime Error**: Transitions to `Stopped` stage.
#### 3. `Stopped` Stage
The ETL is inactive but retains metadata, allowing for future restarts. Upon entering the Stopped state, AIStore automatically cleans up all associated Kubernetes resources (pods, services) across all targets.
- **User Sends [Restart ETL](#api-reference)**: Transitions to `Initializing` stage.
- **User Sends [Delete ETL](#api-reference)**: Permanently removes the ETL instance and its metadata from AIStore.

## API Reference

This section describes how to interact with ETLs via RESTful API.

> `G` - denotes a (`hostname:port`) address of a **gateway** (any gateway in a given AIS cluster)

| Operation | Description | HTTP action | Example |
| --- | --- | --- | --- |
| Init spec ETL | Initializes ETL based on POD `spec` template. Returns `ETL_NAME`. | PUT /v1/etl | `curl -X PUT 'http://G/v1/etl' '{"spec": "...", "id": "..."}'` |
| Init code ETL | Initializes ETL based on the provided source code. Returns `ETL_NAME`. | PUT /v1/etl | `curl -X PUT 'http://G/v1/etl' '{"code": "...", "dependencies": "...", "runtime": "python3", "id": "..."}'` |
| List ETLs | Lists all running ETLs. | GET /v1/etl | `curl -L -X GET 'http://G/v1/etl'` |
| View ETLs Init spec/code | View code/spec of ETL by `ETL_NAME` | GET /v1/etl/ETL_NAME | `curl -L -X GET 'http://G/v1/etl/ETL_NAME'` |
| Transform an object | Transforms an object based on ETL with `ETL_NAME`. | GET /v1/objects/<bucket>/<objname>?etl_name=ETL_NAME | `curl -L -X GET 'http://G/v1/objects/shards/shard01.tar?etl_name=ETL_NAME' -o transformed_shard01.tar` |
| Transform bucket | Transforms all objects in a bucket and puts them to destination bucket. | POST {"action": "etl-bck"} /v1/buckets/SRC_BUCKET | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "etl-bck", "name": "to-name", "value":{"id": "ETL_NAME", "ext":{"SRC_EXT": "DEST_EXT"}, "prefix":"PREFIX_FILTER", "prepend":"PREPEND_NAME"}}' 'http://G/v1/buckets/SRC_BUCKET?bck_to=PROVIDER%2FNAMESPACE%2FDEST_BUCKET%2F'` |
| Transform and synchronize bucket | Synchronize destination bucket with its remote (e.g., Cloud or remote AIS) source. | POST {"action": "etl-bck"} /v1/buckets/SRC_BUCKET | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "etl-bck", "name": "to-name", "value":{"id": "ETL_NAME", "synchronize": true}}' 'http://G/v1/buckets/SRC_BUCKET?bck_to=PROVIDER%2FNAMESPACE%2FDEST_BUCKET%2F'` |
| Dry run transform bucket | Accumulates in xaction stats how many objects and bytes would be created, without actually doing it. | POST {"action": "etl-bck"} /v1/buckets/SRC_BUCKET | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "etl-bck", "name": "to-name", "value":{"id": "ETL_NAME", "dry_run": true}}' 'http://G/v1/buckets/SRC_BUCKET?bck_to=PROVIDER%2FNAMESPACE%2FDEST_BUCKET%2F'` |
| Stop ETL | Stops ETL with given `ETL_NAME`. | POST /v1/etl/ETL_NAME/stop | `curl -X POST 'http://G/v1/etl/ETL_NAME/stop'` |
| Restart ETL | Restarts ETL with given `ETL_NAME`. | POST /v1/etl/ETL_NAME/start | `curl -X POST 'http://G/v1/etl/ETL_NAME/start'` |
| Delete ETL | Delete ETL spec/code with given `ETL_NAME` | DELETE /v1/etl/ETL_NAME | `curl -X DELETE 'http://G/v1/etl/ETL_NAME'` |


## ETL name specifications

Every initialized ETL has a unique user-defined `ETL_NAME` associated with it, used for running transforms/computation on data or stopping the ETL.

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: compute-md5
(...)
```

When initializing ETL from spec/code, a valid and unique user-defined `ETL_NAME` should be assigned using the `--name` CLI parameter as shown below.

```console
$ ais etl init code --name=etl-md5 --from-file=code.py --runtime=python3 --deps-file=deps.txt --comm-type hpull
or
$ ais etl init spec --name=etl-md5 --from-file=spec.yaml --comm-type hpull
```

Below are specifications for a valid `ETL_NAME`:
1. Must start and end with a lowercase alphabet ('a' to 'z') or a number ('1' to '9').
2. Can contain lowercase alphabets, numbers, or hyphen ('-').
3. Should have a length greater than 5 and less than 33.
4. Shouldn't contain special characters except for hyphen (no capitals or underscore).

## References

* For technical blogs with in-depth background and working real-life examples, see:
  - [ETL: Introduction](https://aistore.nvidia.com/blog/2021/10/21/ais-etl-1)
  - [AIStore SDK & ETL: Transform an image dataset with AIS SDK and load into PyTorch](https://aistore.nvidia.com/blog/2023/04/03/transform-images-with-python-sdk)
  - [ETL: Using WebDataset to train on a sharded dataset ](https://aistore.nvidia.com/blog/2021/10/29/ais-etl-3)
* For step-by-step tutorials, see:
  - [Compute the MD5 of the object](/docs/tutorials/etl/compute_md5.md)
* For a quick CLI introduction and reference, see [ETL CLI](/docs/cli/etl.md)
* For initializing ETLs with AIStore Python SDK, see:
  - [Python SDK ETL Usage Docs](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/README.md#etls)
  - [Python SDK ETL Examples](https://github.com/NVIDIA/aistore/tree/main/python/examples/ais-etl)
