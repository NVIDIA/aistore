---
layout: post
title: ETL
permalink: /docs/etl
redirect_from:
 - /etl.md/
 - /docs/etl.md/
---

**ETL** stands for **E**xtract, **T**ransform, **L**oad. More specifically:

* **E**xtract - data from different original formats and/or multiple sources;
* **T**ransform - to the unified common format optimized for subsequent computation (e.g., training deep learning model);
* **L**oad - transformed data into a new destination - e.g., a storage system that supports high-performance computing over large scale datasets.

The latter can be AIStore (AIS). The system is designed from the ground up to support all 3 stages of the ETL pre (or post) processing. With AIS, you can effortlessly manage the AIS cluster by executing custom transformations in two ways:

1. **Inline**: This involves transforming datasets on the fly, where the data is read and streamed in a transformed format directly to computing clients.
2. **Offline**: Here, the transformed output is stored as a new dataset, which AIS makes accessible for any future computations.


> Implementation-wise, *offline* transformations of any kind, on the one hand, and copying datasets, on the other, are closely related - the latter being, effectively, a *no-op* offline transformation.

Most notably, AIS always runs transformations locally - *close to data*. Running *close to data* has always been one of the cornerstone design principles whereby in a deployed cluster each AIStore target proportionally contributes to the resulting cumulative bandwidth - the bandwidth that, in turn, will scale linearly with each added target.

This was the principle behind *distributed shuffle* (code-named [dSort](/docs/dsort.md)).
And this is exactly how we have more recently implemented **AIS-ETL** - the ETL service provided by AIStore. Find more information on the architecture and implementation of `ais-etl` [here](/ext/etl/README.md).

Technically, the service supports running user-provided ETL containers **and** custom Python scripts within the storage cluster.

**Note:** AIS-ETL (service) requires [Kubernetes](https://kubernetes.io).

## Table of Contents

- [Getting Started](#getting-started)
- [Inline ETL example](#inline-etl-example)
- [Offline ETL example](#offline-etl-example)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Extract, Transform and Load using user-defined functions](#extract-transform-and-load-using-user-defined-functions)
- [Extract, Transform and Load using custom containers](#extract-transform-and-load-using-custom-containers)
- [*init code* request](#init-code-request)
  - [`hpush://` communication](#hpush-communication)
  - [`io://` communication](#io-communication)
  - [Runtimes](#runtimes)
  - [Argument Types](#argument-types)
- [*init spec* request](#init-spec-request)
    - [Requirements](#requirements)
    - [Specification YAML](#specification-yaml)
    - [Required or additional fields](#required-or-additional-fields)
    - [Forbidden fields](#forbidden-fields)
    - [Communication Mechanisms](#communication-mechanisms)
    - [Argument Types](#argument-types-1)
- [Transforming objects](#transforming-objects)
- [API Reference](#api-reference)
- [ETL name specifications](#etl-name-specifications)

## Getting Started with ETL in AIStore

To begin using ETLs in AIStore, you'll need to run AIStore within a Kubernetes cluster. There are several ways to achieve this, each suited for different purposes:

1. **AIStore Development with Native Kubernetes (minikube)**:
   - Folder: [deploy/dev/k8s](/deploy/dev/k8s)
   - Intended for: AIStore development using native Kubernetes provided by [minikube](https://minikube.sigs.k8s.io/docs)
   - How to use: Run minikube and deploy the AIS cluster on it using the carefully documented steps available [here](/deploy/dev/k8s/README.md).
   - Documentation: [README](/deploy/dev/k8s/README.md)

2. **Production Deployment with Kubernetes**:
   - Folder: [deploy/prod/k8s](/deploy/prod/k8s)
   - Intended for: Production use
   - How to use: Utilize the Dockerfiles in this folder to build AIS images for production deployment. For this purpose, there is a separate dedicated [repository](https://github.com/NVIDIA/ais-k8s) that contains corresponding tools, scripts, and documentation.
   - Documentation: [AIS/K8s Operator and Deployment Playbooks](https://github.com/NVIDIA/ais-k8s)

To verify that your deployment is correctly set up, execute the following [CLI](/docs/cli.md) command:

```console
$ ais etl show
```

If you receive an empty response without any errors, your AIStore cluster is now ready to run ETL tasks.

## Inline ETL example

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

# Step 4: Create a spec file to initialize the ETL process
# In this example, we are using the MD5 transformer as a sample ETL.
$ curl -s https://raw.githubusercontent.com/NVIDIA/ais-etl/master/transformers/md5/pod.yaml -o md5_spec.yaml

# Step 5: Initialize the ETL process
$ ais etl init spec --from-file md5_spec.yaml --name etl-md5 --comm-type hpull

# Step 6: Check if the ETL is running
$ ais etl show

# Step 7: Run the transformation
# Replace 'ais://src/text.txt' with the appropriate source object location and '-' with the desired destination (e.g., local file path or another AIS bucket).
$ ais etl object etl-md5 ais://src/text.txt -
```

## Offline ETL example
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
$ cat > code.py
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

$ cat > deps.txt
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
## Extract, Transform, and Load using User-Defined Functions

1. To perform Extract, Transform, and Load (ETL) using user-defined functions, send the transform function in the [**init code** request](#init-code-request) to an AIStore endpoint.

2. Upon receiving the **init code** request, the AIStore proxy broadcasts the request to all AIStore targets in the cluster.

3. When an AIStore target receives the **init code**, it initiates the execution of the container **locally** on the target's machine (also known as [Kubernetes Node](https://kubernetes.io/docs/concepts/architecture/nodes/)).

## Extract, Transform, and Load using Custom Containers

1. To perform Extract, Transform, and Load (ETL) using custom containers, execute the [**init spec** API](#init-spec-request) to an AIStore endpoint.
   
   > The request contains a YAML spec and ultimately triggers the creation of [Kubernetes Pods](https://kubernetes.io/docs/concepts/workloads/pods/pod/) that run the user's ETL logic inside.

2. Upon receiving the **init spec** request, the AIStore proxy broadcasts the request to all AIStore targets in the cluster.

3. When a target receives the **init spec**, it starts the user container **locally** on the target's machine (also known as [Kubernetes Node](https://kubernetes.io/docs/concepts/architecture/nodes/)).

## *init code* request

You can write your custom `transform` function that takes input object bytes as a parameter and returns output bytes (the transformed object's content).
You can then use the *init code* request to execute this `transform` on the entire distributed dataset.

In effect, a user can skip the entire step of writing their Dockerfile and building a custom ETL container - the *init code* capability allows the user to skip this step entirely.

> If you are familiar with [FasS](https://en.wikipedia.org/wiki/Function_as_a_service), then you probably will find this type of ETL initialization the most intuitive.

For a detailed step-by-step tutorial on *init code* requests, please see [Python SDK ETL Tutorial](https://github.com/NVIDIA/aistore/blob/main/python/examples/sdk/sdk-etl-tutorial.ipynb) and [Python SDK ETL Examples](https://github.com/NVIDIA/aistore/tree/main/python/examples/ais-etl).

The `init_code` request currently supports two communication types:

### `hpush://` communication

In `hpush` communication type, the user has to define a function that takes bytes as a parameter, processes it and returns bytes. e.g. [ETL to calculate MD5 of an object](https://github.com/NVIDIA/aistore/blob/main/python/examples/ais-etl/etl_md5_hpush.py)

```python
def transform(input_bytes: bytes) -> bytes
```

You can also stream objects in `transform()` by setting the `CHUNK_SIZE` parameter (`CHUNK_SIZE` > 0).

e.g. [ETL to calculate MD5 of an object with streaming](https://github.com/NVIDIA/aistore/blob/main/python/examples/ais-etl/etl_md5_hpush_streaming.py), [ETL to transform images using torchvision](https://github.com/NVIDIA/aistore/blob/main/python/examples/ais-etl/etl_torchvision_hpush.py).

> **Note:**
>- If the function uses external dependencies, a user can provide an optional dependencies file or in the `elt().init()` function of Python SDK. These requirements will be installed on the machine executing the `transform` function and will be available for the function.

### `io://` communication

In `io://` communication type, users have to define a `transform()` function that reads bytes from [`sys.stdin`](https://docs.python.org/3/library/sys.html#sys.stdin), carries out transformations over it, and then writes bytes to [`sys.stdout`](https://docs.python.org/3/library/sys.html#sys.stdout).

```python
def transform() -> None:
    input_bytes = sys.stdin.buffer.read()
    # output_bytes = process(input_bytes)
    sys.stdout.buffer.write(output_bytes)
```

e.g. [ETL to calculate MD5 of an object](https://github.com/NVIDIA/aistore/blob/main/python/examples/ais-etl/etl_md5_io.py), [ETL to transform images using torchvision](https://github.com/NVIDIA/aistore/blob/main/python/examples/ais-etl/etl_torchvision_io.py)

### Runtimes

AIS-ETL provides several *runtimes* out of the box.
Each *runtime* determines the programming language of your custom `transform` function and the set of pre-installed packages and tools that your `transform` can utilize.

Currently, the following runtimes are supported:

| Name | Description |
| --- | --- |
| `python3.8v2` | `python:3.8` is used to run the code. |
| `python3.10v2` | `python:3.10` is used to run the code. |
| `python3.11v2` | `python:3.11` is used to run the code. |

More *runtimes* will be added in the future, with plans to support the most popular ETL toolchains.
Still, since the number of supported  *runtimes* will always remain somewhat limited, there's always the second way: build your ETL container and deploy it via [*init spec* request](#init-spec-request).

### Argument Types

The AIStore `etl init code` provides two `arg_type` parameter options for specifying the type of object specification between the AIStore and ETL container. These options are utilized as follows:

| Parameter Value | Description |
|-----------------|-------------|
| "" (Empty String) | This serves as the default option, allowing the object to be passed as bytes. When initializing ETLs, the `arg_type` parameter can be entirely omitted, and it will automatically default to passing the object as bytes to the transformation function. |
| "url" | When set to "url," this option allows the passing of the URL of the objects to be transformed to the user-defined transform function. It's important to note that this option is limited to '--comm-type=hpull'. In this scenario, the user is responsible for implementing the logic to fetch objects from the buckets based on the URL of the object received as a parameter. |


## *init spec* request

*Init spec* request covers all, even the most sophisticated, cases of ETL initialization.
It allows running any Docker image that implements certain requirements on communication with the cluster.
The *init spec* request requires writing a Pod specification following specification requirements.

For a detailed step-by-step tutorial on *init spec* request, please see the [MD5 ETL playbook](/docs/tutorials/etl/compute_md5.md).

#### Requirements

Custom ETL container is expected to satisfy the following requirements:

1. Start a web server that supports at least one of the listed [communication mechanisms](#communication-mechanisms).
2. The server can listen on any port, but the port must be specified in Pod spec with `containerPort` - the cluster
 must know how to contact the Pod.
3. AIS target(s) may send requests in parallel to the web server inside the ETL container - any synchronization, therefore, must be done on the server-side.

#### Specification YAML

Specification of an ETL should be in the form of a YAML file.
It is required to follow the Kubernetes [Pod template format](https://kubernetes.io/docs/concepts/workloads/pods/#pod-templates)
and contain all necessary fields to start the Pod.

#### Required or additional fields

| Path | Required | Description | Default |
| --- | --- | --- | --- |
| `metadata.annotations.communication_type` | `false` | [Communication type](#communication-mechanisms) of an ETL. | `hpush://` |
| `metadata.annotations.wait_timeout` | `false` | Timeout on ETL Pods starting on target machines. See [annotations](#annotations) | infinity |
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

#### Communication Mechanisms

AIS currently supports 3 (three) distinct target ⇔ container communication mechanisms to facilitate the fly or offline transformation.
Users  can choose and specify (via YAML spec) any of the following:

| Name | Value | Description |
|---|---|---|
| **post** | `hpush://` | A target issues a POST request to its ETL container with the body containing the requested object. After finishing the request, the target forwards the response from the ETL container to the user. |
| **reverse proxy** | `hrev://` | A target uses a [reverse proxy](https://en.wikipedia.org/wiki/Reverse_proxy) to send a (GET) request to a cluster using an ETL container. ETL container should make a GET request to a target, transform bytes, and return the result to the target. |
| **redirect** | `hpull://` | A target uses [HTTP redirect](https://developer.mozilla.org/en-US/docs/Web/HTTP/Redirections) to send a (GET) request to cluster using an ETL container. ETL container should make a GET request to the target, transform bytes, and return it to a user. |
| **input/output** | `io://` | A target remotely runs the binary or the code and sends the data to standard input and excepts the transformed bytes to be sent on standard output. |

> ETL container will have `AIS_TARGET_URL` environment variable set to the URL of its corresponding target.
> To make a request for a given object it is required to add `<bucket-name>/<object-name>` to `AIS_TARGET_URL`, eg. `requests.get(env("AIS_TARGET_URL") + "/" + bucket_name + "/" + object_name)`.

#### Argument Types

The AIStore `etl init spec` provides three `arg_type` parameter options for specifying the type of object specification between the AIStore and ETL container. These options are utilized as follows:

| Parameter Value | Description |
|-----------------|-------------|
| "" (Empty String) | This serves as the default option, allowing the object to be passed as bytes. When initializing ETLs, the `arg_type` parameter can be entirely omitted, and it will automatically default to passing the object as bytes to the transformation function. |
| "url" | Pass the URL of the objects to be transformed to the user-defined transform function. It's important to note that this option is limited to '--comm-type=hpull'. In this scenario, the user is responsible for implementing the logic to fetch objects from the buckets based on the URL of the object received as a parameter. |
| "fqn" | Pass a fully-qualified name (FQN) of the locally stored object. User is responsible for opening, reading, transforming, and closing the corresponding file. |

## Transforming objects

AIStore supports both *inline* transformation of selected objects and *offline* transformation of an entire bucket.

There are two ways to run ETL transformations:
- HTTP RESTful APIs are described in [API Reference section](#api-reference) of this document.
- [ETL CLI](/docs/cli/etl.md)
- [Python SDK](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/README.md#etls)
- [AIS Loader](/docs/aisloader.md)

## API Reference

This section describes how to interact with ETLs via RESTful API.

> `G` - denotes a (`hostname:port`) address of a **gateway** (any gateway in a given AIS cluster)

| Operation | Description | HTTP action | Example |
| --- | --- | --- | --- |
| Init spec ETL | Initializes ETL based on POD `spec` template. Returns `ETL_NAME`. | PUT /v1/etl | `curl -X PUT 'http://G/v1/etl' '{"spec": "...", "id": "..."}'` |
| Init code ETL | Initializes ETL based on the provided source code. Returns `ETL_NAME`. | PUT /v1/etl | `curl -X PUT 'http://G/v1/etl' '{"code": "...", "dependencies": "...", "runtime": "python3", "id": "..."}'` |
| List ETLs | Lists all running ETLs. | GET /v1/etl | `curl -L -X GET 'http://G/v1/etl'` |
| View ETLs Init spec/code | View code/spec of ETL by `ETL_NAME` | GET /v1/etl/ETL_NAME | `curl -L -X GET 'http://G/v1/etl/ETL_NAME'` |
| Transform object | Transforms an object based on ETL with `ETL_NAME`. | GET /v1/objects/<bucket>/<objname>?etl_name=ETL_NAME | `curl -L -X GET 'http://G/v1/objects/shards/shard01.tar?etl_name=ETL_NAME' -o transformed_shard01.tar` |
| Transform bucket | Transforms all objects in a bucket and puts them to destination bucket. | POST {"action": "etl-bck"} /v1/buckets/from-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "etl-bck", "name": "to-name", "value":{"ext":"destext", "prefix":"prefix", "suffix": "suffix"}}' 'http://G/v1/buckets/from-name'` |
| Dry run transform bucket | Accumulates in xaction stats how many objects and bytes would be created, without actually doing it. | POST {"action": "etl-bck"} /v1/buckets/from-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "etl-bck", "name": "to-name", "value":{"ext":"destext", "dry_run": true}}' 'http://G/v1/buckets/from-name'` |
| Stop ETL | Stops ETL with given `ETL_NAME`. | DELETE /v1/etl/ETL_NAME/stop | `curl -X POST 'http://G/v1/etl/ETL_NAME/stop'` |
| Delete ETL | Delete ETL spec/code with given `ETL_NAME` | DELETE /v1/etl/<ETL_NAME> | `curl -X DELETE 'http://G/v1/etl/ETL_NAME' |


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
1. Starts with an alphabet 'A' to 'Z' or 'a' to 'z'.
2. Can contain alphabets, numbers, underscore ('_'), or hyphen ('-').
3. Should have a length greater than 5 and less than 21.
4. Shouldn't contain special characters, except for underscore and hyphen.


## References

* For technical blogs with in-depth background and working real-life examples, see:
  - [ETL: Introduction](https://aiatscale.org/blog/2021/10/21/ais-etl-1)
  - [AIStore SDK & ETL: Transform an image dataset with AIS SDK and load into PyTorch](https://aiatscale.org/blog/2023/04/03/transform-images-with-python-sdk)
  - [ETL: Using WebDataset to train on a sharded dataset ](https://aiatscale.org/blog/2021/10/29/ais-etl-3)
* For step-by-step tutorials, see:
  - [Compute the MD5 of the object](/docs/tutorials/etl/compute_md5.md)
* For a quick CLI introduction and reference, see [ETL CLI](/docs/cli/etl.md)
* For initializing ETLs with AIStore Python SDK, see:
  - [Python SDK ETL Usage Docs](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/README.md#etls)
  - [Python SDK ETL Examples](https://github.com/NVIDIA/aistore/tree/main/python/examples/ais-etl)
  - [Python SDK ETL Tutorial](https://github.com/NVIDIA/aistore/blob/main/python/examples/sdk/sdk-etl-tutorial.ipynb)