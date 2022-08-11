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

The latter can be AIStore (AIS). The system is designed from the ground up to support all 3 stages of the ETL pre (or post) processing. You can easily task the AIS cluster by running custom transformations:

* *inline* - that is, transforming datasets on the fly by (randomly) reading them and streaming a resulting transformed output directly to (computing) clients that perform those reads;
* *offline* - storing transformed output as a new dataset that AIStore will make available for any number of future computations.

> Implementation-wise, *offline* transformations of any kind, on the one hand, and copying datasets, on the other, are closely related - the latter being, effectively, a *no-op* offline transformation.

Most notably, AIS always runs transformations locally - *close to data*. Running *close to data* has always been one of the cornerstone design principles whereby in a deployed cluster each AIStore target proportionally contributes to the resulting cumulative bandwidth - the bandwidth that, in turn, will scale linearly with each added target.

This was the principle behind *distributed shuffle* (code-named [dSort](/docs/dsort.md)).
And this is exactly how we have more recently implemented **AIS-ETL** - the ETL service provided by AIStore.

Technically, the service supports running user-provided ETL containers **and** custom Python scripts *in the* (and *by the*) storage cluster.

**Note:** AIS-ETL (service) requires [Kubernetes](https://kubernetes.io).

## References

* For technical blogs with in-depth background and working real-life examples, see:
  - [ETL: Introduction](https://aiatscale.org/blog/2021/10/21/ais-etl-1)
  - [ETL: Using AIS/PyTorch connector to transform ImageNet](https://aiatscale.org/blog/2021/10/22/ais-etl-2)
  - [ETL: Using WebDataset to train on a sharded dataset](https://aiatscale.org/blog/2021/10/29/ais-etl-3)
* For step-by-step tutorials, see:
  - [PyTorch ImageNet preprocessing](/docs/tutorials/etl/etl_imagenet_pytorch.md)
  - [Compute the MD5 of the object](/docs/tutorials/etl/compute_md5.md)
* For a quick CLI introduction and reference, see [ETL CLI](/docs/cli/etl.md)


The rest of this text is organized as follows:

- [References](#references)
- [Getting Started](#getting-started)
- [Inline ETL example](#inline-etl-example)
- [Offline ETL example](#offline-etl-example)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Defining and initializing ETL](#defining-and-initializing-etl)
  - [*init code* request](#init-code-request)
    - [`transform` function](#transform-function)
    - [Runtimes](#runtimes)
  - [*init spec* request](#init-spec-request)
    - [Requirements](#requirements)
    - [Specification YAML](#specification-yaml)
    - [Required or additional fields](#required-or-additional-fields)
    - [Forbidden fields](#forbidden-fields)
    - [Communication Mechanisms](#communication-mechanisms)
- [Transforming objects](#transforming-objects)
- [API Reference](#api-reference)
- [ETL name specifications](#etl-name-specifications)

## Getting Started

The following [video](https://www.youtube.com/watch?v=4PHkqTSE0ls "AIStore ETL Getting Started (Youtube video)") demonstrates AIStore's ETL feature using Jupyter Notebook.

{% include youtubePlayer.html id="4PHkqTSE0ls" %}

## Inline ETL example

![etl-md5](images/etl-md5.gif)

The example above uses [AIS CLI](/docs/cli.md) to:
1. **Create** a new bucket;
2. **PUT** an object into this bucket;
3. **Init** ETL container that performs MD5 computation.
4. **Transform** the object on the fly via custom ETL - the "transformation" in this case boils down to computing the object's MD5.
5. **Compare** the output with locally computed MD5.

Note that both the container itself and its [YAML specification](https://raw.githubusercontent.com/NVIDIA/ais-etl/master/transformers/md5/pod.yaml) (below) are included primarily for illustration purposes.

* [MD5 ETL YAML](https://raw.githubusercontent.com/NVIDIA/ais-etl/master/transformers/md5/pod.yaml)

## Offline ETL example

![etl-imagenet](images/etl-imagenet.gif)

The example above uses [AIS CLI](/docs/cli.md) to:
1. **Create** a new AIS bucket;
2. **PUT** multiple TAR files containing ImageNet images into the created bucket;
3. **Init** ETL container-based only on a simple python function;
4. **Transform** offline each TAR from the source bucket by standardizing images from the TAR and putting results in a destination bucket;
5. **Verify** the transformation output by downloading one of the transformed TARs and checking its content.

## Kubernetes Deployment

> If you already have a running AIStore cluster deployed on Kubernetes, skip this section and go to the [Initialize ETL](#defining-and-initializing-etl) section.

To deploy the ETL-ready AIStore cluster, please refer to [Getting Started](getting_started.md).

> Note that you have to choose one of the deployment types that supports Kubernetes - for example, [Cloud Deployment](getting_started.md#cloud-deployment).

> During the AIStore on Kubernetes deployment, the `HOSTNAME` environment variable, set by Kubernetes, **shouldn't** be overwritten - AIS target uses it to discover its Pod and Node name.
> In some environments (like `minikube`) the `HOSTNAME` is not reliable and in such cases, it's required to set `K8S_NODE_NAME` in Pod spec:
> ```yaml
> env:
>  - name: K8S_NODE_NAME
>     valueFrom:
>       fieldRef:
>         fieldPath: spec.nodeName
> ```

To verify that your deployment is set up correctly, run the following [CLI](/docs/cli.md) command:
```console
$ ais etl ls
```

If you see an empty response (and no errors) - your AIStore cluster is ready to run ETL.

## Extract, Transform and Load using user-defined functions

1. Send transform function in the [**init code** request](#init-code-request) to an AIStore endpoint
2. Upon receiving the **init code** request, the AIS proxy broadcasts the request to all AIS targets in the cluster.
3. When an AIS target receives **init code**, it starts the container **locally** on the target's machine (aka [Kubernetes Node](https://kubernetes.io/docs/concepts/architecture/nodes/)).

## Extract, Transform and Load using custom containers

1. execute [**init spec** API](#init-spec-request) to an AIStore endpoint.
   >  The request carries YAML spec and ultimately triggers creating [Kubernetes Pods](https://kubernetes.io/docs/concepts/workloads/pods/pod/) that run the user's ETL logic inside.
2. Upon receiving the **init spec** request, the AIS proxy broadcasts the request to all AIS targets in the cluster.
3. When a target receives **init spec**, it starts the user container **locally** on the target's machine (aka [Kubernetes Node](https://kubernetes.io/docs/concepts/architecture/nodes/)).

### *init code* request

You can write your custom `transform` function that takes input object bytes as a parameter and returns output bytes (the transformed object's content).
You can then use the *init code* request to execute this `transform` on the entire distributed dataset.

In effect, a user can skip the entire step of writing their Dockerfile and building a custom ETL container - the *init code* capability allows the user to skip this step entirely.

> If you are familiar with [FasS](https://en.wikipedia.org/wiki/Function_as_a_service), then you probably will find this type of ETL initialization the most intuitive.

For a detailed step-by-step tutorial on *init code* request, please see [ImageNet ETL playbook](/docs/tutorials/etl/etl_imagenet_pytorch.md).

#### `transform` function

To use the *init code* request a user has to provide a Python script with a defined `transform` function that has the following signature:
```python
def transform(input_bytes: bytes) -> bytes
```

If the function uses external dependencies, a user can provide an optional dependencies file, in `pip` [requirements file format](https://pip.pypa.io/en/stable/user_guide/#requirements-files).
These requirements will be installed on the machine executing the `transform` function and will be available for the function.

#### Runtimes

AIS-ETL provides several *runtimes* out of the box.
Each *runtime* determines the programming language of your custom `transform` function and the set of pre-installed packages and tools that your `transform` can utilize.

Currently, the following runtimes are supported:

| Name | Description |
| --- | --- |
| `python3.8v2` | `python:3.8` is used to run the code. |
| `python3.10v2` | `python:3.10` is used to run the code. |

More *runtimes* will be added in the future, with plans to support the most popular ETL toolchains.
Still, since the number of supported  *runtimes* will always remain somewhat limited, there's always the second way: build your ETL container and deploy it via [*init spec* request](#init-spec-request).

### *init spec* request

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

## Transforming objects

AIStore supports both *inline* transformation of selected objects and *offline* transformation of an entire bucket.

There are two ways to run ETL transformations:
- HTTP RESTful APIs are described in [API Reference section](#api-reference) of this document,
- [ETL CLI](/docs/cli/etl.md),
- [AIS Loader](/docs/aisloader.md).

## API Reference

This section describes how to interact with ETLs via RESTful API.

> `G` - denotes a (`hostname:port`) address of a **gateway** (any gateway in a given AIS cluster)

| Operation | Description | HTTP action | Example |
| --- | --- | --- | --- |
| Init spec ETL | Initializes ETL based on POD `spec` template. Returns `ETL_ID`. | PUT /v1/etl | `curl -X PUT 'http://G/v1/etl' '{"spec": "...", "id": "..."}'` |
| Init code ETL | Initializes ETL based on the provided source code. Returns `ETL_ID`. | PUT /v1/etl | `curl -X PUT 'http://G/v1/etl' '{"code": "...", "dependencies": "...", "runtime": "python3", "id": "..."}'` |
| List ETLs | Lists all running ETLs. | GET /v1/etl | `curl -L -X GET 'http://G/v1/etl'` |
| View ETLs Init spec/code | View code/spec of ETL by `ETL_ID` | GET /v1/etl/ETL_ID | `curl -L -X GET 'http://G/v1/etl/ETL_ID'` |
| Transform object | Transforms an object based on ETL with `ETL_ID`. | GET /v1/objects/<bucket>/<objname>?uuid=ETL_ID | `curl -L -X GET 'http://G/v1/objects/shards/shard01.tar?uuid=ETL_ID' -o transformed_shard01.tar` |
| Transform bucket | Transforms all objects in a bucket and puts them to destination bucket. | POST {"action": "etl-bck"} /v1/buckets/from-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "etl-bck", "name": "to-name", "value":{"ext":"destext", "prefix":"prefix", "suffix": "suffix"}}' 'http://G/v1/buckets/from-name'` |
| Dry run transform bucket | Accumulates in xaction stats how many objects and bytes would be created, without actually doing it. | POST {"action": "etl-bck"} /v1/buckets/from-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "etl-bck", "name": "to-name", "value":{"ext":"destext", "dry_run": true}}' 'http://G/v1/buckets/from-name'` |
| Stop ETL | Stops ETL with given `ETL_ID`. | DELETE /v1/etl/ETL_ID/stop | `curl -X POST 'http://G/v1/etl/ETL_ID/stop'` |
| Delete ETL | Delete ETL spec/code with given `ETL_ID` | DELETE /v1/etl/<ETL_ID> | `curl -X DELETE 'http://G/v1/etl/ETL_ID' |


## ETL name specifications

Every initialized ETL has a unique user-defined `ETL_ID` associated with it, used for running transforms/computation on data or stopping the ETL.

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: compute-md5
(...)
```

When initializing ETL from spec/code, a valid and unique user-defined `ETL_ID` should be assigned using the `--name` CLI parameter as shown below.

```console
$ ais etl init code --name=etl-md5 --from-file=code.py --runtime=python3 --deps-file=deps.txt
or
$ ais etl init spec --name=etl-md5 --from-file=spec.yaml
```

Below are specifications for a valid `ETL_ID`:
1. Starts with an alphabet 'A' to 'Z' or 'a' to 'z'.
2. Can contain alphabets, numbers, underscore ('_'), or hyphen ('-').
3. Should have a length greater than 5 and less than 21.
4. Shouldn't contain special characters, except for underscore and hyphen.
