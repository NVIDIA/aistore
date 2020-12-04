# ETL: Getting started 

## Table of Contents

- [Introduction](#introduction)
- [Starting AIStore cluster](#starting-aistore-cluster)
- [Initializing ETL](#defining-and-initializing-etl)
    - [`build` request](#build-request)
        - [Runtimes](#runtimes)
    - [`init` request](#init-request)
        - [Requirements](#requirements)
        - [YAML Specification](#specification-yaml-file)
        - [Communication Mechanisms](#communication-mechanisms)
        - [Annotations](#annotations)
- [Transforming objects with ETL](#transforming-objects-with-created-etl)
- [API Reference](#api-reference)

## Introduction

[ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) is "the general procedure of copying data from one or more sources into a destination system
which represents the data differently from the source(s) or in a different context than the source(s)" ([wikipedia](https://en.wikipedia.org/wiki/Extract,_transform,_load)).
To run data transformations *inline* and *close to data*, AIStore supports running ETL containers *in the storage cluster*.
It allows transforming each object in a dataset efficiently and fast, accordingly to transformation specified by a user.

As such, AIS-ETL (capability) requires [Kubernetes](https://kubernetes.io).

Please refer to [playbooks directory](/docs/playbooks/etl) for examples of ETL.

### Demos

The following demos present interaction with the AIStore ETL feature.
The details about each step are described in subsequent sections.

#### On the fly ETL example

<img src="/docs/images/etl-md5.gif" alt="ETL-MD5" width="900">

The example above uses [AIS CLI](/cmd/cli/README.md) to:
1. **Create** a new AIS bucket

2. **PUT** an object into this bucket

3. **Init** ETL container that performs simple MD5 computation.

   > Both the container itself and its [YAML specification]((https://raw.githubusercontent.com/NVIDIA/ais-etl/master/transformers/md5/pod.yaml) below are included primarily for illustration purposes.

   * [MD5 ETL YAML](https://raw.githubusercontent.com/NVIDIA/ais-etl/master/transformers/md5/pod.yaml)

4. **Transform** the object on the fly via custom ETL - the "transformation" in this case boils down to computing the object's MD5.

5. **Compare** the output with locally computed MD5.

#### Offline ETL example

<img src="/docs/images/etl-imagenet.gif" alt="ETL-ImageNet" width="100%">

The example above uses [AIS CLI](/cmd/cli/README.md) to:
1. **Create** a new AIS bucket

2. **PUT** multiple TAR files containing ImageNet images into the created bucket

3. **Init** ETL container, based only on a simple python function

4. **Transform** offline each TAR from the source bucket by standardizing images from the TAR and putting results in a destination bucket

5. **Verify** the transformation output by downloading one of the transformed TARs and checking its content.

## Starting AIStore cluster

> If you already have running AIStore cluster deployed on Kubernetes, skip this section and go to [Initialize ETL section](#defining-and-initializing-etl).

To deploy ETL-ready AIStore cluster, please refer to [AIStore Getting Started readme](/docs/getting_started.md).
Please note that you have to choose one of the Kubernetes deployment types.
If you don't have the hardware to run a cluster on, try [AIStore on the cloud](/docs/getting_started.md#on-cloud-deployment) deployment.

> During the AIStore on Kubernetes deployment, `HOSTNAME` environment variable, set by Kubernetes, should not be overwritten.
> Target uses it to discover its Pod name.

To verify that your deployment is set up correctly, run the following [CLI](/cmd/cli/README.md) command:
```console
$ ais etl ls
```

If there is no error, just an empty response, it means that your AIStore cluster is ready for ETL.

## Defining and initializing ETL

This section is going to describe how to define and initialize custom ETL transformations in the AIStore cluster.

Deploying ETL consists of the following steps:
1. To start distributed ETL processing, a user either:
   * needs to send transform function in [**build** request](#build-request) to the AIStore endpoint, or
   * needs to send documented [**init** request](#init-request) to the AIStore endpoint.
     >  The request carries YAML spec and ultimately triggers creating [Kubernetes Pods](https://kubernetes.io/docs/concepts/workloads/pods/pod/) that run the user's ETL logic inside.
2. Upon receiving **build**/**init** request, AIS proxy broadcasts the request to all AIS targets in the cluster.
3. When a target receives **build**/**init**, it starts the container **locally** on the target's machine (aka [Kubernetes Node](https://kubernetes.io/docs/concepts/architecture/nodes/)).

### `build` request

You can write your own custom `transform` function (see example below) that takes input object bytes as a parameter and returns output bytes (the transformed object's content).
You can then use the `build` request to execute this `transform` on the entire distributed dataset.

In effect, a user can skip the entire step of writing your own Dockerfile and building a custom ETL container - the `build` capability allows the user to skip this step entirely.

> If you are familiar with [FasS](https://en.wikipedia.org/wiki/Function_as_a_service), then you probably will find this type of ETL initialization the most intuitive.

#### Runtimes

AIS-ETL provides several *runtimes* out of the box.
Each *runtime* determines the programming language of your custom `transform` function, the set of pre-installed packages and tools that your `transform` can utilize.

Currently, the following runtimes are supported:

| Name | Description |
| --- | --- |
| `python2` | `python:2.7.18` is used to run the code. |
| `python3` | `python:3.8.5` is used to run the code. |

More *runtimes* will be added in the future, with the plans to support the most popular ETL toolchains.
Still, since the number of supported  *runtimes* will always remain somewhat limited, there's always the second way: build your own ETL container and deploy it via [`init` request](#init-request).

### `init` request

`Init` request covers all, even the most sophisticated, cases of ETL initialization.
It allows running any Docker image that implements certain requirements on communication with the cluster. 
The 'init' request requires writing a Pod specification following specification requirements.

#### Requirements

Custom ETL container is expected to satisfy the following requirements:

1. Start a web server that supports at least one of the listed [communication mechanisms](#communication-mechanisms).
2. The server can listen on any port, but the port must be specified in Pod spec with `containerPort` - the cluster
 must know how to contact the Pod.
3. AIS target(s) may send requests in parallel to the web server inside the ETL container - any synchronization, therefore, must be done on the server-side.

#### Specification YAML file

Specification of an ETL should be in the form of a YAML file.
It is required to follow the Kubernetes [Pod template format](https://kubernetes.io/docs/concepts/workloads/pods/#pod-templates)
and contain all necessary fields to start the Pod.

##### Required or additional fields

| Path | Required | Description | Default |
| --- | --- | --- | --- |
| `metadata.annotations.communication_type` | `false` | [Communication type](#communication-mechanisms) of an ETL. | `hpush://` |
| `metadata.annotations.wait_timeout` | `false` | Timeout on ETL Pods starting on target machines. See [annotations](#annotations) | infinity | 
| `spec.containers` | `true` | Containers running inside a Pod, exactly one required. | - |
| `spec.containers[0].image` | `true` | Docker image of ETL container. | - |
| `spec.containers[0].ports` | `true` | Ports exposed by a container, at least one expected. | - |
| `spec.containers[0].ports[0].Name` | `true` | Name of the first Pod should be `default`. | - |
| `spec.containers[0].ports[0].containerPort` | `true` | Port which a cluster will contact containers on. | - |
| `spec.containers[0].readinessProbe` | `true` | ReadinessProbe of a container. | - |
| `spec.containers[0].readinessProbe.timeoutSeconds` | `false` | Timeout for a readiness probe in seconds. | `5` | 
| `spec.containers[0].readinessProbe.periodSeconds` | `false` | Period between readiness probe requests in seconds. | `10` | 
| `spec.containers[0].readinessProbe.httpGet.Path` | `true` | Path for HTTP readiness probes. | - |
| `spec.containers[0].readinessProbe.httpGet.Port` | `true` | Port for HTTP readiness probes. Required `default`. | - |

##### Forbidden fields

| Path | Reason |
| --- | --- |
| `spec.affinity.nodeAffinity` | Used by AIStore to colocate ETL containers with targets. |
| `spec.affinity.nodeAntiAffinity` | Used by AIStore to require single ETL at a time. |

#### Communication Mechanisms

AIS currently supports 3 (three) distinct target ⇔ container communication mechanisms to facilitate the fly or offline transformation.
User can choose and specify (via YAML spec) any of the following:

| Name | Value | Description |
|---|---|---|
| **post** | `hpush://` | A target issues a POST request to its ETL container with the body containing the requested object. After finishing the request, the target forwards the response from the ETL container to the user. |
| **reverse proxy** | `hrev://` | A target uses a [reverse proxy](https://en.wikipedia.org/wiki/Reverse_proxy) to send (GET) request to cluster using ETL container. ETL container should make GET request to a target, transform bytes, and return the result to the target. |
| **redirect** | `hpull://` | A target uses [HTTP redirect](https://developer.mozilla.org/en-US/docs/Web/HTTP/Redirections) to send (GET) request to cluster using ETL container. ETL container should make a GET request to the target, transform bytes, and return it to a user. |

#### Annotations

The target communicates with a Pod defined in the Pod specification under `communication_type` key:
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: transformer-name
  annotations:
    communication_type: value
(...)
```

The specification can include `wait_timeout`.
It states how long a target should wait for an ETL container to transition into the `Ready` state.
If the timeout is exceeded, the initialization of the ETL container is considered failed.
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: etl-container-name
  annotations:
    wait_timeout: 2m
(...)
```

> ETL container will have `AIS_TARGET_URL` environment variable set to the URL of its corresponding target.
> To make a request for a given object it is required to add `<bucket-name>/<object-name>` to `AIS_TARGET_URL`, eg. `requests.get(env("AIS_TARGET_URL") + "/" + bucket_name + "/" + object_name)`.

## Transforming objects with created ETL

AIStore supports an on-line transformation of single objects and an offline transformation of the whole buckets.

There are two ways to run ETL transformations:
- HTTP RESTful API described in [API Reference section](#api-reference) of this document,
- [ETL CLI](/cmd/cli/resources/etl.md),
- [AIS Loader](/bench/aisloader/README.md).

## API Reference

This section describes how to interact with ETLs via RESTful API.

> `G` - denotes a (`hostname:port`) address of a **gateway** (any gateway in a given AIS cluster)

| Operation | Description | HTTP action | Example |
| --- | --- | --- | --- |
| Init ETL | Inits ETL based on `spec.yaml`. Returns `ETL_ID`. | POST /v1/etl/init | `curl -X POST 'http://G/v1/etl/init' -T spec.yaml` |
| Build ETL | Builds and initializes ETL based on the provided source code. Returns `ETL_ID`. | POST /v1/etl/build | `curl -X POST 'http://G/v1/etl/build' '{"code": "...", "dependencies": "...", "runtime": "python3"}'` |
| List ETLs | Lists all running ETLs. | GET /v1/etl/list | `curl -L -X GET 'http://G/v1/etl/list'` |
| Transform object | Transforms an object based on ETL with `ETL_ID`. | GET /v1/objects/<bucket>/<objname>?uuid=ETL_ID | `curl -L -X GET 'http://G/v1/objects/shards/shard01.tar?uuid=ETL_ID' -o transformed_shard01.tar` |
| Transform bucket | Transforms all objects in a bucket and puts them to destination bucket. | POST {"action": "etlbck"} /v1/buckets/from-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "etlbck", "name": "to-name", "value":{"ext":"destext", "prefix":"prefix", "suffix": "suffix"}}' 'http://G/v1/buckets/from-name'` |
| Dry run transform bucket | Accumulates in xaction stats how many objects and bytes would be created, without actually doing it. | POST {"action": "etlbck"} /v1/buckets/from-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "etlbck", "name": "to-name", "value":{"ext":"destext", "dry_run": true}}' 'http://G/v1/buckets/from-name'` |
| Stop ETL | Stops ETL with given `ETL_ID`. | DELETE /v1/etl/stop/ETL_ID | `curl -X DELETE 'http://G/v1/etl/stop/ETL_ID'` |
