## Table of Contents

- [Introduction](#introduction)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Initializing ETL](#defining-and-initializing-etl)
    - [`build` request](#build-request)
        - [Runtimes](#runtimes)
    - [`init` request](#init-request)
        - [Requirements](#requirements)
        - [YAML Specification](#specification-yaml-file)
        - [Communication Mechanisms](#communication-mechanisms)
        - [Annotations](#annotations)
- [Transforming objects with ETL](#transforming-objects-with-created-etl)
- [Examples](#examples)
    - [Compute MD5](#compute-md5)
    - [PyTorch ImageNet preprocessing](#pytorch-imagenet-preprocessing)
- [API Reference](#api-reference)

## Introduction

[ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) is "the general procedure of copying data from one or more sources into a destination system
which represents the data differently from the source(s) or in a different context than the source(s)" ([wikipedia](https://en.wikipedia.org/wiki/Extract,_transform,_load)).
To run data transformations *inline* and *close to data*, AIStore supports running ETL containers *in the storage cluster*.

As such, AIS-ETL (capability) requires [Kubernetes](https://kubernetes.io).

> If you want to try ETL, we recommend starting the AIStore cluster on the cloud.
> We have provided scripts to make it easy for you. See [AIStore on the cloud](https://github.com/NVIDIA/ais-k8s/blob/master/terraform/README.md).

## Architecture

The AIStore ETL extension is designed to maximize effectiveness of the transform process.
It minimizes resources waste on unnecessary operations like exchanging data between storage and compute nodes, which takes place in conventional ETL systems.

Based on specification provided by a user, each target starts its own ETL container (worker), which from now on will be responsible for transforming objects stored on the corresponding target.
This approach minimizes I/O operations, as well as assures scalability of ETL with the number of targets in the cluster.

The following picture presents architecture of the ETL extension.

<img src="/docs/images/aistore-etl-arch.png" alt="ETL architecture" width="80%">


## Prerequisites

- Cluster has to be deployed on Kubernetes.
- A target must know on which Kubernetes Node it runs.
To achieve this, the target uses `HOSTNAME` environment variable, set by Kubernetes, to discover its Pod name.
This variable should not be overwritten during the deployment of a target Pod.

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

Specification of an ETL should be in form of YAML file.
It is required to follow Kubernetes [Pod template format](https://kubernetes.io/docs/concepts/workloads/pods/#pod-templates)
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

## Examples

### Compute MD5

To showcase ETL's capabilities, we will go over a simple ETL container that computes the MD5 checksum of the object.
There are two ways of approaching this problem:

1. **Simplified flow**

    In this example, we will be using `python3` runtime.
    In simplified flow, we are only expected to write a simple `transform` function, which can look like this (`code.py`):

    ```python
    import hashlib

    def transform(input_bytes):
        md5 = hashlib.md5()
        md5.update(input_bytes)
        return md5.hexdigest().encode()
    ```

    `transform` function must take bytes as an argument (the object's content) and return output bytes that will be saved in the transformed object.

    Once we have the `transform` function defined, we can use CLI to build and initialize ETL:
    ```console
    $ ais etl build --from-file=code.py --runtime=python3
    JGHEoo89gg
    ```

2. **Regular flow**

    First, we need to write a server.
    In this case, we will write a Python 3 HTTP server.
    The code for it can look like this (`server.py`):

    ```python
    #!/usr/bin/env python

    import argparse
    import hashlib
    from http.server import HTTPServer, BaseHTTPRequestHandler


    class S(BaseHTTPRequestHandler):
        def _set_headers(self):
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()

        def do_POST(self):
            content_length = int(self.headers["Content-Length"])
            post_data = self.rfile.read(content_length)
            md5 = hashlib.md5()
            md5.update(post_data)

            self._set_headers()
            self.wfile.write(md5.hexdigest().encode())


    def run(server_class=HTTPServer, handler_class=S, addr="localhost", port=8000):
        server_address = (addr, port)
        httpd = server_class(server_address, handler_class)

        print(f"Starting httpd server on {addr}:{port}")
        httpd.serve_forever()


    if __name__ == "__main__":
        parser = argparse.ArgumentParser(description="Run a simple HTTP server")
        parser.add_argument(
            "-l",
            "--listen",
            default="localhost",
            help="Specify the IP address on which the server listens",
        )
        parser.add_argument(
            "-p",
            "--port",
            type=int,
            default=8000,
            help="Specify the port on which the server listens",
        )
        args = parser.parse_args()
        run(addr=args.listen, port=args.port)
    ```

    Once we have a server that computes the MD5, we need to create an image out of it.
    For that, we need to write `Dockerfile`, which can look like this:

    ```dockerfile
    FROM python:3.8.5-alpine3.11

    RUN mkdir /code
    WORKDIR /code
    COPY server.py server.py

    EXPOSE 80

    ENTRYPOINT [ "/code/server.py", "--listen", "0.0.0.0", "--port", "80" ]
    ```

    Once we have the docker file, we must build it and publish it to some [Docker Registry](https://docs.docker.com/registry/) so that our Kubernetes cluster can pull this image later.
    In this example, we will use [quay.io](https://quay.io/) Docker Registry.

    ```console
    $ docker build -t quay.io/user/md5_server:v1 .
    $ docker push quay.io/user/md5_server:v1
    ```

    The next step is to create spec of a Pod, that will be run on Kubernetes (`spec.yaml`):

    ```yaml
    apiVersion: v1
    kind: Pod
    metadata:
      name: transformer-md5
      annotations:
        communication_type: hpush://
        wait_timeout: 2m
    spec:
      containers:
        - name: server
          image: quay.io/user/md5_server:v1
          ports:
            - name: default
              containerPort: 80
          command: ['/code/server.py', '--listen', '0.0.0.0', '--port', '80']
    ```

    **Important**: the server listens on the same port as specified in `ports.containerPort`.
    It is required, as a target needs to know the precise address of the ETL container.

    Another note is that we pass additional parameters via the `annotations` field.
    We specified the communication type and wait time (for the Pod to start).

    Once we have our `spec.yaml`, we can initialize ETL with CLI:
    ```console
    $ ais etl init spec.yaml
    JGHEoo89gg
    ```

Just before we started ETL containers, our Pods looked like this:

```console
$ kubectl get pods
NAME                 READY   STATUS    RESTARTS   AGE
ais-proxy-2wvhp      1/1     Running   0          48m
ais-target-9knsb     1/1     Running   0          46m
ais-target-fsxhp     1/1     Running   0          46m
```

We can see that the cluster is running with one proxy and two targets.
After we initialized the ETL, we expect two more Pods to be started (`#targets == #etl_containers`).

```console
$ kubectl get pods
NAME                                  READY   STATUS    RESTARTS   AGE
ais-proxy-2wvhp                       1/1     Running   0          48m
ais-target-9knsb                      1/1     Running   0          46m
ais-target-fsxhp                      1/1     Running   0          46m
transformer-md5-fgjk3-node1           1/1     Running   0          1m
transformer-md5-vspra-node2           1/1     Running   0          1m
```

As expected, two more Pods are up and running - one for each target.

> ETL containers will be run on the same node as the targets that started them.
> In other words, each ETL container runs close to data and does not generate any extract-transform-load
> related network traffic. Given that there are as many ETL containers as storage nodes
> (one container per target) and that all ETL containers run in parallel, the cumulative "transformation"
> bandwidth scales proportionally to the number of storage nodes and disks.

Finally, we can use newly created Pods to transform the objects on the fly for us:

```console
$ ais create bucket transform
$ echo "some text :)" | ais put - transform/shard.in
$ ais etl object JGHEoo89gg transform/shard.in -
393c6706efb128fbc442d3f7d084a426
```

Voila! The ETL container successfully computed the `md5` on the `transform/shard.in` object.

Alternatively, one can use the offline ETL feature to transform the whole bucket.

```console
$ ais create bucket transform
$ echo "some text :)" | ais put - transform/shard.in
$ ais etl bucket JGHEoo89gg ais://transform ais://transform-md5
5JjIuGemR
$ ais wait xaction 5JjIuGemR
```

Once ETL isn't needed anymore, the Pods can be stopped with:

```console
$ ais etl stop JGHEoo89gg
ETL containers stopped successfully.
$ kubectl get pods
NAME                 READY   STATUS    RESTARTS   AGE
ais-proxy-2wvhp      1/1     Running   0          50m
ais-target-9knsb     1/1     Running   0          48m
ais-target-fsxhp     1/1     Running   0          48m
```

### PyTorch ImageNet preprocessing

In this example, we will see how ETL can be used to preprocess the images of ImageNet for learning.
We will be using `python3` runtime.

Before we start writing code, let's put an example tar file with ImageNet images to the AIStore.
The tar we will be using is `n02085620.tar` (saved as `raw-train.tar`) from [ILSVRC2012_img_train_t3.tar](http://www.image-net.org/challenges/LSVRC/2012/dd31405981ef5f776aa17412e1f0c112/ILSVRC2012_img_train_t3.tar).

```console
$ tar -tvf raw-train.tar | head -n 5
-rw-r--r--  0 aditya86 users   27024 Jul  4  2012 n02085620_10074.JPEG
-rw-r--r--  0 aditya86 users   34446 Jul  4  2012 n02085620_10131.JPEG
-rw-r--r--  0 aditya86 users   12891 Jul  4  2012 n02085620_10621.JPEG
-rw-r--r--  0 aditya86 users   34837 Jul  4  2012 n02085620_1073.JPEG
-rw-r--r--  0 aditya86 users   18126 Jul  4  2012 n02085620_10976.JPEG
$ ais create bucket ais://imagenet
"ais://imagenet" bucket created
$ ais put raw-train.tar imagenet
PUT "raw-train.tar" into bucket "imagenet"
```

Our transform code will look like this (`code.py`):
```python
import torch, tarfile, io
from PIL import Image
from torchvision import transforms

preprocessing = transforms.Compose([
    transforms.RandomResizedCrop(224),
    transforms.RandomHorizontalFlip(),
    transforms.ToTensor(),
    transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    transforms.ToPILImage(),
    transforms.Lambda(lambda x: x.tobytes()),
])

def transform(input_bytes: bytes) -> bytes:
    input_tar = tarfile.open(fileobj=io.BytesIO(input_bytes))

    output_bytes = io.BytesIO()
    output_tar = tarfile.open(fileobj=output_bytes, mode="w|")

    for member in input_tar:
        image = Image.open(input_tar.extractfile(member))
        processed = preprocessing(image)

        member.size = len(processed)
        output_tar.addfile(member, io.BytesIO(processed))

    return output_bytes.getvalue()
```

and dependencies (`deps.txt`):
```
torch==1.6.0
torchvision==0.7.0
```

Now we can build the ETL:
```console
$ ais etl build --from-file=code.py --deps-file=deps.txt --runtime=python3
JGHEoo89gg
$ ais etl object JGHEoo89gg imagenet/raw-train.tar preprocessed-train.tar
$ tar -tvf preprocessed-train.tar | head -n 5
-rw-r--r--  0 aditya86 users  150528 Jul  4  2012 n02085620_10074.JPEG
-rw-r--r--  0 aditya86 users  150528 Jul  4  2012 n02085620_10131.JPEG
-rw-r--r--  0 aditya86 users  150528 Jul  4  2012 n02085620_10621.JPEG
-rw-r--r--  0 aditya86 users  150528 Jul  4  2012 n02085620_1073.JPEG
-rw-r--r--  0 aditya86 users  150528 Jul  4  2012 n02085620_10976.JPEG
```

As expected, the size of the new tarball images has been standardized as all images have the same resolution (`224*224*3=150528`).

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
