## Table of Contents

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [`build` request](#build-request)
    - [Runtimes](#runtimes)
- [`init` request](#init-request)
    - [Requirements](#requirements)
    - [Communication Mechanisms](#communication-mechanisms)
    - [Annotations](#annotations)
- [Examples](#examples)
- [API Reference](#api-reference)

## Introduction

[ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) is "the general procedure of copying data from one or more sources into a destination system
which represents the data differently from the source(s) or in a different context than the source(s)" ([wikipedia](https://en.wikipedia.org/wiki/Extract,_transform,_load)).
To run custom ETL transforms *inline* and *close to data*, AIStore supports running custom ETL containers *in the storage cluster*.

As such, AIS-ETL (capability) requires [Kubernetes](https://kubernetes.io).
Each specific ETL is defined by its specification - a regular Kubernetes YAML (examples below).

* To start distributed ETL processing, a user either:
  1. needs to send transform function in [**build** request](#build-request) to the AIStore endpoint, or
  2. needs to send documented [**init** request](#init-request) to the AIStore endpoint.

     >  The request carries YAML spec and ultimately triggers creating [Kubernetes Pods](https://kubernetes.io/docs/concepts/workloads/pods/pod/) that run the user's ETL logic inside.

* Upon receiving **build**/**init**, AIS proxy broadcasts the request to all AIS targets in the cluster.

* When a target receives **build**/**init**, it starts the container **locally** on the target's machine (aka Kubernetes node).

* AIS targets use Kubernetes client to initialize pods and query their status.

## Prerequisites

First, allow targets to assign a ETL container to the same machine/node that the target is running on. To achieve that, `K8S_HOST_NAME = spec.nodeName` variable must be set inside the target pod's container (see more [here](https://kubernetes.io/docs/tasks/inject-data-application/environment-variable-expose-pod-information/)).

* Example:

    ```yaml
      containers:
      - env:
         - name: K8S_HOST_NAME
           valueFrom:
             fieldRef:
               fieldPath: spec.nodeName
    ```

## `build` request

You can write your own custom `transform` function (see example below) that takes input object bytes as a parameter and must return output bytes (the content of the transformed object). You can then use `build` request to execute this `transform` on the entire distributed dataset.

In effect, you can skip the entire step of writing your own Dockerfile and building custom ETL container - the `build` capability allows to skip this step entirely.

> First time users: it is recommended to startr with simple, Hello, World! type transformations.

> If you are familiar with [FasS](https://en.wikipedia.org/wiki/Function_as_a_service) then you probably will find this type of starting ETL most intuitive.

### Runtimes

AIS-ETL provides a number of *runtimes* out of the box. Each *runtime* determines the language of your custom `transform` function and the set of pre-installed packages and tools that your `transform` can utilize.

Currently, the following runtimes are supported:

| Name | Description |
| --- | --- |
| `python2` | `python:2.7.18` is used to run the code. |
| `python3` | `python:3.8.5` is used to run the code. |

We will be adding more *runtimes* in the future, with the plans to support the most popular ETL toolchains. Still, since the number of supported  *runtimes* will always remain somewhat limited, there's always the second way: build your own ETL container and deploy it via [`init` request](#init-request).

## `init` request

Init covers all, even wildest, cases.
It allows for running any Docker image that implements certain requirements that allow communication with the cluster.
It also requires writing pod specification to make sure the targets will know how to start it.

### Requirements

Custom ETL container is expected to satisfy the following requirements:

1. Start web server that supports at least one of the listed [communication mechanisms](#communication-mechanisms).
2. The server can listen on any port but the port must be specified in pod spec with `containerPort` - the cluster must know how to contact the pod.
3. AIS target(s) may send requests in parallel to the web server inside ETL container - any synchronization, therefore, must be done on the server-side.

### Communication Mechanisms

To facilitate on the fly or offline transformation, AIS currently supports 3 (three) distinct target ⇔ container communication mechanisms.
User can choose and specify (via YAML spec) any of the following:

| Name | Value | Description |
|---|---|---|
| **post** | `hpush://` | A target issues a POST request to its ETL container with the body containing the requested object. After finishing the request, the target forwards the response from the ETL container to the user. |
| **reverse proxy** | `hrev://` | A target uses a [reverse proxy](https://en.wikipedia.org/wiki/Reverse_proxy) to send (GET) request to cluster using ETL container. ETL container should make GET request to a target, transform bytes, and return the result to the target. |
| **redirect** | `hpull://` | A target uses [HTTP redirect](https://developer.mozilla.org/en-US/docs/Web/HTTP/Redirections) to send (GET) request to cluster using ETL container. ETL container should make GET request to the target, transform bytes, and return the result to a user. |

### Annotations

The target communicates with a pod defined in the pod specification under `communication_type` key:
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

> NOTE: ETL container will have `AIS_TARGET_URL` environment variable set to the address of its corresponding target.

## Examples

Throughout the examples, we assume that 1. and 2. from [prerequisites](#prerequisites) are fulfilled.

### Compute MD5 on the objects

To showcase the capabilities of ETL, we will go over a simple ETL container that computes MD5 checksum of the object.
There are two ways of approaching this problem:

1. **Simplified flow**

    In this example, we will be using `python3` runtime.
    In simplified flow we are only expected to write a simple `transform` function, which can look like this (`code.py`):

    ```python
    import hashlib

    def transform(input_bytes):
        md5 = hashlib.md5()
        md5.update(input_bytes)
        return md5.hexdigest().encode()
    ```

    `transform` function must take bytes as argument (the content of the object) and return output bytes that will be saved in the transformed object.

    Once we have the `transform` function defined we can use CLI to build and initialize ETL:
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
    For that we need to write `Dockerfile` which can look like this:

    ```dockerfile
    FROM python:3.8.5-alpine3.11

    RUN mkdir /code
    WORKDIR /code
    COPY server.py server.py

    EXPOSE 80

    ENTRYPOINT [ "/code/server.py", "--listen", "0.0.0.0", "--port", "80" ]
    ```

    Once we have the docker file we must build it and publish it to some [Docker Registry](https://docs.docker.com/registry/) so that our Kubernetes cluster can pull this image later.
    In this example, we will use [quay.io](https://quay.io/) Docker Registry.

    ```console
    $ docker build -t quay.io/user/md5_server:v1 .
    $ docker push quay.io/user/md5_server:v1
    ```

    The next step would be to create a pod spec that would be run on Kubernetes (`spec.yaml`):

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
    This is required, as a target needs to know precise address of the ETL container.

    Another note is that we pass additional parameters via the `annotations` field.
    We specified the communication type and wait time (for the pod to start).

    Once we have our `spec.yaml` we can initialize ETL with CLI:
    ```console
    $ ais etl init spec.yaml
    JGHEoo89gg
    ```

Just before we started ETL containers our pods looked like this:

```console
$ kubectl get pods
NAME                 READY   STATUS    RESTARTS   AGE
ais-proxy-2wvhp      1/1     Running   0          48m
ais-target-9knsb     1/1     Running   0          46m
ais-target-fsxhp     1/1     Running   0          46m
```

We can see that the cluster is running with 1 proxy and 2 targets.
After we initialized the ETL, we expect two more pods to be started (`#targets == #etl_containers`).

```console
$ kubectl get pods
NAME                                  READY   STATUS    RESTARTS   AGE
ais-proxy-2wvhp                       1/1     Running   0          48m
ais-target-9knsb                      1/1     Running   0          46m
ais-target-fsxhp                      1/1     Running   0          46m
transformer-md5-fgjk3-node1           1/1     Running   0          1m
transformer-md5-vspra-node2           1/1     Running   0          1m
```

As expected, two more pods are up and running - one for each target.

> **Note:** ETL containers will be run on the same node as the targets that started them.
In other words, each ETL container runs close to data and does not generate any extract-transform-load
related network traffic. Given that there are as many ETL containers as storage nodes
(one container per target) and that all ETL containers run in parallel, the cumulative "transformation"
bandwidth scales proportionally to the number of storage nodes and disks.

Finally, we can use newly created pods to transform the objects on the fly for us:

```console
$ ais create bucket transform
$ echo "some text :)" | ais put - transform/shard.in
$ ais etl object JGHEoo89gg transform/shard.in -
393c6706efb128fbc442d3f7d084a426
```

Voila! The ETL container successfully computed the `md5` on the `transform/shard.in` object.

Alternatively, one can use offline ETL feature, to transform the whole bucket.

```console
$ ais create bucket transform
$ echo "some text :)" | ais put - transform/shard.in
$ XACT_ID=$(ais etl bucket JGHEoo89gg transform transform-md5)
$ ais wait xaction $XACT_ID
```

Once ETL isn't needed anymore, the pods can be stopped with:

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

In this example, we will see how ETL can be used to preprocess the images of ImageNet for the learning.
We will be using `python3` runtime.

Before we start writing code let's put example tar file with ImageNet images to the AIStore.
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

As expected, the size of the images inside the new tarball has been standardized as all images have the same resolution (`224*224*3=150528`).

## API Reference

This section describes how to interact with ETLs via RESTful API.
Alternatively, you can use [ETL CLI](/cmd/cli/resources/etl.md) or [AIS Loader](/bench/aisloader/README.md).

> `G` - denotes a (`hostname:port`) address of a **gateway** (any gateway in a given AIS cluster)

| Operation | Description | HTTP action | Example |
|--- | --- | --- | ---|
| Init ETL | Inits ETL based on `spec.yaml`. Returns `ETL_ID` | POST /v1/etl/init | `curl -X POST 'http://G/v1/etl/init' -T spec.yaml` |
| Build ETL | Builds and initializes ETL based on the provided source code. Returns `ETL_ID` | POST /v1/etl/build | `curl -X POST 'http://G/v1/etl/build' '{"code": "...", "dependencies": "...", "runtime": "python3"}'` |
| List ETLs | Lists all running ETLs | GET /v1/etl/list | `curl -L -X GET 'http://G/v1/etl/list'` |
| Transform object | Transforms an object based on ETL with `ETL_ID` | GET /v1/objects/<bucket>/<objname>?uuid=ETL_ID | `curl -L -X GET 'http://G/v1/objects/shards/shard01.tar?uuid=ETL_ID' -o transformed_shard01.tar` |
| Transform bucket | Transforms all objects in a bucket and puts them to destination bucket | POST {"action": "etlbck"} /v1/buckets/from-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "etlbck", "name": "to-name", "value":{"ext":"destext", "prefix":"prefix", "suffix": "suffix"}}' 'http://G/v1/buckets/from-name'` |
| Dry run transform bucket | Accumulates in xaction stats how many objects and bytes would be created, without actually doing it | POST {"action": "etlbck"} /v1/buckets/from-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "etlbck", "name": "to-name", "value":{"ext":"destext", "dry_run": true}}' 'http://G/v1/buckets/from-name'` |
| Stop ETL | Stops ETL with given `ETL_ID` | DELETE /v1/etl/stop/ETL_ID | `curl -X DELETE 'http://G/v1/etl/stop/ETL_ID'` |
