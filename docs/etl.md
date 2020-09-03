## Table of Contents

- [Introduction](#introduction)
- [Communication Mechanisms](#communication-mechanisms)
- [Prerequisites](#prerequisites)
- [Examples](#examples)
- [API Reference](#api-reference)

## Introduction

[ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) is "the general procedure of copying data from one or more sources into a destination system
which represents the data differently from the source(s) or in a different context than the source(s)" ([wikipedia](https://en.wikipedia.org/wiki/Extract,_transform,_load)).
In order to run custom ETL transforms *inline* and *close to data*, AIStore supports running custom ETL containers *in the storage cluster* .

As such, AIS-ETL (capability) requires [Kubernetes](https://kubernetes.io). Each specific transformation is defined by its specification - a regular Kubernetes YAML (see examples below).

* To start distributed ETL processing, a user needs to send documented **init** request to the AIStore endpoint.

  >  The request carries YAML spec and ultimately triggers creating [Kubernetes Pods](https://kubernetes.io/docs/concepts/workloads/pods/pod/) that run the user's ETL logic inside.

* Upon receiving **init**, AIS proxy broadcasts the request to all AIS targets in the cluster.

* When a target receives **init**, it starts the container locally on the same (the target's) machine.

* Targets use `kubectl` to initialize the pods and gather necessary information for future runtime.

## Communication Mechanisms

To facilitate on the fly or offline transformation, AIS currently supports 3 (three) distinct target <=> container communication mechanisms. User can choose and specify (via YAML spec) any of the following:

| Name | Value | Description |
|---|---|---|
| **post** | `hpush://` | A target issues a POST request to its ETL container with the body containing the requested object. After finishing the request, the target forwards the response from the ETL container to the user. |
| **reverse proxy** | `hrev://` | A target uses a [reverse proxy](https://en.wikipedia.org/wiki/Reverse_proxy) to send (GET) request to cluster using ETL container. ETL container should make GET request to a target, transform bytes, and return the result to the target. |
| **redirect** | `hpull://` | A target uses [HTTP redirect](https://developer.mozilla.org/en-US/docs/Web/HTTP/Redirections) to send (GET) request to cluster using ETL container. ETL container should make GET request to the target, transform bytes, and return the result to a user. |

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
It states how long a target should wait for a ETL container to transition into `Ready` state.
If the timeout is exceeded, the initialization of the ETL container is considered failed.
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: etl-container-name
  annotations:
    wait_timeout: 30s
(...)
```

> NOTE: ETL container will have `AIS_TARGET_URL` environment variable set to the address of its corresponding target.

## Prerequisites

There are a couple of steps that are required to make the ETL work:
1. Target should be able to execute `kubectl` meaning that the binary should be in the `$PATH`.
2. `K8S_HOST_NAME = spec.nodeName` variable must be set inside the target pod's container (see more [here](https://kubernetes.io/docs/tasks/inject-data-application/environment-variable-expose-pod-information/)).
    Example:
    ```yaml
      containers:
      - env:
         - name: K8S_HOST_NAME
           valueFrom:
             fieldRef:
               fieldPath: spec.nodeName
    ```
   This will allow the target to assign an ETL container to the same machine/node that the target is working on.
3. The server inside the pod can listen on any port, but the port must be specified in pod spec with `containerPort` - the cluster must know how to contact the pod.

## Examples

Throughout the examples, we assume that 1. and 2. from [prerequisites](#prerequisites) are fulfilled.

### Compute MD5 on the objects

To showcase the capabilities of ETL, we will go over a simple ETL container that computes MD5 checksum of the object.

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
        content_length = int(self.headers['Content-Length'])
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
FROM python:3.8.3-alpine3.11

RUN mkdir /code
WORKDIR /code
COPY server.py server.py

EXPOSE 80

ENTRYPOINT [ "/code/server.py", "--listen", "0.0.0.0", "--port", "80" ]
```

Once we have the docker file we must build it and publish it to some [Docker Registry](https://docs.docker.com/registry/), so our Kubernetes cluster can pull this image later.
In this example, we will use [DockerHub](https://hub.docker.com/) Docker Registry.

```console
$ docker build -t user/md5_server:v1 .
$ docker push user/md5_server:v1
```

The next step would be to create a pod spec that would be run on Kubernetes (`spec.yaml`):

```yaml
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
      image: user/md5_server:v1
      ports:
        - name: default
          containerPort: 80
      command: ['/code/server.py', '--listen', '0.0.0.0', '--port', '80']
```

**Important**: the server listens on the same port as specified in `ports.containerPort`.
This is required, as a target needs to know precise address of the ETL container.

Another note is that we pass additional parameters via the `annotations` field.
We specified the communication type and wait time (for the pod to start).

After all these steps we are ready to start the ETL containers.
Just before we do that let's take a quick look at how our pods look like:

```console
$ kubectl get pods
NAME                 READY   STATUS    RESTARTS   AGE
ais-proxy-2wvhp      1/1     Running   0          48m
ais-target-9knsb     1/1     Running   0          46m
ais-target-fsxhp     1/1     Running   0          46m
```

We can see that the cluster is running with 1 proxy and 2 targets.
After we initialize the ETL container, we expect two more pods to start (`#targets == #etl_containers`).

```console
$ ais transform init spec.yaml
JGHEoo89gg
$ kubectl get pods
NAME                                  READY   STATUS    RESTARTS   AGE
ais-proxy-2wvhp                       1/1     Running   0          48m
ais-target-9knsb                      1/1     Running   0          46m
ais-target-fsxhp                      1/1     Running   0          46m
transformer-md5-fgjk3-node1           1/1     Running   0          1m
transformer-md5-vspra-node2           1/1     Running   0          1m
```

As expected, two more pods are up and running one for each target.

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

## API Reference

This section describes how to interact with ETLs via RESTful API.
Alternatively, you can use [ETL CLI](/cmd/cli/resources/etl.md) or [AIS Loader](/bench/aisloader/README.md).

> `G` - denotes a (hostname:port) address of a **gateway** (any gateway in a given AIS cluster)


| Operation | Description | HTTP action | Example |
|--- | --- | --- | ---|
| Init ETL | Inits ETL based on `spec.yaml`. Returns `ETL_ID` | POST /v1/etl/init | `curl -X POST 'http://G/v1/etl/init' -T spec.yaml` |
| List ETLs | Lists all running ETLs | GET /v1/etl/list | `curl -L -X GET 'http://G/v1/etl/list'` |
| Transform object | Transforms an object based on ETL with `ETL_ID` | GET /v1/objects/<bucket>/<objname>?uuid=ETL_ID | `curl -L -X GET 'http://G/v1/objects/shards/shard01.tar?uuid=ETL_ID' -o transformed_shard01.tar` |
| Transform bucket | Transforms all objects in a bucket and puts them to destination bucket | POST {"action": "etlbck"} /v1/buckets/from-name | `curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "etlbck", "name": "to-name", "value":{"ext":"destext", "prefix":"prefix", "suffix": "suffix"}}' 'http://G/v1/buckets/from-name'` |
| Stop ETL | Stops ETL with given `ETL_ID` | DELETE /v1/etl/stop/ETL_ID | `curl -X DELETE 'http://G/v1/etl/stop/ETL_ID'` |