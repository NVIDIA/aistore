## Table of Contents

- [Introduction](#introduction)
- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Examples](#examples)
  - [MD5 server](#compute-md5-on-the-objects)


## Introduction

[ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) is the general procedure of copying data from one or more sources into a destination system which represents the data differently from the source(s) or in a different context than the source(s).
AIStore supports ETL and is designed to work close to data to minimize data transfer and latency.

## Overview

ETL is designed for AIStore cluster running in Kubernetes. Every ETL is specified by a spec file (Kubernetes YAML spec), which determines what
and how the ETL should operate. Every such spec file creates an **ETL container** which is a [Kubernetes Pod](https://kubernetes.io/docs/concepts/workloads/pods/pod/) running a server inside it.
To start an ETL the user (client) needs to send an **init** request to the AIStore endpoint (proxy). The proxy broadcasts this request to all
the registered targets.
When the targets receive init requests they start the ETL container which are collocated on the same machine/node as each of the targets.
Targets use `kubectl` to initialize the pod and gather necessary information for future communication.

There are 3 communication types that user can choose from to implement the transform server:

| Name | Value | Description |
|---|---|---|
| **post** | `hpush://` | Target issues a POST request to its transform server with the body containing the requested object. After finished request, target forwards the response from the ETL container to the user. |
| **reverse proxy** | `hrev://` | Target uses a [reverse proxy](https://en.wikipedia.org/wiki/Reverse_proxy) to send (GET) request to cluster using ETL container. ETL container should make GET request to a target, transform bytes, and return them to a target. |
| **redirect** | `hpull://` | Target uses [HTTP redirect](https://developer.mozilla.org/en-US/docs/Web/HTTP/Redirections) to send (GET) request to cluster using ETL container. ETL container should make GET request to a target, transform bytes, and return them to a user. |

The communication type must be specified in the pod specification, so the target can know how to communicate with the ETL container.
It is defined in as pod's annotation, under `communication_type` key:
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
If the timeout is exceeded, initialization of the ETL container is considered failed.
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: etl-container-name
  annotations:
    wait_timeout: 30s
(...)
```

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
3. Server inside the pod can listen on any port, but the port must be specified in pod spec with `containerPort` - the cluster must know how it can contact the pod.

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

Once we have a server that computes the MD5 we need to create an image out of it.
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
In this example we will use [quay.io](https://quay.io/) Docker Registry.

```console
$ docker build -t quay.io/user/md5_server:v1 .
$ docker push quay.io/user/md5_server:v1
```

The next step would be to create a pod spec that would be run on Kuberenetes (`spec.yaml`):

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
      image: quay.io/user/md5_server:v1
      ports:
        - containerPort: 80
      command: ['/code/server.py', '--listen', '0.0.0.0', '--port', '80']
```

The important note here is that the server listens on port `80` that is also specified in `ports.containerPort`.
This is extremely important so that target can know what port it must contact the transformation pod.

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

So we see that we have 1 proxy and 2 targets.
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

As expected two more pods are up and running one for each target.

> **Note:** ETL containers will be run on the same node as the targets that started them.
In other words, each ETL container runs close to data and does not generate any extract-transform-load
related network traffic. Given that there are as many ETL containers as storage nodes
(one container per target) and that all ETL containers run in parallel, the cumulative "transformation"
bandwidth scales proportionally to the number of storage nodes and disks.

Finally, we can use newly created pods to transform the objects for us:

```console
$ ais create bucket transform
$ echo "some text :)" | ais put - transform/shard.in
$ ais etl object JGHEoo89gg transform/shard.in -
393c6706efb128fbc442d3f7d084a426
```

Voila! The ETL container successfully computed the `md5` on the `transform/shard.in` object.

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
