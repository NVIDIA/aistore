## Table of Contents

- [Introduction](#introduction)
- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Examples](#examples)
  - [MD5 server](#compute-md5-on-the-objects)


## Introduction

Transformations are integrated [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) into the AIStore.
They allow to run any transformation on the object.
The transformations are designed to work close to data to minimize data transfer and latency.

## Overview

Transformations are design for AIStore cluster running in Kubernetes.
Transformation is expected to be a [Kubernetes Pod](https://kubernetes.io/docs/concepts/workloads/pods/pod/) which has a server inside it.
When the targets receive initialization requests they start transformation servers which are collocated on the same machine/node as each of the targets.
Targets use `kubectl` to initialize the pod and gather necessary information for future communication.

There are 3 communication types that user can choose from to implement the transform server:
 - **post** - target issues a POST request to its transform server with the body containing the requested object.
 After finished request, target forwards the response from the transformation server to the user.
 - **push-pull** - target uses a [reverse proxy](https://en.wikipedia.org/wiki/Reverse_proxy) to send (GET) request to cluster using transformer server.
 - **redirect** - target uses [HTTP redirect](https://developer.mozilla.org/en-US/docs/Web/HTTP/Redirections) to send (GET) request to cluster using transformer server.

The communication type must be specified in the pod specification, so the target can know how to communicate with the transformer server.

## Prerequisites

There are a couple of steps that are required to make the transformation work:
1. Target should be able to execute `kubectl` meaning that the binary should be in the `$PATH`.
2. `AIS_NODE_NAME = spec.nodeName` variable must be set inside the target pod's container (see more [here](https://kubernetes.io/docs/tasks/inject-data-application/environment-variable-expose-pod-information/)).
    Example:
    ```yaml
      containers:
      - env:
         - name: AIS_NODE_NAME
           valueFrom:
             fieldRef:
               fieldPath: spec.nodeName
    ```
   This will allow the target to assign a transformer to the same machine/node that the target is working on.
3. Server inside the pod can listen on any port, but the port must be specified in pod spec with `containerPort` - the cluster must know how it can contact the pod.

## Examples

Throughout the examples, we assume that 1. and 2. from [prerequisites](#prerequisites) are fulfilled.

### Compute MD5 on the objects

To showcase the capabilities of the transformations we will go over a simple transformation that computes MD5 checksum of the object.

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

After all these steps we are ready to start the transformer.
Just before we do that let's take a quick look at how our pods look like:

```console
$ kubectl get pods
NAME                 READY   STATUS    RESTARTS   AGE
ais-proxy-2wvhp      1/1     Running   0          46m
ais-target-9knsb     1/1     Running   0          43m
ais-target-fsxhp     1/1     Running   0          48m
```

So we see that we have 1 proxy and 2 targets.
After we initialize the transformation we expect that we will see two more pods (`#targets == #pods`).

```console
$ ais transformation init -f spec.yaml
JGHEoo89gg
$ kubectl get pods
NAME                                  READY   STATUS    RESTARTS   AGE
ais-proxy-2wvhp                       1/1     Running   0          46m
ais-target-9knsb                      1/1     Running   0          43m
ais-target-fsxhp                      1/1     Running   0          48m
transformer-md5-fgjk3-node1           1/1     Running   0          1m
transformer-md5-vspra-node2           1/1     Running   0          1m
```

As expected two more pods are running one for each target.

> **Note:** transformers will be run on the same node as the targets that started them.
This means that the transformers are close to data minimizing the latency on data transfer.

Finally, we can use newly created pods to transform the objects for us:

```console
$ ais create bucket transform
$ echo "some text :)" | ais put - transform/shard.in
$ ais transformation object JGHEoo89gg transform/shard.in 
393c6706efb128fbc442d3f7d084a426
```

Voila! The transformer successfully computed the `md5` on the `transform/shard.in` object.
