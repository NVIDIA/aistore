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
Each specific ETL is defined by its specification - a regular Kubernetes YAML (see examples below).

* To start distributed ETL processing, a user either:
  1. needs to send transform function in [**build** request](#build-request) to the AIStore endpoint, or
  2. needs to send documented [**init** request](#init-request) to the AIStore endpoint.

     >  The request carries YAML spec and ultimately triggers creating [Kubernetes Pods](https://kubernetes.io/docs/concepts/workloads/pods/pod/) that run the user's ETL logic inside.

* Upon receiving **build**/**init**, AIS proxy broadcasts the request to all AIS targets in the cluster.

* When a target receives **build**/**init**, it starts the container locally on the same (the target's) machine.

* Targets use `kubectl` to initialize the pods and gather necessary information for future runtime.

## Prerequisites

There are a couple of steps that are required to make the ETL work:
1. Target should be able to execute `kubectl` meaning the binary should be in the `$PATH`.
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

## `build` request

Build simplifies the flow of creating an ETL.
It is recommended to try the ETL feature and for simple transform functions.
If you are familiar with [FasS](https://en.wikipedia.org/wiki/Function_as_a_service) then you probably will find this type of starting ETL most intuitive.

This request requires writing the `transform` function (see example below) that takes input object bytes as a parameter and must return output bytes (the content of the transformed object).
It is also possible to install the required dependencies if the function uses non-standard packages/libraries.

### Runtimes

Runtimes determine in which languages it is possible to write the `transform` function.
Currently, these runtimes are supported:

| Name | Description |
| --- | --- |
| `python2` | `python:2.7.18` is used to run the code. |
| `python3` | `python:3.8.3` is used to run the code. |

Since the number of runtimes is limited we recommend using [`init` request](#init-request) when you have bigger needs.

## `init` request

Init covers all, even wildest, cases.
It allows for running any Docker image that implements certain requirements that allow communication with the cluster.
It also requires writing pod specification to make sure the targets will know how to start it.

### Requirements

Additionally to the [prerequisites](#prerequisites), several requirements need to be fulfilled by the Docker image that is started:
1. It needs to start a server that supports at least one of [communication mechanisms](#communication-mechanisms).
2. The server inside the Docker image can listen on any port, but the port must be specified in pod spec with `containerPort` - the cluster must know how to contact the pod.
3. Target(s) may send requests in parallel to a given server - therefore, any synchronization must be done on the server-side.

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
    FROM python:3.8.3-alpine3.11
    
    RUN mkdir /code
    WORKDIR /code
    COPY server.py server.py
    
    EXPOSE 80
    
    ENTRYPOINT [ "/code/server.py", "--listen", "0.0.0.0", "--port", "80" ]
    ```
    
    Once we have the docker file we must build it and publish it to some [Docker Registry](https://docs.docker.com/registry/), so our Kubernetes cluster can pull this image later.
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
