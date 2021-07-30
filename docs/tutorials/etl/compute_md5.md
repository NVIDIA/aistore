---
layout: post
title: COMPUTE_MD5
permalink: ./tutorials/etl/compute_md5
redirect_from:
 - ./tutorials/etl/compute_md5.md/
 - /docs/./tutorials/etl/compute_md5.md/
---

# Compute MD5

In this example, we will see how ETL can be used to do something as simple as computing MD5 of the object.
We will go over two ways of starting ETL to achieve our goal.
Get ready!

## Prerequisites

* AIStore cluster deployed on Kubernetes. We recommend following guide below.
  * [Deploy AIStore on local Kuberenetes cluster](https://github.com/NVIDIA/ais-k8s/blob/master/operator/README.md)
  * [Deploy AIStore on the cloud](https://github.com/NVIDIA/ais-k8s/blob/master/terraform/README.md)

## Prepare ETL

To showcase ETL's capabilities, we will go over a simple ETL container that computes the MD5 checksum of the object.
There are three ways of approaching this problem:

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
    $ ais etl init code --from-file=code.py --runtime=python3
    JGHEoo89gg
    ```

2. **Simplified flow with input/output**
   Similar to the above example, we will be using the `python3` runtime.
   However, the python code in this case expects data as standard input and writes the output bytes to standard output, as shown in the following `code.py`:

   ```python
    import hashlib
    import sys

    md5 = hashlib.md5()
    for chunk in sys.stdin.buffer.read():
        md5.update(chunk)
    sys.stdout.buffer.write(md5.hexdigest().encode())
   ```

   We can now use the CLI to build and initialize ETL with `io://` communicator type:
   ```console
   $ ais etl init code --from-file=code.py --runtime=python3 --comm-type="io://"
   QWHFsp92yp
   ```

3. **Regular flow**

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
    In this example, we will use [docker.io](https://hub.docker.com/) Docker Registry.

    ```console
    $ docker build -t docker.io/aistore/md5_server:v1 .
    $ docker push docker.io/aistore/md5_server:v1
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
          image: docker.io/aistore/md5_server:v1
          ports:
            - name: default
              containerPort: 80
          command: ['/code/server.py', '--listen', '0.0.0.0', '--port', '80']
    ```

    **Important**: the server listens on the same port as specified in `ports.containerPort`.
    It is required, as a target needs to know the precise socket address of the ETL container.

    Another note is that we pass additional parameters via the `annotations` field.
    We specified the communication type and wait time (for the Pod to start).

    Once we have our `spec.yaml`, we can initialize ETL with CLI:
    ```console
    $ ais etl init spec spec.yaml
    JGHEoo89gg
    ```

Just before we started ETL containers, our Pods looked like this:

```console
$ kubectl get pods
NAME                   READY   STATUS    RESTARTS   AGE
demo-ais-admin-99p8r   1/1     Running   0          31m
demo-ais-proxy-5vqb8   1/1     Running   0          31m
demo-ais-proxy-g7jf7   1/1     Running   0          31m
demo-ais-target-0      1/1     Running   0          31m
demo-ais-target-1      1/1     Running   0          29m
```

We can see that the cluster is running with one proxy and two targets.
After we initialized the ETL, we expect two more Pods to be started (`#targets == #etl_containers`).

```console
$ kubectl get pods
NAME                      READY   STATUS    RESTARTS   AGE
demo-ais-admin-99p8r      1/1     Running   0          41m
demo-ais-proxy-5vqb8      1/1     Running   0          41m
demo-ais-proxy-g7jf7      1/1     Running   0          41m
demo-ais-target-0         1/1     Running   0          41m
demo-ais-target-1         1/1     Running   0          39m
transformer-md5-fgjk3     1/1     Running   0          1m
transformer-md5-vspra     1/1     Running   0          1m
```

As expected, two more Pods are up and running - one for each target.

> ETL containers will be run on the same node as the targets that started them.
> In other words, each ETL container runs close to data and does not generate any extract-transform-load related network traffic.
> Given that there are as many ETL containers as storage nodes (one container per target) and that all ETL containers run in parallel, the cumulative "transformation" bandwidth scales proportionally to the number of storage nodes and disks.

Finally, we can use newly created Pods to transform the objects on the fly for us:

```console
$ ais bucket create transform
$ echo "some text :)" | ais object put - transform/shard.in
$ ais etl object JGHEoo89gg transform/shard.in -
393c6706efb128fbc442d3f7d084a426
```

Voilà! The ETL container successfully computed the `md5` on the `transform/shard.in` object.

Alternatively, one can use the offline ETL feature to transform the whole bucket.

```console
$ ais bucket create transform
$ echo "some text :)" | ais object put - transform/shard.in
$ ais etl bucket JGHEoo89gg ais://transform ais://transform-md5 --wait
```

Once ETL isn't needed anymore, the Pods can be stopped with:

```console
$ ais etl stop JGHEoo89gg
ETL containers stopped successfully.
$ kubectl get pods
NAME                      READY   STATUS    RESTARTS   AGE
demo-ais-admin-99p8r      1/1     Running   0          50m
demo-ais-proxy-5vqb8      1/1     Running   0          50m
demo-ais-proxy-g7jf7      1/1     Running   0          49m
demo-ais-target-0         1/1     Running   0          50m
demo-ais-target-1         1/1     Running   0          49m
```
