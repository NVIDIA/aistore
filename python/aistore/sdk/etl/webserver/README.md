# AIStore ETL Webserver SDK

Build custom ETL (Extract-Transform-Load) containers in minutes by extending one of our plug-and-play Python webserver frameworks. Focus on *your* transformation logic—our SDK handles all HTTP, WebSocket, concurrency, and direct-put plumbing.

---

## Install

```bash
pip install aistore[etl]
```

---

## Why use the ETL Webserver SDK?

* **Fast startup & heavy init**
  Load models or prepare resources once in `__init__()`, not on every request.
* **Rich runtimes & binaries**
  Package native tools (e.g. FFmpeg) in your Docker image—no PyPI-only restriction.
* **Modular code**
  Split logic into multiple methods and modules, not a single `transform()` blob.
* **Multiple server choices**
  Pick from multi-threaded HTTP, Flask, or FastAPI (with async & WebSocket support).
* **Direct-put optimization**
  For bucket-to-bucket jobs, send transformed objects straight to the target node (3 - 5× speedup).

---

## When *not* to use INIT code

The “init code” path is great for tiny one-off functions, but:

* Lacks an init hook for heavy setup
* Can’t bundle system binaries or native libs
* Forces a single-function style
* Limited runtimes & no WebSocket support

For anything beyond the simplest workloads, our ETL Webserver SDK is a better fit.

---

## Quickstart

1. **Extend a server**
   Pick your favorite base and override `transform(data: bytes, path: str, etl_args: str) -> bytes`.

    ```python
    # echo_server.py
    from aistore.sdk.etl.webserver.fastapi_server import FastAPIServer

    class EchoServerFastAPI(FastAPIServer):
        def transform(self, data, *_):
            return data

    # Create the server instance and expose the FastAPI app
    fastapi_server = EchoServerFastAPI(port=8000)
    fastapi_app = fastapi_server.app
    ```

2. **Containerize**

    ```dockerfile
    # Dockerfile
    FROM python:3.13-alpine
    RUN pip install --upgrade aistore[etl]>=1.13.6
    WORKDIR /app
    COPY echo_server.py ./
    ENV PYTHONUNBUFFERED=1
    EXPOSE 8000
    ```

    ```bash
    docker build -t <myrepo>/echo-etl:latest .
    docker push <myrepo>/echo-etl:latest
    ```

3. **Deploy in Kubernetes**

    ```yaml
    # init_spec.yaml
    apiVersion: v1
    kind: Pod
    metadata:
    name: etl-echo
    annotations:
      communication_type: "hpush://"
      wait_timeout: "5m"
    spec:
    containers:
      - name: server
        image: <myrepo>/echo-etl:latest
        ports: [{ name: default, containerPort: 8000 }]
        command: ["uvicorn", "echo_server.py:fastapi_app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
        readinessProbe:
          httpGet: { path: /health, port: default }
    ```

4. **Initialize & run**

    ```bash
    ais etl init spec --name my-echo --from-file init_spec.yaml
    ais etl bucket my-echo ais://<src-bucket> ais://<dst-bucket>
    ```

5. **Optional:** Running other frameworks

   * **Flask-based**

     ```python
     # echo_server.py
     from aistore.sdk.etl.webserver.flask_server import FlaskServer

     class EchoServerFlask(FlaskServer):
         def transform(self, data, *_args):
             return data

     flask_server = EchoServerFlask(port=8000)
     flask_app = flask_server.app
     ```

     In your `init_spec.yaml`:

     ```yaml
     command: ["gunicorn", "echo_server:flask_app", "--bind", "0.0.0.0:8000", "--workers", "4", "--log-level", "debug"]
     ```
   * **HTTP-based**

     ```python
     # echo_server.py
     from aistore.sdk.etl.webserver.http_multi_threaded_server import HTTPMultiThreadedServer

     class EchoServer(HTTPMultiThreadedServer):
         def transform(self, data, *_):
             return data

     if __name__ == "__main__":
         EchoServer(port=8000).start()
     ```

     In your `init_spec.yaml`:

     ```yaml
     command: ["python", "echo_server.py"]
     ```
  > **Note:**
  > To switch to a different webserver type, rebuild your container image and re-initialize the ETL with the new configuration.
---

## Examples

* **Echo**
  HTTP, Flask & FastAPI variants that return input data verbatim.
  [Source](https://github.com/NVIDIA/ais-etl/tree/main/transformers/echo)
* **Hello World**
  Returns `b"Hello World!"` on every request.
  [Source](https://github.com/NVIDIA/ais-etl/tree/main/transformers/hello_world)
* **MD5**
  Computes the MD5 checksum of each object’s bytes.
  [Source](https://github.com/NVIDIA/ais-etl/tree/main/transformers/md5)
* **FFmpeg**
  Converts audio files to WAV with configurable sample rate & channels.
  [Source](https://github.com/NVIDIA/ais-etl/tree/main/transformers/FFmpeg)
* **Audio Splitter**
  Slices audio by `from_time`/`to_time` (JSONL via `etl_args`) and bundles segments into a tar.
  [Source](https://github.com/NVIDIA/ais-etl/tree/main/transformers/NeMo/audio_split_consolidate)

---

## Performance

In our benchmarks:

* **FastAPI** (async + WebSocket) is fastest (3–4×) speedup for small objects or FQN mode.
* **Flask** (Gunicorn) is nearly as fast.
* **HTTPMultiThreadedServer** is solid for simple use cases.

You control concurrency via the `--workers` or `--threads` flags in your container entrypoint.

---

Happy transforming! 🎉
