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
    from aistore.sdk.etl.webserver.http_multi_threaded_server import HTTPMultiThreadedServer

    class EchoServer(HTTPMultiThreadedServer):
        def transform(self, data, *_):
            return data

    if __name__ == "__main__":
        EchoServer(port=8000).start()
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
        command: ["python", "echo_server.py"]
        readinessProbe:
          httpGet: { path: /health, port: default }
    ```

4. **Initialize & run**

    ```bash
    ais etl init spec --name my-echo --from-file init_spec.yaml
    ais etl bucket my-echo ais://<src-bucket> ais://<dst-bucket>
    ```

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
