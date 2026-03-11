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
* **Direct FQN mode**
  Set `ETL_DIRECT_FQN=true` to receive the local file path instead of bytes — zero memory allocation for large objects or tools like `ffmpeg` that read directly from disk.
* **Streaming transforms**
  Override `transform_stream()` instead of `transform()` for constant-memory processing — yield output chunks as they're produced instead of buffering the entire result. Ideal for workloads that build large output incrementally (e.g., multi-GB TAR archives).

---

## `ais etl init code` vs. AIStore ETL SDK Webservers

While AIStore supports `ais etl init code` for deploying simple ETL functions quickly, it comes with several limitations that make it less suitable for production-grade or performance-sensitive workloads.

The **`init code`** approach is ideal for basic, lightweight use cases, but it:

- Offers **no initialization hook** for preloading models or heavy setup
- **Cannot bundle system binaries or native libraries**
- Enforces a **single-function (`transform()`) structure**
- Has **limited runtime support** (e.g., no Go, no WebSocket support)
- Lacks support for **direct-put optimizations**
- Provides **no concurrency tuning or advanced configuration**

For anything beyond the most basic transformation logic, the SDK webserver approach is the recommended path. It lets you scale efficiently, build robust services, and focus entirely on your transformation logic while the SDK takes care of everything else.

---

## Quickstart

1. **Extend a server**

   Override **one** of the two transform methods:

   | Method | Signature | When to use |
   |--------|-----------|-------------|
   | `transform()` | `(data: bytes\|str, path, etl_args) -> bytes` | Most transforms — input buffered, output returned as bytes |
   | `transform_stream()` | `(reader: BinaryIO, path, etl_args) -> Iterator[bytes]` | Large or incremental output — constant memory, chunks yielded as produced |

   If both are overridden, `transform()` takes priority (backward compatibility). If neither is overridden, a `TypeError` is raised at init time.

   **Buffered example (default):**

    ```python
    # echo_server.py
    from aistore.sdk.etl.webserver.fastapi_server import FastAPIServer

    class EchoServerFastAPI(FastAPIServer):
        def transform(self, data, *_):
            return data

    fastapi_server = EchoServerFastAPI(port=8000)
    fastapi_app = fastapi_server.app
    ```

   **Streaming example (constant memory):**

    ```python
    # streaming_upper.py
    from aistore.sdk.etl.webserver.fastapi_server import FastAPIServer

    class UpperCaseStream(FastAPIServer):
        def transform_stream(self, reader, path, etl_args):
            """Upper-case input in 64 KB chunks — constant memory."""
            while True:
                chunk = reader.read(65536)
                if not chunk:
                    break
                yield chunk.upper()

    fastapi_server = UpperCaseStream(port=8000)
    fastapi_app = fastapi_server.app
    ```

   > **Note:** `transform_stream()` always receives a `BinaryIO` reader, even when `ETL_DIRECT_FQN=true` (the framework opens the file for you). Set `ETL_DIRECT_FQN=true` in your container to enable direct file access for `transform()` — it receives the local file path as `str` instead of bytes.

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
          command: ["uvicorn", "echo_server:fastapi_app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4", "--log-level", "info", "--ws-max-size", "17179869184", "--ws-ping-interval", "0", "--ws-ping-timeout", "86400"]
          readinessProbe:
            httpGet: { path: /health, port: default }
      ```

4. **Initialize & run**

    ```bash
    ais etl init spec --name my-echo --spec init_spec.yaml
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

## Streaming vs. Buffered Transforms

| | `transform()` (buffered) | `transform_stream()` (streaming) |
|---|---|---|
| **Input** | `bytes` or `str` (FQN path) | `BinaryIO` reader |
| **Output** | `bytes` (entire result) | `Iterator[bytes]` (chunked) |
| **Memory** | O(input + output) | O(one chunk) |
| **Best for** | Most 1:1 transforms (resize, convert, hash) | Fan-out / aggregation (JSONL -> TAR, large archive builders) |
| **WebSocket** | Supported | Not supported (use HTTP transport) |
| **Pipeline / direct-put** | Supported | Supported (uses `CountingIterator` for size tracking) |

**When to use streaming:**
- Output is significantly larger than input (e.g., small manifest -> multi-GB TAR)
- Output is composed of independent pieces that can be emitted incrementally
- Memory is constrained and you cannot afford buffering the full result

**When to use buffered (default):**
- Most transforms — the input and output are roughly the same size
- You need WebSocket transport
- The transform logic requires random access to the full input

---

## Performance

In our benchmarks:

* **FastAPI** (async + WebSocket) is fastest (3–4×) speedup for small objects or FQN mode.
* **Flask** (Gunicorn) is nearly as fast.
* **HTTPMultiThreadedServer** is solid for simple use cases.

You control concurrency via the `--workers` or `--threads` flags in your container entrypoint.

---

Happy transforming! 🎉
