## AIS Python SDK

AIS Python SDK provides a (growing) set of client-side APIs to access and utilize AIS clusters, buckets, and objects.

The project is, essentially, a Python port of the [AIS Go APIs](https://aistore.nvidia.com/docs/http_api), with additional objectives that prioritize *utmost convenience for Python developers*.

The SDK also includes the `authn` sub-package for managing authentication, users, roles, clusters, and tokens. For more details, refer to the [AuthN sub-package README](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/authn/README.md).

> Supported Python versions: 3.10 through 3.15.

---

## Installation


### Install as a Package

The latest AIS release can be easily installed either with Anaconda or `pip`:

```console
$ conda install aistore
```

```console
$ pip install aistore
```

### Install From Source

If you'd like to work with the current upstream (and don't mind the risk), install the latest master directly from GitHub:

```console
$ git clone https://github.com/NVIDIA/aistore.git

$ cd aistore/python/

# upgrade pip to latest version
$ python -m pip install --upgrade pip

# install dependencies
$ pip install -r aistore/common_requirements

$ pip install -e .
```
---

## Quick Start

For an end-to-end create, upload, list, and download example, follow the [Python first-object tutorial](/docs/python_sdk.md). Start a cluster using the maintained [local playground](/docs/getting_started.md#local-playground), or use [Minimal Compose](/docs/getting_started.md#minimal-compose-deployment) for a limited one-proxy, one-target container smoke test with no TLS or AuthN. Compose is not a production deployment. For Kubernetes, [choose a development or production path](/docs/kubernetes.md); for production authentication, refer to [authentication validation](/docs/auth_validation.md).

In order to interact with your running AIS instance, you will need to create a `client` object:

```python
from aistore.sdk import Client

client = Client("http://localhost:8080")
```

For initialization and cleanup, see [Client Lifecycle](#client-lifecycle).

> Note: `http://localhost:8080` address (above and elsewhere) must be understood as a placeholder for an _arbitrary_ AIStore endpoint (`AIS_ENDPOINT`).

The newly created `client` object can be used to interact with your AIS cluster, buckets, and objects. 
See the [examples](https://github.com/NVIDIA/aistore/blob/main/python/examples/sdk) and the [reference docs](https://docs.nvidia.com/aistore/python/aistore/sdk) for more details

**External Cloud Storage Buckets**

AIS supports a number of different [backend providers](https://aistore.nvidia.com/docs/providers) or, simply, backends.

> For exact definitions and related capabilities, please see [terminology](https://aistore.nvidia.com//docs/overview#terminology).

Many bucket/object operations support remote cloud buckets (third-party backend-based cloud buckets), including a few of the operations shown above. To interact with remote cloud buckets, you need to *specify the provider* of choice when instantiating your bucket object as follows:

```python
# Head AWS bucket
client.bucket("my-aws-bucket", provider="aws").head()
```

```python
# Evict GCP bucket
client.bucket("my-gcp-bucket", provider="gcp").evict()
```

```python
# Get object from Azure bucket
client.bucket("my-azure-bucket", provider="azure").object("filename.ext").get_reader()
```

```python
# List objects in AWS bucket
client.bucket("my-aws-bucket", provider="aws").list_objects()
```

Please note that certain operations do **not** support external cloud storage buckets. Please refer to the [SDK reference documentation](https://docs.nvidia.com/aistore/python/aistore/sdk) for more information on which bucket/object operations support remote cloud buckets, as well as general information on class and method usage.

---

## Client Lifecycle

> These examples require Python SDK 2.3.0 or newer for lifecycle management support.

### Explicit Initialization and Cleanup

Creating a `Client` makes no network requests. The optional `initialize()` method
fetches and caches cluster information, such as the AIS version, for compatibility
and feature decisions. You can make SDK requests without calling it first.

Call `close()` when finished to release HTTP connections and prevent further
requests. The examples below read an existing object from `my-bucket`.

```python
from aistore.sdk import Client

client = Client("http://localhost:8080")
try:
    client.initialize()  # Optional, fetches state from cluster
    obj = client.bucket("my-bucket").object("hello.txt")
    content = obj.get_reader().read_all()
finally:
    client.close()
```

Repeated calls to `initialize()` do not refetch cached state. Repeated calls to
`close()` do nothing after the first call. After closing, `client.closed` is `True`.
Requests through the client or its bucket and object handles raise
`ClientClosedError`, a subclass of `RuntimeError`. Closing does not wait for active
requests.

### Context Manager

If you prefer not to manage the lifecycle yourself, use `with` to initialize the
client on entry and close it on exit:

```python
from aistore.sdk import Client

with Client("http://localhost:8080") as client:
    obj = client.bucket("my-bucket").object("hello.txt")
    content = obj.get_reader().read_all()
```

Note that initialization is always run when using a context manager.

---

## Environment Variables

The AIS Python SDK supports several environment variables that allow you to configure client behavior without explicitly passing parameters to the `Client` constructor. This is particularly useful for containerized environments or when you want to centralize configuration.

### SDK Client Environment Variables

| Environment Variable | Description | Default Value |
|---------------------|-------------|---------------|
| `AIS_AUTHN_TOKEN` | Authentication token for accessing the AIS cluster | None |
| `AIS_SKIP_VERIFY_CRT` | Skip TLS certificate verification for `1`, `t`, `true`, `y`, `yes`, or `on` (case-insensitive) | `false` |
| `AIS_SKIP_VERIFY` | Legacy fallback when `AIS_SKIP_VERIFY_CRT` is unset | `false` |
| `AIS_CLIENT_CA` | Path to CA certificate file for SSL verification | None |
| `AIS_CRT` | Path to client certificate file for mTLS authentication | None |
| `AIS_CRT_KEY` | Path to client certificate key file for mTLS authentication | None |
| `AIS_CONNECT_TIMEOUT` | Connection timeout in seconds (set to `0` to disable) | `3` |
| `AIS_READ_TIMEOUT` | Read timeout in seconds (set to `0` to disable) | `20` |
| `AIS_MAX_CONN_POOL` | Maximum number of connections per host in the connection pool | `10` |

### ETL Webserver Environment Variables

ETL webservers support all SDK client environment variables above (same semantics). In addition, the following are specific to ETL webservers:

| Environment Variable | Description | Default Value |
|---------------------|-------------|---------------|
| `AIS_DIRECT_PUT_CHUNK_SIZE` | Chunk size in bytes for streaming direct-put bodies (`FastAPIServer` only) | `1048576` (1 MiB) |
| `AIS_DIRECT_PUT_RETRIES` | Retries on transient connection errors during direct-put (all ETL servers) | `3` |
| `MAX_CONN` | Maximum total outbound connections in the httpx pool (`FastAPIServer` only) | `256` |
| `MAX_KEEPALIVE_CONN` | Maximum keepalive connections in the httpx pool (`FastAPIServer` only) | `128` |
| `KEEPALIVE_EXPIRY` | Keepalive connection expiry in seconds (`FastAPIServer` only) | `30` |

### Configuration Precedence

When the `Client` is constructed, configuration values are resolved in the following order (highest to lowest precedence):

1. **Explicit parameters** passed to the `Client()` constructor
2. **Environment variables** (listed above)
3. **Default values** (built-in defaults)

Timeout and connection-pool parameters follow this order. TLS and token settings use their documented fallback rules. In particular, `skip_verify=False` still checks the skip-verification environment variable; it does not force verification when that variable is enabled.

### Examples

#### Using Environment Variables

```python
import os
from aistore.sdk import Client

# Set environment variables
os.environ["AIS_AUTHN_TOKEN"] = "your-auth-token"
os.environ["AIS_CONNECT_TIMEOUT"] = "5"
os.environ["AIS_READ_TIMEOUT"] = "30"
os.environ["AIS_MAX_CONN_POOL"] = "20"
os.environ["AIS_CLIENT_CA"] = "/path/to/client_ca.crt"

# Client will use the environment variables
client = Client("https://localhost:8080")
```
---
### HTTPS

The SDK supports HTTPS connectivity if the AIS cluster is configured to use HTTPS. To start using HTTPS:

1. Set up HTTPS on your cluster: [Guide for K8s cluster](https://github.com/NVIDIA/ais-k8s/blob/main/playbooks/ais-deployment/docs/ais_https_configuration.md)
2. If using a self-signed certificate with your own CA, copy the CA certificate to your local machine. If using our built-in cert-manager config to generate your certificates, you can use [our playbook](https://github.com/NVIDIA/ais-k8s/blob/main/playbooks/ais-deployment/docs/generate_https_cert.md)
3. Options to configure the SDK for HTTPS connectivity:
    - Skip verification (for testing, insecure):
      - `client = Client("https://localhost:8080", skip_verify=True)`
   - Point the SDK to use your certificate using one of the below methods:
     - Pass an argument to the path of the certificate when creating the client:
        - `client = Client("https://localhost:8080", ca_cert="/path/to/cert")`
     - Use the environment variable
       - Set `AIS_CLIENT_CA` to the path of your certificate before initializing the client
    - If your AIS cluster is using a certificate signed by a trusted CA, the client will default to using verification without needing to provide a CA cert.
4. Options to configure the SDK to work with mTLS:
   - Pass a tuple argument containing path to client certificate and key pair
      - `client = Client("https://localhost:8080", client_cert=('client.crt', 'client.key'))`
   - Pass a path to a PEM file that contains both client certificate and key
      - `client = Client("https://localhost:8080", client_cert='client.pem')`
   - Use the environment variable
      - Set 'AIS_CRT' and 'AIS_CRT_KEY' to the path of client certificate and key respectively before initializing the client
---

### ETLs

AIStore also supports [ETLs](https://aistore.nvidia.com/docs/etl), short for Extract-Transform-Load. ETLs with AIS are beneficial given that the transformations occur *locally*, which largely contributes to the linear scalability of AIS.

> Note: AIS-ETL requires [Kubernetes](https://kubernetes.io/) and an ETL-enabled `aisnode` image or build. See the [ETL deployment prerequisites](https://aistore.nvidia.com/docs/etl#quick-start).

To learn more about working with AIS ETL, check out [examples](https://github.com/NVIDIA/aistore/tree/main/python/examples/ais-etl).

---

### API Documentation

|Module|Summary|
|--|--|
|[client.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/client.py)|Contains `Client` class, which has methods for making HTTP requests to an AIStore server. Includes factory constructors for `Bucket`, `Cluster`, and `Job` classes.|
|[cluster.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/cluster.py)|Contains `Cluster` class that represents a cluster bound to a client and contains all cluster-related operations, including checking the cluster's health and retrieving vital cluster information.|
|[bucket.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/bucket.py)|Contains `Bucket` class that represents a bucket in an AIS cluster and contains all bucket-related operations, including (but not limited to) creating, deleting, evicting, renaming, copying.|
|[object.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/obj/object.py)|Contains class `Object` that represents an object belonging to a bucket in an AIS cluster, and contains all object-related operations, including (but not limited to) retrieving, adding and deleting objects.|
|[object_group.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/multiobj/object_group.py)|Contains class `ObjectGroup`, representing a collection of objects belonging to a bucket in an AIS cluster. Includes all multi-object operations such as deleting, evicting, prefetching, copying, and transforming objects.|
|[job.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/job.py)|Contains class `Job` and all job-related operations.|
|[dsort/core.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/dsort/core.py)|**Deprecated.** Contains class `Dsort` and dsort-related operations for AIStore before 5.3.|
|[etl.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/etl/etl.py)|Contains class `Etl` and all ETL-related operations.|

For more information on SDK usage, refer to the [SDK reference documentation](https://docs.nvidia.com/aistore/python/aistore/sdk) or see the examples [here](https://github.com/NVIDIA/aistore/blob/main/python/examples/sdk/).

---

## Threading

`Client`, `AuthNClient`, and `SessionManager` do not support concurrent use across threads. Use a separate client per thread, since bucket and object handles and internal `RequestClient` clones share the original client's session.

## Multiprocessing

Create a client in each process, or pass an open client and its handles using `fork`, `spawn`, or `forkserver`, with a separate session created from SDK configuration on first use in each child. For process setup, see Python's [multiprocessing documentation](https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods).

For PyTorch DataLoader support and an example, see [PyTorch DataLoader](../pytorch/README.md#pytorch-dataloader).

### Considerations

- Forking a process with background threads is unsupported. Use `spawn` or `forkserver` instead.
- Passing a client to `spawn` or `forkserver` workers resets a custom `RetryConfig.network_retry` policy to the SDK default. Create and configure the client inside each worker to use a custom network retry policy.
- Sharing readers, writers, streaming responses, or iterators across threads or processes is unsupported. Finish requests and close streams, iterators, and file-like writers before closing the client.
- Parallel reads (`Object.get_reader(num_workers=N)`) require fork support, always use `fork`, and are unsupported inside DataLoader workers. Close the returned `ParallelBuffer` or iterator when finished, including on early exit.

## References

* [AIStore GitHub](https://github.com/NVIDIA/aistore)
* [Documentation](https://aistore.nvidia.com/docs)
* [AIStore pip package](https://pypi.org/project/aistore/)
* [Blog](https://aistore.nvidia.com/blog)
* [KubeCon 2024 presentation](https://www.youtube.com/watch?v=N-d9cbROndg)
