## AIS Python SDK

AIS Python SDK provides a (growing) set of client-side APIs to access and utilize AIS clusters, buckets, and objects.

The project is, essentially, a Python port of the [AIS Go APIs](https://aistore.nvidia.com/docs/http-api), with additional objectives that prioritize *utmost convenience for Python developers*.

The SDK also includes the `authn` sub-package for managing authentication, users, roles, clusters, and tokens. For more details, refer to the [AuthN sub-package README](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/authn/README.md).

> Support is currently limited to Python 3.x, with a minimum requirement of version 3.8 or later.

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

In order to interact with your running AIS instance, you will need to create a `client` object:

```python
from aistore.sdk import Client

client = Client("http://localhost:8080")
```

> Note: `http://localhost:8080` address (above and elsewhere) must be understood as a placeholder for an _arbitrary_ AIStore endpoint (`AIS_ENDPOINT`).

The newly created `client` object can be used to interact with your AIS cluster, buckets, and objects. 
See the [examples](https://github.com/NVIDIA/aistore/blob/main/python/examples/sdk) and the [reference docs](https://aistore.nvidia.com/docs/python-sdk) for more details

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
client.bucket("my-azure-bucket", provider="azure").object("filename.ext").get()
```

```python
# List objects in AWS bucket
client.bucket("my-aws-bucket", provider="aws").list_objects()
```

Please note that certain operations do **not** support external cloud storage buckets. Please refer to the [SDK reference documentation](https://aistore.nvidia.com/docs/python_sdk.md) for more information on which bucket/object operations support remote cloud buckets, as well as general information on class and method usage.

---
### HTTPS

The SDK supports HTTPS connectivity if the AIS cluster is configured to use HTTPS. To start using HTTPS:

1. Set up HTTPS on your cluster: [Guide for K8s cluster](https://github.com/NVIDIA/ais-k8s/blob/main/playbooks/ais-deployment/docs/ais_https_configuration.md)
2. If using a self-signed certificate with your own CA, copy the CA certificate to your local machine. If using our built-in cert-manager config to generate your certificates, you can use [our playbook](https://github.com/NVIDIA/ais-k8s/blob/main/playbooks/ais-deployment/docs/generate_https_cert.md)
3. Options to configure the SDK for HTTPS connectivity:
    - Skip verification (for testing, insecure):
      - `client = Client(skip_verify=True)`
   - Point the SDK to use your certificate using one of the below methods:
     - Pass an argument to the path of the certificate when creating the client:
        - `client = Client(ca_cert=/path/to/cert)`
     - Use the environment variable
       - Set `AIS_CLIENT_CA` to the path of your certificate before initializing the client
    - If your AIS cluster is using a certificate signed by a trusted CA, the client will default to using verification without needing to provide a CA cert.
4. Options to configure the SDK to work with mTLS:
   - Pass a tuple argument containing path to client certificate and key pair
      - `client = Client(client_cert=('client.crt', 'client.key'))
   - Pass a path to a PEM file that contains both client certificate and key
      - `client = Client(client_cert='client.pem')
   - Use the environment variable
      - Set 'AIS_CRT' and 'AIS_CRT_KEY' to the path of client certificate and key respectively before initializing the client
---

### ETLs

AIStore also supports [ETLs](https://aistore.nvidia.com/docs/etl), short for Extract-Transform-Load. ETLs with AIS are beneficial given that the transformations occur *locally*, which largely contributes to the linear scalability of AIS.

> Note: AIS-ETL requires [Kubernetes](https://kubernetes.io/). For more information on deploying AIStore with Kubernetes (or Minikube), refer [here](https://github.com/NVIDIA/aistore/blob/main/deploy/dev/k8s/README.md).

To learn more about working with AIS ETL, check out [examples](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/etl).

---

### API Documentation

|Module|Summary|
|--|--|
|[client.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/client.py)|Contains `Client` class, which has methods for making HTTP requests to an AIStore server. Includes factory constructors for `Bucket`, `Cluster`, and `Job` classes.|
|[cluster.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/cluster.py)|Contains `Cluster` class that represents a cluster bound to a client and contains all cluster-related operations, including checking the cluster's health and retrieving vital cluster information.|
|[bucket.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/bucket.py)|Contains `Bucket` class that represents a bucket in an AIS cluster and contains all bucket-related operations, including (but not limited to) creating, deleting, evicting, renaming, copying.|
|[object.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/obj/object.py)|Contains class `Object` that represents an object belonging to a bucket in an AIS cluster, and contains all object-related operations, including (but not limited to) retreiving, adding and deleting objects.|
|[object_group.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/multiobj/object_group.py)|Contains class `ObjectGroup`, representing a collection of objects belonging to a bucket in an AIS cluster. Includes all multi-object operations such as deleting, evicting, prefetching, copying, and transforming objects.|
|[job.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/job.py)|Contains class `Job` and all job-related operations.|
|[dsort/core.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/dsort/core.py)|Contains class `Dsort` and all dsort-related operations.|
|[etl.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/etl/etl.py)|Contains class `Etl` and all ETL-related operations.|

For more information on SDK usage, refer to the [SDK reference documentation](https://aistore.nvidia.com/docs/python_sdk.md) or see the examples [here](https://github.com/NVIDIA/aistore/blob/main/python/examples/sdk/).


## References

* [AIStore GitHub](https://github.com/NVIDIA/aistore)
* [Documentation](https://aistore.nvidia.com/docs)
* [AIStore pip package](https://pypi.org/project/aistore/)
* [Videos and demos](https://github.com/NVIDIA/aistore/blob/main/docs/videos.md)
