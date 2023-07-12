## AIS Python SDK

AIS Python SDK provides a (growing) set of client-side APIs to access and utilize AIS clusters, buckets, and objects.

The project is, essentially, a Python port of the [AIS Go APIs](https://aiatscale.org/docs/http-api), with additional objectives that prioritize *utmost convenience for Python developers*.

Note that only Python 3.x (version 3.6 or later) is currently supported.

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

The newly created `client` object can be used to interact with your AIS cluster, buckets, and objects. 
See the [examples](https://github.com/NVIDIA/aistore/blob/master/python/examples/sdk) and the [reference docs](https://aiatscale.org/docs/python-sdk) for more details

**External Cloud Storage Buckets**

AIS supports a number of different [backend providers](https://aiatscale.org/docs/providers) or, simply, backends.

> For exact definitions and related capabilities, please see [terminology](https://aiatscale.org//docs/overview#terminology).

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

Please note that certain operations do **not** support external cloud storage buckets. Please refer to the [SDK reference documentation](https://aiatscale.org/docs/python_sdk.md) for more information on which bucket/object operations support remote cloud buckets, as well as general information on class and method usage.

---

### ETLs

AIStore also supports [ETLs](https://aiatscale.org/docs/etl), short for Extract-Transform-Load. ETLs with AIS are beneficial given that the transformations occur *locally*, which largely contributes to the linear scalability of AIS.

> Note: AIS-ETL requires [Kubernetes](https://kubernetes.io/). For more information on deploying AIStore with Kubernetes (or Minikube), refer [here](https://github.com/NVIDIA/aistore/blob/master/deploy/dev/k8s/README.md).

Check out the [provided examples](https://github.com/NVIDIA/aistore/blob/master/python/aistore/sdk/etl_templates.py) to learn more about working with AIS ETL.

---

### API Documentation

|Module|Summary|
|--|--|
|[api.py](https://github.com/NVIDIA/aistore/blob/master/python/aistore/sdk/client.py)|Contains `Client` class, which has methods for making HTTP requests to an AIStore server. Includes factory constructors for `Bucket`, `Cluster`, and `Job` classes.|
|[cluster.py](https://github.com/NVIDIA/aistore/blob/master/python/aistore/sdk/cluster.py)|Contains `Cluster` class that represents a cluster bound to a client and contains all cluster-related operations, including checking the cluster's health and retrieving vital cluster information.|
|[bucket.py](https://github.com/NVIDIA/aistore/blob/master/python/aistore/sdk/bucket.py)|Contains `Bucket` class that represents a bucket in an AIS cluster and contains all bucket-related operations, including (but not limited to) creating, deleting, evicting, renaming, copying.|
|[object.py](https://github.com/NVIDIA/aistore/blob/master/python/aistore/sdk/object.py)|Contains class `Object` that represents an object belonging to a bucket in an AIS cluster, and contains all object-related operations, including (but not limited to) retreiving, adding and deleting objects.|
|[object_group.py](https://github.com/NVIDIA/aistore/blob/master/python/aistore/sdk/object_group.py)|Contains class `ObjectGroup`, representing a collection of objects belonging to a bucket in an AIS cluster. Includes all multi-object operations such as deleting, evicting, prefetching, copying, and transforming objects.|
|[job.py](https://github.com/NVIDIA/aistore/blob/master/python/aistore/sdk/job.py)|Contains class `Job` and all job-related operations.|
|[dsort.py](https://github.com/NVIDIA/aistore/blob/master/python/aistore/sdk/dsort.py)|Contains class `Dsort` and all dsort-related operations.|
|[etl.py](https://github.com/NVIDIA/aistore/blob/master/python/aistore/sdk/etl.py)|Contains class `Etl` and all ETL-related operations.|

For more information on SDK usage, refer to the [SDK reference documentation](https://aiatscale.org/docs/python_sdk.md) or see the examples [here](https://github.com/NVIDIA/aistore/blob/master/python/examples/sdk/).


## References

* [AIStore GitHub](https://github.com/NVIDIA/aistore)
* [Documentation](https://aiatscale.org/docs)
* [AIStore pip package](https://pypi.org/project/aistore/)
* [Videos and demos](https://github.com/NVIDIA/aistore/blob/master/docs/videos.md)
