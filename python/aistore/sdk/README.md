## AIS Python SDK

AIS Python SDK provides a (growing) set of client-side APIs to access and utilize AIS clusters, buckets, and objects.

The project is, essentially, a Python port of the [AIS Go APIs](https://aiatscale.org/docs/http-api), with additional objectives that prioritize *utmost convenience for Python developers*.

Note that only Python 3.x (version 3.6 or later) is currently supported.


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

$ pip install -e .
```


## Quick Start

In order to interact with your running AIS instance, you will need to create a `client` object:

```python
from aistore.sdk import Client

client = Client("http://localhost:8080")
```

The newly created `client` object can be used to interact with your AIS cluster, buckets, and objects. 
See the [examples](https://github.com/NVIDIA/aistore/blob/master/python/aistore/examples/sdk) and the [reference docs](https://aiatscale.org/docs/python-sdk) for more details

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
# List objects in AWS bucket'
client.bucket("my-aws-bucket", provider="aws").list_objects()
```

Please note that certain operations do **not** support external cloud storage buckets. Please refer to the [SDK reference documentation](https://aiatscale.org/docs/python_sdk.md) for more information on which bucket/object operations support remote cloud buckets, as well as general information on class and method usage.


### ETLs

AIStore also supports [ETLs](https://aiatscale.org/docs/etl), short for Extract-Transform-Load. ETLs with AIS are beneficial given that the transformations occur *locally*, which largely contributes to the linear scalability of AIS.

> Note: AIS-ETL requires [Kubernetes](https://kubernetes.io/). For more information on deploying AIStore with Kubernetes (or Minikube), refer [here](https://github.com/NVIDIA/aistore/blob/master/deploy/dev/k8s/README.md).

The following example is a sample workflow involing AIS-ETL.

We can initialize ETLs with either [code](https://aiatscale.org/docs/etl#init-code-request) or [spec](https://aiatscale.org/docs/etl#init-spec-request).

We initialize an ETL w/ [code](https://github.com/NVIDIA/aistore/blob/master/docs/etl.md#init-code-request):

```python
import hashlib

# Defining ETL transformation code
def transform(input_bytes):
    md5 = hashlib.md5()
    md5.update(input_bytes)
    return md5.hexdigest().encode()

# Initializing ETL  with transform()
client.etl().init_code(transform=transform, etl_name="etl-code")
```

We initialize another ETL w/ [spec](https://github.com/NVIDIA/aistore/blob/master/docs/etl.md#init-spec-request):

```python
from aistore.sdk.etl_templates import MD5

template = MD5.format(communication_type="hpush")
client.etl().init_spec(template=template, etl_name="etl-spec")
```

> Refer to more ETL templates [here](https://github.com/NVIDIA/aistore/blob/master/python/aistore/sdk/etl_templates.py).

Once initialized, we can verify the ETLs are running with method `list()`:

```python
# List all running ETLs
client.etl().list()
```

We can get an object with the ETL transformations applied:

```python
# Get object w/ ETL code transformation
obj1 = client.bucket("bucket-demo").object("object-demo").get(etl_name="etl-code").read_all()

# Get object w/ ETL spec transformation
obj2 = client.bucket("bucket-demo").object("object-demo").get(etl_name="etl-spec").read_all()
```

Alternatively, we can transform an entire bucket's contents as follows:

```python
# Transform bucket w/ ETL code transformation
client.bucket("bucket-demo").transform(etl_name="etl-code", to_bck="bucket-transformed")

# Transform bucket w/ ETL spec transformation
client.bucket("bucket-demo").transform(etl_name="etl-spec", to_bck="bucket-transformed")
```

Transform also allows for *on-the-fly* rename operations for objects:

```python
# Add a prefix to the resulting transformed files:
client.bucket("bucket-demo").transform(etl_name="etl-code", to_bck="bucket-transformed", prefix="transformed-")

# Replace existing filename extensions
client.bucket("bucket-demo").transform(etl_name="etl-spec", to_bck="bucket-transformed", ext={"jpg":"txt"})
```

We can stop the ETLs if desired with method `stop()`:

```python
# Stop ETL
client.etl().stop(etl_name="etl-code")
client.etl().stop(etl_name="etl-spec")

# Verify ETLs are not actively running
client.etl().list()
```

If an ETL is stopped, any Kubernetes pods created for the ETL are *stopped*, but *not deleted*. Any transforms by the stopped ETL are terminated. Stopped ETLs can be resumed for use with method `start()`:

```python
# Stop ETLs
client.etl().start(etl_name="etl-code")
client.etl().start(etl_name="etl-spec")

# Verify ETLs are not actively running
client.etl().list()
```

Once completely finished with the ETLs, we cleanup (for storage) by stopping the ETLs with `stop` and substenquently deleting the ETLs with `delete`:

```python
# Stop ETLs
client.etl().stop(etl_name="etl-code")
client.etl().stop(etl_name="etl-spec")

# Delete ETLs
client.etl().delete(etl_name="etl-code")
client.etl().delete(etl_name="etl-spec")

```

Deleting an ETL deletes all pods created by Kubernetes for the ETL as well as any specifications for the ETL on Kubernetes. Consequently, deleted ETLs cannot be started again and will need to be re-initialized.

> For an interactive demo, refer [here](https://github.com/NVIDIA/aistore/blob/master/python/aistore/examples/sdk/sdk-etl-tutorial.ipynb).

### API Documentation

|Module|Summary|
|--|--|
|[api.py](https://github.com/NVIDIA/aistore/blob/master/python/aistore/sdk/client.py)|Contains `Client` class, which has methods for making HTTP requests to an AIStore server. Includes factory constructors for `Bucket`, `Cluster`, and `Job` classes.|
|[cluster.py](https://github.com/NVIDIA/aistore/blob/master/python/aistore/sdk/cluster.py)|Contains `Cluster` class that represents a cluster bound to a client and contains all cluster-related operations, including checking the cluster's health and retrieving vital cluster information.|
|[bucket.py](https://github.com/NVIDIA/aistore/blob/master/python/aistore/sdk/bucket.py)|Contains `Bucket` class that represents a bucket in an AIS cluster and contains all bucket-related operations, including (but not limited to) creating, deleting, evicting, renaming, copying.|
|[object.py](https://github.com/NVIDIA/aistore/blob/master/python/aistore/sdk/object.py)|Contains class `Object` that represents an object belonging to a bucket in an AIS cluster, and contains all object-related operations, including (but not limited to) retreiving, adding and deleting objects.|
|[object_group.py](https://github.com/NVIDIA/aistore/blob/master/python/aistore/sdk/object_group.py)|Contains class `ObjectGroup`, representing a collection of objects belonging to a bucket in an AIS cluster. Includes all multi-object operations such as deleting, evicting, and prefetching objects.|
|[job.py](https://github.com/NVIDIA/aistore/blob/master/python/aistore/sdk/job.py)|Contains class `Job` and all job-related operations.|
|[etl.py](https://github.com/NVIDIA/aistore/blob/master/python/aistore/sdk/etl.py)|Contains class `Etl` and all ETL-related operations.|

For more information on SDK usage, refer to the [SDK reference documentation](https://aiatscale.org/docs/python_sdk.md).


## References

* [AIStore GitHub](https://github.com/NVIDIA/aistore)
* [Documentation](https://aiatscale.org/docs)
* [AIStore pip package](https://pypi.org/project/aistore/)
* [Videos and demos](https://github.com/NVIDIA/aistore/blob/master/docs/videos.md)
