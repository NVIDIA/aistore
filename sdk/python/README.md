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

$ cd aistore/sdk/python

$ pip install -e .
```


## Quick Start

In order to interact with your running AIS instance, you will need to create a `client` object:

```python
from aistore import Client

client = Client("http://localhost:8080")
```

The newly created `client` object can be used to interact with your AIS cluster, buckets, and objects. Here are a few ways to do so:

```python
# Check if AIS is deployed and running
client.cluster().is_aistore_running()
```

```python
# Get cluster information
client.cluster().get_info()
```

```python
# Create a bucket named "my-ais-bucket"
client.bucket("my-ais-bucket").create()
```

```python
# Delete bucket named "my-ais-bucket"
client.bucket("my-ais-bucket").delete()
```

```python
# Head bucket
client.bucket("my-ais-bucket").head()
```

```python
# Head object
client.bucket("my-ais-bucket").object("my-object").head()
```

```python
# Put Object
client.bucket("my-ais-bucket").object("my-new-object").put("path-to-object")
```

> If you are using AIS buckets, you can simply omit the provider argument (defaults to ProviderAIS) when instantiating a bucket object (`client.bucket("my-ais-bucket").create()` is equivalent to `client.bucket("my-ais-bucket", provider="ais").create()`).

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

Please note that certain operations do **not** support external cloud storage buckets. Please refer to the [API reference documentation](https://aiatscale.org/docs/python_api.md) for more information on which bucket/object operations support remote cloud buckets, as well as general information on class and method usage.


### More Examples

For more in-depth examples, please see [SDK tutorial (Jupyter Notebook)](https://github.com/NVIDIA/aistore/blob/master/sdk/python/sdk-tutorial.ipynb).


### API Documentation

|Module|Summary|
|--|--|
|[api.py](https://github.com/NVIDIA/aistore/blob/master/sdk/python/aistore/client/api.py)|Contains `Client` class, which has methods for making HTTP requests to an AIStore server. Includes factory constructors for `Bucket`, `Cluster`, and `Xaction` classes.|
|[cluster.py](https://github.com/NVIDIA/aistore/blob/master/sdk/python/aistore/client/cluster.py)|Contains `Cluster` class that represents a cluster bound to a client and contains all cluster-related operations, including checking the cluster's health and retrieving vital cluster information.|
|[bucket.py](https://github.com/NVIDIA/aistore/blob/master/sdk/python/aistore/client/bucket.py)|Contains `Bucket` class that represents a bucket in an AIS cluster and contains all bucket-related operations, including (but not limited to) creating, deleting, evicting, renaming, copying.|
|[object.py](https://github.com/NVIDIA/aistore/blob/master/sdk/python/aistore/client/object.py)|Contains class `Object` that represents an object belonging to a bucket in an AIS cluster, and contains all object-related operations, including (but not limited to) retreiving, adding and deleting objects.|
|[xaction.py](https://github.com/NVIDIA/aistore/blob/master/sdk/python/aistore/client/xaction.py)|Contains class `Xaction` and all xaction-related operations.|

For more information on API usage, refer to the [API reference documentation](https://aiatscale.org/docs/python_api.md).

### PyTorch Integration

You can list and load data from AIS buckets (buckets that are not 3rd party backend-based) and remote cloud buckets (3rd party backend-based cloud buckets) in PyTorch using [AISFileLister](https://pytorch.org/data/main/generated/torchdata.datapipes.iter.AISFileLister.html#aisfilelister) and [AISFileLoader](https://pytorch.org/data/main/generated/torchdata.datapipes.iter.AISFileLoader.html#torchdata.datapipes.iter.AISFileLoader).

`AISFileLister` and `AISFileLoader` are now available as a part of official [pytorch/data](https://github.com/pytorch/data/blob/main/torchdata/datapipes/iter/load/aisio.py) project. 

```
from torchdata.datapipes.iter import AISFileLister, AISFileLoader

# provide list of prefixes to load and list data from
ais_prefixes = ['gcp://bucket-name/folder/', 'aws:bucket-name/folder/', 'ais://bucket-name/folder/', ...]

# List all files for these prefixes using AISFileLister
dp_ais_urls = AISFileLister(url='localhost:8080', source_datapipe=ais_prefixes)

# print(list(dp_ais_urls))

# Load files using AISFileLoader
dp_files = AISFileLoader(url='localhost:8080', source_datapipe=dp_ais_urls)

for url, file in dp_files:
    pass
```

## References

* [AIStore GitHub](https://github.com/NVIDIA/aistore)
* [Documentation](https://aiatscale.org/docs)
* [Official AIStore PIP Package](https://pypi.org/project/aistore/)
* [Videos and Demos](https://github.com/NVIDIA/aistore/blob/master/docs/videos.md)