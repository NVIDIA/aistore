---
layout: post
title:  "Python SDK: Getting Started"
date:   Jul 20, 2022
author: Ryan Koo
categories: aistore python sdk
---

# Python SDK: Getting Started

Python has grounded itself as a popular language of choice among data scientists and machine learning developers. Python's recent popularity in the field can be attributed to Python's general *ease-of-use*, especially with the popular machine learning framework [PyTorch](https://pytorch.org/), which is itself written in Python.

[AIStore Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore) is a project which includes a growing library of client-side APIs to easily access and utilize AIStore clusters, objects, and buckets, as well as a number of tools for AIStore usage/integration with PyTorch.

The [AIStore Python API](https://aistore.nvidia.com/docs/python-sdk) is essentially a Python port of AIStore's [Go APIs](https://github.com/NVIDIA/aistore/tree/main/api). In terms of functionality, the AIStore Python and Go APIs are quite similar, both of which essentially make simple [HTTP requests](https://aiatscale.org/docs/http-api#api-reference) to an AIStore endpoint. The HTTP requests allow the APIs to interact (reads and writes) with an AIStore instance's metadata. The API provides convenient and flexible ways (similar to those provided by the [CLI](https://aiatscale.org/docs/cli)) to move data (as objects) in and out of buckets on AIStore, manage AIStore clusters, and much more.

This technical blog will demonstrate a few potential ways the Python API provided in the [Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore) could be used with a running AIStore instance to manage and utilize data.

## Getting Started

### Installing & Deploying AIStore

The latest AIStore release can be easily installed either with Anaconda or `pip`:

```console
$ conda install aistore
```

```console
$ pip install aistore
```

> Note that only Python 3.x (version 3.6 or later) is currently supported for AIStore.

While there are a number of options available for deploying AIStore - as is demonstrated [here](https://github.com/NVIDIA/aistore/blob/main/docs/getting_started.md) - for the sake of simplicity, we will be using AIStore's [minimal standalone docker deployment](https://github.com/NVIDIA/aistore/blob/main/deploy/prod/docker/single/README.md):

```console
# Deploying the AIStore cluster in a container on port 51080
docker run -d \
    -p 51080:51080 \
    -v /disk0:/ais/disk0 \
    aistore/cluster-minimal:latest
```

### Moving Data To AIStore

Let's say we want to move a copy of the [TinyImageNet](https://paperswithcode.com/dataset/tiny-imagenet) dataset from our local filesystem to a bucket on our running instance of AIStore.

First, we import the Python API and initialize the client to the running instance of AIStore:

```python
from aistore import Client

client = Client("http://localhost:51080")
```

Before moving any data into AIStore, we can first check to see AIStore is fully deployed and ready:

```python
client.cluster().is_aistore_running()
```

Once AIStore is verified as running, moving the dataset to a bucket on AIStore as a *compressed* format is as easy as:

```python
BUCKET_NAME = "tinyimagenet_compressed"
COMRPESSED_TINYIMAGENET = "~/Datasets/tinyimagenet-compressed.zip"
OBJECT_NAME = "tinyimagenet-compressed.zip"

# Create a new bucket [BUCKET_NAME] to store dataset
client.bucket(BUCKET_NAME).create()

# Verify bucket creation operation
client.cluster().list_buckets()

# Put dataset [COMPRESSED_TINYIMAGENET] in bucket [BUCKET_NAME] as object with name [OBJECT_NAME]
client.bucket(BUCKET_NAME).object(OBJECT_NAME).put(COMPRESSED_TINYIMAGENET)

# Verify object put operation
client.bucket(BUCKET_NAME).list_objects().get_entries()
```

Say we now want to instead move an *uncompressed* version of TinyImageNet to AIStore. The uncompressed format of TinyImageNet is comprised of several sub-directories which divide the dataset's many image samples into separate sets (train, validation, test) as well as separate classes (based on numbers mapped to image labels).

As opposed to traditional file storage systems which operate on the concept of multi-level directories and sub-directories, object storage systems, such as AIStore, maintain a *strict* two-level hierarchy of *buckets* and *objects*. However, we can still maintain a "symbolic" directory by manipulating how we name the data.

We can move the dataset to an AIStore bucket while preserving the directory-based structure of the dataset by using the bucket `put_files` command along with the `recursive` option:

```python
BUCKET_NAME = "tinyimagenet_uncompressed"
TINYIMAGENET_DIR = <local-path-to-dataset> + "/tinyimagenet/"

# Create a new bucket [BUCKET_NAME] to store dataset
bucket = client.bucket(BUCKET_NAME).create()

bucket.put_files(TINYIMAGENET_DIR, recursive=True)

# Verify object put operations
bucket.list_objects().get_entries()
```

### Getting Data From AIStore

Getting the *compressed* TinyImageNet dataset from AIStore bucket `ais://tinyimagenet_compressed` is as easy as:

```python
BUCKET_NAME = "tinyimagenet_compressed"
OBJECT_NAME = "tinyimagenet-compressed.zip"

# Get object [OBJECT_NAME] from bucket [BUCKET_NAME]
client.bucket(BUCKET_NAME).object(OBJECT_NAME).get()
```

If we want to get the *uncompressed* TinyImageNet from AIStore bucket `ais://tinyimagenet_uncompressed`, we can easily do that with [Bucket.list_objects()](https://aistore.nvidia.com/docs/python-sdk#bucket.Bucket.list_objects) and [Object.get()](https://aistore.nvidia.com/docs/python-sdk#obj.object.Object.get).

```python
BUCKET_NAME = "tinyimagenet_uncompressed"

# List all objects in bucket [BUCKET_NAME]
TINYIMAGENET_UNCOMPRESSED = client.bucket(BUCKET_NAME).list_objects().get_entries()

for FILENAME in TINYIMAGENET_UNCOMPRESSED:
    # Get object [filename.name] from bucket [BUCKET_NAME]
    client.bucket(BUCKET_NAME).object(FILENAME.name).get()
```

We can also pick a *specific* section of the uncompressed dataset and only get those specific objects. By specifying a `prefix` to our [Bucket.list_objects()](https://aistore.nvidia.com/docs/python-sdk#bucket.Bucket.list_objects) call, we can manipulate the *symbolic* file system and list only the contents in our desired directory.

```python
BUCKET_NAME = "tinyimagenet_uncompressed"

# Listing only objects with prefix "validation/" bucket [tinyimagenet_uncompressed]
TINYIMAGENET_UNCOMPRESSED_VAL = client.bucket(BUCKET_NAME).list_objects(prefix="validation/").get_entries()

for FILENAME in TINYIMAGENET_UNCOMPRESSED_VAL:
    # Get operation on objects with prefix "validation/" from bucket [tinyimagenet_uncompressed]
    client.bucket(BUCKET_NAME).object(FILENAME.name).get()
```

### External Cloud Storage Providers

AIStore also supports third-party remote backends, including Amazon S3, Google Cloud, and Microsoft Azure.

> For exact definitions and related capabilities, please see [terminology](https://aiatscale.org//docs/overview#terminology).

We shutdown the previous instance of AIStore and re-deploy AIStore with AWS S3 and GCP backends attached:

```console
# Similarly deploying AIStore cluster in a container on port 51080, but with GCP and AWS backends attached
docker run -d \
    -p 51080:51080 \
    -v <path_to_gcp_config>.json:/credentials/gcp.json \
    -e GOOGLE_APPLICATION_CREDENTIALS="/credentials/gcp.json" \
    -e AWS_ACCESS_KEY_ID="AWSKEYIDEXAMPLE" \
    -e AWS_SECRET_ACCESS_KEY="AWSSECRETEACCESSKEYEXAMPLE" \
    -e AWS_REGION="us-east-2" \
    -e AIS_BACKEND_PROVIDERS="gcp aws" \
    -v /disk0:/ais/disk0 \
    aistore/cluster-minimal:latest
```

> Deploying an AIStore cluster with third-party cloud backends simply *imports/copies the buckets and objects from the provided third-party backends to AIStore*. The client-side APIs themselves do **not** interact with the actual external backends at any point. The client-side APIs only interact with duplicate instances of those external cloud storage buckets residing in the AIStore cluster.

The [Object.get()](https://aistore.nvidia.com/docs/python-sdk#obj.object.Object.get) works with external cloud storage buckets as well. We can use the method in a similar fashion as shown previously to easily get either a compressed or uncompressed version of the dataset from, for examples, `gcp://tinyimagenet_compressed` and `gcp://tinyimagenet_uncompressed`. 

```python
# Getting compressed TinyImageNet dataset from [gcp://tinyimagenet_compressed]
BUCKET_NAME = "tinyimagenet_compressed"
OBJECT_NAME = "tinyimagenet-compressed.zip"
client.bucket(BUCKET_NAME, provider="gcp").object(OBJECT_NAME).get()


# Getting uncompressed TinyImageNet dataset from [gcp://tinyimagenet_uncompressed]
BUCKET_NAME = "tinyimagenet_uncompressed"
TINYIMAGENET_UNCOMPRESSED = client.bucket(BUCKET_NAME, provider="gcp").list_objects().get_entries()
for FILENAME in TINYIMAGENET_UNCOMPRESSED:
    client.bucket(BUCKET_NAME, provider="gcp").object(FILENAME.name).get()


# Getting only objects with prefix "validation/" from bucket [gcp://tinyimagenet_uncompressed]
TINYIMAGENET_UNCOMPRESSED_VAL = client.bucket(BUCKET_NAME).list_objects(prefix="validation/").get_entries()
for FILENAME in TINYIMAGENET_UNCOMPRESSED_VAL:
    client.bucket(BUCKET_NAME).object(FILENAME.name).get()
```

> Note the added argument `provider` supplied in [`Client.bucket()`](https://aistore.nvidia.com/docs/python-sdk#client.Client.bucket) for the examples shown above.

We can instead choose to *copy* the contents of an external cloud storage bucket on AIStore to a native (AISProvider) AIStore bucket with [`Bucket.copy()`](https://aistore.nvidia.com/docs/python-sdk#bucket.Bucket.copy) as well:

```python
# Copy bucket [gcp://tinyimagenet_uncompressed] and its objects to new bucket [ais://tinyimagetnet_validationset]
FROM_BUCKET = "tinyimagenet_uncompressed"
TO_BUCKET = "tinyimagenet_validationset"
client.bucket(FROM_BUCKET, provider="gcp").copy(TO_BUCKET)

# Evict external cloud storage bucket [gcp://tinyimagenet_uncompressed] if not needed anymore for cleanup (free space on cluster)
client.bucket(FROM_BUCKET, provider="gcp").evict()
```

Eviction of a cloud storage bucket destroys any instance of the cloud storage bucket (and its objects) from the AIStore cluster metadata. Eviction does **not** delete or affect the actual cloud storage bucket (in AWS S3, GCP, or Azure).


## PyTorch

PyTorch provides built-in [tools](https://github.com/pytorch/data/tree/main/torchdata/datapipes/iter/load#aistore-io-datapipe) for AIStore integration, allowing machine learning developers to easily use AIStore as a viable storage system option with PyTorch. In fact, the dataloading classes [`AISFileLister`](https://pytorch.org/data/main/generated/torchdata.datapipes.iter.AISFileLister.html#aisfilelister) and [`AISFileLoader`](https://pytorch.org/data/main/generated/torchdata.datapipes.iter.AISFileLoader.html#torchdata.datapipes.iter.AISFileLoader) found in [`aisio.py`](https://github.com/pytorch/data/blob/main/torchdata/datapipes/iter/load/aisio.py) provided by PyTorch make use of several of the client-side APIs referenced in this article.

For more information on dataloading from AIStore with PyTorch, please refer to this [article](https://aiatscale.org/blog/2022/07/12/aisio-pytorch).


## More Examples & Resources

For more examples, please refer to additional documentation [AIStore Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore) and try out the [SDK tutorial (Jupyter Notebook)](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk-tutorial.ipynb).

For information on specific API usage, please refer to the [API reference](https://aistore.nvidia.com/docs/python-sdk).


## References

* [AIStore GitHub](https://github.com/NVIDIA/aistore)
* [AIStore Go API](https://github.com/NVIDIA/aistore/tree/main/api)
* [AIStore Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore)
* [Documentation](https://aiatscale.org/docs)
* [Official AIStore PIP Package](https://pypi.org/project/aistore/)
