---
layout: post
title:  "AIStore Python SDK: Getting Started"
date:   2022-05-23 16:08:24 -0700
author: Vladimir Markelov
categories: python sdk
---

## Introduction

Python is used in machine learning everywhere.
AIStore provides [Python SDK](https://github.com/NVIDIA/aistore/tree/master/sdk/python) for easier adoption by the ML scientists.
AIS Python SDK is a growing set of client-side APIs to access and utilize AIS clusters.

The project is, essentially, a Python port of the [AIS Go APIs](https://github.com/NVIDIA/aistore/tree/master/api), with additional objectives that include:

* utmost convenience for Python developers;
* minimal, or no changes whatsoever, to apps that already use [S3](https://github.com/NVIDIA/aistore/blob/master/docs/s3compat.md).

Note that only Python 3.x (version 3.6 or later) is currently supported.

## Installation

The latest AIS release can be easily installed either with Anaconda (recommended) or `pip`:

```console
$ conda install aistore
```

```console
$ pip install aistore
```

## Quick start

If you've already used Python SDK library for AWS (aka `Boto3`), AIS SDK should be very familiar.

Similar to `Boto3`, the steps include:

1. First, initialize the connection to storage by **creating a client**.
2. Second, call client methods with assorted (and explicitly enumerated) **named arguments**.

Names of the most common operations are also identical, e.g.:

* `create_bucket` - create a new empty bucket
* `put_object` - upload an object to a bucket

and so on.

### Step by step example

To import the SDK library, add this to your project:

```python
from aistore.client.api import Client
```

Initialize the client. In the example we assume that AIStore gateway is running on a local machine(`localhost:8080`):

```python
client = Client("http://localhost:8080")
```

After the first start, the storage contains no buckets. Let's create one.
While AIStore supports cloud buckets, like AWS, Azure or GCP, only AIS buckets can be created and destroyed.

```python
BUCKET_NAME = "sdk_test"
client.create_bucket(BUCKET_NAME)
```

Get the list of all buckets of all providers. By default `list_buckets` returns only AIS buckets,
pass empty string as a provider to get all accessible by AIStore buckets.
The supported providers are defined at AIStore deployment and cannot be changed on the fly.
Print bucket names and their providers:

```python
buckets = client.list_buckets(provider="")
for bucket in buckets:
	print(bucket.name, bucket.provider)
```

Put an object to the newly created bucket. The object content is read from a local file:

```python
s = f"test string"
content = s.encode('utf-8')
obj_name = "obj1"
with tempfile.NamedTemporaryFile() as f:
	f.write(content)
	f.flush()
	# Observe the PUT call here
	client.put_object(BUCKET_NAME, obj_name, f.name)
```

Get the object properties and print the object size:

```python
obj_attrs = client.head_object(BUCKET_NAME, obj_name)["Content-Length"]
print(obj_attrs["Content-Length"])
```

List objects in your bucket and print their names, sizes, and versions:

```python
objects = client.list_objects(BUCKET_NAME).entries
for obj in objects:
	print(obj.name, obj.size, obj.version)
```

Read the object content:

```python
cont = client.get_object(BUCKET_NAME, obj_name).read_all()
```

Cleanup everything: destroy the object and the bucket.
Note that if you want to destroy bucket, you do not need to delete objects manually beforehand.
So, the first call to `delete_object` in the example can be omitted.

```python
client.delete_object(BUCKET_NAME, obj_name)
client.destroy_bucket(BUCKET_NAME)
```

## References

* [API](https://github.com/NVIDIA/aistore/tree/master/api)
* [Python SDK](https://github.com/NVIDIA/aistore/tree/master/sdk/python)
* [SDK tutorial (Jupyter Notebook)](https://github.com/NVIDIA/aistore/blob/master/sdk/python/sdk-tutorial.ipynb)
