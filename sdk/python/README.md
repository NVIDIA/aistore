## AIS Python SDK

AIS Python SDK provides a (growing) set of client-side APIs to access and utilize AIS clusters.

The project is, essentially, a Python port of the [AIS Go APIs](/api), with additional objectives that include:

* utmost convenience for Python developers;
* minimal, or no changes whatsoever, to apps that already use [S3](/docs/s3compat.md).

Note that only Python 3.x (version 3.6 or later) is currently supported.

## Installation

### Install from a package

The latest AIS release can be easily installed either with Anaconda (recommended) or `pip`:

```console
$ conda install aistore
```

```console
$ pip install aistore
```

### Install from the sources

If you'd like to work with the current upstream (and don't mind the risk), install the latest master directly from GitHub:

```console
$ cd sdk/python # If you are not here already.
$ pip install -e .
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

## AIS supports multiple [backends](/docs/providers.md)

AWS works only with one kind of buckets - AWS buckets. AWS SDK functions accept only the bucket name, e.g. `create_bucket(Bucket="bck")`.

AIS, on the other hand, supports a number of different [backend providers](/docs/providers.md) or, simply, backends.

> For exact definitions and related capabilities, please see [terminology](/docs/overview.md#terminology)

And so, for AIS a bucket name, strictly speaking, does not define the bucket.

That is why majority of the SDK functions accept two arguments:

* `bck_name` - for bucket name, and
* optional `provider` - for backend provider.

The default `provider` is `ProviderAIS` (see `const.py` for this and other system constants).

If you only work with AIS buckets, in most cases you can simply omit the `provider`.

### Calling Client methods

Every Client method can be called in two ways: with named arguments in arbitrary order and with positional arguments.
For instance, `list_objects` method is declared as:

```python
def list_objects(self,
     bck_name: str,
     provider: str = ProviderAIS,
     prefix: str = "",
     props: str = "",
     count: int = 0,
     page_size: int = 0,
) -> List[BucketEntry]:
```

To get first 10 objects of AIS bucket `bck1` which names start with `img-`, execute either with positional arguments:

```python
objects = client.list_objects("bck1", ProviderAIS, "img-", "", 10)
```

or with named ones:

```python
# ProviderAIS is omitted because it is default value for a provider argument
objects = client.list_objects(bck_name="bck1", prefix="img-", count=10)
```

### Example

```python
from aistore.client.api import Client
from aistore.client.const import ProviderAIS

# Assuming that AIStore server is running on the same machine
client = Client("http://localhost:8080")

# Create a new AIS bucket.
# Note: this function does not accept 'provider' because AIStore SDK supports creating of AIS buckets only.
client.create_bucket("bck")

# List the buckets.
# By default, it returns only AIS buckets. If you want to get all buckets including Cloud ones,
# pass empty string as a provider:
#   bucket_list = client.list_buckets(provider = "")
# The call below is the same as 'bucket_list = client.list_buckets(provider = ProviderAIS)'
bucket_list = client.list_buckets()

# Put an object to the new bucket. The object content is read from a local file '/tmp/obj1_content'
# The method returns properties of the new object like 'ETag'.
# Argument 'provider' is optional and can be omitted in this example. It is added for clarity.
obj_props = client.put_object(bck_name="bck", obj_name="obj1", path="/tmp/obj1_content", provider=ProviderAIS)

# Destroy the bucket and its content.
# Note: this function also does not accept 'provider' because AIStore SDK supports destroying of AIS buckets only.
client.destroy_bucket("bck")
```

## References

* [AIStore GitHub](https://github.com/NVIDIA/aistore)
* [Documentation](https://aiatscale.org/docs)
* [Videos and demos](https://github.com/NVIDIA/aistore/blob/master/docs/videos.md)
