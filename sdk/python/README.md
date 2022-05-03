# AIS Python SDK

Project to provide Client to access AIS cluster from Python applications.
Only Python 3 (version 3.6 and newer) is supported.

## Installation

### Install from a package

The latest release of AIStore package is easily installed either via Anaconda (recommended):

```console
$ conda install aistore
```

or via pip:
```console
$ pip install aistore
```

### Install from the sources

If you'd like to try our bleeding edge features (and don't mind potentially running into the occasional bug here or there), you can install the latest master directly from GitHub. For a basic install, run:

```console
$ cd cmd/python-sdk
$ pip install -e .
```

## Quick start

If you have worked with Boto3 - Python SDK library for AWS, AIStore SDK should be familiar to you.
First, you initialize a connection to a storage by creating a client.
Second, you call client methods and provide them a bunch of named arguments.

Both libraries use the similar names for common operations:
`create_bucket` to create a new empty bucket, `put_object` to upload an object to a bucket etc.
But the names of method arguments often differ due to architectural difference.
E.g, AWS works only with one kind of buckets - AWS buckets, so AWS SDK functions accept only bucket name - `create_bucket(Bucket="bck")`.
AIStore supports a number of various bucket providers, so a bucket name may be insufficient and you have to pass a bucket provider name.
That is why majority of AIStore SDK functions accept two arguments: `bck_name` - a bucket name,
and optional `provider` - a bucket provider name.
The default value of `provider` is `ProviderAIS`. So if you work only with AIS buckets, in most cases you can omit `provider`.

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
				 page_size: int = 0) -> List[BucketEntry]:
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

Please also see:
* the main [AIStore repository](https://github.com/NVIDIA/aistore),
* [AIStore documentation](https://aiatscale.org/docs), and
* [AIStore and ETL videos](https://github.com/NVIDIA/aistore/blob/master/docs/videos.md).
