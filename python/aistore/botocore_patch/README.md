
# AIS Botocore Patch

As an alternative to the [AIStore Python SDK](https://aistore.nvidia.com/docs/python_sdk.md) for accessing AIStore, you might prefer using other popular object storage client libraries. For example, you can use Amazon's [Boto3](https://github.com/boto/boto3) library, or its underlying [botocore](https://github.com/boto/botocore) library.

This package `aistore.botocore_patch.botocore` exposes an interface to access AIStore as if it were Amazon S3, allowing developers to utilize AIStore object storage without changing their existing S3 client code.

## Install and Import AIStore `botocore_patch` Package

By default, botocore doesn't handle [HTTP redirects](https://www.rfc-editor.org/rfc/rfc7231#page-54), which prevents you from using it with AIStore.

To resolve this, install `aistore` with the `botocore` extra, and then import `aistore.botocore_patch.botocore` in your code. This will dynamically patch HTTP redirect support into botocore, via [monkey patch](https://www.tutorialspoint.com/explain-monkey-patching-in-python).

```shell
$ pip install aistore[botocore]
```

```python
import boto3
from aistore.botocore_patch import botocore
```

## Cluster Configuration

To use AIS as an S3 client, you must first configure the cluster. 

Set the cluster to run as an S3 client:

```shell
ais config cluster features S3-API-via-Root
```

S3 uses MD5 hashes, so set the cluster to use it:

```shell
ais config cluster checksum.type=md5
```

Now AIS will accept S3 commands and behave as an S3 client. 

## References

* [AIStore GitHub](https://github.com/NVIDIA/aistore)
* [Documentation](https://aistore.nvidia.com/docs)
* [AIStore pip package](https://pypi.org/project/aistore/)
* [Videos and demos](https://github.com/NVIDIA/aistore/blob/main/docs/videos.md)
