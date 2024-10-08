
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

## Boto3 with AIStore Authentication

When [Authentication (AuthN)](https://github.com/NVIDIA/aistore/blob/main/docs/authn.md) is enabled on the AIStore server, AIStore expects a JWT authorization token with each request to grant the required permissions. Normally, `boto3` signs requests by adding complex signature and signing information to the `Authorization` header. However, AIStore doesn’t need this extra information—just the token.

To handle this in `boto3`, you can modify the `Authorization` header to include your JWT token before the request is made to AIStore.

### Example:

```python
# Create the S3 client
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:8080/s3",
)

# Custom request hook to inject the Authorization header
def add_auth_header(request, **kwargs):
    request.headers['Authorization'] = "Bearer <token>"

# Attach the request hook to modify headers
s3.meta.events.register('before-send.s3.*', add_auth_header)
```

This setup replaces the `Authorization` header with the correct token before the request is sent to the AIStore server.

## References

* [AIStore GitHub](https://github.com/NVIDIA/aistore)
* [Documentation](https://aistore.nvidia.com/docs)
* [AIStore pip package](https://pypi.org/project/aistore/)
* [Videos and demos](https://github.com/NVIDIA/aistore/blob/main/docs/videos.md)
