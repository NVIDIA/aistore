# Your First AIStore Object with Python

AIStore's Python SDK lets you create a bucket, upload bytes, list objects, and verify a download. For deployment choices, refer to [Getting Started](/docs/getting_started.md).

## Prerequisites

- Python 3.10 or later, as required by the SDK's `python/pyproject.toml`.
- A running AIStore cluster where you can create buckets. For a maintained local source deployment, use the [local playground](/docs/getting_started.md#local-playground). Alternatively, the [Minimal Compose deployment](/docs/getting_started.md#minimal-compose-deployment) is a limited, one-proxy, one-target container smoke test, not a production deployment; it does not configure TLS or AuthN.
- The HTTP endpoint of a reachable AIS proxy. The default local playground endpoint is `http://localhost:8080`; the default Minimal Compose host endpoint is `http://localhost:51080`. For a remote or differently configured cluster, obtain its actual endpoint from the cluster administrator and use that instead.
- Client access to the target URLs returned by that proxy for object PUT and GET, with trusted TLS certificates at both proxy and target URLs when using HTTPS. The default host-local Playground and Compose paths provide local proxy and target access. For a remote cluster, check the advertised target addresses with the administrator: a proxy-only SSH or Kubernetes port-forward can allow bucket creation but fail object I/O after redirection. Refer to [read and write traffic patterns](/docs/traffic_patterns.md#read-and-write-traffic-patterns) for the proxy-to-target path.

## Install and Connect

Ensure `python3` resolves to Python 3.10 or later, then create a virtual environment and install the SDK:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install aistore
```

Select the endpoint for the deployment you started. For the default local playground:

```sh
export AIS_ENDPOINT=http://localhost:8080
```

For the default Minimal Compose smoke test, use this instead:

```sh
export AIS_ENDPOINT=http://localhost:51080
```

Use your cluster's actual proxy URL if it differs from these defaults.

If your cluster requires an authentication token, supply one through `AIS_AUTHN_TOKEN` or the `Client(token=...)` argument; refer to [authentication validation](/docs/auth_validation.md) and the [SDK authentication settings](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/README.md#environment-variables). For HTTPS with a private trusted certificate authority (CA), set `AIS_CLIENT_CA` to its certificate file or pass `Client(ca_cert=...)`; refer to the [SDK HTTPS guidance](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/README.md#https). Use certificate verification, not `skip_verify`, and confirm that skip-verification environment variables are not enabled.

## Create, List, and Read an Object

Run this from the shell where you installed the SDK and set `AIS_ENDPOINT`:

```sh
python - <<'PY'
import os
from uuid import uuid4

from aistore.sdk import Client

client = Client(os.environ['AIS_ENDPOINT'])
bucket = client.bucket(f"python-first-object-{uuid4().hex}")
print(f"Tutorial bucket: {bucket.name}", flush=True)
created = False
try:
    bucket.create()  # Do not adopt an existing bucket.
    created = True
    obj = bucket.object("hello.txt")
    content = b"Hello AIStore\n"
    obj.get_writer().put_content(content)
    listed = any(entry.name == obj.name for entry in bucket.list_objects_iter())
    if not listed:
        raise RuntimeError(f"Object {obj.name} was not found in the bucket listing")
    downloaded = obj.get_reader().read_all()
    if downloaded != content:
        raise RuntimeError(f"Downloaded content for {obj.name} did not match")
finally:
    if created:
        bucket.delete()  # Deletes only the new bucket and its tutorial object.
print("Verified hello.txt; content matches; tutorial bucket deleted.")
PY
```

Expected output:

```text
Tutorial bucket: python-first-object-<unique-hex-suffix>
Verified hello.txt; content matches; tutorial bucket deleted.
```

The script prints its unique AIS bucket name before creation, checks that the uploaded object appears in the listing, compares downloaded bytes, and deletes the bucket it created. If creation fails, it does not delete an existing bucket. A failed request, verification, or cleanup raises an exception instead of printing final success; confirm the endpoint and permissions before retrying. If interrupted before cleanup, use the printed bucket name to inspect the tutorial bucket before removing it; leave other buckets untouched.

For more SDK operations, refer to the [Python SDK reference](https://docs.nvidia.com/aistore/python/aistore/sdk) and [SDK README](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/README.md).
