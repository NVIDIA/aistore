# AIStore fsspec Adapter

The AIStore fsspec adapter provides read-only access to AIS objects through the
standard fsspec filesystem interface.

Install the optional dependency:

```console
pip install "aistore[fsspec]"
```

Use the adapter with an AIS endpoint:

```python
import fsspec

fs = fsspec.filesystem("ais", endpoint="http://ais-proxy:8080")
print(fs.ls("ais://dataset/prefix"))
data = fs.cat("ais://dataset/object.bin")
```

The initial adapter supports object listing, metadata lookup, binary reads, and
range reads. Mutating operations are intentionally unsupported.
