AIStore's on-disk layout supports multiple remote backends, configurable namespaces, and AIS-to-AIS caching with full data recovery capabilities.

Here's a simplified drawing depicting two [providers](/docs/providers.md), AIS and AWS, and two buckets, `ABC` and `XYZ`, respectively. In the picture, `mpath` is a single [mountpath](/docs/configuration.md) - a single disk **or** a volume formatted with a local filesystem of choice, **and** a local directory (`mpath/`):

![on-disk hierarchy](/docs/images/PBCT.png)

Further, each bucket would have a unified structure with several system directories (e.g., `%ec` that stores erasure coded content) and, of course, user data under `%ob` ("object") locations.

Needless to say, the same exact structure reproduces itself across all AIS storage nodes, and all data drives of each clustered node.

With namespaces, the picture becomes only slightly more complicated. The following shows two AIS buckets, `DEF` and `GHJ`, under their respective user-defined namespaces called `#namespace-local` and `#namespace-remote`.  Unlike a local namespace of *this* cluster, the remote one would have to be prefixed with UUID - to uniquely identify another AIStore cluster hosting `GHJ` (in this example) and from where this bucket's content will be replicated or cached, on-demand or via Prefetch API and [similar](/docs/overview.md#existing-datasets).

![on-disk hierarchy with namespaces](/docs/images/PBCT-with-namespaces.png)

### Example

Say, we have an `gs://llm-data` bucket, and an object "images/dog.jpeg" in it. Given two different bucket's namespaces, the respective FQNs inside AIStore may look like:

```
   /vdi/@gcp/#prod/llm-data/%ob/images/dog.jpeg

and

   /vdh/@gcp/#dev/llm-data/%ob/images/dog.jpeg
```

where:

| Component | Example 1 | Example 2 | Meaning |
|-----------|-----------|-----------|---------|
| Mountpath | `/vdi` | `/vdh` | Physical (mounted) device |
| Provider | `gcp` | `gcp` | Backend provider |
| Namespace | `#prod` | `#dev` | Account, profile, or user-defined alias |
| Bucket | `llm-data` | `llm-data` | Bucket name |
| Content type | `%ob` | `%ob` | Content kind: objects, EC slices, chunks, manifests |
| Object | `images/dog.jpeg` | `images/dog.jpeg` | Object name (preserves virtual directory structure) |

### Content types

Within each bucket directory, AIS organizes content by type:

| Marker | Constant | Content |
|--------|----------|---------|
| `%ob` | `fs.ObjCT` | Object data |
| `%wk` | `fs.WorkCT` | Work/temporary files |
| `%ec` | `fs.ECSliceCT` | Erasure-coded slices |
| `%mt` | `fs.ECMetaCT` | Erasure-coded metadata |
| `%ch` | `fs.ChunkCT` | Chunked object data |
| `%ut` | `fs.ChunkMetaCT` | Chunked object metadata |
| `%ds` | `fs.DsortFileCT` | Distributed sort files |
| `%dw` | `fs.DsortWorkCT` | Distributed sort work |

> See the [source](https://github.com/NVIDIA/aistore/blob/main/fs/content.go) for the most updated enumeration.

### References

For the purposes of full disclosure and/or in-depth review, following are initial references into AIS sources that also handle on-disk representation of object metadata:

* [local object metadata (LOM)](https://github.com/NVIDIA/aistore/blob/main/core/lom_xattr.go)

 and AIS control structures:

* [bucket metadata (BMD)](https://github.com/NVIDIA/aistore/blob/main/ais/bucketmeta.go)
* [cluster map (Smap)](https://github.com/NVIDIA/aistore/blob/main/ais/clustermap.go)

## System Files

In addition to user data, AIStore stores, maintains, and utilizes itself a relatively small number of system files that serve a variety of different purposes. Full description of the AIStore *persistence* would not be complete without listing those files (and their respective purposes) - for details, please refer to:

* [System Files](/docs/sysfiles.md)
