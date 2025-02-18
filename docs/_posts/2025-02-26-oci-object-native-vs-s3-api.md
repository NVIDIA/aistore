---
layout: post
title: "Comparing OCI's Native Object Storage and S3 API Backends"
date: February 26, 2025
author: Ed McClanahan
categories: aistore oci
--- 

The newly available support for Oracle Cloud Infrastructure ("OCI") Object Storage was made
possible by adopting OCI's API via their [OCI Golang SDK](https://github.com/oracle/oci-go-sdk).
OCI also supports the S3 API as well allowing users the choice of either protocol when configuring
OCI Object Storage as a backend for AIStore. To assist in making the choice of backend protocols
used to reach OCI Object Storage, this post provides some performance insights.

## Functional Comparison of the OCI Native Object Store and S3 APIs

Similar to how AIStore backend now supportsx OCI's Native API, AIStore has long supported
access to OCI's Object Store via the S3 protocol. S3 support utilizes the
[AWS Golang SDK](http://github.com/aws/aws-sdk-go-v2). In addition to the REST operations
expected of each Object Store available as an AIStore backend, this SDK includes support for
very large objects (e.g. >> 5GiB) via a set of Multi-Part-Upload ("MPU") APIs. These APIs
additionally enable these multiple parts to be uploaded simultaneously thus improving upload
(PUT) performance dramatically. Similarly, large and performant object downloads (GETs) are enabled
via support for "ranged" GETs that, in combination, fetch the entire contents of an object. Indeed,
the [AWS Golang SDK](http://github.com/aws/aws-sdk-go-v2) will automatically utilize both of these
techniques guided by tunable parameters.

S3 supports two URI models:
- **Virtual-Host style** where the bucket name is the first dot-separated element of the URI's domain name
- **Path style** where the bucket name is the first slash-separated element of the URL's path

Currently, **Virtual-Host style** is the default for many S3 API implementations and tools/SDKs
used for S3 access. OCI Object Storage, however, prefers **Path style** for its support of the
S3 API. To configure the AIStore S3 backend to utilize **Path style**, issue the following:

```sh
$ ais config cluster features S3-Use-Path-Style
```

The [OCI Golang SDK](https://github.com/oracle/oci-go-sdk) provides parity with most of the
[AWS Golang SDK](http://github.com/aws/aws-sdk-go-v2) including support for MPU and "ranged"
GETs mentioned above. A previous blog post (see
[AIS OCI Support](https://aistore.nvidia.com/blog/2025/02/06/oci-object-storage-support))
detailed how MPU and MPD ("Multi-Part-Download", the GET complement to MPU) are configured
for automatic optimization when using the OCI Native backend.

## Comparing Read (GET) Performance: OCI Native API vs. S3 API

While utilizing MPD for fetching objects via the OCI Native API can be of some benefit in
particular cases, we will present a simple comparison for a single threaded GET here. We
construct a 10MB object and upload it to an OCI Object Storage bucket and compare GET
performance via both OCI Native and S3 APIs. Note that subsequent GETs of the same object
may benefit from caching in the pipeline between AIStore and the OCI Object Storage
infrastructure, so to avoid this we follow the PUT-Evict sequence on our test object ("z"):

| OCI Native backend      | S3 backend              |
| ----------------------- | ----------------------- |
| $ ais put z oc://edmc/  | $ ais put z s3://edmc/  |
| $ ais evict oc://edmc/z | $ ais evict s3://edmc/z |

Finally, we can perform a "cold" GET operation via either backend and compare the results:

| Timed GET command          | Duration    |
| -------------------------- | ----------- |
| $ time ais get oc://edmc/z | 1.8-2.7 sec |
| $ time ais get s3://edmc/z | 3.6-3.9 sec |

This is just one simple datapoint that happened to report the OCI Native API backend performing
somewhat better than the S3 API backend, but results will vary widely. Perhaps the bigger benefit
of utilizing the OCI Native API backend may be realized by avoiding the S3 emulating proxy tier
provided by the infrastructure that could impose a bottleneck under contention.

## Comparing Write (PUT) Performance: OCI Native API vs. S3 API

The write (PUT) path is a bit more interesting. While it is possible to use MPU APIs directly, it
is common for tools to automatically convert simple PUT requests to MPU API invocations either to
support very large (e.g. >> 5GiB) uploads or to optimize PUT performance. As mentioned above, the
[AWS Golang SDK](http://github.com/aws/aws-sdk-go-v2) provides precisely this optimization with
configurable parameters that have reasonable defaults (see also
[AWS Multipart size threshold](https://github.com/NVIDIA/aistore/blob/main/docs/cli/aws_profile_endpoint.md#multipart-size-threshold)).
The AIStore OCI Native backend provides
a similar set of parameters that we can examine here.

When utilizing the OCI Native backend, the automatic utilization of MPU is triggered by a PUT
object size of at least **OCI_MULTI_PART_UPLOAD_THRESHOLD** resulting in segments being uploaded
(many in parallel) of size up to **OCI_MAX_UPLOAD_SEGMENT_SIZE**. The current defaults for these two
parameters are:

| ENV variable                        | Default |
| :---------------------------------- | ------: |
| OCI_MAX_UPLOAD_SEGMENT_SIZE         |  256MiB |
| OCI_MULTI_PART_UPLOAD_THRESHOLD     |  512MiB |

There are other parameters affecting MPU as well. We will use a 64MB file/object ("z") this time
and either use the above defaults or the MPU-triggering values:

| ENV variable                        | MPU-few | MPU-many |
| :---------------------------------- | ------: | -------: |
| OCI_MAX_UPLOAD_SEGMENT_SIZE         |    10MB |      1MB |
| OCI_MULTI_PART_UPLOAD_THRESHOLD     |    20MB |      2MB

To compare the performance, we issue the following:

| OCI Native backend          | S3 backend                  |
| --------------------------- | --------------------------- |
| $ time ais put z oc://edmc/ | $ time ais put z oc://edmc/ |

Here is the comparison

| Backend      | PUT Duration  |
| :----------- | ------------: |
| S3           | 25.7-26.5 sec |
| OCI Default  | 53.8-55.9 sec |
| OCI MPU-few  | 23.3-25.9 sec |
| OCI MPU-many |  9.5-10.4 sec |

Here, we see how dramatically the MPU parameters can affect performance. The current AIStore
default OCI Native backend MPU settings result in this 64MB PUT taking roughly twice as long
as when uploaded via AIStore's S3 API backend to OCI Object Storage. Slicing up the 64MB file
into seven (7) segments (the last is only 4MB, the others all 10MB) reaches performance parity
with the S3 backend (likely due to the S3 SDK applying roughly the same "behind-the-scenes" MPU).
Utilizing 64 1MB segments more than doubles the performance of the S3 backend's result.

## Conclusion

This analysis hightlights how backend selection impacts performance in AIStore when working
with OCI Object Storage. While the S3 API provides interoperability, the OCI Native API
can outperform the S3 API in both reads and writes when properly tuned.

Key takeaways:
1. Read (GET) performance is better with the OCI Native API likely due to S3 API emulation and S3 to OCI Native proxy overhead
2. Write (PUT) performance benefits significantly from tuning MPU settings in the OCI Native backend
3. Proper configuration involves aligning MPU (and MPD if helpful) tuning parameters of the OCI Native backend with the intended object sizes and workload characteristics

## References

- [AIStore](https://github.com/NVIDIA/aistore)
- [AIS as a Fast-Tier](https://aistore.nvidia.com/blog/2023/11/27/aistore-fast-tier)
- [AIS OCI Support](https://aistore.nvidia.com/blog/2025/02/06/oci-object-storage-support)
- [OCI Golang SDK](https://github.com/oracle/oci-go-sdk)
- [OCI Object Storage](https://www.oracle.com/cloud/storage/object-storage/)
- [AWS Golang SDK](https://github.com/aws/aws-sdk-go-v2)
- [AWS Multipart size threshold](https://github.com/NVIDIA/aistore/blob/main/docs/cli/aws_profile_endpoint.md#multipart-size-threshold)
