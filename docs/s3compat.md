## Table of Contents

- [Overview](#overview)
- [S3 Compatibility](#s3-compatibility)
- [Examples](#examples)
- [TensorFlow Demo](#tensorflow-demo)

## Overview

AIS cluster provides an AWS S3 compatibility layer for clients to access AIS buckets as regular S3 ones.

S3 supports the following API requests:

- Create and delete a bucket
- HEAD bucket
- Get a list of buckets
- PUT, GET, HEAD, and DELETE an object
- Get a list of objects in a bucket (name prefix and paging are supported)
- Copy an object (within the same bucket or from one bucket to another one)
- Multiple object deletion
- Get, enable, and disable bucket versioning (though, multiple versions of the same object are not supported yet. Only the last version of an object is accessible)

## S3 Compatibility

S3 API provides `ETag` in the response header that is the object's checksum.
Amazon S3 calculates a checksum using `md5` algorithm.
The default checksum type for an AIS cluster is `xxhash`.

A client usually calculates the object's checksum on the fly during GET and PUT operations.
When the operation finishes, the client compares the calculated checksum and the value of `ETag` in the response header.
If checksums differ, the client raises the error "MD5 sum mismatch".

To enable MD5 checksum, create a bucket with `MD5`:

```console
$ ais create bucket ais://bck --bucket-props="checksum.type=md5"
"ais://bck2" bucket created
$ ais show props ais://bck | grep checksum
checksum         Type: md5 | Validate: ColdGET
```

or change bucket's checksum type **before** putting objects to the bucket:

```console
$ ais set props ais://bck checksum.type=md5
Bucket props successfully updated
"checksum.type" set to:"md5" (was:"xxhash")
```

Please note that changing the bucket's checksum does not recalculate `ETag` for *existing* objects. The existing `ETag` value will be recalculated using a new checksum type when an object is updated.

## Examples

Use any S3 client to access an AIS bucket. Examples below use standard AWS CLI. To access an AIS bucket, one has to pass the correct `endpoint` to the client. The endpoint is the primary proxy URL and `/s3` path, e.g, `http://10.0.0.20:8080/s3`.

### Create a bucket

```shell
# check that AIS cluster has no buckets, and create a new one
$ ais ls ais://
AIS Buckets (0)
$ s3cmd --host http://localhost:8080/s3 s3 mb s3://bck1
make_bucket: bck1

# check that the bucket appears everywhere
$ ais ls ais://
AIS Buckets (1)
```

### Remove a bucket

```shell
$ s3cmd --host http://localhost:8080/s3 s3 ls s3://
2020-04-21 16:21:08 bck1

$ s3cmd --host http://localhost:8080/s3 s3 mb s3://bck1
remove_bucket: aws1
$ s3cmd --host http://localhost:8080/s3 s3 ls s3://
$
```

## TensorFlow Demo

Set up `S3_ENDPOINT` and `S3_USE_HTTPS` environment variables prior to running a TensorFlow job. `S3_ENDPOINT` must be primary proxy hostname:port and URL path `/s3`(e.g., `S3_ENDPOINT=10.0.0.20:8080/s3`). Secure HTTP is disabled by default. so `S3_USE_HTTPS` must be `0`.

Example of running a training task:

```
S3_ENDPOINT=10.0.0.20:8080/s3 S3_USE_HTTPS=0 python mnist.py
```

Here is the screencast of the TF training in action:

<img src="/docs/images/ais-s3-tf.gif" alt="TF training in action">
