---
layout: post
title: S3COMPAT
permalink: docs/s3compat
redirect_from:
 - docs/s3compat.md/
---

## Table of Contents

- [Overview](#overview)
- [Examples](#examples)
- [TensorFlow Demo](#tensorflow-demo)

## Overview

AIS cluster provides an AWS S3 compatibility layer for client to assess AIS buckets as regular S3 ones.

S3 supports the following API requests:

- Create and delete a bucket
- HEAD bucket
- Get list of buckets
- PUT,GET, HEAD, and DELETE an object
- Get list of objects in a bucket (name prefix and paging are supported)
- Copy an object (within the same bucket or from one bucket to another one)
- Multiple object deletion
- Get, enable, and disable bucket versioning (though, multiple versions of the same object are not supported yet. Only the last version of an object is accessible)

## Examples

Use any S3 client to access AIS bucket. Examples below use standard AWS CLI. To access AIS bucket, one has to pass correct `endpoint` to the client. The endpoint is the primary proxy URL and `/s3` path, e.g, `http://10.0.0.20:8080/s3`.

### Create a bucket

```shell
# check that AIS cluster has no buckets, and create a new one
$ ais ls ais://
AIS Buckets (0)
$ aws --endpoint-url http://localhost:8080/s3 s3 mb s3://bck1
make_bucket: bck1

# check that the bucket appears everywhere
$ ais ls ais://
AIS Buckets (1)
```

### Remove a bucket

```shell
$ aws --endpoint-url http://localhost:8080/s3 s3 ls s3://
2020-04-21 16:21:08 bck1

$ aws --endpoint-url http://localhost:8080/s3 s3 mb s3://bck1
remove_bucket: aws1
$ aws --endpoint-url http://localhost:8080/s3 s3 ls s3://
$
```

## TensorFlow Demo

Set up `S3_ENDPOINT` and `S3_USE_HTTPS` environment variables prior to running a TensorFlow job. `S3_ENDPOINT` must be primary proxy hostname:port and URL path `/s3`(e.g., `S3_ENDPOINT=10.0.0.20:8080/s3`). Secure HTTP is disabled by default. so `S3_USE_HTTPS` must be `0`.

Example of running a training task:

```
S3_ENDPOINT=10.0.0.20:8080/s3 S3_USE_HTTPS=0 python mnist.py
```

Here is the screencast of the TF training in action:

<img src="/aistore/docs/images/ais-s3-tf.gif" alt="TF training in action">
