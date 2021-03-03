## Table of Contents

- [Overview](#overview)
- [Client Configuration](#client-configuration)
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

## Client Configuration

S3 client must be properly configured before connecting to an AIS cluster.
The section explains how to configure the `s3cmd` client with CLI options.
As an alternative, you can run `s3cmd configure`, apply the rules below, and then use `s3cmd` without repeating the same CLI arguments every time.

To connect an AIS server, pass the endpoint to the client.
The endpoint consists of a cluster gateway(proxy) IP and port followed by `/s3`.

Example: if a gateway IP is `10.10.0.1` and it listens on port `8080`, the endpoint is `10.10.0.1:8080/s3`:

```console
$ s3cmd ls --host=10.10.0.1:8080/s3
```

If an AIS cluster is deployed with HTTP protocol (default deployment disables HTTPS), turn off HTTPS in the client:

```console
$ s3cmd ls --host=10.10.0.1:8080/s3 --no-ssl
```

If the cluster deployed with HTTPS but it uses a self-signed certificate, the client may refuse the server certificate.
In this case disable certificate check:

```console
$ s3cmd ls --host=10.10.0.1.8080/s3 --no-check-certificate
```

If the client complains about an empty S3 region, you could specify any valid region - as follows:

```console
$ s3cmd ls --host=10.10.0.1.8080/s3 --region us-west-1
```

The steps above will make all top-level commands to work.
Note, however, that accessing bucket's objects requires defining the path format for a bucket:

```console
$ s3cmd ls s3://buckename --host=10.10.0.1.8080/s3 --host-bucket="10.10.0.1:8080/s3/%(bucket)"
```

The summary table of client settings:

| Options | Usage | Example |
| --- | --- | --- |
| `--host` | Define an AIS cluster endpoint | `--host=10.10.0.1:8080/s3` |
| `--host-bucket` | Define URL path to access a bucket of an AIS cluster | `--host-bucket="10.10.0.1:8080/s3/%(bucket)"` |
| `--no-ssl` | Use HTTP instead of HTTPS | |
| `--no-check-certificate` | Disable checking server's certificate in case of self-signed ones | |
| `--region` | Define a bucket region | `--region=us-west-1` |


## S3 Compatibility

### Object checksum

S3 API provides `ETag` in the response header that is the object's checksum.
Amazon S3 calculates a checksum using `md5` algorithm.
The default checksum type for an AIS cluster is `xxhash`.

A client usually calculates the object's checksum on the fly during GET and PUT operations.
When the operation finishes, the client compares the calculated checksum and the value of `ETag` in the response header.
If checksums differ, the client raises the error "MD5 sum mismatch".

To enable MD5 checksum, create a bucket with `MD5`:

```console
$ ais bucket create ais://bck --bucket-props="checksum.type=md5"
"ais://bck2" bucket created
$ ais show bucket ais://bck | grep checksum
checksum         Type: md5 | Validate: ColdGET
```

or change bucket's checksum type **before** putting objects to the bucket:

```console
$ ais bucket props ais://bck checksum.type=md5
Bucket props successfully updated
"checksum.type" set to:"md5" (was:"xxhash")
```

Please note that changing the bucket's checksum does not recalculate `ETag` for *existing* objects. The existing `ETag` value will be recalculated using a new checksum type when an object is updated.

### Last modification time

AIS tracks object last *access* time and returns it as `LastModified` for S3 clients. If an object has never been accessed, which can happen when AIS bucket uses a Cloud bucket as a backend one, zero Unix time is returned.

Example when access time is undefined (not set):

```console
# create an AIS bucket with AWS backend bucket
$ ais bucket create ais://bck
$ ais bucket props ais://bck backend_bck=aws://bckaws
$ ais bucket props ais://bck checksum.type=md5

# put an object with ais - it affects access time
$ ais object put object.txt ais://bck/obj-ais

# put another object with s3cmd - the object bypasses ais, so no access time in list
$ s3cmd put object.txt s3://bck/obj-aws --host=localhost:8080 --host-bucket="localhost:8080/s3/%(bucket)"

$ ais bucket ls ais://bck --props checksum,size,atime
NAME            CHECKSUM                                SIZE            ATIME
obj-ais         a103a20a4e8a207fe7ba25eeb2634c96        69.99KiB        08 Dec 20 11:25 PST
obj-aws         a103a20a4e8a207fe7ba25eeb2634c96        69.99KiB

$ s3cmd ls s3://bck --host=localhost:8080 --host-bucket="localhost:8080/s3/%(bucket)"
2020-12-08 11:25     71671   s3://test/obj-ais
1969-12-31 16:00     71671   s3://test/obj-aws
```

## Examples

Use any S3 client to access an AIS bucket. Examples below use standard AWS CLI. To access an AIS bucket, one has to pass the correct `endpoint` to the client. The endpoint is the primary proxy URL and `/s3` path, e.g, `http://10.0.0.20:8080/s3`.

### Create a bucket

```shell
# check that AIS cluster has no buckets, and create a new one
$ ais bucket ls ais://
AIS Buckets (0)
$ s3cmd --host http://localhost:8080/s3 s3 mb s3://bck1
make_bucket: bck1

# check that the bucket appears everywhere
$ ais bucket ls ais://
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
