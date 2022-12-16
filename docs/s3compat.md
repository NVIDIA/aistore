---
layout: post
title: S3COMPAT
permalink: /docs/s3compat
redirect_from:
 - /s3compat.md/
 - /docs/s3compat.md/
---

AIS supports Amazon S3 in two distinct and different ways:

1. On the back, via [backend](providers.md) abstraction. Specifically for S3 the corresponding [backend](providers.md) implementation currently utilizes [AWS SDK for Go](https://aws.amazon.com/sdk-for-go);
2. On the client-facing front, AIS provides S3 compatible API, so that existing S3 applications could use AIStore out of the box and without the need to change their (existing) code.

This document talks about the latter - about AIS providing S3 compatible API.

There's a separate, albeit closely related, [document](/docs/s3cmd.md) that explains how to configure `s3cmd` and then maybe tweak AIStore configuration to work with it:

* [Getting Started with `s3cmd`](/docs/s3cmd.md) - also contains configuration, tips, usage examples, and more.

For additional background, see:

* [High-level AIS block diagram](overview.md#at-a-glance) that shows frontend and backend APIs and capabilities.
* [Setting custom S3 endpoint](/docs/cli/bucket.md) can come in handy when a bucket is hosted by an S3 compliant backend (such as, e.g., minio).

## Table of Contents

- [Quick example using `aws` CLI](#quick-example-using-aws-cli)
- [Quick example using Internet Browser](#quick-example-using-internet-browser)
- [`s3cmd` command line](#s3cmd-command-line)
- [ETag and MD5](#etag-and-md5)
- [Last Modification Time](#last-modification-time)
- [Multipart Upload using `aws`](#multipart-upload-using-aws)
- [More Usage Examples](#more-usage-examples)
  - [Create bucket](#create-bucket)
  - [Remove bucket](#remove-bucket)
- [TensorFlow Demo](#tensorflow-demo)
- [S3 Compatibility](#s3-compatibility)
  - [Supported S3](#supported-s3)
  - [Unsupported S3](#unsupported-s3)
- [Boto3 Compatibility](#boto3-compatibility)
- [Amazon CLI tools](#amazon-cli-tools)

## Quick example using `aws` CLI

The following was tested with an _older_ version of `aws` CLI, namely:

```console
$ aws --version
aws-cli/1.15.58 Python/3.5.2 Linux/5.4.0-124-generic botocore/1.10.57
```

You can create buckets and execute PUT/GET verbs, etc.

```console
$ aws --endpoint-url http://localhost:8080/s3 s3 mb s3://abc
make_bucket: abc
```

```console
# PUT using AIS CLI:
$ ais put README.md ais://abc

# The same via `aws`:
PUT "README.md" to ais://abc
$ aws --endpoint-url http://localhost:8080/s3 s3api put-object --bucket abc --key LICENSE --body LICENSE
$ ais ls ais://abc
NAME             SIZE
LICENSE          1.05KiB
README.md        10.44KiB
```

and GET as well:

```console
$ aws --endpoint-url http://localhost:8080/s3 s3api get-object --bucket abc --key README.md /tmp/readme
{
    "ContentType": "text/plain; charset=utf-8",
    "Metadata": {},
    "ContentLength": 10689
}
$ diff -uN README.md /tmp/readme
```

## Quick example using Internet Browser

AIStore gateways provide HTTP/HTTPS interface, which is also why it is maybe sometimes convenient (and very fast) to use your Browser to execute `GET` type queries.

Specifically - since in this document we are talking about s3-compatible API - here's an example that utilizes `/s3` endpoint to list all buckets:

```xml
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01">
  <Owner>
     <ID>1</ID>
     <DisplayName>ListAllMyBucketsResult</DisplayName>
  </Owner>
<Buckets>
  <Bucket>
    <Name>bucket-111</Name>
    <CreationDate>2022-08-23T09:16:40-04:00</CreationDate>
    <String>Provider: ais</String>
  </Bucket>
  <Bucket>
    <Name>bucket-222</Name>
    <CreationDate>2022-08-23T13:47:00-04:00</CreationDate>
    <String>Provider: ais</String>
  </Bucket>
  <Bucket>
  <Name>bucket-222</Name>
    <CreationDate>2022-08-23T13:21:21-04:00</CreationDate>
    <String>Provider: aws (WARNING: {bucket-222, Provider: ais} and {bucket-222, Provider: aws} share the same name)</String>
  </Bucket>
  <Bucket>
    <Name>bucket-333</Name>
    <CreationDate>2022-08-23T13:26:32-04:00</CreationDate>
    <String>Provider: gcp</String>
  </Bucket>
</Buckets>
</ListBucketResult>
```

> Notice the "sharing the same name" warning above. For background, please refer to [backend providers](/docs/providers.md).

> In re `/s3 endpoint` mentioned above, the corresponding request URL in the browser's address bar would look something like `ais-gateway-host:port/s3`.

## `s3cmd` command line

The following table enumerates some of the `s3cmd` options that may appear to be useful:

| Options | Usage | Example |
| --- | --- | --- |
| `--host` | Define an AIS cluster endpoint | `--host=10.10.0.1:51080/s3` |
| `--host-bucket` | Define URL path to access a bucket of an AIS cluster | `--host-bucket="10.10.0.1:51080/s3/%(bucket)"` |
| `--no-ssl` | Use HTTP instead of HTTPS | |
| `--no-check-certificate` | Disable checking server's certificate in case of self-signed ones | |
| `--region` | Define a bucket region | `--region=us-west-1` |

## ETag and MD5

When you are reading an object from Amazon S3, the response will contain [ETag](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag).

Amazon S3 ETag is the object's checksum. Amazon computes those checksums using `md5`.

On the other hand, the default checksum type that AIS uses is [xxhash](http://cyan4973.github.io/xxHash/).

Therefore, it is advisable to:

1. keep in mind this dichotomy, and
2. possibly, configure AIS bucket in question with `md5`.

Here's a simple scenario:

Say, an S3-based client performs a GET or a PUT operation and calculates `md5` of an object that's being GET (or PUT). When the operation finishes, the client then compares the checksum with the `ETag` value in the response header. If checksums differ, the client raises the error "MD5 sum mismatch."

To enable MD5 checksum at bucket creation time:

```console
$ ais bucket create ais://bck --bucket-props="checksum.type=md5"
"ais://bck2" bucket created

$ ais show bucket ais://bck | grep checksum
checksum         Type: md5 | Validate: ColdGET
```

Or, you can change bucket's checksum type at any later time:

```console
$ ais bucket props ais://bck checksum.type=md5
Bucket props successfully updated
"checksum.type" set to:"md5" (was:"xxhash")
```

Please note that changing the bucket's checksum does not trigger updating (existing) checksums of *existing* objects - only new writes will be checksummed with the newly configured checksum.

## Last Modification Time

AIS tracks object last *access* time and returns it as `LastModified` for S3 clients. If an object has never been accessed, which can happen when AIS bucket uses a Cloud bucket as a backend one, zero Unix time is returned.

Example when access time is undefined (not set):

```console
# create AIS bucket with AWS backend bucket (for supported backends and details see docs/providers.md)
$ ais bucket create ais://bck
$ ais bucket props ais://bck backend_bck=aws://bckaws
$ ais bucket props ais://bck checksum.type=md5

# put an object using native ais API and note access time (same as creation time in this case)
$ ais object put object.txt ais://bck/obj-ais

# put object with s3cmd - the request bypasses ais, so no access time in the `ls` results
$ s3cmd put object.txt s3://bck/obj-aws --host=localhost:51080 --host-bucket="localhost:51080/s3/%(bucket)"

$ ais bucket ls ais://bck --props checksum,size,atime
NAME            CHECKSUM                                SIZE            ATIME
obj-ais         a103a20a4e8a207fe7ba25eeb2634c96        69.99KiB        08 Dec 20 11:25 PST
obj-aws         a103a20a4e8a207fe7ba25eeb2634c96        69.99KiB

$ s3cmd ls s3://bck --host=localhost:51080 --host-bucket="localhost:51080/s3/%(bucket)"
2020-12-08 11:25     71671   s3://test/obj-ais
1969-12-31 16:00     71671   s3://test/obj-aws
```

## Multipart Upload using `aws`

Example below reproduces the following [Amazon Knowledge-Center instruction](https://aws.amazon.com/premiumsupport/knowledge-center/s3-multipart-upload-cli/).

> Used `aws-cli/1.15.58 Python/3.5.2 Linux/5.15.0-46-generic botocore/1.10.57`

> Compare with (user-friendly and easy-to-execute) multipart examples from the [s3cmd companion doc](/docs/s3cmd.md).

But first and separately, we create `ais://` bucket and configure it with MD5:

```console
$ ais create ais://abc
"ais://abc" created (see https://github.com/NVIDIA/aistore/blob/master/docs/bucket.md#default-bucket-properties)
$ ais bucket props set ais://abc checksum.type=md5
Bucket props successfully updated
"checksum.type" set to: "md5" (was: "xxhash")
```

Next, the uploading sequence:

```console
# 1. initiate multipart upload
$ aws s3api create-multipart-upload --bucket abc --key large-test-file --endpoint-url http://localhost:8080/s3                                   {
    "Key": "large-test-file",
    "UploadId": "uu3DuXsJG",
    "Bucket": "abc"
}
```

```console
# 2. upload the first part (w/ upload-id copied from the previous command)
$ aws s3api upload-part --endpoint-url http://localhost:8080/s3 --bucket abc --key large-test-file --part-number 1 --body README.md --upload-id uu3DuXsJG
{
    "ETag": "9bc8111718e22a34f9fa6a099da1f3df"
}
```

```console
# 3. upload the second, etc. parts
$ aws s3api upload-part --endpoint-url http://localhost:8080/s3 --bucket abc --key large-test-file --part-number 2 --body LICENSE --upload-id uu3DuXsJG
{
    "ETag": "f70a21a0c5fa26a93820b0bef5be7619"
}
```

```console
# 4. list active upload by its ID (upload-id)
$ aws s3api list-parts --endpoint-url http://localhost:8080/s3 --bucket abc --key large-test-file --upload-id uu3DuXsJG                          {
    "Owner": null,
    "StorageClass": null,
    "Initiator": null,
    "Parts": [
        {
            "PartNumber": 1,
            "ETag": "9bc8111718e22a34f9fa6a099da1f3df",
            "Size": 10725
        },
        {
            "PartNumber": 2,
            "ETag": "f70a21a0c5fa26a93820b0bef5be7619",
            "Size": 1075
        }
    ]
}
```

And finally:

```console
# 5. complete upload, and be done
$ aws s3api complete-multipart-upload --endpoint-url http://localhost:8080/s3 --bucket abc --key large-test-file --multipart-upload file://up.json  --upload-id uu3DuXsJG
{
    "Key": "large-test-file",
    "Bucket": "abc",
    "ETag": "799e69a43a00794a86eebffb5fbaf4e6-2"
}
$ s3cmd ls s3://abc
2022-08-31 20:36        11800  s3://abc/large-test-file
```

Notice `file://up.json` in the `complete-multipart-upload` command. It simply contains the "Parts" section(**) copied from the "list active upload" step (above).

> (**) with no sizes

See https://aws.amazon.com/premiumsupport/knowledge-center/s3-multipart-upload-cli for details.


## More Usage Examples

Use any S3 client to access an AIS bucket. Examples below use standard AWS CLI. To access an AIS bucket, one has to pass the correct `endpoint` to the client. The endpoint is the primary proxy URL and `/s3` path, e.g, `http://10.0.0.20:51080/s3`.

### Create bucket

```shell
# check that AIS cluster has no buckets, and create a new one
$ ais bucket ls ais://
AIS Buckets (0)
$ s3cmd --host http://localhost:51080/s3 s3 mb s3://bck1
make_bucket: bck1

# list buckets via native CLI
$ ais bucket ls ais://
AIS Buckets (1)
```

### Remove bucket

```shell
$ s3cmd --host http://localhost:51080/s3 s3 ls s3://
2020-04-21 16:21:08 bck1

$ s3cmd --host http://localhost:51080/s3 s3 mb s3://bck1
remove_bucket: aws1
$ s3cmd --host http://localhost:51080/s3 s3 ls s3://
```

## TensorFlow Demo

Setup `S3_ENDPOINT` and `S3_USE_HTTPS` environment variables prior to running a TensorFlow job. `S3_ENDPOINT` must be primary proxy hostname:port and URL path `/s3` (e.g., `S3_ENDPOINT=10.0.0.20:51080/s3`). Secure HTTP is disabled by default, so `S3_USE_HTTPS` must be `0`.

Example running a training task:

```
S3_ENDPOINT=10.0.0.20:51080/s3 S3_USE_HTTPS=0 python mnist.py
```

TensorFlow on AIS training screencast:

![TF training in action](images/ais-s3-tf.gif)

## S3 Compatibility

AIStore fully supports [Amazon S3 API](https://docs.aws.amazon.com/s3/index.html) with a few exceptions documented and detailed below. The functionality has been tested using native Amazon S3 clients:

* [TensorFlow](https://docs.w3cub.com/tensorflow~guide/deploy/s3)
* [s3cmd](https://github.com/s3tools/s3cmd)
* [aws CLI](https://aws.amazon.com/cli)

Speaking of command-line tools, in addition to its own native [CLI](/docs/cli.md) AIStore also supports Amazon's `s3cmd` and `aws` CLIs. Python-based Amazon S3 clients that will often use Amazon Web Services (AWS) Software Development Kit for Python called [Boto3](https://github.com/boto/boto3) are also supported - see a note below on [AIS <=> Boto3 compatibility](#boto3-compatibility).

By way of quick summary, Amazon S3 supports the following API categories:

- Create and delete a bucket
- HEAD bucket
- Get a list of buckets
- PUT, GET, HEAD, and DELETE objects
- Get a list of objects in a bucket (important options include name prefix and page size)
- Copy object within the same bucket or between buckets
- Multi-object deletion
- Get, enable, and disable bucket versioning

and a few more. The following table summarizes S3 APIs and provides the corresponding AIS (native) CLI, as well as [s3cmd](https://github.com/s3tools/s3cmd) and [aws CLI](https://aws.amazon.com/cli) examples (along with comments on limitations, if any).

> See also: a note on [AIS <=> Boto3 compatibility](#boto3-compatibility).

### Supported S3

| API | AIS CLI and comments | [s3cmd](https://github.com/s3tools/s3cmd) | [aws CLI](https://aws.amazon.com/cli) |
| --- | --- | --- | --- |
| Create bucket | `ais bucket create ais://bck` (note: consider using S3 default `md5` checksum - see [discussion](#object-checksum) and examples below) | `s3cmd mb` | `aws s3 mb` |
| Head bucket | `ais bucket show ais://bck` | `s3cmd info s3://bck` | `aws s3api head-bucket` |
| Destroy bucket (aka "remove bucket") | `ais bucket rm ais://bck` | `s3cmd rb`, `aws s3 rb` ||
| List buckets | `ais ls ais://` (or, same: `ais ls ais:`) | `s3cmd ls s3://` | `aws s3 ls s3://` |
| PUT object | `ais object put filename ais://bck/obj` | `s3cmd put ...` | `aws s3 cp ..` |
| GET object | `ais object get ais://bck/obj filename` | `s3cmd get ...` | `aws s3 cp ..` |
| GET object(range) | `ais object get ais://bck/obj --offset 0 --length 10` | **Not supported** | `aws s3api get-object --range= ..` |
| HEAD object | `ais object show ais://bck/obj` | `s3cmd info s3://bck/obj` | `aws s3api head-object` |
| List objects in a bucket | `ais ls ais://bck` | `s3cmd ls s3://bucket-name/` | `aws s3 ls s3://bucket-name/` |
| Copy object in a given bucket or between buckets | S3 API is fully supported; we have yet to implement our native CLI to copy objects (we do copy buckets, though) | **Limited support**: `s3cmd` performs GET followed by PUT instead of AWS API call | `aws s3api copy-object ...` calls copy object API |
| Last modification time | AIS always stores only one - the last - version of an object. Therefore, we track creation **and** last access time but not "modification time". | - | - |
| Bucket creation time | `ais bucket show ais://bck` | `s3cmd` displays creation time via `ls` subcommand: `s3cmd ls s3://` | - |
| Versioning | AIS tracks and updates versioning information but only for the **latest** object version. Versioning is enabled by default; to disable, run: `ais bucket props ais://bck versioning.enabled=false` | - | `aws s3api get/put-bucket-versioning` |
| ACL | Limited support; AIS provides an extensive set of configurable permissions - see `ais bucket props ais://bck access` and `ais auth` and the corresponding documentation | - | - |
| Multipart upload(**) | - (added in v3.12) | `s3cmd put ... s3://bck --multipart-chunk-size-mb=5` | `aws s3api create-multipart-upload --bucket abc ...` |

> (**) With the only exception of [UploadPartCopy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html) operation.

### Unsupported S3

* Amazon Regions (us-east-1, us-west-1, etc.)
* Retention Policy
* CORS
* Website endpoints
* CloudFront CDN

## Boto3 Compatibility

Arguably, extremely few HTTP client-side libraries do _not_ follow [HTTP redirects](https://www.rfc-editor.org/rfc/rfc7231#page-54), and Amazon's [botocore](https://github.com/boto/botocore), used by [Boto3](https://github.com/boto/boto3), just happens to be one of those (libraries).

AIStore provides a shim that you can use to alter `botocore` and `boto3`'s behavior to work as expected with AIStore.

To use `boto3` or `botocore` as client libraries for AIStore:

 - Install the [aistore python api](/docs/s3cmd.md) with the `botocore` extra.

```shell
$ pip install aistore[botocore]
```

 - Import `aistore.botocore_patch.botocore` in your source code alongside `botocore` and / or `boto3`.

```python
import boto3
from aistore.botocore_patch import botocore
```

For more context, see perhaps the following `aws-cli` ticket and discussion at:

* [Support S3 HTTP redirects to non-Amazon URI's](https://github.com/aws/aws-cli/issues/6559)

## Amazon CLI tools

As far as existing Amazon-native CLI tools, `s3cmd` would be the preferred and recommended option. Please see [`s3cmd` readme](/docs/s3cmd.md) for usage examples and a variety of topics, including:

- [`s3cmd` Configuration](/docs/s3cmd.md#s3cmd-configuration)
- [Getting Started](/docs/s3cmd.md#getting-started)
  - [1. AIS Endpoint](/docs/s3cmd.md#1-ais-endpoint)
  - [2. How to have `s3cmd` calling AIS endpoint](/docs/s3cmd.md#2-how-to-have-s3cmd-calling-ais-endpoint)
  - [3. Alternatively](/docs/s3cmd.md#3-alternatively)
  - [4. Note and, possibly, update AIS configuration](/docs/s3cmd.md#4-note-and-possibly-update-ais-configuration)
  - [5. Create bucket and PUT/GET objects using `s3cmd`](/docs/s3cmd.md#5-create-bucket-and-putget-objects-using-s3cmd)
  - [6. Multipart upload using `s3cmd`](/docs/s3cmd.md#6-multipart-upload-using-s3cmd)

