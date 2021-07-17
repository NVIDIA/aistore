## Table of Contents

- [S3 Compatibility](#s3-compatibility)
- [Client Configuration](#client-configuration)
- [Object Checksum](#object-checksum)
- [Last Modification Time](#last-modification-time)
- [More Examples](#more-examples)
  - [Create bucket](#create-bucket)
  - [Remove bucket](#remove-bucket)
- [TensorFlow Demo](#tensorflow-demo)
- [Boto3 Compatibility](#boto3-compatibility)

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

and a few more. The following table summarizes S3 APIs and provides the corresponding AIS (native) CLI as well as [s3cmd](https://github.com/s3tools/s3cmd) and [aws CLI](https://aws.amazon.com/cli) examples along with comments on limitations - iff there are any. In the rightmost [aws CLI](https://aws.amazon.com/cli) column all mentions of `s3rproxy` refer to [AIS <=> Boto3 compatibility](#boto3-compatibility) at the end of this document.

| API | AIS CLI and comments | [s3cmd](https://github.com/s3tools/s3cmd) | [aws CLI](https://aws.amazon.com/cli) |
| --- | --- | --- | --- |
| Create bucket | `ais bucket create ais://bck` (note: consider using S3 default `md5` checksum - see [discussion](#object-checksum) and examples below) | `s3cmd mb` | `aws s3 mb` |
| Head bucket | `ais bucket show ais://bck` | `s3cmd info s3://bck` | `aws s3api head-bucket` |
| Destroy bucket (aka "remove bucket") | `ais bucket rm ais://bck` | `s3cmd rb`, `aws s3 rb` ||
| List buckets | `ais ls ais://` | `s3cmd ls s3://` | `aws s3 ls s3://` |
| PUT object | `ais object put filename ais://bck/obj` | `s3cmd put ...` | `aws s3 cp ..`(needs `s3rproxy` tag) |
| GET object | `ais object get ais://bck/obj filename` | `s3cmd get ...` | `aws s3 cp ..`(needs `s3rproxy` tag) |
| GET object(range) | `ais object get ais://bck/obj --offset 0 --length 10` | **Not supported** | `aws s3api get-object --range= ..`(needs `s3rproxy` tag) |
| HEAD object | `ais object show ais://bck/obj` | `s3cmd info s3://bck/obj` | `aws s3api head-object`(needs `s3rproxy` tag) |
| List objects in a bucket | `ais ls ais://bck` | `s3cmd ls s3://bucket-name/` | `aws s3 ls s3://bucket-name/`(needs `s3rproxy` tag) |
| Copy object in a given bucket or between buckets | S3 API is fully supported; we have yet to implement our native CLI to copy objects (we do copy buckets, though) | **Limited support**: `s3cmd` performs GET followed by PUT instead of AWS API call | `aws s3api copy-object ...` calls copy object API(needs `s3rpoxy` tag) |
| Regions | **Not supported**; AIS has a single built-in region called `ais`; regions sent by S3 clients are simply ignored. | - | - |
| Last modification time | AIS always stores only one - the last - version of an object. Therefore, we track creation **and** last access time but not "modification time". | - | - |
| Bucket creation time | `ais bucket show ais://bck` | `s3cmd` displays creation time via `ls` subcommand: `s3cmd ls s3://` | - |
| Versioning | AIS tracks and updates versioning information but only for the **latest** object version. Versioning is enabled by default; to disable, run: `ais bucket props ais://bck versioning.enabled=false` | - | `aws s3api get/put-bucket-versioning` |
| ACL | Limited support; AIS provides an extensive set of configurable permissions - see `ais bucket props ais://bck access` and `ais auth` and the corresponding documentation | - | - |
| Multipart (upload, download) | **Not supported** | - | - |
| Retention Policy | **Not supported** | - | - |
| CORS| **Not supported** | - | - |
| Website endpoints | **Not supported** | - | - |
| CloudFront CDN | **Not supported** | - | - |


## Client Configuration

The section explains how to configure `s3cmd` client to connect to AIStore.

You can run `s3cmd configure`, apply the simple rules listed below, and then use `s3cmd` **without repeating the same for each** `s3cmd` command.

To connect an AIS server, specify the correct AIS endpoint. The endpoint consists of a cluster gateway (aka proxy) hostname and its port followed by `/s3`. For example: given IP = `10.10.0.1` and AIS service port `51080` AIS endpoint would be `10.10.0.1:51080/s3`:

```console
$ s3cmd ls --host=10.10.0.1:51080/s3
```

If AIS cluster uses HTTP (default) and not HTTPS, turn off HTTPS in the client:

```console
$ s3cmd ls --host=10.10.0.1:51080/s3 --no-ssl
```

On the other hand, if the cluster has been deployed with HTTPS but it uses a self-signed certificate, the client may refuse the server certificate. In this case disable certificate check:

```console
$ s3cmd ls --host=10.10.0.1.51080/s3 --no-check-certificate
```

If the client complains about empty S3 region, you could specify any valid region as follows:

```console
$ s3cmd ls --host=10.10.0.1.51080/s3 --region us-west-1
```

That's all. The steps above will make all top-level commands to work.
Note, however, that accessing bucket's objects requires defining the path format for a bucket:

```console
$ s3cmd ls s3://buckename --host=10.10.0.1.51080/s3 --host-bucket="10.10.0.1:51080/s3/%(bucket)"
```

The following table summarizes all the options shown in the examples above:

| Options | Usage | Example |
| --- | --- | --- |
| `--host` | Define an AIS cluster endpoint | `--host=10.10.0.1:51080/s3` |
| `--host-bucket` | Define URL path to access a bucket of an AIS cluster | `--host-bucket="10.10.0.1:51080/s3/%(bucket)"` |
| `--no-ssl` | Use HTTP instead of HTTPS | |
| `--no-check-certificate` | Disable checking server's certificate in case of self-signed ones | |
| `--region` | Define a bucket region | `--region=us-west-1` |

## Object Checksum

S3 API provides `ETag` in the response header that is object's checksum.
Amazon S3 computes those checksums using `md5`. Note that the default checksum type that AIS uses is [xxhash](http://cyan4973.github.io/xxHash/).

Therefore, it is advisable to a) keep in mind this dichotomy and b) possibly, configure AIS bucket in question with `md5`. Here's a simple scenario when you may want to do the b):

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

## More Examples

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

<img src="/docs/images/ais-s3-tf.gif" alt="TF training in action">

## Boto3 Compatibility

Very few HTTP client-side libraries do _not_ follow HTTP redirects, and Amazon's [boto3](https://github.com/boto/boto3) just happens to be one of those (libraries).

To circumvent the limitation, we provided a special **build tag**: `s3rproxy`. Building `aisnode` executable with this tag causes each AIS proxy to become, effectively, reverse proxy vis-a-vis the rest clustered nodes.

> **NOTE**: reverse-proxying datapath requests might adversely affect performance! It is, therefore, strongly recommended _not_ to use `s3rproxy` build tag, if possible.

To build with `s3rproxy` tag (or any other supported build tag), simply specify the `TAGS` environment variable, for example:

```console
$ TAGS=s3rproxy make deploy
```

See [Makefile](https://github.com/NVIDIA/aistore/blob/master/Makefile) in the root directory for further details.
