---
layout: post
title: AWS_PROFILE_ENDPOINT
permalink: /docs/cli/aws_profile_endpoint
redirect_from:
 - /cli/aws_profile_endpoint.md/
 - /docs/cli/aws_profile_endpoint.md/
---

AIStore supports vendor-specific configuration on a per bucket basis. For instance, any bucket _backed up_ by an AWS S3 bucket (**) can be configured to use alternative:

* named AWS profiles (with alternative credentials and/or AWS region)
* s3 endpoints

(**) Terminology-wise, when we say "s3 bucket" or "google cloud bucket" we in fact reference a bucket in an AIS cluster that is either:

* (A) denoted with the respective `s3:` or `gs:` protocol schema, or
* (B) is a differently named AIS (that is, `ais://`) bucket that has its `backend_bck` property referencing the s3 (or google cloud) bucket in question.

For supported backends (that include, but are not limited, to AWS S3), see also:

* [Backend Provider](/docs/bucket.md#backend-provider)
* [Backend Bucket](/docs/bucket.md#backend-bucket)

## Table of Contents
- [Viewing vendor-specific properties](#viewing-vendor-specific-properties)
- [Environment variables](#environment-variables)
- [Setting profile with alternative access/secret keys and/or region](#setting-profile-with-alternative-accesssecret-keys-andor-region)
- [When bucket does not exist](#when-bucket-does-not-exist)
- [Configuring custom AWS S3 endpoint](#configuring-custom-aws-s3-endpoint)
- [Multipart size threshold](#multipart-size-threshold)

## Viewing vendor-specific properties

While `ais show bucket` will show all properties (which is a lengthy list), the way to maybe focus on vendor-specific extension is to look for the section called "extra". For example:

```console
$ ais show bucket s3://abc | grep extra
```
or, same:
```console
$ ais bucket props show s3://abc extra
```

The typical result would include the following defaults:

```console
extra.aws.cloud_region      us-east-2
extra.aws.endpoint
extra.aws.max_pagesize           0
extra.aws.multipart_size         0B
extra.aws.profile
```

Notice that the bucket's region (`cloud_region` above) is automatically populated when AIS looks up the bucket in s3. But the other two varables are settable and can provide alternative credentials and/or access endpoint.

## Environment variables

AIStore supports the well-known `S3_ENDPOINT` and `AWS_PROFILE` environment. While `S3_ENDPOINT` is often used to utilize AIS cluster as s3-providing service, configurable `AWS_PROFILE` specifies what's called a _named_ configuration profile:

* [Using named AWS profiles](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html#cli-configure-files-using-profiles)

The rule is simple:

* `S3_ENDPOINT` and `AWS_PROFILE` are loaded once upon AIS node startup.
* Bucket configuration takes **precedence** over the environment and can be changed at **any** time.

## Setting profile with alternative access/secret keys and/or region

Assuming, on the one hand:

```console
$ cat ~/.aws/config
[default]
region = us-east-2

[profile prod]
region = us-west-1
```
and

```console
$ cat ~/.aws/credentials
[default]
aws_access_key_id = foo
aws_secret_access_key = bar

[prod]
aws_access_key_id = 123
aws_secret_access_key = 456
```

on the other, we can then go ahead and set the "prod" profile directly into the bucket:

```console
$ ais bucket props set s3://abc extra.aws.profile prod
"extra.aws.profile" set to: "prod" (was: "")
```

and show resulting "extra.aws" configuration:

```console
$ ais show bucket s3://abc | grep extra
extra.aws.cloud_region      us-west-1
extra.aws.endpoint
extra.aws.profile           prod
```

From this point on, all calls to read, write, list `s3://abc` and get/set its properties will use AWS "prod" profile (see above).

## When bucket does not exist

But what if we need to set alternative profile (with alternative access and secret keys) on a bucket that does not yet exist in the cluster?

That must be a fairly common situation, and the way to resolve it is to use `--skip-lookup` option:

```console
$ ais create --help
...
OPTIONS:
   --props value   bucket properties, e.g. --props="mirror.enabled=true mirror.copies=4 checksum.type=md5"
   --skip-lookup   add Cloud bucket to aistore without checking the bucket's accessibility and getting its Cloud properties
                   (usage must be limited to setting up bucket's aistore properties with alternative profile and/or endpoint)


$ ais create s3://abc --skip-lookup
"s3://abc" created
```

Once this is done (**), we simply go ahead and run `ais bucket props set s3://abc extra.aws.profile` (as shown above). Assuming, the updated profile contains correct access keys, the bucket will then be fully available for reading, writing, listing, and all the rest operations.

> (**) `ais create` command results in adding the bucket to aistore `BMD` - a protected, versioned, and replicated bucket metadata that is further used to update properties of any bucket in the cluster, including certainly the one that we have just added.

## Configuring custom AWS S3 endpoint

When a bucket is hosted by an S3 compliant backend (such as, e.g., minio), we may want to specify an alternative S3 endpoint,
so that AIS nodes use it when reading, writing, listing, and generally, performing all operations on remote S3 bucket(s).

Globally, S3 endpoint can be overridden for _all_ S3 buckets via "S3_ENDPOINT" environment.

If you decide to make the change, you may need to restart AIS cluster while making sure that "S3_ENDPOINT" is available for the AIS nodes
when they are starting up.

But it can be also be done - and will take precedence over the global setting - on a per-bucket basis.

Here are some examples:

```console
# Let's say, there exists a bucket called s3://abc:
$ ais ls s3://abc
NAME             SIZE
README.md        8.96KiB
```

First, we override empty the endpoint property in the bucket's configuration.
To see that a non-empty value *applies* and works, we will use the default AWS S3 endpoint: `https://s3.amazonaws.com`

```console
$ ais bucket props set s3://abc extra.aws.endpoint=s3.amazonaws.com
Bucket "aws://abc": property "extra.aws.endpoint=s3.amazonaws.com", nothing to do
$ ais ls s3://abc
NAME             SIZE
README.md        8.96KiB
```

Second, set the endpoint=foo (or, it could be any other invalid value), and observe that the bucket becomes unreachable:

```console
$ ais bucket props set s3://abc extra.aws.endpoint=foo
Bucket props successfully updated
"extra.aws.endpoint" set to: "foo" (was: "s3.amazonaws.com")

$ ais ls s3://abc
RequestError: send request failed: dial tcp: lookup abc.foo: no such host
```

Finally, revert the endpoint back to empty, and check that the bucket is visible again:

```console
$ ais bucket props set s3://abc extra.aws.endpoint=""
Bucket props successfully updated
"extra.aws.endpoint" set to: "" (was: "foo")

$ ais ls s3://abc
NAME             SIZE
README.md        8.96KiB
```

> Global `export S3_ENDPOINT=...` override is static and readonly. Use it with extreme caution as it applies to all buckets.

> On the other hand, for any given `s3://bucket` its S3 endpoint can be set, unset, and otherwise changed at any time - at runtime. As shown above.


## Multipart size threshold

Multipart upload size threshold is, effectively, **yet another performance tunable** that, according to Amazon documentation, must be greater than or equal to 5MB.

Further,

- AWS SDK v2 sets the default `5MB`
  - see https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/feature/s3/manager
- AIS default, however, is `128MiB`
  - at time of this writing but check ais/s3/const.go for the most recent update

In addition, you can always configure a different value on a per-bucket basis, as follows:

```console
$ ais bucket props set s3://abc extra.aws.multipart_size
PROPERTY                         VALUE
extra.aws.multipart_size         0B

$ ais bucket props set s3://abc extra.aws.multipart_size 1gb
"extra.aws.multipart_size" set to: "1GiB" (was: "0B")

Bucket props successfully updated.
```

To show all AWS-specific settings for the bucket:


```console
$ ais bucket props set s3://abc extra.aws
PROPERTY                         VALUE
extra.aws.cloud_region           us-east-2
extra.aws.endpoint
extra.aws.max_pagesize           0
extra.aws.multipart_size         1GiB
extra.aws.profile
```

## Disabling MultiPart Uploads

Some 3rd party s3 providers don't fully support the `aws-chunked-encoding` used by the AWS SDK for multipart upload.

With [recent updates](https://github.com/aws/aws-sdk-go-v2/discussions/2960), the default for the s3 SDK is now to provide the checksum for any supported API, and to do so using their proprietary `aws-chunked-encoding`.
AWS provides a way to disable this with the option `request_checksum_calculation=when_required`.
However, as of writing, the s3 manager tool [does not support this option](https://github.com/aws/aws-sdk-go-v2/issues/3007).
For these backend buckets, the checksum cannot be read from the client and users will see this error:

```console
InvalidArgument: x-amz-content-sha256 must be UNSIGNED-PAYLOAD, or a valid sha256 value
```

To disable multipart uploads for compatibility with these backends (or any other reason), you can set

```console
extra.aws.multipart_size  -1
```

**NOTE:** Setting this to false will result in much slower "single-part" uploads
