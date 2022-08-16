---
layout: post
title: S3CMD
permalink: /docs/s3cmd
redirect_from:
 - /s3cmd.md/
 - /docs/s3cmd.md/
---

While the preferred and recommended management client for AIStore is its own [CLI](/docs/cli.md), Amazon's [`s3cmd`](https://s3tools.org/s3cmd) client can also be used, with certain minor limitations.

## TODO

Using `s3cmd` to operate on remote and AIS buckets - as long as those buckets can be unambiguously resolved by name.

```console
$ ais ls ais:
$ ais bucket create ais://abc
"ais://abc" created (see https://github.com/NVIDIA/aistore/blob/master/docs/bucket.md#default-bucket-properties)
$ ais bucket props set ais://abc checksum.type=md5
Bucket props successfully updated
"checksum.type" set to: "md5" (was: "xxhash")
$ s3cmd put README.md s3://abc
upload: 'README.md' -> 's3://abc/README.md'  [1 of 1]
 10689 of 10689   100% in    0s     3.13 MB/s  done
upload: 'README.md' -> 's3://abc/README.md'  [1 of 1]
 10689 of 10689   100% in    0s     4.20 MB/s  done
$ s3cmd rm s3://abc/README.md
delete: 's3://abc/README.md'
```

Similarly:

```console
$ ais ls s3:
aws://my-s3-bucket
...

$ s3cmd put README.md s3://my-s3-bucket
upload: 'README.md' -> 's3://my-s3-bucket/README.md'  [1 of 1]
 10689 of 10689   100% in    0s     3.13 MB/s  done
upload: 'README.md' -> 's3://abc/README.md'  [1 of 1]
 10689 of 10689   100% in    0s     4.20 MB/s  done
$ s3cmd rm s3://my-s3-bucket/README.md
delete: 's3://my-s3-bucket/README.md'
```

## Table of Contents

- [`s3cmd` Configuration](#s3cmd-configuration)
- [Getting Started](#getting-started)
  - [1. AIS Endpoint](#1-ais-endpoint)
  - [2. How to have `s3cmd` call AIS endpoint](#2-how-to-have-s3cmd-call-ais-endpoint)
  - [3. Alternatively](#3-alternatively)
  - [4. Notice and possibly update AIS configuration](#4-notice-and-possibly-update-ais-configuration)
  - [5. Create bucket and PUT/GET objects using `s3cmd`](#5-create-bucket-and-putget-objects-using-s3cmd)
- [S3 URI and Further References](#s3-uri-and-further-references)

## `s3cmd` Configuration

When using `s3cmd` the very first time, **or** if your AWS access credentials have changed, **or** if you'd want to change certain `s3cmd` defaults (also shown below) - in each one and all of those cases run `s3cmd --configure`.

**NOTE:** it is important to have `s3cmd` client properly configured.

For example:

```console
# s3cmd --configure

Enter new values or accept defaults in brackets with Enter.
Refer to user manual for detailed description of all options.

Access key and Secret key are your identifiers for Amazon S3. Leave them empty for using the env variables.
Access Key [ABCDABCDABCDABCDABCD]: EFGHEFGHEFGHEFGHEFGH
Secret Key [abcdabcdABCDabcd/abcde/abcdABCDabc/ABCDe]: efghEFGHefghEFGHe/ghEFGHe/ghEFghef/hEFGH
Default Region [us-east-2]:

Use "s3.amazonaws.com" for S3 Endpoint and not modify it to the target Amazon S3.
S3 Endpoint [s3.amazonaws.com]:

Use "%(bucket)s.s3.amazonaws.com" to the target Amazon S3. "%(bucket)s" and "%(location)s" vars can be used
if the target S3 system supports dns based buckets.
DNS-style bucket+hostname:port template for accessing a bucket [%(bucket)s.s3.amazonaws.com]:

Encryption password is used to protect your files from reading
by unauthorized persons while in transfer to S3
Encryption password:
Path to GPG program [/usr/bin/gpg]:

When using secure HTTPS protocol all communication with Amazon S3
servers is protected from 3rd party eavesdropping. This method is
slower than plain HTTP, and can only be proxied with Python 2.7 or newer
Use HTTPS protocol [Yes]:

On some networks all internet access must go through a HTTP proxy.
Try setting it here if you can't connect to S3 directly
HTTP Proxy server name:

New settings:
  Access Key: EFGHEFGHEFGHEFGHEFGH
  Secret Key: efghEFGHefghEFGHe/ghEFGHe/ghEFghef/hEFGH
  Default Region: us-east-2
  S3 Endpoint: s3.amazonaws.com
  DNS-style bucket+hostname:port template for accessing a bucket: %(bucket)s.s3.amazonaws.com
  Encryption password:
  Path to GPG program: /usr/bin/gpg
  Use HTTPS protocol: True
  HTTP Proxy server name:
  HTTP Proxy server port: 0

Test access with supplied credentials? [Y/n] n
Save settings? [y/N] y
Configuration saved to '/home/.s3cfg'
```

> It is maybe a good idea to also notice the version of the `s3cmd` you have, e.g.:

```console
$ s3cmd --version
s3cmd version 2.0.1
```

## Getting Started

In this section we walk the most basic and simple (and simplified) steps to get `s3cmd` to conveniently work with AIStore.

### 1. AIS Endpoint

With `s3cmd` client configuration safely stored in `$HOME/.s3cfg`, the next immediate step is to figure out AIS endpoint

> AIS cluster must be running, of course.

The endpoint consists of a gateway's hostname and its port followed by `/s3` suffix.

> AIS clusters usually run multiple gateways all of which are equivalent in terms of supporting all operations and providing access (to their respective clusters).

For example: given AIS gateway at `10.10.0.1:51080` (where `51080` would be the gateway's listening port), AIS endpoint then would be `10.10.0.1:51080/s3`.

> **NOTE** the `/s3` suffix. It is important to have it in all subsequent `s3cmd` requests to AIS, and the surest way to achieve that is to have it in the endpoint.

### 2. How to have `s3cmd` call AIS endpoint

But then the question is, how to transfer AIS endpoint into `s3cmd` commands. There are several ways; the one we show here is probably the easiest, exemplified by the `diff` below:

```sh
# diff -uN .s3cfg.orig $HOME/.s3cfg
--- .s3cfg.orig   2022-07-18 09:42:36.502271267 -0400
+++ .s3cfg        2022-07-18 10:14:50.878813029 -0400
@@ -29,8 +29,8 @@
 gpg_encrypt = %(gpg_command)s -c --verbose --no-use-agent --batch --yes --passphrase-fd %(passphrase_fd)s -o %(output_file)s %(input_file)s
 gpg_passphrase =
 guess_mime_type = True
-host_base = s3.amazonaws.com
-host_bucket = %(bucket)s.s3.amazonaws.com
+host_base = 10.10.0.1:51080/s3
+host_bucket = 10.10.0.1:51080/s3
 human_readable_sizes = False
 invalidate_default_index_on_cf = False
 invalidate_default_index_root_on_cf = True
```

Here we hack `s3cmd` configuration: replace Amazon's default `s3.amazonaws.com` endpoint with the correct one, and be done.

From this point on, `s3cmd` will be calling AIStore at 10.10.0.1:51080, with `/s3` suffix causing the latter to execute special handling (specifically) designed to support S3 compatibility.

### 3. Alternatively

Alternatively, instead of hacking `.s3cfg` once and for all we could use `--host` and `--host-bucket` command-line options (of the `s3cmd`). For instance:

```console
$ s3cmd put README.md s3://mmm/saved-readme.md --no-ssl --host=10.10.0.1:51080/s3 --host-bucket=10.10.0.1:51080/s3
```

> Compare with the identical `PUT` example [in the section 5 below](#5-create-bucket-and-putget-objects-using-s3cmd).

Goes without saying that, as long as `.s3cfg` keeps pointing to `s3.amazonaws.com`, the `--host` and `--host-bucket` must be explicitly specified in every `s3cmd` command.

### 4. Notice and possibly update AIS configuration

This next step actually depends on the AIStore configuration - the configuration of the cluster we intend to use with `s3cmd` client.

Specifically, there are two config knobs of interest:

```console
# ais config cluster net.http.use_https
PROPERTY                 VALUE
net.http.use_https       false

# ais config cluster checksum.type
PROPERTY         VALUE
checksum.type    xxhash
```

Note that HTTPS is `s3cmd` default, and so if AIStore runs on HTTP every single `s3cmd` command must have the `--no-ssl` option.

> Setting `net.http.use_https=true` requires AIS cluster restart. In other words, HTTPS is configurable but for the HTTP => HTTPS change to take an effect AIS cluster must be restarted.

> **NOTE** `--no-ssl` flag, e.g.: `s3cmd ls --no-ssl` to list buckets.

```console
$ s3cmd ls --host=10.10.0.1:51080/s3
```

If the AIS cluster in question is deployed with HTTP (the default) and not HTTPS:

```console
$ ais config cluster net.http
PROPERTY                         VALUE
net.http.server_crt              server.crt
net.http.server_key              server.key
net.http.write_buffer_size       65536
net.http.read_buffer_size        65536
net.http.use_https               false # <<<<<<<<< (NOTE) <<<<<<<<<<<<<<<<<<
net.http.skip_verify             false
net.http.chunked_transfer        true
```

we need turn HTTPS off in the `s3cmd` client using its `--no-ssl` option.

For example:

```console
$ s3cmd ls --host=10.10.0.1:51080/s3 --no-ssl
```

Secondly, there's the second important knob mentioned above: `checksum.type=xxhash` (where `xxhash` is the AIS's default).

However:

When using `s3cmd` with AIStore, it is strongly recommended to update the checksum to `md5`.

The following will update checksum type globally, on the level of the entire cluster:

```console
# This update will cause all subsequently created buckets to use `md5`.
# But note: all existing buckets will keep using `xxhash`, as per their own - per-bucket - configuration.

$ ais config cluster checksum.type
PROPERTY         VALUE
checksum.type    xxhash

# ais config cluster checksum.type=md5
{
    "checksum.type": "md5"
}
```

Alternatively, and preferably, update specific bucket's property (e.g. `ais://nnn` below):

```console
$ ais bucket props set ais://nnn checksum.type=md5

Bucket props successfully updated
"checksum.type" set to: "md5" (was: "xxhash")
```

### 5. Create bucket and PUT/GET objects using `s3cmd`

Once the 3 steps (above) are done, the rest must be really easy. Just start using `s3cmd` as [described](https://s3tools.org/s3cmd-howto), for instance:

```console
# Create bucket `mmm` using `s3cmd` make-bucket (`mb`) command:
$ s3cmd mb s3://mmm --no-ssl
Bucket 's3://mmm/' created

# And double-check it using AIS CLI:
$ ais ls ais:
AIS Buckets (2)
  ais://mmm
  ...
```

Not to forget to change the bucket's checksum to `md5` (needed iff the default cluster-level checksum != `md5`):

```console
$ ais bucket props set ais://mmm checksum.type=md5
```

PUT:

```console
$ s3cmd put README.md s3://mmm/saved-readme.md --no-ssl
```

GET:

```console
$ s3cmd get s3://mmm/saved-readme.md /tmp/copied-readme.md --no-ssl
download: 's3://mmm/saved-readme.md -> '/tmp/copied-readme.md'  [1 of 1]
```

And so on.

## S3 URI and Further References

Note that `s3cmd` expects S3 URI, simethin like `s3://bucket-name`.

In other words, `s3cmd` does not recognize any prefix other than `s3://`.

In the examples above, the `mmm` and `nnn` buckets are, actually, AIS buckets with no [remote backends](/docs/providers.md).

Nevertheless, when using `s3cmd` we have to reference them as `s3://mmm` and `s3://nnn`, respectively.

For table summary documenting AIS/S3 compatibility and further discussion, please see:

* [AIStore S3 compatibility](/docs/s3compat.md)
