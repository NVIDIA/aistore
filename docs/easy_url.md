## Definitions

"Easy URL" is a user-friendly mapping of AIS native APIs:

1. GET(object)
2. PUT(object)
3. list-objects(bucket)
4. list-buckets

The feature is intended to enable (convenient) usage of your Internet Browser (or `curl`, etc. tools) to run commands that look as follows:

* **GET http(s)://host:port/provider/[bucket[/object]]**
* **PUT http(s)://host:port/provider/[bucket[/object]]**,

where:

* `host` and `port` are, as usual, the hostname and listening port of any available AIS gateway (aka "proxy");
* `provider` is one of: (`gs` | `az` | `ais`), where the first 3 encode well-known Amazon S3, Google Cloud, and Microsoft Azure Blob Storage, respectively.
* `bucket` and `object` names are optional and further depend on a given operation (one of the 4 listed above).

Let's now assume that there is an AIS cluster with gateway at `10.0.0.207:51080`. The following example illustrates all 4 (four) "easy URLs":

```console
$ ais create ais://abc
"ais://abc" created

# 1. destination name is required:
$ curl -L -X PUT 'http://10.0.0.207:51080/ais/abc' -T README.md
Error: missing destination object name in the "easy URL": PUT /ais/abc

# 2. correct PUT:
$ curl -L -X PUT 'http://10.0.0.207:51080/ais/abc/qq' -T README.md

# 3. GET(object) as /tmp/qq:
$ curl -L -X GET 'http://10.0.0.207:51080/ais/abc/qq' -o /tmp/qq
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   132  100   132    0     0  66000      0 --:--:-- --:--:-- --:--:--  128k
100 10689  100 10689    0     0  3479k      0 --:--:-- --:--:-- --:--:-- 3479k

# 4. list-objects in a bucket, with `jq` to pretty-print JSON output:
$ curl -L -X GET 'http://10.0.0.207:51080/ais/abc' | jq
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   167  100   167    0     0  83500      0 --:--:-- --:--:-- --:--:-- 83500
{
  "uuid": "MksSfasdg",
  "continuation_token": "",
  "entries": [
    {
      "name": "qq",
      "checksum": "fd0f7acc8e278588",
      "atime": "24 Aug 22 10:00 EDT",
      "size": "10689",
      "flags": 64
    }
  ],
  "flags": 0
}

5. finally, list all Google Cloud buckets (that we have permissions to see):
$ curl -L -X GET 'http://10.0.0.207:51080/gs' | jq
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   507  100   507    0     0    533      0 --:--:-- --:--:-- --:--:--   533
[
  {
    "name": "bucket-111",
    "provider": "gcp",
    "namespace": {
      "uuid": "",
      "name": ""
    }
  },
  {
    "name": "bucket-222",
    "provider": "gcp",
    "namespace": {
      "uuid": "",
      "name": ""
    }
  },
...
...
]
```

> In the examples above, instead of `ais` provider we could as well use `gs` (Google Cloud) or `az`(Azure), etc.

## S3 compatibility - and one important distinction

In addition to its native REST API, AIS provides [S3-compatible API](/docs/s3compat.md) via `/s3` API endpoint.

For instance, the following performs a GET on an object in Amazon S3 bucket:

```console
# first, write it there
$ ais put README.md s3://my-s3-bucket

# GET the same and place output into /tmp/rr
$ curl -L -X GET 'http://10.0.0.207:51080/s3/my-s3-bucket/README.md' -o /tmp/rr
```

Notice that GET URL (above) looks indistinguishable from the "easy URL" examples from the previous section.

That, in fact, is a mere coincidence - and here's why:

* `/s3` is an API endpoint rather than a namesake provider;
* as such, `/s3` provides a a whole set of Amazon S3 compatible APIs whereby the output is xml-formated, etc. - as specified in the respective Amazon documentation.

To illustrate this distinction further, let's take a look at a `list-buckets` example using `/s3` endpoint:

```console
$ curl -L -X GET 'http://10.0.0.207:51080/s3'
```

```xml
<ListBucketResult>
  <Owner>
    <ID>1</ID>
    <DisplayName>ListAllMyBucketsResult</DisplayName>
  </Owner>
  <Buckets>
    <Bucket>
       <Name>abc</Name>
       <CreationDate>2022-08-24T09:59:56-04:00</CreationDate>
       <String>Provider: ais</String>
    </Bucket>
    <Bucket>
    ...
    </Bucket>
</Buckets>
</ListBucketResult>
```

Now, if you compare this with the example from the previous section (where we used GET 'http://10.0.0.207:51080/gs' URL) - the difference must become clear:

* GET 'http://10.0.0.207:51080/s3' implements Amazon S3 `list-buckets` API; as such it must report all buckets (across all providers and not only `s3`) that can be accessed, read, and written.
* GET 'http://10.0.0.207:51080/gs' provides "easy URL" capability, whereby `gs` explicitly specifies the [backend provider](/docs/providers.md).

> Note that `/s3` and its subordinate URL paths currently can only "see" buckets that are already present in AIStore BMD, while native API, when given sufficient permissions, can immediately access (read, write, list) any remote buckets, while adding them to the BMD "on the fly".

> That is one of the limitations of *not* using native API.

## References

- [REST API](/docs/http_api.md)
