## Validate Warm GET

One way to deal with out-of-band updates is to configure aistore bucket, as follows:

```console
$ ais bucket props set s3://abc versioning.validate_warm_get true
"versioning.validate_warm_get" set to: "true" (was: "false")
```

Here, `s3://abc` is presumably an Amazon S3 bucket, but it could be any Cloud or remote AIS bucket.

> It could also be any `ais://` bucket with Cloud or remote AIS backend. For usage,  see `backend_bck` option in CLI documentation and examples.

Once `validate_warm_get` is set, any read operation on the bucket will take a bit of extra time to compare in-cluster and remote object metadata.

Further, if and when this comparison fails, aistore performs a _cold_ GET, to make sure that it has the latest version.

Needless to say, the latest version will be always returned to the user as well.

## Out-of-band writes, deletes, and more...

1. with version validation enabled, aistore will detect both out-of-band writes and deletes;
2. buckets with versioning disabled are also supported;
3. decision on whether to perform cold-GET is made upon comparing remote and local metadata;
4. the latter always includes object size, but also may include any combination of:
   - `version`
   - `ETag`
   - `ais object checksum` (by default, xxhash that we store as part of custom Cloud metadata)
   - `MD5`
   - `CRC32C`

To enable version validation, run:

```console
$ ais bucket props set BUCKET versioning.validate_warm_get true

## optionally:

$ ais bucket props show BUCKET versioning
PROPERTY                         VALUE
versioning.enabled               ...
versioning.validate_warm_get     true
versioning.sync_warm_get         false
```

No assumption is being made on whether any of the above is present (except, of course, the size aka "Content-Length").

The rules are simple:

* compare _existing_ items of the same kind (`size` vs `size`, `MD5` and `MD5`, etc.);
* fail immediately - that is, require cold GET - if any pair of comparable items differ;
* count all matches except `size` (in other words, same size does _not_ contribute to decision in favor of skipping cold GET);
* exclude double counting (which is mostly relevant for `ETag` vs `MD5`);
* require **two or more matches**.

When there are no matches, we go ahead with cold GET.

A single match - e.g. only the `version` (if exists), or only `ETag`, etc. - is currently resolved positively iff the source backend is the same as well.

> E.g., copying object from Amazon to Google and then performing validated GET with aistore backend "pointing" to Google - will fail the match.

> TODO: make it configurable to require at least two matches.

Needless to say, if querying remote metadata fails the corresponding GET transaction will fail as well.

## When reading in-cluster data causes deletion

But there's one special condition when the call to query remote metadata returns "object not found". In other words, when the remote backend unambiguously indicates that the remote object does not exist (any longer).

In this case, there are two configurable choices as per (already shown) `versioning` section of the bucket config:

```console
$ ais bucket props show BUCKET versioning
PROPERTY                         VALUE
versioning.enabled               ...
versioning.validate_warm_get     true
versioning.sync_warm_get         false  ## <<<<<<<<<<<<<<<< note!
```

The knob called `versioning.sync_warm_get` is simply a stronger variant of the `versioning.validate_warm_get`;
that entails both:

1. validating remote object version, and
2. deleting in-cluster object if its remote ("cached") counterpart does not exist.

To recap:

if an attempt to read remote metadata returns "object not found", and `versioning.sync_warm_get` is set to `true`, then
we go ahead and delete the object locally, thus effectively _synchronizing_ in-cluster content with it's remote source.

## GET latest version

But sometimes, there may be a need to have a more fine-grained, operation level, control over this functionality.

AIS API supports that. In CLI, the corresponding option is called `--latest`. Let's see a brief example, where:

1. `s3:///abc` is a bucket that contains
2. `s3://abc/README.md` object that was previously
3. out-of-band updated

In other words, the setup we describe boils down to a single main point:

* aistore contains a different version of an object (in this example: `s3://abc/README.md`).

Namely:

```console
$ aws s3api list-object-versions --bucket abc --prefix README.md --max-keys 1
{
    "Name": "abc",
    "KeyMarker": "",
    "MaxKeys": 1,
    "IsTruncated": true,
    "NextVersionIdMarker": "KJOQsGcR3qBX5WvXbwiB.2LAQW12opbQ",
...
    "Versions": [
        {
            "IsLatest": true,
...
        }
    ],
    "Prefix": "README.md"
}
```

AIS, on the other hand, shows:


```console
$ ais show object s3://abc/README.md --props version
PROPERTY         VALUE
version          1yNHzpfd9Y16nDS71V5scjTMfbRZUPJI
```

Moreover, GET operation with default parameters doesn't help:

```console
$ ais get s3://abc/README.md /dev/null
GET (and discard) README.md from s3://abc (13.82KiB)

$ ais show object s3://abc/README.md --props version
PROPERTY         VALUE
version          1yNHzggpfd9Y16nDS71V5scjTMfbRZUPJI
```

To reconcile, we employ the `--latest` option:

```console
$ ais get s3://abc/README.md /dev/null --latest
GET (and discard) README.md from s3://abc (13.82KiB)

$ ais show object s3://abc/README.md --props version
PROPERTY         VALUE
version          KJOQsGcR3qBX5WvXbwiB.2LAQW12opbQ
```

Notice that we now have the latest `KJOQsGc...` version (that `s3api` also calls `VersionIdMarker`).
