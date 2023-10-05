## Validate Warm GET: a quick synopsys

1. with version validation enabled, aistore will now detect both out-of-band writes and deletes;
2. buckets with versioning disabled are also supported;
3. decision on whether to perform cold-GET is made upon comparing remote and local metadata;
4. the latter always includes object size, but also may include any combination of:
   - `version`
   - `ETag`
   - `ais object checksum` (by default, xxhash that we store as part of custom Cloud metadata)
   - `MD5`
   - `CRC32C`

To enable validation, run:

```console
$ ais bucket props set BUCKET versioning.validate_warm_get true

## optionally:

$ ais bucket props show BUCKET versioning
PROPERTY                         VALUE
versioning.enabled               ...
versioning.validate_warm_get     true
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

Needless to say, if querying remote metadata fails the corresponding GET will fail as well.

But there's one special condition when we receive "object not found". In this case, we go ahead and delete the object locally (returning the same "not found" to client).

This (behavior) is the current default; to disable local deletion, simply run:

```console
$ ais config cluster features <TAB-TAB>
Enforce-IntraCluster-Access        Do-not-Auto-Detect-FileShare   LZ4-Block-1MB                   Ignore-LimitedCoexistence-Conflict
Do-not-HEAD-Remote-Bucket          Provide-S3-API-via-Root        LZ4-Frame-Checksum              Dont-Rm-via-Validate-Warm-GET
Skip-Loading-VersionChecksum-MD    Fsync-PUT                      Dont-Allow-Passing-FQN-to-ETL   none

$ ais config cluster features Dont-Rm-via-Validate-Warm-GET
PROPERTY         VALUE
features         Dont-Rm-via-Validate-Warm-GET

Cluster config updated
```
