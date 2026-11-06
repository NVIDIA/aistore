## Supported Checksums and Brief Theory of Operations

1. `xxhash` is the system-default checksum.

2. `xxhash` can be overridden on a bucket level; the following [CLI](/docs/cli.md) example configures bucket `abc` with `sha256` and bucket `xyz` without any checksum protection whatsoever:

	```console
	$ ais bucket props ais://abc checksum.type  <TAB-TAB>
	crc32c   md5      none     sha256   sha512   xxhash

	$ ais bucket props ais://abc checksum.type sha256
	Bucket props successfully updated
	"checksum.type" set to:"sha256" (was:"xxhash")

	$ ais bucket props ais://xyz checksum.type none
	Bucket props successfully updated
	"checksum.type" set to:"none" (was:"xxhash")
	```

	> AIS-own metadata, both cluster-level and object metadata, is currently always protected with `xxhash`.

3. Unless checksum is disabled, objects stored in this bucket are protected with the checksum; user can override the system default on a bucket level by setting checksum=`none` (see example above).

4. Bucket (re)configuration can be done at any time. For instance, bucket's checksumming option can be changed from `xxhash` to `sha512`,  and later to `crc32c`, and then back to `xxhash` - multiple times with no limitations.

5. An object with a bad checksum cannot be read from the bucket and cannot be replicated or migrated. Corrupted objects get eventually removed from the system.

6. GET and PUT operations support an option to validate checksums; validation is done against a checksum stored with an object (GET), or a checksum provided by a user (PUT).

7. Checksum configuration supports a number of options that can be changed both globally (for the entire cluster) and on a bucket level - notice the defaults below:

	```json
		"checksum": {
			"type":			"xxhash",
			"validate_cold_get":	true,      # validate cold GET from Cloud buckets
			"validate_warm_get":	false,     # validate warm GET
			"validate_obj_move":	false,     # validate object migration
			"enable_read_range":	false      # enable checksumming for ranges
		},
	```

8. In more detail:

	* `checksum.type` (`string`): supports a number of checksums including `xxhash` (the current default);
	* `checksum.validate_cold_get` (`bool`): indicates whether to perform checksum validation when cold GET-ing objects from Cloud buckets;
	* `checksum.validate_warm_get` (`bool`): prescribes whether to perform checksum validation when reading objects stored in AIS cluster;
	* `checksum.enable_read_range` (`bool`): indicates whether to generate checksums when executing GET(object, range), where `range` is offset and length (in bytes) to read;
	* `checksum.validate_obj_move` (`bool`): indicates whether to perform checksum validation upon object migration.

9. Object replication is always checksum-protected. If an object does not have a checksum (see #3 above), the latter gets computed on the fly and stored with the object, so that subsequent replications/migrations could reuse it.

10. Finally, when two objects in the cluster have identical (bucket, object) names and identical checksums, they are considered to be full replicas of each other - the fact that allows optimizing PUT, replication, and object migration in a variety of use cases.

## Chunked objects

A chunked object carries two kinds of checksums:

- per-chunk: CRC32C, stored with each chunk
- whole-object: reported by object `HEAD` via `Ais-Checksum-Type` and `Ais-Checksum-Value`

When computed locally, the whole-object checksum is either the bucket's configured `checksum.type` or CRC32C:

- configured type: computed while writing, provided the chunks arrive strictly in order (each chunk written in full before the next one starts)
- CRC32C: otherwise; combined from the per-chunk CRC32C values in object order, without rereading the data

Both are valid checksums of the same content. Which one a given object gets depends on how it was written. Blob download can also preserve a remote whole-object checksum, as described below.

### Multipart upload

- AIStore computes CRC32C for every part (and MD5, for S3 ETag compatibility).
- Parts uploaded sequentially, in order: configured type.
- Parts uploaded concurrently (the usual client behavior) or out of order: CRC32C.

For example, the same 3MiB file uploaded in 1MiB parts, concurrently and then sequentially, into a bucket configured with `xxhash`:

```console
$ ais put data.bin ais://abc/obj1 --chunk-size 1MiB
$ ais put data.bin ais://abc/obj2 --chunk-size 1MiB --num-workers 1

$ ais object show ais://abc/obj1 --props checksum
PROPERTY         VALUE
checksum         crc32c[dc14d650]

$ ais object show ais://abc/obj2 --props checksum
PROPERTY         VALUE
checksum         xxhash[99f1f1796627872f]
```

### PUT chunked by AIStore

A regular (single-stream) PUT into a bucket with chunking enabled is written chunk by chunk, in order, and gets the configured type.

### Blob download

- With `checksum.type=none`, no checksums are computed.
- If the remote object supplies a whole-object checksum and `checksum.validate_cold_get` is false, that checksum is stored as is.
- Otherwise, configured type when chunks arrive in order; CRC32C when they overlap or arrive out of order.
- Blob download does not validate the downloaded payload against the remote whole-object checksum. Setting `checksum.validate_cold_get=true` selects a locally computed checksum.

### Cold GET with parallel range reads

A cold GET that qualifies for parallel range reads preserves the remote whole-object checksum when supplied. Otherwise, it combines per-chunk CRC32C in object order, without rereading the data. With `checksum.type=none`, no checksum is stored. The cold GET qualifies when all of the following hold:

- chunking is enabled in the bucket
- the object is at least the minimum size for parallel cold GET (see [limits](/docs/howto_large_cold_objects.md#limits-and-defaults))
- `checksum.validate_cold_get` is false
- erasure coding is disabled
- the backend is not OCI

To keep the configured type, set `checksum.validate_cold_get=true`: the cold GET then reads the object as a single stream.

### Copying chunked objects

A copy within the same target determines the destination's whole-object checksum as follows:

- Destination bucket with `checksum.type=none`: no whole-object checksum; existing per-chunk checksums are preserved.
- Source checksum of the destination's configured type, or CRC32C: the copy reuses the source's whole-object and per-chunk checksums without hashing the data.
- Otherwise: the copy combines per-chunk CRC32C in object order, computing CRC32C during the copy only for chunks that lack it.

For objects sent to another target, the receiver applies the destination bucket's checksum configuration. A bucket copy uses both paths, so after copying a bucket of chunked objects into a bucket configured with `checksum.type=md5`, some objects may report CRC32C and others MD5.
