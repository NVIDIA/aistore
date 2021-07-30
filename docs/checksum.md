---
layout: post
title: CHECKSUM
permalink: docs/checksum
redirect_from:
 - docs/checksum.md/
---

## Supported Checksums and Brief Theory of Operations

1. `xxhash` is the system-default checksum.

2. `xxhash` can be overridden on a bucket level; the following [CLI](/cmd/cli/README.md) example configures bucket `abc` with `sha256` and bucket `xyz` without any checksum protection whatsoever:

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

3. unless checksum is disabled, objects stored in this bucket are protected with the checksum; user can override the system default on a bucket level by setting checksum=`none` (see example above).

4. bucket (re)configuration can be done at any time. For instance, bucket's checksumming option can be changed from `xxhash` to `sha512`,  and later to `crc32c`, and then back to `xxhash` - multiple times with no limitations.

5. an object with a bad checksum cannot be read from the bucket and cannot be replicated or migrated. Corrupted objects get eventually removed from the system.

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
	* `checksum.validate_warm_get` (`true` | `false`): prescribes whether to perform checksum validation when reading objects stored in AIS cluster;
	* `checksum.enable_read_range` (`true` | `false`): indicates whether to generate checksums when executing GET(object, range), where `range` is offset and length (in bytes) to read;
	* `checksum.validate_obj_move` (`true` | `false`): indicates whether to perform checksum validation upon object migration.

9. object replication is always checksum-protected. If an object does not have a checksum (see #3 above), the latter gets computed on the fly and stored with the object, so that subsequent replications/migrations could reuse it.

10. finally, when two objects in the cluster have identical (bucket, object) names and identical checksums, they are considered to be full replicas of each other - the fact that allows optimizing PUT, replication, and object migration in a variety of use cases.
