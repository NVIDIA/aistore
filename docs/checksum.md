## Table of Contents
- [Object checksums: brief theory of operations](#object-checksums-brief-theory-of-operations)

## Object checksums: brief theory of operations

1. objects are stored in the cluster with their content checksums and in accordance with their bucket configurations.

2. xxhash is the system-default checksum.

3. user can override the system default on a bucket level, by setting checksum=none.

4. bucket (re)configuration can be done at any time. Bucket's checksumming option can be changed from xxhash to none and back, potentially multiple times and with no limitations.

5. an object with a bad checksum cannot be retrieved (via GET) and cannot be replicated or migrated. Corrupted objects get eventually removed from the system.

6. GET and PUT operations support an option to validate checksums. The validation is done against a checksum stored with an object (GET), or a checksum provided by a user (PUT).

7. object replications and migrations are always checksum-protected. If an object does not have checksum (see #3 above), the latter gets computed on the fly and stored with the object, so that subsequent replications/migrations could reuse it.

8. when two objects in the cluster have identical (bucket, object) names and checksums, they are considered to be full replicas of each other - the fact that allows optimizing PUT,replication, and object migration in a variety of use cases.

