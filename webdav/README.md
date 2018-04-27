DFC Webdav server
-----------------------------------------------------------------

## Overview

A webdav server fronting DFC allows DFC users to access objects stored in DFC using familiar file explorer like tools instead of S3 like HTTP APIs.

DFC webdav server is implemented based on golang.org’s existing webdav server, this allows fast development by reusing the subsystems already developed by Go. Built on top of the HTTP server, XML parser, locking, property management and common file system functionalities, DFC webdav implemented DFC specific file system interface and directory/file objects, integrated these two components with Go’s webdav server.

Since DFC is different than a normal file system, customizations and improvements are made to the original webdav servers. Majority of the changes are for performance reasons, for example, reduce round trips to DFC by reusing already fetched file stat information while doing list files. Doing parallel file system walk is a good example of an improvement that benefits both DFC and other users.

## Bucket and object to directory and file mapping

- Buckets are mapped as first level directory.
- Objects without “/” are mapped as files under a first level directory(bucket), for example, object “obj” under bucket “testbucket” is mapped as /testbucket/obj.
- Objects with “/” are mapped as files under directories following each segment of the path, for example, DFC object “loader/photos/obj” in bucket “testbucket” is mapped as /testbucket/loader/photos/obj.

## Getting Started

```
$ go build
$ go install
$ webdav
```

## Clients
1. Cadaver:
```
cadaver http://127.0.0.1:8079
```

2. GVFS
```
gvfs-mount dav://127.0.0.1:8079
```

3. File explorer
- On Linux: connect to dav://127.0.0.1:8079
- On Mac: connect to http://127.0.0.1:8079

## Litmus compliance test

Original webdav server doesn’t pass all the tests for different reasons: return error code mismatch, interpretation of the RFC, etc. DFC behaves the same way as the original server, it passes the basic, copymove and http group, but fails on others.

## Known limitations

- Currently only local buckets are supported. Supporting remote buckets are not hard to add.
- Empty directories are only supported in memory, they are not persisted to DFC. In order to support that, it requires extra metadata objects are added to DFC which in turn will cause other DFC APIs to be aware those metadata objects.
- Permissions, ACLs, O_Flags are not verified, they may work as expected.
- Directory access time and size are currently not supported.
- Have not tested with a Windows client
- Performance is still an issue. For example, when doing ‘ls’, some clients asks for file type, this requires reading the first 512 bytes of an object, plus the normal open and close pair, and may be a stat on top of all of that, this means 4 trips to DFC per object, which can be very slow when there are many objects in a directory.

