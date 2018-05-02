DFC WebDAV Server
-----------------------------------------------------------------

## Overview

WebDAV aka "Web Distributed Authoring and Versioning" is the [IETF standard](https://tools.ietf.org/html/rfc4918) that defines HTTP extension for collaborative **file** management and editing. WebDAV extends the set of standard HTTP verbs (GET, PUT, etc.) with the new verbs that include COPY - to copy a resource from one URI to another, MOVE - to move a resource from one URI to another, and more.

WebDAV implementation in the DFC is, essentially, a reverse proxy with interoperable WebDAV on the front and DFC's RESTful interface on the back. The included server can be used with any of the popular [WebDAV-compliant clients](https://en.wikipedia.org/wiki/Comparison_of_WebDAV_software).

The implementation is based on the [golang.org/x/net WebDAV package](https://godoc.org/golang.org/x/net/webdav). Built on top of HTTP server, XML parser, and the golang's basic WebDAV functionality, the DFC WebDAV provides locking, property management, and the common file system semantics to manipulate files and directories.

## Mappings

On the DFC's backend, WebDAV provided file and directory names are translated as follows:

- Top level directories are mapped as buckets (containing DFC objects)
- File names that don't contain file name separator (“/” for Unix) are mapped as objects under the corresponding buckets. For example, a DFC object named “testbucket/obj” becomes /testbucket/obj on the WebDAV client side, and vise versa.
- Filenames that do contain "/" separators are mapped as expected.

## Getting Started

To run DFC WebDAV, execute the usual three steps:

```
$ go build
$ go install
$ webdav
```

The last step assumes that the PATH environment variable is set [as documented](https://golang.org/doc/code.html#GOPATH).

By default, the server listens on port 8079.

## Clients

DFC WebDAV is being tested with a growing number of clients, including:


1. Cadaver

[Cadaver](https://wiki.archlinux.org/index.php/WebDAV#Cadaver) is a lightweight command-line FTP-like client. Connect it using:

```
cadaver http://127.0.0.1:8079
```

2. GVFS
```
gvfs-mount dav://127.0.0.1:8079
```

3. File Explorer

In addition to command-line clients, DFC WebDAV can be used with the native Linux and Mac desktop file explorers.

- On Linux: connect to dav://127.0.0.1:8079
- On Mac: connect to http://127.0.0.1:8079

## Litmus compliance test

The well-known [Litmus compliance test](http://sabre.io/dav/litmus/) can be run from a local build or using a docker image provided by Litmus. The source code is available at https://github.com/tolsen/litmus

DFC WebDAV passes basic, copymove, and http groups of the Litmus compliance tests.

## Known limitations

- Only local buckets are supported (remote buckets are not difficult to add).
- Empty directories are not persistent and remain in memory.
- Permissions, ACLs, O_Flags are not verified.
- Directory access time and directory size are currently not supported.
- Not tested with Windows clients.
- Even though customizations and improvements were made, performance of the DFC WebDAV may still be an issue. For example, when listing directories some WebDAV clients also ask for a file type (to be included in the resulting list) - thus requiring reading the first 512 bytes of every listed object, etc. For a bucket containing a million objects the implications of this is easy to understand.

