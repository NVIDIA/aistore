---
layout: post
title: ARCHIVE
permalink: /docs/archive
redirect_from:
 - /archive.md/
 - /docs/archive.md/
---

Training neural networks on very large datasets is not easy (an understatement).

One of the many associated challenges is a so-called [small-file problem](https://scholar.google.com/scholar?hl=en&as_sdt=0%2C5&q=%22small+file+problem%22) - the problem that gets progressively worse given continuous random access to the entirety of an underlying dataset (that often also has a tendency to annually double in size).

One way to address the small-file problem involves providing some sort of *serialization* or *sharding* that allows to run **unmodified** clients and apps.

Sharding - is exactly the approach that we took in AIStore. Archiving or sharding, in the context, means utilizing TAR, for instance, to combine small files into .tar formatted shards.

> While I/O performance was always the primary motivation, the fact that a sharded dataset is, effectively, a backup of the original one must be considered an important added bonus.

Today AIS equally supports formats: TAR, TGZ (TAR.GZ), TAR.LZ4, ZIP, where:

* TAR is a well-known format first introduced in Unix V7 circa 1979 with specific formatting flavors including USTAR, PAX, and GNU TAR (all three are equally supported);
* TGZ (aka TAR.GZ) and TAR.LZ4 provide, respectively, gzip and lz4 compression to tar files (aka tarballs);
* and ZIP is [PKWARE ZIP](https://www.pkware.com/appnote) first introduced in 1989.

AIS can natively read, write, append(**), and list archives.

All sharding formats are equally supported across the entire set of AIS APIs. For instance, `list-objects` API supports "opening" objects formatted as one of the supported archival types and including contents of archived directories into generated result sets. Clients can run concurrent multi-object (source bucket => destination bucket) transactions to en masse generate new archives from [selected](/docs/batch.md) subsets of files, and more.

APPEND to existing archives is also provided but limited to [TAR only](https://aiatscale.org/blog/2021/08/10/tar-append).

> Maybe with exception of TAR, none of the listed sharding/archiving formats was ever designed to be append-able - that is, not if we are actually talking about *appending* and not some sort of extract-all-create-new type emulation (that will certainly break the performance in several well-documented ways).

See also:

* [CLI examples](/docs/cli/archive.md)
* [More CLI examples](/docs/cli/object.md)
* [API](/docs/http_api.md)
