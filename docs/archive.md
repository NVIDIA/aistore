Training on very large datasets is not easy. One of the many associated challenges is a so-called [small-file problem](https://scholar.google.com/scholar?hl=en&as_sdt=0%2C5&q=%22small+file+problem%22) - the problem that gets progressively worse given continuous random access to the entirety of an underlying dataset.

Addressing the problem often means providing some sort of serialization (formatting, logic) that, ideally, also hides the fact and allows to run unmodified clients and apps. AIS approach to this and closely related problems (choices, tradeoffs) can be summarized in one word: TAR. As in: TAR archive.

More precisely, AIS equally supports several archival mime types, including TAR, TGZ (TAR.GZ), and ZIP.

The support itself started way back when we introduced [distributed shuffle](/docs/dsort.md) (extension) that works with all the 3 listed formats and performs massively-parallel custom sorting of any-size datasets. Version 3.7 adds an API-level native capability to read, write and list archives.

In particular, `list-objects` API supports "opening" objects formatted as one of the supported archival types and including contents of archived directories into generated result sets.

APPEND to existing archives is also supported, although at the time of this writing is limited to TAR (format).

In addition, clients can run concurrent multi-object (source bucket to destination bucket) transactions to generate new archives, and more.

See also:

* [CLI examples](/docs/cli/archive.md)
* [More CLI examples](/docs/cli/object.md)
* [API](/docs/http_api.md)
