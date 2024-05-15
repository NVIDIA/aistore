This document is complementary to [Getting Started](/docs/getting_started.md) showing how to configure Go and ensure it is on your path.

Intended audience includes developers and first-time users, including those who'd never used Go before.

## `$GOPATH` and `$PATH`

There's of course a ton of [tutorials](https://tip.golang.org/doc/tutorial/getting-started) and markdowns on the web, but the gist of it boils down to a single word: `GOPATH`. Namely, setting it up and exporting, e.g.:

```console
$ mkdir /tmp/go/src /tmp/go/pkg /tmp/go/bin
$ export GOPATH=/tmp/go
```

Secondly, `$GOPATH/bin` must be in your `$PATH`. Something like:

```console
$ export PATH=$GOPATH/bin:$PATH
```

The intuition behind that is very simple: `$GOPATH` defines location for:

* Go sources
* Go packages
* Go binaries

Yes, all of the above, and respectively: `$GOPATH/src`, `$GOPATH/pkg`, and `$GOPATH/bin`.

And so, since you'd likely want to run binaries produced out of Go sources, you need to add the path, etc.

