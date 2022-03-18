---
layout: post
title: MSGPACK
permalink: /docs/msgp
redirect_from:
 - /msgp.md/
 - /docs/msgp.md/
---

## Table of Contents

- [Introduction](#introduction)
- [Code generation](#code-generation)

## Introduction

MsgPack is a binary exchange format that provides better performance and lower bandwidth usage comparing to JSON.
To make a struct MsgPack-compatible, add tags to the struct in the same way you add JSON tags. Example from `cmn/bucket_list.go`:

```go
type BucketEntry struct {
	Name      string `json:"name" msg:"n"`
	Size      int64  `json:"size,string,omitempty" msg:"s,omitempty"`
	// the rest struct fields
```

Note `msg:"n"` tag value - in the resulting binary data, the field `Name` is marked with `n` title.

## Code generation

After you change a struct that supports MsgPack, encoding and decoding functions are not updated automatically.
You have to run MsgPack generator for the changed file.

First, install the generator if you haven't done it yet:

```console
$ make msgp-update
```

Second, generate the encoding and decoding functions.
For better compatibility and to minimize the size of a diff patch, use the same flags as in example:

```console
$ msgp -file <PATH_TO_CHANGED_FILE> -tests=false -marshal=false
```

When the generation completes, `msgp` creates a file with the original name and `_gen` prefix.
Do not forget to prepend a common AIS header to the generated file and commit it to the repository.

Workflow example:

```console
$ ls cmn/bucket_list*
bucket_list.go
$ msgp -file cmn/bucket_list.go -tests=false -marshal=false
$ ls cmn/bucket_list*
bucket_list.go  bucket_list_gen.go
```
