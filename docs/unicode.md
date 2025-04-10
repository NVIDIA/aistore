# Unicode and Special Symbols in Object Names

AIStore provides seamless support for object names containing **Unicode** characters (like Japanese, Chinese, or Korean text), emojis, and **special symbols**.

This README demonstrates handling these names properly across both native (`ais://`) buckets and Cloud storage.

## Table of Contents

- [Client vs. Server Responsibilities](#client-vs-server-responsibilities)
- [Working with Unicode Object Names](#working-with-unicode-object-names)
- [Cross-Backend Compatibility (S3)](#cross-backend-compatibility-s3)
- [Terminal and Environment Considerations](#terminal-and-environment-considerations)
- [Curl](#curl)
- [Special Symbols in Object Names](#special-symbols-in-object-names)
- [Encoding Helper (Python)](#encoding-helper-python)

## Client vs. Server Responsibilities

In accordance with standard HTTP behavior, clients are responsible for percent-encoding object names into valid URL paths. This is especially important when object names contain characters like spaces, slashes, or symbols such as `?`, `#`, or `"`.

The AIS server — like all HTTP servers — automatically decodes (unescapes) the request path before processing it.

For example:

- If a client sends a request to read or write `bucket/my%20file`, the server will unescape it and handle it as `bucket/my file`.

- In terms of RESTful API, when a client sends a request with URL path `/v1/objects/bucket/my%20file`, AIS will handle it as `/v1/objects/bucket/my file`.

The bottom line is, AIStore always uses the original UTF-8 string decoded from the HTTP path. The path that you may (or may not) encode on the client side.

For convenience, AIS CLI provides the `--encode-objname` flag to do this automatically, so you can use natural-looking object names even if they contain special characters.

But if you use tools like `curl`, you must encode URLs manually.

Below are examples that demonstrate this behavior.

## Working with Unicode Object Names

```console
#
# Set a Unicode object name (Japanese "Hello World")
#
$ export helloworld="こんにちは世界"

#
# Put an object with Unicode name into an AIS bucket
#
$ ais put LICENSE ais://onebucket/$helloworld
PUT "LICENSE" => ais://onebucket/こんにちは世界

#
# Content is preserved correctly
#
$ ais object cat ais://onebucket/$helloworld
MIT License
Copyright (c) 2017 NVIDIA Corporation
Permission is hereby granted, free of charge, to any person obtaining a copy
...

#
# List the object details - Unicode name displays properly
#
$ ais ls ais://onebucket/$helloworld
PROPERTY         VALUE
atime            09 Apr 25 16:49 EDT
checksum         xxhash2[ed5b3e74f9f3516a]
name             ais://onebucket/こんにちは世界
size             1.05KiB
```

## Cross-Backend Compatibility (S3)

```console
#
# Put the same object into an S3 bucket
#
$ ais put LICENSE s3://twobucket/$helloworld
PUT "LICENSE" => s3://twobucket/こんにちは世界

#
# Verify with native S3 tools - Unicode is preserved
#
$ s3cmd ls s3://twobucket/$helloworld
2025-04-09 20:50         1075  s3://twobucket/こんにちは世界

#
# Retrieve with native S3 tools - content is preserved
#
$ s3cmd get s3://twobucket/$helloworld - | cat
download: 's3://twobucket/こんにちは世界' -> '-'  [1 of 1]
download: 's3://twobucket/こんにちは世界' -> '-'  [1 of 1]
 1075 of 1075   100% in    0s   897.27 KB/s
MIT License
Copyright (c) 2017 NVIDIA Corporation
Permission is hereby granted, free of charge, to any person obtaining a copy
...
 1075 of 1075   100% in    0s   868.32 KB/s  done
```

## Terminal and Environment Considerations

For proper display of Unicode characters in your terminal:

1. Ensure your terminal supports UTF-8 (most modern terminals do)
2. Set your locale to UTF-8: `export LANG=en_US.UTF-8`
3. If using VIM to edit configuration files with Unicode:
   ```bash
   # Add to your .vimrc
   set encoding=utf-8
   set fileencoding=utf-8
   set termencoding=utf-8
   ```

## Curl

For programmatic access to objects with Unicode names, remember that the URL must be properly encoded:

```console
$ export helloworld="こんにちは世界"
$ curl -L -X GET "http://ais-endpoint/v1/objects/onebucket/$helloworld"

MIT License
Copyright (c) 2017 NVIDIA Corporation
Permission is hereby granted, free of charge, to any person obtaining a copy
...
```


## Special Symbols in Object Names

Special symbols such as `;`, `:`, `'`, `"`, `<`, `>`, `/`, `\`, `|`, `?`, `#`, `%`, `+`, and `&` may require encoding depending on your shell or toolchain.

When using the CLI, specify `--encode-objname` flag with GET and PUT commands:

```console
$ ais put LICENSE "ais://threebucket/aaa bbb ccc" --encode-objname
PUT "LICENSE" => ais://threebucket/aaa bbb ccc

$ ais ls ais://threebucket
NAME             SIZE
aaa bbb ccc      1.05KiB

$ ais object cat "ais://threebucket/aaa bbb ccc" --encode-objname

MIT License
Copyright (c) 2017 NVIDIA Corporation
Permission is hereby granted, free of charge, to any person obtaining a copy
...
```

> Note: object names are always displayed in their original, human-readable form.

---

## Encoding Helper (Python)

For programmatic clients, here's how to encode object names using Python:

```python
import urllib.parse

name = 'my weird/obj?name#with!symbols'
encoded = urllib.parse.quote(name, safe='')  # fully encode all special chars
print(encoded)
# Output: my%20weird%2Fobj%3Fname%23with%21symbols
```

This encoded string can be used in direct HTTP requests (e.g., with `curl`).
Similar encoding functions exist in other programming languages - refer to your language's URL encoding documentation.

---

If you're building your own clients in Go or another language, make sure to encode the object name into a valid `URL.Path` — just as you would for any other HTTP API.

For more information, see the full AIStore documentation at [https://github.com/NVIDIA/aistore](https://github.com/NVIDIA/aistore)
