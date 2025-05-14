---
layout: post
title: ARCHIVE
permalink: /docs/cli/archive
redirect_from:
 - /cli/archive.md/
 - /docs/cli/archive.md/
---

# Working with Archives (Shards)

In AIStore, *archives* (also called *shards*) are special objects that contain multiple files packaged together in formats like TAR, TGZ, ZIP, or TAR.LZ4. Working with archives is essential for efficiently managing collections of related files and for operations like distributed sorting.

In this document:
* Commands to read, write, extract, and list *archives* - objects formatted as `TAR`, `TGZ` (or `TAR.GZ`) , `ZIP`, or `TAR.LZ4`.

> For the most recently updated list of supported archival formats, please refer to [this source](https://github.com/NVIDIA/aistore/blob/main/cmn/archive/mime.go).

The corresponding subset of CLI commands starts with `ais archive`, from where you can `<TAB-TAB>` to the actual (reading, writing, etc.) operation.

## Table of Contents
- [Subcommands](#archive-commands)
- [Archive Files and Directories (`ais archive put`)](#archive-files-and-directories-ais-archive-put)
- [Append Files to Existing Archives](#append-files-to-existing-archives)
- [Archive Multiple Objects (`ais archive bucket`)](#archive-multiple-objects-ais-archive-bucket)
- [List Archived Content (`ais archive ls`)](#list-archived-content)
- [Get Archived Content ('ais archive get`)](#get-archived-content-ais-archive-get)
- [Get Archived Content: Multiple-Selection](#get-archived-content-multiple-selection)
- [Generate Shards for Testing](#generate-shards)

## Subcommands

The corresponding subset of subcommands starts with `ais archive`, from where you can `<TAB-TAB>` to the actual operation:

```
ais archive ls		# List archived content
ais archive put		# Archive files from local filesystem
ais archive get		# Extract content from archives
ais archive bucket	# Archive objects from a bucket
ais archive gen-shards	# Generate test archives
```

For detailed help on any command, use the `--help` option:

## Archive files and directories (`ais archive put`)

Archive multiple files.

```console
$ ais archive put --help

NAME:
   ais archive put - Archive a file, a directory, or multiple files and/or directories as
     (.tar, .tgz or .tar.gz, .zip, .tar.lz4)-formatted object - aka "shard".
     Both APPEND (to an existing shard) and PUT (a new version of the shard) are supported.
     Examples:
     - 'local-file s3://q/shard-00123.tar.lz4 --append --archpath name-in-archive' - append file to a given shard,
        optionally, rename it (inside archive) as specified;
     - 'local-file s3://q/shard-00123.tar.lz4 --append-or-put --archpath name-in-archive' - append file to a given shard if exists,
        otherwise, create a new shard (and name it shard-00123.tar.lz4, as specified);
     - 'src-dir gs://w/shard-999.zip --append' - archive entire 'src-dir' directory; iff the destination .zip doesn't exist create a new one;
     - '"sys, docs" ais://dst/CCC.tar --dry-run -y -r --archpath ggg/' - dry-run to recursively archive two directories.
     Tips:
     - use '--dry-run' if in doubt;
     - to archive objects from a ais:// or remote bucket, run 'ais archive bucket' (see --help for details).

USAGE:
   ais archive put [-|FILE|DIRECTORY[/PATTERN]] BUCKET/SHARD_NAME [command options]

OPTIONS:
   --append             Add newly archived content to the destination object ("archive", "shard") that must exist
   --append-or-put      Append to an existing destination object ("archive", "shard") iff exists; otherwise PUT a new archive (shard);
                        note that PUT (with subsequent overwrite if the destination exists) is the default behavior when the flag is omitted
   --archpath value     Filename in an object ("shard") formatted as: .tar, .tgz or .tar.gz, .zip, .tar.lz4
   --cont-on-err        Keep running archiving xaction (job) in presence of errors in a any given multi-object transaction
   --dry-run            Preview the results without really running the action
   --include-src-dir    Prefix the names of archived files with the (root) source directory
   --list value         Comma-separated list of object or file names, e.g.:
                        --list 'o1,o2,o3'
                        --list "abc/1.tar, abc/1.cls, abc/1.jpeg"
                        or, when listing files and/or directories:
                        --list "/home/docs, /home/abc/1.tar, /home/abc/1.jpeg"
   --num-workers value  Number of concurrent client-side workers (to execute PUT or append requests);
                        use (-1) to indicate single-threaded serial execution (ie., no workers);
                        any positive value will be adjusted _not_ to exceed twice the number of client CPUs (default: 10)
   --progress           Show progress bar(s) and progress of execution in real time
   --recursive, -r      Recursive operation
   --refresh value      Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                        valid time units: ns, us (or µs), ms, s (default), m, h
   --skip-vc            Skip loading object metadata (and the associated checksum & version related processing)
   --template value     Template to match object or file names; may contain prefix (that could be empty) with zero or more ranges
                        (with optional steps and gaps), e.g.:
                        --template "" # (an empty or '*' template matches everything)
                        --template 'dir/subdir/'
                        --template 'shard-{1000..9999}.tar'
                        --template "prefix-{0010..0013..2}-gap-{1..2}-suffix"
                        and similarly, when specifying files and directories:
                        --template '/home/dir/subdir/'
                        --template "/abc/prefix-{0010..9999..2}-suffix"
   --timeout value      Maximum time to wait for a job to finish; if omitted: wait forever or until Ctrl-C;
                        valid time units: ns, us (or µs), ms, s (default), m, h
   --units value        Show statistics and/or parse command-line specified sizes using one of the following units of measurement:
                        iec - IEC format, e.g.: KiB, MiB, GiB (default)
                        si  - SI (metric) format, e.g.: KB, MB, GB
                        raw - do not convert to (or from) human-readable format
   --verbose, -v        Verbose output
   --wait               Wait for an asynchronous operation to finish (optionally, use '--timeout' to limit the waiting time)
   --yes, -y            Assume 'yes' to all questions
   --help, -h           Show help
```

The operation accepts either an explicitly defined *list* or template-defined *range* of file names (to archive).

**NOTE:**

* `ais archive put` works with locally accessible (source) files and shall _not_ be confused with `ais archive bucket` command (below).

Also, note that `ais put` command with its `--archpath` option provides an alternative way to archive multiple objects:

For the most recently updated list of supported archival formats, please see:

* [this source](https://github.com/NVIDIA/aistore/blob/main/cmn/archive/mime.go).

## Append files and directories to an existing archive

APPEND operation provides for appending files to existing archives (shards). As such, APPEND is a variation of PUT (above) with additional **two boolean flags**:

| Name | Description |
| --- | --- |
| `--append` | add newly archived content to the destination object (\"archive\", \"shard\") that **must** exist |
| `--append-or-put` | **if** destination object (\"archive\", \"shard\") exists append to it, otherwise archive a new one |

### Example 1: add file to archive

#### step 1. create archive (by archiving a given source dir)

```console
$ ais archive put sys ais://nnn/sys.tar.lz4
Warning: multi-file 'archive put' operation requires either '--append' or '--append-or-put' option
Proceed to execute 'archive put --append-or-put'? [Y/N]: y
Files to upload:
EXTENSION        COUNT   SIZE
.go              11      17.46KiB
TOTAL            11      17.46KiB
APPEND 11 files (one directory, non-recursive) => ais://nnn/sys.tar.lz4? [Y/N]: y
Done
```

#### step 2. add a single file to existing archive

```console
$ ais archive put README.md ais://nnn/sys.tar.lz4 --archpath=docs/README --append
APPEND README.md to ais://nnn/sys.tar.lz4 as "docs/README"
```

#### step 3. list entire bucket with an `--archive` option to show all archived entries

```console
$ ais ls ais://nnn --archive
NAME                             SIZE
sys.tar.lz4                      16.84KiB
    sys.tar.lz4/api_linux.go     1.07KiB
    sys.tar.lz4/cpu.go           1.07KiB
    sys.tar.lz4/cpu_darwin.go    802B
    sys.tar.lz4/cpu_linux.go     2.14KiB
    sys.tar.lz4/docs/README      13.85KiB
    sys.tar.lz4/mem.go           1.16KiB
    sys.tar.lz4/mem_darwin.go    2.04KiB
    sys.tar.lz4/mem_linux.go     2.81KiB
    sys.tar.lz4/proc.go          784B
    sys.tar.lz4/proc_darwin.go   369B
    sys.tar.lz4/proc_linux.go    1.40KiB
    sys.tar.lz4/sys_test.go      3.88KiB
Listed: 13 names
```

Alternatively, use regex to select:

```console
$ ais ls ais://nnn --archive --regex docs
NAME                             SIZE
    sys.tar.lz4/docs/README      13.85KiB
```

### Example 2: use `--template` flag to add source files

Generally, the `--template` option combines (an optional) prefix and/or one or more ranges (e.g., bash brace expansions).

In this case, the template we use is a simple prefix with no ranges.

```console
$ ls -l /tmp/w
total 32
-rw-r--r-- 1 root root 14180 Dec 11 18:18 111
-rw-r--r-- 1 root root 14180 Dec 11 18:18 222

$ ais archive put ais://nnn/shard-001.tar --template /tmp/w/ --append
Files to upload:
EXTENSION        COUNT   SIZE
                 2       27.70KiB
TOTAL            2       27.70KiB
APPEND 2 files (one directory, non-recursive) => ais://nnn/shard-001.tar? [Y/N]: y
Done
$ ais ls ais://nnn/shard-001.tar --archive
NAME                                             SIZE
shard-001.tar                                    37.50KiB
    shard-001.tar/111                            13.85KiB
    shard-001.tar/222                            13.85KiB
    shard-001.tar/23ed44d8bf3952a35484-1.test    1.00KiB
    shard-001.tar/452938788ebb87807043-4.test    1.00KiB
    shard-001.tar/7925bc9b5eb1daa12ed0-2.test    1.00KiB
    shard-001.tar/8264574b49bd188a4b27-0.test    1.00KiB
    shard-001.tar/f1f25e52c5edd768e0ec-3.test    1.00KiB
```

### Example 3: add file to archive

In this example, we assume that `arch.tar` already exists.

```console
# contents _before_:
$ ais archive ls ais://abc/arch.tar
NAME                SIZE
arch.tar            4.5KiB
    arch.tar/obj1   1.0KiB
    arch.tar/obj2   1.0KiB

# add file to existing archive:
$ ais archive put /tmp/obj1.bin ais://abc/arch.tar --archpath bin/obj1
APPEND "/tmp/obj1.bin" to object "ais://abc/arch.tar[/bin/obj1]"

# contents _after_:
$ ais archive ls ais://abc/arch.tar
NAME                    SIZE
arch.tar                6KiB
    arch.tar/bin/obj1   2.KiB
    arch.tar/obj1       1.0KiB
    arch.tar/obj2       1.0KiB
```

### Example 4: add file to archive

```console
# contents _before_:

$ ais archive ls ais://nnn/shard-2.tar
NAME                                             SIZE
shard-2.tar                                      5.50KiB
    shard-2.tar/0379f37cbb0415e7eaea-3.test      1.00KiB
    shard-2.tar/504c563d14852368575b-5.test      1.00KiB
    shard-2.tar/c7bcb7014568b5e7d13b-4.test      1.00KiB

# append and note that `--archpath` can specify a fully qualified destination name

$ ais archive put LICENSE ais://nnn/shard-2.tar --archpath shard-2.tar/license.test
APPEND "/go/src/github.com/NVIDIA/aistore/LICENSE" to "ais://nnn/shard-2.tar[/shard-2.tar/license.test]"

# contents _after_:
$ ais archive ls ais://nnn/shard-2.tar
NAME                                             SIZE
shard-2.tar                                      7.50KiB
    shard-2.tar/0379f37cbb0415e7eaea-3.test      1.00KiB
    shard-2.tar/504c563d14852368575b-5.test      1.00KiB
    shard-2.tar/c7bcb7014568b5e7d13b-4.test      1.00KiB
    shard-2.tar/license.test                     1.05KiB
```

## Archive multiple objects (`ais archive bucket`)

The `ais archive bucket` command creates archives (shards) from multiple objects stored in a bucket. This is a powerful operation that:

1. Takes objects from a specified **source bucket**
2. Archives them as a single shard in the specified **destination bucket**

### Features

- Source and destination buckets can be the same or different
- Supports multiple selection methods (lists, templates, prefixes)
- Supports all backend providers
- Supports various archival formats (.tar, .tar.gz/.tgz, .zip, .tar.lz4)
- Executes asynchronously and in parallel across all AIS nodes for maximum performance

### Usage

```console
$ ais archive bucket --help
NAME:
   ais archive bucket - Archive selected or matching objects from SRC_BUCKET[/OBJECT_NAME_or_TEMPLATE] as
   (.tar, .tgz or .tar.gz, .zip, .tar.lz4)-formatted object (a.k.a. "shard"):
     - 'ais archive bucket ais://src gs://dst/a.tar.lz4 --template "trunk-{001..997}"'       - archive (prefix+range) matching objects from ais://src;
     - 'ais archive bucket "ais://src/trunk-{001..997}" gs://dst/a.tar.lz4'                  - same as above (notice double quotes);
     - 'ais archive bucket "ais://src/trunk-{998..999}" gs://dst/a.tar.lz4 --append-or-put'  - add two more objects to an existing shard;
     - 'ais archive bucket s3://src/trunk-00 ais://dst/b.tar'                                - archive "trunk-00" prefixed objects from an s3 bucket as a given TAR destinati
on

USAGE:
   ais archive bucket SRC_BUCKET[/OBJECT_NAME_or_TEMPLATE] DST_BUCKET/SHARD_NAME [command options]

OPTIONS:
   append-or-put     Append to an existing destination object ("archive", "shard") iff exists; otherwise PUT a new archive (shard);
                     note that PUT (with subsequent overwrite if the destination exists) is the default behavior when the flag is omitted
   cont-on-err       Keep running archiving xaction (job) in presence of errors in a any given multi-object transaction
   dry-run           Preview the results without really running the action
   include-src-bck   Prefix the names of archived files with the source bucket name
   list              Comma-separated list of object or file names, e.g.:
                     --list 'o1,o2,o3'
                     --list "abc/1.tar, abc/1.cls, abc/1.jpeg"
                     or, when listing files and/or directories:
                     --list "/home/docs, /home/abc/1.tar, /home/abc/1.jpeg"
   non-recursive,nr  Non-recursive operation, e.g.:
                     - 'ais ls gs://bck/sub --nr'               - list objects and/or virtual subdirectories with names starting with the specified prefix;
                     - 'ais ls gs://bck/sub/ --nr'              - list only immediate contents of 'sub/' subdirectory (non-recursive);
                     - 'ais prefetch s3://bck/abcd --nr'        - prefetch a single named object;
                     - 'ais evict gs://bck/sub/ --nr'           - evict only immediate contents of 'sub/' subdirectory (non-recursive);
                     - 'ais evict gs://bck --prefix=sub/ --nr'  - same as above
   prefix            Select virtual directories or objects with names starting with the specified prefix, e.g.:
                     '--prefix a/b/c'   - matches names 'a/b/c/d', 'a/b/cdef', and similar;
                     '--prefix a/b/c/'  - only matches objects from the virtual directory a/b/c/
   skip-lookup       Skip checking source and destination buckets' existence (trading off extra lookup for performance)

   template   Template to match object or file names; may contain prefix (that could be empty) with zero or more ranges
              (with optional steps and gaps), e.g.:
              --template "" # (an empty or '*' template matches everything)
              --template 'dir/subdir/'
              --template 'shard-{1000..9999}.tar'
              --template "prefix-{0010..0013..2}-gap-{1..2}-suffix"
              and similarly, when specifying files and directories:
              --template '/home/dir/subdir/'
              --template "/abc/prefix-{0010..9999..2}-suffix"
   wait       Wait for an asynchronous operation to finish (optionally, use '--timeout' to limit the waiting time)
   help, h    Show help

```

## Selection Options

The command provides multiple ways to select objects for archiving:

1. **Template matching**: Use patterns with ranges to select objects
   ```
   ais archive bucket ais://src gs://dst/a.tar --template "trunk-{001..997}"
   ```

2. **List-based selection**: Specify a comma-separated list of objects
   ```
   ais archive bucket ais://bck/arch.tar --list obj1,obj2,obj3
   ```

3. **Prefix-based selection**: Select objects that share a common prefix
   ```
   ais archive bucket ais://src ais://dst/archive.tar --prefix data/logs/
   ```

### Non-Recursive Option (--nr)

The `--nr` (or `--non-recursive`) flag limits the scope of the archiving operation to only include objects at the specified directory level, without descending into subdirectories.

### Examples with Non-Recursive Flag

1. Archive only the files directly in a directory (not its subdirectories):
   ```
   ais archive bucket ais://nnn/aaa/ ais://dst/archive.tar --nr
   ```
   This will only archive objects directly in the `aaa/` directory, skipping any objects in subdirectories like `aaa/bbb/`.

2. Compare with recursive archiving (default behavior):
   ```
   ais archive bucket ais://nnn/aaa/ ais://dst/archive.tar
   ```
   This will archive all objects under the `aaa/` prefix, including those in subdirectories like `aaa/bbb/`.

### Visual Example

For a bucket with this structure:
```
ais://nnn
├── aaa/
│   ├── 777
│   ├── 888
│   ├── 999
│   └── bbb/
│       ├── 111
│       ├── 222
│       └── 333
```

With `--nr` flag:
```
ais archive bucket ais://nnn/aaa/ ais://dst/f.tar --nr
```
Result:
```
f.tar
├── aaa/777
├── aaa/888
└── aaa/999
```

Without `--nr` flag:
```
ais archive bucket ais://nnn/aaa/ ais://dst/g.tar
```
Result:
```
g.tar
├── aaa/777
├── aaa/888
├── aaa/999
├── aaa/bbb/111
├── aaa/bbb/222
└── aaa/bbb/333
```

### Additional Options

- `--append-or-put`: Append to an existing archive if it exists; otherwise create new
- `--cont-on-err`: Continue archiving despite errors in multi-object transactions
- `--dry-run`: Preview the results without executing
- `--include-src-bck`: Prefix archived file names with the source bucket name
- `--skip-lookup`: Skip checking bucket existence for better performance
- `--wait`: Wait for the asynchronous operation to complete

### Complete Examples

**1**. Archive objects with a specific prefix, non-recursively:

```console
$ ais archive bucket s3://src-bck/aaa/ ais://dst/example.tar --nr
Archived s3://src-bck/aaa/ => ais://dst/example.tar

$ ais ls ais://dst/example.tar --archive
NAME				  SIZE
example.tar			  106.00KiB
    example.tar/aaa/777	          16.84KiB
    example.tar/aaa/888	          16.84KiB
    example.tar/aaa/999	          16.84KiB
    example.tar/aaa/trunk-777     16.84KiB
    example.tar/aaa/trunk-888     16.84KiB
    example.tar/aaa/trunk-999     16.84KiB
```

**2**. Archive objects using a template range:

```console
$ ais archive bucket ais://src ais://dst/range.tar --template "obj-{0..9}"
Archiving "ais://dst/range.tar" ...

$ ais archive ls ais://dst/range.tar

NAME                     SIZE
range.tar                92.60KiB
    range.tar/obj-0      9.26KiB
    range.tar/obj-1      9.26KiB
    ...
    range.tar/obj-9      9.26KiB
```

**3**. Incrementally append to an existing archive:

```console
$ ais archive bucket ais://bck/incremental.tar --template "obj{1..3}"
Archived "ais://bck/incremental.tar" ...

$ ais archive bucket ais://bck/incremental.tar --template "obj{4..5}" --append
Archived "ais://bck/incremental.tar"
```

**4**. Archive a list of objects from a given bucket:

```console
$ ais archive bucket ais://bck/arch.tar --list obj1,obj2
Archiving "ais://bck/arch.tar" ...
```

Resulting `ais://bck/arch.tar` contains objects `ais://bck/obj1` and `ais://bck/obj2`.

**5**. Archive objects from a different bucket, use template (range):

```console
$ ais archive bucket ais://src ais://dst/arch.tar --template "obj-{0..9}"

Archiving "ais://dst/arch.tar" ...
```

`ais://dst/arch.tar` now contains 10 objects from bucket `ais://src`: `ais://src/obj-0`, `ais://src/obj-1` ... `ais://src/obj-9`.

**6**. Archive 3 objects and then append 2 more:

```console
$ ais archive bucket ais://bck/arch1.tar --template "obj{1..3}"
Archived "ais://bck/arch1.tar" ...
$ ais archive ls ais://bck/arch1.tar
NAME                     SIZE
arch1.tar                31.00KiB
    arch1.tar/obj1       9.26KiB
    arch1.tar/obj2       9.26KiB
    arch1.tar/obj3       9.26KiB

$ ais archive bucket ais://bck/arch1.tar --template "obj{4..5}" --append
Archived "ais://bck/arch1.tar"

$ ais archive ls ais://bck/arch1.tar
NAME                     SIZE
arch1.tar                51.00KiB
    arch1.tar/obj1       9.26KiB
    arch1.tar/obj2       9.26KiB
    arch1.tar/obj3       9.26KiB
    arch1.tar/obj4       9.26KiB
    arch1.tar/obj5       9.26KiB
```

### Notes

- `ais archive bucket` must not be confused with `ais archive put`
    - `archive bucket` archives objects in the cluster
       - more precisely, objects accessible by the cluster
    - `archive put` archives files from your local or locally accessible (NFS, SMB) directories
- The operation runs asynchronously
    - use `--wait` to wait for completion; see `--help` for details
- When using the `--nr` (non-recursive) flag, only the immediate contents of the specified virtual directory is archived
- For more information on multi-object operations, please see:
    - [operations on lists and ranges documentation](/docs/cli/object.md#operations-on-lists-and-ranges-and-entire-buckets).

## List archived content

```console
NAME:
   ais archive ls - list archived content (supported formats: .tar, .tgz or .tar.gz, .zip, .tar.lz4)

USAGE:
   ais archive ls BUCKET[/SHARD_NAME] [command options]
```

List archived content as a tree with archive ("shard") name as a root and archived files as leaves.
Filenames are always sorted alphabetically.

### Options

```console
$ ais archive ls --help

NAME:
   ais archive ls - List archived content (supported formats: .tar, .tgz or .tar.gz, .zip, .tar.lz4)

USAGE:
   ais archive ls BUCKET[/SHARD_NAME] [command options]

OPTIONS:
   --all                  Depending on the context, list:
                          - all buckets, including accessible (visible) remote buckets that are not in-cluster
                          - all objects in a given accessible (visible) bucket, including remote objects and misplaced copies
   --cached               Only list in-cluster objects, i.e., objects from the respective remote bucket that are present ("cached") in the cluster
   --count-only           Print only the resulting number of listed objects and elapsed time
   --diff                 Perform a bidirectional diff between in-cluster and remote content, which further entails:
                          - detecting remote version changes (a.k.a. out-of-band updates), and
                          - remotely deleted objects (out-of-band deletions (*));
                            the option requires remote backends supporting some form of versioning (e.g., object version, checksum, and/or ETag);
                          see related:
                               (*) options: --cached; --latest
                               commands:    'ais get --latest'; 'ais cp --sync'; 'ais prefetch --latest'
   --dont-add             List remote bucket without adding it to cluster's metadata - e.g.:
                            - let's say, s3://abc is accessible but not present in the cluster (e.g., 'ais ls' returns error);
                            - then, if we ask aistore to list remote buckets: `ais ls s3://abc --all'
                              the bucket will be added (in effect, it'll be created);
                            - to prevent this from happening, either use this '--dont-add' flag or run 'ais evict' command later
   --dont-wait            When _summarizing_ buckets do not wait for the respective job to finish -
                          use the job's UUID to query the results interactively
   --inv-id value         Bucket inventory ID (optional; by default, we use bucket name as the bucket's inventory ID)
   --inv-name value       Bucket inventory name (optional; system default name is '.inventory')
   --inventory            List objects using _bucket inventory_ (docs/s3compat.md); requires s3:// backend; will provide significant performance
                          boost when used with very large s3 buckets; e.g. usage:
                            1) 'ais ls s3://abc --inventory'
                            2) 'ais ls s3://abc --inventory --paged --prefix=subdir/'
                          (see also: docs/s3compat.md)
   --limit value          The maximum number of objects to list, get, or otherwise handle (0 - unlimited; see also '--max-pages'),
                          e.g.:
                          - 'ais ls gs://abc/dir --limit 1234 --cached --props size,custom,atime'  - list no more than 1234 objects
                          - 'ais get gs://abc /dev/null --prefix dir --limit 1234'                 - get --/--
                          - 'ais scrub gs://abc/dir --limit 1234'                                  - scrub --/-- (default: 0)
   --max-pages value      Maximum number of pages to display (see also '--page-size' and '--limit')
                          e.g.: 'ais ls az://abc --paged --page-size 123 --max-pages 7 (default: 0)
   --name-only            Faster request to retrieve only the names of objects (if defined, '--props' flag will be ignored)
   --no-dirs              Do not return virtual subdirectories (applies to remote buckets only)
   --no-footers, -F       Display tables without footers
   --no-headers, -H       Display tables without headers
   --non-recursive, --nr  Non-recursive operation, e.g.:
                          - 'ais ls gs://bucket/prefix --nr'   - list objects and/or virtual subdirectories with names starting with the specified prefix;
                          - 'ais ls gs://bucket/prefix/ --nr'  - list contained objects and/or immediately nested virtual subdirectories _without_ recursing into the latter;
                          - 'ais prefetch s3://bck/abcd --nr'  - prefetch a single named object (see 'ais prefetch --help' for details);
                          - 'ais rmo gs://bucket/prefix --nr'  - remove a single object with the specified name (see 'ais rmo --help' for details)
   --page-size value      Maximum number of object names per page; when the flag is omitted or 0
                          the maximum is defined by the corresponding backend; see also '--max-pages' and '--paged' (default: 0)
   --paged                List objects page by page - one page at a time (see also '--page-size' and '--limit')
                          note: recommended for use with very large buckets
   --prefix value         List objects with names starting with the specified prefix, e.g.:
                          '--prefix a/b/c' - list virtual directory a/b/c and/or objects from the virtual directory
                          a/b that have their names (relative to this directory) starting with the letter 'c'
   --props value          Comma-separated list of object properties including name, size, version, copies, and more; e.g.:
                          --props all
                          --props name,size,cached
                          --props "ec, copies, custom, location"
   --refresh value        Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                          valid time units: ns, us (or µs), ms, s (default), m, h
   --regex value          Regular expression; use it to match either bucket names or objects in a given bucket, e.g.:
                          ais ls --regex "(m|n)"         - match buckets such as ais://nnn, s3://mmm, etc.;
                          ais ls ais://nnn --regex "^A"  - match object names starting with letter A
   --show-unmatched       List also objects that were not matched by regex and/or template (range)
   --silent               Server-side flag, an indication for aistore _not_ to log assorted errors (e.g., HEAD(object) failures)
   --skip-lookup          Do not execute HEAD(bucket) request to lookup remote bucket and its properties; possible usage scenarios include:
                           1) adding remote bucket to aistore without first checking the bucket's accessibility
                              (e.g., to configure the bucket's aistore properties with alternative security profile and/or endpoint)
                           2) listing public-access Cloud buckets where certain operations (e.g., 'HEAD(bucket)') may be disallowed
   --start-after value    List bucket's content alphabetically starting with the first name _after_ the specified
   --summary              Show object numbers, bucket sizes, and used capacity;
                          note: applies only to buckets and objects that are _present_ in the cluster
   --template value       Template to match object or file names; may contain prefix (that could be empty) with zero or more ranges
                          (with optional steps and gaps), e.g.:
                          --template "" # (an empty or '*' template matches everything)
                          --template 'dir/subdir/'
                          --template 'shard-{1000..9999}.tar'
                          --template "prefix-{0010..0013..2}-gap-{1..2}-suffix"
                          and similarly, when specifying files and directories:
                          --template '/home/dir/subdir/'
                          --template "/abc/prefix-{0010..9999..2}-suffix"
   --units value          Show statistics and/or parse command-line specified sizes using one of the following units of measurement:
                          iec - IEC format, e.g.: KiB, MiB, GiB (default)
                          si  - SI (metric) format, e.g.: KB, MB, GB
                          raw - do not convert to (or from) human-readable format
   --help, -h             Show help
```

### Examples

```console
$ ais archive ls ais://bck/arch.tar
NAME                SIZE
arch.tar            4.5KiB
    arch.tar/obj1   1.0KiB
    arch.tar/obj2   1.0KiB
```

### Example: use '--prefix' that crosses shard boundary

For starters, we recursively archive all aistore docs:

```console
$ ais put docs ais://A.tar --archive -r
```

To list a virtual subdirectory _inside_ this newly created shard (e.g.):

```console
$ ais archive ls ais://nnn --prefix "A.tar/tutorials"
NAME                                             SIZE
    A.tar/tutorials/README.md                    561B
    A.tar/tutorials/etl/compute_md5.md           8.28KiB
    A.tar/tutorials/etl/etl_imagenet_pytorch.md  4.16KiB
    A.tar/tutorials/etl/etl_webdataset.md        3.97KiB
Listed: 4 names
````

or, same:

```console
$ ais ls ais://nnn --prefix "A.tar/tutorials" --archive
NAME                                             SIZE
    A.tar/tutorials/README.md                    561B
    A.tar/tutorials/etl/compute_md5.md           8.28KiB
    A.tar/tutorials/etl/etl_imagenet_pytorch.md  4.16KiB
    A.tar/tutorials/etl/etl_webdataset.md        3.97KiB
Listed: 4 names
```

## Get archived content ('ais archive get`)

```console
$ ais archive get --help
NAME:
   ais archive get - Get a shard and extract its content; get an archived file;
              write the content locally with destination options including: filename, directory, STDOUT ('-'), or '/dev/null' (discard);
              assorted options further include:
              - '--prefix' to get multiple shards in one shot (empty prefix for the entire bucket);
              - '--progress' and '--refresh' to watch progress bar;
              - '-v' to produce verbose output when getting multiple objects.
   'ais archive get' examples:
              - ais://abc/trunk-0123.tar.lz4 /tmp/out - get and extract entire shard to /tmp/out/trunk/*
              - ais://abc/trunk-0123.tar.lz4 --archpath file45.jpeg /tmp/out - extract one named file
              - ais://abc/trunk-0123.tar.lz4/file45.jpeg /tmp/out - same as above (and note that '--archpath' is implied)
              - ais://abc/trunk-0123.tar.lz4/file45 /tmp/out/file456.new - same as above, with destination explicitly (re)named
   'ais archive get' multi-selection examples:
              - ais://abc/trunk-0123.tar 111.tar --archregx=jpeg --archmode=suffix - return 111.tar with all *.jpeg files from a given shard
              - ais://abc/trunk-0123.tar 222.tar --archregx=file45 --archmode=wdskey - return 222.tar with all file45.* files --/--
              - ais://abc/trunk-0123.tar 333.tar --archregx=subdir/ --archmode=prefix - 333.tar with all subdir/* files --/--

USAGE:
   ais archive get BUCKET[/SHARD_NAME] [OUT_FILE|OUT_DIR|-] [command options]

OPTIONS:
   archive         List archived content (see docs/archive.md for details)
   archmime        Expected format (mime type) of an object ("shard") formatted as .tar, .tgz or .tar.gz, .zip, .tar.lz4;
                   especially usable for shards with non-standard extensions
   archmode        Enumerated "matching mode" that tells aistore how to handle '--archregx', one of:
                     * regexp - general purpose regular expression;
                     * prefix - matching filename starts with;
                     * suffix - matching filename ends with;
                     * substr - matching filename contains;
                     * wdskey - WebDataset key
                   example:
                     given a shard containing (subdir/aaa.jpg, subdir/aaa.json, subdir/bbb.jpg, subdir/bbb.json, ...)
                     and wdskey=subdir/aaa, aistore will match and return (subdir/aaa.jpg, subdir/aaa.json)
   archpath        Extract the specified file from an object ("shard") formatted as: .tar, .tgz or .tar.gz, .zip, .tar.lz4;
                   see also: '--archregx'
   archregx        Specifies prefix, suffix, substring, WebDataset key, _or_ a general-purpose regular expression
                   to select possibly multiple matching archived files from a given shard;
                   is used in combination with '--archmode' ("matching mode") option
   blob-download   Utilize built-in blob-downloader (and the corresponding alternative datapath) to read very large remote objects
   cached          Only get in-cluster objects, i.e., objects from the respective remote bucket that are present ("cached") in the cluster
   checksum        Validate checksum
   chunk-size      Chunk size in IEC or SI units, or "raw" bytes (e.g.: 4mb, 1MiB, 1048576, 128k; see '--units')
   encode-objname  Encode object names that contain special symbols (; : ' " < > / \ | ? #) that may otherwise break shell parsing or URL interpretation
   extract,x       Extract all files from archive(s)
   inv-id          Bucket inventory ID (optional; by default, we use bucket name as the bucket's inventory ID)
   inv-name        Bucket inventory name (optional; system default name is '.inventory')
   inventory       List objects using _bucket inventory_ (docs/s3compat.md); requires s3:// backend; will provide significant performance
                   boost when used with very large s3 buckets; e.g. usage:
                     1) 'ais ls s3://abc --inventory'
                     2) 'ais ls s3://abc --inventory --paged --prefix=subdir/'
                   (see also: docs/s3compat.md)
   latest          Check in-cluster metadata and, possibly, GET, download, prefetch, or otherwise copy the latest object version
                   from the associated remote bucket;
                   the option provides operation-level control over object versioning (and version synchronization)
                   without the need to change the corresponding bucket configuration: 'versioning.validate_warm_get';
                   see also:
                     - 'ais show bucket BUCKET versioning'
                     - 'ais bucket props set BUCKET versioning'
                     - 'ais ls --check-versions'
                   supported commands include:
                     - 'ais cp', 'ais prefetch', 'ais get'
   limit           The maximum number of objects to list, get, or otherwise handle (0 - unlimited; see also '--max-pages'),
                   e.g.:
                   - 'ais ls gs://abc/dir --limit 1234 --cached --props size,custom,atime'  - list no more than 1234 objects
                   - 'ais get gs://abc /dev/null --prefix dir --limit 1234'                 - get --/--
                   - 'ais scrub gs://abc/dir --limit 1234'                                  - scrub --/--
   num-workers     Number of concurrent blob-downloading workers (readers); system default when omitted or zero
   prefix          Get objects with names starting with the specified prefix, e.g.:
                   '--prefix a/b/c' - get objects from the virtual directory a/b/c and objects from the virtual directory
                   a/b that have their names (relative to this directory) starting with 'c';
                   '--prefix ""' - get entire bucket (all objects)
   progress        Show progress bar(s) and progress of execution in real time
   refresh         Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                   valid time units: ns, us (or µs), ms, s (default), m, h
   silent          Server-side flag, an indication for aistore _not_ to log assorted errors (e.g., HEAD(object) failures)
   skip-lookup     Do not execute HEAD(bucket) request to lookup remote bucket and its properties; possible usage scenarios include:
                    1) adding remote bucket to aistore without first checking the bucket's accessibility
                       (e.g., to configure the bucket's aistore properties with alternative security profile and/or endpoint)
                    2) listing public-access Cloud buckets where certain operations (e.g., 'HEAD(bucket)') may be disallowed
   units           Show statistics and/or parse command-line specified sizes using one of the following units of measurement:
                   iec - IEC format, e.g.: KiB, MiB, GiB (default)
                   si  - SI (metric) format, e.g.: KB, MB, GB
                   raw - do not convert to (or from) human-readable format
   verbose,v       Verbose output
   yes,y           Assume 'yes' to all questions
   help, h         Show help
```

### Example: extract one file

```console
$ ais archive get ais://dst/A.tar.gz /tmp/w --archpath 111.ext1
GET 111.ext1 from ais://dst/A.tar.gz as "/tmp/w/111.ext1" (12.56KiB)

$ ls /tmp/w
111.ext1
```

Alternatively, use fully qualified name:

```console
$ ais archive get ais://dst/A.tar.gz/111.ext1 /tmp/w
```

### Example: extract one file using its fully-qualified name::

```console
$ ais archive get ais://nnn/A.tar/tutorials/README.md /tmp/out
```

### Example: extract all files from a single shard

Let's say, we have a certain shard in a certain bucket:

```console
$ ais ls ais://dst --archive
NAME                     SIZE
A.tar.gz                 5.18KiB
    A.tar.gz/111.ext1    12.56KiB
    A.tar.gz/222.ext1    12.56KiB
    A.tar.gz/333.ext2    12.56KiB
```

We can then go ahead to GET and extract it to local directory, e.g.:

```console
$ ais archive get ais://dst/A.tar.gz /tmp/www --extract
GET A.tar.gz from ais://dst as "/tmp/www/A.tar.gz" (5.18KiB) and extract to /tmp/www/A/

$ ls /tmp/www/A
111.ext1  222.ext1  333.ext2
```

But here's an alternative syntax to achieve the same:

```console
$ ais get ais://dst --archive --prefix A.tar.gz /tmp/www
```

or even:

```console
$ ais get ais://dst --archive --prefix A.tar.gz /tmp/www --progress --refresh 1 -y

GET 51 objects from ais://dst/tmp/ggg (total size 1.08MiB)
Objects:                   51/51 [==============================================================] 100 %
Total size:  1.08 MiB / 1.08 MiB [==============================================================] 100 %
```

The difference is that:

* in the first case we ask for a specific shard,
* while in the second (and third) we filter bucket's content using a certain prefix
* and the fact (the convention) that archived filenames are prefixed with their parent (shard) name.

### Example: extract all files from all shards (with a given prefix)

Let's say, there's a bucket `ais://dst` with a virtual directory `abc/` that in turn contains:

```console
$ ais ls ais://dst
NAME             SIZE
A.tar.gz         5.18KiB
B.tar.lz4        247.88KiB
C.tar.zip        4.15KiB
D.tar            2.00KiB
```

Next, we GET and extract them all in the respective sub-directories (note `--verbose` option):

```console
$ ais archive get ais://dst /tmp/w --prefix "" --extract -v

GET 4 objects from ais://dst to /tmp/w (total size 259.21KiB) [Y/N]: y
GET D.tar from ais://dst as "/tmp/w/D.tar" (2.00KiB) and extract as /tmp/w/D
GET A.tar.gz from ais://dst as "/tmp/w/A.tar.gz" (5.18KiB) and extract as /tmp/w/A
GET C.tar.zip from ais://dst as "/tmp/w/C.tar.zip" (4.15KiB) and extract as /tmp/w/C
GET B.tar.lz4 from ais://dst as "/tmp/w/B.tar.lz4" (247.88KiB) and extract as /tmp/w/B
```

### Example: use '--prefix' that crosses shard boundary

For starters, we recursively archive all aistore docs:

```console
$ ais put docs ais://A.tar --archive -r
```

To list a virtual subdirectory _inside_ this newly created shard (e.g.):

```console
$ ais archive ls ais://nnn --prefix A.tar/tutorials
NAME                                             SIZE
    A.tar/tutorials/README.md                    561B
    A.tar/tutorials/etl/compute_md5.md           8.28KiB
    A.tar/tutorials/etl/etl_imagenet_pytorch.md  4.16KiB
    A.tar/tutorials/etl/etl_webdataset.md        3.97KiB
Listed: 4 names
```

Now, extract matching files _from_ the bucket to /tmp/out:

```console
$ ais archive get ais://nnn --prefix A.tar/tutorials /tmp/out
GET 6 objects from ais://nnn/tmp/out (total size 17.81MiB) [Y/N]: y

$ ls -al /tmp/out/tutorials/
total 20
drwxr-x--- 4 root root 4096 May 13 20:05 ./
drwxr-xr-x 3 root root 4096 May 13 20:05 ../
drwxr-x--- 2 root root 4096 May 13 20:05 etl/
-rw-r--r-- 1 root root  561 May 13 20:05 README.md
drwxr-x--- 2 root root 4096 May 13 20:05 various/
```

## Get archived content: multiple selection

Generally, both single and multi-selection from a given source shard is realized using one of the following 4 (four) options:

```console
   --archpath value     extract the specified file from an object ("shard") formatted as: .tar, .tgz or .tar.gz, .zip, .tar.lz4;
                        see also: '--archregx'
   --archmime value     expected format (mime type) of an object ("shard") formatted as: .tar, .tgz or .tar.gz, .zip, .tar.lz4;
                        especially usable for shards with non-standard extensions
   --archregx value     string that specifies prefix, suffix, substring, WebDataset key, _or_ a general-purpose regular expression
                        to select possibly multiple matching archived files from a given shard;
                        is used in combination with '--archmode' ("matching mode") option
   --archmode value     enumerated "matching mode" that tells aistore how to handle '--archregx', one of:
                          * regexp - general purpose regular expression;
                          * prefix - matching filename starts with;
                          * suffix - matching filename ends with;
                          * substr - matching filename contains;
                          * wdskey - WebDataset key
                        example:
                          given a shard containing (subdir/aaa.jpg, subdir/aaa.json, subdir/bbb.jpg, subdir/bbb.json, ...)
                          and wdskey=subdir/aaa, aistore will match and return (subdir/aaa.jpg, subdir/aaa.json)
```

In particular, '--archregx' and '--archmode' pair defines multiple selection that can be further demonstrated on the following examples.

> But first, note that in all multi-selection cases, the result is (currently) invariably formatted as .TAR (that contains the aforementioned selection).

### Example: suffix match

Select all `*.jpeg` files from a given shard and return them all as 111.tar:

```console
$ ais archive get ais://abc/trunk-0123.tar 111.tar --archregx=jpeg --archmode=suffix
```

### Example: [WebDataset](https://github.com/webdataset/webdataset) key

Select all files that have a given [WebDataset](https://github.com/webdataset/webdataset) key; return the result as 222.tar:

```console
$ ais archive get ais://abc/trunk-0123.tar 222.tar --archregx=file45 --archmode=wdskey
```

### Example: prefix match

Similar to the above except that in this case '--archregx' value specifies virtual subdirectory inside a given named shard:

```console
$ ais archive get ais://abc/trunk-0123.tar 333.tar --archregx=subdir/ --archmode=prefix
```

## Generate shards

`ais archive gen-shards "BUCKET/TEMPLATE.EXT"`

Put randomly generated shards that can be used for dSort testing.
The `TEMPLATE` must be bash-like brace expansion (see examples) and `.EXT` must be one of: `.tar`, `.tar.gz`.

**Warning**: Remember to always quote the argument (`"..."`) otherwise the brace expansion will happen in terminal.

### Options

```console
$ ais archive gen-shards --help

NAME:
   ais archive gen-shards - Generate random (.tar, .tgz or .tar.gz, .zip, .tar.lz4)-formatted objects ("shards"), e.g.:
              - gen-shards 'ais://bucket1/shard-{001..999}.tar' - write 999 random shards (default sizes) to ais://bucket1
              - gen-shards "gs://bucket2/shard-{01..20..2}.tgz" - 10 random gzipped tarfiles to Cloud bucket
              (notice quotation marks in both cases)

USAGE:
   ais archive gen-shards "BUCKET/TEMPLATE.EXT" [command options]

OPTIONS:
   --cleanup            Remove old bucket and create it again (warning: removes the entire content of the old bucket)
   --fcount value       Number of files in a shard (default: 5)
   --fext value         Comma-separated list of file extensions (default ".test"), e.g.:
                        --fext .mp3
                        --fext '.mp3,.json,.cls' (or, same: ".mp3,  .json,  .cls")
   --fsize value        Size of the files in a shard (default: "1024")
   --num-workers value  Limits the number of shards created concurrently (default: 10)
   --tform value        TAR file format selection (one of "Unknown", "USTAR", "PAX", or "GNU")
   --help, -h           Show help
```

### Examples

#### Generate shards with varying numbers of files and file sizes

Generate 10 shards each containing 100 files of size 256KB and put them inside `ais://dsort-testing` bucket (creates it if it does not exist).
Shards will be named: `shard-0.tar`, `shard-1.tar`, ..., `shard-9.tar`.

```console
$ ais archive gen-shards "ais://dsort-testing/shard-{0..9}.tar" --fsize 262144 --fcount 100
Shards created: 10/10 [==============================================================] 100 %
$ ais ls ais://dsort-testing
NAME		SIZE		VERSION
shard-0.tar	25.05MiB	1
shard-1.tar	25.05MiB	1
shard-2.tar	25.05MiB	1
shard-3.tar	25.05MiB	1
shard-4.tar	25.05MiB	1
shard-5.tar	25.05MiB	1
shard-6.tar	25.05MiB	1
shard-7.tar	25.05MiB	1
shard-8.tar	25.05MiB	1
shard-9.tar	25.05MiB	1
```

#### Generate shards using custom naming template

Generates 100 shards each containing 5 files of size 256KB and put them inside `dsort-testing` bucket.
Shards will be compressed and named: `super_shard_000_last.tgz`, `super_shard_001_last.tgz`, ..., `super_shard_099_last.tgz`

```console
$ ais archive gen-shards "ais://dsort-testing/super_shard_{000..099}_last.tar" --fsize 262144 --cleanup
Shards created: 100/100 [==============================================================] 100 %
$ ais ls ais://dsort-testing
NAME				SIZE	VERSION
super_shard_000_last.tgz	1.25MiB	1
super_shard_001_last.tgz	1.25MiB	1
super_shard_002_last.tgz	1.25MiB	1
super_shard_003_last.tgz	1.25MiB	1
super_shard_004_last.tgz	1.25MiB	1
super_shard_005_last.tgz	1.25MiB	1
super_shard_006_last.tgz	1.25MiB	1
super_shard_007_last.tgz	1.25MiB	1
...
```

#### Multi-extension example


```console
$ ais archive gen-shards 'ais://nnn/shard-{01..99}.tar' -fext ".mp3,  .json,  .cls"

$ ais archive ls ais://nnn | head -n 20
NAME                                             SIZE
shard-01.tar                                     23.50KiB
    shard-01.tar/541701ae863f76d0f7e0-0.cls      1.00KiB
    shard-01.tar/541701ae863f76d0f7e0-0.json     1.00KiB
    shard-01.tar/541701ae863f76d0f7e0-0.mp3      1.00KiB
    shard-01.tar/8f8c5fa2934c90138833-1.cls      1.00KiB
    shard-01.tar/8f8c5fa2934c90138833-1.json     1.00KiB
    shard-01.tar/8f8c5fa2934c90138833-1.mp3      1.00KiB
    shard-01.tar/9a42bd12d810d890ea86-3.cls      1.00KiB
    shard-01.tar/9a42bd12d810d890ea86-3.json     1.00KiB
    shard-01.tar/9a42bd12d810d890ea86-3.mp3      1.00KiB
    shard-01.tar/c5bd7c7a34e12ebf3ad3-2.cls      1.00KiB
    shard-01.tar/c5bd7c7a34e12ebf3ad3-2.json     1.00KiB
    shard-01.tar/c5bd7c7a34e12ebf3ad3-2.mp3      1.00KiB
    shard-01.tar/f13522533ecafbad4fe5-4.cls      1.00KiB
    shard-01.tar/f13522533ecafbad4fe5-4.json     1.00KiB
    shard-01.tar/f13522533ecafbad4fe5-4.mp3      1.00KiB
shard-02.tar                                     23.50KiB
    shard-02.tar/095e6ae644ff4fd1778b-7.cls      1.00KiB
    shard-02.tar/095e6ae644ff4fd1778b-7.json     1.00KiB
...
```
