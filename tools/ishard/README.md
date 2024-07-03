# Initial Sharder

This package provides a utility for archiving objects from a bucket into shard files. It ensures that objects from the same directory are not split across different output shards.

## Assumptions

**Do Not Split A Record Across Directories:** The utility assumes that objects within the same record should NOT be split across multiple subdirectories. This ensures that members of the same record are archived together. Users will be able to customize the **record key** and the **extensions** associated with the record through configuration (feature to be implemented).

> This limitation will become a configurable user selection at a later point, with capabilities to customize record keys, etc.

## Parameters

- `-shard_size`: The desired size of each output shard in bytes. Default is `1024000`.
- `-src_bck`: The source bucket name or URI. If empty, a bucket with a random name will be created.
- `-dst_bck`: The destination bucket name or URI. If empty, a bucket with a random name will be created.
- `-shard_template`: the template used for generating output shards. Accepts Bash, Fmt, or At formats.
- `-ext`: the extension used for generating output shards.
- `-collapse`: If true, files in a subdirectory will be flattened and merged into its parent directory if their overall size doesn't reach the desired shard size.

## Initial Setup

**Build the Package:**

```sh
$ cd cmd/ishard
$ go build -o ishard .
```

## Sample Usage

Correct Usage

```sh
$ ./ishard -max_shard_size=1024000 -src_bck=source_bucket -dst_bck=destination_bucket -collapse -shard_template="prefix-{0000..1023..8}-suffix"
$ ais archive ls ais://destination_bucket

NAME                     SIZE            
prefix-0000-suffix.tar   1.01MiB         
prefix-0008-suffix.tar   1.41MiB         
prefix-0016-suffix.tar   1.00MiB         
prefix-0024-suffix.tar   1.00MiB         
prefix-0032-suffix.tar   1.05MiB         
prefix-0040-suffix.tar   1.09MiB         
prefix-0048-suffix.tar   1.04MiB         
prefix-0056-suffix.tar   1.02MiB         
prefix-0064-suffix.tar   1.02MiB         
prefix-0072-suffix.tar   1.04MiB         
prefix-0080-suffix.tar   1.03MiB         
prefix-0088-suffix.tar   1.01MiB         
prefix-0096-suffix.tar   1.02MiB         
prefix-0104-suffix.tar   1.06MiB         
...
```

Incorrect Usage
```
$ ./ishard -max_shard_size=1024000 -src_bck=source_bucket -dst_bck=destination_bucket -collapse -shard_template="prefix-{0000..0050..8}-suffix"
Error: number of shards to be created exceeds expected number of shards (7)


```

## Running the Tests

```sh
go test -v
```

## TODO List

### MUST HAVE/DESIRABLE
- [X] Shard name patterns
   - [X] Utilize existing name template tools
- [ ] goroutine
- [ ] configurable record key, extensions
- [ ] logging (timestamp, nlog)
- [ ] reports missing member in a record
- [ ] allow user to specify target directories to include/exclude
- [ ] E2E testing from CLI

### GOOD TO HAVE
- [ ] progress bar (later)
- [ ] integration into aistore (later)
