# Initial Sharder

This package provides a utility for archiving objects from a bucket into shard files. It ensures that objects from the same directory are not split across different output shards.

## Assumptions

**Do Not Split A Record Across Directories:** The utility assumes that objects within the same record should NOT be split across multiple subdirectories. This ensures that members of the same record are archived together. Users will be able to customize the **record key** and the **extensions** associated with the record through configuration (feature to be implemented).

> This limitation will become a configurable user selection at a later point, with capabilities to customize record keys, etc.

## Parameters

- `-shard_size`: The desired size of each output shard in bytes. Default is `1024000`.
- `-src_bck`: The source bucket name or URI. If empty, a bucket with a random name will be created.
- `-dst_bck`: The destination bucket name or URI. If empty, a bucket with a random name will be created.
- `-src_bck_provider`: The provider for the source bucket (`ais`, `aws`, `azure`, `gcp`). Default is `ais`.
- `-dst_bck_provider`: The provider for the destination bucket (`ais`, `aws`, `azure`, `gcp`). Default is `ais`.
- `-collapse`: If true, files in a subdirectory will be flattened and merged into its parent directory if their overall size doesn't reach the desired shard size.

## Initial Setup

**Build the Package:**

```sh
go build -o ishard .
```

## Sample Usage

```sh
./ishard -shard_size=10240 -src_bck=source_bucket -dst_bck=destination_bucket -src_bck_provider=ais -dst_bck_provider=ais
```

## Running the Tests

```sh
go test -v
```

## TODO List

### MUST HAVE/DESIRABLE
- [ ] Shard name patterns
   - [ ] Utilize existing name template tools
- [ ] goroutine
- [ ] configurable record key, extensions
- [ ] logging (timestamp, nlog)
- [ ] reports missing member in a record
- [ ] allow user to specify target directories to include/exclude
- [ ] E2E testing from CLI

### GOOD TO HAVE
- [ ] progress bar (later)
- [ ] integration into aistore (later)
