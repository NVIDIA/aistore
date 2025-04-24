# Understanding Virtual Directories in AIStore: The `--nr` and `--no-dirs` Flags

## Table of Contents
- [What are Virtual Directories?](#what-are-virtual-directories)
- [Flag Definitions](#flag-definitions)
- [How These Flags Interact](#how-these-flags-interact)
- [Flag Summary Table](#flag-summary-table)
- [Examples](#examples)
  - [Default Recursive Listing](#default-recursive-listing)
  - [Non-Recursive Listing](#non-recursive-listing)
  - [Filtering Directories](#filtering-directories)
  - [Combined Flags](#combined-flags)
  - [Exploring Complex Directory Structures](#exploring-complex-directory-structures)
- [Common Use Cases](#common-use-cases)
- [Recap](#recap)

AIStore supports the concept of virtual directories, allowing for hierarchical organization of objects within buckets.

When listing objects via supported APIs (or CLI `ais ls`), two important flags control how these virtual directories are handled: `--nr` (non-recursive) and `--no-dirs`. Understanding the interaction between these flags is essential.

## What are Virtual Directories?

In AIStore, virtual directories are indicated implicitly by using the slash (`/`) character in object names.

For example, an object named `data/logs/file1.log` implies virtual directories `data/` and `data/logs/`.

Unlike traditional POSIX filesystems, the virtual directories are derived from the object names and provide a hierarchical view of your stored objects.

## Flag Definitions

### `--nr` (Non-Recursive)
- **Purpose**: Limits listing to only the immediate objects under the specified prefix
- **Behavior**: Prevents traversal into subdirectories but includes virtual directories
- **Default**: Off (recursive listing is the default behavior)

### `--no-dirs`
- **Purpose**: Excludes virtual directories from the listing results
- **Behavior**: Filters out directory entries (object prefixes ending with `/`)
- **Default**: On (directories are hidden by default), unless `--nr` is specified

## How These Flags Interact

The interaction between these flags can be confusing but follows a logical pattern:

1. **Default Behavior (no flags)**: Lists all objects recursively but hides virtual directories
2. **With `--nr`**: Shows only top-level entries (both objects and directories) without recursing
3. **With `--no-dirs`**: Lists all objects recursively but hides virtual directory entries
4. **With both `--nr --no-dirs`**: Shows only immediate objects (no directories, no recursion)

## Flag Summary Table

| Flags Used        | API Constants                        | Behavior                                           | Shows Directories | Shows Objects in Subdirectories |
|-------------------|------------------------------------|----------------------------------------------------|-------------------|--------------------------------|
| (none)            | -                                  | Lists all objects recursively                      | No                | Yes                            |
| `--nr`            | `apc.LsNoRecursion`                | Lists immediate entries only                       | Yes               | No                             |
| `--no-dirs`       | `apc.LsNoDirs`                     | Lists all objects without directories              | No                | Yes                            |
| `--nr --no-dirs`  | `apc.LsNoRecursion | apc.LsNoDirs` | Lists immediate objects only, excluding directories| No                | No                             |

## Examples

### Default Recursive Listing

The default behavior shows all objects recursively and hides virtual directories:

```console
$ ais ls s3://bucket --prefix data/
NAME                 SIZE            CACHED
data/logs/file1      16.84KiB        yes
data/logs/file2      22.53KiB        yes
data/config/settings 8.12KiB         yes
```

Using Unix-style path notation produces the same result:

```console
$ ais ls s3://bucket/data/
NAME                 SIZE            CACHED
data/logs/file1      16.84KiB        yes
data/logs/file2      22.53KiB        yes
data/config/settings 8.12KiB         yes
```

A more complex example showing recursive listing with a specific prefix:

```console
$ ais ls s3://speech --prefix .inventory
NAME                                                                  SIZE       CACHED
.inventory/speech/data/
.inventory/speech/2024-05-31T01-00Z/manifest.checksum                 33B        no
.inventory/speech/2024-05-31T01-00Z/manifest.json                     406B       no
.inventory/speech/data/985fc9cb-5957-4fc8-b26d-092685a747e8.csv.gz    54.14MiB   no
.inventory/speech/data/9dac8de5-cff9-432c-9663-b054ae5ce357.csv.gz    54.14MiB   no
.inventory/speech/hive/dt=2024-05-30-01-00/symlink.txt                85B        no
.inventory/speech/hive/dt=2024-05-31-01-00/symlink.txt                85B        no
```

You can also use Unix-style directory notation, which is equivalent to the previous command:

```console
$ ais ls s3://speech/.inventory
```

### Non-Recursive Listing

The `--nr` flag shows only top-level entries without recursing into subdirectories:

```console
$ ais ls s3://bucket --prefix data/ --nr
NAME             SIZE    CACHED
data/logs/
data/config/
```

This is especially useful for exploring directory structure at a specific depth:

```console
$ ais ls s3://speech --prefix .inventory/speech/ --nr
NAME                                       SIZE    CACHED
.inventory/speech/2024-05-31T01-00Z/
.inventory/speech/data/
.inventory/speech/hive/
```

### Filtering Directories

The `--no-dirs` flag is implied by default and can be explicitly specified to hide directory entries:

```console
$ ais ls s3://bucket --prefix data/ --no-dirs
NAME                SIZE            CACHED
data/logs/file1     16.84KiB        no
data/logs/file2     22.53KiB        no
data/config/settings 8.12KiB        no
```

Similarly, with a more complex structure:

```console
$ ais ls s3://speech --prefix .inventory --no-dirs
NAME                                                                 SIZE       CACHED
.inventory/speech/2024-05-31T01-00Z/manifest.checksum                33B        no
.inventory/speech/2024-05-31T01-00Z/manifest.json                    406B       no
.inventory/speech/data/985fc9cb-5957-4fc8-b26d-092685a747e8.csv.gz   54.14MiB   no
.inventory/speech/data/9dac8de5-cff9-432c-9663-b054ae5ce357.csv.gz   54.14MiB   no
.inventory/speech/hive/dt=2024-05-30-01-00/symlink.txt               85B        no
.inventory/speech/hive/dt=2024-05-31-01-00/symlink.txt               85B        no
```

### Combined Flags

When both `--nr` and `--no-dirs` flags are used together, only immediate objects are shown (no directories and no recursion):

```console
$ ais ls s3://bucket --prefix data/ --nr --no-dirs
NAME     SIZE    CACHED

```

In this case, no results appear because there are only directories at the top level. The `--nr` flag restricts listing to immediate objects, while `--no-dirs` excludes the virtual directories.

### Exploring Complex Directory Structures

You can view both directories and objects at a specific level using `--nr`:

```console
$ ais ls s3://speech --prefix .inventory/speech/data/ --nr
NAME                                                                  SIZE        CACHED
.inventory/speech/data/
.inventory/speech/data/985fc9cb-5957-4fc8-b26d-092685a747e8.csv.gz    54.14MiB    no
.inventory/speech/data/9dac8de5-cff9-432c-9663-b054ae5ce357.csv.gz    54.14MiB    no
```

This shows both the current directory and immediate objects without recursing further.

## Common Use Cases

1. **List top-level directories only:**
```sh
$ ais ls s3://bucket --prefix data/ --nr
```

2. **List all objects without directory entries:**
```sh
$ ais ls s3://bucket --prefix data/
```
*Note: `--no-dirs` is implied by default*

3. **Show all objects with full paths:**
```sh
$ ais ls s3://bucket --prefix data/ --all
```
*The `--all` flag shows both objects and directories at all levels*

## Recap

- The `--nr` flag is particularly useful when exploring a bucket's structure level by level
- When using prefixes with `--nr`, only the immediate level below the prefix is shown
- If a prefix points directly to an object rather than a directory, using `--nr` won't affect the output
- When both flags are used together, you might see an empty result if there are no immediate objects at that level
- For large buckets with complex directory structures, using these flags wisely can significantly improve listing performance and readability
