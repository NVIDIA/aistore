# `xmeta`: AIS Metadata Tool

`xmeta` is a low-level utility for inspecting, extracting, formatting, and (in limited cases)
editing internal AIS (AIStore) metadata structures.

It is intended for **advanced troubleshooting, recovery, and diagnostics**.
In normal operation, AIS metadata should be managed via AIS CLI and APIs.

Supported metadata types include:

| Name | Comment |
|---|---|
| **Smap** | cluster map |
| **BMD** | bucket metadata |
| **RMD** | rebalance metadata |
| **Config** | cluster and node configuration |
| **VMD** | volume metadata |
| **EC** | erasure-coding metadata |
| **LOM** | object metadata |
| **ETL** | extract-transform-load metadata |

> **Note on paths in examples:**
> Paths like `~/.ais0/.ais.smap` are from local development deployments.
> In production:
> - Cluster-wide metadata (Smap, BMD, RMD, Config) is stored in the node’s config directory (commonly under `/etc/ais/`)
> - VMD and BMD are stored at the root of each mountpath (for example, `/ais/nvme0n1/.ais.vmd`)

**Table of Contents**

- [Important Notes](#important-notes)
  - [Backup First](#backup-first)
- [Usage](#usage)
- [Build](#build)
  - [Using the main makefile (recommended)](#using-the-main-makefile-recommended)
  - [Using go install](#using-go-install)
  - [Notes](#notes)
- [Examples](#examples)
  - [General Help](#general-help)
  - [Metadata Extraction & Formatting](#metadata-extraction--formatting)
    - [Smap](#smap)
    - [BMD](#bmd)
    - [RMD](#rmd)
    - [Cluster Config](#cluster-config)
    - [VMD (Volume Metadata)](#vmd-volume-metadata)
  - [VMD In-Place Edit (Recovery / Troubleshooting)](#vmd-in-place-edit-recovery--troubleshooting)
    - [Typical Recovery Workflow](#typical-recovery-workflow)
    - [Notes on VMD Edit Mode](#notes-on-vmd-edit-mode)
  - [EC Metadata (mt)](#ec-metadata-mt)
  - [LOM (Object Metadata)](#lom-object-metadata)
  - [ETL Metadata (EMD)](#etl-metadata-emd)
- [Summary](#summary)
- [References](#references)

---

## Important Notes

- `xmeta` operates **directly on on-disk metadata files**.
- Incorrect use can render a node or cluster unusable.
- **Back up metadata before making any changes.** (See below.)
- For replicated metadata (e.g., VMD), **all copies should be kept consistent**.

This tool is primarily intended for **offline use** (AIS target stopped),
unless explicitly stated otherwise.

### Backup First

Before inspecting or modifying **any** on-disk AIS metadata:

- Archive metadata from the node's configuration directory _and_ (for storage targets) - **all mountpaths**; keep the archive off-node.
- AIS keeps multiple copies of critical metadata, but redundancy is not a substitute for backups.
- When in doubt: **copy first, edit later**.

---

## Usage

```console
Usage of xmeta:
  -x
        Mode switch:
          true  - extract AIS-formatted metadata into plain text
          false - pack plain text into AIS-formatted metadata

  -in string
        Fully-qualified input filename

  -out string
        Output filename (optional for extraction)

  -f string
        Override automatic format detection.
        Accepted values: smap, bmd, rmd, conf, vmd, mt, lom, emd

  -enable string
        VMD only: enable mountpath (in-place edit, requires -x)

  -disable string
        VMD only: disable mountpath (in-place edit, requires -x)

  -q
        Quiet mode (VMD edit): suppress reminder banner

  -h
        Show help and exit
````

---

## Build

### Using the main [Makefile](https://github.com/NVIDIA/aistore/blob/main/Makefile) (recommended)

```console
cd <aistore-repo>
make xmeta
````

This builds the `xmeta` binary and places it under:

```text
$GOPATH/bin/xmeta
```

The Makefile build may apply size-reduction flags (for example, stripping debug
symbols), resulting in a smaller binary.

---

### Using `go install`

You can also build and install `xmeta` directly with Go tooling:

```console
# from the repository root
go install ./cmd/xmeta

# or, from inside cmd/xmeta
go install .
```

This installs `xmeta` into:

```text
$GOPATH/bin/xmeta
```

By default, `go install` preserves full debug and symbol information, which may
result in a noticeably larger binary compared to the Makefile build.

To produce a smaller binary comparable to `make xmeta`, you can strip symbols:

```console
go install -ldflags="-s -w" ./cmd/xmeta
```

### Notes

* Avoid `go install xmeta.go`: this builds `xmeta` in single-file (standalone)
  mode and can significantly increase binary size.
* Both build methods produce functionally identical binaries; the size
  difference is due to build flags and symbol retention.

---

## Examples

### General Help

```console
xmeta -h
```

---

## Metadata Extraction & Formatting

### Smap

```console
xmeta -x -in=~/.ais0/.ais.smap                      # Extract to STDOUT
xmeta -x -in=~/.ais0/.ais.smap -out=/tmp/smap.txt   # Extract to file
xmeta -in=/tmp/smap.txt -out=/tmp/.ais.smap         # Format plain text to AIS format
```

---

### BMD

```console
xmeta -x -in=~/.ais0/.ais.bmd
xmeta -x -in=~/.ais0/.ais.bmd -out=/tmp/bmd.txt
xmeta -in=/tmp/bmd.txt -out=/tmp/.ais.bmd
```

---

### RMD

```console
xmeta -x -in=~/.ais0/.ais.rmd
xmeta -x -in=~/.ais0/.ais.rmd -out=/tmp/rmd.txt
xmeta -in=/tmp/rmd.txt -out=/tmp/.ais.rmd
```

---

### Cluster Config

```console
xmeta -x -in=~/.ais0/.ais.conf
xmeta -x -in=~/.ais0/.ais.conf -out=/tmp/conf.txt
xmeta -in=/tmp/conf.txt -out=/tmp/.ais.conf
```

---

### VMD (Volume Metadata)

```console
xmeta -x -in=/ais/nvme0n1/.ais.vmd
xmeta -x -in=/ais/nvme0n1/.ais.vmd -out=/tmp/vmd.txt
xmeta -in=/tmp/vmd.txt -out=/ais/nvme7n1/.ais.vmd
```

#### CLI Example: mountpaths in production may show up as follows

```console
$ ais storage mountpath t[bhyjkodotf]

        Used: min=65%, avg=67%, max=68%
                                        /ais/nvme0n1  /dev/nvme0n1(xfs)
                                        /ais/nvme10n1 /dev/nvme10n1(xfs)
                                        /ais/nvme11n1 /dev/nvme11n1(xfs)
                                        /ais/nvme1n1  /dev/nvme1n1(xfs)
                                        /ais/nvme2n1  /dev/nvme2n1(xfs)
                                        /ais/nvme3n1  /dev/nvme3n1(xfs)
                                        /ais/nvme4n1  /dev/nvme4n1(xfs)
                                        /ais/nvme5n1  /dev/nvme5n1(xfs)
                                        /ais/nvme6n1  /dev/nvme6n1(xfs)
                                        /ais/nvme7n1  /dev/nvme7n1(xfs)
                                        /ais/nvme8n1  /dev/nvme8n1(xfs)
                                        /ais/nvme9n1  /dev/nvme9n1(xfs)
```

Note the two columns corresponding to distinct block devices (e.g., `/dev/nvme7n1`) and respective filesystems (typically `xfs`).

---

## VMD In-Place Edit (Recovery / Troubleshooting)

`xmeta` supports **in-place editing of VMD** to enable or disable mountpaths.
This is intended for **offline recovery scenarios**, such as:

* disk failure
* mountpath pointing to the wrong filesystem
* target refusing to start due to VMD consistency checks

### Typical Recovery Workflow

1. **Stop the AIS target**
2. Inspect existing mountpaths:

   ```console
   ais storage mountpath show  ## NOTE: all storage targets (ie., num-targets times num-per-target-mountpaths)
   ```

or choose specific (existing) target, e.g.:

   ```console
   ais storage mountpath show t[bhyjkodotf]
   ```


3. Disable a failed mountpath in VMD:

   ```console
   xmeta -x -in=/ais/nvme0n1/.ais.vmd -disable /ais/nvme7n1
   ```
4. Copy the updated VMD to all remaining mountpaths:

   ```console
   cp /ais/nvme0n1/.ais.vmd /ais/nvme1n1/.ais.vmd
   cp /ais/nvme0n1/.ais.vmd /ais/nvme2n1/.ais.vmd
   ```
5. Restart the target and verify:

   ```console
   ais storage mountpath show
   ```

### Notes on VMD Edit Mode

* Requires `-x` (AIS-formatted input).
* `-out` is **not allowed** (edit is always in-place).
* VMD version is automatically incremented.
* Mountpaths are normalized before lookup.
* `-q` suppresses the reminder banner.

> **All VMD copies for a target should be updated consistently.**

AIS will always select the **highest** VMD version during startup.

However, keeping all copies in sync is strongly recommended to ensure a clean,
predictable replicated state and to avoid unnecessary warnings or extra I/O
during initialization.

---

## EC Metadata (`mt`)

```console
xmeta -x -in=/data/@ais/abc/%mt/readme     # Auto-detect format
xmeta -x -in=./readme -f mt                # Explicit format override
```

---

## LOM (Object Metadata)

LOM extraction is **read-only** and does not support auto-detection.

```console
xmeta -x -in=/data/@ais/abc/%ob/img001.tar -f lom
xmeta -x -in=/data/@ais/abc/%ob/img001.tar -out=/tmp/lom.txt -f lom
```

---

## ETL Metadata (EMD)

```console
xmeta -x -in=~/.ais0/.ais.emd
xmeta -x -in=~/.ais0/.ais.emd -out=/tmp/emd.txt
```

---

## Summary

`xmeta` is a **tool**

* that can be very useful for debugging and recovery - offline in particular,
* dangerous if misused,
* and finally, is intended to be minimal and explicit.

## References

* [AIStore: Terminology](https://github.com/NVIDIA/aistore/blob/main/docs/overview.md#terminology)
* [AIStore: Troubleshooting](https://github.com/NVIDIA/aistore/blob/main/docs/troubleshooting.md)
* [On-disk layout](https://github.com/NVIDIA/aistore/blob/main/docs/on_disk_layout.md)
* [AIS buckets: on-disk layout](/docs/bucket.md#appendix-a-on-disk-layout)
