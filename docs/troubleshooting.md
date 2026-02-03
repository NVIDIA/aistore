# Troubleshooting AIStore

This document describes common AIStore (AIS) failure modes and **actionable recovery steps**.
It is intended for operators and developers troubleshooting clusters that fail to start,
fail to join, or exhibit integrity errors.

In most cases, the AIS CLI is the first and best tool to use.

**Table of Contents**

- [First Checks: Cluster State via CLI](#first-checks-cluster-state-via-cli)
- [Error Categories](#error-categories)
- [Cluster Integrity Errors (CIE)](#cluster-integrity-errors-cie)
  - [Common Causes](#common-causes)
  - [CIE Error Reference](#cie-error-reference)
  - [Recovery (CIE)](#recovery-cie)
- [Storage Integrity Errors (SIE)](#storage-integrity-errors-sie)
  - [Key Concepts](#key-concepts)
- [SIE Error Reference](#sie-error-reference)
- [Target Fails to Start: Lost or Mismatched Mountpath (SIE#50)](#target-fails-to-start-lost-or-mismatched-mountpath-sie50)
  - [Symptoms](#symptoms)
- [Recovery: Offline VMD Edit (Recommended)](#recovery-offline-vmd-edit-recommended)
  - [Workflow](#workflow)
  - [Notes](#notes)
- [References](#references)

> **Note:** Some example paths in this document may reflect local dev deployments.
> In production, cluster-wide metadata is stored in the node’s config directory, while BMD and VMD - bucket and volume metadata, respectively -
> live at the root of each mountpath.
> See [xmeta (tool) README](https://github.com/NVIDIA/aistore/blob/main/cmd/xmeta/README.md) for more details and examples.

---

## First Checks: Cluster State via CLI

AIS provides extensive CLI tab-completion and discovery.

Start with:

```console
$ ais show cluster
````

or explore available subcommands interactively:

```console
$ ais show cluster <TAB><TAB>
```

Example output:

```console
PROXY            MEM USED %    MEM AVAIL       UPTIME
202446p8082      0.06%         31.28GiB        19m
279128p8080      0.07%         31.28GiB        19m
928059p8081[P]   0.08%         31.28GiB        19m

TARGET           MEM USED %    MEM AVAIL       CAP USED %      CAP AVAIL       CPU USED %    REBALANCE      UPTIME
147665t8084      0.07%         31.28GiB        14%             2.511TiB        0.00%         -              19m
...
```

At any time there is exactly one **primary proxy**.
If needed, you can change it administratively:

```console
$ ais cluster set-primary <TAB><TAB>
$ ais cluster set-primary p[279128p8080]
```

---

## Error Categories

AIS integrity errors fall into two distinct categories:

1. **Cluster Integrity Errors (`cie#`)**
   Inconsistent or conflicting cluster-wide metadata (Smap, BMD, etc.)

2. **Storage Integrity Errors (`sie#`)**
   Inconsistent, missing, or invalid [mountpath](/docs/overview.md#mountpath) metadata on a target

Understanding which category you are dealing with is critical:
**CIE errors are cluster-scoped; SIE errors are target-scoped.**

---

## Cluster Integrity Errors (CIE)

Cluster Integrity Errors are raised when a node attempts to join or operate
in a cluster with **incompatible cluster-wide metadata**.

Example:

```text
cluster integrity error `cie#50`:
Smaps have different origins: Smap v9[...] vs p[232268p8080]: Smap v13[...]
```

These errors usually indicate that a node:

* belonged to a **different AIS cluster** in the past, or
* has **stale local metadata** that conflicts with the cluster majority.

### Common Causes

* Reusing disks or nodes from a previous cluster
* Mixing nodes from different deployments
* Partial cleanup after redeployments

### CIE Error Reference

| Error    | When                  | Meaning                                         |
| -------- | --------------------- | ----------------------------------------------- |
| `cie#10` | Primary startup       | Primary’s local Smap conflicts with other nodes |
| `cie#30` | Startup               | Targets disagree on cluster UUID                |
| `cie#40` | Startup or BMD update | Local BMD conflicts with cluster                |
| `cie#50` | Join / metasync       | Node is not permitted to join cluster           |
| `cie#60` | Primary startup       | Conflicting incompatible BMD versions           |
| `cie#70` | Primary startup       | Conflicting BMDs with simple majority           |
| `cie#80` | Node join             | Node believes it belongs to a different cluster |
| `cie#90` | Metasync              | Split-brain detected during metadata sync       |

### Recovery (CIE)

Recovery often involves **carefully cleaning obsolete metadata**:

* local Smap copies
* local BMD copies

> **This must be done with extreme caution.**
Removing the wrong metadata can permanently orphan data.

CIE recovery is intentionally conservative and usually requires
manual inspection and understanding of cluster history.

---

## Storage Integrity Errors (SIE)

Storage Integrity Errors relate to **mountpaths attached to a storage target**.
Each target maintains **Volume Metadata (VMD)** describing its mountpaths,
their filesystems, and the target’s persistent Node ID.

Example:

```text
storage integrity error sie#50:
lost or missing mountpath "/ais/nvme7n1"
```

### Key Concepts

* VMD is **persisted and replicated** across all mountpaths of a target
* Each [mountpath](/docs/overview.md#mountpath) records:

  * filesystem identity
  * filesystem type
  * target Node ID
* VMD validation happens **at target startup**, before runtime checks (FSHC)

---

## SIE Error Reference

| Error    | When    | Meaning                                          |
| -------- | ------- | ------------------------------------------------ |
| `sie#10` | Startup | Mountpaths record different Node IDs             |
| `sie#20` | Startup | Target Node ID conflicts with mountpath metadata |
| `sie#30` | Startup | Mountpaths disagree on persisted metadata        |
| `sie#40` | Startup | Corrupted metadata on a mountpath                |
| `sie#50` | Startup | Mountpath mismatch between config and VMD        |

---

## Target Fails to Start: Lost or Mismatched Mountpath (SIE#50)

### Symptoms

Target fails during startup with an error similar to:

```text
storage integrity error sie#50:
lost or missing mountpath "<path>"
```

This commonly occurs after:

* disk failure or replacement
* filesystem remounted on the wrong device
* OS block-device re-enumeration
* upgrade or restart while a disk is unavailable

In this state:

* the target **cannot reach runtime**
* filesystem health checks (FSHC) **cannot run**
* the target exits fatally to prevent data corruption

---

## Recovery: Offline VMD Edit (Recommended)

This recovery method is **safe, explicit, and reversible**.

### Workflow

1. If applicable: put **the AIS target** in maintenance mode (or shutdown entire cluster)
2. Identify a failed mountpath (e.g., `/ais/nvme7n1`)
3. Possibly, SSH into the target; use `xmeta` tool to disable the failed mountpath in a given selected VMD replica:

```console
xmeta -x -in=/ais/nvme0n1/.ais.vmd -disable /ais/nvme7n1
```

4. Copy the updated VMD to all remaining mountpaths, e.g.:

```console
for mp in /ais/nvme{1,2,3,4,5,6,8,9,10,11}n1; do
    cp /ais/nvme0n1/.ais.vmd $mp/.ais.vmd
done
```

5. Restart the target (/ cluster)

6. Verify:

```console
ais storage mountpath show ### all storage nodes, all mountpaths
```

or:

```console
ais storage mountpath TARGET
```

The target will now restart in a **degraded but safe state**, with `/ais/nvme7n1` disabled.

### Notes


* **Backup First**

Before troubleshooting that involves inspecting or modifying **any** on-disk metadata:

- **Archive all AIS metadata** periodically (and especially before any manual edits).
- AIS keeps multiple copies of critical metadata, but redundancy is not a substitute for backups.
- When in doubt: **copy first, edit later**.

Speaking of VMD, at minimum back up each mountpath’s metadata and keep the archive somewhere outside /this/ node.

Additionally:

* Keeping all VMD copies in sync is strongly recommended.
* Disabled mountpaths are **not probed or used**.
* The mountpath can be re-enabled later after disk replacement.
* Prefer **explicit, minimal changes** over broad metadata deletion.
* Avoid `ignore-missing-mountpath` unless you fully understand the implications.
* `xmeta` is a **power tool**: indispensable for recovery, dangerous if misused.

If unsure, stop to inspect existing metadata before proceeding, and maybe back it up as well.

## References

* [AIStore: Terminology](/docs/overview.md#terminology)
* [On-disk layout](https://github.com/NVIDIA/aistore/blob/main/docs/on_disk_layout.md)
* [xmeta](https://github.com/NVIDIA/aistore/blob/main/cmd/xmeta/README.md) - utility to inspect, extract, format, and (in limited cases) edit internal AIS metadata structures
* [AIS buckets: on-disk layout](/docs/bucket.md#appendix-a-on-disk-layout)
