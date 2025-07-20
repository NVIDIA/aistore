# Machine‑Learning Operations (`ais ml`)

Introduced in **v3.30**, the `ml` namespace is intended for commands that target ML‑centric data
workflows — bulk extraction of training samples, cross‑bucket collation of model
artifacts, and manifest‑driven slicing of large corpora.

In this document:

* `ais ml get-batch` – one‑shot consolidation of objects (and archived
  sub‑objects) into a single TAR/TGZ/ZIP/TAR.LZ4.

* `ais ml lhotse-get-batch` – higher‑level driver that reads **Lhotse** cut
  manifests and spawns one or many `get-batch` jobs on your behalf.

Jump straight to the section you need:

## Table of Contents
- [Subcommands](#subcommands)
- [`ais ml get-batch`](#ais-ml-get-batch)
  - [Usage](#usage)
  - [Examples](#examples)
  - [Key Options](#key-options)
- [`ais ml lhotse-get-batch`](#ais-ml-lhotse-get-batch)
  - [Lhotse Manifest](#lhotse-manifest)
  - [Usage](#usage-1)
  - [Examples](#examples-1)
  - [Key Options](#key-options-1)
- [Tips & Best Practices](#tips-bestpractices)
- [See Also](#see-also)

---

## Subcommands

```console
$ ais ml <TAB><TAB>
get-batch         # Consolidate objects / archived files into one archive
lhotse-get-batch  # Drive multiple get‑batch runs from Lhotse manifests
````

`--help` is available at every level:

```console
$ ais ml get-batch --help
$ ais ml lhotse-get-batch --help
```

---

## `ais ml get-batch`

Fetch **one set** of inputs—objects, archived files, or byte ranges—possibly
spanning multiple buckets and providers, and package them into a **single**
output archive.
Default format is TAR; pass the destination name with `.tgz`, `.zip`,
`.tar.lz4`, etc. to switch.

Typical uses:

* Build a reproducible training snapshot from disparate buckets.
* Extract a handful of files from a huge shard without rewriting the shard.
* Stream an archive directly into a training job (`--streaming`).

### Usage

```console
ais ml get-batch [SRC ...] DST_ARCHIVE [flags]

# SRC can be:
#   - bucket or virtual directory      (ais://speech/)
#   - single object                    (s3://models/chkpt‑001.pth)
#   - archived path inside an object   (ais://data/shard.tar/file.wav)
```

### Examples

| Goal                                  | One‑liner                                                                               |
| ------------------------------------- | --------------------------------------------------------------------------------------- |
| Package two objects as `training.tar` | `ais ml get-batch ais://dataset --list "audio1.wav,audio2.wav" training.tar`            |
| Build compressed range archive        | `ais ml get-batch ais://models --template "checkpoint-{001..100}.pth" model-v2.tgz`     |
| Extract a single file from a shard    | `ais ml get-batch ais://data/shard.tar/file.wav dataset.zip`                            |
| Drive via JSON spec on disk           | `ais ml get-batch training.tar --spec batch.json`                                       |
| Inline YAML spec, stream to disk      | `ais ml get-batch /tmp/out.tar --spec '{ in: [...], streaming_get: true }' --streaming` |

### Key Options

| Flag                   | Purpose                                                         |
| ---------------------- | --------------------------------------------------------------- |
| `--spec, -f PATH`      | JSON or YAML request; overrides CLI flags.                      |
| `--list OBJ1,OBJ2,...` | Explicit comma‑separated object list.                           |
| `--template STR`       | Brace‑expansion template (supports ranges, steps).              |
| `--prefix DIR/`        | Only objects whose names start with the prefix.                 |
| `--omit-src-bck`       | Strip bucket name inside the resulting archive.                 |
| `--streaming`          | Stream archive as it is built (lower latency, constant memory). |
| `--cont-on-err`        | Keep going on per‑object errors (best‑effort).                  |
| `--yes, -y`            | Assume “yes” to all prompts (non‑interactive).                  |
| `--non-verbose, -nv`   | Minimal progress output.                                        |

Full proto definition: [api/apc/ml.go](https://github.com/NVIDIA/aistore/blob/main/api/apc/ml.go).

---

## `ais ml lhotse-get-batch`

Consumes a **Lhotse** `cuts.jsonl[.gz | .lz4]` manifest and spawns one or many
`get-batch` transactions.  Ideal for speech/ASR pipelines where a manifest
describes thousands of time‑offsets across many recordings.

Key distinction:

| Command            | Output archives | Driving data                |
| ------------------ | --------------- | --------------------------- |
| `get-batch`        | **exactly one** | CLI flags or JSON/YAML spec |
| `lhotse-get-batch` | **one or many** | Lhotse cut manifest         |

### Lhotse Manifest

In a Lhotse manifest, each *cut* JSON line lists one or more **recording sources**
(URI + byte/time offsets).

Manifests may be:

* **Plain text** – `cuts.jsonl`
* **Gzip‑compressed** – `cuts.jsonl.gz` or `cuts.jsonl.gzip`
* **LZ4‑compressed** – `cuts.jsonl.lz4`

Each line is an independent cut.  See the
[Lhotse docs](https://lhotse.readthedocs.io/) for the full schema.

### Usage

```console
ais ml lhotse-get-batch --cuts manifest.jsonl[.gz | .lz4] [DST] [flags]

# With --output-template you may omit DST; the template expands per batch.
```

```console
$ ais ml lhotse-get-batch --help
NAME:
   ais ml lhotse-get-batch - Get multiple objects from Lhotse manifests and package into consolidated archive(s).
     Returns TAR by default; supported formats include: .tar, .tgz or .tar.gz, .zip, .tar.lz4.
     Supports chunking, filtering, and multi-output generation from Lhotse cut manifests.
     Lhotse manifest format: each line contains a single cut JSON object with recording sources;
     Lhotse manifest may be plain (`.jsonl`), gzip‑compressed (`.jsonl.gz` / `.gzip`), or LZ4‑compressed (`.jsonl.lz4`).
     Examples:
     - 'ais ml lhotse-get-batch --cuts manifest.jsonl.gz output.tar'                                        - entire manifest as single TAR;
     - 'ais ml lhotse-get-batch --cuts cuts.jsonl --sample-rate 16000 output.tar'                           - with sample rate conversion;
     - 'ais ml lhotse-get-batch --cuts m.jsonl.lz4 --batch-size 1000 --output-template "a-{001..999}.tar"'  - generate 999 'a-*.tar' batches (1000 cuts each)
     See [Lhotse docs](https://lhotse.readthedocs.io/) for manifest format details.

USAGE:
   ais ml lhotse-get-batch [BUCKET[/NAME_or_TEMPLATE] ...] [DST_ARCHIVE] --spec [JSON_SPECIFICATION|YAML_SPECIFICATION] [command options]

OPTIONS:
   batch-size       number of cuts per output file
   cont-on-err      Keep running archiving xaction (job) in presence of errors in any given multi-object transaction
   cuts             path to Lhotse cuts.jsonl or cuts.jsonl.gz or cuts.jsonl.lz4
   list             Comma-separated list of object or file names, e.g.:
                    --list 'o1,o2,o3'
                    --list "abc/1.tar, abc/1.cls, abc/1.jpeg"
                    or, when listing files and/or directories:
                    --list "/home/docs, /home/abc/1.tar, /home/abc/1.jpeg"
   non-verbose,nv   Non-verbose (quiet) output, minimized reporting, fewer warnings
   omit-src-bck     When set, strip source bucket names from paths inside the archive (ie., use object names only)
   output-template  template for multiple output files (e.g. 'batch-{001..999}.tar')
   prefix           Select virtual directories or objects with names starting with the specified prefix, e.g.:
                    '--prefix a/b/c'   - matches names 'a/b/c/d', 'a/b/cdef', and similar;
                    '--prefix a/b/c/'  - only matches objects from the virtual directory a/b/c/
   sample-rate      audio sample-rate (Hz); used to convert sample offsets (in seconds) to byte offsets
   spec,f           Path to JSON or YAML request specification
   streaming        stream the resulting archive prior to finalizing it in memory
   template         Template to match object or file names; may contain prefix (that could be empty) with zero or more ranges
                    (with optional steps and gaps), e.g.:
                    --template "" # (an empty or '*' template matches everything)
                    --template 'dir/subdir/'
                    --template 'shard-{1000..9999}.tar'
                    --template "prefix-{0010..0013..2}-gap-{1..2}-suffix"
                    and similarly, when specifying files and directories:
                    --template '/home/dir/subdir/'
                    --template "/abc/prefix-{0010..9999..2}-suffix"
   yes,y            Assume 'yes' to all questions
   help, h          Show help
```

### Examples

| Goal                            | Command                                                                                                       |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| Single TAR from entire manifest | `ais ml lhotse-get-batch --cuts manifest.jsonl.gz output.tar`                                                 |
| Resample offsets for 16 kHz     | `ais ml lhotse-get-batch --cuts cuts.jsonl --sample-rate 16000 output.tar`                                    |
| Chunk into 1,000‑cut archives   | `ais ml lhotse-get-batch --cuts manifest.jsonl.lz4 --batch-size 1000 --output-template "batch-{001..999}.tar"` |

### Key Options

| Flag                           | Purpose                                                 |
| -------------------------------| ------------------------------------------------------- |
| `--cuts PATH`                  | **Required.** Path to `cuts.jsonl` or `.jsonl.gz` or `.jsonl.lz4`. |
| `--batch-size N`               | Number of cuts per output archive (default: all).       |
| `--output-template STR`        | Brace‑expansion template for multi‑file output.         |
| `--sample-rate HZ`             | Convert time offsets (sec) → byte ranges for this rate. |
| `--spec, -f PATH`              | Optional JSON/YAML spec (applies to every batch).       |


Remaining flags (`--list`, `--template`, `--prefix`, `--omit-src-bck`, `--streaming`, `--cont-on-err`, `--yes`, `--nv`) behave exactly like in [`get-batch`](#ais-ml-get-batch).

---

## Tips & Best Practices

* **Streaming (`--streaming`) vs multipart (buffered)**
  The flag only toggles *how AIStore emits the response* — nothing changes in the CLI itself.

  | Mode | AIS behavior on the wire | When it helps |
  |------|---------------------------|---------------|
  | `--streaming` | AIStore delivers the _next_ set of requested samples as soon as possible; the resulting (received) archive grows incrementally; there is **no leading `apc.MossResp` JSON message**. | • You require early‑data/low‑latency playback<br>• AIStore shows `low-memory` alert (via `ais show cluster` or Grafana) |
  | *Multipart* (flag omitted) | AIS first assembles the entire resulting archive **in memory**. The HTTP reply then has two parts:<br>1. `apc.MossResp` JSON (ordered one‑to‑one with the request)<br>2. TAR/TGZ/.tar.lz4/ZIP payload. | • Plenty of memory on the server side<br>• You want the header (sizes, order) upfront<br>• Maximum bulk throughput on a fast LAN |

* **Leaving `bucket` / `provider` blank in a spec entry**
  * Pass a *default bucket* when you call the API — e.g. `ais ml get-batch ais://my‑bck --spec …`.
    Any entry with an empty `bucket` field inherits `my‑bck`.
  * If `provider` is omitted the cluster assumes `ais://`.  For **S3, GCS, Azure, OCI**, etc., **always** spell out `"provider":"s3"`, `"gcp"`, … per entry.
  * Mixed‑provider requests are fine; just be explicit for every non‑AIS object.

* **Manifests at scale.** `lhotse-get-batch --batch-size` divides the manifest
  on the client side—no need to pre‑split the file.
* **Check specs into Git.** Treat JSON/YAML batch specs as immutable
  artifacts for reproducibility.

---

## See Also

* **API reference:** [`apc/ml.go`](https://github.com/NVIDIA/aistore/blob/main/api/apc/ml.go)
* **Lhotse docs:** [https://lhotse.readthedocs.io/](https://lhotse.readthedocs.io/)
* **Tutorial:** *Building a speech‑dataset snapshot with `ais ml lhotse-get-batch`* (WIP)
* **General CLI search:** [`ais search`](./search.md)
