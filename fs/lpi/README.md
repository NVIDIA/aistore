# fs/lpi: local page iterator (LPI)

AIStore stores objects on local filesystems (mountpaths) but often mirrors or synchronizes with a [remote backend](https://github.com/NVIDIA/aistore/blob/main/docs/overview.md#at-a-glance).
When the remote side can change **out-of-band** (objects removed remotely, not via AIS), synchronization must be able to answer:

> "which local objects are no longer present remotely and should be removed locally?"

Doing this safely at scale requires a **streaming** (paged) local traversal with deterministic enough ordering to support a merge/diff
against the remote listing stream.

That is the reason `fs/lpi` exists.

---

## What it does

`fs/lpi` provides a **paged iterator over local content** rooted at a bucket directory on a *single mountpath*.

- It returns pages as `Page` (currently `map[name]size`).
- It supports two ways to bound a page:
  - by **count** (`Msg.Size`)
  - by an **end-of-page marker** (`Msg.EOP`)
- It supports a **prefix** (object-name prefix) to narrow traversal.
- In "real" (non-unit-test) mode it can filter the returned set to objects that are **HRW-local** to the current target
  (using `smap` + `lom.HrwTarget`), so a target only iterates objects it owns.

A companion helper (`allmps.go`) runs per-mountpath LPIs concurrently and is currently used to produce "remote removed"
tombstones during listing/sync flows.

---

## How it works

### 1) per-mountpath iterator (`lpi.go`)

`Iter` is constructed with:

- `root`: bucket content directory on a given mountpath (typically `%mp/%ob/<bucket...>`)
- `prefix`: optional object-name prefix
- `smap`: optional cluster map used to HRW-filter objects (nil in unit tests)

Each `Next(Msg, out Page)` call:

1. Clears and fills `out` in-place.
2. Uses `godirwalk.Walk(root, Unsorted:false, ...)` to traverse the directory tree.
3. Prunes aggressively:
   - skips directories lexicographically "behind" the current start position
   - skips directories outside `root/prefix` when `prefix` is provided
   - skips entries that do not match `prefix`
   - when `smap != nil`, skips objects that are not HRW-local to the current target
4. Stops when either:
   - the page reaches `Msg.Size` entries, or
   - traversal passes beyond `Msg.EOP` (unless `Msg.EOP == AllPages`)
5. Records a resume marker accessible via `Pos()`:
   - `Pos() == ""` means exhausted (no more local content)
   - otherwise `Pos()` is the next starting point for subsequent calls

> **Important:** LPI is not a snapshot iterator. It walks the live filesystem view. It is designed for repeated sync cycles
where best-effort, deterministic traversal is sufficient.

### 2) multi-mountpath coordinator (`allmps.go`)

`Lpis` instantiates one `Iter` per available mountpath and runs them concurrently.

Its current use is "delete-by-absence" support:

- given a remote page (`lastPage`) and its last object name,
- LPI enumerates local objects up to that boundary on each mountpath,
- and emits entries present locally but absent remotely as "version removed" tombstones.

This is how AIS can discover objects removed remotely out-of-band and propagate that removal locally.

---

## Internals

- `type Page map[string]int64`
  Maps **relative object name** (path relative to `root`) to **size**.

- `type Msg struct { EOP string; Size int }`
  Bounds a page:
  - `Size == 0` means "no size limit"
  - `EOP == AllPages` means "no end-of-page bound" (fill all remaining)

- `type Iter struct { ... }`
  Single-mountpath iterator.

### typical pattern (page-by-size)

```go
it, _ := lpi.New(root, prefix, smap)
page := make(lpi.Page, 1024)
msg := lpi.Msg{Size: 1000}

for it.Pos() != "" || it.Pos() == root { // or simply: for { ... break on Pos()=="" }
    _ = it.Next(msg, page)
    // consume page
    if it.Pos() == "" {
        break
    }
}
```

### typical pattern (page-by-EOP)

Use `Msg{EOP: marker}` to request "everything up to marker".
See `fs/lpi_test.go` for an example of generating markers via `Pos()` and replaying them as EOP bounds.

---

## Invariants

* Bounded memory: returns pages, does not materialize the full namespace.
* Stable enough traversal to support "merge/diff" against a sorted remote listing stream.
* Optional HRW filtering to return only objects owned by the target.

### non-guarantees

* No global snapshot consistency under concurrent mutations.
* No global ordering across mountpaths (unless a higher layer merges/sorts).
* `Page` currently contains only size; additional props are not provided yet.

---

## Testing

`fs/lpi_test.go` uses `genTree` to create controlled directory structures, then validates:

| Test name | Description |
|---|---|
| page-by-size | Multiple page sizes (1, 2, exact-fit, larger-than-total) eventually cover all generated files with no duplicates. |
| page-by-EOP | Replaying resume markers from a page-by-size run produces the same complete set. |
| prefix filtering | Iteration with a prefix returns only matching entries (including nested prefixes); a non-existent prefix returns zero. |
| edge cases | Handles empty root (immediate exhaustion), single file, and deep nesting (7 levels). |
| determinism | Two independent iterations over the same tree produce identical names and page boundaries. |
| completeness | LPI results match a `filepath.WalkDir` ground-truth traversal (bidirectional check). |
| file sizes | Returned sizes match actual on-disk sizes. |

---

## Notes / future work

Currently, LPI self-disables on detecting non-monotonic remote pages (first <= prevRemoteLast).

Consider adding a small "forgiveness window" and logic:
- when disabled, x-lso keeps calling the main LPI's Do() method;
- the latter should not walk local mountpaths; it should only observe remote page
  boundaries (first/last) and track a sequence of monotonic pages;
- maintain a counter of consecutive "good" pages where first > prevRemoteLast (and last >= first).
- auto-re-enable after N good pages (e.g. N=5);
- and reset/realign local iterator state (Clear() + restart, or re-init Lpis) so that
  local cursor cannot remain out of sync with remote progression.
