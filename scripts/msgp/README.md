# Msgpack code generation for control-plane types

This directory contains the helper script and notes for generating msgpack
encoders/decoders used by selected AIS control-plane APIs.

## What is covered

The current msgpack coverage is limited and intentional.

It includes the wire types needed for selected control-plane responses, notably:

- `stats.NodeStatus`
- `stats.Node`
- `fs.TcdfExt`

and their transitive dependencies from packages such as:

- `api/apc`
- `fs`
- `sys`
- `cmn/cos`
- `core`
- `core/meta`

The implementation uses a mix of:

- `*_gen.go` — generated msgpack codecs
- `*_wire.go` — transport-focused type definitions extracted from original sources
- `*_msg.go` — hand-written helpers for cases that `msgp` cannot follow or infer

This coverage is not global. It exists to support the current control-plane/API
paths that explicitly negotiate msgpack while preserving the existing JSON API.

## Why this exists

`msgp` works on individual source files and does not automatically follow all
type relationships across files and packages.

For that reason:

- some wire-facing definitions are isolated into `*_wire.go`
- some types require hand-written `*_msg.go` helpers
- generated code is kept separate from handwritten code

The goal is to keep msgpack support compact, explicit, and maintainable.

## How to regenerate

Generate:

```bash
scripts/msgp/control_plane.sh
```

Remove generated files:

```bash
scripts/msgp/control_plane.sh clean
```

Do not edit `*_gen.go` manually.

## How to msgpack-encode another structure

When extending msgpack coverage to another control-plane response:

1. Start with the top-level API structure that will be sent or received.

2. Walk its transitive wire graph and identify all embedded and referenced
   types that must be encodable/decodable.

3. Add compact `msg:"..."` tags to fields that belong on the wire.

4. If a type is awkward to generate from its original source, move or duplicate
   the wire-facing definition into a `*_wire.go` file.

5. If `msgp` reports missing `EncodeMsg`, `DecodeMsg`, or `Msgsize` methods for
   a named map, array, alias-like, or otherwise external type, add a
   hand-written `*_msg.go` helper following existing patterns such as:

   * `stats/copyTracker`
   * `cmn/cos/AllDiskStats`
   * `cmn/cos/FsID`

6. Use `msgp:ignore` and `msgp:shim` directives when needed to keep generation
   under control.

7. Regenerate and inspect the resulting `*_gen.go` code, especially for:

   * embedded-field flattening
   * unexpected field names
   * nested-vs-flat wire shape
   * map handling

8. Add or update a round-trip test for the affected top-level API structure.

## Important conventions

* `*_gen.go` — generated code only
* `*_msg.go` — hand-written msgpack helpers
* `*_wire.go` — extracted wire-facing definitions
* prefer short `msg` tags for compact wire format
* use `msg:",flatten"` carefully:

  * it is natural for embedded structs
  * embedded plain maps may require manual handling
* preserve existing JSON API behavior unless a change is explicitly intended

## Notes

If a new change only affects direct per-node APIs, avoid accidentally changing
cluster-wide aggregate/raw response formats in the same patch.

When in doubt, keep the top-level contract simple and make wire-shape changes
explicit.
