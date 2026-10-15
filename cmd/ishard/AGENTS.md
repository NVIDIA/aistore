# iShard Agent Instructions

Apply throughout `cmd/ishard/`. Follow [root instructions](../../AGENTS.md), using
the module commands below. Root `go test ./...` and `make lint-scoped` exclude
this separate Go module; use its [Go version](go.mod).

For local core changes, run `go work init ../.. .` in `cmd/ishard`.
Do not commit generated workspace files; remove them after verification.

## References

Read [README.md](README.md) for sample grouping, shard sizing, and options.
Keep every sample's files together in one output shard.

## Build and Verification

From the repository root (build enables `dsort`; lint appends it if missing):

```bash
make ishard lint-ishard
```

From `cmd/ishard`, run tests with `dsort`. Integration tests in
`ishard/ishard_test.go` use `config.DefaultConfig.URL` (currently
`http://127.0.0.1:8080`) and ignore `AIS_ENDPOINT`; deploy there per root
instructions. Select a relevant test and confirm it runs.

```bash
go test -v -tags='debug dsort' -count=1 ./ishard \
  -run '^TestIshardNoRecordsSplit$'
```

Root integration suites exclude iShard. For shared sharding changes, also verify
affected `ext/dsort` code with `dsort`.
