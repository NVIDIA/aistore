# AIS CLI Agent Instructions

Apply throughout `cmd/cli/`. Follow [root instructions](../../AGENTS.md), using
the module commands below. Root `go test ./...` and `make lint-scoped` exclude
this separate Go module; use its [Go version](go.mod).

For local core changes, run `go work init ../.. .` in `cmd/cli`.
Do not commit generated workspace files; remove them after verification.

## References

- Read [README.md](README.md) and [CLI documentation](../../docs/cli.md).
- For help text, follow [Help Style Guide](cli/help_style_guide.md).
- Before reusing commands across namespaces, read [makeAlias](cli/make_alias.md).

## Build and Verification

From the repository root, match feature tags across CLI build, lint, tests,
and cluster deployment:

```bash
make cli lint-cli
# Optional-feature example; omit unused tags.
TAGS='dsort' make cli lint-cli
```

Before E2E tests, deploy per root instructions and put rebuilt `ais` on `PATH`.
From `cmd/cli`, set `AIS_ENDPOINT` for the cluster; focus on the affected
`test/*.in` case and confirm it runs.

```bash
AIS_ENDPOINT=http://localhost:8080 go test -v -tags=debug -count=1 ./test \
  -run '^TestE2E$' -args -ginkgo.focus='copy_object'
```

Add relevant test build tags. Use `.in`, `.stdout`, and `.cleanup` fixtures
for command regressions; run affected package tests from this module.
