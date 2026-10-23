# Core AIStore Agent Instructions

## Scope and References

Apply to these directories and all their subdirectories:

```text
ais/        proxy/target nodes, integration tests
api/        Go client API/SDK
cmd/        node, CLI, utility executables
cmn/        shared configuration, types, utilities
core/       object/cluster metadata, internal APIs
ec/         erasure coding
ext/        ETL, dSort, downloader
fs/         mountpaths, object file layout
hk/         periodic housekeeping callbacks
ios/        disk I/O stats, OS storage interfaces
memsys/     slab/SGL memory management
mirror/     local object replicas
nl/         progress/completion notifications
reb/        cluster-wide rebalance
res/        resilvering across local mountpaths
space/      LRU eviction, storage cleanup
stats/      metrics tracking and export
sys/        CPU, memory, load, processes
tracing/    OpenTelemetry distributed tracing
transport/  intra-cluster object streaming
volume/     volume configuration and metadata
xact/       background jobs, registry, implementations
```

Before changing, building, or testing CLI/iShard, read [CLI](cmd/cli/AGENTS.md)
or [iShard](cmd/ishard/AGENTS.md) instructions. Elsewhere, check for local instructions.
Flag conflicts and cite the relevant document before work.

Consult these references as needed for the task:

- [CONTRIBUTING.md](CONTRIBUTING.md): Human responsibility, disclosure, licensing, and DCO.
- [overview.md](docs/overview.md): Architecture, design, and traffic patterns.
- [terminology.md](docs/terminology.md): Core abstractions and vocabulary.
- [getting_started.md](docs/getting_started.md#local-playground): Local cluster setup.
- [development.md](docs/development.md): Debugging, scripts, and performance comparisons.

## Build and Verification

Run from the repository root in Bash, using the Go version in `go.mod` and
[Makefile](Makefile) targets. Choose commands for the task.

```bash
# Local playground: 3 targets, 3 proxies, 4 mountpaths per target.
MODE=debug make cli deploy <<< $'3\n3\n4'
```

Remaining prompts default to no cloud backends, loopback devices, or tracing.

Use `make kill` before redeploying, or `make restart` with existing configs.
Repeat required `MODE`, `TAGS`, and `GORACE` settings on rebuilds/restarts.

Validate affected behavior against a running local AIStore cluster. Run focused tests for the changed subsystem; CI provides broader regression coverage.
Use existing test infrastructure for behavior and regressions; remove temporary
validation tests that have no upstream.

Replace `TestName` with a relevant test and confirm it runs; no matches is not
verification. Set `AIS_ENDPOINT` and `BUCKET` for the test cluster. Match
deployment `TAGS` and test `TEST_TAGS` to changed code; omit unused tags.

```bash
AIS_ENDPOINT=http://localhost:8080 BUCKET=ais://tmp \
  TESTS_DIR=ais/test RE='^TestName$' make test-run
```

For concurrency changes, enable race detection in both daemons (`GORACE`) and
affected tests (`TEST_RACE=true` for `make test-run`).

For performance-sensitive changes, measure the affected path against a baseline
under comparable conditions. Use relevant microbenchmarks for isolated code and
`aisloader` for cluster workloads. Confirm measurements are produced.

Run formatting and spelling checks; for core Go changes, also `gofmt` changed
files and lint. Install a missing linter with `make lint-update-ci` (CI version).
Activate a Python virtual environment before `make fmt-check`, which installs Ruff.

```bash
make fmt-check spell-check
make lint-scoped
```

Before using Delve, read [debugging.md](deploy/dev/local/debugging.md). Track
checks, results, bug reproduction, and incomplete verification for human review;
the human contributor owns claims and disclosure.

## Engineering Contracts

- Preserve user data. Test changes to integrity, persistence, and transport.
- Use unique test bucket and object names; delete only data created by the test.
  Do not stop or redeploy a shared cluster without authorization. Keep credentials
  out of logs.
- Read implementations and callers for each codepath's contracts and lifetimes.
  Preserve them; update all affected callers when changing a contract.
  Do not redesign shared contracts around a single use.
- Preserve API compatibility. Change APIs only when explicitly instructed;
  follow maintainer decisions on deprecation.
- Prioritize correctness over performance. Understand synchronization primitives
  and their responsibilities before changing concurrent code or critical sections.
- Follow [Coding Style](CONTRIBUTING.md#coding-style).
- Before committing, follow [Commit Messages](CONTRIBUTING.md#commit-messages)
  and contributor requirements for human authorship, sign-off, and disclosure.

## External Sources

AIStore is MIT-licensed. Agents must not copy or adapt third-party code,
regardless of license, without copyright-holder permission and explicit
maintainer approval.
Consult external documentation for interfaces and behavior; implement using
AIStore's existing patterns and contracts.
