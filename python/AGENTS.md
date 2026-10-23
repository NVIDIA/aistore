# AIS Python SDK Agent Instructions

Python clients and integrations for AIStore: SDK, PyTorch datasets, Botocore
support, and MCP tools.

Before changing PyTorch code, tests, or examples, read its
[instructions](aistore/pytorch/AGENTS.md). Paths below are relative to `python/`:

```text
aistore/                 installable AIStore package
aistore/sdk/             cluster, bucket, and object client APIs
aistore/sdk/authn/       authentication, users, roles, and tokens
aistore/sdk/batch/       batch object retrieval and response extraction
aistore/sdk/dataset/     dataset and shard creation utilities
aistore/sdk/dsort/       distributed sorting job configuration and control
aistore/sdk/etl/         ETL configuration and transformation servers
aistore/sdk/multiobj/    object groups, ranges, and bulk operations
aistore/sdk/obj/         object readers, writers, and streaming I/O
aistore/pytorch/         PyTorch datasets, shard readers, and samplers
aistore/botocore_patch/  Botocore redirect support for AIStore S3 access
aistore/mcp/             MCP server and tools for inspecting AIStore
tests/unit/              unit tests grouped by component
tests/integration/       cluster tests grouped by component
tests/perf/              performance benchmarks
tests/s3compat/          S3 compatibility tests
examples/                SDK, PyTorch, ETL, Dask, and deployment notebooks
pyaisloader/             separately packaged Python SDK benchmark CLI
```

## References

Consult only the references needed for the change:

- [SDK README](aistore/sdk/README.md): installation, client APIs, and configuration.
- [Test guide](tests/README.md) and [Makefile](Makefile): dependencies and test suites.
- [pyproject.toml](pyproject.toml): supported Python versions and package extras.
- [Local playground](../docs/getting_started.md#local-playground): cluster setup.
- [CONTRIBUTING.md](../CONTRIBUTING.md): contributor requirements and commit guidance.

## Setup and Verification

Use Python 3.10+ and reuse an existing virtual environment when available.
For environment setup or missing dependencies, follow the SDK README and test guide.
Run pytest from `python/`, selecting only tests affected by the change. Confirm
they ran; skips or no matches do not verify behavior.

For integration tests, set `AIS_ENDPOINT` to a running, disposable AIStore cluster.
Set `BUCKET` only for remote-bucket tests and provide the matching
backend credentials. ETL, AuthN, and extended tests need additional setup; consult
the test guide and Makefile when affected. Keep `nonparallel` tests serial.

Use unique test bucket and object names; delete only data created by the test.
Do not stop or redeploy a shared cluster without authorization. Keep credentials
out of logs.

Extend an existing test with an assertion or parameterized case when it covers
the changed behavior; add a separate test for a distinct failure scenario.
Reuse existing fixtures and helpers in `tests/unit/sdk/` and
`tests/integration/sdk/`. Botocore patch unit tests require a fresh process per
file; run affected files in separate pytest invocations.

For code fixes, follow [Testing Changes](../CONTRIBUTING.md#testing-changes) for
integration evidence and verification with and without the fix. Rerun affected
checks against the final changes. For test-only changes, run the affected tests;
for documentation-only changes, run applicable documentation and spelling checks.

For Python code changes, run these checks from the repository root with the
virtual environment still active:

```bash
make fmt-check spell-check
make lint-python
```

Report checks, results, and any missing cluster, credentials, or skipped coverage.

## Engineering Contracts

- Before changing shared code, inspect its callers and affected tests. Fix shared
  causes at the appropriate layer while preserving caller contracts. Keep unrelated
  cleanup out of the change.
- Preserve public signatures, defaults, return types, and exception behavior.
  Keep public APIs typed and documented; update relevant examples and docstrings.
- Check request/response formats against the Go API and server handlers when
  changing protocol behavior. Reuse existing SDK clients, models, and errors.
- Preserve streaming, retry/resume, and resource ownership contracts; test byte
  integrity and cleanup on failures for changes to object I/O.
- Keep optional dependencies in their existing extras; core SDK imports must work
  without PyTorch or other optional integrations installed.
