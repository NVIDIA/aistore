---
layout: post
title: CLIENT ECOSYSTEM
permalink: /docs/client-ecosystem
redirect_from:
 - /docs/client_ecosystem.md/
---

AIStore exposes three client-facing surfaces:

- the native HTTP API, documented via the generated [HTTP API reference](/docs/http-api);
- the Go API in [`api/`](https://github.com/NVIDIA/aistore/tree/main/api);
- the Python package in [`python/aistore`](https://github.com/NVIDIA/aistore/tree/main/python/aistore), including the SDK, PyTorch integration, botocore patch, and related tooling.

This document describes how additional clients and ecosystem integrations should fit around those surfaces.

## Goals

The client ecosystem should make AIStore easy to use from data-processing and ML workloads while keeping maintenance practical for the core repository.

- Keep the native HTTP API as the stable contract for language SDKs.
- Keep generated API documentation as the shared source of truth for request paths, payloads, headers, query parameters, and response schemas.
- Allow language-specific SDKs to evolve with their own release cadence when they become large enough to need it.
- Prefer integrations with established data interfaces where they already exist, such as S3-compatible clients, fsspec, OpenDAL, Spark, Dask, PyTorch, and TensorFlow.
- Avoid duplicating server-side semantics in every client. Common behavior such as retries, redirects, authentication, multipart/ranged reads, and streaming should be specified once and tested consistently.

## Current State

| Client surface | Location | Notes |
| --- | --- | --- |
| Go API | [`api/`](https://github.com/NVIDIA/aistore/tree/main/api) | Native Go client used by AIS tools and tests. |
| Python SDK | [`python/aistore/sdk`](https://github.com/NVIDIA/aistore/tree/main/python/aistore/sdk) | Native Python client with object, bucket, job, ETL, batch, authn, and retry support. |
| Python integrations | [`python/aistore/pytorch`](https://github.com/NVIDIA/aistore/tree/main/python/aistore/pytorch), [`python/aistore/botocore_patch`](https://github.com/NVIDIA/aistore/tree/main/python/aistore/botocore_patch) | ML and S3-client compatibility integrations. |
| S3-compatible clients | [`/s3`](/docs/s3compat.md) | Enables existing S3 tools and SDKs for compatible operations. |
| HTTP API reference | [HTTP API reference](/docs/http-api) | Generated from source annotations and model definitions. |

## Repository Strategy

The AIStore monorepo is the right place for the server, Go API, CLI, generated API documentation, and Python SDK while those components are tightly coupled to server development.

Separate SDK repositories are useful when at least one of the following is true:

- the client has a different release cadence from the server;
- the client depends on language-specific build, packaging, or CI systems that are expensive to run in the server repository;
- the client has external maintainers or downstream users who need a smaller repository and issue tracker;
- the client is an ecosystem integration, such as fsspec or OpenDAL, where upstream project conventions should drive the package layout.

If an SDK is split out, the main AIStore repository should keep:

- links to the SDK repository and package registry;
- the generated OpenAPI artifact or generation workflow used by the SDK;
- compatibility tests or conformance fixtures that can be reused by the SDK;
- release notes pointing to client releases when server changes affect client behavior.

## Shared SDK Contract

Every native SDK should implement the following baseline behavior before being considered an official client:

1. Cluster bootstrap and health checks.
2. Bucket create, delete, list, and property operations.
3. Object GET, PUT, DELETE, HEAD, range reads, and listing.
4. Redirect handling for proxy-to-target data paths.
5. AuthN token and TLS configuration.
6. Retry behavior for throttling, transient network errors, and recoverable server responses.
7. Streaming object reads and writes without loading entire objects into memory.
8. Error types that preserve HTTP status, AIS error message, method, path, and node context.
9. Integration tests against a local AIS deployment.

Advanced features can be added incrementally:

- batch object retrieval;
- multipart upload and download;
- ETL initialization and transform APIs;
- job/xaction monitoring;
- dSort;
- native bucket inventory;
- object archive operations;
- dataset and ML framework helpers.

## Candidate SDKs

### Rust

Rust is a strong fit for data-processing systems, OpenDAL integration, and high-throughput streaming clients.

Recommended first milestone:

- a small `ais-client` crate focused on native HTTP operations;
- async support through `reqwest` or a similar ecosystem-standard HTTP client;
- streaming object GET/PUT and range reads;
- bucket and object listing;
- structured AIS error type;
- integration tests against a local AIS cluster.

Recommended follow-up:

- OpenDAL service implementation, either in an AIS-owned repository first or directly upstream if maintainers prefer;
- multipart and batch retrieval support;
- optional authn helper.

### Java

Java is important for JVM data ecosystems such as Spark, Hadoop, Flink, and Trino.

Recommended first milestone:

- a small Java client library with bucket and object operations;
- streaming GET/PUT;
- range reads;
- redirect handling;
- AIS error model;
- Gradle or Maven publication workflow.

Recommended follow-up:

- Hadoop `FileSystem` connector;
- Spark examples;
- Trino or other analytics integration examples if there is maintainer interest.

## Ecosystem Integrations

### fsspec

An fsspec implementation would make AIStore accessible to pandas, Dask, PyArrow, xarray, and many other Python tools.

Recommended first milestone:

- URL scheme such as `ais://bucket/path`;
- read-only object open, listing, info, and exists operations;
- range reads mapped to AIS object range GETs;
- optional write support after the read path is stable;
- reuse the Python SDK transport, retry, TLS, and authn configuration where possible.

### OpenDAL

OpenDAL can provide a common Rust data access layer and unlock integrations with systems that already use OpenDAL.

Recommended first milestone:

- native AIS service implementation with list, stat, read, write, delete, and range read support;
- configuration for endpoint, provider, namespace, TLS, and auth;
- conformance tests for the supported operation set.

If the integration starts outside OpenDAL, it should be structured so it can be upstreamed later with minimal package-layout changes.

## OpenAPI and Code Generation

AIStore already generates OpenAPI documentation from source annotations. That workflow should become the common input for generated or semi-generated clients.

Recommended improvements:

- publish the generated OpenAPI artifact as a release or CI artifact;
- keep examples for authentication, redirects, range reads, and streaming behavior alongside the generated spec;
- add conformance fixtures for common operations so each SDK can validate the same behavior;
- document endpoint stability and compatibility expectations for SDK authors.

Generated clients can accelerate coverage, but idiomatic SDKs should still wrap generated primitives where streaming, retries, redirects, and error handling need language-specific behavior.

## Suggested Roadmap

| Phase | Scope | Output |
| --- | --- | --- |
| 1 | Document client ecosystem contract and repository strategy | This document and links from the docs index. |
| 2 | Publish generated OpenAPI artifact for SDK authors | Stable artifact name and generation instructions. |
| 3 | Add shared conformance fixtures | Language-neutral request/response tests. |
| 4 | Build Rust native client or OpenDAL prototype | Minimal streaming object client and range reads. |
| 5 | Build Java native client prototype | Minimal bucket/object client and range reads. |
| 6 | Add fsspec integration | Python data ecosystem access through `ais://` URLs. |
| 7 | Decide which clients move to separate repositories | Split only after ownership, CI, release cadence, and package publishing are clear. |

## Contribution Guidelines

New SDK or integration proposals should include:

- target language or ecosystem;
- initial operation set;
- packaging and CI plan;
- authentication and TLS behavior;
- retry and redirect behavior;
- test plan against a local AIS deployment;
- whether the code should live in the AIStore repository, an AIS-owned SDK repository, or an upstream ecosystem repository.

For early prototypes, prefer small, reviewable milestones over a full client implementation in one pull request.
