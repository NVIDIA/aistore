---
layout: post
title: "Automated API Documentation Generation with GenDocs"
date: August 29, 2025
author: Anshika Ojha
categories: aistore tools documentation api swagger openapi
---

# Automated API Documentation Generation with GenDocs

Maintaining accurate and up-to-date HTTP API documentation is critical for the developer experience when building and debugging SDKs. Clear HTTP documentation saves developers from digging through AIStore source code to understand expected endpoints, actions, query parameters, and request formats—whether implementing new features or troubleshooting issues in the SDK. With REST API endpoints spanning object management, cluster operations, ETL workflows, and administrative functions, manually maintaining this documentation quickly becomes a bottleneck that leads to inconsistencies and outdated information.

This is where **GenDocs** comes in—a powerful tool that automatically generates comprehensive [OpenAPI](https://spec.openapis.org/oas/latest.html)/[Swagger](https://swagger.io/tools/swagger-ui/) documentation directly from AIStore's Go source code using descriptive annotation-based parsing.

GenDocs streamlines AIStore's documentation workflow, eliminates manual maintenance overhead, and ensures that API documentation stays perfectly synchronized with the codebase as it evolves.

## The Challenge: Scale and Consistency

AIStore's REST API surface is extensive, covering everything from basic object operations to complex multi-cloud data management and ETL transformations. Each endpoint requires documentation that includes:

- HTTP methods and paths with parameter definitions
- Request/response schemas and examples 
- Action-based operations with multiple model variants
- Interactive code samples and curl commands
- Proper categorization and cross-references

Maintaining this manually across a rapidly evolving codebase presents several challenges:

- **Synchronization Drift**: Documentation inevitably falls behind code changes
- **Human Error**: Manual updates are prone to inconsistencies and omissions
- **Developer Overhead**: Engineers spend valuable time on documentation maintenance
- **Scalability**: As the API grows, manual processes become increasingly unsustainable

## The GenDocs Solution

GenDocs solves these problems through **annotation-driven documentation generation**. Instead of maintaining separate documentation files, developers add lightweight annotations directly in the Go source code alongside their API handlers. GenDocs then parses these annotations to automatically generate comprehensive OpenAPI specifications which are rendered into a formatted website that developers can easily reference.

### Core Design Principles

1. **Developer-Friendly**: Minimal annotation syntax that doesn't clutter code
2. **Source of Truth**: Documentation lives alongside implementation code
3. **Automatic Generation**: Zero manual steps to update documentation
4. **Universal format**: Generates standard OpenAPI spec (YAML/JSON)

### Annotation Syntax

GenDocs uses a simple but powerful annotation format. Here's how developers document an API endpoint:

```go
// +gen:endpoint GET /v1/buckets/{bucket-name}/objects/{object-name} [provider=string]
// Retrieves an object from the specified bucket.
// Supports streaming for large objects and conditional requests.
func GetObject(w http.ResponseWriter, r *http.Request) {
    // implementation...
}
```

This single annotation automatically generates:
- OpenAPI endpoint definition
- Parameter documentation
- HTTP examples with proper curl commands

### Advanced Features

#### Action-Based Endpoints

Many AIStore endpoints support multiple operations through action parameters. In AIStore, an "action" is a JSON message in the request body that at minimum includes an `{"action":"..."}` string; some actions also carry a structured `value` field. The action constants (e.g. `apc.ActCopyBck`) map to the action string used in the body, and the associated model defines the `value`.

```go
// +gen:endpoint PUT /v1/buckets/{bucket-name} action=[apc.ActCopyBck=apc.TCBMsg|apc.ActETLBck=apc.TCBMsg]
// +gen:payload apc.ActCopyBck={"action": "copy-bck", "value": {"dry_run": false}}
// +gen:payload apc.ActETLBck={"action": "etl-bck", "value": {"id": "ETL_NAME"}}
// Administrative bucket operations including copy and ETL transformations.
func BucketHandler(w http.ResponseWriter, r *http.Request) {
    // implementation...
}
```

This generates comprehensive documentation showing:
- All supported actions and their models
- Complete JSON payload examples

#### Automatic Model Discovery

GenDocs automatically discovers Go structs marked with `// swagger:model` and incorporates them into the API documentation:

```go
// swagger:model
type Transform struct {
    Name     string       `json:"id,omitempty"`
    Pipeline []string     `json:"pipeline,omitempty"`
    Timeout  cos.Duration `json:"request_timeout,omitempty" swaggertype:"primitive,integer"`
}
```

Note: `swaggertype` is only needed by swagger when mapping custom Go types (e.g., cos.Duration) to primitive types (e.g. `integer`) in the generated OpenAPI spec. Primitive fields like `string`, `int`, `bool`, etc. do not require it.

#### Intelligent Payload Generation

For simple actions that only require an action name, GenDocs automatically generates basic payloads, reducing annotation overhead:

```go
// +gen:endpoint PUT /v1/cluster action=[apc.ActResetConfig=apc.ActMsg|apc.ActRotateLogs=apc.ActMsg]
// These simple actions auto-generate: {"action": "reset-config"} and {"action": "rotate-logs"}
```

## Integration with AIStore's Workflow

GenDocs is seamlessly integrated into AIStore's development workflow and CI pipeline:

### Documentation Website Deployment Workflow

1. **Code Changes**: Developers add/modify API endpoints with annotations
2. **Local Testing**: `make api-docs-website` generates documentation locally
3. **CI Pipeline**: GitHub Actions automatically regenerates docs on merge
4. **Website Deployment**: Updated documentation is deployed to the AIStore website

### Build Process

![GenDocs Workflow](/assets/gendocs/gendocs-workflow.png)
*Figure: GenDocs multi-phase pipeline transforming source code annotations into comprehensive API documentation*

The documentation generation process is a multi-stage pipeline that transforms source code annotations into an OpenAPI specification and markdown.

(1) The process begins when GenDocs scans the entire AIStore codebase, discovering every `+gen:endpoint` annotation and building a complete inventory of API endpoints, parameters, and data models. (2) During this discovery phase, the tool also collects `+gen:payload` definitions and (3) action mappings that will be used to generate realistic examples.

(4) Once the scanning is complete, GenDocs transforms these annotations into standard Swagger comments that can be processed by the OpenAPI toolchain. This transformation includes generating operation IDs, parameter documentation, and request/response schemas for each endpoint.

(5) The OpenAPI specification is then generated using the Swagger tooling, producing both YAML and JSON formats that contain the complete API definition. However, the standard OpenAPI specification lacks some of the rich metadata that makes AIStore's documentation particularly useful.

(6) This is where GenDocs' vendor extension system comes into play. The tool injects AIStore-specific extensions into the OpenAPI specification, including action-to-model mappings and complete HTTP examples with curl commands. These extensions are what enable the interactive features and comprehensive examples in the final documentation.

(7) The final step involves converting the enhanced OpenAPI specification into markdown format using the OpenAPI Generator CLI with custom templates. (8) This produces the website-ready documentation that is integrated into AIStore's Jekyll-based documentation site.

In practice, the CI pipeline runs this workflow automatically—developers only need to provide the GenDocs annotation syntax.

### User Experience Enhancements

The auto-generated documentation provides users with:

```bash
# Working curl examples for every endpoint
curl -i -L -X PUT \
  -H 'Content-Type: application/json' \
  -d '{"action": "copy-bck", "value": {"dry_run": false}}' \
  'AIS_ENDPOINT/v1/buckets/source-bucket'
```

## Technical Architecture and Annotations

### Parsing Engine

Maintaining accurate API documentation is difficult when complex model structs are spread across the codebase. Manually discovering these structs and keeping cross-references between endpoints, actions, and data models in sync is time-consuming and error-prone.

To solve this, the Abstract Syntax Tree (AST) parsing approach is used to analyze the codebase directly. It automatically discovers model structs, builds a complete inventory of API models and their relationships, and maintains precise links between `+gen:endpoint` annotations and their corresponding handler functions.

A second challenge is flexibility: developers often want to place annotations close to the logic they describe, even if that means spreading them across multiple files. For example, a payload definition might live near a helper function rather than in the main endpoint file.

By using a file walker that recursively scans the codebase, GenDocs is collecting every `+gen:payload` annotation. It then parses endpoints file-by-file, ensuring that payload definitions are correctly applied to their endpoints regardless of where they are declared.

To further reduce drift, we started with [`go generate`](https://pkg.go.dev/cmd/go#hdr-Generate_Go_files_by_processing_source) as the primary goal was to keep documentation annotations in line with the code. Annotations live next to handlers and regeneration runs with builds, so the docs track the exact code state—no separate “docs repo,” less drift, and less context‑switching for developers.

To prevent annotations from becoming too verbose, we auto‑generate simple `{ "action":"..." }` payloads where possible. When an action takes a structured `value` or a `name`, we add a `+gen:payload`. S3‑compatible endpoints are the exception—they expect XML. For those, we point to an XML body via `payload=`, and the generator switches the `Content‑Type` automatically.

On the spec side, [Swaggo](https://github.com/swaggo/swag) scans Go code and inline annotations and emits an OpenAPI document that feeds straight into the website pipeline. For custom wrappers (for example, `cos.Duration`), the `swaggertype` tag tells the generator how the field should appear in the spec, keeping models faithful to the API’s serialization.

### Descriptive Comments

Right after a `+gen:endpoint` line, GenDocs reads the plain comment lines and encapsulates them into the endpoint’s summary. Those few sentences become the description on the website detailing what it does, why a developer would call it, and any guardrails (auth, permissions, size limits).

Separately, model struct fields can include Go comments alongside their JSON which become per‑field descriptions in the generated schema (e.g., allowed values, units, defaults). Keeping these comments close to the code ensures the final API docs reflect the intended behavior and field semantics without manual editing. In addition, the vendor extension framework enables injection of AIStore-specific metadata while maintaining full OpenAPI specification compliance.

### Case Study: Isolating a client issue with a direct API call
A bucket deletion operation failed when invoked via CLI. To determine whether the issue was in the client or the AIStore cluster, the operation was executed directly using the documented HTTP example generated by GenDocs. The direct API call succeeded, confirming server behavior was correct and narrowing the problem to the CLI implementation. This illustrates how canonical HTTP examples enable developers to easily isolate client‑versus‑server issues, reduce time to root cause, and focus fixes on the right component.

```bash
curl -i -L -X DELETE \
  -H 'Content-Type: application/json' \
  -d '{"action":"destroy-bck"}' \
  'AIS_ENDPOINT/v1/buckets/BUCKET_NAME'
```

## Conclusion

GenDocs is a shift in AIStore's approach to API documentation—from manual 
maintenance to automated generation that scales with the codebase. By embedding documentation 
directly in source code through lightweight annotations, the tool eliminates synchronization 
issues while significantly improving documentation quality and the developer experience.

In practice, this yielded measurable benefits: comprehensive endpoint coverage, immediate updates with code changes, and consistent formatting across the API surface. Developers can focus on feature development rather than documentation maintenance, while users receive accurate, up‑to‑date documentation with working examples.

## References

- [GenDocs Tool Documentation](https://github.com/NVIDIA/aistore/tree/main/tools/gendocs)
- [AIStore HTTP API Documentation](https://aistore.nvidia.com/docs/http-api)
- [OpenAPI Specification](https://spec.openapis.org/oas/latest.html)
- [AIStore Repository](https://github.com/NVIDIA/aistore)
- [Swagger UI Documentation](https://swagger.io/tools/swagger-ui/)
