# AIStore API Documentation Generator

This tool automatically generates OpenAPI/Swagger documentation from Go source code comments using special annotations.

## How It Works

The documentation generator scans Go source files in the `../ais` directory looking for `+gen:endpoint` annotations in comments. These annotations define REST API endpoints and their parameters.

## Annotation Syntax

### Basic Endpoint Annotation
```go
// +gen:endpoint method /path/to/endpoint
func HandlerFunction() {
    // handler implementation
}
```

### Endpoint with Parameters
```go
// +gen:endpoint method /path/to/endpoint [param1=type,param2=type]
func HandlerFunction() {
    // handler implementation
}
```

## Annotation Components

### 1. Method
The HTTP method for the endpoint:
- `GET` - Retrieve data
- `POST` - Create new resource
- `PUT` - Update existing resource
- `DELETE` - Remove resource
- `PATCH` - Partial update

### 2. Path
The URL path for the endpoint. Can include:
- Static segments: `/buckets/list`
- Path parameters: `/buckets/{bucket-name}/objects/{object-name}`
- Version prefixes: `/v1/clusters`

### 3. Parameters (Optional)
Parameters are specified in square brackets `[param1=type,param2=type]`. Each parameter has:
- **Name**: The parameter name (should match definitions in `../api/apc/query.go`)
- **Type**: The parameter type (`string`, `int`, `bool`, etc.)

## Parameter Definitions

Parameters must be defined in `../api/apc/query.go` with descriptions:

```go
var QueryParameters = map[string]ParameterDefinition{
    "apc.QparamProvider": {
        Name:        "provider",
        Type:        "string", 
        Description: "Cloud provider name (e.g., aws, gcp, azure)",
    },
    // ... more parameters
}
```

## Comment Documentation

Add descriptive comments after the annotation to provide endpoint summaries:

```go
// +gen:endpoint GET /v1/buckets [provider=string,namespace=string]
// Lists all buckets for the specified provider and namespace.
// Returns bucket metadata including creation time and storage class.
func ListBuckets() {
    // implementation
}
```

## Generated Tags

The system automatically generates API tags (groupings) based on the endpoint path:
- `/v1/buckets/...` → **Buckets** tag
- `/v1/objects/...` → **Objects** tag  
- `/v1/health/...` → **Health** tag
- `/v1/etl/...` → **Etl** tag
- `/v1/daemon/...` → **Daemon** tag

## Operation IDs

Operation IDs are automatically generated from function names:
- Simple case: `ListBuckets` → `ListBuckets`
- Multiple endpoints per function: `CreateBucket` + `/buckets/{name}` → `CreateBucketbuckets`

## Complete Example

```go
// +gen:endpoint GET /v1/clusters/{cluster-id}/buckets [provider=string,namespace=string]
// Retrieves all buckets in the specified cluster.
// Supports filtering by cloud provider and namespace.
// Returns detailed bucket information including size and object count.
func GetClusterBuckets(w http.ResponseWriter, r *http.Request) {
    // Extract cluster ID from path
    clusterID := mux.Vars(r)["cluster-id"]
    
    // Get query parameters
    provider := r.URL.Query().Get("provider")
    namespace := r.URL.Query().Get("namespace")
    
    // Implementation...
}
```

This generates:
- **Method**: GET
- **Path**: `/v1/clusters/{cluster-id}/buckets`
- **Parameters**: `provider` (string), `namespace` (string)
- **Tag**: `Clusters`
- **Operation ID**: `GetClusterBuckets`
- **Summary**: "Retrieves all buckets in the specified cluster. Supports filtering by cloud provider and namespace. Returns detailed bucket information including size and object count."

## Running the Generator

### Locally
```bash
# Generate annotations and markdown documentation  
make api-docs-website
```

### Via GitHub Actions
The documentation is automatically generated and deployed when changes are pushed to the main branch via the `deploy-website.yml` workflow.

## Output Files

- `.docs/swagger.yaml` - OpenAPI specification
- `.docs/swagger.json` - OpenAPI specification (JSON format)
- `docs-generated/README.md` - Generated markdown documentation
- `docs/api-documentation.md` - Final documentation for website