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

### 4. Actions vs Models: When to use which

There are two ways to annotate endpoints based on their payload structure:

#### Action-Based Endpoints: `action=[...]`
Use when an endpoint supports **multiple operations** that share the same URL but perform different actions. Each action expects a JSON payload with an `action` field wrapper.

```json
{
  "action": "selected action",
  "value": "the model",
  "name": "specify if needed for the selected action"
}
```

**Characteristics:**
- Multiple actions share the same endpoint
- Each action has its own model for the `value` field
- JSON payload structure: `{"action": "action-name", "value": model-data}`
- All actions must have a corresponding model
- Requires `+gen:payload` annotations for each action

#### Model-Based Endpoints: `model=[...]`
Use when an endpoint accepts a **single, direct model** as the JSON payload without action wrapper.

### 5. Payload Annotations (Required)
You **must** provide a `+gen:payload` annotation for every action or model specified in your endpoint. This defines the JSON payload structure that will appear in the generated curl examples.

#### For Action-Based Endpoints:
```go
// +gen:endpoint POST /v1/buckets/bucket-name action=[apc.ActCopyBck=apc.TCBMsg|apc.ActETLBck=apc.TCBMsg]
// +gen:payload apc.ActCopyBck={"action": "copy-bck", "value": {"dry_run": false}}
// +gen:payload apc.ActETLBck={"action": "etl-bck", "value": {"id": "ETL_NAME"}}
```

#### For Model-Based Endpoints:
```go
// +gen:endpoint GET /v1/ml/moss/{bucket} model=[apc.MossReq]
// +gen:payload apc.MossReq={"in":[{"objname":"file.tar"}],"mime":"tar","coer":true}
```

**Steps:**
1. **Identify each action or model** in your endpoint annotation
2. **Write the JSON payload** following the appropriate structure (action-wrapped or direct model)
3. **Add the annotation** using: `// +gen:payload`

#### Payload Annotation Placement

For better code maintainability, place payload annotations **above the subfunction** that implements the action (if it exists):

```go
// +gen:endpoint PUT /v1/cluster action=[apc.ActSetConfig=cmn.ConfigToSet|apc.ActXactStart=apc.ActMsg]
// Administrative cluster operations
func (p *proxy) httpcluput(w http.ResponseWriter, r *http.Request) {
    // Main endpoint handler - clean, no payload clutter
}

// +gen:payload apc.ActSetConfig={"action": "set-config", "value": {"timeout": {"send_file_time": "10m"}}}
func (p *proxy) setCluCfgPersistent(w http.ResponseWriter, r *http.Request, toUpdate *cmn.ConfigToSet, msg *apc.ActMsg) {
    // Subfunction implementation - payload annotation right where the action is implemented
}

// +gen:payload apc.ActXactStart={"action": "start-xaction", "name": "rebalance"}
func (p *proxy) xstart(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg) {
    // Another subfunction with its payload annotation
}
```

**Fallback:** If an action doesn't have a dedicated subfunction, place the payload annotation above the main endpoint handler.

These annotations automatically generate the HTTP command examples in the documentation with proper JSON payloads.

### Automatic Payload Generation

The system automatically generates simple payloads for actions that only require the `action` field, reducing manual annotation overhead.

#### Auto-Generated Payloads (Simple Actions)
Actions that only need `{"action": "action-name"}` are **automatically generated** and don't require manual `+gen:payload` annotations:

```go
// +gen:endpoint PUT /v1/cluster action=[apc.ActResetConfig=apc.ActMsg|apc.ActRotateLogs=apc.ActMsg]
// No manual payload needed - these are auto-generated:
// ✓ apc.ActResetConfig → {"action": "reset-config"}  
// ✓ apc.ActRotateLogs → {"action": "rotate-logs"}
```

#### Decision Logic
- **Auto-generated**: Actions that map to `apc.ActMsg` and only need the action name
- **Manual required**: Actions with:
  - Complex `value` objects (configurations, node options, etc.)
  - Additional fields like `name` (xaction names, provider names, etc.)
  - Custom model structures (non-action based endpoints)

The auto-generation uses gap-filling logic: it only generates payloads for actions that don't already have manual annotations, ensuring developers maintain full control when needed.

### Action Constant Mapping
The system automatically maps action constants to their string values:

| Action Constant | Display Name |
|----------------|--------------|
| `apc.ActCopyBck` | `copy-bck` |
| `apc.ActETLBck` | `etl-bck` |
| `apc.ActPromote` | `promote` |
| ... | ... |

This mapping ensures consistency between the `+gen:endpoint` action definitions and the `+gen:payload` labels.

## Data Model Annotations

### Swagger Model Annotation
Use `// swagger:model` to mark Go structs as API data models that should be included in the OpenAPI specification:

```go
// swagger:model
type PromoteArgs struct {
    DaemonID  string `json:"tid,omitempty"` // target ID
    SrcFQN    string `json:"src,omitempty"` // source file or directory
    // ... more fields
}
```

**Model Usage Flow:**
1. Define struct with `// swagger:model` annotation
2. Reference the model in endpoint `action` parameters
3. Generator automatically creates OpenAPI schema definitions
4. Request/response documentation includes the model's fields and types

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
- Multiple endpoints per function: `dsortHandler` + `POST` + `/sort` → `dsortHandlerpostsort`
- Different methods on same path: `dsortHandler` + `GET` + `/sort` → `dsortHandlergetsort`


### Example 1: GET Endpoint with Query Parameters
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
- **Summary**: "Retrieves all buckets in the specified cluster. Supports filtering by cloud provider and
namespace. Returns detailed bucket information including size and object count."

### Example 2: POST Endpoint with Action Parameters and Data Models
```go
// Define the data model first
// swagger:model
type PromoteArgs struct {
    DaemonID  string `json:"tid,omitempty"` // target ID
    SrcFQN    string `json:"src,omitempty"` // source file path
    ObjName   string `json:"obj,omitempty"` // destination object name
    Recursive bool   `json:"rcr,omitempty"` // recursively promote nested dirs
}

// +gen:endpoint POST /v1/objects/{bucket-name}/{object-name}[apc.QparamProvider=string] action=[apc.ActPromote=apc.PromoteArgs]
func PromoteObjects(w http.ResponseWriter, r *http.Request) {
    var args PromoteArgs
    if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
        // Handle error...
        return
    }

    // Implementation...
}
```

This generates documentation that appears on the website as:

```
Supported actions: ActPromote

APC.PROMOTEARGS

apc.PromoteArgs
Properties
Name     Type      Description
tid      String    target ID
src      String    source file path
obj      String    destination object name
rcr      Boolean   recursively promote nested dirs
```

**Supported Actions**: Action names are automatically converted to clickable HTML links that navigate to their corresponding model documentation.

## Limitations of Swagger with Custom Go Types

### Why Manual Swagger Type Annotations Are Needed

Swagger (OpenAPI) documentation generators, such as Swaggo and go-swagger, attempt to infer the OpenAPI primitive type from your Go struct fields. However, they often cannot automatically determine the correct type when your code uses custom types that wrap primitives (e.g., `type Duration time.Duration`). As a result, these fields are either omitted, incorrectly documented, or default to ambiguous or empty object schemas in the generated documentation.

**Main Reasons:**
- **Custom Types**: Types like `cos.Duration` are Go-defined wrappers around primitives (`int64`, etc.), but Swagger sees them as opaque types and cannot guess their intended serialization.
- **Ambiguous Serialization**: Swagger does not understand that `cos.Duration` is stored as nanoseconds, for example, and does not know to emit an integer type automatically.
- **Resulting Issues**: Without intervention, the OpenAPI schema may describe fields as `{}` (empty objects), or the documentation generator may raise errors at build time.

### How to Fix: The swaggertype Tag

To bridge this gap, Swaggo and similar tools offer special struct tags (such as `swaggertype`) so you can explicitly declare how a custom type should appear in the OpenAPI output. This ensures your API consumers see clear, validated primitive types.

**Example Usage:**
```go
// swagger:model
type Transform struct {
    Name    string       `json:"id,omitempty"`
    Timeout cos.Duration `json:"request_timeout,omitempty" swaggertype:"primitive,integer"` // appears as plain integer in OpenAPI
}
```

**Syntax Reference:**
```go
FieldName CustomType `json:"field_name" swaggertype:"primitive,type"`
```
Where `type` can be `integer`, `string`, `boolean`, etc., matching the OpenAPI spec.

### Common Cases Needing Manual Annotation

| Custom Type | Example Tag | OpenAPI Type | Intended Semantics |
|-------------|-------------|--------------|-------------------|
| `cos.Duration` | `swaggertype:"primitive,integer"` | `integer` | Duration, e.g. nanoseconds |
| `cos.SizeIEC` | `swaggertype:"primitive,string"` | `string` | Size, e.g. "1GB", "512MiB" |

### What Happens Without Explicit Annotation

- **Unannotated**: Field may show as an empty object (`{}`), or be omitted or cause validation/documentation errors during Swagger generation.
- **Annotated** (`swaggertype`): Field appears with correct type, format, and examples—aligned with how your code handles serialization and deserialization.

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
- `docs/http-api.md` - Final documentation for website
