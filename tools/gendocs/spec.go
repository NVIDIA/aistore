// Package main provides OpenAPI 3.0 spec building from parsed endpoint data.
//
// Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

// decodeJSONExample parses a +gen:payload JSON string into a generic value
// suitable for the Fern example.request field. Returns nil on malformed JSON.
func decodeJSONExample(s string) any {
	if s == "" {
		return nil
	}
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return nil
	}
	return v
}

// payloadToRequest converts a +gen:payload body into a value for
// x-fern-examples[].request. JSON parses to structured data; other formats
// (XML, plain text) round-trip as raw strings; empty input returns nil.
func payloadToRequest(body string) any {
	if body == "" {
		return nil
	}
	if parsed := decodeJSONExample(body); parsed != nil {
		return parsed
	}
	return body
}

// OpenAPI 3.0 document types.

type openapiSpec struct {
	OpenAPI    string              `yaml:"openapi"`
	Info       openapiInfo         `yaml:"info"`
	Paths      map[string]pathItem `yaml:"paths"`
	Components *openapiComponents  `yaml:"components,omitempty"`
}

type openapiInfo struct {
	Title       string `yaml:"title"`
	Description string `yaml:"description,omitempty"`
	Version     string `yaml:"version"`
}

type openapiComponents struct {
	Schemas map[string]*schemaObject `yaml:"schemas,omitempty"`
}

type pathItem map[string]*operation

type operation struct {
	Summary       string                `yaml:"summary,omitempty"`
	OperationID   string                `yaml:"operationId,omitempty"`
	Tags          []string              `yaml:"tags,omitempty"`
	Parameters    []oaParameter         `yaml:"parameters,omitempty"`
	RequestBody   *oaRequestBody        `yaml:"requestBody,omitempty"`
	Responses     map[string]oaResponse `yaml:"responses"` // required by OpenAPI 3.0 (§4.7.15)
	XFernExamples []fernExample         `yaml:"x-fern-examples,omitempty"`
}

// fernExample is a single entry in the `x-fern-examples` operation-level array.
// Fern's OpenAPI importer uses these to populate its named-example selector in
// the rendered docs. Names shared across requests/responses get correlated.
// Per-example CodeSamples override Fern's auto-cURL generator, which is
// method-driven and strips request bodies on GET/HEAD/DELETE.
// See https://buildwithfern.com/learn/api-definitions/openapi/extensions/request-response-examples
type fernExample struct {
	Name        string           `yaml:"name,omitempty"`
	Request     any              `yaml:"request,omitempty"`
	CodeSamples []fernCodeSample `yaml:"code-samples,omitempty"`
}

type fernCodeSample struct {
	Language string `yaml:"language"`
	Code     string `yaml:"code"`
}

type oaParameter struct {
	Name        string          `yaml:"name"`
	In          string          `yaml:"in"`
	Description string          `yaml:"description,omitempty"`
	Required    bool            `yaml:"required,omitempty"`
	Schema      *schemaProperty `yaml:"schema,omitempty"`
}

type oaRequestBody struct {
	Description string               `yaml:"description,omitempty"`
	Required    bool                 `yaml:"required,omitempty"`
	Content     map[string]oaContent `yaml:"content"`
}

type oaContent struct {
	Schema *schemaProperty `yaml:"schema"`
}

type oaResponse struct {
	Description string `yaml:"description"`
}

// buildSpec constructs the full OpenAPI 3.0 document.
func buildSpec(endpoints []endpoint, schemas map[string]*schemaObject, actionMap, globalPayloads map[string]string) *openapiSpec {
	spec := &openapiSpec{
		OpenAPI: "3.0.3",
		Info: openapiInfo{
			Title:   "AIStore REST API",
			Version: "3.0",
		},
		Paths: make(map[string]pathItem),
	}

	if len(schemas) > 0 {
		spec.Components = &openapiComponents{Schemas: schemas}
	}

	if spec.Components == nil {
		spec.Components = &openapiComponents{Schemas: make(map[string]*schemaObject)}
	}
	// Group by (path, method). Track first-seen order for deterministic output.
	type pmKey struct{ path, method string }
	groups := make(map[pmKey][]*endpoint)
	order := make([]pmKey, 0, len(endpoints))
	for i := range endpoints {
		ep := &endpoints[i]
		k := pmKey{ep.Path, ep.Method}
		if _, seen := groups[k]; !seen {
			order = append(order, k)
		}
		groups[k] = append(groups[k], ep)
	}
	for _, k := range order {
		eps := groups[k]
		var op *operation
		if len(eps) == 1 {
			op = buildOperation(eps[0], actionMap, globalPayloads, spec.Components.Schemas)
		} else {
			op = mergeOperations(eps, actionMap, globalPayloads, spec.Components.Schemas)
		}
		if _, exists := spec.Paths[k.path]; !exists {
			spec.Paths[k.path] = make(pathItem)
		}
		spec.Paths[k.path][k.method] = op
	}

	return spec
}

// mergeOperations combines endpoints sharing a method+path into one OAS
// operation. OAS 3.0 allows only one operation per route, but AIS dispatches
// some endpoints by query-param presence (e.g. /s3/{bucket} on `?versioning`).
func mergeOperations(eps []*endpoint, actionMap, globalPayloads map[string]string,
	schemas map[string]*schemaObject) *operation {
	ops := make([]*operation, len(eps))
	for i, ep := range eps {
		ops[i] = buildOperation(ep, actionMap, globalPayloads, schemas)
	}
	return &operation{
		Summary:       joinSummaries(ops),
		OperationID:   ops[0].OperationID,
		Tags:          ops[0].Tags,
		Parameters:    unionParameters(ops),
		RequestBody:   mergeRequestBody(ops),
		Responses:     ops[0].Responses,
		XFernExamples: concatExamples(eps, ops, globalPayloads),
	}
}

// joinSummaries concatenates variant summaries with " / " as the alternatives
// separator (titles are not modified).
func joinSummaries(ops []*operation) string {
	parts := make([]string, 0, len(ops))
	for _, op := range ops {
		if op.Summary != "" {
			parts = append(parts, op.Summary)
		}
	}
	return strings.Join(parts, " / ")
}

// unionParameters concatenates parameters across variants, deduped by (In, Name).
func unionParameters(ops []*operation) []oaParameter {
	seen := make(map[string]bool)
	var out []oaParameter
	for _, op := range ops {
		for _, p := range op.Parameters {
			key := p.In + ":" + p.Name
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, p)
		}
	}
	return out
}

// mergeRequestBody returns the first non-nil body. Forces required=false
// when any variant lacks a body, since OAS 3.0 cannot express conditional
// requiredness.
func mergeRequestBody(ops []*operation) *oaRequestBody {
	var first *oaRequestBody
	allHave := true
	for _, op := range ops {
		if op.RequestBody == nil {
			allHave = false
			continue
		}
		if first == nil {
			first = op.RequestBody
		}
	}
	if first == nil {
		return nil
	}
	if !allHave {
		cpy := *first
		cpy.Required = false
		return &cpy
	}
	return first
}

// concatExamples appends each variant's examples, relabels by summary, and
// sets a non-nil Request to anchor Fern's named-example selector.
func concatExamples(eps []*endpoint, ops []*operation, globalPayloads map[string]string) []fernExample {
	var out []fernExample
	for i, op := range ops {
		label := eps[i].Summary
		for _, ex := range op.XFernExamples {
			if label != "" {
				ex.Name = label
			}
			if ex.Request == nil {
				ex.Request = requestAnchor(eps[i], globalPayloads)
			}
			out = append(out, ex)
		}
	}
	return out
}

// requestAnchor returns the variant's payload (via payloadToRequest) or an
// empty object. Anchors Fern's named-example selector.
func requestAnchor(ep *endpoint, globalPayloads map[string]string) any {
	if ep.PayloadRef != "" {
		if body, ok := globalPayloads[ep.PayloadRef]; ok {
			if req := payloadToRequest(body); req != nil {
				return req
			}
		}
	}
	return map[string]any{}
}

// buildOperation converts a parsed endpoint into an OpenAPI operation.
func buildOperation(ep *endpoint, actionMap, globalPayloads map[string]string, schemas map[string]*schemaObject) *operation {
	op := &operation{
		Summary:     ep.Summary,
		OperationID: ep.OperationID,
		Tags:        []string{ep.Tag},
		// TODO: Model actual response types per endpoint via +gen:endpoint annotations
		Responses: map[string]oaResponse{
			"default": {Description: "Successful operation"},
		},
	}

	// Query and header parameters
	for _, p := range ep.Params {
		if p.Description == "" {
			continue
		}
		in := p.In
		if in == "" {
			in = "query"
		}
		op.Parameters = append(op.Parameters, oaParameter{
			Name:        p.Value,
			In:          in,
			Description: p.Description,
			Schema:      &schemaProperty{Type: mapParamType(p.Type)},
		})
	}

	// Path parameters
	for _, seg := range strings.Split(ep.Path, "/") {
		if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "}") {
			op.Parameters = append(op.Parameters, oaParameter{
				Name:     seg[1 : len(seg)-1],
				In:       "path",
				Required: true,
				Schema:   &schemaProperty{Type: "string"},
			})
		}
	}

	// Emit request body whenever the endpoint declares actions or a
	// model. AIS carries an ActMsg body on non-HEAD methods (including
	// GET and DELETE) to select a variant; HEAD is bodyless.
	hasActions := ep.Method != "head" && len(ep.Actions) > 0
	if ep.Method != "head" {
		if hasActions {
			op.RequestBody = buildActionRequestBody(ep, ep.Actions, actionMap, schemas)
			op.XFernExamples = buildFernExamples(ep.Actions, actionMap, globalPayloads, ep.HTTPExamples)
		} else if len(ep.Models) > 0 {
			op.RequestBody = buildModelRequestBody(ep.Models)
		}
	}

	// For bodyless endpoints (HEAD, or any without actions), emit
	// x-fern-examples carrying just the code-samples. Fern's own
	// method-driven auto-generator strips bodies on HEAD/GET/DELETE and
	// produces a minimal bare-cURL; x-fern-examples is honored in all
	// cases and renders our fully-qualified command (with AIS_ENDPOINT
	// and query-parameter placeholders) verbatim.
	if !hasActions {
		for _, ex := range ep.HTTPExamples {
			op.XFernExamples = append(op.XFernExamples, fernExample{
				Name:        ex.Label,
				CodeSamples: []fernCodeSample{{Language: "curl", Code: ex.Command}},
			})
		}
	}

	return op
}

// buildActionRequestBody emits a tagged-union request body for an
// action-based endpoint. The wire format is the universal ActMsg envelope
// ({action, name, value}) where action is the tag and value's shape is
// determined by the tag. For each supported wire we create a dedicated
// request schema that pins action to a single-value enum and declares the
// exact value shape for that wire (a $ref to the typed payload struct, or
// omitted entirely for payloadless actions). These per-wire schemas are tied
// together with oneOf + discriminator at the request-body level — the
// idiomatic OAS 3.0 tagged-union pattern, which Fern renders as one tab per
// action.
func buildActionRequestBody(ep *endpoint, actions []actionModel, actionMap map[string]string, schemas map[string]*schemaObject) *oaRequestBody {
	var (
		wires   = make([]string, 0, len(actions))
		refs    = make([]schemaProperty, 0, len(actions))
		mapping = make(map[string]string, len(actions))
	)
	for _, action := range actions {
		wire := strings.TrimPrefix(action.Action, apcPrefix)
		if val, exists := actionMap[wire]; exists {
			wire = val
		}
		wires = append(wires, wire)

		branchName := ep.OperationID + dot + pascalWire(wire)
		branch := &schemaObject{
			Type:     "object",
			Required: []string{"action"},
			Properties: map[string]schemaProperty{
				"action": {
					Type: "string",
					Enum: []string{wire},
				},
			},
		}
		// ActMsg.Name is a sibling of action/value in the wire envelope, but
		// its semantics are per-action and not inferrable from type info. Only
		// surface it on branches whose action opts in via a `+gen:name`
		// annotation; other branches omit `name` to avoid misleading readers.
		if action.NameDesc != "" || action.NameRequired {
			branch.Properties["name"] = schemaProperty{
				Type:        "string",
				Description: action.NameDesc,
			}
			if action.NameRequired {
				branch.Required = append(branch.Required, "name")
			}
		}
		// Model semantics:
		//  - apc.ActMsg (Sentinel): Action is payloadless; omit value.
		//  - Qualified Name (e.g. cmn.TCBMsg): Typed struct; $ref into
		//    components/schemas.
		//  - Bare Identifier (e.g. int, string, bool): Go primitive whose
		//    OAS mapping is resolved via identToSchema.
		// An optional +gen:value description is attached as a schema
		// description (scalar) or an allOf+$ref wrapper (struct, per OAS
		// 3.0 which disallows siblings to $ref).
		if action.Model != actMsgModel {
			branch.Required = append(branch.Required, "value")
			switch {
			case strings.Contains(action.Model, dot):
				valueRef := schemaProperty{Ref: "#/components/schemas/" + action.Model}
				if action.ValueDesc != "" {
					branch.Properties["value"] = schemaProperty{
						Description: action.ValueDesc,
						AllOf:       []schemaProperty{valueRef},
					}
				} else {
					branch.Properties["value"] = valueRef
				}
			default:
				prop := identToSchema(action.Model)
				if prop.Type == "object" {
					fmt.Printf("  Warning: unrecognized primitive %q in action %q; emitting generic object\n", action.Model, action.Action)
				}
				prop.Description = action.ValueDesc
				branch.Properties["value"] = prop
			}
		}
		schemas[branchName] = branch

		ref := "#/components/schemas/" + branchName
		refs = append(refs, schemaProperty{Ref: ref})
		mapping[wire] = ref
	}

	schema := &schemaProperty{
		OneOf: refs,
		Discriminator: &oaDiscriminator{
			PropertyName: "action",
			Mapping:      mapping,
		},
	}

	return &oaRequestBody{
		Description: supportedActionsHeader + strings.Join(wires, ", "),
		Required:    true,
		Content: map[string]oaContent{
			"application/json": {Schema: schema},
		},
	}
}

// buildFernExamples emits one `x-fern-examples` entry per action-wire.
// Each entry carries a named request body that matches the shape of the
// corresponding per-wire oneOf branch, and an explicit code-samples block
// so Fern renders our cURL verbatim instead of falling back to its
// method-driven auto-generator (which strips bodies on GET, HEAD, DELETE).
//
// When the action has a +gen:payload annotation, we use it verbatim;
// otherwise we synthesize a minimal {"action": <wire>} body.
func buildFernExamples(actions []actionModel, actionMap, globalPayloads map[string]string, httpExamples []commandExample) []fernExample {
	curlByLabel := make(map[string]string, len(httpExamples))
	for _, ex := range httpExamples {
		curlByLabel[ex.Label] = ex.Command
	}
	examples := make([]fernExample, 0, len(actions))
	for _, action := range actions {
		wire := strings.TrimPrefix(action.Action, apcPrefix)
		if mapped, ok := actionMap[wire]; ok {
			wire = mapped
		}
		var body any
		if raw, ok := globalPayloads[action.Action]; ok {
			body = payloadToRequest(raw)
		}
		if body == nil {
			body = map[string]any{"action": wire}
		}
		ex := fernExample{Name: wire, Request: body}
		if curl, ok := curlByLabel[wire]; ok {
			ex.CodeSamples = []fernCodeSample{{Language: "curl", Code: curl}}
		}
		examples = append(examples, ex)
	}
	return examples
}

// pascalWire converts a wire value (e.g. "copy-bck", "make-n-copies") into
// PascalCase suitable for a schema name suffix (e.g. "CopyBck", "MakeNCopies").
func pascalWire(wire string) string {
	parts := strings.Split(wire, "-")
	for i, p := range parts {
		if p != "" {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	return strings.Join(parts, "")
}

func buildModelRequestBody(models []string) *oaRequestBody {
	if len(models) == 0 {
		return nil
	}
	var schema *schemaProperty
	if len(models) == 1 {
		schema = &schemaProperty{Ref: "#/components/schemas/" + models[0]}
	} else {
		var refs []schemaProperty
		for _, model := range models {
			refs = append(refs, schemaProperty{Ref: "#/components/schemas/" + model})
		}
		schema = &schemaProperty{OneOf: refs}
	}
	return &oaRequestBody{
		Required: true,
		Content: map[string]oaContent{
			"application/json": {Schema: schema},
		},
	}
}

func mapParamType(t string) string {
	switch t {
	case "bool":
		return "boolean"
	case "int", "int64":
		return "integer"
	default:
		return "string"
	}
}
