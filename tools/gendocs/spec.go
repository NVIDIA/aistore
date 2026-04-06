// Package main provides OpenAPI 3.0 spec building from parsed endpoint data.
//
// Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
package main

import (
	"strings"
)

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
	Summary      string                `yaml:"summary,omitempty"`
	OperationID  string                `yaml:"operationId,omitempty"`
	Tags         []string              `yaml:"tags,omitempty"`
	Parameters   []oaParameter         `yaml:"parameters,omitempty"`
	RequestBody  *oaRequestBody        `yaml:"requestBody,omitempty"`
	Responses    map[string]oaResponse `yaml:"responses"` // required by OpenAPI 3.0 (§4.7.15)
	XCodeSamples []codeSample          `yaml:"x-code-samples,omitempty"`
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

type codeSample struct {
	Lang   string `yaml:"lang"`
	Label  string `yaml:"label,omitempty"`
	Source string `yaml:"source"`
}

// buildSpec constructs the full OpenAPI 3.0 document.
func buildSpec(endpoints []endpoint, schemas map[string]*schemaObject, actionMap map[string]string) *openapiSpec {
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

	for i := range endpoints {
		ep := &endpoints[i]
		op := buildOperation(ep, actionMap)

		if _, exists := spec.Paths[ep.Path]; !exists {
			spec.Paths[ep.Path] = make(pathItem)
		}
		spec.Paths[ep.Path][ep.Method] = op
	}

	return spec
}

// buildOperation converts a parsed endpoint into an OpenAPI operation.
func buildOperation(ep *endpoint, actionMap map[string]string) *operation {
	op := &operation{
		Summary:     ep.Summary,
		OperationID: ep.OperationID,
		Tags:        []string{ep.Tag},
		// TODO: Model actual response types per endpoint via +gen:endpoint annotations
		Responses: map[string]oaResponse{
			"default": {Description: "Successful operation"},
		},
	}

	// Query parameters
	for _, p := range ep.Params {
		if p.Description == "" {
			continue
		}
		op.Parameters = append(op.Parameters, oaParameter{
			Name:        p.Value,
			In:          "query",
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

	// Request body (not valid on GET, HEAD)
	if ep.Method != "get" && ep.Method != "head" {
		if len(ep.Actions) > 0 {
			op.RequestBody = buildActionRequestBody(ep.Actions, actionMap)
		} else if len(ep.Models) > 0 {
			op.RequestBody = buildModelRequestBody(ep.Models)
		}
	}

	// Code samples
	for _, ex := range ep.HTTPExamples {
		op.XCodeSamples = append(op.XCodeSamples, codeSample{
			Lang:   bashLang,
			Label:  ex.Label,
			Source: ex.Command,
		})
	}

	return op
}

func buildActionRequestBody(actions []actionModel, actionMap map[string]string) *oaRequestBody {
	var names []string
	seen := make(map[string]bool)
	var uniqueModels []string
	for _, action := range actions {
		name := strings.TrimPrefix(action.Action, apcPrefix)
		if val, exists := actionMap[name]; exists {
			name = val
		}
		names = append(names, name)
		if !seen[action.Model] {
			seen[action.Model] = true
			uniqueModels = append(uniqueModels, action.Model)
		}
	}

	var schema *schemaProperty
	if len(uniqueModels) == 1 {
		schema = &schemaProperty{Ref: "#/components/schemas/" + uniqueModels[0]}
	} else {
		var refs []schemaProperty
		for _, model := range uniqueModels {
			refs = append(refs, schemaProperty{Ref: "#/components/schemas/" + model})
		}
		schema = &schemaProperty{OneOf: refs}
	}

	return &oaRequestBody{
		Description: supportedActionsHeader + strings.Join(names, ", "),
		Required:    true,
		Content: map[string]oaContent{
			"application/json": {Schema: schema},
		},
	}
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
