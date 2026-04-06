// Package main generates an OpenAPI 3.0 specification from AIStore source code annotations.
//
// Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/NVIDIA/aistore/cmn/cos"
)

var targetRoot string

var (
	// Parameter directories to scan for Qparam constants
	paramDirectories = []string{"api/apc", "ais/s3"}
)

const (
	// File paths and extensions
	aisRelativePath  = "ais"
	docsRelativePath = ".docs"
	goFileExt        = ".go"

	// Generation annotation prefixes
	endpointPrefix = "+gen:endpoint"
	payloadPrefix  = "+gen:payload"

	// Comment parsing
	commentPrefix    = "//"
	commentWithSpace = "// "
	funcKeyword      = "func "

	baseURL          = "AIS_ENDPOINT"
	paramPlaceholder = "<value>"

	// Curl command generation
	curlBase        = "curl -i -L -X "
	headerFlag      = "-H"
	dataFlag        = "-d"
	contentTypeJSON = "Content-Type: application/json"
	contentTypeXML  = "Content-Type: application/xml"
	backslash       = " \\"

	// Action parsing constants
	actionPrefix     = "action=["
	modelPrefix      = "model=["
	payloadRefPrefix = "payload="
	apcPrefix        = "apc."
)

type (
	actionModel struct {
		Action string
		Model  string
	}

	commandExample struct {
		Label   string `yaml:"label"`   // Example title (e.g., "Copy Bucket")
		Command string `yaml:"command"` // HTTP command
	}

	endpoint struct {
		Method       string
		Path         string
		Params       []param
		Summary      string
		OperationID  string
		Tag          string
		Actions      []actionModel
		Models       []string
		PayloadRef   string
		HTTPExamples []commandExample
	}

	fileParser struct {
		Path           string
		ParamSet       *paramSet
		ActionMap      map[string]string
		ModelActions   map[string][]string
		GlobalPayloads map[string]string
		Endpoints      *[]endpoint
	}

	fileWalker struct {
		Root  string
		Files []string
	}

	endpointProcessor struct {
		Walker         *fileWalker
		ParamSet       *paramSet
		ActionMap      map[string]string
		ModelActions   map[string][]string
		GlobalPayloads map[string]string
		Endpoints      []endpoint
	}
)

// Auto-generate URL from gen:endpoint annotation
func buildURLFromEndpoint(ep *endpoint) string {
	url := baseURL + ep.Path

	// Add query parameters if they exist
	if len(ep.Params) > 0 {
		var queryParams []string
		for _, param := range ep.Params {
			queryParams = append(queryParams, fmt.Sprintf("%s=%s", param.Value, paramPlaceholder))
		}
		if len(queryParams) > 0 {
			url += queryStart + strings.Join(queryParams, queryJoin)
		}
	}

	return url
}

// Auto-generate complete HTTP command from endpoint data and optional payload
func generateHTTPCommand(ep *endpoint, payload string) string {
	method := strings.ToUpper(ep.Method)
	url := buildURLFromEndpoint(ep)

	cmd := curlBase + method + backslash + newlineChar

	// Add headers and payload only if payload provided
	if payload != "" {
		// Detect S3 endpoints and use appropriate content type
		isS3Endpoint := strings.HasPrefix(ep.Path, "/s3/") || ep.Path == "/s3"
		contentType := cos.Ternary(isS3Endpoint, contentTypeXML, contentTypeJSON)
		cmd += "  " + headerFlag + " '" + contentType + "'" + backslash + newlineChar
		cmd += fmt.Sprintf("  %s '%s'%s", dataFlag, payload, backslash) + newlineChar
	}

	cmd += fmt.Sprintf("  '%s'", url)
	return cmd
}

// Parse payload annotation: apc.ActCopyBck={"action": "copy-bck"}
func parsePayload(line string) (string, string) {
	content := strings.TrimSpace(line[len(commentWithSpace+payloadPrefix):])
	before, after, ok := strings.Cut(content, "=")
	if !ok {
		return "", ""
	}

	action := strings.TrimSpace(before)
	payload := strings.TrimSpace(after)

	return action, payload
}

// Auto-generate simple payloads that just need the action field
func autoGenerateSimplePayloads(ep *endpoint, payloads, actionMap map[string]string) {
	for _, action := range ep.Actions {
		if _, exists := payloads[action.Action]; exists {
			continue
		}

		actionName := getActionString(action.Action, actionMap)
		if actionName != "" {
			payloads[action.Action] = fmt.Sprintf(`{"action": "%s"}`, actionName)
		}
	}
}

func getActionString(actionConstant string, actionMap map[string]string) string {
	actionName := strings.TrimPrefix(actionConstant, apcPrefix)
	if mappedName, exists := actionMap[actionName]; exists {
		return mappedName
	}
	return ""
}

// Collect all payload annotations from all files in the walker
func (ep *endpointProcessor) collectGlobalPayloads() error {
	for _, file := range ep.Walker.Files {
		content, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("failed to read file %s: %w", file, err)
		}

		lines := strings.SplitSeq(string(content), "\n")
		for line := range lines {
			trimmedLine := strings.TrimSpace(line)
			if strings.Contains(trimmedLine, payloadPrefix) {
				action, payload := parsePayload(trimmedLine)
				if action != "" && payload != "" {
					ep.GlobalPayloads[action] = payload
				}
			}
		}
	}
	return nil
}

// Generate readable labels from endpoint and action (example: "Copy Bucket")
func generateLabelFromAction(action actionModel, actionMap map[string]string) string {
	actionName := strings.TrimPrefix(action.Action, apcPrefix)
	if mappedName, exists := actionMap[actionName]; exists {
		return mappedName
	}
	return actionName
}

// Generate readable label from endpoint path (example: "Objects")
func generateLabelFromPath(path string) string {
	segments := strings.Split(strings.Trim(path, "/"), "/")
	if len(segments) >= 2 {
		return segments[1]
	}
	return defaultLabel
}

// Collect payloads and auto-generate HTTP examples
func collectAndGenerateHTTPExamples(ep *endpoint, actionMap, globalPayloads map[string]string) []commandExample {
	var examples []commandExample
	payloads := make(map[string]string) // action -> payload mapping

	// Copy action payloads from global collection
	for _, action := range ep.Actions {
		if payload, exists := globalPayloads[action.Action]; exists {
			payloads[action.Action] = payload
		}
	}

	// Copy model payloads from global collection
	for _, model := range ep.Models {
		if payload, exists := globalPayloads[model]; exists {
			payloads[model] = payload
		}
	}

	autoGenerateSimplePayloads(ep, payloads, actionMap)

	// Auto-generate examples based on endpoint type
	switch {
	case len(ep.Actions) > 0:
		for _, action := range ep.Actions {
			payload := payloads[action.Action]
			httpCmd := generateHTTPCommand(ep, payload)
			label := generateLabelFromAction(action, actionMap)

			examples = append(examples, commandExample{
				Label:   label,
				Command: httpCmd,
			})
		}
	case len(ep.Models) > 0:
		// Handle model-based endpoints
		for _, model := range ep.Models {
			payload := payloads[model]
			httpCmd := generateHTTPCommand(ep, payload)

			method := strings.ToLower(ep.Method)
			label := strings.ToUpper(method[:1]) + method[1:] + " " + generateLabelFromPath(ep.Path)

			examples = append(examples, commandExample{
				Label:   label,
				Command: httpCmd,
			})
		}
	case len(examples) == 0:
		if ep.PayloadRef != "" {
			if payload, exists := globalPayloads[ep.PayloadRef]; exists {
				httpCmd := generateHTTPCommand(ep, payload)

				method := strings.ToLower(ep.Method)
				label := strings.ToUpper(method[:1]) + method[1:] + " " + generateLabelFromPath(ep.Path)

				examples = append(examples, commandExample{
					Label:   label,
					Command: httpCmd,
				})
			}
		}

		if len(examples) == 0 {
			httpCmd := generateHTTPCommand(ep, "")

			method := strings.ToLower(ep.Method)
			label := strings.ToUpper(method[:1]) + method[1:] + " " + generateLabelFromPath(ep.Path)

			examples = append(examples, commandExample{
				Label:   label,
				Command: httpCmd,
			})
		}
	}

	return examples
}

func main() {
	projectRoot, err := getProjectRoot()
	if err != nil {
		panic(fmt.Errorf("failed to determine project root: %v", err))
	}
	targetRoot = filepath.Join(projectRoot, aisRelativePath)

	var paramSet paramSet
	if err := paramSet.loadFromDirectories(projectRoot, paramDirectories); err != nil {
		panic(err)
	}

	// Load action constants from actmsg.go
	actionMap, err := loadActionsFromFile(filepath.Join(projectRoot, "api/apc/actmsg.go"))
	if err != nil {
		panic(err)
	}

	walker := &fileWalker{Root: targetRoot}
	processor := &endpointProcessor{
		Walker:         walker,
		ParamSet:       &paramSet,
		ActionMap:      actionMap,
		ModelActions:   make(map[string][]string),
		GlobalPayloads: make(map[string]string),
	}

	if err := processor.run(targetRoot); err != nil {
		panic(err)
	}

	// Collect all model names referenced by endpoints
	modelNames := collectReferencedModels(processor.Endpoints)

	// Build import map from parsed source files
	importMap, err := buildImportMap(projectRoot, targetRoot)
	if err != nil {
		panic(fmt.Errorf("failed to build import map: %v", err))
	}

	// Parse Go structs into OpenAPI schemas
	schemas := parseModelSchemas(projectRoot, modelNames, importMap)

	// Add x-supported-actions to schemas
	for model, actions := range processor.ModelActions {
		if schema, exists := schemas[model]; exists {
			schema.XSupportedActions = actions
		}
	}

	// Build and write the OpenAPI 3.0 spec
	spec := buildSpec(processor.Endpoints, schemas, processor.ActionMap)

	outDir := filepath.Join(projectRoot, docsRelativePath)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		panic(err)
	}

	data, err := yaml.Marshal(spec)
	if err != nil {
		panic(fmt.Errorf("failed to marshal spec: %v", err))
	}

	outPath := filepath.Join(outDir, "openapi.yaml")
	if err := os.WriteFile(outPath, data, 0o644); err != nil {
		panic(err)
	}

	fmt.Printf("Generated OpenAPI 3.0 spec: %s\n", outPath)
	fmt.Printf("  Endpoints: %d\n", len(processor.Endpoints))
	fmt.Printf("  Schemas:   %d\n", len(schemas))
}

// collectReferencedModels gathers all model names referenced from endpoint annotations.
func collectReferencedModels(endpoints []endpoint) []string {
	seen := make(map[string]bool)
	var models []string
	for i := range endpoints {
		for _, action := range endpoints[i].Actions {
			if !seen[action.Model] {
				seen[action.Model] = true
				models = append(models, action.Model)
			}
		}
		for _, model := range endpoints[i].Models {
			if !seen[model] {
				seen[model] = true
				models = append(models, model)
			}
		}
	}
	return models
}

// Extracts action/model pairs from the action=[...] clause
func parseActionClause(annotationLine string) []actionModel {
	actions := make([]actionModel, 0, 4)
	actionStart := strings.Index(annotationLine, actionPrefix)
	if actionStart == -1 {
		return actions
	}

	openIdx := actionStart + len(actionPrefix)
	closeIdx := strings.Index(annotationLine[openIdx:], closeBracket)
	if closeIdx == -1 {
		return actions
	}

	actionBlock := annotationLine[openIdx : openIdx+closeIdx]
	pairs := strings.SplitSeq(actionBlock, pipe)

	for pair := range pairs {
		parts := strings.SplitN(strings.TrimSpace(pair), equals, 2)
		if len(parts) != 2 {
			continue
		}
		actions = append(actions, actionModel{
			Action: strings.TrimSpace(parts[0]),
			Model:  strings.TrimSpace(parts[1]),
		})
	}

	return actions
}

// Extracts payload reference from payload=value clause
func parsePayloadClause(annotationLine string) string {
	payloadStart := strings.Index(annotationLine, payloadRefPrefix)
	if payloadStart == -1 {
		return ""
	}

	valueStart := payloadStart + len(payloadRefPrefix)
	remaining := annotationLine[valueStart:]

	var endIdx int
	if spaceIdx := strings.Index(remaining, " "); spaceIdx != -1 {
		endIdx = spaceIdx
	} else {
		endIdx = len(remaining)
	}

	return strings.TrimSpace(remaining[:endIdx])
}

// Extracts models from the model=[...] clause
func parseModelClause(annotationLine string) []string {
	models := make([]string, 0, 4)
	modelStart := strings.Index(annotationLine, modelPrefix)
	if modelStart == -1 {
		return models
	}

	openIdx := modelStart + len(modelPrefix)
	closeIdx := strings.Index(annotationLine[openIdx:], closeBracket)
	if closeIdx == -1 {
		return models
	}

	modelBlock := annotationLine[openIdx : openIdx+closeIdx]
	modelList := strings.SplitSeq(modelBlock, pipe)

	for model := range modelList {
		model = strings.TrimSpace(model)
		if model != "" {
			models = append(models, model)
		}
	}

	return models
}

func (fp *fileParser) parseEndpoint(lines []string, i int) (endpoint, error) {
	line := strings.TrimSpace(lines[i])

	// check that only one annotation type is used
	hasAction := strings.Contains(line, actionPrefix)
	hasModel := strings.Contains(line, modelPrefix)
	hasPayload := strings.Contains(line, payloadRefPrefix)

	if (hasAction && hasModel) || (hasAction && hasPayload) || (hasModel && hasPayload) {
		return endpoint{}, fmt.Errorf("endpoint annotation can only contain one of 'action=', 'model=', or 'payload=' in the same line: %s", line)
	}

	trimmed := strings.TrimSpace(line[len(commentWithSpace+endpointPrefix):])
	paramStart := strings.Index(trimmed, openBracket)
	paramEnd := strings.Index(trimmed, closeBracket)
	paramString := ""
	if paramStart != -1 && paramEnd != -1 && paramEnd > paramStart {
		paramString = trimmed[paramStart+1 : paramEnd]
		trimmed = strings.TrimSpace(trimmed[:paramStart])
	}
	fields := strings.Fields(trimmed)
	if len(fields) < 2 {
		return endpoint{}, errors.New(malformedEndpointErr)
	}
	method := strings.ToLower(fields[0])
	path := strings.TrimSpace(fields[1])
	var params []param
	if paramString != "" {
		paramList := strings.SplitSeq(paramString, comma)
		for entry := range paramList {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			parts := strings.Split(entry, equals)
			if len(parts) != 2 {
				continue
			}
			fullKey, typ := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
			desc := ""
			value := ""
			if def, ok := (*fp.ParamSet)[fullKey]; ok {
				desc = def.Description
				value = def.Value
			}
			params = append(params, param{
				Name:        fullKey,
				Type:        typ,
				Description: desc,
				Value:       value,
			})
		}
	}
	summaryLines := collectSummaryLines(lines, i)
	summary := strings.Join(summaryLines, " ")

	actions := parseActionClause(line)
	models := parseModelClause(line)
	payloadRef := parsePayloadClause(line)

	tempEndpoint := &endpoint{
		Method:     method,
		Path:       path,
		Params:     params,
		Actions:    actions,
		Models:     models,
		PayloadRef: payloadRef,
	}

	httpExamples := collectAndGenerateHTTPExamples(tempEndpoint, fp.ActionMap, fp.GlobalPayloads)

	// Find the function declaration and extract function name first
	operationID := ""
	for j := i + 1; j < len(lines); j++ {
		next := strings.TrimSpace(lines[j])
		if isFunctionDeclaration(next) {
			functionName := extractFunctionName(next)
			// Only generate unique operation IDs when there are multiple endpoints for the same function
			if hasMultipleEndpoints(lines, i) {
				operationID = generateUniqueOperationID(functionName, method, path)
			} else {
				operationID = functionName
			}
			break
		}
	}

	tag := determineTag(path)

	// Reverse mapping: which actions use which models (plain text, no HTML)
	for _, action := range actions {
		actionName := strings.TrimPrefix(action.Action, apcPrefix)
		if realValue, exists := fp.ActionMap[actionName]; exists {
			if !slices.Contains(fp.ModelActions[action.Model], realValue) {
				fp.ModelActions[action.Model] = append(fp.ModelActions[action.Model], realValue)
			}
		}
	}
	return endpoint{
		Method:       method,
		Path:         path,
		Params:       params,
		Summary:      summary,
		OperationID:  operationID,
		Tag:          tag,
		Actions:      actions,
		Models:       models,
		HTTPExamples: httpExamples,
	}, nil
}

// process scans a source file for +gen:endpoint annotations and collects endpoints.
func (fp *fileParser) process() error {
	content, err := os.ReadFile(fp.Path)
	if err != nil {
		return err
	}
	lines := strings.Split(string(content), newlineChar)

	for i := range lines {
		line := strings.TrimSpace(lines[i])
		if strings.HasPrefix(line, commentWithSpace+endpointPrefix) {
			ep, err := fp.parseEndpoint(lines, i)
			if err != nil {
				fmt.Fprintf(os.Stderr, errorParsingEndpoint, err)
				continue
			}
			*fp.Endpoints = append(*fp.Endpoints, ep)
		}
	}
	return nil
}

func (fw *fileWalker) walk() error {
	fw.Files = nil
	return filepath.WalkDir(fw.Root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if filepath.Ext(path) == goFileExt {
			fw.Files = append(fw.Files, path)
		}
		return nil
	})
}

func (ep *endpointProcessor) run(root string) error {
	ep.Walker.Root = root
	if err := ep.Walker.walk(); err != nil {
		return err
	}

	// Collect all payload annotations globally before processing endpoints
	if err := ep.collectGlobalPayloads(); err != nil {
		return err
	}

	for _, file := range ep.Walker.Files {
		parser := &fileParser{
			Path:           file,
			ParamSet:       ep.ParamSet,
			ActionMap:      ep.ActionMap,
			ModelActions:   ep.ModelActions,
			GlobalPayloads: ep.GlobalPayloads,
			Endpoints:      &ep.Endpoints,
		}
		if err := parser.process(); err != nil {
			return err
		}
	}
	return nil
}

func collectSummaryLines(lines []string, i int) []string {
	var summaryLines []string
	j := i + 1
	for ; j < len(lines); j++ {
		next := strings.TrimSpace(lines[j])
		if isFunctionDeclaration(next) {
			break
		}
		if next == "" {
			continue
		}
		if !strings.HasPrefix(next, commentPrefix) {
			break
		}
		if strings.Contains(next, endpointPrefix) || strings.Contains(next, payloadPrefix) {
			continue
		}
		cleanLine := strings.TrimSpace(strings.TrimPrefix(next, commentPrefix))
		if cleanLine != "" {
			summaryLines = append(summaryLines, cleanLine)
		}
	}
	return summaryLines
}
