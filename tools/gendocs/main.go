// Package main generates swagger annotations from AIStore source code comments.
//
// Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/NVIDIA/aistore/cmn/cos"
)

var (
	targetRoot      string
	annotationsPath string
	swaggerYamlPath string
)

var (
	// Parameter directories to scan for Qparam constants
	paramDirectories = []string{"api/apc", "ais/s3"}
)

const (
	// File paths and extensions
	aisRelativePath         = "ais"
	annotationsRelativePath = "tools/gendocs/gendocs-temp/annotations.go"
	docsRelativePath        = ".docs"
	tempDirRelativePath     = "tools/gendocs/gendocs-temp"
	goFileExt               = ".go"

	// Generation annotation prefixes
	endpointPrefix = "+gen:endpoint"
	payloadPrefix  = "+gen:payload"

	// Comment parsing
	commentPrefix    = "//"
	commentWithSpace = "// "
	funcKeyword      = "func "

	// Swagger annotation keywords
	atSummary = "@Summary"
	atParam   = "@Param"
	atSuccess = "@Success"
	atRouter  = "@Router"

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

	// Temporary files for vendor extensions
	modelActionsFileName = "model-actions.yaml"
	httpExamplesFileName = "http-examples.yaml"

	// YAML keys for vendor extensions
	definitionsKey       = "definitions"
	xSupportedActionsKey = "x-supported-actions"
	XHTTPExamplesKey     = "x-http-examples"

	// Template strings for Swagger generation

	summaryAnnotation = "// @Summary "
	paramTemplate     = "// @Param %s query %s false \"%s\""
	successTemplate   = "// @Success 200 {object} object \"Success\""
	routerTemplate    = "// @Router %s [%s]"
	idTemplate        = "// @ID %s"
	tagsTemplate      = "// @Tags %s"
	bodyParamTemplate = "// @Param request body %s true \"%s\""
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

	// maps operation IDs to their HTTP examples
	operationHTTPExamples map[string][]commandExample

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
		HTTPExamples   operationHTTPExamples
		GlobalPayloads map[string]string
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
		HTTPExamples   operationHTTPExamples
		GlobalPayloads map[string]string
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
		contentType := contentTypeJSON
		if strings.HasPrefix(ep.Path, "/s3/") || ep.Path == "/s3" {
			contentType = contentTypeXML
		}
		cmd += "  " + headerFlag + " '" + contentType + "'" + backslash + newlineChar
		cmd += fmt.Sprintf("  %s '%s'%s", dataFlag, payload, backslash) + newlineChar
	}

	cmd += fmt.Sprintf("  '%s'", url)
	return cmd
}

// Parse payload annotation: apc.ActCopyBck={"action": "copy-bck"}
func parsePayload(line string) (string, string) {
	content := strings.TrimSpace(line[len(commentWithSpace+payloadPrefix):])
	equalsIndex := strings.Index(content, "=")
	if equalsIndex == -1 {
		return "", ""
	}

	action := strings.TrimSpace(content[:equalsIndex])
	payload := strings.TrimSpace(content[equalsIndex+1:])

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

		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
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
	// Set up dynamic paths
	projectRoot, err := getProjectRoot()
	if err != nil {
		panic(fmt.Errorf("failed to determine project root: %v", err))
	}
	targetRoot = filepath.Join(projectRoot, aisRelativePath)
	annotationsPath = filepath.Join(projectRoot, annotationsRelativePath)
	swaggerYamlPath = filepath.Join(projectRoot, docsRelativePath, "swagger.yaml")

	if len(os.Args) > 1 && os.Args[1] == "-cleanup" {
		cleanupAnnotations()
		return
	}

	if len(os.Args) > 1 && os.Args[1] == "-inject-extensions" {
		if err := injectModelExtensions(); err != nil {
			panic(fmt.Errorf("failed to inject model extensions: %v", err))
		}
		if err := injectCodeSamples(); err != nil {
			panic(fmt.Errorf("failed to inject code extensions: %v", err))
		}
		return
	}

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
		HTTPExamples:   make(operationHTTPExamples),
		GlobalPayloads: make(map[string]string),
	}

	if err := processor.run(targetRoot); err != nil {
		panic(err)
	}
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
	pairs := strings.Split(actionBlock, pipe)

	for _, pair := range pairs {
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
	modelList := strings.Split(modelBlock, pipe)

	for _, model := range modelList {
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
		paramList := strings.Split(paramString, comma)
		for _, entry := range paramList {
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

	// Get API class tag for the link
	tag := determineTag(path)

	// Reverse mapping for model actions with API links
	for _, action := range actions {
		actionName := strings.TrimPrefix(action.Action, apcPrefix)
		if realValue, exists := fp.ActionMap[actionName]; exists {
			actionLink := fmt.Sprintf(apiLinkFormat, tag, operationID, realValue)
			if !cos.StringInSlice(actionLink, fp.ModelActions[action.Model]) {
				fp.ModelActions[action.Model] = append(fp.ModelActions[action.Model], actionLink)
			}
		}
	}
	return endpoint{
		Method:       method,
		Path:         path,
		Params:       params,
		Summary:      summary,
		OperationID:  operationID,
		Tag:          determineTag(path),
		Actions:      actions,
		Models:       models,
		HTTPExamples: httpExamples,
	}, nil
}

var dummyFuncCounter int

func writeToAnnotations(swaggerComments []string) error {
	// Read existing content
	content, err := os.ReadFile(annotationsPath)
	if err != nil {
		return err
	}

	// Append the swagger comments with a unique function (needed for swagger to generate docs)
	existingContent := string(content)
	swaggerBlock := strings.Join(swaggerComments, newlineChar)
	dummyFuncCounter++
	dummyFunc := fmt.Sprintf("func dummyHandler%d() {}", dummyFuncCounter) + newlineChar
	newContent := existingContent + swaggerBlock + newlineChar + dummyFunc + newlineChar

	return os.WriteFile(annotationsPath, []byte(newContent), 0o644)
}

func (fp *fileParser) process() error {
	content, err := os.ReadFile(fp.Path)
	if err != nil {
		return err
	}
	lines := strings.Split(string(content), newlineChar)
	out := make([]string, 0, len(lines))

	for i := 0; i < len(lines); {
		line := strings.TrimSpace(lines[i])

		if isSwaggerComment(line) {
			i++
			continue
		}

		if strings.HasPrefix(line, commentWithSpace+endpointPrefix) {
			out = append(out, lines[i])
			ep, err := fp.parseEndpoint(lines, i)
			if err != nil {
				fmt.Fprintf(os.Stderr, errorParsingEndpoint, err)
				i++
				continue
			}
			swaggerComments := generateSwaggerComments(&ep, fp.ActionMap, fp.HTTPExamples)
			writeToAnnotations(swaggerComments)
			i++
			continue
		}

		out = append(out, lines[i])
		i++
	}

	return os.WriteFile(fp.Path, []byte(strings.Join(out, newlineChar)), 0o644)
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

	// Clear annotations file and reset counter
	if err := ep.clearAnnotations(); err != nil {
		return err
	}

	for _, file := range ep.Walker.Files {
		parser := &fileParser{
			Path:           file,
			ParamSet:       ep.ParamSet,
			ActionMap:      ep.ActionMap,
			ModelActions:   ep.ModelActions,
			HTTPExamples:   ep.HTTPExamples,
			GlobalPayloads: ep.GlobalPayloads,
		}
		if err := parser.process(); err != nil {
			return err
		}
	}

	// Save ModelActions to temporary file for extension injection
	if err := ep.saveModelActions(); err != nil {
		return err
	}

	// Save HTTPExamples to temporary file for code samples injection
	return ep.saveHTTPExamples()
}

// Writes the collected ModelActions to a temporary file for inject-extensions
func (ep *endpointProcessor) saveModelActions() error {
	if len(ep.ModelActions) == 0 {
		return nil
	}

	projectRoot, err := getProjectRoot()
	if err != nil {
		return err
	}

	modelActionsPath := filepath.Join(projectRoot, tempDirRelativePath, modelActionsFileName)

	// Ensure the temp directory exists
	if err := os.MkdirAll(filepath.Dir(modelActionsPath), 0o755); err != nil {
		return err
	}

	data, err := yaml.Marshal(ep.ModelActions)
	if err != nil {
		return fmt.Errorf("failed to marshal model actions: %v", err)
	}

	return os.WriteFile(modelActionsPath, data, 0o644)
}

func (*endpointProcessor) clearAnnotations() error {
	if err := os.MkdirAll(filepath.Dir(annotationsPath), 0o755); err != nil {
		return err
	}
	// Write the basic header content
	headerContent := "package main\n"
	dummyFuncCounter = 0
	return os.WriteFile(annotationsPath, []byte(headerContent), 0o644)
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
		if strings.Contains(next, endpointPrefix) {
			continue
		}
		if strings.Contains(next, payloadPrefix) {
			continue
		}
		if isSwaggerComment(next) {
			continue
		}
		cleanLine := strings.TrimSpace(strings.TrimPrefix(next, commentPrefix))
		if cleanLine != "" {
			summaryLines = append(summaryLines, cleanLine)
		}
	}
	return summaryLines
}

func generateSwaggerComments(ep *endpoint, actionMap map[string]string, httpExamples operationHTTPExamples) []string {
	swaggerComments := []string{}
	if ep.Summary != "" {
		swaggerComments = append(swaggerComments, summaryAnnotation+ep.Summary)
	}
	if ep.OperationID != "" {
		swaggerComments = append(swaggerComments, fmt.Sprintf(idTemplate, ep.OperationID))
	}
	swaggerComments = append(swaggerComments, fmt.Sprintf(tagsTemplate, ep.Tag))

	if len(ep.HTTPExamples) > 0 {
		httpExamples[ep.OperationID] = ep.HTTPExamples
	}

	for _, param := range ep.Params {
		paramName := param.Value
		desc := param.Description
		if desc == "" {
			fmt.Fprintf(os.Stderr, warningNoComment, param.Name)
			continue
		}
		desc = strings.ReplaceAll(desc, quote, escapedQuote)
		swaggerComments = append(swaggerComments, fmt.Sprintf(paramTemplate, paramName, param.Type, desc))
	}

	// Add single parameters that references the action(s)
	if len(ep.Actions) > 0 {
		var actionLinks []string
		for _, action := range ep.Actions {
			actionName := strings.TrimPrefix(action.Action, apcPrefix)
			if realValue, exists := actionMap[actionName]; exists {
				actionName = realValue
			}
			link := fmt.Sprintf(actionLinkFormat, action.Model, actionName)
			actionLinks = append(actionLinks, link)
		}

		description := fmt.Sprintf("%s%s", supportedActionsHeader, strings.Join(actionLinks, ", "))

		// Generate one @Param request body for each model
		for _, action := range ep.Actions {
			bodyParamComment := fmt.Sprintf(bodyParamTemplate, action.Model, description)
			swaggerComments = append(swaggerComments, bodyParamComment)
		}
	}

	// Add request body parameters for direct model references
	if len(ep.Models) > 0 {
		var modelLinks []string
		for _, model := range ep.Models {
			modelName := model
			if strings.Contains(model, dot) {
				parts := strings.Split(model, dot)
				modelName = parts[len(parts)-1]
			}
			link := fmt.Sprintf(actionLinkFormat, model, modelName)
			modelLinks = append(modelLinks, link)
		}

		description := "Request model: " + strings.Join(modelLinks, ", ")

		// Generate one @Param request body for each model
		for _, model := range ep.Models {
			bodyParamComment := fmt.Sprintf(bodyParamTemplate, model, description)
			swaggerComments = append(swaggerComments, bodyParamComment)
		}
	}

	swaggerComments = append(swaggerComments,
		successTemplate,
		fmt.Sprintf(routerTemplate, ep.Path, ep.Method))
	return swaggerComments
}

func (ep *endpointProcessor) saveHTTPExamples() error {
	if len(ep.HTTPExamples) == 0 {
		return nil
	}

	projectRoot, err := getProjectRoot()
	if err != nil {
		return err
	}

	httpExamplesPath := filepath.Join(projectRoot, tempDirRelativePath, httpExamplesFileName)
	yamlData, err := yaml.Marshal(ep.HTTPExamples)
	if err != nil {
		return fmt.Errorf("failed to marshal HTTP examples: %v", err)
	}

	return os.WriteFile(httpExamplesPath, yamlData, 0o644)
}
