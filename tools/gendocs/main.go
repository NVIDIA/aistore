// Package main generates swagger annotations from AIStore source code comments.
//
// Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

var (
	apcPath         string
	targetRoot      string
	annotationsPath string
)

const (
	// Relative paths from project root
	apcRelativePath         = "api/apc/query.go"
	aisRelativePath         = "ais"
	annotationsRelativePath = "tools/gendocs/annotations.go"

	goFileExt   = ".go"
	newlineChar = "\n"

	endpointPrefix    = "+gen:endpoint"
	summaryAnnotation = "// @Summary "
	commentPrefix     = "//"
	commentWithSpace  = "// "

	atSummary = "@Summary"
	atParam   = "@Param"
	atSuccess = "@Success"
	atRouter  = "@Router"

	openBracket  = "["
	closeBracket = "]"
	comma        = ","
	equals       = "="
	pipe         = "|"

	actionPrefix = "action=" + openBracket
	apcPrefix    = "apc."

	errorParsingEndpoint = "Error parsing endpoint: %v\n"
	warningNoComment     = "Warning: no comment for %s\n"
	cleanupMessage       = "Cleaning up %d files in %s\n"
	malformedEndpointErr = "malformed endpoint line"

	paramTemplate   = "// @Param %s query %s false \"%s\""
	successTemplate = "// @Success 200 {object} object \"Success\""
	routerTemplate  = "// @Router %s [%s]"
	idTemplate      = "// @ID %s"
	tagsTemplate    = "// @Tags %s"

	bodyParamTemplate      = "// @Param body body %s true \"%s\""
	supportedActionsHeader = "<h3>Supported Actions</h3><br/>"
	actionLabelFormat      = "**Action:** %s"
	modelExampleFormat     = "**Model Example Value:**<br/>%s"
	modelLabelFormat       = "**Model:** %s"
	fieldDetailsNA         = "*(Field details not available)*"
	actionSeparator        = "<br/><br/>---<br/><br/>"
	lineBreak              = " <br/> "

	warningModelNotFound = "Warning: model %s not found for action %s\n"

	quote        = `"`
	escapedQuote = `\"`

	funcKeyword = "func "
	openParen   = "("
)

type (
	actionModel struct {
		Action string
		Model  string
	}

	endpoint struct {
		Method      string
		Path        string
		Params      []param
		Summary     string
		OperationID string
		Tag         string
		Actions     []actionModel
	}

	fileParser struct {
		Path     string
		ParamSet *paramSet
		ModelSet *modelSet
	}

	fileWalker struct {
		Root  string
		Files []string
	}

	endpointProcessor struct {
		Walker   *fileWalker
		ParamSet *paramSet
		ModelSet *modelSet
	}
)

// Returns the absolute path to the project root directory
func getProjectRoot() (string, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("unable to get caller info")
	}
	// from tools/gendocs/main.go, project root is 2 levels up
	root := filepath.Join(filepath.Dir(filename), "../..")
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	return absRoot, nil
}

func main() {
	// Set up dynamic paths
	projectRoot, err := getProjectRoot()
	if err != nil {
		panic(fmt.Errorf("failed to determine project root: %v", err))
	}
	apcPath = filepath.Join(projectRoot, apcRelativePath)
	targetRoot = filepath.Join(projectRoot, aisRelativePath)
	annotationsPath = filepath.Join(projectRoot, annotationsRelativePath)

	if len(os.Args) > 1 && os.Args[1] == "-cleanup" {
		cleanupAnnotations()
		return
	}

	var paramSet paramSet
	if err := paramSet.loadFromFile(apcPath); err != nil {
		panic(err)
	}

	var modelSet modelSet
	apcDir := filepath.Dir(apcPath)
	if err := modelSet.loadFromDirectory(apcDir); err != nil {
		panic(fmt.Errorf("failed to load models: %v", err))
	}

	walker := &fileWalker{Root: targetRoot}
	processor := &endpointProcessor{
		Walker:   walker,
		ParamSet: &paramSet,
		ModelSet: &modelSet,
	}

	if err := processor.run(targetRoot); err != nil {
		panic(err)
	}
}

// Extracts the function name from a function declaration line
func extractFunctionName(line string) string {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, funcKeyword) {
		return ""
	}

	// Remove "func " prefix
	withoutFunc := strings.TrimSpace(trimmed[len(funcKeyword):])

	// Handle receiver (e.g., "(p *proxy) functionName" or "functionName")
	if strings.HasPrefix(withoutFunc, "(") {
		// Find the closing parenthesis of the receiver
		closingParen := strings.Index(withoutFunc, ")")
		if closingParen == -1 {
			return ""
		}
		withoutFunc = strings.TrimSpace(withoutFunc[closingParen+1:])
	}

	// Extract function name
	openParenIndex := strings.Index(withoutFunc, "(")
	if openParenIndex == -1 {
		return ""
	}

	return strings.TrimSpace(withoutFunc[:openParenIndex])
}

// Creates a unique operation ID by combining function name with path segments
func generateUniqueOperationID(functionName, path string) string {
	cleanPath := strings.Trim(path, "/")
	segments := strings.Split(cleanPath, "/")

	var filteredSegments []string
	for _, segment := range segments {
		// Skip version segments like "v1"
		if strings.HasPrefix(segment, "v") && len(segment) <= 3 {
			continue
		}
		// Skip parameter placeholders like "{bucket-name}", "{etl-name}"
		if strings.HasPrefix(segment, "{") && strings.HasSuffix(segment, "}") {
			continue
		}
		// Convert to lowercase and remove hyphens
		cleaned := strings.ToLower(strings.ReplaceAll(segment, "-", ""))
		if cleaned != "" {
			filteredSegments = append(filteredSegments, cleaned)
		}
	}

	// Use only the last filtered segment for concise but descriptive names
	if len(filteredSegments) > 0 {
		return functionName + filteredSegments[len(filteredSegments)-1]
	}
	return functionName
}

// Checks if there are multiple +gen:endpoint annotations for the same function to produce unique names
func hasMultipleEndpoints(lines []string, currentIndex int) bool {
	var functionLine string
	for j := currentIndex + 1; j < len(lines); j++ {
		next := strings.TrimSpace(lines[j])
		if isFunctionDeclaration(next) {
			functionLine = next
			break
		}
	}

	if functionLine == "" {
		return false
	}

	count := 0
	for k := currentIndex; k >= 0; k-- {
		line := strings.TrimSpace(lines[k])
		if strings.HasPrefix(line, commentWithSpace+endpointPrefix) {
			count++
			if count > 1 {
				return true
			}
		}
		if k != currentIndex && isFunctionDeclaration(line) {
			break
		}
	}

	return false
}

// Determines API Class based on the endpoint path by parsing the first segment
func determineTag(path string) string {
	cleanPath := strings.Trim(path, "/")
	segments := strings.Split(cleanPath, "/")

	for _, segment := range segments {
		// Skip version segments like "v1"
		if strings.HasPrefix(segment, "v") && len(segment) <= 3 {
			continue
		}
		// Skip parameter placeholders like "{bucket-name}", "{etl-name}"
		if strings.HasPrefix(segment, "{") && strings.HasSuffix(segment, "}") {
			continue
		}
		// Use the first meaningful segment, capitalize first letter
		if segment != "" {
			// Remove hyphens and capitalize first letter
			cleaned := strings.ReplaceAll(segment, "-", "")
			if cleaned != "" {
				return strings.ToUpper(cleaned[:1]) + strings.ToLower(cleaned[1:])
			}
		}
	}
	return "Default"
}

// Extracts action/model pairs from the action=[...] clause
func parseActionClause(annotationLine string) []actionModel {
	var actions []actionModel
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

func (fp *fileParser) parseEndpoint(lines []string, i int) (endpoint, error) {
	line := strings.TrimSpace(lines[i])
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
			if def, ok := (*fp.ParamSet)[fullKey]; ok {
				desc = def.Description
			}
			params = append(params, param{
				Name:        fullKey,
				Type:        typ,
				Description: desc,
			})
		}
	}
	summaryLines := collectSummaryLines(lines, i)
	summary := strings.Join(summaryLines, " ")

	actions := parseActionClause(line)

	// Find the function declaration and extract function name
	operationID := ""
	for j := i + 1; j < len(lines); j++ {
		next := strings.TrimSpace(lines[j])
		if isFunctionDeclaration(next) {
			functionName := extractFunctionName(next)
			// Only generate unique operation IDs when there are multiple endpoints for the same function
			if hasMultipleEndpoints(lines, i) {
				operationID = generateUniqueOperationID(functionName, path)
			} else {
				operationID = functionName
			}
			break
		}
	}

	return endpoint{
		Method:      method,
		Path:        path,
		Params:      params,
		Summary:     summary,
		OperationID: operationID,
		Tag:         determineTag(path),
		Actions:     actions,
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
			swaggerComments := generateSwaggerComments(&ep, fp.ModelSet)
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

	// Clear annotations file and reset counter
	if err := ep.clearAnnotations(); err != nil {
		return err
	}

	for _, file := range ep.Walker.Files {
		parser := &fileParser{
			Path:     file,
			ParamSet: ep.ParamSet,
			ModelSet: ep.ModelSet,
		}
		if err := parser.process(); err != nil {
			return err
		}
	}
	return nil
}

func (*endpointProcessor) clearAnnotations() error {
	// Write the basic header content
	headerContent := `package main

// @title AIStore API
// @version 3.24
// @description AIStore: scalable storage for AI applications
// @contact.name AIStore
// @contact.url https://aiatscale.org
// @contact.email aistore@nvidia.com
// @license.name MIT
// @license.url https://github.com/NVIDIA/aistore/blob/main/LICENSE
// @host localhost:8080
// @BasePath /v1
// Generated endpoint documentation

`
	dummyFuncCounter = 0
	return os.WriteFile(annotationsPath, []byte(headerContent), 0o644)
}

func isSwaggerComment(line string) bool {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, commentPrefix) {
		return false
	}
	content := strings.TrimSpace(strings.TrimPrefix(trimmed, commentPrefix))
	swaggerPrefixes := []string{atSummary, atParam, atSuccess, atRouter}
	for _, prefix := range swaggerPrefixes {
		if strings.HasPrefix(content, prefix) {
			return true
		}
	}
	return false
}

func isFunctionDeclaration(line string) bool {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, funcKeyword) {
		return false
	}
	return strings.Contains(trimmed, openParen)
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

func generateSwaggerComments(ep *endpoint, modelSet *modelSet) []string {
	swaggerComments := []string{}
	if ep.Summary != "" {
		swaggerComments = append(swaggerComments, summaryAnnotation+ep.Summary)
	}
	if ep.OperationID != "" {
		swaggerComments = append(swaggerComments, fmt.Sprintf(idTemplate, ep.OperationID))
	}
	swaggerComments = append(swaggerComments, fmt.Sprintf(tagsTemplate, ep.Tag))

	for _, param := range ep.Params {
		short := strings.TrimPrefix(param.Name, apcQparamPrefix)
		short = strings.ToLower(short)
		desc := param.Description
		if desc == "" {
			fmt.Fprintf(os.Stderr, warningNoComment, param.Name)
			continue
		}
		desc = strings.ReplaceAll(desc, quote, escapedQuote)
		swaggerComments = append(swaggerComments, fmt.Sprintf(paramTemplate, short, param.Type, desc))
	}

	// Add single body parameter documenting all actions
	if len(ep.Actions) > 0 {
		primaryModel := ep.Actions[0].Model

		var actionDescriptions []string
		for _, action := range ep.Actions {
			modelName := strings.TrimPrefix(action.Model, apcPrefix)
			if !modelSet.hasModel(modelName) {
				fmt.Fprintf(os.Stderr, warningModelNotFound, action.Model, action.Action)
				continue
			}

			actionName := strings.TrimPrefix(action.Action, apcPrefix)
			structDetails := getStructFieldDetails(modelSet, modelName)
			actionParts := []string{
				fmt.Sprintf(actionLabelFormat, actionName),
			}

			if structDetails != "" {
				actionParts = append(actionParts, fmt.Sprintf(modelExampleFormat, structDetails))
			} else {
				actionParts = append(actionParts, fmt.Sprintf(modelLabelFormat, action.Model))
				actionParts = append(actionParts, fieldDetailsNA)
			}

			actionDescriptions = append(actionDescriptions, strings.Join(actionParts, lineBreak))
		}

		fullDesc := supportedActionsHeader + strings.Join(actionDescriptions, actionSeparator)

		fullDesc = strings.ReplaceAll(fullDesc, `"`, `\"`)

		bodyParamComment := fmt.Sprintf(bodyParamTemplate, primaryModel, fullDesc)
		swaggerComments = append(swaggerComments, bodyParamComment)
	}

	swaggerComments = append(swaggerComments,
		successTemplate,
		fmt.Sprintf(routerTemplate, ep.Path, ep.Method))
	return swaggerComments
}

func cleanupAnnotations() error {
	return os.Remove(annotationsPath)
}
