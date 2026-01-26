// Package main provides utility functions for the AIStore API documentation generator.
//
// Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
package main

import (
	"errors"
	"path/filepath"
	"runtime"
	"strings"
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

// Extracts the function name from a function declaration line
func extractFunctionName(line string) string {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, funcKeyword) {
		return ""
	}

	withoutFunc := strings.TrimSpace(trimmed[len(funcKeyword):])

	// Handle receiver (e.g., "(p *proxy) functionName" or "functionName")
	if strings.HasPrefix(withoutFunc, openParen) {
		closingParen := strings.Index(withoutFunc, closeParen)
		if closingParen == -1 {
			return ""
		}
		withoutFunc = strings.TrimSpace(withoutFunc[closingParen+1:])
	}

	// Extract function name
	openParenIndex := strings.Index(withoutFunc, openParen)
	if openParenIndex == -1 {
		return ""
	}

	return strings.TrimSpace(withoutFunc[:openParenIndex])
}

// Creates a unique operation ID by combining function name with path segments and method
func generateUniqueOperationID(functionName, method, path string) string {
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

	methodPrefix := strings.ToLower(method)

	// Use only the last filtered segment for concise but descriptive names
	if len(filteredSegments) > 0 {
		return functionName + methodPrefix + filteredSegments[len(filteredSegments)-1]
	}
	return functionName + methodPrefix
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

// Checks if a line is a function declaration
func isFunctionDeclaration(line string) bool {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, funcKeyword) {
		return false
	}
	return strings.Contains(trimmed, openParen)
}

// Checks if a line is a swagger annotation comment
func isSwaggerComment(line string) bool {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, commentPrefix) {
		return false
	}
	content := strings.TrimSpace(strings.TrimPrefix(trimmed, commentPrefix))
	for _, prefix := range []string{atSummary, atParam, atSuccess, atRouter} {
		if strings.HasPrefix(content, prefix) {
			return true
		}
	}
	return false
}
