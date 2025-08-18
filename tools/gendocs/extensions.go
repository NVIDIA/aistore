// Package main provides vendor extension injection and cleanup functionality for AIStore API documentation.
//
// Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Removes temporary files and directories
func cleanupAnnotations() error {
	projectRoot, err := getProjectRoot()
	if err != nil {
		return err
	}
	tempDirPath := filepath.Join(projectRoot, tempDirRelativePath)
	return os.RemoveAll(tempDirPath)
}

// Reads the swagger.yaml file, simplifies model names, and injects model vendor extensions
func injectModelExtensions() error {
	projectRoot, err := getProjectRoot()
	if err != nil {
		return err
	}
	modelActionsPath := filepath.Join(projectRoot, tempDirRelativePath, modelActionsFileName)
	modelActionsData, err := os.ReadFile(modelActionsPath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("No model actions found, skipping extension injection")
			return nil
		}
		return fmt.Errorf("failed to read model actions: %v", err)
	}

	var modelActions map[string][]string
	if err := yaml.Unmarshal(modelActionsData, &modelActions); err != nil {
		return fmt.Errorf("failed to parse model actions: %v", err)
	}

	// Read swagger.yaml
	yamlData, err := os.ReadFile(swaggerYamlPath)
	if err != nil {
		return fmt.Errorf("failed to read swagger.yaml: %v", err)
	}

	var spec map[string]any
	if err := yaml.Unmarshal(yamlData, &spec); err != nil {
		return fmt.Errorf("failed to parse swagger.yaml: %v", err)
	}

	// Navigate to definitions section
	definitions, ok := spec[definitionsKey].(map[string]any)
	if !ok {
		fmt.Println("No definitions section found in swagger.yaml")
		return nil
	}

	// Simplify model names
	simpleName := make(map[string]any)
	for qualifiedName, definition := range definitions {
		parts := strings.Split(qualifiedName, dot)
		baseName := parts[len(parts)-1]
		simpleName[baseName] = definition
	}

	spec[definitionsKey] = simpleName

	// Inject x-supported-actions for each model using the simplified names
	injected := 0
	for model, actions := range modelActions {
		if len(actions) == 0 {
			continue
		}

		// Convert model name [apc.ActMsg] to [ActMsg]
		parts := strings.Split(model, dot)
		baseName := parts[len(parts)-1]

		if modelDef, exists := simpleName[baseName]; exists {
			if modelDefMap, ok := modelDef.(map[string]any); ok {
				modelDefMap[xSupportedActionsKey] = actions
				injected++
				fmt.Printf("  Injected extensions for model: %s\n", baseName)
			}
		}
	}

	// Write back to file
	updatedYaml, err := yaml.Marshal(spec)
	if err != nil {
		return fmt.Errorf("failed to marshal updated yaml: %v", err)
	}

	if err := os.WriteFile(swaggerYamlPath, updatedYaml, 0o644); err != nil {
		return fmt.Errorf("failed to write updated swagger.yaml: %v", err)
	}

	fmt.Printf("Successfully injected extensions for %d models\n", injected)

	return nil
}

// Reads the swagger.yaml file and injects curl vendor extensions
func injectCodeSamples() error {
	projectRoot, err := getProjectRoot()
	if err != nil {
		return err
	}

	httpExamplesPath := filepath.Join(projectRoot, tempDirRelativePath, httpExamplesFileName)
	httpExamplesData, err := os.ReadFile(httpExamplesPath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("No HTTP examples found, skipping code samples injection")
			return nil
		}
		return fmt.Errorf("failed to read HTTP examples: %v", err)
	}

	var endpointExamples operationHTTPExamples
	if err := yaml.Unmarshal(httpExamplesData, &endpointExamples); err != nil {
		return fmt.Errorf("failed to parse HTTP examples: %v", err)
	}

	yamlData, err := os.ReadFile(swaggerYamlPath)
	if err != nil {
		return fmt.Errorf("failed to read swagger.yaml: %v", err)
	}

	var spec map[string]any
	if err := yaml.Unmarshal(yamlData, &spec); err != nil {
		return fmt.Errorf("failed to parse swagger.yaml: %v", err)
	}

	paths, ok := spec["paths"].(map[string]any)
	if !ok {
		fmt.Println("No paths section found in swagger.yaml")
		return nil
	}

	injected := 0
	for operationID, examples := range endpointExamples {
		if len(examples) == 0 {
			continue
		}

		// Convert curlExample to x-codeSamples format
		var codeSamples []map[string]any
		for _, example := range examples {
			codeSamples = append(codeSamples, map[string]any{
				"lang":   bashLang,
				"label":  example.Label,
				"source": example.Command,
			})
		}

		if injectCodeSamplesInPaths(paths, operationID, codeSamples) {
			injected++
			fmt.Printf("  Injected code samples for operation: %s\n", operationID)
		}
	}

	updatedYaml, err := yaml.Marshal(spec)
	if err != nil {
		return fmt.Errorf("failed to marshal updated yaml: %v", err)
	}

	if err := os.WriteFile(swaggerYamlPath, updatedYaml, 0o644); err != nil {
		return fmt.Errorf("failed to write updated swagger.yaml: %v", err)
	}

	fmt.Printf("Successfully injected code samples for %d operations\n", injected)
	return nil
}

// Injects code samples into the swagger paths section
func injectCodeSamplesInPaths(paths map[string]any, operationID string, codeSamples []map[string]any) bool {
	for _, pathItem := range paths {
		if pathMap, ok := pathItem.(map[string]any); ok {
			for _, operation := range pathMap {
				if opMap, ok := operation.(map[string]any); ok {
					if opID, exists := opMap["operationId"]; exists && opID == operationID {
						opMap[XHTTPExamplesKey] = codeSamples
						return true
					}
				}
			}
		}
	}
	return false
}
