// Package main provides parameter parsing functionality for AIStore API documentation generation.
//
// Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
)

const (
	qparamPrefix    = "Qparam"
	apcQparamPrefix = "apc.Qparam"
)

type param struct {
	Name        string
	Type        string
	Description string
	Value       string
}

// paramSet is a map of parameter names to their definitions
type paramSet map[string]param

// populates the paramSet from a Go source file
func (ps *paramSet) loadFromFile(filePath string) error {
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, filePath, nil, parser.ParseComments)
	if err != nil {
		return fmt.Errorf("failed to parse file %s: %v", filePath, err)
	}
	*ps = extractQparamConstants(node)
	return nil
}

// loads Action constants from actmsg.go and returns a map of constant names to their string values
func loadActionsFromFile(filePath string) (map[string]string, error) {
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, filePath, nil, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("failed to parse file %s: %v", filePath, err)
	}
	return extractActionConstants(node), nil
}

// extracts Action constants from the AST and reads their actual values
func extractActionConstants(node *ast.File) map[string]string {
	actions := make(map[string]string)
	for _, decl := range node.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			valSpec := spec.(*ast.ValueSpec)
			for i, name := range valSpec.Names {
				if !strings.HasPrefix(name.Name, "Act") {
					continue
				}

				actualValue := ""
				if valSpec.Values != nil && i < len(valSpec.Values) {
					if lit, ok := valSpec.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
						actualValue = strings.Trim(lit.Value, `"`)
					}
				}

				if actualValue != "" {
					actions[name.Name] = actualValue
				}
			}
		}
	}
	return actions
}

// extracts Qparam constants from the AST
func extractQparamConstants(node *ast.File) map[string]param {
	params := make(map[string]param)
	for _, decl := range node.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			valSpec := spec.(*ast.ValueSpec)
			for i, name := range valSpec.Names {
				if !strings.HasPrefix(name.Name, qparamPrefix) {
					continue
				}
				actualValue := ""
				if valSpec.Values != nil && i < len(valSpec.Values) {
					if lit, ok := valSpec.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
						actualValue = strings.Trim(lit.Value, `"`)
					}
				}

				desc := extractDescription(valSpec)
				paramName := apcQparamPrefix + name.Name[6:]
				params[paramName] = param{
					Name:        paramName,
					Description: desc,
					Value:       actualValue,
				}
			}
		}
	}
	return params
}

// gets the description from comments
func extractDescription(valSpec *ast.ValueSpec) string {
	var desc string
	if valSpec.Comment != nil {
		desc = strings.TrimPrefix(valSpec.Comment.List[0].Text, commentPrefix)
	}
	if desc == "" && valSpec.Doc != nil {
		var lines []string
		for _, c := range valSpec.Doc.List {
			text := strings.TrimPrefix(c.Text, commentPrefix)
			text = strings.TrimSpace(text)
			if text != "" {
				lines = append(lines, text)
			}
		}
		desc = strings.Join(lines, " ")
	}
	return strings.TrimSpace(desc)
}
