// Package main provides model struct parsing functionality for AIStore API documentation generation.
//
// Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

const (
	// JSON tag parsing
	jsonTag        = "json:"
	jsonTagStart   = `json:"`
	jsonTagExclude = "-"

	// HTML/JSON formatting
	htmlOpenBrace  = "{<br/>"
	htmlCloseBrace = "}"
	htmlLineBreak  = "<br/>"
	htmlQuote      = "&quot;"

	// JSON field formatting
	jsonFieldFormat      = `&quot;%s&quot;: &quot;%s&quot;`
	jsonFieldWithComment = `&quot;%s&quot;: &quot;%s&quot;,   → %s`
	commentSeparator     = ",   →"

	// Go type strings
	goTypeString   = "string"
	goTypeInt      = "int"
	goTypeDuration = "Duration"
	goTypeBool     = "bool"
	goTypeArray    = "[]"
	goTypeMap      = "map"
	goTypeStrKVs   = "StrKVs"

	// JSON type strings
	jsonTypeString  = "string"
	jsonTypeNumber  = "number"
	jsonTypeBoolean = "boolean"
	jsonTypeArray   = "array"
	jsonTypeObject  = "object"

	// Default types
	defaultType = "interface{}"

	// Punctuation
	dot  = "."
	star = "*"
)

// A map of struct names to their AST definitions
type modelSet map[string]*ast.StructType

// Parses all Go files in specified directory and extracts struct definitions
func (ms *modelSet) loadFromDirectory(dirPath string) error {
	fset := token.NewFileSet()
	structs := make(map[string]*ast.StructType)

	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(path) != goFileExt {
			return nil
		}

		file, err := parser.ParseFile(fset, path, nil, parser.AllErrors|parser.ParseComments)
		if err != nil {
			return err
		}

		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}

			for _, spec := range gen.Specs {
				tspec, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}

				stype, ok := tspec.Type.(*ast.StructType)
				if ok {
					structs[tspec.Name.Name] = stype
				}
			}
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to parse directory %s: %v", dirPath, err)
	}

	*ms = structs
	return nil
}

// Checks if a model exists in the modelSet
func (ms *modelSet) hasModel(modelName string) bool {
	_, exists := (*ms)[modelName]
	return exists
}

// Returns the struct definition for a given model name
func (ms *modelSet) getModel(modelName string) (*ast.StructType, bool) {
	structType, exists := (*ms)[modelName]
	return structType, exists
}

// Extracts field information from a struct and formats it as proper JSON
func getStructFieldDetails(modelSet *modelSet, modelName string) string {
	structType, exists := modelSet.getModel(modelName)
	if !exists {
		return ""
	}

	lines := extractFieldsForJSON(structType, modelSet)

	// Remove duplicates (e.g. embedded structs)
	seen := make(map[string]bool)
	var uniqueLines []string
	for _, line := range lines {
		if !seen[line] {
			seen[line] = true
			uniqueLines = append(uniqueLines, line)
		}
	}

	if len(uniqueLines) == 0 {
		return ""
	}

	// HTML formatting
	jsonBlock := htmlOpenBrace
	for i, line := range uniqueLines {
		jsonBlock += "  " + line
		if i < len(uniqueLines)-1 {
			// Only add comma if the line doesn't already have one (for comments)
			if !strings.Contains(line, commentSeparator) {
				jsonBlock += comma
			}
		}
		jsonBlock += htmlLineBreak
	}
	jsonBlock += htmlCloseBrace

	return jsonBlock
}

// Recursively extracts fields formatted as JSON lines
func extractFieldsForJSON(structType *ast.StructType, modelSet *modelSet) []string {
	var lines []string

	for _, field := range structType.Fields.List {
		if len(field.Names) > 0 {
			// Regular field with name
			jsonName, jsonType, comment := extractFieldInfo(field)
			if jsonName != "" {
				if comment != "" {
					// Escape quotes in comments and format with comma before comment
					comment = strings.ReplaceAll(comment, quote, htmlQuote)
					lines = append(lines, fmt.Sprintf(jsonFieldWithComment, jsonName, jsonType, comment))
				} else {
					lines = append(lines, fmt.Sprintf(jsonFieldFormat, jsonName, jsonType))
				}
			}
		} else {
			if ident, ok := field.Type.(*ast.Ident); ok {
				embeddedStruct, exists := modelSet.getModel(ident.Name)
				if exists {
					embeddedFields := extractFieldsForJSON(embeddedStruct, modelSet)
					lines = append(lines, embeddedFields...)
				}
			}
		}
	}

	return lines
}

// Extracts JSON name, type, and comment from a field
func extractFieldInfo(field *ast.Field) (string, string, string) {
	var jsonName string

	// Extract JSON tag if present
	if field.Tag != nil {
		tag := field.Tag.Value
		if strings.Contains(tag, jsonTag) {
			start := strings.Index(tag, jsonTagStart) + 6
			if start > 5 {
				end := strings.Index(tag[start:], quote)
				if end > 0 {
					jsonTag := tag[start : start+end]
					if jsonTag != jsonTagExclude {
						jsonName = strings.Split(jsonTag, comma)[0]
					}
				}
			}
		}
	}

	// Skip fields without JSON tags or with "-"
	if jsonName == "" || jsonName == jsonTagExclude {
		return "", "", ""
	}

	// Convert Go types to JSON types
	fieldType := formatFieldType(field.Type)
	jsonType := goTypeToJsonType(fieldType)

	// Extract inline comment
	var comment string
	if field.Comment != nil {
		comment = strings.TrimSpace(field.Comment.Text())
		comment = strings.TrimPrefix(comment, commentPrefix)
		comment = strings.TrimSpace(comment)
	}

	return jsonName, jsonType, comment
}

// Converts Go types to JSON type representations
func goTypeToJsonType(goType string) string {
	switch {
	case strings.Contains(goType, goTypeString):
		return jsonTypeString
	case strings.Contains(goType, goTypeInt) || strings.Contains(goType, goTypeDuration):
		return jsonTypeNumber
	case strings.Contains(goType, goTypeBool):
		return jsonTypeBoolean
	case strings.Contains(goType, goTypeArray):
		return jsonTypeArray
	case strings.Contains(goType, goTypeMap) || strings.Contains(goType, goTypeStrKVs):
		return jsonTypeObject
	default:
		return jsonTypeObject
	}
}

// Converts an ast.Expr to a readable type string
func formatFieldType(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.SelectorExpr:
		return formatFieldType(t.X) + dot + t.Sel.Name
	case *ast.StarExpr:
		return star + formatFieldType(t.X)
	case *ast.ArrayType:
		return goTypeArray + formatFieldType(t.Elt)
	case *ast.MapType:
		return goTypeMap + openBracket + formatFieldType(t.Key) + closeBracket + formatFieldType(t.Value)
	default:
		return defaultType
	}
}
