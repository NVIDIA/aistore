// Package main provides Go struct to OpenAPI schema extraction via go/ast.
//
// Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
package main

import (
	"bufio"
	"errors"
	"fmt"
	"go/ast"
	"go/doc/comment"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
)

// schemaProperty represents a single field in an OpenAPI schema.
type schemaProperty struct {
	Type                 string                    `yaml:"type,omitempty"`
	Format               string                    `yaml:"format,omitempty"`
	Description          string                    `yaml:"description,omitempty"`
	Items                *schemaProperty           `yaml:"items,omitempty"`
	AdditionalProperties *schemaProperty           `yaml:"additionalProperties,omitempty"`
	Ref                  string                    `yaml:"$ref,omitempty"`
	OneOf                []schemaProperty          `yaml:"oneOf,omitempty"`
	Properties           map[string]schemaProperty `yaml:"properties,omitempty"`
}

// schemaObject represents an OpenAPI schema definition.
type schemaObject struct {
	Type              string                    `yaml:"type"`
	Description       string                    `yaml:"description,omitempty"`
	Required          []string                  `yaml:"required,omitempty"`
	Properties        map[string]schemaProperty `yaml:"properties,omitempty"`
	XSupportedActions []string                  `yaml:"x-supported-actions,omitempty"`
}

// schemaParser holds state for parsing Go structs into schemas.
type schemaParser struct {
	projectRoot string
	importMap   map[string]string // package short name → relative dir
	schemas     map[string]*schemaObject
	pkgCache    map[string]map[string]*ast.File // dir → filename → parsed file
}

// parseModelSchemas parses all referenced model names and returns their OpenAPI schemas.
func parseModelSchemas(projectRoot string, modelNames []string, importMap map[string]string) map[string]*schemaObject {
	sp := &schemaParser{
		projectRoot: projectRoot,
		importMap:   importMap,
		schemas:     make(map[string]*schemaObject),
		pkgCache:    make(map[string]map[string]*ast.File),
	}
	for _, name := range modelNames {
		if err := sp.resolveSchema(name); err != nil {
			fmt.Printf("  Warning: could not resolve schema for %s: %v\n", name, err)
		}
	}
	return sp.schemas
}

// buildImportMap parses Go source files in sourceDir, collects their imports,
// and returns a map from package short name to directory path relative to projectRoot.
func buildImportMap(projectRoot, sourceDir string) (map[string]string, error) {
	modulePath, err := readModulePath(filepath.Join(projectRoot, "go.mod"))
	if err != nil {
		return nil, err
	}

	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, sourceDir, nil, parser.ImportsOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s: %v", sourceDir, err)
	}

	importMap := make(map[string]string)
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, imp := range file.Imports {
				path := strings.Trim(imp.Path.Value, `"`)
				if !strings.HasPrefix(path, modulePath+"/") {
					continue
				}
				relDir := strings.TrimPrefix(path, modulePath+"/")
				var shortName string
				if imp.Name != nil {
					shortName = imp.Name.Name
				} else {
					parts := strings.Split(path, "/")
					shortName = parts[len(parts)-1]
				}
				if existing, ok := importMap[shortName]; ok && existing != relDir {
					return nil, fmt.Errorf("import conflict: %q maps to both %q and %q", shortName, existing, relDir)
				}
				importMap[shortName] = relDir
			}
		}
	}
	return importMap, nil
}

func readModulePath(goModPath string) (string, error) {
	f, err := os.Open(goModPath)
	if err != nil {
		return "", fmt.Errorf("failed to open go.mod: %v", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "module ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "module ")), nil
		}
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("error reading go.mod: %v", err)
	}
	return "", errors.New("module directive not found in go.mod")
}

// resolveSchema parses a single model by qualified name (e.g. "apc.TCBMsg").
func (sp *schemaParser) resolveSchema(qualifiedName string) error {
	if _, exists := sp.schemas[qualifiedName]; exists {
		return nil
	}

	pkg, typeName := splitQualifiedName(qualifiedName)
	if pkg == "" {
		return fmt.Errorf("unqualified type: %s", qualifiedName)
	}

	dir, ok := sp.importMap[pkg]
	if !ok {
		return fmt.Errorf("unknown package %q (not imported by source files)", pkg)
	}

	ts, gen, err := sp.findStruct(dir, typeName)
	if err != nil {
		return err
	}

	schema := &schemaObject{
		Type:        "object",
		Description: structDoc(ts, gen),
		Properties:  make(map[string]schemaProperty),
	}
	sp.schemas[qualifiedName] = schema // register early to handle cycles

	sp.extractFields(schema, ts.Type.(*ast.StructType), pkg)
	return nil
}

// findStruct locates a struct type declaration by name. The enclosing GenDecl
// is returned so callers can recover the struct-level godoc, which may live on
// either the TypeSpec or the GenDecl.
func (sp *schemaParser) findStruct(dir, typeName string) (*ast.TypeSpec, *ast.GenDecl, error) {
	files, err := sp.parseDir(dir)
	if err != nil {
		return nil, nil, err
	}

	for _, file := range files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, spec := range gen.Specs {
				ts, ok := spec.(*ast.TypeSpec)
				if !ok || ts.Name.Name != typeName {
					continue
				}
				if _, ok := ts.Type.(*ast.StructType); !ok {
					return nil, nil, fmt.Errorf("%s is not a struct", typeName)
				}
				return ts, gen, nil
			}
		}
	}
	return nil, nil, fmt.Errorf("struct %s not found in %s", typeName, dir)
}

// structDoc returns the struct-level godoc. A group-level doc is only used
// when the group declares a single spec; otherwise it documents the group,
// not this type.
func structDoc(ts *ast.TypeSpec, gen *ast.GenDecl) string {
	if ts.Doc != nil {
		return commentGroupText(ts.Doc)
	}
	if gen.Doc != nil && len(gen.Specs) == 1 {
		return commentGroupText(gen.Doc)
	}
	return ""
}

// parseDir parses all Go files in a directory, caching the results.
func (sp *schemaParser) parseDir(dir string) (map[string]*ast.File, error) {
	if cached, ok := sp.pkgCache[dir]; ok {
		return cached, nil
	}

	dirPath := filepath.Join(sp.projectRoot, dir)
	fset := token.NewFileSet()

	pkgs, err := parser.ParseDir(fset, dirPath, nil, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s: %v", dir, err)
	}

	files := make(map[string]*ast.File)
	for _, pkg := range pkgs {
		for name, file := range pkg.Files {
			files[name] = file
		}
	}

	sp.pkgCache[dir] = files
	return files, nil
}

// extractFields walks struct fields and populates schema properties and required list.
// A field is required if it is not a pointer and its json tag does not contain omitempty.
func (sp *schemaParser) extractFields(schema *schemaObject, st *ast.StructType, currentPkg string) {
	for _, field := range st.Fields.List {
		if len(field.Names) == 0 {
			sp.handleEmbedded(schema, field.Type, currentPkg)
			continue
		}

		jsonName, omitempty := getJSONTag(field)
		if jsonName == "" || jsonName == "-" {
			continue
		}

		_, isPointer := field.Type.(*ast.StarExpr)
		if !isPointer && !omitempty {
			schema.Required = append(schema.Required, jsonName)
		}

		prop := sp.goTypeToSchema(field.Type, currentPkg)
		if d := fieldDescription(field); d != "" {
			prop.Description = d
		}
		schema.Properties[jsonName] = prop
	}
}

// handleEmbedded resolves an embedded struct and merges its fields.
func (sp *schemaParser) handleEmbedded(schema *schemaObject, expr ast.Expr, currentPkg string) {
	typeName := exprToString(expr)
	qualified := typeName
	if !strings.Contains(typeName, dot) {
		qualified = currentPkg + dot + typeName
	}

	if err := sp.resolveSchema(qualified); err != nil {
		return
	}

	if embedded, exists := sp.schemas[qualified]; exists {
		for name, prop := range embedded.Properties {
			if _, exists := schema.Properties[name]; !exists {
				schema.Properties[name] = prop
				if slices.Contains(embedded.Required, name) && !slices.Contains(schema.Required, name) {
					schema.Required = append(schema.Required, name)
				}
			}
		}
	}
}

// goTypeToSchema converts a Go AST type expression to an OpenAPI schema property.
func (sp *schemaParser) goTypeToSchema(expr ast.Expr, currentPkg string) schemaProperty {
	switch t := expr.(type) {
	case *ast.Ident:
		if t.Name == "any" {
			return schemaProperty{Type: "object"}
		}
		if prop := identToSchema(t.Name); prop.Type != "object" {
			return prop // built-in type
		}
		// Same-package struct reference — resolve as $ref
		qualified := currentPkg + dot + t.Name
		if err := sp.resolveSchema(qualified); err == nil {
			return schemaProperty{Ref: "#/components/schemas/" + qualified}
		}
		return schemaProperty{Type: "object"}
	case *ast.SelectorExpr:
		return sp.externalTypeToSchema(exprToString(t.X), t.Sel.Name)
	case *ast.ArrayType:
		if t.Len == nil {
			if ident, ok := t.Elt.(*ast.Ident); ok && ident.Name == "byte" {
				return schemaProperty{Type: "string", Format: "binary"}
			}
			items := sp.goTypeToSchema(t.Elt, currentPkg)
			return schemaProperty{Type: "array", Items: &items}
		}
		items := sp.goTypeToSchema(t.Elt, currentPkg)
		return schemaProperty{Type: "array", Items: &items}
	case *ast.MapType:
		valProp := sp.goTypeToSchema(t.Value, currentPkg)
		return schemaProperty{Type: "object", AdditionalProperties: &valProp}
	case *ast.StarExpr:
		return sp.goTypeToSchema(t.X, currentPkg)
	case *ast.InterfaceType:
		return schemaProperty{Type: "object"}
	default:
		return schemaProperty{Type: "object"}
	}
}

// identToSchema maps Go built-in type names to OpenAPI types.
func identToSchema(name string) schemaProperty {
	switch name {
	case "string":
		return schemaProperty{Type: "string"}
	case "bool":
		return schemaProperty{Type: "boolean"}
	case "int", "int8", "int16", "int32":
		return schemaProperty{Type: "integer", Format: "int32"}
	case "int64":
		return schemaProperty{Type: "integer", Format: "int64"}
	case "uint", "uint8", "uint16", "uint32":
		return schemaProperty{Type: "integer", Format: "int32"}
	case "uint64":
		return schemaProperty{Type: "integer", Format: "int64"}
	case "float32":
		return schemaProperty{Type: "number", Format: "float"}
	case "float64":
		return schemaProperty{Type: "number", Format: "double"}
	default:
		return schemaProperty{Type: "object"}
	}
}

// externalTypeToSchema handles qualified types like cos.Duration, cos.StrKVs.
func (sp *schemaParser) externalTypeToSchema(pkg, typeName string) schemaProperty {
	switch pkg + dot + typeName {
	case "cos.Duration":
		return schemaProperty{Type: "string"}
	case "cos.StrKVs":
		return schemaProperty{Type: "object", AdditionalProperties: &schemaProperty{Type: "string"}}
	case "cos.SizeIEC":
		return schemaProperty{Type: "string"}
	case "cos.JSONRawMsgs":
		return schemaProperty{Type: "object", AdditionalProperties: &schemaProperty{Type: "object"}}
	}

	if _, ok := sp.importMap[pkg]; ok {
		qualified := pkg + dot + typeName
		if err := sp.resolveSchema(qualified); err == nil {
			return schemaProperty{Ref: "#/components/schemas/" + qualified}
		}
	}

	return schemaProperty{Type: "object"}
}

// fieldDescription returns text from a // line comment after the field, else Doc above the field.
func fieldDescription(field *ast.Field) string {
	if field.Comment != nil {
		if s := commentGroupText(field.Comment); s != "" {
			return s
		}
	}
	if field.Doc != nil {
		return commentGroupText(field.Doc)
	}
	return ""
}

// commentGroupText renders a Go comment group as Markdown via go/doc/comment,
// preserving paragraphs, lists, and code blocks.
func commentGroupText(g *ast.CommentGroup) string {
	var p comment.Parser
	var pr comment.Printer
	return strings.TrimSpace(string(pr.Markdown(p.Parse(g.Text()))))
}

func getJSONTag(field *ast.Field) (name string, omitempty bool) {
	if field.Tag == nil {
		if len(field.Names) > 0 {
			return field.Names[0].Name, false
		}
		return "", false
	}
	tag := reflect.StructTag(strings.Trim(field.Tag.Value, "`"))
	jsonTag := tag.Get("json")
	if jsonTag == "" || jsonTag == "-" {
		return jsonTag, false
	}
	name, rest, _ := strings.Cut(jsonTag, ",")
	return name, strings.Contains(rest, "omitempty")
}

func splitQualifiedName(name string) (string, string) {
	pkg, typeName, ok := strings.Cut(name, dot)
	if !ok {
		return "", name
	}
	return pkg, typeName
}

func exprToString(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.SelectorExpr:
		return exprToString(t.X) + dot + t.Sel.Name
	case *ast.StarExpr:
		return exprToString(t.X)
	default:
		return ""
	}
}
