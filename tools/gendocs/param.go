// Package main provides parameter parsing functionality for AIStore API documentation generation.
//
// Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
package main

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"maps"
	"path/filepath"
	"sort"
	"strings"
)

const (
	qparamPrefix    = "Qparam"
	apcQparamPrefix = "apc.Qparam"
	hdrPrefix       = "Hdr"
	actPrefix       = "Act"

	// OpenAPI parameter `in` values
	inQuery  = "query"
	inHeader = "header"
)

type param struct {
	Name        string
	Type        string
	Description string
	Value       string
	In          string // "query" or "header"
}

// paramSet is a map of parameter names to their definitions.
type paramSet map[string]param

// loadConstantsFromPackages walks each directory (including any subdirectories
// that contain Go source) as a Go package, type-checks it, and extracts
// Qparam, Hdr, and Action string constants in one pass.
//
// Type-checking at package scope resolves intra-package identifier aliases
// such as `ActXactStart = Start` where the right-hand side is defined in a
// sibling file. Cross-package references are not resolved.
func loadConstantsFromPackages(projectRoot string, dirs []string) (paramSet, map[string]string, error) {
	params := paramSet{}
	actions := map[string]string{}

	for _, dir := range dirs {
		root := filepath.Join(projectRoot, dir)
		pkgDirs, err := goPackageDirs(root)
		if err != nil {
			return nil, nil, fmt.Errorf("scan %s: %w", dir, err)
		}
		for _, pkgDir := range pkgDirs {
			files, info, err := typeCheckPackage(pkgDir)
			if err != nil {
				return nil, nil, fmt.Errorf("load %s: %w", pkgDir, err)
			}
			if len(files) == 0 {
				continue
			}
			pkgPrefix := files[0].Name.Name + dot

			maps.Copy(params, extractQparamConstants(files, info, pkgPrefix))
			maps.Copy(params, extractHeaderConstants(files, info, pkgPrefix))
			// Action keys are unprefixed: today only api/apc/actmsg.go defines
			// Act* string constants in the scanned directories.
			maps.Copy(actions, extractActionConstants(files, info))
		}
	}
	return params, actions, nil
}

// goPackageDirs returns root and every subdirectory containing at least one
// Go source file, sorted for deterministic order.
func goPackageDirs(root string) ([]string, error) {
	seen := map[string]struct{}{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != goFileExt {
			return nil
		}
		seen[filepath.Dir(path)] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(seen))
	for d := range seen {
		out = append(out, d)
	}
	sort.Strings(out)
	return out, nil
}

// typeCheckPackage parses every non-test .go file in dir as one Go package
// and type-checks them together. The returned types.Info is suitable for
// evalStringConst. Type errors (unresolved imports, etc.) are silently
// ignored — only constant evaluation is required.
func typeCheckPackage(dir string) ([]*ast.File, *types.Info, error) {
	fset := token.NewFileSet()
	skipTests := func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}
	pkgs, err := parser.ParseDir(fset, dir, skipTests, parser.ParseComments)
	if err != nil {
		return nil, nil, err
	}
	if len(pkgs) == 0 {
		return nil, nil, nil
	}

	// A valid Go directory has at most one non-test package; pick it
	// deterministically by name in case of accidental drift.
	names := make([]string, 0, len(pkgs))
	for name := range pkgs {
		names = append(names, name)
	}
	sort.Strings(names)
	pkg := pkgs[names[0]]

	files := make([]*ast.File, 0, len(pkg.Files))
	for _, f := range pkg.Files {
		files = append(files, f)
	}

	info := &types.Info{Types: make(map[ast.Expr]types.TypeAndValue)}
	conf := types.Config{Error: func(error) {}}
	_, _ = conf.Check(pkg.Name, fset, files, info)
	return files, info, nil
}

// evalStringConst returns the computed string value of a Go constant
// expression via the type-checker. Empty if unresolvable.
func evalStringConst(expr ast.Expr, info *types.Info) string {
	tv, ok := info.Types[expr]
	if !ok || tv.Value == nil || tv.Value.Kind() != constant.String {
		return ""
	}
	return constant.StringVal(tv.Value)
}

// walkStringConsts visits every CONST value-spec across files. For each name
// matching predicate, it evaluates the right-hand side to a string via the
// type-checker and calls yield. Names without a string constant value are
// skipped.
func walkStringConsts(
	files []*ast.File,
	info *types.Info,
	match func(name string) bool,
	yield func(name, value string, vs *ast.ValueSpec),
) {
	for _, f := range files {
		for _, decl := range f.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, name := range vs.Names {
					if !match(name.Name) {
						continue
					}
					if vs.Values == nil || i >= len(vs.Values) {
						continue
					}
					value := evalStringConst(vs.Values[i], info)
					if value == "" {
						continue
					}
					yield(name.Name, value, vs)
				}
			}
		}
	}
}

// extracts Qparam constants and tags them as query parameters.
func extractQparamConstants(files []*ast.File, info *types.Info, pkgPrefix string) paramSet {
	out := paramSet{}
	walkStringConsts(files, info,
		func(n string) bool { return strings.HasPrefix(n, qparamPrefix) },
		func(name, value string, vs *ast.ValueSpec) {
			full := pkgPrefix + name
			out[full] = param{
				Name:        full,
				Description: extractDescription(vs),
				Value:       value,
				In:          inQuery,
			}
		})
	return out
}

// extracts HTTP header constants (any name containing "Hdr") and tags them as
// header parameters.
func extractHeaderConstants(files []*ast.File, info *types.Info, pkgPrefix string) paramSet {
	out := paramSet{}
	walkStringConsts(files, info,
		func(n string) bool { return strings.Contains(n, hdrPrefix) },
		func(name, value string, vs *ast.ValueSpec) {
			full := pkgPrefix + name
			out[full] = param{
				Name:        full,
				Description: extractDescription(vs),
				Value:       value,
				In:          inHeader,
			}
		})
	return out
}

// extracts Act* string constants. Keys are bare names (no package prefix);
// today only api/apc/actmsg.go defines them within the scanned directories.
func extractActionConstants(files []*ast.File, info *types.Info) map[string]string {
	out := map[string]string{}
	walkStringConsts(files, info,
		func(n string) bool { return strings.HasPrefix(n, actPrefix) },
		func(name, value string, _ *ast.ValueSpec) { out[name] = value })
	return out
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
