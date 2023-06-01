// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/urfave/cli"
)

// walk locally accessible files and directories; handle file/dir matching wildcards and patterns
// (no regex)

type (
	// Struct to keep info groupped by file extension.
	// Properties start with capital for reflexion (templates)
	counter struct {
		Cnt  int64
		Size int64
	}
	fobj struct {
		path string
		name string
		size int64
	}
	// recursive walk
	walkCtx struct {
		pattern      string
		trimPrefix   string
		appendPrefix string
		fobjs        fobjs
	}
	fobjs []fobj // sortable
)

// removes base part from the path making object name from it.
// Extra step - removing leading '/' if base does not end with it
func cutPrefixFromPath(path, base string) string {
	str := strings.TrimPrefix(path, base)
	return strings.TrimPrefix(str, "/")
}

// Returns files from the 'path' directory. No recursion.
// If shell filename-matching pattern is present include only those that match.
func listDir(path, trimPrefix, appendPrefix, pattern string) (fobjs fobjs, _ error) {
	dentries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	for _, dent := range dentries {
		if dent.IsDir() || !dent.Type().IsRegular() {
			continue
		}
		if matched, err := filepath.Match(pattern, filepath.Base(dent.Name())); !matched || err != nil {
			continue
		}
		if finfo, err := dent.Info(); err == nil {
			debug.Assert(finfo.Name() == dent.Name())
			fullPath := filepath.Join(path, dent.Name())
			fo := fobj{
				name: appendPrefix + cutPrefixFromPath(fullPath, trimPrefix), // empty strings ignored
				path: fullPath,
				size: finfo.Size(),
			}
			fobjs = append(fobjs, fo)
		}
	}
	return fobjs, nil
}

// Traverse 'path' recursively
// If shell filename-matching pattern is present include only those that match
func listRecurs(path, trimPrefix, appendPrefix, pattern string) (fobjs, error) {
	ctx := &walkCtx{
		pattern:      pattern,
		trimPrefix:   trimPrefix,
		appendPrefix: appendPrefix,
	}
	if err := filepath.Walk(path, ctx.do); err != nil {
		return nil, err
	}
	return ctx.fobjs, nil
}

// in:
// - path with optional wildcard(s)
// - base to generate object names
// - recursive flag
// returns:
// - slice of matching {fname or dirname, destination object name, size} triplets
// - true if source is a directory
func lsFobj(c *cli.Context, path, trimPrefix, appendPrefix string, ndir *int, recurs bool) (fobjs, error) {
	var (
		pattern    = "*" // default pattern: entire directory
		finfo, err = os.Stat(path)
	)
	debug.Assert(trimPrefix == "" || strings.HasPrefix(path, trimPrefix))

	// single file
	if err == nil && !finfo.IsDir() {
		if trimPrefix == "" {
			// [convention] the default is to trim everything leaving only the base
			trimPrefix = filepath.Dir(path)
		}
		objName := cutPrefixFromPath(path, trimPrefix)
		fo := fobj{name: appendPrefix + objName, path: path, size: finfo.Size()}
		return []fobj{fo}, nil
	}

	if err != nil {
		// expecting the base to be a filename-matching pattern
		pattern = filepath.Base(path)
		isPattern := strings.Contains(pattern, "*") ||
			strings.Contains(pattern, "?") ||
			strings.Contains(pattern, "\\")
		if !isPattern {
			warn := fmt.Sprintf("%q is not a directory and does not appear to be a shell filename matching pattern (%q)",
				path, pattern)
			actionWarn(c, warn)
		}
		path = filepath.Dir(path)
		finfo, err = os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("%q does not exist", path)
		}
		if !finfo.IsDir() {
			return nil, fmt.Errorf("%q is not a directory", path)
		}
	}

	*ndir++
	// [convention] ditto
	if trimPrefix == "" {
		trimPrefix = path
	}
	f := listDir
	if recurs {
		f = listRecurs
	}
	return f(path, trimPrefix, appendPrefix, pattern)
}

func groupByExt(files []fobj) (int64, map[string]counter) {
	totalSize := int64(0)
	extSizes := make(map[string]counter, 10)
	for _, f := range files {
		totalSize += f.size
		ext := filepath.Ext(f.path)
		c := extSizes[ext]
		c.Size += f.size
		c.Cnt++
		extSizes[ext] = c
	}
	return totalSize, extSizes
}

/////////////
// walkCtx //
/////////////

func (w *walkCtx) do(fqn string, info os.FileInfo, err error) error {
	if err != nil {
		if os.IsPermission(err) {
			return nil
		}
		if cmn.IsErrObjNought(err) {
			return nil
		}
		return fmt.Errorf("filepath-walk invoked with err: %v", err)
	}
	if info.IsDir() {
		return nil
	}
	if matched, _ := filepath.Match(w.pattern, filepath.Base(fqn)); !matched {
		return nil
	}
	fo := fobj{
		name: w.appendPrefix + cutPrefixFromPath(fqn, w.trimPrefix), // empty strings ignored
		path: fqn,
		size: info.Size(),
	}
	w.fobjs = append(w.fobjs, fo)
	return nil
}

///////////
// fobjs //
///////////

func (a fobjs) Len() int           { return len(a) }
func (a fobjs) Less(i, j int) bool { return a[i].path < a[j].path }
func (a fobjs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
