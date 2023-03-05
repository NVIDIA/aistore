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
	fobj struct {
		path string
		name string
		size int64
	}
	// Struct to keep info groupped by file extension.
	// Properties start with capital for reflexion (templates)
	counter struct {
		Cnt  int64
		Size int64
	}
	// recursive walk
	walkCtx struct {
		pattern      string
		trimPrefix   string
		appendPrefix string
		files        []fobj
	}

	fobjSlice []fobj // sortable
)

// removes base part from the path making object name from it.
// Extra step - removing leading '/' if base does not end with it
func cutPrefixFromPath(path, base string) string {
	str := strings.TrimPrefix(path, base)
	return strings.TrimPrefix(str, "/")
}

// Returns files from the 'path' directory. No recursion.
// If shell-filename matching pattern is used, includes only the matching files.
func listDir(path, trimPrefix, appendPrefix, pattern string) ([]fobj, error) {
	var (
		files         []fobj
		dentries, err = os.ReadDir(path)
	)
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
			files = append(files, fo)
		}
	}
	return files, nil
}

// Recursively traverses the 'path' dir.
// If shell-filename matching pattern is used, includes only the matching files.
func listRecurs(path, trimPrefix, appendPrefix, pattern string) ([]fobj, error) {
	ctx := &walkCtx{
		pattern:      pattern,
		trimPrefix:   trimPrefix,
		appendPrefix: appendPrefix,
	}
	if err := filepath.Walk(path, ctx.do); err != nil {
		return nil, err
	}
	return ctx.files, nil
}

// in:
// - path with optional wildcard(s)
// - base to generate object names
// - recursive flag
// returns:
// - list of triplets (file or dir path, object name, size) that match
func lsFobj(c *cli.Context, path, trimPrefix, appendPrefix string, recursive bool) (fobjSlice, error) {
	debug.Assert(trimPrefix == "" || strings.HasPrefix(path, trimPrefix))
	var (
		pattern   string
		info, err = os.Stat(path)
	)
	if err != nil {
		// expecting filename-matching pattern base
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
		info, err = os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("%q does not exist", path)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("%q is not a directory", path)
		}
	} else {
		if !info.IsDir() { // one file without custom name
			if trimPrefix == "" {
				// if trim prefix is not specified the default is to cut everything
				// leaving only the base
				trimPrefix = filepath.Dir(path)
			}
			objName := cutPrefixFromPath(path, trimPrefix)
			fo := fobj{name: appendPrefix + objName, path: path, size: info.Size()}
			files := []fobj{fo}
			return files, nil
		}

		// otherwise, the entire directory (note that '*' is applied automatically)
		pattern = "*"
	}

	// if trim prefix not specified by a called, cut the whole path from object name and use what's
	// left in 'pattern' part of a filename
	if trimPrefix == "" {
		trimPrefix = path
	}
	if !recursive {
		return listDir(path, trimPrefix, appendPrefix, pattern)
	}
	return listRecurs(path, trimPrefix, appendPrefix, pattern)
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
	w.files = append(w.files, fo)
	return nil
}

///////////////
// fobjSlice //
///////////////

func (a fobjSlice) Len() int           { return len(a) }
func (a fobjSlice) Less(i, j int) bool { return a[i].path < a[j].path }
func (a fobjSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
