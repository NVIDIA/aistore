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
	"github.com/NVIDIA/aistore/cmn/cos"
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
		path    string
		dstName string
		size    int64
	}
	// recursive walk
	walkCtx struct {
		pattern    string
		trimPref   string
		appendPref string
		fobjs      fobjs // result
	}
	fobjs []fobj // sortable
)

// removes base part from the path making object name from it.
// Extra step - removing leading '/' if base does not end with it
func trimPrefix(path, base string) string {
	str := strings.TrimPrefix(path, base /*prefix*/)
	return strings.TrimPrefix(str, "/")
}

// Returns longest common prefix ending with '/' (exclusive) for objects in the template
// /path/to/dir/test{0..10}/dir/another{0..10} => /path/to/dir
// /path/to/prefix-@00001-gap-@100-suffix => /path/to
func rangeTrimPrefix(pt *cos.ParsedTemplate) string {
	i := strings.LastIndex(pt.Prefix, string(os.PathSeparator))
	debug.Assert(i >= 0)
	return pt.Prefix[:i+1]
}

// Returns files from the 'path' directory. No recursion.
// If shell filename-matching pattern is present include only those that match.
func listDir(path, trimPref, appendPref, pattern string) (fobjs fobjs, _ error) {
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
			fullPath := filepath.Join(path, dent.Name())
			fobj := fobj{
				dstName: appendPref + trimPrefix(fullPath, trimPref), // empty strings ignored
				path:    fullPath,
				size:    finfo.Size(),
			}
			fobjs = append(fobjs, fobj)
		}
	}
	return fobjs, nil
}

// Traverse 'path' recursively
// If shell filename-matching pattern is present include only those that match
func listRecurs(path, trimPref, appendPref, pattern string) (fobjs, error) {
	ctx := &walkCtx{
		pattern:    pattern,
		trimPref:   trimPref,
		appendPref: appendPref,
	}
	if err := filepath.Walk(path, ctx.do); err != nil {
		return nil, err
	}
	return ctx.fobjs, nil
}

// IN:
// - source path that may contain wildcard(s)
// - (trimPref, appendPref) combo to influence destination naming
// - recursive, etc.
// Returns:
// - a slice of matching triplets: {source fname or dirname, destination name, size in bytes}
func lsFobj(c *cli.Context, path, trimPref, appendPref string, ndir *int, recurs, incl bool) (fobjs, error) {
	var (
		pattern    = "*" // default pattern: entire directory
		finfo, err = os.Stat(path)
	)
	debug.Assert(trimPref == "" || strings.HasPrefix(path, trimPref))

	// single file (uses cases: reg file, --template, --list)
	if err == nil && !finfo.IsDir() {
		if trimPref == "" {
			// [convention] trim _everything_ leaving only the base, unless (below)
			trimPref = filepath.Dir(path)
			if incl {
				// --include-source-(root)-dir: retain the last snippet
				trimPref = filepath.Dir(trimPref)
			}
		}
		fo := fobj{
			dstName: appendPref + trimPrefix(path, trimPref),
			path:    path,
			size:    finfo.Size(),
		}
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
	if trimPref == "" {
		trimPref = path
		if incl {
			trimPref = strings.TrimSuffix(path, filepath.Base(path))
		}
	}
	f := listDir
	if recurs {
		f = listRecurs
	}
	return f(path, trimPref, appendPref, pattern)
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
	fobj := fobj{
		dstName: w.appendPref + trimPrefix(fqn, w.trimPref), // empty strings ignored
		path:    fqn,
		size:    info.Size(),
	}
	w.fobjs = append(w.fobjs, fobj)
	return nil
}

///////////
// fobjs //
///////////

func (a fobjs) Len() int           { return len(a) }
func (a fobjs) Less(i, j int) bool { return a[i].path < a[j].path }
func (a fobjs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
