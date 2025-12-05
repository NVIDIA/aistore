// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	iofs "io/fs"
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
	// Struct to keep info grouped by file extension.
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
		c          *cli.Context
		pattern    string
		trimPref   string
		appendPref string
		fobjs      fobjs // result
		cont       bool  // continueOnErrorFlag
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
func listDir(c *cli.Context, path, trimPref, appendPref, pattern string) (fobjs fobjs, _ error) {
	dentries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	cont := flagIsSet(c, continueOnErrorFlag)
	for _, dent := range dentries {
		if dent.IsDir() || !dent.Type().IsRegular() {
			continue
		}
		if matched, err := filepath.Match(pattern, dent.Name()); !matched || err != nil {
			continue
		}
		finfo, err := dent.Info()
		if err != nil {
			err = fmt.Errorf("failed to info(%s): %w", filepath.Join(path, dent.Name()), err)
			if !cont {
				return nil, err
			}
			if cliConfVerbose() {
				actionWarn(c, err.Error())
			}
			continue
		}

		fullPath := filepath.Join(path, dent.Name())
		fobj := fobj{
			dstName: appendPref + trimPrefix(fullPath, trimPref), // empty strings ignored
			path:    fullPath,
			size:    finfo.Size(),
		}
		fobjs = append(fobjs, fobj)
	}
	return fobjs, nil
}

// Traverse 'path' recursively
// If shell filename-matching pattern is present include only those that match
func listRecurs(c *cli.Context, path, trimPref, appendPref, pattern string) (fobjs, error) {
	ctx := &walkCtx{
		c:          c,
		pattern:    pattern,
		trimPref:   trimPref,
		appendPref: appendPref,
		cont:       flagIsSet(c, continueOnErrorFlag),
	}
	if err := filepath.WalkDir(path, ctx.do); err != nil {
		return nil, err
	}
	return ctx.fobjs, nil
}

// IN:
// - source path that may contain wildcard(s)
// - (trimPref, appendPref) combo to influence destination naming
// - recursive, etc.
// OUT:
// - a slice of matching triplets: {source fname or dirname, destination name, size in bytes}
func lsFobj(c *cli.Context, srcpath, trimPref, appendPref string, ndir *int, recurs, incl, globbed bool) (fobjs fobjs, _ error) {
	// 1. fstat ok
	finfo, err := os.Stat(srcpath)
	if err == nil {
		if finfo.IsDir() {
			return _lsDir(c, srcpath, trimPref, appendPref, cos.WildcardMatchAll, ndir, recurs, incl)
		}
		return _lsFil(finfo, srcpath, trimPref, appendPref, incl)
	}

	if globbed {
		return nil, &errDoesNotExist{what: "srcpath", name: srcpath}
	}
	// 2. glob
	const fmte = "%q is not a directory and does not appear to be a filename-matching pattern"
	all, e := filepath.Glob(srcpath)
	if e != nil {
		return nil, fmt.Errorf(fmte+": %v", srcpath, e)
	}

	// no matches? extract basename and use it as a pattern to list the parent directory
	if len(all) == 0 {
		pattern := filepath.Base(srcpath)
		if !isPattern(pattern) {
			return nil, fmt.Errorf(fmte, srcpath)
		}
		parent := filepath.Dir(srcpath)
		if _, err := os.Stat(parent); err != nil {
			return nil, &errDoesNotExist{what: "path", name: parent}
		}
		return _lsDir(c, parent, trimPref, appendPref, pattern, ndir, recurs, incl)
	}

	// 3. append all
	for _, src := range all {
		fob, err := lsFobj(c, src, trimPref, appendPref, ndir, recurs, incl, true /*globbed*/)
		if err != nil {
			return nil, fmt.Errorf("nested failure to 'ls %s': %v", src, err)
		}
		fobjs = append(fobjs, fob...)
	}
	return fobjs, nil
}

func _lsDir(c *cli.Context, srcpath, trimPref, appendPref, pattern string, ndir *int, recurs, incl bool) (fobjs, error) {
	*ndir++
	// [NOTE convention] destination naming:
	// trim the entire srcpath (dir) _or_ retain its last snippet ("a/b/c/" => "c/<basename>")
	if trimPref == "" {
		trimPref = srcpath
		if incl {
			// `--include-src-dir` flag: retain the last snippet
			trimPref = strings.TrimSuffix(srcpath, filepath.Base(srcpath))
		}
	}
	if recurs {
		return listRecurs(c, srcpath, trimPref, appendPref, pattern)
	}
	return listDir(c, srcpath, trimPref, appendPref, pattern)
}

func _lsFil(finfo os.FileInfo, srcpath, trimPref, appendPref string, incl bool) (fobjs, error) {
	if trimPref == "" {
		// [NOTE convention] destination naming: just the basename or <last-snippet/basename> (e.g. above)
		trimPref = filepath.Dir(srcpath)
		if incl {
			// `--include-src-dir` flag: retain the last snippet
			trimPref = filepath.Dir(trimPref)
		}
	}
	fo := fobj{
		dstName: appendPref + trimPrefix(srcpath, trimPref),
		path:    srcpath,
		size:    finfo.Size(),
	}
	return []fobj{fo}, nil
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
// walkCtx - fs.WalkDir recursive
/////////////

// callback via `filepath.WalkDir` of the type `fs.WalkDirFunc`
func (w *walkCtx) do(fqn string, dent iofs.DirEntry, err error) error {
	const tag = "filepath-walkdir"
	if err != nil {
		if dent == nil {
			return err // FATAL: cannot read root
		}
		parent := filepath.Dir(fqn)
		if errors.Is(err, iofs.ErrPermission) {
			if cliConfVerbose() {
				warn := fmt.Sprintf("%s(%s) access denied: %v", tag, parent, err)
				actionWarn(w.c, warn)
			}
			return iofs.SkipDir // NOTE: always skip nested os.ReadDir permissions
		}
		if cmn.IsErrObjNought(err) {
			return nil
		}
		return fmt.Errorf("%s(%s) failed: %w", tag, parent, err)
	}

	if dent.IsDir() {
		// TODO: optimize-out the entire dir (fs.SkipDir) if it doesn't match (tbd) directory-matching-pattern
		return nil
	}
	if !dent.Type().IsRegular() {
		return nil
	}
	if w.pattern != "" {
		if matched, _ := filepath.Match(w.pattern, dent.Name()); !matched {
			return nil
		}
	}

	finfo, errV := dent.Info()
	if errV != nil {
		err := fmt.Errorf("%s: failed to info(%s): %w", tag, fqn, errV)
		if !w.cont { // continueOnErrorFlag
			return err
		}
		if cliConfVerbose() {
			actionWarn(w.c, err.Error())
		}
		return nil
	}

	fobj := fobj{
		dstName: w.appendPref + trimPrefix(fqn, w.trimPref), // empty strings ignored
		path:    fqn,
		size:    finfo.Size(),
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
