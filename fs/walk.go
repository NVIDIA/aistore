// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	iofs "io/fs"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"

	"github.com/karrick/godirwalk"
)

const (
	// Determines the threshold of error count which will result in halting
	// the walking operation.
	errThreshold = 1000

	// Determines the buffer size of the mpath worker queue.
	mpathQueueSize = 100
)

type (
	DirEntry interface {
		IsDir() bool
	}

	walkFunc func(fqn string, de DirEntry) error

	WalkOpts struct {
		Mi       *Mountpath
		Callback walkFunc
		Bck      cmn.Bck
		Dir      string
		Prefix   string
		CTs      []string
		Sorted   bool // Ignored when using stdlib implementation (always sorted).
	}

	errCallbackWrapper struct {
		counter atomic.Int64 // (soft errors)
	}

	walkDirWrapper struct {
		ucb func(string, DirEntry) error // user-provided callback
		dir string                       // root pathname
		errCallbackWrapper
	}

	walker interface {
		walk(fqn []string, opts *WalkOpts) error
		mpathChildren(opts *WalkOpts) ([]string, error)
	}
)

// /////////////////////////////////
// HARDCODED TO GODIRWALK IMPLEMENTATION FOR BETTER MEMORY EFFICIENCY (15-30%)
// TO SWITCH TO STANDARD LIBRARY VERSION:
// REPLACE WITH: var useWalker walker = &stdlib{}
// IN wd.wcb() REPLACE PathErrToAction and godirwalk.Halt
// WITH PathWalkError and err
// /////////////////////////////////
var useWalker walker = &godir{}

func Walk(opts *WalkOpts) error {
	fqns, err := resolveFQNs(opts)
	if err != nil {
		return err
	}
	return useWalker.walk(fqns, opts)
}

func resolveFQNs(opts *WalkOpts) (fqns []string, err error) {
	switch {
	case opts.Dir != "":
		debug.Assert(opts.Prefix == "")
		fqns = append(fqns, opts.Dir)
	case opts.Bck.Name != "":
		debug.Assert(len(opts.CTs) > 0)
		// one bucket
		for _, ct := range opts.CTs {
			bdir := opts.Mi.MakePathCT(&opts.Bck, ct)
			if opts.Prefix != "" {
				fqns = append(fqns, _join(bdir, opts.Prefix))
			} else {
				fqns = append(fqns, bdir)
			}
		}
	default: // all buckets
		debug.Assert(len(opts.CTs) > 0)
		fqns, err = allMpathCTpaths(opts)
	}
	return
}

func _join(bdir, prefix string) string {
	if cos.IsLastB(prefix, filepath.Separator) {
		// easy choice: is the sub-directory to walk
		// (ie., not walking the entire parent - just this one)
		return bdir + cos.PathSeparator + prefix
	}
	if !cmn.Rom.Features().IsSet(feat.DontOptimizeVirtualDir) {
		sub := bdir + cos.PathSeparator + prefix
		// uneasy choice: if `sub` is an actual directory we further assume
		// (unless user says otherwise via feature flag)
		// _not_ to have the names that contain it as a prefix substring
		// (as in: "subdir/foo" and "subdir_bar")
		if finfo, err := os.Lstat(sub); err == nil && finfo.IsDir() {
			return sub
		}
	}
	return bdir
}

func allMpathCTpaths(opts *WalkOpts) (fqns []string, err error) {
	children, erc := useWalker.mpathChildren(opts)
	if erc != nil {
		return nil, erc
	}
	if len(opts.CTs) > 1 {
		fqns = make([]string, 0, len(children)*len(opts.CTs))
	} else {
		fqns = children[:0] // optimization to reuse previously allocated slice
	}
	bck := opts.Bck
	for _, child := range children {
		bck.Name = child
		if err := bck.ValidateName(); err != nil {
			continue
		}
		for _, ct := range opts.CTs {
			bdir := opts.Mi.MakePathCT(&bck, ct)
			if opts.Prefix != "" {
				fqns = append(fqns, _join(bdir, opts.Prefix))
			} else {
				fqns = append(fqns, bdir)
			}
		}
	}
	return
}

func AllMpathBcks(opts *WalkOpts) (bcks []cmn.Bck, err error) {
	children, erc := useWalker.mpathChildren(opts)
	if erc != nil {
		return nil, erc
	}
	bck := opts.Bck
	for _, child := range children {
		bck.Name = child
		if err := bck.ValidateName(); err != nil {
			continue
		}
		bcks = append(bcks, bck)
	}
	return
}

////////////////////
// WalkDir & walkDirWrapper - non-recursive walk
////////////////////

// NOTE: using Go filepath.WalkDir
// pros: lexical deterministic order; cons: reads the entire directory
func WalkDir(dir string, ucb func(string, DirEntry) error) error {
	wd := &walkDirWrapper{dir: dir, ucb: ucb}
	return filepath.WalkDir(dir, wd.wcb)
}

// wraps around user callback to implement default error handling and skipping.
func (wd *walkDirWrapper) wcb(path string, de iofs.DirEntry, err error) error {
	if err != nil {
		// Walk and WalkDir share the same error-processing logic
		// IF USING THE STANDARD LIBRARY: REPLACE WITH PathWalkError AND err
		if path != wd.dir && wd.PathErrToAction(path, err) != godirwalk.Halt {
			err = nil
		}
		return err
	}
	if de.IsDir() && path != wd.dir {
		return filepath.SkipDir
	}
	if !de.Type().IsRegular() {
		return nil
	}
	// user callback
	return wd.ucb(path, de)
}
