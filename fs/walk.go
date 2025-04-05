// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"context"
	iofs "io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

const (
	// Determines the threshold of error count which will result in halting
	// the walking operation.
	errThreshold = 1000

	// Determines the buffer size of the mpath worker queue.
	mpathQueueSize = 100
)

type (
	WalkOpts struct {
		Mi       *Mountpath
		Callback iofs.WalkDirFunc
		Bck      cmn.Bck
		Dir      string
		Prefix   string
		CTs      []string
	}

	errCallbackWrapper struct {
		counter atomic.Int64
	}

	walkDirWrapper struct {
		ucb func(string, iofs.DirEntry, error) error // user-provided callback
		dir string                                   // root pathname
		errCallbackWrapper
	}
)

// PathErrToAction is a default error callback for fast filepath.WalkDir
// The idea is that on any error that was produced during the walk we dispatch
// this handler and act upon the error.
//
// By default it halts on bucket level errors because there is no option to
// continue walking if there is a problem with a bucket. Also we count "soft"
// errors and abort if we reach certain amount of them.
func (ew *errCallbackWrapper) PathErrToAction(_ string, err error) error {
	if cmn.IsErrBucketLevel(err) {
		return err
	}
	if ew.counter.Load() > errThreshold {
		return err
	}
	if cmn.IsErrObjLevel(err) {
		ew.counter.Inc()
		return nil
	}
	return err
}

func (opts *WalkOpts) callback(ew *errCallbackWrapper) iofs.WalkDirFunc {
	return func(fqn string, de iofs.DirEntry, err error) error {
		if err != nil {
			return ew.PathErrToAction(fqn, err)
		}
		return opts.Callback(fqn, de, err)
	}
}

func Walk(opts *WalkOpts) error {
	var (
		fqns []string
		err  error
		ew   = &errCallbackWrapper{}
	)
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
		if len(fqns) == 0 || err != nil {
			return err
		}
	}

	for _, fqn := range fqns {
		err1 := filepath.WalkDir(fqn, opts.callback(ew))
		if err1 == nil || os.IsNotExist(err1) {
			continue
		}
		if cmn.IsErrMpathNotFound(err1) {
			nlog.Errorln(err1) // mountpath is getting detached or disabled
			continue
		}
		if cmn.IsErrAborted(err1) {
			// Errors different from cmn.ErrAborted should not be overwritten
			// by cmn.ErrAborted. Assign err = err1 only when there wasn't any other error
			if err == nil {
				err = err1
			}
			continue
		}
		if err1 != context.Canceled && !cmn.IsErrObjNought(err1) {
			nlog.Errorln(err)
		}
		err = err1
	}
	return err
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
		if finfo, err := os.Stat(sub); err == nil && finfo.IsDir() {
			return sub
		}
	}
	return bdir
}

func allMpathCTpaths(opts *WalkOpts) (fqns []string, err error) {
	children, erc := mpathChildren(opts)
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
	children, erc := mpathChildren(opts)
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

func mpathChildren(opts *WalkOpts) (children []string, err error) {
	fqn := opts.Mi.MakePathBck(&opts.Bck)
	// os.ReadDir returns the entries in lexical order so additional
	// sorting is not needed.
	entries, err := os.ReadDir(fqn)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return
	}
	children = make([]string, 0, len(entries))
	for _, entry := range entries {
		children = append(children, entry.Name())
	}
	sort.Strings(children)
	return
}

////////////////////
// WalkDir & walkDirWrapper - non-recursive walk
////////////////////

// NOTE: using Go filepath.WalkDir
// pros: lexical deterministic order; cons: reads the entire directory
func WalkDir(dir string, ucb func(string, iofs.DirEntry, error) error) error {
	wd := &walkDirWrapper{dir: dir, ucb: ucb}
	return filepath.WalkDir(dir, wd.wcb)
}

// wraps around user callback to implement default error handling and skipping
func (wd *walkDirWrapper) wcb(path string, de iofs.DirEntry, err error) error {
	if err != nil {
		// Walk and WalkDir share the same error-processing logic
		if path != wd.dir && wd.PathErrToAction(path, err) != err {
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
	return wd.ucb(path, de, err)
}
