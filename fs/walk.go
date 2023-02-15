// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"context"
	iofs "io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
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
		CTs      []string
		Sorted   bool
	}

	errCallbackWrapper struct {
		counter atomic.Int64
	}

	walkDirWrapper struct {
		ucb func(string, DirEntry) error // user-provided callback
		dir string                       // root pathname
		errCallbackWrapper
	}
)

// PathErrToAction is a default error callback for fast godirwalk.Walk.
// The idea is that on any error that was produced during the walk we dispatch
// this handler and act upon the error.
//
// By default it halts on bucket level errors because there is no option to
// continue walking if there is a problem with a bucket. Also we count "soft"
// errors and abort if we reach certain amount of them.
func (ew *errCallbackWrapper) PathErrToAction(_ string, err error) godirwalk.ErrorAction {
	if cmn.IsErrBucketLevel(err) {
		return godirwalk.Halt
	}
	if ew.counter.Load() > errThreshold {
		return godirwalk.Halt
	}
	if cmn.IsErrObjLevel(err) {
		ew.counter.Inc()
		return godirwalk.SkipNode
	}
	return godirwalk.Halt
}

// godirwalk is used by default. If you want to switch to standard filepath.Walk do:
// 1. Rewrite `callback` to:
//   func (opts *WalkOpts) callback(fqn string, de os.FileInfo, err error) error {
//     if err != nil {
//        if err := cmn.PathWalkErr(err); err != nil {
//          return err
//        }
//        return nil
//     }
//     return opts.callback(fqn, de)
//   }
// 2. Replace `Walk` body with one-liner:
//   return filepath.Walk(fqn, opts.callback)
// No more changes required.
// NOTE: for standard filepath.Walk option 'Sorted' is ignored

// interface guard
var _ DirEntry = (*godirwalk.Dirent)(nil)

func (opts *WalkOpts) callback(fqn string, de *godirwalk.Dirent) error {
	return opts.Callback(fqn, de)
}

func Walk(opts *WalkOpts) error {
	var (
		fqns []string
		err  error
		ew   = &errCallbackWrapper{}
	)
	if opts.Dir != "" {
		fqns = append(fqns, opts.Dir)
	} else {
		debug.Assert(len(opts.CTs) > 0)
		if opts.Bck.Name != "" {
			// walk specific content-types inside the bucket.
			for _, ct := range opts.CTs {
				fqns = append(fqns, opts.Mi.MakePathCT(&opts.Bck, ct))
			}
		} else {
			// all content-type paths for all bucket subdirectories
			fqns, err = allMpathCTpaths(opts)
			if len(fqns) == 0 || err != nil {
				return err
			}
		}
	}
	scratch, slab := memsys.PageMM().AllocSize(memsys.DefaultBufSize)
	gOpts := &godirwalk.Options{
		ErrorCallback: ew.PathErrToAction, // "halts the walk" or "skips the node" (detailed comment above)
		Callback:      opts.callback,
		Unsorted:      !opts.Sorted,
		ScratchBuffer: scratch,
	}
	for _, fqn := range fqns {
		err1 := godirwalk.Walk(fqn, gOpts)
		if err1 == nil || os.IsNotExist(err1) {
			continue
		}
		// NOTE: mountpath is getting detached or disabled
		if cmn.IsErrMountpathNotFound(err1) {
			glog.Error(err1)
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
		if err1 != context.Canceled {
			glog.Error(err1)
		}
		err = err1
	}
	slab.Free(scratch)
	return err
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
			fqns = append(fqns, opts.Mi.MakePathCT(&bck, ct))
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
	var (
		fqn           = opts.Mi.MakePathBck(&opts.Bck)
		scratch, slab = memsys.PageMM().AllocSize(memsys.DefaultBufSize)
	)
	children, err = godirwalk.ReadDirnames(fqn, scratch)
	slab.Free(scratch)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return
	}
	if opts.Sorted {
		sort.Strings(children)
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

// wraps around user callback to implement default error handling and skipping
func (wd *walkDirWrapper) wcb(path string, de iofs.DirEntry, err error) error {
	if err != nil {
		// Walk and WalkDir share the same error-processing logic (hence, godirwalk enum)
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
