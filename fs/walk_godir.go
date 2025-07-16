//go:build !stdlibwalk

// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"context"
	"sort"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/memsys"

	"github.com/karrick/godirwalk"
)

type godir struct{}

// interface guard
var _ DirEntry = (*godirwalk.Dirent)(nil)

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

func (opts *WalkOpts) cb(fqn string, de *godirwalk.Dirent) error {
	return opts.Callback(fqn, de)
}

func (godir) walk(fqns []string, opts *WalkOpts) error {
	var (
		err error
		ew  = &errCallbackWrapper{}
	)
	scratch, slab := memsys.PageMM().AllocSize(memsys.DefaultBufSize)
	gOpts := &godirwalk.Options{
		ErrorCallback: ew.PathErrToAction, // "halts the walk" or "skips the node" (detailed comment above)
		Callback:      opts.cb,
		Unsorted:      !opts.Sorted,
		ScratchBuffer: scratch,
	}
	for _, fqn := range fqns {
		err1 := godirwalk.Walk(fqn, gOpts)
		if err1 == nil || cos.IsNotExist(err1) {
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
			nlog.Errorln(err1)
		}
		err = err1
	}
	slab.Free(scratch)
	return err
}

func (godir) mpathChildren(opts *WalkOpts) (children []string, err error) {
	var (
		fqn           = opts.Mi.MakePathBck(&opts.Bck)
		scratch, slab = memsys.PageMM().AllocSize(memsys.DefaultBufSize)
	)

	children, err = godirwalk.ReadDirnames(fqn, scratch)
	slab.Free(scratch)
	if err != nil {
		if cos.IsNotExist(err) {
			err = nil
		}
		return
	}
	if opts.Sorted {
		sort.Strings(children)
	}
	return
}
