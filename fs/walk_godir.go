//go:build !stdlibwalk

// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"cmp"
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"strings"

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
	switch {
	case err == ErrWalkStopped, cmn.IsErrBucketLevel(err), ew.counter.Load() > errThreshold:
		return godirwalk.Halt
	case cmn.IsErrObjLevel(err):
		ew.counter.Inc()
		return godirwalk.SkipNode
	default:
		return godirwalk.Halt
	}
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
		var err1 error
		if opts.Sorted {
			err1 = WalkSorted(fqn, gOpts)
		} else {
			err1 = godirwalk.Walk(fqn, gOpts)
		}
		if err1 == nil || cos.IsNotExist(err1) {
			continue
		}
		if cmn.IsErrMpathNotFound(err1) {
			nlog.Errorln(err1) // mountpath is getting detached or disabled
			continue
		}
		if err1 == ErrWalkStopped {
			break
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

// WalkSorted walks the tree in the order of object names ("a.json" before "a/b")
func WalkSorted(root string, gOpts *godirwalk.Options) error {
	if len(gOpts.ScratchBuffer) < godirwalk.MinimumScratchBufferSize {
		gOpts.ScratchBuffer = make([]byte, godirwalk.MinimumScratchBufferSize)
	}
	root = filepath.Clean(root)
	de, err := godirwalk.NewDirent(root)
	if err != nil {
		return err
	}
	if !de.IsDir() {
		return fmt.Errorf("cannot walk non-directory: %s", root)
	}
	switch err := _walkSorted(root, de, gOpts); err {
	case nil, filepath.SkipDir:
		return nil
	default:
		return err
	}
}

func _walkSorted(fqn string, de *godirwalk.Dirent, gOpts *godirwalk.Options) error {
	if err := gOpts.Callback(fqn, de); err != nil {
		if err == filepath.SkipDir {
			return err
		}
		if gOpts.ErrorCallback(fqn, err) == godirwalk.SkipNode {
			return nil
		}
		return err
	}
	if !de.IsDir() {
		return nil
	}
	children, err := godirwalk.ReadDirents(fqn, gOpts.ScratchBuffer)
	if err != nil {
		if gOpts.ErrorCallback(fqn, err) == godirwalk.SkipNode {
			return nil
		}
		return err
	}
	slices.SortFunc(children, cmpDirent)

	for _, child := range children {
		err := _walkSorted(filepath.Join(fqn, child.Name()), child, gOpts)
		switch {
		case err == nil:
		case err != filepath.SkipDir:
			return err
		case !child.IsDir():
			return nil // skip the remaining siblings
		}
	}
	return nil
}

// sort a directory as its name with a trailing '/'
func cmpDirent(a, b *godirwalk.Dirent) int {
	an, bn := a.Name(), b.Name()
	n := min(len(an), len(bn))
	if c := strings.Compare(an[:n], bn[:n]); c != 0 {
		return c
	}
	return cmp.Compare(_nextByte(an, n, a.IsDir()), _nextByte(bn, n, b.IsDir()))
}

func _nextByte(name string, n int, isDir bool) int {
	switch {
	case n < len(name):
		return int(name[n])
	case isDir:
		return '/'
	default:
		return -1
	}
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
