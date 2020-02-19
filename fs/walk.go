// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"errors"
	"os"
	"sort"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/karrick/godirwalk"
)

const (
	// Determines the threshold of error count which will result in halting
	// the walking operation.
	errThreshold = 1000
)

type (
	errFunc  func(string, error) godirwalk.ErrorAction
	WalkFunc func(fqn string, de DirEntry) error
)

type (
	DirEntry interface {
		IsDir() bool
	}

	Options struct {
		Dir string

		Mpath *MountpathInfo
		Bck   cmn.Bck
		CTs   []string

		ErrCallback errFunc
		Callback    WalkFunc
		Sorted      bool
	}

	errCallbackWrapper struct {
		counter atomic.Int64
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
		ew.counter.Add(1)
		return godirwalk.SkipNode
	}
	return godirwalk.Halt
}

// godirwalk is used by default. If you want to switch to standard filepath.Walk do:
// 1. Rewrite `callback` to:
//   func (opts *Options) callback(fqn string, de os.FileInfo, err error) error {
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

var _ DirEntry = &godirwalk.Dirent{}

func (opts *Options) callback(fqn string, de *godirwalk.Dirent) error {
	return opts.Callback(fqn, de)
}

func Walk(opts *Options) error {
	// For now `ErrCallback` is not used. Remove if something changes and ensure
	// that we have
	cmn.Assert(opts.ErrCallback == nil)

	ew := &errCallbackWrapper{}
	// Using default error callback which halts on bucket errors and halts
	// on `errThreshold` lom errors.
	opts.ErrCallback = ew.PathErrToAction

	var fqns []string
	if opts.Dir != "" {
		fqns = append(fqns, opts.Dir)
	} else {
		cmn.Assert(len(opts.CTs) > 0)
		if opts.Bck.Name != "" {
			// If bucket is defined we want to only walk specific content-types
			// inside the bucket.
			for _, ct := range opts.CTs {
				fqns = append(fqns, opts.Mpath.MakePathCT(opts.Bck, ct))
			}
		} else {
			// If bucket is undefined we must first list all of them and then
			// for each of them generate the content-type paths.
			fqn := opts.Mpath.MakePathBck(opts.Bck)
			children, err := godirwalk.ReadDirnames(fqn, nil)
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			if opts.Sorted {
				sort.Strings(children)
			}

			bck := opts.Bck
			if len(opts.CTs) > 1 {
				fqns = make([]string, 0, len(children)*len(opts.CTs))
			} else {
				fqns = children[:0] // optimization to reuse previously allocated slice
			}
			for _, child := range children {
				for _, ct := range opts.CTs {
					bck.Name = child
					fqns = append(fqns, opts.Mpath.MakePathCT(bck, ct))
				}
			}
		}
	}

	gOpts := &godirwalk.Options{
		ErrorCallback: opts.ErrCallback,
		Callback:      opts.callback,
		Unsorted:      !opts.Sorted,
	}

	var err error
	for _, fqn := range fqns {
		if err1 := godirwalk.Walk(fqn, gOpts); err1 != nil && !os.IsNotExist(err1) {
			if errors.As(err1, &cmn.AbortedError{}) {
				glog.Info(err1)
				// Errors different from cmn.AbortedError should not be overwritten
				// by cmn.AbortedError. Assign err = err1 only when there wasn't any other error
				if err == nil {
					err = err1
				}
			} else {
				glog.Error(err1)
				err = err1
			}
		}
	}
	return err
}
