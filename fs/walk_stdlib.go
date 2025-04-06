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

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

//nolint:unused // alternative implementation; unused because godirwalk is hardcoded in walk.go
type stdlib struct{}

// PathWalkError is a default error callback for filepath.WalkDir
//
// By default it halts on bucket level errors because there is no option to
// continue walking if there is a problem with a bucket. Also we count "soft"
// errors and abort if we reach certain amount of them.
func (ew *errCallbackWrapper) PathWalkError(_ string, err error) error {
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

//nolint:unused // alternative implementation; unused because godirwalk is hardcoded in walk.go
func (stdlib) walk(fqns []string, opts *WalkOpts) error {
	var (
		err error
		ew  = &errCallbackWrapper{}
	)
	walkFn := func(path string, de iofs.DirEntry, err error) error {
		if err != nil {
			if path != "" && ew.PathWalkError(path, err) != err {
				return nil
			}
			return err
		}
		return opts.Callback(path, de)
	}
	for _, fqn := range fqns {
		err1 := filepath.WalkDir(fqn, walkFn)
		if err1 == nil || os.IsNotExist(err1) {
			continue
		}
		if cmn.IsErrMpathNotFound(err1) {
			nlog.Errorln(err1)
			continue
		}
		if cmn.IsErrAborted(err1) {
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
	return err
}

//nolint:unused // alternative implementation; unused because godirwalk is hardcoded in walk.go
func (stdlib) mpathChildren(opts *WalkOpts) (children []string, err error) {
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
	return
}
