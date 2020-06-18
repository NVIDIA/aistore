// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"container/heap"
	"context"
	"errors"
	"os"
	"sort"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/karrick/godirwalk"
	"golang.org/x/sync/errgroup"
)

const (
	// Determines the threshold of error count which will result in halting
	// the walking operation.
	errThreshold = 1000

	// Determines the buffer size of the mpath worker queue.
	mpathQueueSize = 100
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

	objInfo struct {
		mpathIdx int
		fqn      string
		objName  string
	}
	objInfos []objInfo
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

func (h objInfos) Len() int           { return len(h) }
func (h objInfos) Less(i, j int) bool { return h[i].objName < h[j].objName }
func (h objInfos) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *objInfos) Push(x interface{}) {
	info := x.(objInfo)
	debug.Assert(info.objName == "")
	parsedFQN, err := Mountpaths.ParseFQN(info.fqn)
	if err != nil {
		return
	}
	info.objName = parsedFQN.ObjName
	*h = append(*h, info)
}

func (h *objInfos) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
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
				bck.Name = child
				if cmn.ValidateBckName(bck.Name) != nil {
					continue
				}
				for _, ct := range opts.CTs {
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

func WalkBck(opts *Options) error {
	var (
		mpaths, _  = Mountpaths.Get()
		mpathChs   = make([]chan string, len(mpaths))
		group, ctx = errgroup.WithContext(context.Background())
	)

	for i := 0; i < len(mpaths); i++ {
		mpathChs[i] = make(chan string, mpathQueueSize)
	}

	cmn.Assert(opts.Mpath == nil)
	idx := 0
	for _, mpath := range mpaths {
		group.Go(func(idx int, mpath *MountpathInfo) func() error {
			return func() error {
				defer close(mpathChs[idx])
				o := *opts
				o.Mpath = mpath
				o.Callback = func(fqn string, de DirEntry) error {
					select {
					case <-ctx.Done():
						return cmn.NewAbortedError("mpath: " + mpath.Path)
					default:
						break
					}
					if de.IsDir() {
						return nil
					}
					mpathChs[idx] <- fqn
					return nil
				}
				return Walk(&o)
			}
		}(idx, mpath))
		idx++
	}

	// TODO: handle case when `opts.Sorted == false`
	cmn.Assert(opts.Sorted)
	group.Go(func() error {
		var (
			h = &objInfos{}
		)
		heap.Init(h)

		for i := 0; i < len(mpathChs); i++ {
			if fqn, ok := <-mpathChs[i]; ok {
				heap.Push(h, objInfo{mpathIdx: i, fqn: fqn})
			}
		}

		for h.Len() > 0 {
			v := heap.Pop(h)
			info := v.(objInfo)
			if err := opts.Callback(info.fqn, nil); err != nil {
				return err
			}
			if fqn, ok := <-mpathChs[info.mpathIdx]; ok {
				heap.Push(h, objInfo{mpathIdx: info.mpathIdx, fqn: fqn})
			}
		}
		return nil
	})

	return group.Wait()
}
