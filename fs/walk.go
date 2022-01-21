// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"container/heap"
	"context"
	"os"
	"path/filepath"
	"sort"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
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
	DirEntry interface {
		IsDir() bool
	}

	walkFunc func(fqn string, de DirEntry) error

	Options struct {
		Dir      string
		Mi       *MountpathInfo
		Bck      cmn.Bck
		CTs      []string
		Callback walkFunc
		Sorted   bool
	}

	WalkBckOptions struct {
		Options
		ValidateCallback walkFunc // should return filepath.SkipDir to skip directory without an error
	}

	errCallbackWrapper struct {
		counter atomic.Int64
	}

	objInfo struct {
		mpathIdx int
		fqn      string
		objName  string
		dirEntry DirEntry
	}
	objInfos []objInfo

	walkEntry struct {
		fqn      string
		dirEntry DirEntry
	}
	walkCb struct {
		mi       *MountpathInfo
		validate walkFunc
		ctx      context.Context
		workCh   chan *walkEntry
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

// interface guard
var _ DirEntry = (*godirwalk.Dirent)(nil)

func (opts *Options) callback(fqn string, de *godirwalk.Dirent) error {
	return opts.Callback(fqn, de)
}

func (h objInfos) Len() int           { return len(h) }
func (h objInfos) Less(i, j int) bool { return h[i].objName < h[j].objName }
func (h objInfos) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *objInfos) Push(x interface{}) {
	info := x.(objInfo)
	debug.Assert(info.objName == "")
	parsedFQN, err := ParseFQN(info.fqn)
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
				fqns = append(fqns, opts.Mi.MakePathCT(opts.Bck, ct))
			}
		} else {
			// all content-type paths for all bucket subdirectories
			fqns, err = allMpathCTpaths(opts)
			if len(fqns) == 0 || err != nil {
				return err
			}
		}
	}
	scratch, slab := memsys.PageMM().AllocSize(memsys.PageSize * 2)
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

func allMpathCTpaths(opts *Options) (fqns []string, err error) {
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
			fqns = append(fqns, opts.Mi.MakePathCT(bck, ct))
		}
	}
	return
}

func AllMpathBcks(opts *Options) (bcks []cmn.Bck, err error) {
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

func mpathChildren(opts *Options) (children []string, err error) {
	var (
		fqn           = opts.Mi.MakePathBck(opts.Bck)
		scratch, slab = memsys.PageMM().AllocSize(memsys.PageSize * 2)
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

func WalkBck(opts *WalkBckOptions) error {
	var (
		availablePaths = GetAvail()
		mpathChs       = make([]chan *walkEntry, len(availablePaths))
		group, ctx     = errgroup.WithContext(context.Background())
	)
	for i := 0; i < len(availablePaths); i++ {
		mpathChs[i] = make(chan *walkEntry, mpathQueueSize)
	}
	debug.Assert(opts.Mi == nil)
	idx := 0
	for _, mi := range availablePaths {
		group.Go(func(idx int, mi *MountpathInfo) func() error {
			return func() error {
				var (
					o      = opts.Options
					workCh = mpathChs[idx]
				)
				defer close(workCh)
				o.Mi = mi
				wcb := &walkCb{mi: mi, validate: opts.ValidateCallback, ctx: ctx, workCh: workCh}
				o.Callback = wcb.walkBckMpath
				return Walk(&o)
			}
		}(idx, mi))
		idx++
	}

	// TODO: handle case when `opts.Sorted == false`
	debug.Assert(opts.Sorted)
	group.Go(func() error {
		h := &objInfos{}
		heap.Init(h)

		for i := 0; i < len(mpathChs); i++ {
			if pair, ok := <-mpathChs[i]; ok {
				heap.Push(h, objInfo{mpathIdx: i, fqn: pair.fqn, dirEntry: pair.dirEntry})
			}
		}

		for h.Len() > 0 {
			v := heap.Pop(h)
			info := v.(objInfo)
			if err := opts.Callback(info.fqn, info.dirEntry); err != nil {
				return err
			}
			if pair, ok := <-mpathChs[info.mpathIdx]; ok {
				heap.Push(h, objInfo{mpathIdx: info.mpathIdx, fqn: pair.fqn, dirEntry: pair.dirEntry})
			}
		}
		return nil
	})

	return group.Wait()
}

func (wcb *walkCb) walkBckMpath(fqn string, de DirEntry) error {
	select {
	case <-wcb.ctx.Done():
		return cmn.NewErrAborted(wcb.mi.String(), "walk-bck-mpath", nil)
	default:
		break
	}

	if wcb.validate != nil {
		if err := wcb.validate(fqn, de); err != nil {
			// If err != filepath.SkipDir, Walk will propagate the error
			// to group.Go. Then context will be canceled, which terminates
			// all other go routines running.
			return err
		}
	}

	if de.IsDir() {
		return nil
	}

	select {
	case <-wcb.ctx.Done():
		return cmn.NewErrAborted(wcb.mi.String(), "walk-bck-mpath", nil)
	case wcb.workCh <- &walkEntry{fqn, de}:
		return nil
	}
}

func Scanner(dir string, cb func(fqn string, entry DirEntry) error) error {
	scanner, err := godirwalk.NewScanner(dir)
	if err != nil {
		return err
	}
	for scanner.Scan() {
		dirent, err := scanner.Dirent()
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		if err := cb(filepath.Join(dir, dirent.Name()), dirent); err != nil {
			return err
		}
	}
	return scanner.Err()
}
