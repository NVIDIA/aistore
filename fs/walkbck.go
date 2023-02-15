// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"container/heap"
	"context"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"golang.org/x/sync/errgroup"
)

// provides default low-level `jogger` to traverse a bucket - a poor-man's joggers of sorts
// for those (very few) clients that don't have their own custom implementation

type WalkBckOpts struct {
	ValidateCallback walkFunc // should return filepath.SkipDir to skip directory without an error
	WalkOpts
}

// internals
type (
	joggerBck struct {
		workCh   chan *wbe
		mi       *Mountpath
		validate walkFunc
		ctx      context.Context
		opts     WalkOpts
	}
	wbe struct { // walk bck entry
		dirEntry DirEntry
		fqn      string
	}
	wbeInfo struct {
		dirEntry DirEntry
		fqn      string
		objName  string
		mpathIdx int
	}
	wbeHeap []wbeInfo
)

func WalkBck(opts *WalkBckOpts) error {
	debug.Assert(opts.Mi == nil && opts.Sorted) // TODO: support `opts.Sorted == false`
	var (
		availablePaths = GetAvail()
		l              = len(availablePaths)
		joggers        = make([]*joggerBck, l)
		group, ctx     = errgroup.WithContext(context.Background())
		idx            int
	)
	for _, mi := range availablePaths {
		workCh := make(chan *wbe, mpathQueueSize)
		jg := &joggerBck{
			workCh:   workCh,
			mi:       mi,
			validate: opts.ValidateCallback,
			ctx:      ctx,
			opts:     opts.WalkOpts,
		}
		jg.opts.Callback = jg.cb
		jg.opts.Mi = mi
		joggers[idx] = jg
		idx++
	}

	for i := 0; i < l; i++ {
		group.Go(joggers[i].walk)
	}
	group.Go(func() error {
		h := &wbeHeap{}
		heap.Init(h)

		for i := 0; i < l; i++ {
			if wbe, ok := <-joggers[i].workCh; ok {
				heap.Push(h, wbeInfo{mpathIdx: i, fqn: wbe.fqn, dirEntry: wbe.dirEntry})
			}
		}
		for h.Len() > 0 {
			v := heap.Pop(h)
			info := v.(wbeInfo)
			if err := opts.Callback(info.fqn, info.dirEntry); err != nil {
				return err
			}
			if wbe, ok := <-joggers[info.mpathIdx].workCh; ok {
				heap.Push(h, wbeInfo{mpathIdx: info.mpathIdx, fqn: wbe.fqn, dirEntry: wbe.dirEntry})
			}
		}
		return nil
	})

	return group.Wait()
}

///////////////
// joggerBck //
///////////////

func (jg *joggerBck) walk() (err error) {
	err = Walk(&jg.opts)
	close(jg.workCh)
	return
}

func (jg *joggerBck) cb(fqn string, de DirEntry) error {
	const tag = "fs-walk-bck-mpath"
	select {
	case <-jg.ctx.Done():
		return cmn.NewErrAborted(jg.mi.String(), tag, nil)
	default:
		break
	}
	if jg.validate != nil {
		if err := jg.validate(fqn, de); err != nil {
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
	case <-jg.ctx.Done():
		return cmn.NewErrAborted(jg.mi.String(), tag, nil)
	case jg.workCh <- &wbe{de, fqn}:
		return nil
	}
}

/////////////
// wbeHeap //
/////////////

func (h wbeHeap) Len() int           { return len(h) }
func (h wbeHeap) Less(i, j int) bool { return h[i].objName < h[j].objName }
func (h wbeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *wbeHeap) Push(x any) {
	info := x.(wbeInfo)
	debug.Assert(info.objName == "")
	parsedFQN, err := ParseFQN(info.fqn)
	if err != nil {
		return
	}
	info.objName = parsedFQN.ObjName
	*h = append(*h, info)
}

func (h *wbeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
