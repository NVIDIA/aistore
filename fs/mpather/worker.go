// Package mpather provides per-mountpath concepts.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package mpather

import (
	"fmt"
	"runtime"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"golang.org/x/sync/errgroup"
)

type (
	WorkerGroupOpts struct {
		Callback  func(lom *core.LOM, buf []byte)
		Slab      *memsys.Slab
		QueueSize int
	}

	// WorkerGroup starts one worker per mountpath; each worker receives (*core.LOM) tasks
	// and executes the specified callback.
	WorkerGroup struct {
		wg      *errgroup.Group
		workers map[string]*worker
	}
	worker struct {
		opts   *WorkerGroupOpts
		mi     *fs.Mountpath
		workCh chan core.LIF
		stopCh cos.StopCh
	}
)

func NewWorkerGroup(opts *WorkerGroupOpts) *WorkerGroup {
	var (
		mpaths  = fs.GetAvail()
		workers = make(map[string]*worker, len(mpaths))
	)
	debug.Assert(opts.QueueSize > 0) // expect buffered channels
	for _, mi := range mpaths {
		workers[mi.Path] = newWorker(opts, mi)
	}
	return &WorkerGroup{
		wg:      &errgroup.Group{},
		workers: workers,
	}
}

func (wg *WorkerGroup) Run() {
	for _, worker := range wg.workers {
		wg.wg.Go(worker.work)
	}
}

func (wg *WorkerGroup) PostLIF(lom *core.LOM) (chanFull bool, err error) {
	mi := lom.Mountpath()
	worker, ok := wg.workers[mi.Path]
	if !ok {
		return false, fmt.Errorf("post-lif: %s not found", mi)
	}
	worker.workCh <- lom.LIF()
	if l, c := len(worker.workCh), cap(worker.workCh); l > c/2 {
		runtime.Gosched() // poor man's throttle
		chanFull = l == c
	}
	return
}

// Stop aborts all the workers. It should be called after we are sure no more
// new tasks will be dispatched.
func (wg *WorkerGroup) Stop() (n int) {
	for _, worker := range wg.workers {
		n += worker.abort()
	}
	_ = wg.wg.Wait()
	return
}

func newWorker(opts *WorkerGroupOpts, mi *fs.Mountpath) (w *worker) {
	w = &worker{
		opts:   opts,
		mi:     mi,
		workCh: make(chan core.LIF, opts.QueueSize),
	}
	w.stopCh.Init()
	return
}

func (w *worker) work() error {
	var buf []byte
	if w.opts.Slab != nil {
		buf = w.opts.Slab.Alloc()
		defer w.opts.Slab.Free(buf)
	}
	for {
		select {
		case lif := <-w.workCh:
			lom, err := lif.LOM()
			if err != nil {
				break
			}
			if err = lom.Load(false /*cache it*/, false); err == nil {
				w.opts.Callback(lom, buf)
			} else {
				core.FreeLOM(lom)
			}
		case <-w.stopCh.Listen(): // ABORT
			close(w.workCh)

			// `workCh` must be empty (if it is not, workers were not aborted correctly!)
			_, ok := <-w.workCh
			debug.Assert(!ok)

			return cmn.NewErrAborted(w.String(), "mpath-work", nil)
		}
	}
}

func (w *worker) abort() int {
	n := drainWorkCh(w.workCh)
	w.stopCh.Close()
	return n
}

func (w *worker) String() string { return fmt.Sprintf("worker %q", w.mi.Path) }

func drainWorkCh(workCh chan core.LIF) (n int) {
	for {
		select {
		case <-workCh:
			n++
		default:
			return
		}
	}
}
