// Package mpather provides per-mountpath concepts.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package mpather

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"

	"golang.org/x/sync/errgroup"
)

//
// one-worker (with a worker.do method) per mountpath
// currently, used only by mirror/x-put
//

type (
	WorkerGroupOpts struct {
		Callback   func(lom *core.LOM, buf []byte)
		Slab       *memsys.Slab
		WorkChSize int
	}

	// WorkerGroup starts one worker per mountpath; each worker receives (*core.LOM) tasks
	// and executes the specified callback.
	WorkerGroup struct {
		wg      *errgroup.Group
		workers map[string]*worker
	}
	worker struct {
		opts     *WorkerGroupOpts
		mi       *fs.Mountpath
		workCh   chan core.LIF
		chanFull cos.ChanFull
		stopCh   cos.StopCh
	}
)

func NewWorkerGroup(opts *WorkerGroupOpts) (*WorkerGroup, error) {
	var (
		avail   = fs.GetAvail()
		l       = len(avail)
		workers = make(map[string]*worker, l)
	)
	if l == 0 {
		return nil, cmn.ErrNoMountpaths
	}
	for _, mi := range avail {
		workers[mi.Path] = newWorker(opts, mi)
	}
	wkg := &WorkerGroup{wg: &errgroup.Group{}, workers: workers}
	return wkg, nil
}

func (wkg *WorkerGroup) Run() {
	for _, worker := range wkg.workers {
		wkg.wg.Go(worker.do)
	}
}

func (wkg *WorkerGroup) ChanFullTotal() (n int64) {
	for _, worker := range wkg.workers {
		n += worker.chanFull.Load()
	}
	return n
}

func (wkg *WorkerGroup) PostLIF(lom *core.LOM) error {
	mi := lom.Mountpath()
	worker, ok := wkg.workers[mi.Path]
	if !ok {
		return fmt.Errorf("post-lif: %s not found", mi)
	}
	l, c := len(worker.workCh), cap(worker.workCh)
	worker.chanFull.Check(l, c)
	worker.workCh <- lom.LIF()

	return nil
}

// Stop aborts all the workers. It should be called after we are sure no more
// new tasks will be dispatched.
func (wkg *WorkerGroup) Stop() (n int) {
	for _, worker := range wkg.workers {
		n += worker.abort()
	}
	_ = wkg.wg.Wait()
	return
}

func newWorker(opts *WorkerGroupOpts, mi *fs.Mountpath) (w *worker) {
	w = &worker{
		opts:   opts,
		mi:     mi,
		workCh: make(chan core.LIF, opts.WorkChSize),
	}
	w.stopCh.Init()
	return
}

func (w *worker) do() error {
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
	n := core.DrainLIF(w.workCh)
	w.stopCh.Close()
	return n
}

func (w *worker) String() string { return fmt.Sprintf("worker %q", w.mi.Path) }
