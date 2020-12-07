// Package mpather provides per-mountpath concepts.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package mpather

import (
	"fmt"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"golang.org/x/sync/errgroup"
)

type (
	WorkerGroupOpts struct {
		Callback  func(lom *cluster.LOM, buf []byte)
		QueueSize int
		Slab      *memsys.Slab
	}

	// WorkerGroup starts worker per mountpath which can receive tasks (*cluster.LOM).
	// When a task is received then it's scheduled to corresponding mountpath worker
	// which executes a specified callback on the received LOM.
	WorkerGroup struct {
		wg      *errgroup.Group
		workers map[string]*worker
	}

	worker struct {
		opts      *WorkerGroupOpts
		mpathInfo *fs.MountpathInfo
		workCh    chan *cluster.LOM
		stopCh    *cmn.StopCh
	}
)

func NewWorkerGroup(opts *WorkerGroupOpts) *WorkerGroup {
	debug.Assert(opts.QueueSize > 0) // For now we expect buffered channels.

	var (
		mpaths, _ = fs.Get()
		workers   = make(map[string]*worker, len(mpaths))
	)

	for _, mpathInfo := range mpaths {
		workers[mpathInfo.Path] = newWorker(opts, mpathInfo)
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

func (wg *WorkerGroup) Do(lom *cluster.LOM) bool {
	path := lom.MpathInfo.Path
	if worker, ok := wg.workers[path]; ok {
		worker.workCh <- lom
		return true
	}
	return false
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

func newWorker(opts *WorkerGroupOpts, mpathInfo *fs.MountpathInfo) *worker {
	return &worker{
		opts:      opts,
		mpathInfo: mpathInfo,
		workCh:    make(chan *cluster.LOM, opts.QueueSize),
		stopCh:    cmn.NewStopCh(),
	}
}

func (w *worker) work() error {
	glog.Infof("%s started", w)

	var buf []byte
	if w.opts.Slab != nil {
		buf = w.opts.Slab.Alloc()
		defer w.opts.Slab.Free(buf)
	}

	for {
		select {
		case src := <-w.workCh:
			lom := src.Clone(src.FQN)
			w.opts.Callback(lom, buf)
		case <-w.stopCh.Listen(): // Worker has been aborted.
			close(w.workCh)

			// Make sure there is nothing in the `workCh` once we aborted.
			// In case there is, this means that workers were not aborted correctly.
			_, ok := <-w.workCh
			cmn.Assert(!ok)

			return cmn.NewAbortedError(w.String())
		}
	}
}

func (w *worker) abort() int {
	n := drainWorkCh(w.workCh)
	w.stopCh.Close()
	return n
}

func (w *worker) String() string { return fmt.Sprintf("worker %q", w.mpathInfo.Path) }

func drainWorkCh(workCh chan *cluster.LOM) (n int) {
	for {
		select {
		case <-workCh:
			n++
		default:
			return
		}
	}
}
