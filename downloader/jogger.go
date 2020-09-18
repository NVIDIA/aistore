// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"context"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
)

const queueChSize = 1000

type (
	queueEntry = map[string]struct{}

	queue struct {
		sync.RWMutex
		ch chan *singleObjectTask // for pending downloads
		m  map[string]queueEntry  // jobID -> set of request uid
	}

	// Each jogger corresponds to an mpath. All types of download requests
	// corresponding to the jogger's mpath are forwarded to the jogger. Joggers
	// exist in the Downloader's jogger member variable, and run only when there
	// are dlTasks.
	jogger struct {
		mpath       string
		terminateCh *cmn.StopCh // synchronizes termination
		parent      *dispatcher

		q *queue

		mtx sync.RWMutex
		// lock protected
		task      *singleObjectTask // currently running download task
		stopAgent bool
	}
)

func newJogger(d *dispatcher, mpath string) *jogger {
	return &jogger{
		mpath:       mpath,
		parent:      d,
		q:           newQueue(),
		terminateCh: cmn.NewStopCh(),
	}
}

func (j *jogger) jog() {
	glog.Infof("Starting jogger for mpath %q.", j.mpath)
	for {
		t, skip := j.q.get()
		if t == nil {
			break
		}
		if skip {
			t.job.throttler().release()
			continue
		}

		j.mtx.Lock()
		if j.stopAgent {
			j.mtx.Unlock()
			break
		}
		j.task = t
		j.mtx.Unlock()

		t.download()
		t.job.throttler().release()

		j.mtx.Lock()
		j.task.persist()
		j.task = nil
		j.mtx.Unlock()
		if exists := j.q.delete(t); exists {
			j.parent.parent.DecPending()
		}
	}

	j.q.cleanup()
	j.terminateCh.Close()
}

// Stop terminates the jogger
func (j *jogger) stop() {
	glog.Infof("Stopping jogger for mpath: %s", j.mpath)

	j.mtx.Lock()
	j.stopAgent = true
	if j.task != nil {
		j.task.markFailed(internalErrorMsg)
	}
	j.mtx.Unlock()

	<-j.terminateCh.Listen()
}

// Returns channel which task should be put into.
func (j *jogger) putCh(t *singleObjectTask) chan<- *singleObjectTask {
	ok, ch := j.q.putCh(t)
	if ok {
		j.parent.parent.IncPending()
	}
	return ch
}

func (j *jogger) getTask() (t *singleObjectTask) {
	j.mtx.RLock()
	t = j.task
	j.mtx.RUnlock()
	return
}

func (j *jogger) abortJob(id string) {
	j.mtx.RLock()
	defer j.mtx.RUnlock()

	// Remove pending tasks in queue.
	cnt := j.q.removeJob(id)
	j.parent.parent.SubPending(cnt)

	// Abort currently running task, if belongs to a given job.
	if j.task != nil && j.task.id() == id {
		j.task.cancel()
	}
}

// Returns `true` if there is any pending task for a given job (either running
// or still in queue), `false` otherwise.
func (j *jogger) pending(id string) bool {
	task := j.getTask()
	return (task != nil && task.id() == id) || j.q.pending(id)
}

func newQueue() *queue {
	return &queue{
		ch: make(chan *singleObjectTask, queueChSize),
		m:  make(map[string]queueEntry),
	}
}

func (q *queue) putCh(t *singleObjectTask) (ok bool, ch chan<- *singleObjectTask) {
	q.Lock()
	if q.stopped() || q.exists(t.id(), t.uid()) {
		// If task already exists or the queue was stopped we should just omit it
		// hence return channel which immediately accepts and omits the task.
		q.Unlock()
		return false, make(chan *singleObjectTask, 1)
	}
	q.putToSet(t.id(), t.uid())
	q.Unlock()

	return true, q.ch
}

// Get tries to find first task which was not yet Aborted
func (q *queue) get() (foundTask *singleObjectTask, skip bool) {
	t, ok := <-q.ch
	if !ok {
		return nil, false
	}

	q.RLock()
	defer q.RUnlock()
	if !q.exists(t.id(), t.uid()) {
		// The job was removed so we must skip tasks which no longer exist.
		return t, true
	}

	// NOTE: We do not delete task here but postpone it until the task
	//  has `Finished` to prevent situation where we put task which is
	//  being downloaded.

	ctx, cancel := context.WithCancel(context.Background())
	t.downloadCtx = ctx
	t.cancelFunc = cancel
	return t, false
}

func (q *queue) delete(t *singleObjectTask) bool {
	q.Lock()
	exists := q.exists(t.id(), t.uid())
	q.removeFromSet(t.id(), t.uid())
	q.Unlock()
	return exists
}

func (q *queue) cleanup() {
	q.Lock()
	q.ch = nil
	q.m = nil
	q.Unlock()
}

// NOTE: Should be called under `q.RLock()`.
func (q *queue) stopped() bool {
	return q.m == nil || q.ch == nil
}

// NOTE: Should be called under `q.RLock()`.
func (q *queue) exists(jobID, requestUID string) bool {
	jobM, ok := q.m[jobID]

	if !ok {
		return false
	}

	_, ok = jobM[requestUID]
	return ok
}

func (q *queue) pending(jobID string) bool {
	q.RLock()
	defer q.RUnlock()
	_, exists := q.m[jobID]
	return exists
}

// NOTE: Should be called under `q.Lock()`.
func (q *queue) putToSet(jobID, requestUID string) {
	if _, ok := q.m[jobID]; !ok {
		q.m[jobID] = make(queueEntry)
	}

	q.m[jobID][requestUID] = struct{}{}
}

// NOTE: Should be called under `q.Lock()`.
func (q *queue) removeFromSet(jobID, requestUID string) {
	jobM, ok := q.m[jobID]
	if !ok {
		return
	}

	if _, ok := jobM[requestUID]; ok {
		delete(jobM, requestUID)

		if len(jobM) == 0 {
			delete(q.m, jobID)
		}
	}
}

func (q *queue) removeJob(id string) int {
	q.Lock()
	defer q.Unlock()
	if q.stopped() {
		return 0
	}
	jobM, ok := q.m[id]
	if !ok {
		return 0
	}
	delete(q.m, id)
	return len(jobM)
}
