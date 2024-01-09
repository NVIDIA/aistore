// Package dload implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package dload

import (
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
)

const queueChSize = 1000

type (
	queueEntry = map[string]struct{}

	queue struct {
		ch chan *singleTask      // for pending downloads
		m  map[string]queueEntry // jobID -> set of request uid
		mu sync.RWMutex
	}

	// Each jogger corresponds to an mpath. All types of download requests
	// corresponding to the jogger's mpath are forwarded to the jogger. Joggers
	// exist in the Downloader's jogger member variable, and run only when there
	// are dlTasks.
	jogger struct {
		mpath       string
		terminateCh cos.StopCh // synchronizes termination
		parent      *dispatcher
		q           *queue
		task        *singleTask // currently running download task
		mtx         sync.Mutex
		stopAgent   bool
	}
)

func newJogger(d *dispatcher, mpath string) (j *jogger) {
	j = &jogger{mpath: mpath, parent: d, q: newQueue()}
	j.terminateCh.Init()
	return
}

func (j *jogger) jog() {
	for {
		t := j.q.get()
		if t == nil {
			break
		}

		j.mtx.Lock()
		// Check if the task exists to ensure that the job wasn't removed while
		// we waited on the queue. We must do it under the jogger's lock to ensure that
		// there is no race between aborting job and marking it as being handled.
		if !j.taskExists(t) {
			t.job.throttler().release()
			j.mtx.Unlock()
			continue
		}

		if j.stopAgent {
			// Jogger has been stopped so we must mark task as failed. We do not
			// `break` here because we want to drain the queue, otherwise some
			// of the tasks may be in the queue and therefore the finished
			// counter won't be correct.
			t.job.throttler().release()
			t.markFailed(internalErrorMsg)
			j.mtx.Unlock()
			continue
		}

		j.task = t
		j.task.init()
		j.mtx.Unlock()

		// do
		lom := core.AllocLOM(t.obj.objName)
		t.download(lom)

		// finish, cleanup
		core.FreeLOM(lom)
		t.cancel()

		t.job.throttler().release()

		j.mtx.Lock()
		j.task.persist()
		j.task = nil
		j.mtx.Unlock()
		if j.q.del(t) {
			j.parent.xdl.DecPending()
		}
	}

	j.q.cleanup()
	j.terminateCh.Close()
}

// stop terminates the jogger and waits for it to finish.
func (j *jogger) stop() {
	nlog.Infof("Stopping jogger for mpath: %s", j.mpath)

	j.mtx.Lock()
	j.stopAgent = true
	if j.task != nil {
		j.task.cancel() // Stops running task (cancels download).
	}
	j.mtx.Unlock()
	j.q.close()

	<-j.terminateCh.Listen()
}

// Returns channel which task should be put into.
func (j *jogger) putCh(t *singleTask) chan<- *singleTask {
	j.q.mu.Lock()
	ok, ch := j.q.putCh(t)
	j.q.mu.Unlock()
	if ok {
		j.parent.xdl.IncPending()
	}
	return ch
}

func (j *jogger) getTask(jobID string) (task *singleTask) {
	j.mtx.Lock()
	if j.task != nil && j.task.jobID() == jobID {
		task = j.task
	}
	j.mtx.Unlock()
	return task
}

func (j *jogger) abortJob(id string) {
	var task *singleTask

	j.mtx.Lock()

	j.q.mu.Lock()
	cnt := j.q.removeJob(id) // remove from pending
	j.q.mu.Unlock()
	j.parent.xdl.SubPending(cnt)

	if j.task != nil && j.task.jobID() == id {
		task = j.task
		// iff the task belongs to the specified job
		j.task.cancel()
	}

	j.mtx.Unlock()

	if task != nil && cmn.Rom.FastV(4, cos.SmoduleDload) /*verbose*/ {
		nlog.Infof("%s: abort-job[%s, mpath=%s], task=%s", core.T.String(), id, j.mpath, j.task.String())
	}
}

func (j *jogger) taskExists(t *singleTask) (exists bool) {
	j.q.mu.RLock()
	exists = j.q.exists(t.jobID(), t.uid())
	j.q.mu.RUnlock()
	return exists
}

// Returns true if there is any pending task for a given job (either running or in queue),
// false otherwise.
func (j *jogger) pending(id string) bool {
	task := j.getTask(id)
	return task != nil || j.q.pending(id)
}

func newQueue() *queue {
	return &queue{
		ch: make(chan *singleTask, queueChSize),
		m:  make(map[string]queueEntry),
	}
}

// PRECONDITION: `q.Lock()` must be taken.
func (q *queue) putCh(t *singleTask) (ok bool, ch chan<- *singleTask) {
	if q.stopped() || q.exists(t.jobID(), t.uid()) {
		// If task already exists or the queue was stopped we should just omit it
		// hence return channel which immediately accepts and omits the task.
		return false, make(chan *singleTask, 1)
	}
	q.putToSet(t.jobID(), t.uid())
	return true, q.ch
}

// get retrieves first task in the queue.
func (q *queue) get() (foundTask *singleTask) {
	t, ok := <-q.ch
	if !ok {
		return nil
	}

	// NOTE: We do not delete task here but postpone it until the task
	//  has `Finished` to prevent situation where we put task which is
	//  being downloaded.
	return t
}

func (q *queue) del(t *singleTask) bool {
	q.mu.Lock()
	deleted := q.removeFromSet(t.jobID(), t.uid())
	q.mu.Unlock()
	return deleted
}

func (q *queue) cleanup() {
	q.mu.Lock()
	q.ch = nil
	q.m = nil
	q.mu.Unlock()
}

// PRECONDITION: `q.RLock()` must be taken.
func (q *queue) stopped() bool {
	return q.m == nil || q.ch == nil
}

// PRECONDITION: `q.RLock()` must be taken.
func (q *queue) exists(jobID, requestUID string) bool {
	jobM, ok := q.m[jobID]
	if !ok {
		return false
	}

	_, ok = jobM[requestUID]
	return ok
}

func (q *queue) pending(jobID string) (exists bool) {
	q.mu.RLock()
	_, exists = q.m[jobID]
	q.mu.RUnlock()
	return exists
}

// PRECONDITION: `q.Lock()` must be taken.
func (q *queue) putToSet(jobID, requestUID string) {
	if _, ok := q.m[jobID]; !ok {
		q.m[jobID] = make(queueEntry)
	}
	q.m[jobID][requestUID] = struct{}{}
}

// PRECONDITION: `q.Lock()` must be taken.
func (q *queue) removeFromSet(jobID, requestUID string) (deleted bool) {
	jobM, ok := q.m[jobID]
	if !ok {
		return false
	}

	if _, ok := jobM[requestUID]; ok {
		delete(jobM, requestUID)
		if len(jobM) == 0 {
			delete(q.m, jobID)
		}
		return true
	}
	return false
}

// PRECONDITION: `q.Lock()` must be taken.
func (q *queue) removeJob(id string) int {
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

func (q *queue) close() {
	if q.ch != nil {
		close(q.ch)
	}
}
