// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
)

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
		terminateCh chan struct{} // synchronizes termination
		parent      *dispatcher

		q *queue

		sync.Mutex
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
		terminateCh: make(chan struct{}, 1),
	}
}

func (j *jogger) enqueue(task *singleObjectTask) error {
	cmn.Assert(task != nil)
	added, err := j.q.put(task)
	if added {
		j.parent.parent.IncPending()
	}

	return err
}

func (j *jogger) jog() {
	glog.Infof("Starting jogger for mpath %q.", j.mpath)
	for {
		t := j.q.get()
		if t == nil {
			break
		}
		j.Lock()
		if j.stopAgent {
			j.Unlock()
			break
		}

		j.task = t
		j.Unlock()

		// Start download
		go t.download()

		// Await abort or completion
		<-t.waitForFinish()

		j.Lock()
		j.task.persist()
		j.task = nil
		j.Unlock()
		if exists := j.q.delete(t.request); exists {
			j.parent.parent.DecPending()
		}
	}

	j.q.cleanup()
	j.terminateCh <- struct{}{}
}

// Stop terminates the jogger
func (j *jogger) stop() {
	glog.Infof("Stopping jogger for mpath: %s", j.mpath)
	j.q.stop()

	j.Lock()
	j.stopAgent = true
	if j.task != nil {
		j.task.abort(internalErrorMessage(), errors.New("stopped jogger"))
	}
	j.Unlock()

	<-j.terminateCh
}

func newQueue() *queue {
	return &queue{
		ch: make(chan *singleObjectTask, queueChSize),
		m:  make(map[string]queueEntry),
	}
}

func (q *queue) put(t *singleObjectTask) (added bool, err error) {
	q.Lock()
	defer q.Unlock()
	if q.exists(t.request.id, t.request.uid()) {
		// If request already exists we should just omit this
		return false, nil
	}

	select {
	case q.ch <- t:
		break
	default:
		return false, fmt.Errorf("error trying to process task %v: queue is full, try again later", t)
	}
	q.putToSet(t.id, t.request.uid())
	return true, nil
}

// Get tries to find first task which was not yet Aborted
func (q *queue) get() (foundTask *singleObjectTask) {
	for foundTask == nil {
		t, ok := <-q.ch
		if !ok {
			foundTask = nil
			return
		}

		q.RLock()
		if q.exists(t.request.id, t.request.uid()) {
			// NOTE: We do not delete task here but postpone it until the task
			// has Finished to prevent situation where we put task which is being
			// downloaded.
			foundTask = t
		}
		q.RUnlock()
	}

	timeout := cmn.GCO.Get().Downloader.Timeout
	if foundTask.timeout != "" {
		var err error
		timeout, err = time.ParseDuration(foundTask.timeout)
		cmn.AssertNoErr(err) // This should be checked beforehand
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	foundTask.downloadCtx = ctx
	foundTask.cancelFunc = cancel
	return
}

func (q *queue) delete(req *request) bool {
	q.Lock()
	exists := q.exists(req.id, req.uid())
	q.removeFromSet(req.id, req.uid())
	q.Unlock()
	return exists
}

func (q *queue) stop() {
	q.RLock()
	if q.ch != nil {
		close(q.ch)
	}
	q.RUnlock()
}

func (q *queue) cleanup() {
	q.Lock()
	q.ch = nil
	q.m = nil
	q.Unlock()
}

// exists should be called under RLock()
func (q *queue) exists(jobID, requestUID string) bool {
	jobM, ok := q.m[jobID]

	if !ok {
		return false
	}

	_, ok = jobM[requestUID]
	return ok
}

// putToSet should be called under Lock()
func (q *queue) putToSet(jobID, requestUID string) {
	if _, ok := q.m[jobID]; !ok {
		q.m[jobID] = make(map[string]struct{})
	}

	q.m[jobID][requestUID] = struct{}{}
}

// removeFromSet should be called under Lock()
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
