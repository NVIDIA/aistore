// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
)

type (
	queue struct {
		sync.RWMutex
		ch chan *task // for pending downloads
		m  map[string]struct{}
	}

	// Each jogger corresponds to an mpath. All types of download requests
	// corresponding to the jogger's mpath are forwarded to the jogger. Joggers
	// exist in the Downloader's jogger member variable, and run only when there
	// are dlTasks.
	jogger struct {
		mpath       string
		terminateCh chan struct{} // synchronizes termination
		parent      *Downloader

		q *queue

		sync.Mutex
		// lock protected
		task      *task // currently running download task
		stopAgent bool
	}
)

func newJogger(d *Downloader, mpath string) *jogger {
	return &jogger{
		mpath:       mpath,
		parent:      d,
		q:           newQueue(),
		terminateCh: make(chan struct{}, 1),
	}
}

func (j *jogger) putIntoDownloadQueue(task *task) (response, bool) {
	cmn.Assert(task != nil)
	added, err, errCode := j.q.put(task)
	if err != nil {
		return response{
			err:        err,
			statusCode: errCode,
		}, false
	}

	return response{
		resp: fmt.Sprintf("Download request %s added to queue", task),
	}, added
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
			j.parent.DecPending()
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
		ch: make(chan *task, queueChSize),
		m:  make(map[string]struct{}),
	}
}

func (q *queue) put(t *task) (added bool, err error, errCode int) {
	q.Lock()
	defer q.Unlock()
	if _, exists := q.m[t.request.uid()]; exists {
		// If request already exists we should just omit this
		return false, nil, 0
	}

	select {
	case q.ch <- t:
		break
	default:
		return false, fmt.Errorf("error trying to process task %v: queue is full, try again later", t), http.StatusBadRequest
	}
	q.m[t.request.uid()] = struct{}{}
	return true, nil, 0
}

// Get tries to find first task which was not yet aborted
func (q *queue) get() (foundTask *task) {
	for foundTask == nil {
		t, ok := <-q.ch
		if !ok {
			foundTask = nil
			return
		}

		q.RLock()
		if _, exists := q.m[t.request.uid()]; exists {
			// NOTE: We do not delete task here but postpone it until the task
			// has finished to prevent situation where we put task which is being
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

func (q *queue) exists(req *request) bool {
	q.RLock()
	_, exists := q.m[req.uid()]
	q.RUnlock()
	return exists
}

func (q *queue) delete(req *request) bool {
	q.Lock()
	_, exists := q.m[req.uid()]
	delete(q.m, req.uid())
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
