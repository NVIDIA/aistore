// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"golang.org/x/sync/errgroup"
)

// Dispatcher serves as middle layer between receiving download requests
// and serving them to joggers which actually download objects from a remote location.

type (
	dispatcher struct {
		parent *Xact

		startupSema startupSema        // Semaphore which synchronizes goroutines at dispatcher startup.
		joggers     map[string]*jogger // mpath -> jogger

		mtx      sync.RWMutex           // Protects map defined below.
		abortJob map[string]*cos.StopCh // jobID -> abort job chan

		downloadCh chan DlJob

		stopCh *cos.StopCh
	}

	startupSema struct {
		started atomic.Bool
	}
)

////////////////
// dispatcher //
////////////////

func newDispatcher(parent *Xact) *dispatcher {
	initInfoStore(db) // initialize only once // TODO -- FIXME: why here?

	return &dispatcher{
		parent:      parent,
		startupSema: startupSema{},
		joggers:     make(map[string]*jogger, 8),
		downloadCh:  make(chan DlJob),
		stopCh:      cos.NewStopCh(),
		abortJob:    make(map[string]*cos.StopCh, 100),
	}
}

func (d *dispatcher) run() (err error) {
	var (
		// limit the number of concurrent job dispatches (goroutines)
		sema       = cos.NewSemaphore(5 * fs.NumAvail())
		group, ctx = errgroup.WithContext(context.Background())
	)
	availablePaths := fs.GetAvail()
	for mpath := range availablePaths {
		d.addJogger(mpath)
	}
	// allow other goroutines to run
	d.startupSema.markStarted()

mloop:
	for {
		select {
		case <-d.parent.IdleTimer():
			glog.Infof("%s timed out. Exiting...", d.parent.Name())
			break mloop
		case errCause := <-d.parent.ChanAbort():
			glog.Infof("%s aborted (cause %v). Exiting...", d.parent.Name(), errCause)
			break mloop
		case <-ctx.Done():
			break mloop
		case job := <-d.downloadCh:
			// Start dispatching each job in new goroutine to make sure that
			// all joggers are busy downloading the tasks (jobs with limits
			// may not saturate the full downloader throughput).
			d.mtx.Lock()
			d.abortJob[job.ID()] = cos.NewStopCh()
			d.mtx.Unlock()

			select {
			case <-d.parent.IdleTimer():
				glog.Infof("%s has timed out. Exiting...", d.parent.Name())
				break mloop
			case errCause := <-d.parent.ChanAbort():
				glog.Infof("%s has been aborted (cause %v). Exiting...", d.parent.Name(), errCause)
				break mloop
			case <-ctx.Done():
				break mloop
			case <-sema.TryAcquire():
				group.Go(func() error {
					defer sema.Release()
					if !d.dispatchDownload(job) {
						return cmn.NewErrAborted(job.String(), "download", nil)
					}
					return nil
				})
			}
		}
	}

	d.stop()
	return group.Wait()
}

// stop running joggers
// no need to cleanup maps, dispatcher should not be used after stop()
func (d *dispatcher) stop() {
	d.stopCh.Close()
	for _, jogger := range d.joggers {
		jogger.stop()
	}
}

func (d *dispatcher) addJogger(mpath string) {
	_, ok := d.joggers[mpath]
	debug.Assert(!ok)
	j := newJogger(d, mpath)
	go j.jog()
	d.joggers[mpath] = j
}

func (d *dispatcher) cleanupJob(jobID string) {
	d.mtx.Lock()
	if ch, exists := d.abortJob[jobID]; exists {
		ch.Close()
		delete(d.abortJob, jobID)
	}
	d.mtx.Unlock()
}

// forward request to designated jogger
func (d *dispatcher) dispatchDownload(job DlJob) (ok bool) {
	defer func() {
		debug.Infof("Waiting for job %q", job.ID())
		d.waitFor(job.ID())
		debug.Infof("Job %q finished waiting for all tasks", job.ID())
		d.cleanupJob(job.ID())
		debug.Infof("Job %q cleaned up", job.ID())
		job.cleanup()
		debug.Infof("Job %q has finished", job.ID())
	}()

	if aborted := d.checkAborted(); aborted || d.checkAbortedJob(job) {
		return !aborted
	}

	diffResolver := NewDiffResolver(nil)

	diffResolver.Start()

	// In case of `!job.Sync()` we don't want to traverse the whole bucket.
	// We just want to download requested objects so we know exactly which
	// objects must be checked (compared) and which not. Therefore, only traverse
	// bucket when we need to sync the objects.
	if job.Sync() {
		go func() {
			defer diffResolver.CloseSrc()
			opts := &fs.WalkBckOpts{
				WalkOpts: fs.WalkOpts{CTs: []string{fs.ObjectType}, Sorted: true},
			}
			opts.WalkOpts.Bck.Copy(job.Bck())
			opts.Callback = func(fqn string, de fs.DirEntry) error {
				if diffResolver.Stopped() {
					return cmn.NewErrAborted(job.String(), "diff-resolver stopped", nil)
				}
				lom := &cluster.LOM{}
				if err := lom.InitFQN(fqn, job.Bck()); err != nil {
					return err
				}
				if !job.checkObj(lom.ObjName) {
					return nil
				}
				diffResolver.PushSrc(lom)
				return nil
			}
			err := fs.WalkBck(opts)
			if err != nil && !cmn.IsErrAborted(err) {
				diffResolver.Abort(err)
			}
		}()
	}

	go func() {
		defer func() {
			diffResolver.CloseDst()
			if !job.Sync() {
				diffResolver.CloseSrc()
			}
		}()

		for {
			objs, ok, err := job.genNext()
			if err != nil {
				diffResolver.Abort(err)
				return
			}
			if !ok || diffResolver.Stopped() {
				return
			}

			for _, obj := range objs {
				if d.checkAborted() {
					err := cmn.NewErrAborted(job.String(), "", nil)
					diffResolver.Abort(err)
					return
				} else if d.checkAbortedJob(job) {
					diffResolver.Stop()
					return
				}

				if !job.Sync() {
					// When it is not a sync job, push LOM for a given object
					// because we need to check if it exists.
					lom := &cluster.LOM{ObjName: obj.objName}
					if err := lom.InitBck(job.Bck()); err != nil {
						diffResolver.Abort(err)
						return
					}
					diffResolver.PushSrc(lom)
				}

				if obj.link != "" {
					diffResolver.PushDst(&WebResource{
						ObjName: obj.objName,
						Link:    obj.link,
					})
				} else {
					diffResolver.PushDst(&BackendResource{
						ObjName: obj.objName,
					})
				}
			}
		}
	}()

	for {
		result, err := diffResolver.Next()
		if err != nil {
			return false
		}
		switch result.Action {
		case DiffResolverRecv, DiffResolverSkip, DiffResolverErr, DiffResolverDelete:
			var obj dlObj
			if dst := result.Dst; dst != nil {
				obj = dlObj{
					objName:    dst.ObjName,
					link:       dst.Link,
					fromRemote: dst.Link == "",
				}
			} else {
				src := result.Src
				cos.Assert(result.Action == DiffResolverDelete)
				obj = dlObj{
					objName:    src.ObjName,
					link:       "",
					fromRemote: true,
				}
			}

			dlStore.incScheduled(job.ID())

			if result.Action == DiffResolverSkip {
				dlStore.incSkipped(job.ID())
				continue
			}

			t := &singleObjectTask{
				parent: d.parent,
				obj:    obj,
				job:    job,
			}

			if result.Action == DiffResolverErr {
				t.markFailed(result.Err.Error())
				continue
			}

			if result.Action == DiffResolverDelete {
				cos.Assert(job.Sync())
				if _, err := d.parent.t.EvictObject(result.Src); err != nil {
					t.markFailed(err.Error())
				} else {
					dlStore.incFinished(job.ID())
				}
				continue
			}

			ok, err := d.blockingDispatchDownloadSingle(t)
			if err != nil {
				glog.Errorf("%s failed to download %s: %v", job, obj.objName, err)
				dlStore.setAborted(job.ID()) // TODO -- FIXME: pass (report, handle) error, here and elsewhere
				return ok
			}
			if !ok {
				dlStore.setAborted(job.ID())
				return false
			}
		case DiffResolverSend:
			cos.Assert(job.Sync())
		case DiffResolverEOF:
			dlStore.setAllDispatched(job.ID(), true)
			return true
		}
	}
}

func (d *dispatcher) jobAbortedCh(jobID string) *cos.StopCh {
	d.mtx.RLock()
	defer d.mtx.RUnlock()
	if abCh, ok := d.abortJob[jobID]; ok {
		return abCh
	}

	// Channel always sending something if entry in the map is missing.
	abCh := cos.NewStopCh()
	abCh.Close()
	return abCh
}

func (d *dispatcher) checkAbortedJob(job DlJob) bool {
	select {
	case <-d.jobAbortedCh(job.ID()).Listen():
		return true
	default:
		return false
	}
}

func (d *dispatcher) checkAborted() bool {
	select {
	case <-d.stopCh.Listen():
		return true
	default:
		return false
	}
}

// returns false if dispatcher encountered hard error, true otherwise
func (d *dispatcher) blockingDispatchDownloadSingle(task *singleObjectTask) (ok bool, err error) {
	bck := cluster.CloneBck(task.job.Bck())
	if err := bck.Init(d.parent.t.Bowner()); err != nil {
		return true, err
	}

	mi, _, err := cluster.HrwMpath(bck.MakeUname(task.obj.objName))
	if err != nil {
		return false, err
	}
	jogger, ok := d.joggers[mi.Path]
	if !ok {
		err := fmt.Errorf("no jogger for mpath %s exists", mi.Path)
		return false, err
	}

	// NOTE: Throttle job before making jogger busy - we don't want to clog the
	//  jogger as other tasks from other jobs can be already ready to download.
	select {
	case <-task.job.throttler().tryAcquire():
		break
	case <-d.jobAbortedCh(task.job.ID()).Listen():
		return true, nil
	}

	// Secondly, try to push the new task into queue.
	select {
	// TODO -- FIXME: currently, dispatcher halts if any given jogger is "full" but others available
	case jogger.putCh(task) <- task:
		return true, nil
	case <-d.jobAbortedCh(task.job.ID()).Listen():
		task.job.throttler().release()
		return true, nil
	case <-d.stopCh.Listen():
		task.job.throttler().release()
		return false, nil
	}
}

func (d *dispatcher) adminReq(req *request) (resp any, statusCode int, err error) {
	debug.Infof("Admin request (id: %q, action: %q, onlyActive: %t)", req.id, req.action, req.onlyActive)

	// Need to make sure that the dispatcher has fully initialized and started,
	// and it's ready for processing the requests.
	d.startupSema.waitForStartup()

	switch req.action {
	case actStatus:
		d.handleStatus(req)
	case actAbort:
		d.handleAbort(req)
	case actRemove:
		d.handleRemove(req)
	default:
		debug.Assertf(false, "%v; %v", req, req.action)
	}
	r := req.response
	return r.value, r.statusCode, r.err
}

func (d *dispatcher) handleRemove(req *request) {
	jInfo, err := d.parent.checkJob(req)
	if err != nil {
		return
	}

	// There's a slight chance this doesn't happen if target rejoins after target checks for download not running
	dlInfo := jInfo.ToDlJobInfo()
	if dlInfo.JobRunning() {
		req.errRsp(fmt.Errorf("download job with id = %s is still running", jInfo.ID), http.StatusBadRequest)
		return
	}

	dlStore.delJob(req.id)
	req.okRsp(nil)
}

func (d *dispatcher) handleAbort(req *request) {
	_, err := d.parent.checkJob(req)
	if err != nil {
		return
	}

	d.jobAbortedCh(req.id).Close()

	for _, j := range d.joggers {
		j.abortJob(req.id)
	}

	dlStore.setAborted(req.id)
	req.okRsp(nil)
}

func (d *dispatcher) handleStatus(req *request) {
	var (
		finishedTasks []TaskDlInfo
		dlErrors      []TaskErrInfo
	)

	jInfo, err := d.parent.checkJob(req)
	if err != nil {
		return
	}

	currentTasks := d.activeTasks(req.id)
	if !req.onlyActive {
		finishedTasks, err = dlStore.getTasks(req.id)
		if err != nil {
			req.errRsp(err, http.StatusInternalServerError)
			return
		}

		dlErrors, err = dlStore.getErrors(req.id)
		if err != nil {
			req.errRsp(err, http.StatusInternalServerError)
			return
		}
		sort.Sort(TaskErrByName(dlErrors))
	}

	req.okRsp(&DlStatusResp{
		DlJobInfo:     jInfo.ToDlJobInfo(),
		CurrentTasks:  currentTasks,
		FinishedTasks: finishedTasks,
		Errs:          dlErrors,
	})
}

func (d *dispatcher) activeTasks(reqID string) []TaskDlInfo {
	currentTasks := make([]TaskDlInfo, 0, len(d.joggers))
	for _, j := range d.joggers {
		task := j.getTask()
		if task != nil && task.jobID() == reqID {
			currentTasks = append(currentTasks, task.ToTaskDlInfo())
		}
	}

	sort.Sort(TaskInfoByName(currentTasks))
	return currentTasks
}

// pending returns `true` if any joggers has pending tasks for a given `reqID`,
// `false` otherwise.
func (d *dispatcher) pending(jobID string) bool {
	for _, j := range d.joggers {
		if j.pending(jobID) {
			return true
		}
	}
	return false
}

// PRECONDITION: All tasks should be dispatched.
func (d *dispatcher) waitFor(jobID string) {
	for ; ; time.Sleep(time.Second) {
		if !d.pending(jobID) {
			return
		}
	}
}

/////////////////
// startupSema //
/////////////////

func (ss *startupSema) markStarted() { ss.started.Store(true) }

func (ss *startupSema) waitForStartup() {
	const (
		sleep   = 500 * time.Millisecond
		timeout = 10 * time.Second
		errmsg  = "FATAL: dispatcher takes too much time to start"
	)
	if ss.started.Load() {
		return
	}
	for total := time.Duration(0); !ss.started.Load(); total += sleep {
		time.Sleep(sleep)
		// should never happen even on slowest machines
		debug.Assert(total < timeout, errmsg)
		if total >= timeout && total < timeout+sleep*2 {
			glog.Errorln(errmsg)
		}
	}
}
